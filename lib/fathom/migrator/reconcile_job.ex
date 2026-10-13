defmodule Fathom.Migrator.ReconcileJob do
  @moduledoc """
  Oban cron job that re-runs the rollout sweep, so even never-touched shards
  converge to HEAD and any drift self-heals. Idempotent — per-shard uniqueness
  means re-enqueuing already-queued shards is a no-op — so it can run forever on a
  schedule (see the Oban `:crontab` config).
  """
  use Oban.Worker, queue: :migrations, max_attempts: 1

  require Logger

  alias Fathom.{Directory, Migrator}
  alias Fathom.Shard.Storage
  alias Fathom.Snapshots
  alias Fathom.Snapshots.Retention
  alias Fathom.Tenants.DeleteJob

  # The rollout sweep's per-run cap (expert review 2026-07-18 #19). The default 100 keeps a single
  # hourly run bounded (migration-queue depth + S3 headroom), but at fleet scale a deep cold tail
  # (100k–millions) converges glacially — 100/hour is 2,400/day, i.e. months. Operators raise
  # `:reconcile_batch_size` to size convergence to their fleet + S3 pool; the per-shard uniqueness
  # dedup keeps a bigger bite idempotent, and enqueue_unique chunks under the Postgres bind cap.
  @default_batch_size 100

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    # Erasure and retention sweeps run FIRST and each rescues its own failures: a raise from the
    # migration steps below used to skip the delete re-drive for that hour (expert review
    # 2026-10-12 #7).
    redrive_deletes()
    sweep_promotion_snapshots()

    # Reclaim any shard stuck in `migrating` (its Oban job was lost) back to `active` first,
    # so the rollout sweep below sees it as a laggard again and re-migrates it — otherwise it
    # is invisible to every sweep forever (finding #20).
    case Directory.reclaim_stale_migrating() do
      [] ->
        :ok

      ids ->
        Logger.warning(
          "reconcile: reclaimed #{length(ids)} stale migrating shard(s): #{inspect(ids)}"
        )
    end

    # Expert review 2026-10-10 #4: a quarantine was permanent until an operator ran
    # `retry_failed/0`, so a transient brownout froze a fleet slice at the old version for good.
    # Requeue after a cool-off; a deterministically broken shard re-quarantines (one attempt per
    # cool-off period, not a hot loop).
    # Fix-review R3-2: best-effort. A Postgres blip here must not raise out of `perform/1` and skip
    # the rollout/revert sweeps below; the next hourly run retries the requeue.
    case safe_retry_cooled() do
      {:ok, requeued} when requeued > 0 ->
        Logger.warning("reconcile: re-enqueued #{requeued} cooled-off migration_failed shard(s)")

      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("reconcile: cool-off requeue failed (#{inspect(reason)}); continuing")
    end

    {:ok, _count} = Migrator.rollout(batch_size())

    # Converge shards stranded ON a yanked version above HEAD (round-2 #22): a
    # migration completing in the yank-cancel race window cut them over AFTER the
    # fleet revert read its shard set, and no laggard sweep sees schema_version >
    # head. Enqueue their reverts (idempotent — per-shard uniqueness dedups).
    case Migrator.revert_stranded() do
      {:ok, 0} ->
        :ok

      {:ok, n} ->
        Logger.warning(
          "reconcile: enqueued reverts for #{n} shard(s) stranded on yanked versions"
        )
    end

    :ok
  end

  # Expert review 2026-10-12 #10. A DeleteJob that exhausted its attempts (long storage outage) or
  # whose enqueue failed leaves a tombstoned tenant's object in storage with nothing to re-drive it.
  # Re-enqueue the (unique, idempotent) DeleteJob for recently tombstoned rows whose live object
  # still exists. Bounded: only rows deleted inside the window, at most @redrive_max per run, so the
  # per-run storage HEADs do not scale with cumulative lifetime deletions. Best-effort, like the
  # cool-off requeue above.
  @redrive_window_days 30
  @redrive_max 500

  defp redrive_deletes do
    since = DateTime.add(DateTime.utc_now(), -@redrive_window_days * 86_400, :second)

    # Newest deletions first, and the leftover test is a prefix List over every object kind, not a
    # `.db` HEAD (expert review 2026-10-12 #7): `purge_shard` deletes in lexicographic order and
    # halts on the first failure, and `<id>.db` sorts before `<id>@snap-*` / `<id>@v*`, so a
    # halted purge leaves full tenant copies behind with the `.db` already gone.
    leftovers =
      since
      |> Directory.recently_deleted_shard_ids(@redrive_max)
      |> Enum.filter(&match?({:ok, true}, Storage.any_shard_object?(&1)))

    for id <- leftovers do
      {:ok, _} = Oban.insert(DeleteJob.new(%{shard_id: id}))
    end

    if leftovers != [] do
      Logger.warning(
        "reconcile: re-enqueued DeleteJob for #{length(leftovers)} tombstoned shard(s) " <>
          "whose objects still exist: #{inspect(leftovers)}"
      )
    end
  rescue
    e -> Logger.warning("reconcile: delete re-drive failed (#{inspect(e)}); continuing")
  catch
    :exit, reason ->
      Logger.warning("reconcile: delete re-drive failed (#{inspect(reason)}); continuing")
  end

  # Expert review 2026-10-12 #8. The 14-day `-promoexp` expiry used to run only inside RetentionJob,
  # which is off by default and only samples active, scheduler-snapshotted rows. `PromoteOnOpen`
  # stamps `promotion_snapshot_at`; this always-on sweep expires the snapshots of any row (any
  # status) whose stamp has aged out, then clears the stamp. A failed list/drop leaves the stamp so
  # the next hourly run retries. Operator-labelled snapshots are never touched
  # (`Retention.expired_pre_promotion/3` matches only the reserved marker).
  @promo_sweep_max 100

  defp sweep_promotion_snapshots do
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, -Retention.pre_promotion_max_age_s(), :second)

    for %{shard_id: id, promotion_snapshot_at: stamped} <-
          Directory.promotion_snapshots_due(cutoff, @promo_sweep_max) do
      with {:ok, snaps} <- Snapshots.list(id),
           expired = Retention.expired_pre_promotion(Enum.map(snaps, & &1.id), now),
           true <- Enum.all?(expired, &(Snapshots.drop(id, &1) == :ok)) do
        Directory.clear_promotion_snapshot(id, stamped)
      else
        other ->
          Logger.warning(
            "reconcile: promotion snapshot expiry incomplete for #{id}: #{inspect(other)}"
          )
      end
    end

    :ok
  rescue
    e -> Logger.warning("reconcile: promotion snapshot sweep failed (#{inspect(e)}); continuing")
  catch
    :exit, reason ->
      Logger.warning(
        "reconcile: promotion snapshot sweep failed (#{inspect(reason)}); continuing"
      )
  end

  defp safe_retry_cooled do
    Migrator.retry_cooled_failed(failed_cool_off_ms())
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, reason}
  end

  @default_failed_cool_off_ms :timer.minutes(15)

  defp failed_cool_off_ms,
    do: Application.get_env(:fathom, :migration_failed_cool_off_ms, @default_failed_cool_off_ms)

  defp batch_size, do: Application.get_env(:fathom, :reconcile_batch_size, @default_batch_size)
end
