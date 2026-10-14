defmodule Fathom.Migrator.ReconcileJob do
  @moduledoc """
  Oban cron job that re-runs the rollout sweep, so even never-touched shards
  converge to HEAD and any drift self-heals. Idempotent — per-shard uniqueness
  means re-enqueuing already-queued shards is a no-op — so it can run forever on a
  schedule (see the Oban `:crontab` config).
  """
  use Oban.Worker,
    queue: :migrations,
    max_attempts: 1,
    # No stacking (expert review 2026-10-13 #8): without this each hourly cron added another copy
    # behind a stuck one on the 10-slot :migrations queue. `period: :infinity` because a keyword
    # list inherits `period: 60`; no `:completed`, so only an in-flight run blocks the next.
    unique: [
      fields: [:worker, :queue],
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable]
    ]

  require Logger

  import Ecto.Query, only: [from: 2]

  alias Fathom.{Directory, Migrator, Repo}

  # The rollout sweep's per-run cap (expert review 2026-07-18 #19). The default 100 keeps a single
  # hourly run bounded (migration-queue depth + S3 headroom), but at fleet scale a deep cold tail
  # (100k–millions) converges glacially — 100/hour is 2,400/day, i.e. months. Operators raise
  # `:reconcile_batch_size` to size convergence to their fleet + S3 pool; the per-shard uniqueness
  # dedup keeps a bigger bite idempotent, and enqueue_unique chunks under the Postgres bind cap.
  @default_batch_size 100

  # Bound the run under Lifeline's `rescue_after` (config.exs, 60 min) so a wedged job ends instead
  # of being rescued while its process is still alive (expert review 2026-10-13 #6). Oban's default
  # is :infinity. Pinned by test/fathom/oban_worker_timeout_test.exs.
  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(50)

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    # The delete re-drive and promotion-snapshot expiry live in `ErasureSweepJob` (own queue, own
    # cron): they used to run first here, serial and unbounded, so a slow object store froze this
    # rollout (expert review 2026-10-13 #8). A raise below still cannot skip them.
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

    {:ok, _count} = rollout_to_watermark()

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

  # Perf review 2026-10-14 #14: ONE `rollout(100)` per hourly run drained a cold tail at a flat 100
  # shards/hour (1M laggards = 417 days). Now the run keeps enqueuing `batch_size`-sized chunks
  # while the live ShardMigrationJob backlog (the queue itself, so the watermark also counts the
  # earlier runs' unfinished work) is below `:reconcile_watermark`, and stops at the time budget.
  # Laggards already holding a live job are excluded by `Directory.laggards/2`, so each chunk is NEW
  # shards, never the same 100 re-offered. The jobs a run adds are bounded by watermark + one chunk,
  # so the :migrations queue and S3 headroom (the reason for the old 100 cap) stay bounded.
  # `:reconcile_watermark <= :reconcile_batch_size` reproduces the old one-chunk behaviour.
  defp rollout_to_watermark do
    deadline = System.monotonic_time(:millisecond) + budget_ms()
    chunk = batch_size()
    {:ok, rollout_loop(chunk, watermark(), deadline, 0)}
  end

  defp rollout_loop(chunk, watermark, deadline, total) do
    # Time never blocks the first chunk (a zero budget must not starve the rollout); the watermark
    # gates every chunk, so a backlog already at the mark adds nothing.
    if (total > 0 and System.monotonic_time(:millisecond) >= deadline) or
         live_migration_jobs() >= watermark do
      total
    else
      {:ok, n} = Migrator.rollout(chunk)
      if n < chunk, do: total + n, else: rollout_loop(chunk, watermark, deadline, total + n)
    end
  end

  # The same live states `Directory.laggards/2` excludes a shard for.
  defp live_migration_jobs do
    Repo.aggregate(
      from(j in "oban_jobs",
        where:
          j.worker == "Fathom.Migrator.ShardMigrationJob" and
            j.state in ["scheduled", "available", "executing", "retryable", "suspended"]
      ),
      :count
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

  @default_watermark 1_000
  @default_rollout_budget_ms :timer.minutes(10)

  defp watermark, do: Application.get_env(:fathom, :reconcile_watermark, @default_watermark)

  defp budget_ms,
    do: Application.get_env(:fathom, :reconcile_rollout_budget_ms, @default_rollout_budget_ms)

  defp batch_size, do: Application.get_env(:fathom, :reconcile_batch_size, @default_batch_size)
end
