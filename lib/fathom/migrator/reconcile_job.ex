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

  # The rollout sweep's per-run cap (expert review 2026-07-18 #19). The default 100 keeps a single
  # hourly run bounded (migration-queue depth + S3 headroom), but at fleet scale a deep cold tail
  # (100k–millions) converges glacially — 100/hour is 2,400/day, i.e. months. Operators raise
  # `:reconcile_batch_size` to size convergence to their fleet + S3 pool; the per-shard uniqueness
  # dedup keeps a bigger bite idempotent, and enqueue_unique chunks under the Postgres bind cap.
  @default_batch_size 100

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
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

  defp batch_size, do: Application.get_env(:fathom, :reconcile_batch_size, @default_batch_size)
end
