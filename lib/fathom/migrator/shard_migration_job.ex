defmodule Fathom.Migrator.ShardMigrationJob do
  @moduledoc """
  Oban worker that migrates one shard to a target version — a thin wrapper over
  `Fathom.Migrator.ShardMigration.run/2`.

  Unique per `shard_id` (so the lazy path and the sweep never migrate the same
  shard twice at once). The retained old version's `Fathom.Migrator.RetirementJob`
  is enqueued by the migration's cutover ATOMICALLY (the outbox, expert review
  2026-07-18 #5 — see `ShardMigration.cutover_with_retirement/3`), not here, so a
  Postgres blip can't leave a cut-over shard with an unscheduled retirement. A
  `{:retry, ...}` (shard busy / lease held) snoozes; an `{:error, ...}` retries
  with Oban's backoff and, once attempts are exhausted, quarantines the shard
  (`Directory.mark_failed/1`) so the rest of the rollout keeps converging.
  """
  use Oban.Worker,
    queue: :migrations,
    max_attempts: 5,
    unique: [
      keys: [:shard_id],
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  require Logger

  alias Fathom.Directory
  alias Fathom.Migrator.ShardMigration
  @snooze_seconds 5

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id, "target" => target}, id: id} = job) do
    # Pass the Oban job id as the migration-lease operation token: it's stable across this job's
    # retries/snoozes (so a snoozed job reclaims its own lock) and distinct from any RevertJob's,
    # so a forward and a revert on one shard can't merge (finding #9).
    case ShardMigration.run(shard_id, target, id) do
      :ok ->
        :ok

      # The migration's cutover already enqueued the old version's retention deletion atomically (the
      # retirement outbox, expert review 2026-07-18 #5), so a Postgres blip can't leave a cut-over
      # shard without a scheduled retirement — and this job's crash-forward retry (bare `:ok` above)
      # doesn't need to re-enqueue.
      {:ok, %{from: from}} ->
        # Expert review 2026-08-01 #43: one event per shard that actually moved, for a per-node
        # rollout-throughput dashboard. Emitted HERE and not on the bare `:ok` above, which is the
        # crash-forward retry of a shard some earlier attempt already cut over — counting it would
        # inflate the rate by the retry rate. The fleet-wide rate `Migrator.status/0` reports is
        # derived from `cutover_at` in Postgres instead, because a counter is node-local and dies
        # with the node (see `Fathom.Directory.count_cutovers_since/2`).
        :telemetry.execute(
          [:fathom, :migrator, :shard_migrated],
          %{count: 1},
          %{shard_id: shard_id, from: from, to: target}
        )

        :ok

      {:retry, reason} ->
        Logger.info("shard #{shard_id}: migration deferred (#{inspect(reason)})")
        {:snooze, @snooze_seconds}

      # The target is unknown or YANKED (round-2 #23) — deterministic: the version
      # will never exist again, so retrying burns attempts against nothing, and
      # mark_failed would QUARANTINE a shard that was never touched and is healthy
      # at its old version (quarantine also hides it from shards_at_version, so a
      # later revert skips it too). Cancel without marking; the shard stays a
      # normal active citizen of whatever sweep applies next.
      {:error, {:unknown_version, target}} ->
        Logger.warning(
          "shard #{shard_id}: migration target v#{target} is unknown/yanked; " <>
            "cancelling (shard untouched, not quarantined)"
        )

        {:cancel, :unknown_version}

      {:error, reason} ->
        handle_error(job, shard_id, reason)
    end
  end

  defp handle_error(%Oban.Job{attempt: attempt, max_attempts: max}, shard_id, reason)
       when attempt >= max do
    Directory.mark_failed(shard_id)
    Logger.error("shard #{shard_id}: migration failed permanently (#{inspect(reason)})")
    {:cancel, reason}
  end

  defp handle_error(_job, shard_id, reason) do
    Logger.warning("shard #{shard_id}: migration error, will retry (#{inspect(reason)})")
    {:error, reason}
  end
end
