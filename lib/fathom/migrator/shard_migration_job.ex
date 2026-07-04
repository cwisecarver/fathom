defmodule Fathom.Migrator.ShardMigrationJob do
  @moduledoc """
  Oban worker that migrates one shard to a target version — a thin wrapper over
  `Fathom.Migrator.ShardMigration.run/2`.

  Unique per `shard_id` (so the lazy path and the sweep never migrate the same
  shard twice at once). On success it schedules a `Fathom.Migrator.RetirementJob`
  to drop the retained old version after the retention window. A `{:retry, ...}`
  (shard busy / lease held) snoozes; an `{:error, ...}` retries with Oban's backoff
  and, once attempts are exhausted, quarantines the shard
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
  alias Fathom.Migrator.{RetirementJob, ShardMigration}

  @retention_seconds 7 * 24 * 60 * 60
  @snooze_seconds 5

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id, "target" => target}, id: id} = job) do
    # Pass the Oban job id as the migration-lease operation token: it's stable across this job's
    # retries/snoozes (so a snoozed job reclaims its own lock) and distinct from any RevertJob's,
    # so a forward and a revert on one shard can't merge (finding #9).
    case ShardMigration.run(shard_id, target, id) do
      :ok ->
        :ok

      {:ok, %{from: from}} ->
        schedule_retirement(shard_id, from)
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

  defp schedule_retirement(shard_id, version) do
    %{shard_id: shard_id, version: version}
    |> RetirementJob.new(schedule_in: @retention_seconds)
    |> Oban.insert()
  end
end
