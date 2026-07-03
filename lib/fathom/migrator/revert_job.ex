defmodule Fathom.Migrator.RevertJob do
  @moduledoc """
  Oban worker that reverts one shard to a prior version (restore its retained copy
  + cut the directory back), a thin wrapper over `Fathom.Migrator.ShardMigration.revert/2`.
  Unique per `shard_id`; snoozes if the shard is busy, retries on error.
  """
  use Oban.Worker,
    queue: :migrations,
    max_attempts: 5,
    unique: [
      keys: [:shard_id],
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  require Logger

  import Ecto.Query, only: [from: 2]

  alias Fathom.Migrator.{RetirementJob, ShardMigration}

  @retention_seconds 7 * 24 * 60 * 60

  @impl Oban.Worker
  def perform(
        %Oban.Job{
          args: %{"shard_id" => shard_id, "to_version" => to_version} = args,
          id: id
        } = job
      ) do
    # Job id as the lease token — distinct from any forward ShardMigrationJob's owner, so the two
    # can't merge via the same-owner reclaim path (finding #9).
    case ShardMigration.revert(shard_id, to_version, id, force: Map.get(args, "force", false)) do
      {:ok, %{from: backed_up}} ->
        # The revert backed the live vN object up as <shard>@<backed_up> (finding #13); retire it
        # after the retention window so it doesn't leak (RetirementJob only ever drops the
        # forward `from` version otherwise).
        schedule_retirement(shard_id, backed_up)
        # And cancel the forward migration's pending retirement of the version we just
        # restored (expert review #22, per the design doc's revert sequence): to_version
        # is LIVE again, and its retained copy near the retention deadline is what the
        # next revert restores from — leaving the drop scheduled made reverts near the
        # boundary race it and fail on a 404 restore.
        cancel_retirement(shard_id, to_version)
        :ok

      {:retry, reason} ->
        Logger.info("shard #{shard_id}: revert deferred (#{inspect(reason)})")
        {:snooze, 5}

      # The force-guard refused (finding #13): the shard was written since its cutover, so the
      # revert would discard tenant data. Deterministic — retrying can only see MORE writes —
      # so cancel rather than burn retries; the operator re-issues with force: true to confirm
      # the discard.
      {:error, {guard, details}} when guard in [:writes_since_cutover, :unknown_write_age] ->
        Logger.warning(
          "shard #{shard_id}: revert REFUSED (#{guard}) — post-cutover writes would be " <>
            "discarded; re-run with force: true to confirm (#{inspect(details)})"
        )

        {:cancel, guard}

      {:error, reason} ->
        handle_error(job, shard_id, reason)
    end
  end

  # Expert review #24: a RevertJob that exhausted its attempts was just discarded by
  # Oban — no directory mark, no quarantine analog of the forward path — so a partial
  # fleet revert silently stranded shards on the bad version while the operator
  # believed the revert landed. Mirror ShardMigrationJob.handle_error: the final
  # attempt quarantines the shard (durable fleet state, visible via
  # Migrator.revert_status/1 and Directory.count_failed/0).
  defp handle_error(%Oban.Job{attempt: attempt, max_attempts: max}, shard_id, reason)
       when attempt >= max do
    Fathom.Directory.mark_failed(shard_id)
    Logger.error("shard #{shard_id}: revert failed permanently (#{inspect(reason)})")
    {:cancel, reason}
  end

  defp handle_error(_job, shard_id, reason) do
    Logger.warning("shard #{shard_id}: revert error, will retry (#{inspect(reason)})")
    {:error, reason}
  end

  defp cancel_retirement(shard_id, version) do
    Oban.cancel_all_jobs(
      from(j in Oban.Job,
        where: j.worker == "Fathom.Migrator.RetirementJob",
        where: j.state in ["scheduled", "available", "retryable"],
        where: fragment("?->>'shard_id' = ?", j.args, ^shard_id),
        where: fragment("(?->>'version')::bigint = ?", j.args, ^version)
      )
    )
  end

  defp schedule_retirement(shard_id, version) do
    %{shard_id: shard_id, version: version}
    |> RetirementJob.new(schedule_in: @retention_seconds)
    |> Oban.insert()
  end
end
