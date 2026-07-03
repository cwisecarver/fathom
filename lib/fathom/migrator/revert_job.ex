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

  alias Fathom.Migrator.{RetirementJob, ShardMigration}

  @retention_seconds 7 * 24 * 60 * 60

  @impl Oban.Worker
  def perform(%Oban.Job{
        args: %{"shard_id" => shard_id, "to_version" => to_version} = args,
        id: id
      }) do
    # Job id as the lease token — distinct from any forward ShardMigrationJob's owner, so the two
    # can't merge via the same-owner reclaim path (finding #9).
    case ShardMigration.revert(shard_id, to_version, id, force: Map.get(args, "force", false)) do
      {:ok, %{from: backed_up}} ->
        # The revert backed the live vN object up as <shard>@<backed_up> (finding #13); retire it
        # after the retention window so it doesn't leak (RetirementJob only ever drops the
        # forward `from` version otherwise).
        schedule_retirement(shard_id, backed_up)
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
        Logger.warning("shard #{shard_id}: revert error, will retry (#{inspect(reason)})")
        {:error, reason}
    end
  end

  defp schedule_retirement(shard_id, version) do
    %{shard_id: shard_id, version: version}
    |> RetirementJob.new(schedule_in: @retention_seconds)
    |> Oban.insert()
  end
end
