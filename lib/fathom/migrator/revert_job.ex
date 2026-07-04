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
    # Cancel the forward migration's pending retirement of to_version BEFORE touching
    # storage (expert review #22, tightened by round-2 #17): to_version's retained copy
    # is this revert's RESTORE SOURCE, and the #22 cancel lived in the :ok branch —
    # after the whole race window — so a retirement dequeuing just before/mid-revert
    # still deleted it (its skip-when-live guard reads the directory, which says
    # from_version until our cutover). Cancelling up front is safe even if the revert
    # then fails or is refused: a retained copy that outlives its drop merely leaks
    # until the S3 lifecycle backstop; a deleted restore source quarantines the shard
    # unrecoverably. An already-EXECUTING retirement is uncancellable — that side is
    # closed by RetirementJob's revert-in-flight guard.
    cancel_retirement(shard_id, to_version)

    # Job id as the lease token — distinct from any forward ShardMigrationJob's owner, so the two
    # can't merge via the same-owner reclaim path (finding #9).
    case ShardMigration.revert(shard_id, to_version, id, force: Map.get(args, "force", false)) do
      # Already fully reverted (a crashed-after-cutover retry, round-2 #30) — nothing
      # was touched, so there is no fresh backup to schedule retirement for.
      :ok ->
        :ok

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
      # the discard. EXCEPT (round-2 #21a): a force sweep may have rewritten this job's ROW
      # args while we were executing — the jsonb update can't reach the copy this execution
      # deserialized — and going terminal would silently drop the operator's explicit force.
      # Re-check the row first; changed args snooze so the next execution runs with them.
      {:error, {guard, details}} when guard in [:writes_since_cutover, :unknown_write_age] ->
        if row_args_changed?(id, args) do
          Logger.info("shard #{shard_id}: revert args upgraded mid-execution; re-running")
          {:snooze, 1}
        else
          Logger.warning(
            "shard #{shard_id}: revert REFUSED (#{guard}) — post-cutover writes would be " <>
              "discarded; re-run with force: true to confirm (#{inspect(details)})"
          )

          {:cancel, guard}
        end

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

  # Whether the job's persisted row args differ from the args this execution ran
  # with (a mid-execution force retarget — round-2 #21a). A nil/absent id (e.g.
  # Oban.Testing's unsaved job struct) reads as unchanged.
  defp row_args_changed?(id, args) when is_integer(id) do
    case Fathom.Repo.get(Oban.Job, id) do
      %Oban.Job{args: current} -> current != args
      _ -> false
    end
  end

  defp row_args_changed?(_id, _args), do: false

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
