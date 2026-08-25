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
      # `period: :infinity` is NOT the default and its absence was a real hole (expert review
      # 2026-08-24 #23). A keyword list here MERGES into Oban's @unique_defaults, which sets
      # `period: 60` — only the bare `unique: true` gets `:infinity`. So dedup applied only
      # against jobs inserted in the last SIXTY SECONDS, while every long-lived state this worker
      # reaches is far longer: the snooze backoff caps at 60 s with jobs documented reaching
      # attempt 122, `:migration_stall_after_ms` is 10 minutes, and Lifeline's `rescue_after` is
      # asserted to be at least 60 MINUTES (see oban_config_test.exs) — and a row stranded in
      # `:executing` is by definition older than that when Lifeline finds it.
      #
      # Safe BECAUSE `:completed` is deliberately absent from the states below: an infinite period
      # blocks re-enqueue only while a job for that shard is genuinely pending, never forever after
      # one finishes. The panel's caution was `:suspended` — a permanently suspended job does block
      # indefinitely, which is the intended reading (a suspended job for that shard still exists).
      period: :infinity,
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  require Logger

  import Ecto.Query, only: [from: 2]

  alias Fathom.Migrator.ShardMigration

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

      # The revert backed the live vN object up as <shard>@<from> (finding #13) and its cutover
      # enqueued that object's retention deletion atomically (the outbox, expert review 2026-07-18
      # #5), so a Postgres blip can't leak it and this path needn't re-enqueue.
      {:ok, %{from: _backed_up}} ->
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

      # THE RETAINED VERSION WAS NEVER CREATED (expert review 2026-08-24 #16). Deterministic in
      # exactly the way the force-guard above is: no number of retries will conjure the object.
      #
      # A forward migration retains exactly ONE object, `<shard>@prev`, where `prev` is the shard's
      # version before it ran. A cold-tail shard walks `current+1 … target` in a SINGLE job, so a
      # shard that was at v5 and migrated to HEAD=9 has only `<shard>@5`. A fleet revert, though,
      # picks one fleet-wide `to_version` — `Migrator.revert(9, 8)`, or `head()` from
      # `revert_stranded/0` — and asks every shard for `<shard>@8`, which the chain-jumpers never
      # created. Both backends answer deterministically (`{:error, :enoent}` from Local,
      # `{:error, :version_absent}` from S3) and neither had a clause here, so they fell to
      # `handle_error/3`, burned 5 attempts, and quarantined a shard whose only problem is that the
      # operator asked for a version it never passed through.
      #
      # That made a fleet revert read as LANDED while leaving those shards on the bad schema:
      # quarantined rows drop out of `revert_status/1`'s `status == "active"` filter, so `remaining`
      # goes to 0 and the loss shows only in the separate `failed` count. `revert_stranded/0` then
      # re-enqueued the same doomed revert on every reconcile tick.
      #
      # THE QUARANTINE IS KEPT, and that is a correction to the finding. It recommended cancelling
      # WITHOUT marking, on the grounds that the shard is healthy and quarantine hides it from a
      # later revert. True for the chain-jump case — but "no object at `<shard>@N`" has a second
      # cause storage cannot distinguish from it: the backup EXISTED and was legitimately retired
      # past its retention window. Expert review #24 added the quarantine for exactly that case,
      # `shard_migration_job_test.exs`'s "an exhausted revert quarantines the shard" pins it, and
      # dropping it would re-open the silent partial-revert that #24 closed.
      #
      # So this branch changes what was genuinely wrong — five doomed retries and a generic
      # "revert failed permanently" that named neither the cause nor the likely reason — and keeps
      # the durable record. Whether the chain-jump case should be distinguished at all is the
      # finding's real fix (per-shard revert targeting) and is parked; see the run's progress file.
      {:error, absent} when absent in [:enoent, :version_absent] ->
        Fathom.Directory.mark_failed(shard_id)

        Logger.error(
          "shard #{shard_id}: revert to v#{to_version} FAILED — no retained copy at " <>
            "#{shard_id}@#{to_version}, and no retry can create one. Either it was retired past " <>
            "its retention window, or this shard never passed through v#{to_version} at all: a " <>
            "cold-tail shard chain-migrates several versions in ONE job and retains only the " <>
            "version it came FROM, so a fleet-wide revert target does not exist for it. This " <>
            "shard is still on its pre-revert schema. It is now quarantined, which means it also " <>
            "drops out of revert_status/1 — so `remaining: 0` there does NOT mean the fleet " <>
            "revert landed; check the failed count."
        )

        :telemetry.execute(
          [:fathom, :migrator, :revert_no_retained_version],
          %{count: 1},
          %{shard_id: shard_id, to_version: to_version}
        )

        {:cancel, :no_retained_version}

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
end
