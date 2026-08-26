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
  alias Fathom.Migrator.ShardMigrationJob

  @impl Oban.Worker
  def perform(
        %Oban.Job{
          args: %{"shard_id" => shard_id, "to_version" => to_version} = args,
          id: id
        } = job
      ) do
    # THE TARGET THIS SHARD CAN ACTUALLY REACH, which is not always the one the fleet asked for
    # (expert review 2026-08-24 #16b). Resolved FIRST, because everything below depends on it: the
    # retirement cancel protects the restore source, and the restore source is `<shard>@<landing>`.
    landing = landing_version(shard_id, to_version)

    # Cancel the forward migration's pending retirement of the landing version BEFORE touching
    # storage (expert review #22, tightened by round-2 #17): that version's retained copy
    # is this revert's RESTORE SOURCE, and the #22 cancel lived in the :ok branch —
    # after the whole race window — so a retirement dequeuing just before/mid-revert
    # still deleted it (its skip-when-live guard reads the directory, which says
    # from_version until our cutover). Cancelling up front is safe even if the revert
    # then fails or is refused: a retained copy that outlives its drop merely leaks
    # until the S3 lifecycle backstop; a deleted restore source quarantines the shard
    # unrecoverably. An already-EXECUTING retirement is uncancellable — that side is
    # closed by RetirementJob's revert-in-flight guard.
    cancel_retirement(shard_id, landing)

    # Job id as the lease token — distinct from any forward ShardMigrationJob's owner, so the two
    # can't merge via the same-owner reclaim path (finding #9).
    case ShardMigration.revert(shard_id, landing, id, force: Map.get(args, "force", false)) do
      # Already fully reverted (a crashed-after-cutover retry, round-2 #30) — nothing
      # was touched, so there is no fresh backup to schedule retirement for.
      :ok ->
        climb_back(shard_id, landing, to_version)

      # The revert backed the live vN object up as <shard>@<from> (finding #13) and its cutover
      # enqueued that object's retention deletion atomically (the outbox, expert review 2026-07-18
      # #5), so a Postgres blip can't leak it and this path needn't re-enqueue.
      {:ok, %{from: _backed_up}} ->
        climb_back(shard_id, landing, to_version)

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
            "#{shard_id}@#{landing}, and no retry can create one. The chain-jump case is now " <>
            "handled before this point: shards.retained_version says " <>
            "#{inspect(retained_version(shard_id))}, and a value BELOW the target would have been " <>
            "restored and then migrated forward. Reaching here means the column is NULL (a " <>
            "pre-column row, or RetirementJob dropped the copy past its retention window) or it " <>
            "named a version storage does not actually have. This shard is still on its " <>
            "pre-revert schema. It is now quarantined, which means it also drops out of " <>
            "revert_status/1 — so `remaining: 0` there does NOT mean the fleet revert landed; " <>
            "check the failed count."
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

  # WHICH VERSION THIS SHARD CAN ACTUALLY BE RESTORED TO (expert review 2026-08-24 #16b).
  #
  # A forward migration retains exactly ONE object, `<shard>@<current>`. A cold-tail shard walks
  # `current+1 … target` in a SINGLE job, so a shard that sat at v5 while the fleet reached v9
  # retains only `<shard>@5`. A fleet revert names one fleet-wide target — `Migrator.revert(9, 8)`
  # — and `<shard>@8` was never created for that shard. Before this, every such shard burned the
  # absent-object branch below and stayed on the bad schema; a fleet revert read as landed while
  # only the one-version-behind shards had moved.
  #
  # `shards.retained_version` records what the retain actually wrote, in the cutover's own
  # transaction. Reading it is one indexed column, not a storage listing — there is no
  # `list_versions` callback and adding one would mean implementing it on Local, S3 and the test
  # double to answer a question Postgres already holds more truthfully.
  #
  # DEVIATES ONLY DOWNWARD, and only on a definite integer. Equal (the ordinary case, the shard came
  # from the target) and NULL (a pre-column row, or a copy `RetirementJob` has dropped) both return
  # the requested version, so the common path is bit-for-bit what it was and an unknown column
  # still lands in the absent-object branch with its diagnosis. A retained version ABOVE the target
  # is not a landing site either: restoring it would move the shard to a version newer than the
  # fleet asked for, which is not a revert.
  defp landing_version(shard_id, to_version) do
    case retained_version(shard_id) do
      r when is_integer(r) and r < to_version -> r
      _ -> to_version
    end
  end

  defp retained_version(shard_id) do
    case Fathom.Directory.get(shard_id) do
      {:ok, %{retained_version: r}} -> r
      :error -> nil
    end
  end

  # THE SECOND HALF OF A CHAIN-JUMPER'S REVERT, and the reason this lands on the requested version
  # rather than "as far back as it can go" (expert review 2026-08-24 #16b).
  #
  # Stopping at v5 would be the cheaper implementation and it is WRONG. `docs/migration.md` commits
  # the fleet to exactly vN-1/vN tolerance — "the app reads both vN-1 and vN" — so parking a shard
  # three versions below what the fleet's app expects turns "on the bad schema" into "on a schema
  # nothing running can read". Reverting to the retained copy and then migrating FORWARD to the
  # requested version keeps the fleet homogeneous, which is the whole point of a fleet revert.
  #
  # The forward leg needs no new machinery: `Migrator.revert/3` yanks the bad version first, so
  # `head()` is already the requested target and this is the ordinary migration path a laggard
  # takes. `ShardMigrationJob` is unique per shard, so a reconcile sweep that enqueues the same
  # climb concurrently is deduped rather than doubled.
  defp climb_back(_shard_id, landing, to_version) when landing >= to_version, do: :ok

  defp climb_back(shard_id, landing, to_version) do
    # WARNING, not info, and the level is the point: this shard is now BELOW the fleet version, and
    # `docs/migration.md` only commits the app to vN-1/vN. That is a real (if brief) window in which
    # a request for this tenant can hit a schema the running app does not expect — the same class of
    # operator-visible consequence as the "writes DISCARDED" warning the revert itself emits, and it
    # would be invisible at :info, which `config/test.exs` and most deploys filter out.
    Logger.warning(
      "shard #{shard_id}: reverted to v#{landing} (its only retained copy — it chain-migrated " <>
        "past v#{to_version}); now migrating forward v#{landing} -> v#{to_version} to rejoin the " <>
        "fleet. It is briefly below the fleet version, which the app's vN-1/vN tolerance does " <>
        "not cover: watch for it in the laggard count until the forward job lands."
    )

    :telemetry.execute(
      [:fathom, :migrator, :revert_climb_back],
      %{count: 1},
      %{shard_id: shard_id, landed_at: landing, target: to_version}
    )

    case Oban.insert(ShardMigrationJob.new(%{shard_id: shard_id, target: to_version})) do
      {:ok, _job} ->
        :ok

      # The revert itself LANDED — the shard is on `landing` and storage is consistent. Only the
      # climb failed to enqueue, and returning an error here would re-run a revert that has already
      # cut over. Report it and let the reconcile sweep pick the shard up as an ordinary laggard,
      # which is exactly what it now is.
      {:error, reason} ->
        Logger.error(
          "shard #{shard_id}: reverted to v#{landing} but could not enqueue the forward climb to " <>
            "v#{to_version} (#{inspect(reason)}). The shard is BELOW the fleet version; the " <>
            "laggard sweep should collect it, but check that it does."
        )

        :ok
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
