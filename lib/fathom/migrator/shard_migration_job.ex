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

  alias Fathom.Directory
  alias Fathom.Migrator.ShardMigration
  @snooze_seconds 5

  # A snooze does NOT burn an attempt — Oban raises `max_attempts` alongside `attempt` — so a job
  # that can never acquire its lease retries forever in `scheduled` with an EMPTY `errors` array,
  # `failed: 0`, no quarantine, and nothing logged above `[info]`. Observed at attempt 122/127 while
  # a tenant sat permanently unmigratable (the 2026-08-04 rig straggler): the shard that could not
  # make progress for twenty minutes was indistinguishable from one about to succeed on its next
  # tick, and the deploy gate simply never converged with no explanation anywhere.
  #
  # Retrying forever is the RIGHT behaviour and is deliberately kept: `{:retry, _}` means busy or
  # lease-held, both of which legitimately clear, and cancelling would leave the shard behind
  # permanently even after the cause went away. What was wrong is that it was silent and unpaced.
  # So: back the interval off, and escalate past a stall threshold — the same shape as the
  # coordinator's consecutive-flush-failure escalation (`Fathom.Shard`'s `flush_failures`).
  @max_snooze_seconds 60
  @default_stall_after_ms :timer.minutes(10)

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
        handle_retry(job, shard_id, target, reason)

      # A version this chain needs is unknown or YANKED (round-2 #23) — deterministic: it will
      # never exist again, so retrying burns attempts against nothing, and mark_failed would
      # QUARANTINE a shard that was never touched and is healthy at its old version (quarantine
      # also hides it from shards_at_version, so a later revert skips it too). Cancel without
      # marking; the shard stays a normal active citizen of whatever sweep applies next.
      #
      # `missing` is NOT necessarily the job's target. `head/0` is `max(version) WHERE NOT yanked`,
      # so yanking a MIDDLE version (say v4 of v1..v9) leaves HEAD at 9, and
      # `statement_chain(current, 9)` then needs v4 for every shard below it and answers
      # `{:error, {:unknown_version, 4}}`. The chain, not the target, is what is unbuildable.
      #
      # Expert review 2026-08-24 #15 claimed this clause was a PIN — that binding `target` here,
      # where `target` is already bound in the function head, matched only when the missing version
      # WAS the target, sending the intermediate case to `handle_error/3` and quarantining the
      # whole cold tail. That reading is wrong: a variable in an Elixir `case` pattern REBINDS,
      # pinning needs `^target`, and reverting this clause and running the intermediate scenario
      # returns `{:cancel, :unknown_version}` with the shard still `active` — verified, not
      # reasoned. What was genuinely wrong was the LOG, which named the missing version as the
      # "migration target" and sent an operator to look at the wrong one.
      {:error, {:unknown_version, missing}} ->
        Logger.warning(
          "shard #{shard_id}: cannot build the chain to v#{target} — v#{missing} is " <>
            "unknown/yanked; cancelling (shard untouched, not quarantined)"
        )

        # The panel's fair objection to a plain cancel — and the part of #15 that WAS right — is
        # that the shard is now permanently non-converging with NO durable record: it quietly stops
        # being migrated and nothing above [info] says so. A distinct directory status would be a
        # schema change; this makes the condition alertable now, and carries BOTH versions, because
        # when they differ it is the RELEASE GRAPH that is broken and not the shard.
        :telemetry.execute(
          [:fathom, :migrator, :unbuildable_chain],
          %{count: 1},
          %{shard_id: shard_id, target: target, missing: missing}
        )

        {:cancel, :unknown_version}

      {:error, reason} ->
        handle_error(job, shard_id, reason)
    end
  end

  # A deferral: the shard is busy or its lease is held. Keep retrying — both clear on their own —
  # but pace it and make a STALL visible.
  #
  # `inserted_at`, not the attempt count, is the operator-meaningful clock: "this shard has not been
  # able to migrate for 20 minutes" is actionable, "attempt 122" is not, and the attempt number also
  # moves for genuine errors. Past `:migration_stall_after_ms` the log goes to `[warning]` and
  # `[:fathom, :migrator, :migration_stalled]` fires every tick, so the condition is alertable and
  # `Migrator.status/0` can count it (see `stalled_shards/0`).
  defp handle_retry(
         %Oban.Job{attempt: attempt, inserted_at: inserted_at},
         shard_id,
         target,
         reason
       ) do
    stalled_for_ms = DateTime.diff(DateTime.utc_now(), inserted_at, :millisecond)
    snooze = snooze_seconds(attempt)

    if stalled_for_ms >= stall_after_ms() do
      Logger.warning(
        "shard #{shard_id}: migration to v#{target} STALLED — deferred for " <>
          "#{div(stalled_for_ms, 1000)}s over #{attempt} attempts (#{inspect(reason)}). The job " <>
          "keeps retrying (a snooze burns no attempt), so this will not surface as a failure: the " <>
          "shard serves normally and simply never converges. Check whether its lease is held by a " <>
          "coordinator that no longer exists (`Fathom.Shard.Storage.lease_holder/1`)."
      )

      :telemetry.execute(
        [:fathom, :migrator, :migration_stalled],
        %{stalled_for_ms: stalled_for_ms, attempt: attempt},
        %{shard_id: shard_id, target: target, reason: reason}
      )
    else
      Logger.info("shard #{shard_id}: migration deferred (#{inspect(reason)})")
    end

    {:snooze, snooze}
  end

  # Exponential, capped. A fleet-wide stall used to mean every stuck shard re-polling storage every
  # 5s forever; at the 300-tenant rig scale that is pure load against a condition that is not going
  # to clear on this tick. Capped rather than unbounded so a shard whose lease DOES free up is still
  # picked up promptly.
  defp snooze_seconds(attempt) when attempt <= 1, do: @snooze_seconds

  defp snooze_seconds(attempt) do
    min(@snooze_seconds * Integer.pow(2, min(attempt - 1, 8)), @max_snooze_seconds)
  end

  defp stall_after_ms,
    do: Application.get_env(:fathom, :migration_stall_after_ms, @default_stall_after_ms)

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
