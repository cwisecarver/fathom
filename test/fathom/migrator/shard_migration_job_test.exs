defmodule Fathom.Migrator.ShardMigrationJobTest do
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  import ExUnit.CaptureLog

  alias Fathom.Migrator
  alias Fathom.Migrator.{RetirementJob, RevertJob, ShardMigrationJob}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Directory

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002', 'now')"
  ]

  setup do
    shard = "job_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for path <- Path.wildcard(Path.join(remote_dir(), "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{shard}*"])),
          do: File.rm(path)

      File.rm(Path.join([remote_dir(), "heartbeats", URI.encode_www_form("thief@node")]))
    end)

    %{shard: shard}
  end

  defp seed_v1!(shard) do
    seed =
      Path.join(System.tmp_dir!(), "seedjob_#{shard}_#{System.unique_integer([:positive])}.db")

    {:ok, conn} = Connection.open(seed)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)")
    :ok = Connection.exec(conn, "INSERT INTO app_thing (id, name) VALUES (1, 'alice')")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"
      )

    :ok =
      Connection.exec(
        conn,
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001', 'now')"
      )

    :ok = Connection.exec(conn, "PRAGMA user_version = 1")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)

    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, 1)
    :ok
  end

  defp put_foreign_lock(shard) do
    File.mkdir_p!(remote_dir())
    exp = System.system_time(:millisecond) + 60_000

    File.write!(
      Path.join(remote_dir(), "#{shard}.lock"),
      Jason.encode!(%{"owner" => "thief@node", "epoch" => 1, "expires_at_ms" => exp})
    )

    # Liveness is the per-node heartbeat now: a held lock needs a live owner, else
    # the migrator would just steal it instead of snoozing.
    hb_dir = Path.join(remote_dir(), "heartbeats")
    File.mkdir_p!(hb_dir)

    File.write!(
      Path.join(hb_dir, URI.encode_www_form("thief@node")),
      Jason.encode!(%{"owner" => "thief@node", "expires_at_ms" => exp})
    )
  end

  # A lock held under the OLD shared migrator owner (no per-operation token), fresh TTL, NO
  # heartbeat — so the #11 lock-TTL fallback keeps it live. Post-fix a new operation's owner
  # `migrator@<node>@<job.id>` is foreign to this, so it can't reclaim it (finding #9).
  defp put_migrator_lock(shard) do
    File.mkdir_p!(remote_dir())

    File.write!(
      Path.join(remote_dir(), "#{shard}.lock"),
      Jason.encode!(%{
        "owner" => "migrator@#{node()}",
        "epoch" => 1,
        "expires_at_ms" => System.system_time(:millisecond) + 60_000
      })
    )
  end

  test "migrates the shard, cuts over, and schedules retirement of the old version",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)
    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 1})
  end

  # Expert review 2026-08-01 #43. The event is the per-node rollout-throughput signal, so it must
  # fire exactly once per shard that ACTUALLY moved. The second perform_job here is the
  # crash-forward retry (ShardMigration.run returns bare :ok — the directory is already at target);
  # counting it would inflate a node's reported rate by its retry rate, which is precisely wrong
  # since retries spike when the rollout is struggling.
  test "emits shard_migrated once per real migration, not on the crash-forward retry",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    handler = "shard-migrated-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:fathom, :migrator, :shard_migrated],
      fn _e, measurements, meta, _ -> send(parent, {:migrated, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
    assert_receive {:migrated, %{count: 1}, %{shard_id: ^shard, from: 1, to: 2}}

    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
    refute_receive {:migrated, _, _}
  end

  test "RetirementJob drops the retained version", %{shard: shard} do
    seed_v1!(shard)
    :ok = Storage.retain(shard, 1)
    # The retained version is genuinely OLD (live has moved past it) — the normal case.
    {:ok, _} = Directory.cutover(shard, 2)
    assert File.exists?(Path.join(remote_dir(), "#{shard}@1.db"))

    assert :ok = perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})
    refute File.exists?(Path.join(remote_dir(), "#{shard}@1.db"))
  end

  # Expert review #22: the drop must skip a version the directory shows LIVE — a revert
  # restored it after this retirement was scheduled, and its retained copy is the
  # recovery point the next revert restores from, not garbage.
  test "RetirementJob skips a version that is live again", %{shard: shard} do
    seed_v1!(shard)
    :ok = Storage.retain(shard, 1)

    assert {:cancel, :version_live} =
             perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})

    assert File.exists?(Path.join(remote_dir(), "#{shard}@1.db")),
           "the live version's retained copy must not be dropped"
  end

  # Expert review round-2 #17: the #22 skip-when-live guard reads the directory, which
  # still says from_version until the revert's cutover — so a RetirementJob dequeuing
  # just before/during a revert passed the guard and deleted the retained copy that is
  # the revert's RESTORE SOURCE (restore then 404s every retry; the shard quarantines
  # with its recovery point destroyed). RevertJob's cancel can't reach an
  # already-executing retirement, so the retirement itself must check for an in-flight
  # revert referencing its version as to_version.
  test "RetirementJob skips a version whose revert is in flight", %{shard: shard} do
    seed_v1!(shard)
    :ok = Storage.retain(shard, 1)
    # The shard is past v1 (the normal retirement case) ...
    {:ok, _} = Directory.cutover(shard, 2)
    # ... but a revert BACK to v1 is in flight (inserted, not yet cut over — the
    # directory still shows v2, so the #22 live-guard alone passes).
    {:ok, _} = Oban.insert(RevertJob.new(%{shard_id: shard, to_version: 1}))

    assert {:cancel, :revert_in_flight} =
             perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})

    assert File.exists?(Path.join(remote_dir(), "#{shard}@1.db")),
           "an in-flight revert's restore source must not be dropped"
  end

  # The other half of #17: RevertJob cancels the pending retirement at the TOP of
  # perform (before restore), not in the :ok branch — so the restore source is
  # protected even when the revert itself then fails and retries.
  test "RevertJob cancels the pending retirement before restoring, even on failure",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Directory.cutover(shard, 2)
    # A retirement of v1 is pending (scheduled by the forward migration).
    {:ok, _} = Oban.insert(RetirementJob.new(%{shard_id: shard, version: 1}, schedule_in: 60))
    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 1})

    # The live object is gone, so the revert's retain-backup step FAILS after the
    # cancel — pre-fix the cancel lived in the :ok branch and never ran on this path.
    File.rm!(Path.join(remote_dir(), "#{shard}.db"))

    capture_log(fn ->
      assert {:error, _} =
               perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1})
    end)

    refute_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 1})
  end

  # Round-2 #30: a RevertJob dying between the cutover and the Oban ack retries with
  # the revert ALREADY complete — and the re-run took the destructive path again:
  # retain(current == to_version) copied live over the retained @to_version backup,
  # destroying the recovery copy the NEXT revert restores from. The invariant: a
  # completed revert's retry is a no-op that touches no storage.
  test "a revert retry after a completed cutover is a no-op, not a destructive re-run",
       %{shard: shard} do
    # The completed revert's end state: directory AND live file both at v1 ...
    seed_v1!(shard)
    # ... and a retained @1 backup whose bytes must survive the retry.
    backup = Path.join(remote_dir(), "#{shard}@1.db")
    File.write!(backup, "the-retained-backup-bytes")
    on_exit(fn -> File.rm(backup) end)

    assert :ok = perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1})

    assert File.read!(backup) == "the-retained-backup-bytes",
           "a completed-revert retry must not clobber the retained backup"
  end

  test "a held lease snoozes the job", %{shard: shard} do
    # Jobs only ever target directory-known shards (they're enqueued from directory
    # queries) — register it, since run/3 no longer implicitly mints rows (#40).
    {:ok, _} = Directory.resolve(shard)
    put_foreign_lock(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    assert {:snooze, _} = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
  end

  describe "a deferral that never clears" do
    # An Oban snooze raises `max_attempts` alongside `attempt`, so a job that can never acquire its
    # lease retries FOREVER: state `scheduled`, EMPTY `errors`, `failed: 0`, no quarantine, and
    # nothing logged above `[info]`. On the 2026-08-04 rig one sat at attempt 122/127 while its
    # tenant was permanently unmigratable and the deploy gate never converged, with no explanation
    # anywhere. Retrying is correct — busy and lease-held both clear on their own — so the fix is
    # visibility and pacing, not cancellation.
    setup %{shard: shard} do
      {:ok, _} = Directory.resolve(shard)
      put_foreign_lock(shard)
      {:ok, _} = Migrator.release(2, "v2", @v2_statements)
      :ok
    end

    # NOT a regression test — pre-fix nothing ever logged STALLED, so this passed anyway. It is the
    # false-positive guard for its sibling below: an escalation that fires on every ordinary
    # deferral is worse than no escalation, because it trains the warning into background noise.
    test "below the stall threshold it stays quiet at [info]", %{shard: shard} do
      log =
        capture_log(fn ->
          assert {:snooze, _} =
                   perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2},
                     attempt: 1,
                     inserted_at: DateTime.utc_now()
                   )
        end)

      refute log =~ "STALLED", "an ordinary short deferral must not cry wolf"
    end

    test "past the threshold it escalates to [warning] and emits telemetry", %{shard: shard} do
      test_pid = self()
      handler = "stalled-#{shard}"

      :telemetry.attach(
        handler,
        [:fathom, :migrator, :migration_stalled],
        fn _e, meas, meta, _ -> send(test_pid, {:stalled, meas, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      log =
        capture_log(fn ->
          assert {:snooze, _} =
                   perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2},
                     attempt: 40,
                     inserted_at:
                       DateTime.add(DateTime.utc_now(), -:timer.minutes(30), :millisecond)
                   )
        end)

      assert log =~ "STALLED",
             "a shard deferring for 30 minutes must be alertable, not [info]"

      assert_receive {:stalled, %{attempt: 40}, %{shard_id: ^shard, target: 2}}, 2_000
    end

    # Before this, a fleet-wide stall meant every stuck shard re-polling storage every 5s forever.
    # The cap keeps a shard whose lease DOES free up from waiting minutes to notice.
    test "the snooze backs off with attempts and stays capped", %{shard: shard} do
      snooze = fn attempt ->
        result =
          with_log(fn ->
            perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2},
              attempt: attempt,
              inserted_at: DateTime.utc_now()
            )
          end)

        {{:snooze, seconds}, _log} = result
        seconds
      end

      first = snooze.(1)
      later = snooze.(4)
      far = snooze.(50)

      assert first == 5, "the first deferral should retry promptly"
      assert later > first, "repeated deferrals must back off"
      assert far <= 60, "and stay capped so a freed lease is still picked up quickly"
    end
  end

  # Finding #9: forward and revert jobs must not merge via the same-owner lease reclaim. Each
  # operation now owns `migrator@<node>@<job.id>`, so a lock held under the OLD shared owner
  # `migrator@<node>` is foreign to a new job — it snoozes (serialize-and-retry) instead of
  # reclaiming and running a second copy concurrently. (No heartbeat: the #11 lock-TTL fallback
  # keeps the fresh lock live.)
  test "a new migration does not merge with a bare migrator@node lock", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    put_migrator_lock(shard)

    assert {:snooze, _} = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
  end

  # Expert review #24: a RevertJob that exhausted its attempts was silently discarded —
  # no directory mark, no telemetry, no quarantine analog of the forward path — so a
  # partial fleet revert stranded shards on the bad version with the operator believing
  # the revert landed. The invariant: revert failure is durable fleet state, and
  # Migrator.revert_status/1 answers "did the fleet revert complete?".
  test "an exhausted revert quarantines the shard and shows up in revert_status", %{
    shard: shard
  } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})

    # Retire the retained v1 so the revert's restore 404s — a permanent storage error.
    assert :ok = perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})

    capture_log(fn ->
      assert {:cancel, _} =
               perform_job(
                 RevertJob,
                 %{"shard_id" => shard, "to_version" => 1, "force" => true},
                 attempt: 5
               )
    end)

    assert {:ok, %{status: "migration_failed"}} = Directory.get(shard)

    status = Migrator.revert_status(2)
    assert status.failed >= 1, "the quarantined shard must be visible in revert_status"
  end

  test "exhausted attempts quarantine the shard", %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)
    # A RELEASED target but no live storage object -> a persistent {:error, _}
    # through every retry. (An unknown/yanked target no longer quarantines — that
    # cancels without marking, round-2 #23 — so it can't be the vehicle here.)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    capture_log(fn ->
      assert {:cancel, _} =
               perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2}, attempt: 5)
    end)

    assert {:ok, %{status: "migration_failed"}} = Directory.get(shard)
  end

  test "enqueue_migration enqueues a unique-per-shard job", %{shard: shard} do
    assert {:ok, _} = Migrator.enqueue_migration(shard, 2)
    assert_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => shard, "target" => 2})
  end

  # Finding #13: RevertJob must back up the live vN object and schedule its retirement, or the
  # <shard>@vN backup leaks (RetirementJob otherwise only drops the forward `from` version).
  test "revert backs up the live version and schedules its retirement", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})

    assert :ok = perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1})

    assert File.exists?(Path.join(remote_dir(), "#{shard}@2.db")),
           "the live v2 object is backed up"

    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 2})
  end

  # Expert review #22: the design doc's revert sequence ends with "cancel the pending
  # RetirementJob", but the cancellation was never implemented — after a revert, the
  # forward migration's scheduled drop of the restored version stayed live, and a revert
  # issued near the retention deadline raced it (restore 404s, the RevertJob burns its
  # attempts). The invariant: a successful revert cancels the restored version's pending
  # retirement, while the new backup's own retirement stays scheduled.
  test "a revert cancels the pending retirement of the restored version", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 1})

    assert :ok = perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1})

    refute_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 1})

    # The revert's own backup (v2) still gets retired after the window.
    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 2})
  end

  # Finding #13 (force-guard at the job level): a guard refusal is deterministic — retrying can
  # only observe MORE post-cutover writes — so the job must CANCEL, not burn its 5 attempts, and
  # a force: true re-issue is the operator's confirmation path.
  test "revert job cancels on the write-age guard and proceeds with force", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})

    # Activity after cutover — the revert would discard it.
    {:ok, _} = Directory.resolve(shard)

    log =
      capture_log(fn ->
        assert {:cancel, :writes_since_cutover} =
                 perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1})
      end)

    assert log =~ "revert REFUSED"
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)

    assert :ok =
             perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1, "force" => true})

    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
  end

  # Round-2 #23: a job surviving a yank (or dequeuing after it) hit
  # statements/1 == nil → {:error, {:unknown_version, target}} — a DETERMINISTIC
  # error it retried 5 times against a version that will never exist, and then
  # mark_failed QUARANTINED a shard that was never touched and is healthy at its old
  # version (quarantine also hides it from shards_at_version, so a later revert
  # skipped it too). The invariant: an unknown/yanked target cancels the job and
  # leaves the shard an ordinary active citizen.
  test "a yanked target cancels the job without quarantining the untouched shard",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = Migrator.yank(2)

    capture_log(fn ->
      # Even on the FINAL attempt, no quarantine — pre-fix this marked the shard
      # migration_failed and returned {:cancel, {:unknown_version, 2}}.
      assert {:cancel, :unknown_version} =
               perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2}, attempt: 5)
    end)

    assert {:ok, %{status: "active", schema_version: 1}} = Directory.get(shard),
           "a healthy shard must not be quarantined for a target that no longer exists"
  end

  # Round-2 #21a: an EXECUTING RevertJob already deserialized its args, so the force
  # sweep's jsonb row update couldn't reach it — the execution hit the write-age
  # guard, returned {:cancel, guard} (terminal), and the operator's explicit
  # force: true was silently dropped (the sweep's dedup had already counted this job
  # as handled). The invariant: a guard refusal re-checks the ROW args before going
  # terminal, and snoozes to re-run when they changed mid-execution.
  test "a guard refusal re-runs when the row args were force-upgraded mid-execution",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})

    # Post-cutover activity — the non-forced revert will hit the write-age guard.
    {:ok, _} = Directory.resolve(shard)

    stale_args = %{"shard_id" => shard, "to_version" => 1, "force" => false}
    {:ok, job} = Oban.insert(RevertJob.new(stale_args))

    # A force sweep rewrites the ROW while the job is executing (the running
    # execution still carries the stale deserialized copy).
    {1, _} =
      Fathom.Repo.update_all(
        from(j in Oban.Job, where: j.id == ^job.id),
        set: [args: %{"shard_id" => shard, "to_version" => 1, "force" => true}]
      )

    # Drive perform with the STALE args + the real row id, as the in-flight
    # execution would. Pre-fix: {:cancel, :writes_since_cutover} — force dropped.
    capture_log(fn ->
      assert {:snooze, 1} = RevertJob.perform(%{job | args: stale_args})
    end)
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
