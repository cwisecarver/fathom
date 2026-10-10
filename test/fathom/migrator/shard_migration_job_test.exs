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

  # `file_version` stamps the STORED object's `user_version`; the directory row is always v1. A
  # value above the job's target makes a stored file that is ahead of it.
  defp seed_v1!(shard, file_version \\ 1) do
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

    :ok = Connection.exec(conn, "PRAGMA user_version = #{file_version}")
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

  # Expert review 2026-09-29 #31. Any Host-minted id registers an ACTIVE v0 directory row before it
  # has ever flushed, so a sweep enqueues it as a laggard with nothing stored to migrate. The inline
  # path already treats `:no_live_object` as benign; the job burned five attempts on it and then
  # QUARANTINED (`migration_failed`) a healthy never-flushed shard — which also hides it from later
  # sweeps after it does flush. Invariant: no live object ⇒ cancel, never retry, never quarantine.
  test "a never-flushed shard behind HEAD is cancelled, not retried or quarantined",
       %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app_thing (id INTEGER PRIMARY KEY)"])
    assert {:ok, %{schema_version: 0, status: "active"}} = Directory.get(shard)

    for attempt <- [1, 5] do
      assert {:cancel, :no_live_object} =
               perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 1},
                 attempt: attempt,
                 max_attempts: 5
               )
    end

    assert {:ok, %{schema_version: 0, status: "active"}} = Directory.get(shard)
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

  # Expert review 2026-08-31 #8: the guard above keyed on to_version == version, which holds only
  # for a one-step revert. A CHAIN-JUMPER — a cold-tail shard reverted v5 -> v9 in one job — carries
  # the fleet TARGET as to_version while its restore SOURCE is a LOWER landing version. So a
  # retirement of that landing version passed the to_version-keyed guard and dropped the revert's
  # restore source. The guard now matches ANY in-flight revert for the shard.
  test "RetirementJob skips the restore source of a chain-jump revert (to_version != version)",
       %{shard: shard} do
    seed_v1!(shard)
    :ok = Storage.retain(shard, 1)
    # The shard is at v3 (from_version of the revert), NOT at v1 — so the #22 live-guard passes.
    {:ok, _} = Directory.cutover(shard, 3)
    # A revert whose FLEET TARGET is v2 is in flight, but its restore source for this shard is the
    # lower retained landing v1. Pre-fix: revert_in_flight?(shard, 1) looked for to_version == 1,
    # found to_version == 2, and dropped <shard>@1 — the restore source.
    {:ok, _} = Oban.insert(RevertJob.new(%{shard_id: shard, to_version: 2}))

    assert {:cancel, :revert_in_flight} =
             perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})

    assert File.exists?(Path.join(remote_dir(), "#{shard}@1.db")),
           "a chain-jump revert's restore source (a landing version below to_version) was dropped"
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

  # A Django ledger the migrator cannot reconcile with the label (Fathom.Migrator.Ledger) is
  # deterministic, so the job quarantines on the FIRST attempt instead of retrying to the same
  # answer. Pre-fix the run migrated and the job returned :ok.
  test "a ledger mismatch quarantines the shard without retrying", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    {:ok, _} =
      Migrator.release(3, "v3", [
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0003_x', 'now')"
      ])

    # Plant v3's ledger row in the v1 object: a migration the label does not admit to.
    tmp = Path.join(System.tmp_dir!(), "lmm_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, _} = Storage.pull(shard, tmp)
    {:ok, conn} = Connection.open(tmp)

    :ok =
      Connection.exec(
        conn,
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0003_x', 'now')"
      )

    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)

    capture_log(fn ->
      assert {:cancel, :ledger_mismatch} =
               perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2}, attempt: 1)
    end)

    assert {:ok, %{status: "migration_failed", schema_version: 1}} = Directory.get(shard)
  end

  # Expert review 2026-09-29 #20, the purge half: a revert of a DELETED tenant must not create a
  # `<shard>@2` backup copy (a copy of an erased tenant the purge may never see), and must cancel
  # rather than `mark_failed` over the deleted status. Pre-fix the retain ran and the job fell to
  # `handle_error/3`.
  test "RevertJob of a deleted tenant creates no backup copy and cancels", %{shard: shard} do
    # A REAL v2 live object: with only the directory cut over, the live file is still v1 and the
    # revert takes its crash-forward branch, which never retains — the test would pass pre-fix.
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
    File.rm(Path.join(remote_dir(), "#{shard}@2.db"))
    Fathom.Tenants.Tombstones.put(shard)
    on_exit(fn -> :ets.delete(Fathom.Tenants.Tombstones, shard) end)

    capture_log(fn ->
      assert {:cancel, :not_active} =
               perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1})
    end)

    refute File.exists?(Path.join(remote_dir(), "#{shard}@2.db")),
           "the revert copied a deleted tenant's live object to a backup"

    refute match?({:ok, %{status: "failed"}}, Directory.get(shard)),
           "the cancelled revert quarantined the shard"
  end

  # THE RETAINED BACKUP MUST BE LABELLED WITH THE BYTES IT HOLDS (expert review 2026-08-24 #22).
  #
  # `do_run/3` treats the FILE as authoritative — `current = live_version(old)` reads
  # `PRAGMA user_version`. `forward/7` then computed `prev = current_version(shard_id)`, which reads
  # `schema_version` from POSTGRES, and called `Storage.retain(shard_id, prev)` — which copies the
  # LIVE OBJECT, at file version `current`, to `<shard>@prev`. When the two stamps disagree the
  # retained object is mislabelled, and `Storage.retain/2` overwrites any existing `<shard>@prev`
  # unconditionally.
  #
  # The skew needs nothing exotic: a flush landing while `cutover_with_retirement/3`'s Postgres
  # transaction failed leaves file=v2 / directory=v1, and a Postgres PITR does it fleet-wide. A
  # later `revert(3, 1)` then restores v2 bytes and stamps `schema_version = 1` — the tenant runs a
  # v2 schema with a v1 directory stamp and v2 rows in `django_migrations`, a three-way divergence
  # the operator believes the revert undid. Silent, because the forward path self-corrects by
  # re-reading the file.
  #
  # The fixture stages exactly that skew: migrate to v2 so the FILE is v2, then roll the DIRECTORY
  # back to v1 the way a failed cutover transaction or a PITR would, and migrate to v3.
  test "the retained backup is named for the FILE's version, not the directory's",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})

    # v3 is released AFTER the first hop: a job now migrates to the current head, not its stale arg
    # (expert review 2026-10-10 #31), so releasing it earlier would skip straight past v2.
    {:ok, _} = Migrator.release(3, "v3", ["ALTER TABLE app_thing ADD COLUMN note TEXT"])

    # The divergence: the file is v2, the directory says v1.
    {:ok, _} = Directory.cutover(shard, 1)
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)

    capture_log(fn ->
      assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 3})
    end)

    # The bytes retained are the v2 file, so the object must be @2. Pre-fix it was written to @1,
    # where a revert to v1 would later restore v2 bytes under a v1 stamp.
    assert File.exists?(Path.join(remote_dir(), "#{shard}@2.db")),
           "the v2 bytes were retained under the DIRECTORY's stale version instead of their own"

    # And the retirement scheduled matches what was retained, rather than naming a different object.
    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 2})
  end

  # A FLEET REVERT TO A VERSION THIS SHARD NEVER PASSED THROUGH (expert review 2026-08-24 #16).
  #
  # A forward migration retains exactly ONE object, `<shard>@prev` — the version the shard came
  # FROM. A cold-tail shard walks `current+1 … target` in a single job, so one that was at v1 and
  # migrated to HEAD=3 has only `<shard>@1`. A fleet revert picks one fleet-wide `to_version`
  # (`Migrator.revert(3, 2)`, or `head()` from `revert_stranded/0`) and asks every shard for
  # `<shard>@2`, which the chain-jumpers never created.
  #
  # Both backends answer deterministically — `{:error, :enoent}` from Local, `:version_absent` from
  # S3 — and RevertJob had no clause for either, so they fell to `handle_error/3` and burned five
  # doomed retries before a generic "revert failed permanently" that named neither the cause nor
  # the likely reason.
  #
  # THE QUARANTINE IS DELIBERATELY KEPT, against the finding's recommendation. It argued for
  # cancelling without marking, since the shard is healthy and quarantine hides it from a later
  # revert. That is right for the chain-jump case — but "no object at `<shard>@N`" has a second
  # cause storage cannot distinguish: the backup existed and was legitimately RETIRED past its
  # retention window. Expert review #24 added the quarantine for that case and the test above
  # ("an exhausted revert quarantines the shard") pins it; dropping it would re-open the silent
  # partial revert #24 closed. So this asserts the deterministic cancel and the durable record
  # together, and the operator's discrimination comes from the log and the telemetry event.
  #
  # WHAT THIS COVERS CHANGED ON 2026-08-26 (#16b), and the fixture stayed identical, which is worth
  # stating plainly. It was written as the chain-jump case: v1 retained, live at v3, revert asked
  # for v2. That case is now HANDLED — `RevertJob` reads `shards.retained_version`, restores v1 and
  # migrates forward to v2 (see the test below). This fixture cuts over with `Directory.cutover/2`,
  # which does not write that column, so what it actually exercises now is the NULL case: a
  # pre-column row, or a retained copy `RetirementJob` dropped past its retention window. That is
  # still a real path and still has to fail deterministically, so the test is kept and relabelled
  # rather than deleted.
  #
  # It also previously asserted the phrase "never passed through", which the message no longer
  # carries — deliberately, because that explanation is now wrong when the column is NULL. The
  # message says what NULL means instead.
  test "a revert with no recorded retained version fails deterministically, not after retries",
       %{shard: shard} do
    seed_v1!(shard)
    # Live at v3 with only @1 on disk, and NO retained_version recorded (cutover/2).
    :ok = Storage.retain(shard, 1)
    {:ok, _} = Directory.cutover(shard, 3)

    assert {:ok, %{retained_version: nil}} = Directory.get(shard),
           "the fixture must leave the column NULL — that absence is what this test is about"

    refute File.exists?(Path.join(remote_dir(), "#{shard}@2.db")),
           "the fixture must NOT have a @2 backup — its absence is the whole scenario"

    log =
      capture_log(fn ->
        # attempt: 1, NOT the final attempt — the point is that it does not wait for one.
        assert {:cancel, :no_retained_version} =
                 perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 2}, attempt: 1)
      end)

    # The diagnosis is the deliverable here: both causes produce the same storage error, so the
    # log is what lets an operator tell "retired past retention" from a column that was never set.
    assert log =~ "retained_version says nil"
    assert log =~ "revert_status"

    assert {:ok, %{status: "migration_failed"}} = Directory.get(shard),
           "the durable record from review #24 must survive — a revert that did not land has to " <>
             "be visible somewhere other than a log line"
  end

  # THE CHAIN-JUMP, LANDED (expert review 2026-08-24 #16b). The fixture above is the same shape;
  # the only difference is that `retained_version` is recorded, which is what a real cutover does.
  #
  # A cold-tail shard walks `current+1 … target` in ONE job and retains only the version it came
  # FROM, so a fleet revert naming a version it skipped used to quarantine it and leave it on the
  # bad schema — a fleet revert that read as landed while only the one-version-behind shards moved.
  #
  # IT LANDS ON THE REQUESTED VERSION, not on the retained one, and that is the substance. Stopping
  # at v1 is the cheaper implementation and it is wrong: `docs/migration.md` commits the fleet to
  # vN-1/vN tolerance, so parking a shard two versions below what the app expects trades "on the
  # bad schema" for "on a schema nothing running can read". So the revert restores v1 and enqueues
  # the forward climb back to v2.
  test "a chain-jumped shard reverts to its retained copy and is sent forward to the target",
       %{shard: shard} do
    seed_v1!(shard)
    :ok = Storage.retain(shard, 1)
    # A REAL cutover: live at v3, and the column records that @1 is what was retained.
    {:ok, _} = Directory.cutover(shard, 3, 1)

    refute File.exists?(Path.join(remote_dir(), "#{shard}@2.db")),
           "the fixture must NOT have a @2 backup — the shard skipped v2 entirely"

    log =
      capture_log(fn ->
        assert :ok =
                 perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 2}, attempt: 1)
      end)

    # Landed on the retained copy ...
    assert {:ok, %{schema_version: 1, status: "active"}} = Directory.get(shard),
           "the revert should have restored the retained v1 rather than quarantining"

    # ... and is on its way back to the version the fleet asked for. Without this the shard sits
    # below the fleet forever, which is the failure mode that made "revert as far back as it can
    # go" the wrong answer.
    assert_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => shard, "target" => 2})

    assert log =~ "migrating forward v1 -> v2"
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
    # v1 released too so the 0->2 chain is BUILDABLE (expert review 2026-09-18 #11): run/3 now
    # pre-flights chain buildability BEFORE with_lease, so a v0->v2 job with v1 missing would cancel
    # (unbuildable) instead of ever reaching the held-lease snooze this test exercises. A v2 with no
    # v1 is a gap production flags anyway, so releasing v1 makes the fixture realistic.
    {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app_v1 (id INTEGER PRIMARY KEY)"])
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

      # v1 released so the 0->2 chain is BUILDABLE — run/3's #11 pre-flight would otherwise cancel a
      # v1-missing chain before ever reaching the held-lease deferral these tests exercise.
      {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app_v1 (id INTEGER PRIMARY KEY)"])
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

    # Expert review 2026-10-08 #20: RevertJob deferred with a flat `{:snooze, 5}` at `[info]`. Every
    # attempt drains the shard, and a drain refuses new checkouts until it gives up, so during a
    # fleet revert a hot tenant spent about half its time refusing opens — forever, and silently.
    test "a deferred revert backs off and escalates the same way", %{shard: shard} do
      test_pid = self()
      handler = "revert-stalled-#{shard}"

      :telemetry.attach(
        handler,
        [:fathom, :migrator, :migration_stalled],
        fn _e, meas, meta, _ -> send(test_pid, {:stalled, meas, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      revert = fn attempt, inserted_at ->
        {{:snooze, seconds}, log} =
          with_log(fn ->
            perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1},
              attempt: attempt,
              inserted_at: inserted_at
            )
          end)

        {seconds, log}
      end

      {first, _} = revert.(1, DateTime.utc_now())
      {later, quiet} = revert.(4, DateTime.utc_now())

      {far, loud} =
        revert.(50, DateTime.add(DateTime.utc_now(), -:timer.minutes(30), :millisecond))

      assert later > first, "a revert deferred again and again kept snoozing #{first}s"
      assert far <= 60
      refute quiet =~ "STALLED"
      assert loud =~ "STALLED", "a revert deferring for 30 minutes must be alertable"
      assert_receive {:stalled, %{attempt: 50}, %{shard_id: ^shard, kind: :revert}}, 2_000
    end

    test "a stalled revert counts in Migrator.status/0's stalled", %{shard: shard} do
      {:ok, job} = Oban.insert(RevertJob.new(%{shard_id: shard, to_version: 1}, schedule_in: 60))
      old = DateTime.add(DateTime.utc_now(), -:timer.minutes(30), :millisecond)

      {1, _} =
        Fathom.Repo.update_all(
          Ecto.Query.from(j in Oban.Job, where: j.id == ^job.id),
          set: [inserted_at: old]
        )

      assert Migrator.status().stalled >= 1,
             "a revert stalled for 30 minutes was invisible to the status endpoint"
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
    # A stored file AHEAD of the target (directory v1, file stamped v3, target v2) -> a persistent
    # {:error, {:ahead_of_target, 3}} through every retry. This used a missing storage object as
    # its vehicle until expert review 2026-09-29 #31 made that a benign cancel (a never-flushed
    # shard is healthy, not failed). An unknown/yanked target or an unbuildable chain cancels
    # without marking too (round-2 #23, #11), so neither can be the vehicle either.
    seed_v1!(shard, 3)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    capture_log(fn ->
      assert {:cancel, {:ahead_of_target, 3}} =
               perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2}, attempt: 5)
    end)

    assert {:ok, %{status: "migration_failed"}} = Directory.get(shard)
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

  # THE INTERMEDIATE VERSION — a GUARD, not a regression test (expert review 2026-08-24 #15, which
  # was WRONG, and this is the record of why).
  #
  # The finding claimed that `{:error, {:unknown_version, target}}` is a PIN, because `target` is
  # already bound from the job's args in the function head — so it would match only when the
  # unavailable version WAS the job's target, and a yanked INTERMEDIATE version would fall through
  # to `handle_error/3`, burn 5 attempts, and `Directory.mark_failed/1` the whole cold tail.
  #
  # That is not Elixir. A variable in a `case` pattern REBINDS; pinning requires `^target`. So the
  # clause always matched any missing version, and the cold-tail quarantine the finding describes
  # cannot happen. Verified by execution rather than by reading: with the fix reverted, this exact
  # scenario returns `{:cancel, :unknown_version}` and leaves the shard `active` at v1.
  #
  # Kept because nothing covered the intermediate case at all — the test above yanks the job's own
  # target, so it could not tell the two apart even though the code handles both. What DID change
  # for #15 is the log line (it said "migration target v2" when 2 was an intermediate, which sends
  # an operator looking at the wrong thing) and a `[:fathom, :migrator, :unbuildable_chain]` event,
  # since the panel was right that a plain cancel leaves the shard permanently non-converging with
  # no durable record.
  test "yanking an INTERMEDIATE version cancels without quarantining the cold tail",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    {:ok, _} = Migrator.release(3, "v3", @v2_statements)

    # v2 is yanked but v3 is not, so HEAD stays 3 and a shard at v1 needs the missing v2.
    assert :ok = Migrator.yank(2)
    assert Migrator.head() == 3, "the fixture must leave HEAD ABOVE the yanked version"

    capture_log(fn ->
      assert {:cancel, :unknown_version} =
               perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 3}, attempt: 5)
    end)

    assert {:ok, %{status: "active", schema_version: 1}} = Directory.get(shard),
           "the shard was QUARANTINED for a hole in the release graph it had nothing to do " <>
             "with. It is healthy at v1, and quarantine also hides it from a later fleet revert."
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
  # Expert review 2026-08-26 #34. `revert_status/1`'s `in_flight` keyset-streamed EVERY active
  # shard_id at the version in 5 000-row pages and ran one `Repo.aggregate` over `oban_jobs` per
  # page — ~200 round trips and ~1M bind parameters at 1M shards, and an operator polls this
  # repeatedly while watching an emergency fleet revert drain.
  #
  # The finding named its own falsifying condition (the planner hash-joining instead of using
  # `oban_jobs_worker_shard_id_live_index`, which would make the chunked form better). Measured on
  # 20 000 shards / 80 000 jobs: the planner DOES hash-join, and the join is still 6x faster
  # (35 ms -> 6 ms) with identical results. That measurement is recorded at the call site.
  #
  # What this test pins is the CORRECTNESS half of that swap — the join must count the same
  # relation the chunked form did, including the boundaries where a naive join goes wrong.
  describe "revert_status/1 in_flight counts the same relation as the chunked form (#34)" do
    test "counts only live RevertJobs whose shard is still at the version" do
      at_version = "rs_at_#{System.unique_integer([:positive])}"
      moved_on = "rs_moved_#{System.unique_integer([:positive])}"
      other_worker = "rs_other_#{System.unique_integer([:positive])}"

      for id <- [at_version, moved_on, other_worker] do
        {:ok, _} = Directory.resolve(id)
      end

      {:ok, _} = Directory.cutover(at_version, 3)
      {:ok, _} = Directory.cutover(other_worker, 3)
      # This one has already reverted past the version, so its job must NOT be counted.
      {:ok, _} = Directory.cutover(moved_on, 2)

      {:ok, _} = Oban.insert(RevertJob.new(%{shard_id: at_version, to_version: 2}))
      {:ok, _} = Oban.insert(RevertJob.new(%{shard_id: moved_on, to_version: 2}))

      # A different worker for a shard that IS at the version — the worker predicate has to hold.
      {:ok, _} = Oban.insert(ShardMigrationJob.new(%{shard_id: other_worker, target: 4}))

      assert %{in_flight: 1} = Migrator.revert_status(3)
    end

    test "a shard with no job, and a job with no shard, are both excluded" do
      # The join's two failure modes. An inner join drops a job whose shard row is gone (a deleted
      # tenant mid-revert), which is correct — but it would also drop the shard if the join
      # direction were wrong, so both sides are asserted.
      no_job = "rs_nojob_#{System.unique_integer([:positive])}"
      {:ok, _} = Directory.resolve(no_job)
      {:ok, _} = Directory.cutover(no_job, 7)

      {:ok, _} = Oban.insert(RevertJob.new(%{shard_id: "rs_ghost_never_resolved", to_version: 6}))

      assert %{in_flight: 0} = Migrator.revert_status(7)
    end

    test "a completed job is not in flight" do
      # `:completed` is deliberately absent from @unique_states; the count must follow.
      id = "rs_done_#{System.unique_integer([:positive])}"
      {:ok, _} = Directory.resolve(id)
      {:ok, _} = Directory.cutover(id, 5)

      {:ok, job} = Oban.insert(RevertJob.new(%{shard_id: id, to_version: 4}))
      assert %{in_flight: 1} = Migrator.revert_status(5)

      Fathom.Repo.update_all(
        from(j in Oban.Job, where: j.id == ^job.id),
        set: [state: "completed"]
      )

      assert %{in_flight: 0} = Migrator.revert_status(5),
             "a completed revert job is still counted as in flight"
    end
  end

  # ---------------------------------------------------------------------------------------------
  # Expert review 2026-10-10 #3: a coordinator on ANOTHER node. `Shards.drain/2` only sees this
  # node's Registry, so it answered :ok, the lease read `held`, and the job snoozed forever — a hot
  # shard (served by a coordinator on 2 of 3 nodes) never migrated. The fake remote below plays the
  # holder node's CommandPoller: it sees the migrator's command in Postgres, releases the lease
  # (removes the lock) and completes the command.
  # ---------------------------------------------------------------------------------------------
  describe "a lease held by a coordinator on another node" do
    @remote_owner "remotenode#inc1"

    setup %{shard: shard} do
      seed_v1!(shard)
      {:ok, _} = Migrator.release(2, "v2", @v2_statements)
      put_remote_lock(shard)

      on_exit(fn ->
        File.rm(Path.join([remote_dir(), "heartbeats", URI.encode_www_form(@remote_owner)]))
      end)

      :ok
    end

    defp put_remote_lock(shard) do
      File.mkdir_p!(remote_dir())
      exp = System.system_time(:millisecond) + 60_000

      File.write!(
        Path.join(remote_dir(), "#{shard}.lock"),
        Jason.encode!(%{"owner" => @remote_owner, "epoch" => 1, "expires_at_ms" => exp})
      )

      hb_dir = Path.join(remote_dir(), "heartbeats")
      File.mkdir_p!(hb_dir)

      File.write!(
        Path.join(hb_dir, URI.encode_www_form(@remote_owner)),
        Jason.encode!(%{"owner" => @remote_owner, "expires_at_ms" => exp})
      )
    end

    defp fake_remote_node(shard, outcome) do
      Task.async(fn ->
        cmds =
          Stream.repeatedly(fn ->
            Process.sleep(20)
            Fathom.Rebalancer.Commands.pending_for("remotenode")
          end)
          |> Enum.find(&(&1 != []))

        for cmd <- cmds do
          assert cmd.command == "drain_for_migration"
          assert cmd.shard_id == shard

          if outcome == :drain do
            File.rm!(Path.join(remote_dir(), "#{shard}.lock"))
            Fathom.Rebalancer.Commands.complete(cmd, "done", "drained")
          else
            Fathom.Rebalancer.Commands.complete(cmd, "failed", "drain failed (:busy)")
          end
        end
      end)
    end

    test "the migration drains the remote holder through the command channel and proceeds",
         %{shard: shard} do
      remote = fake_remote_node(shard, :drain)

      assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
      Task.await(remote)

      assert {:ok, %{schema_version: 2}} = Directory.get(shard)
    end

    test "a remote holder that refuses the drain leaves the job snoozing, not failing",
         %{shard: shard} do
      remote = fake_remote_node(shard, :refuse)

      assert {:snooze, _} = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
      Task.await(remote)

      assert {:ok, %{schema_version: 1, status: "active"}} = Directory.get(shard)
    end

    test "an unresponsive remote node (poller off) times out, cancels its command, and snoozes",
         %{shard: shard} do
      Application.put_env(:fathom, :migration_remote_drain_ms, 150)
      on_exit(fn -> Application.delete_env(:fathom, :migration_remote_drain_ms) end)

      assert {:snooze, _} = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})

      assert [] = Fathom.Rebalancer.Commands.pending_for("remotenode"),
             "a timed-out migrator command must not stay pending for the holder to fire later"
    end

    test "the revert path drains the remote holder too", %{shard: shard} do
      # Migrate first (the remote releases), then a coordinator on the remote node re-takes the lease.
      remote = fake_remote_node(shard, :drain)
      assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
      Task.await(remote)

      put_remote_lock(shard)
      remote = fake_remote_node(shard, :drain)

      assert :ok =
               perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1, "force" => true})

      Task.await(remote)
      assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    end
  end

  # Expert review 2026-10-10 #4.
  describe "transient errors" do
    test "environmental errors are transient; deterministic ones are not" do
      for r <- [
            {:lease_unavailable, :timeout},
            {:drain_failed, :busy},
            {:fence_failed, :closed},
            {:s3_get_status, 503},
            {:s3_put_status, 429},
            :timeout
          ] do
        assert ShardMigrationJob.transient?(r), "#{inspect(r)} should be transient"
      end

      for r <- [{:ahead_of_target, 3}, {:s3_get_status, 404}, {:s3_put_status, 403}, :enoent] do
        refute ShardMigrationJob.transient?(r), "#{inspect(r)} must keep counting"
      end
    end

    # Symptom: a 5-minute S3 brownout burned all 5 attempts and QUARANTINED healthy shards.
    # Invariant: a transient error on the LAST attempt snoozes and leaves the shard active.
    test "a transient error on the final attempt does not quarantine", %{shard: shard} do
      seed_v1!(shard)
      {:ok, _} = Migrator.release(2, "v2", @v2_statements)

      # A lock-store failure: make the lock path a directory so acquire_lease errors.
      lock = Path.join(remote_dir(), "#{shard}.lock")
      File.mkdir_p!(lock)
      on_exit(fn -> File.rm_rf(lock) end)

      result =
        capture_log(fn ->
          send(
            self(),
            {:result,
             perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2},
               attempt: 5,
               max_attempts: 5
             )}
          )
        end)

      assert is_binary(result)
      assert_received {:result, {:snooze, _}}
      assert {:ok, %{status: "active"}} = Directory.get(shard)
    end

    test "backoff grows and caps" do
      assert ShardMigrationJob.backoff(%Oban.Job{attempt: 1}) == 30
      assert ShardMigrationJob.backoff(%Oban.Job{attempt: 3}) == 120
      assert ShardMigrationJob.backoff(%Oban.Job{attempt: 20}) == 900
    end
  end

  # Expert review 2026-10-10 #31: unique-per-shard means a job queued at an old head blocks a newer
  # enqueue. The job must therefore migrate to the CURRENT head, not its stale arg.
  test "a job carrying a stale target migrates to the current head", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    {:ok, _} =
      Migrator.release(3, "v3", [
        "ALTER TABLE app_thing ADD COLUMN extra TEXT",
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0003', 'now')"
      ])

    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
    assert {:ok, %{schema_version: 3}} = Directory.get(shard)
  end
end
