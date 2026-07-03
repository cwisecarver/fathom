defmodule Fathom.Migrator.ShardMigrationJobTest do
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  import ExUnit.CaptureLog

  alias Fathom.Migrator
  alias Fathom.Migrator.{RetirementJob, RevertJob, ShardMigrationJob}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Directory

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002', 'now')"
  ]

  setup do
    shard = "job_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for path <- Path.wildcard(Path.join(@remote_dir, "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join([System.tmp_dir!(), "fathom_shards", "#{shard}*"])),
          do: File.rm(path)

      File.rm(Path.join([@remote_dir, "heartbeats", "thief@node"]))
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
    File.mkdir_p!(@remote_dir)
    exp = System.system_time(:millisecond) + 60_000

    File.write!(
      Path.join(@remote_dir, "#{shard}.lock"),
      Jason.encode!(%{"owner" => "thief@node", "epoch" => 1, "expires_at_ms" => exp})
    )

    # Liveness is the per-node heartbeat now: a held lock needs a live owner, else
    # the migrator would just steal it instead of snoozing.
    hb_dir = Path.join(@remote_dir, "heartbeats")
    File.mkdir_p!(hb_dir)

    File.write!(
      Path.join(hb_dir, "thief@node"),
      Jason.encode!(%{"owner" => "thief@node", "expires_at_ms" => exp})
    )
  end

  # A lock held under the OLD shared migrator owner (no per-operation token), fresh TTL, NO
  # heartbeat — so the #11 lock-TTL fallback keeps it live. Post-fix a new operation's owner
  # `migrator@<node>@<job.id>` is foreign to this, so it can't reclaim it (finding #9).
  defp put_migrator_lock(shard) do
    File.mkdir_p!(@remote_dir)

    File.write!(
      Path.join(@remote_dir, "#{shard}.lock"),
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

  test "RetirementJob drops the retained version", %{shard: shard} do
    seed_v1!(shard)
    :ok = Storage.retain(shard, 1)
    # The retained version is genuinely OLD (live has moved past it) — the normal case.
    {:ok, _} = Directory.cutover(shard, 2)
    assert File.exists?(Path.join(@remote_dir, "#{shard}@1.db"))

    assert :ok = perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})
    refute File.exists?(Path.join(@remote_dir, "#{shard}@1.db"))
  end

  # Expert review #22: the drop must skip a version the directory shows LIVE — a revert
  # restored it after this retirement was scheduled, and its retained copy is the
  # recovery point the next revert restores from, not garbage.
  test "RetirementJob skips a version that is live again", %{shard: shard} do
    seed_v1!(shard)
    :ok = Storage.retain(shard, 1)

    assert {:cancel, :version_live} =
             perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})

    assert File.exists?(Path.join(@remote_dir, "#{shard}@1.db")),
           "the live version's retained copy must not be dropped"
  end

  test "a held lease snoozes the job", %{shard: shard} do
    put_foreign_lock(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    assert {:snooze, _} = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
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
    # target 9 has no released statements -> a permanent {:error, :unknown_version}.
    capture_log(fn ->
      assert {:cancel, _} =
               perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 9}, attempt: 5)
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

    assert File.exists?(Path.join(@remote_dir, "#{shard}@2.db")),
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
end
