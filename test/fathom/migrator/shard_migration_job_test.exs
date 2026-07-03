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
    assert File.exists?(Path.join(@remote_dir, "#{shard}@1.db"))

    assert :ok = perform_job(RetirementJob, %{"shard_id" => shard, "version" => 1})
    refute File.exists?(Path.join(@remote_dir, "#{shard}@1.db"))
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
end
