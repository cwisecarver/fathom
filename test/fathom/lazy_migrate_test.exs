defmodule Fathom.LazyMigrateTest do
  # Migrate-then-serve on checkout: a behind-HEAD shard is migrated inline before
  # it serves. Real shard machinery + storage + directory; not async.
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.{Directory, Migrator, ShardExecutor}
  alias Fathom.Migrator.ShardMigrationJob
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.{Stmt, StmtResult}

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002', 'now')"
  ]

  setup do
    shard = "lazy_#{System.unique_integer([:positive])}"
    prev = Application.get_env(:fathom, :lazy_migrate)
    prev_mode = Application.get_env(:fathom, :migrate_on_touch)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :lazy_migrate),
        else: Application.put_env(:fathom, :lazy_migrate, prev)

      if prev_mode == nil,
        do: Application.delete_env(:fathom, :migrate_on_touch),
        else: Application.put_env(:fathom, :migrate_on_touch, prev_mode)

      for path <- Path.wildcard(Path.join(remote_dir(), "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{shard}*"])),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp seed_v1!(shard) do
    seed = Path.join(System.tmp_dir!(), "seedl_#{shard}_#{System.unique_integer([:positive])}.db")
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

  defp exec(conn, sql), do: ShardExecutor.execute(conn, %Stmt{sql: sql, args: []})

  test "checkout migrates a behind-HEAD shard before serving", %{shard: shard} do
    Application.put_env(:fathom, :lazy_migrate, true)
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    # HEAD is read from the TTL cache on the checkout path; force it fresh so the test
    # doesn't race the background refresh window (see Fathom.Migrator.HeadCache).
    Migrator.HeadCache.refresh()

    {:ok, conn} = ShardExecutor.open(shard)

    # The shard was migrated to v2 before serving.
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)

    # And the connection serves v2 (new column present, original row preserved).
    assert {:ok, %StmtResult{rows: [[1, "alice", nil]]}} =
             exec(conn, "SELECT id, name, created_at FROM app_thing")

    ShardExecutor.close(conn)
  end

  # Round-2 #23 (the client-visible half): when HEAD drops on a yank, other nodes
  # read a stale-HIGH head from their TTL cache for up to one TTL. The lazy path
  # then targets the yanked version, gets {:error, {:unknown_version, _}}, and
  # pre-fix FAILED THE CLIENT CHECKOUT — for a shard that is perfectly healthy at
  # its old version, which is exactly what the fleet wants it to serve.
  test "a stale-high HEAD after a yank serves the old version instead of failing checkout",
       %{shard: shard} do
    Application.put_env(:fathom, :lazy_migrate, true)
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    assert :ok = Migrator.yank(2)

    # Another node's stale cache window: HEAD still reads 2 for up to one TTL.
    pt_key = {Fathom.Migrator.HeadCache, :head}
    prev_head = :persistent_term.get(pt_key, 0)
    on_exit(fn -> :persistent_term.put(pt_key, prev_head) end)
    :persistent_term.put(pt_key, 2)

    {:ok, conn} = ShardExecutor.open(shard)

    # Served at the old version — no failure, no migration to the yanked target.
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    assert {:ok, %StmtResult{cols: ["id", "name"]}} = exec(conn, "SELECT * FROM app_thing")

    ShardExecutor.close(conn)
  end

  test "without lazy_migrate, checkout serves the shard as-is", %{shard: shard} do
    # lazy_migrate stays disabled (default).
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)

    {:ok, conn} = ShardExecutor.open(shard)

    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    # No new column — the shard is still v1.
    assert {:ok, %StmtResult{cols: ["id", "name"]}} = exec(conn, "SELECT * FROM app_thing")

    ShardExecutor.close(conn)
  end

  # Expert review #40: the enqueue-on-touch (:async) middle mode — spot the laggard on checkout,
  # ENQUEUE its migration (deduped), and serve vN-1 THIS request (expand-contract makes that safe),
  # instead of either the up-to-an-hour stale window (:off) or the multi-second inline block
  # (:inline). The first request is not blocked; the tenant converges on the next job cycle.
  test "migrate_on_touch: :async enqueues the migration and serves vN-1 this request",
       %{shard: shard} do
    Application.put_env(:fathom, :migrate_on_touch, :async)
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    Migrator.HeadCache.refresh()

    {:ok, conn} = ShardExecutor.open(shard)

    # Served as-is (v1) — the checkout did NOT block on the migration.
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    assert {:ok, %StmtResult{cols: ["id", "name"]}} = exec(conn, "SELECT * FROM app_thing")

    # But the migration to HEAD was enqueued for the async rollout to converge it.
    assert_enqueued(worker: ShardMigrationJob, args: %{shard_id: shard, target: 2})

    ShardExecutor.close(conn)
  end

  test "migrate_on_touch: :async does not enqueue for a shard already at HEAD", %{shard: shard} do
    Application.put_env(:fathom, :migrate_on_touch, :async)
    seed_v1!(shard)
    # No release beyond v1 → HEAD is (at most) 1, the shard is not behind.
    Migrator.HeadCache.refresh()

    {:ok, conn} = ShardExecutor.open(shard)
    refute_enqueued(worker: ShardMigrationJob, args: %{shard_id: shard})
    ShardExecutor.close(conn)
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
