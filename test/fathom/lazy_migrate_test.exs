defmodule Fathom.LazyMigrateTest do
  # Migrate-then-serve on checkout: a behind-HEAD shard is migrated inline before
  # it serves. Real shard machinery + storage + directory; not async.
  use Fathom.DataCase, async: false

  # These build fixtures by releasing versions with a capture template configured, which is
  # exactly the configuration `Migrator.release/6` warns about (novel tenants born empty).
  # The warning is correct here and not what these tests are about, so capture it: ExUnit
  # still prints captured logs when a test FAILS, so this hides noise without hiding signal.
  @moduletag :capture_log
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

  # The reserved capture template must NEVER be migrate-on-touched. Django migrates it directly, so
  # its directory stamp never advances and it always reads as a laggard — but replaying its own
  # captured DDL back onto itself is "already exists", and a drain racing an in-flight
  # `manage.py migrate` can drop the capture buffer and fork the fleet from the template.
  #
  # `Directory.laggard_query/1` already excluded it from the reconcile/rollout sweep (expert review
  # 2026-07-14 #8), but migrate-on-touch (#40) added a SECOND independent behind-HEAD check that
  # never inherited the exclusion — reopening the same bug through a different door. Caught by
  # running a real `manage.py migrate` against a stack with MIGRATE_ON_TOUCH=inline: the template's
  # own migration silently stopped being captured, so the fleet stopped receiving versions.
  #
  # Both modes are pinned because they take different branches (:inline migrates in-band, :async
  # enqueues a job), and both were broken.
  defp as_template(shard) do
    prev = Application.get_env(:fathom, :template_shard_id)
    Application.put_env(:fathom, :template_shard_id, shard)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :template_shard_id),
        else: Application.put_env(:fathom, :template_shard_id, prev)
    end)
  end

  test "migrate_on_touch: :inline never migrates the capture template", %{shard: shard} do
    Application.put_env(:fathom, :migrate_on_touch, :inline)
    as_template(shard)
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    Migrator.HeadCache.refresh()

    {:ok, conn} = ShardExecutor.open(shard)

    # Untouched at v1: Django owns this shard's schema, not the rollout.
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)

    assert {:ok, %StmtResult{cols: ["id", "name"]}} = exec(conn, "SELECT * FROM app_thing"),
           "the capture template must not have HEAD's DDL replayed onto itself"

    ShardExecutor.close(conn)
  end

  test "migrate_on_touch: :async never enqueues a migration for the capture template",
       %{shard: shard} do
    Application.put_env(:fathom, :migrate_on_touch, :async)
    as_template(shard)
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    Migrator.HeadCache.refresh()

    {:ok, conn} = ShardExecutor.open(shard)

    refute_enqueued(worker: ShardMigrationJob, args: %{shard_id: shard})
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)

    ShardExecutor.close(conn)
  end

  # Normalization (finding #19): a mixed-case :template_shard_id must still match the canonical id,
  # or the exclusion silently misses and the bug is back for anyone who typed capitals.
  test "the template exclusion compares normalized ids", %{shard: shard} do
    Application.put_env(:fathom, :migrate_on_touch, :async)
    as_template(String.upcase(shard))
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    Migrator.HeadCache.refresh()

    {:ok, conn} = ShardExecutor.open(shard)
    refute_enqueued(worker: ShardMigrationJob, args: %{shard_id: shard})
    ShardExecutor.close(conn)
  end

  # A FAILING inline migration must not take the tenant offline.
  #
  # Reported live 2026-07-31: a demo fleet whose tenants had been provisioned by a direct per-shard
  # `manage.py migrate` — so each shard's schema was ahead of fathom's version stamp — had every one
  # of its pages fail with an opaque `STREAM_NOT_FOUND` the moment a version was released. The
  # replay hit "table already exists", `lazy_migrate` returned that error, and it failed the
  # CHECKOUT. Every subsequent request retried the same doomed multi-second copy, so the tenant was
  # permanently down with no useful error.
  #
  # The shard is intact at its current version, and serving vN-1 is safe by the same expand-contract
  # argument that makes `:off` and `:async` correct modes — `{:unknown_version, _}` above already
  # reasons exactly this way. So a failure degrades to `:async` behavior: hand the shard to the
  # rollout (which owns retry and quarantine) and serve it now.
  test "an inline migration that FAILS still serves the shard, and hands it to the rollout",
       %{shard: shard} do
    Application.put_env(:fathom, :migrate_on_touch, :inline)
    seed_v1!(shard)

    # A version whose replay cannot succeed against this shard: `app_thing` already exists, which is
    # exactly the shape a directly-migrated tenant produces.
    {:ok, _} =
      Migrator.release(2, "v2", [
        "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002', 'now')"
      ])

    Migrator.HeadCache.refresh()

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:ok, conn} = ShardExecutor.open(shard),
               "a failed migration must not fail the checkout — that takes the tenant offline"

        # Served at its EXISTING version, with its data intact.
        assert {:ok, %StmtResult{rows: [[1, "alice"]]}} =
                 exec(conn, "SELECT id, name FROM app_thing")

        ShardExecutor.close(conn)
      end)

    # Loud, not silent: an operator has to be able to find this.
    assert log =~ "inline migrate-on-touch"

    # Still at v1, and handed to the async rollout rather than retried on the next request.
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    assert_enqueued(worker: ShardMigrationJob, args: %{shard_id: shard, target: 2})
  end

  # `mark_migrating/1` runs just before the copy and had no counterpart, so a failed migration left
  # the row `migrating` forever. Every laggard/reconcile query filters `status == "active"`, so the
  # shard went invisible to every sweep until the hourly `reclaim_stuck_migrating` — it self-healed
  # an hour late instead of immediately. Observed live as `home-00001: v=0 status=migrating` after a
  # replay hit "table already exists".
  test "a failed migration restores the shard to active instead of wedging it in migrating",
       %{shard: shard} do
    seed_v1!(shard)

    # A version whose replay cannot succeed against this shard.
    {:ok, _} =
      Migrator.release(2, "v2", ["CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)"])

    assert {:error, _} = Fathom.Migrator.ShardMigration.run(shard, 2)

    # Untouched at v1 — nothing was applied (the copy is to a temp file, the replay is one
    # transaction) — and visible to the sweeps again rather than stuck.
    assert {:ok, %{schema_version: 1, status: "active", migrating_since: nil}} =
             Directory.get(shard)
  end

  test "the restore never resurrects a tenant that was deleted mid-copy", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Directory.mark_migrating(shard)
    # Whatever else happened to the row during the copy window wins — unmark is conditional.
    {:ok, _} = Directory.tombstone(shard)

    assert Directory.unmark_migrating(shard) == 0
    assert {:ok, %{status: "deleted"}} = Directory.get(shard)
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
