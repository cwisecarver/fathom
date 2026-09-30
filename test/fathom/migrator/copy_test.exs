defmodule Fathom.Migrator.CopyTest do
  use ExUnit.Case, async: true

  alias Fathom.Migrator.Copy
  alias Fathom.Shard.Connection

  setup do
    base = Path.join(System.tmp_dir!(), "fathom_copy_#{System.unique_integer([:positive])}")
    source = base <> "-old.db"
    dest = base <> "-new.db"

    on_exit(fn ->
      for path <- [source, dest], suffix <- ["", "-wal", "-shm"], do: File.rm(path <> suffix)
    end)

    %{source: source, dest: dest}
  end

  # A v0 shard: an app table with a row + Django's own bookkeeping table.
  defp seed_v0!(path) do
    {:ok, conn} = Connection.open(path)
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
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001_initial', '2026-01-01')"
      )

    Connection.close(conn)
  end

  defp query!(path, sql) do
    {:ok, conn} = Connection.open(path)
    {:ok, result} = Connection.query(conn, sql, [])
    Connection.close(conn)
    result
  end

  test "replays a Django-style DDL batch onto a copy, keeping bookkeeping consistent",
       %{source: source, dest: dest} do
    seed_v0!(source)

    statements = [
      {"ALTER TABLE app_thing ADD COLUMN created_at TEXT", []},
      {"INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002_add_created_at', '2026-02-01')",
       []}
    ]

    assert :ok = Copy.migrate(source, dest, 2, statements)

    # New column present, original row preserved.
    assert %{rows: [[1, "alice", nil]]} =
             query!(dest, "SELECT id, name, created_at FROM app_thing")

    # Django bookkeeping carries both migrations.
    assert %{rows: [["0001_initial"], ["0002_add_created_at"]]} =
             query!(dest, "SELECT name FROM django_migrations ORDER BY name")

    # Version stamped on the new file; source untouched.
    assert %{rows: [[2]]} = query!(dest, "PRAGMA user_version")
    assert %{rows: [[0]]} = query!(source, "PRAGMA user_version")
  end

  test "a failing statement rolls back, leaving the copy at the old schema",
       %{source: source, dest: dest} do
    seed_v0!(source)

    statements = [
      {"ALTER TABLE app_thing ADD COLUMN created_at TEXT", []},
      {"INSERT INTO does_not_exist (x) VALUES (1)", []}
    ]

    assert {:error, _} = Copy.migrate(source, dest, 2, statements)

    # The ALTER rolled back: the copy is still the old schema, version unchanged.
    assert %{columns: ["id", "name"]} = query!(dest, "SELECT * FROM app_thing")
    assert %{rows: [[0]]} = query!(dest, "PRAGMA user_version")
  end

  # THE bug that made the whole forward rollout non-functional against real Django. Django sends
  # PARAMETERIZED SQL — its bookkeeping row is `INSERT INTO django_migrations … VALUES (?, ?, ?)`
  # with the values carried alongside — and replay ran the statement TEXT with no args, so SQLite
  # bound NULL and died on `NOT NULL constraint failed: django_migrations.app`, rolling back the
  # entire copy. Every Django migration ends with that row, so NO captured migration could be
  # replayed onto any tenant. Measured live 2026-07-30 before the fix:
  #
  #     ShardMigration.run("mig-0003", 2)
  #     => {:error, "NOT NULL constraint failed: django_migrations.app"}
  #
  # Invisible to this suite because every test above writes its values inline as literal SQL, which
  # real Django never does. Values are BOUND, never interpolated into the statement — a migration
  # name is attacker-influenceable (it is a filename) and one apostrophe would be enough.
  test "binds parameters instead of leaving placeholders unbound (real Django shape)",
       %{source: source, dest: dest} do
    seed_v0!(source)

    statements = [
      {"ALTER TABLE app_thing ADD COLUMN created_at TEXT", []},
      {~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?)|,
       ["finance", "0002_budget", "2026-07-30T00:00:00"]}
    ]

    assert :ok = Copy.migrate(source, dest, 2, statements)

    assert %{rows: [["finance", "0002_budget"]]} =
             query!(dest, "SELECT app, name FROM django_migrations WHERE name = '0002_budget'")

    assert %{rows: [[2]]} = query!(dest, "PRAGMA user_version")
  end

  # A value that would break string interpolation, and one that JSON cannot carry raw. Args ride as
  # bind values through `Filo.Value`'s tagged encoding, so neither is a special case.
  test "binds values that would break interpolation (quotes, blobs, nil)",
       %{source: source, dest: dest} do
    seed_v0!(source)

    statements = [
      {"ALTER TABLE app_thing ADD COLUMN note TEXT", []},
      {"ALTER TABLE app_thing ADD COLUMN payload BLOB", []},
      {"INSERT INTO app_thing (id, name, note, payload) VALUES (?, ?, ?, ?)",
       [2, "o'brien; DROP TABLE app_thing; --", nil, {:blob, <<0, 255, 10>>}]}
    ]

    assert :ok = Copy.migrate(source, dest, 2, statements)

    assert %{rows: [["o'brien; DROP TABLE app_thing; --", nil, <<0, 255, 10>>]]} =
             query!(dest, "SELECT name, note, payload FROM app_thing WHERE id = 2")

    # The table the injected fragment tried to drop is still there, with both rows.
    assert %{rows: [[2]]} = query!(dest, "SELECT count(*) FROM app_thing")
  end

  # Expert review 2026-09-29 #15. Django's SQLite schema editor sends `PRAGMA foreign_keys = OFF`
  # BEFORE `BEGIN`, so it is never in the captured buffer, and `Connection.open` turns FKs ON. The
  # replay then ran Django's table rebuild with FKs enforced: the DROP of a parent counts a deferred
  # violation for every child row that references it, and COMMIT fails. Every AlterField /
  # RemoveField / constraint change on a referenced model failed on every POPULATED tenant. The
  # fixture is populated on purpose — an empty child table passes either way, which is exactly why
  # capture (on the template) and the existing tests never saw it. Pre-fix this returns
  # {:error, _} ("FOREIGN KEY constraint failed").
  defp seed_parent_child!(path) do
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "CREATE TABLE app_parent (id INTEGER PRIMARY KEY, name TEXT)")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE app_child (id INTEGER PRIMARY KEY, " <>
          "parent_id INTEGER NOT NULL REFERENCES app_parent (id) DEFERRABLE INITIALLY DEFERRED)"
      )

    :ok = Connection.exec(conn, "INSERT INTO app_parent (id, name) VALUES (1, 'p')")
    :ok = Connection.exec(conn, "INSERT INTO app_child (id, parent_id) VALUES (1, 1)")
    Connection.close(conn)
  end

  # Django's `_remake_table`, as captured: build new__x, copy, drop x, rename.
  @rebuild_parent [
    {"CREATE TABLE \"new__app_parent\" (\"id\" integer NOT NULL PRIMARY KEY, " <>
       "\"name\" varchar(200) NOT NULL)", []},
    {"INSERT INTO \"new__app_parent\" (\"id\", \"name\") SELECT \"id\", \"name\" " <>
       "FROM \"app_parent\"", []},
    {"DROP TABLE \"app_parent\"", []},
    {"ALTER TABLE \"new__app_parent\" RENAME TO \"app_parent\"", []}
  ]

  test "a Django table rebuild of an FK-referenced table replays onto a populated shard",
       %{source: source, dest: dest} do
    seed_parent_child!(source)

    assert :ok = Copy.migrate(source, dest, 2, @rebuild_parent),
           "the rebuild failed under enforced foreign keys on a shard with child rows"

    assert %{rows: [[1, 1]]} = query!(dest, "SELECT id, parent_id FROM app_child")
    assert %{rows: [[1, "p"]]} = query!(dest, "SELECT id, name FROM app_parent")
  end

  # The other half: FKs are OFF during the replay, so the per-step `foreign_key_check` is what still
  # refuses a step that GENUINELY breaks a reference — the check Django's schema editor runs on exit.
  test "a step that genuinely orphans a child row is refused, not shipped",
       %{source: source, dest: dest} do
    seed_parent_child!(source)

    assert {:error, {:foreign_key_violation, 2, [_ | _]}} =
             Copy.migrate(source, dest, 2, [{"DELETE FROM app_parent", []}])
  end
end
