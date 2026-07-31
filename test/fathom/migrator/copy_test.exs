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
end
