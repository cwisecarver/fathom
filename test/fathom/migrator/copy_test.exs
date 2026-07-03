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
      "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
      "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002_add_created_at', '2026-02-01')"
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
      "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
      "INSERT INTO does_not_exist (x) VALUES (1)"
    ]

    assert {:error, _} = Copy.migrate(source, dest, 2, statements)

    # The ALTER rolled back: the copy is still the old schema, version unchanged.
    assert %{columns: ["id", "name"]} = query!(dest, "SELECT * FROM app_thing")
    assert %{rows: [[0]]} = query!(dest, "PRAGMA user_version")
  end
end
