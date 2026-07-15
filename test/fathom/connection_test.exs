defmodule Fathom.Shard.ConnectionTest do
  @moduledoc """
  `Fathom.Shard.Connection.open/1` pragmas — foreign-key enforcement
  (expert review 2026-07-14 #2). Not async: the `:foreign_keys` toggle case mutates
  global app config.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  setup do
    path =
      Path.join(System.tmp_dir!(), "fathom_conn_test_#{System.unique_integer([:positive])}.db")

    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    %{path: path}
  end

  defp with_fk_tables(conn) do
    :ok = Connection.exec(conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))"
      )
  end

  # Regression (#2): SQLite defaults foreign_keys=OFF, so without the server-side
  # `PRAGMA foreign_keys=ON` a bad-FK insert SILENTLY succeeds (orphan rows accumulate) and
  # Django's on_delete=CASCADE/PROTECT stops being enforced. Pre-fix this insert returns
  # {:ok, _}; post-fix it must error. Pins the invariant "FK enforcement is on by default".
  test "foreign keys are enforced by default on a fresh connection", %{path: path} do
    {:ok, conn} = Connection.open(path)
    with_fk_tables(conn)

    # pid 999 has no parent row → FK violation.
    assert {:error, _} = Connection.query(conn, "INSERT INTO child (id, pid) VALUES (1, 999)", [])

    :ok = Connection.close(conn)
  end

  test "config :foreign_keys false leaves enforcement off (opt-out preserved)", %{path: path} do
    prev = Application.get_env(:fathom, :foreign_keys)
    Application.put_env(:fathom, :foreign_keys, false)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :foreign_keys),
        else: Application.put_env(:fathom, :foreign_keys, prev)
    end)

    {:ok, conn} = Connection.open(path)
    with_fk_tables(conn)

    # With enforcement off, the same bad-FK insert succeeds.
    assert {:ok, _} = Connection.query(conn, "INSERT INTO child (id, pid) VALUES (1, 999)", [])

    :ok = Connection.close(conn)
  end
end
