defmodule Fathom.Shard.ConnectionResetForReuseTest do
  @moduledoc """
  The reset a POOLED handle gets before another stream of the SAME shard reuses it (connection
  pooling, docs/pooling-spike-plan.md). This is cross-REQUEST hygiene for one tenant — a handle
  never crosses shards — but an open transaction or a toggled pragma left by the previous stream
  must not carry into the next one. The transaction rollback is the load-bearing guard.
  """
  use ExUnit.Case, async: true

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection

  setup do
    dir = Path.join(System.tmp_dir!(), "reset_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "s.db")
    {:ok, c0} = Sqlite3.open(path)
    Sqlite3.execute(c0, "PRAGMA journal_mode=WAL")
    Sqlite3.execute(c0, "CREATE TABLE t(x INTEGER)")
    Sqlite3.execute(c0, "INSERT INTO t VALUES (1)")
    Sqlite3.close(c0)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, path: path}
  end

  defp count(conn) do
    {:ok, st} = Sqlite3.prepare(conn, "SELECT count(*) FROM t")
    {:row, [n]} = Sqlite3.step(conn, st)
    :ok = Sqlite3.release(conn, st)
    n
  end

  test "rolls back an open transaction on a :rw handle, discarding its uncommitted writes", %{
    path: path
  } do
    {:ok, conn} = Connection.open(path, tenant?: true, scope: :rw)
    :ok = Sqlite3.execute(conn, "BEGIN")
    :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (999)")
    refute Connection.autocommit?(conn), "precondition: in a transaction"
    assert count(conn) == 2, "precondition: the uncommitted row is visible inside the txn"

    assert :ok = Connection.reset_for_reuse(conn, :rw)

    assert Connection.autocommit?(conn), "the open transaction was rolled back"
    assert count(conn) == 1, "the previous stream's uncommitted INSERT was discarded"
    Connection.close(conn)
  end

  test "re-asserts foreign_keys after the previous stream turned it off (:rw)", %{path: path} do
    {:ok, conn} = Connection.open(path, tenant?: true, scope: :rw)
    :ok = Sqlite3.execute(conn, "PRAGMA foreign_keys=OFF")
    assert {:ok, 0} = Connection.pragma(conn, "foreign_keys"), "precondition: FK enforcement off"

    assert :ok = Connection.reset_for_reuse(conn, :rw)

    assert {:ok, 1} = Connection.pragma(conn, "foreign_keys"),
           "FK enforcement must be restored — a client relies on it being on by default"

    Connection.close(conn)
  end

  test "rolls back a read transaction on a :ro handle", %{path: path} do
    {:ok, conn} = Connection.open(path, tenant?: true, scope: :ro)
    :ok = Sqlite3.execute(conn, "BEGIN")
    {:ok, st} = Sqlite3.prepare(conn, "SELECT * FROM t")
    Sqlite3.step(conn, st)
    :ok = Sqlite3.release(conn, st)
    refute Connection.autocommit?(conn), "precondition: in a read transaction"

    assert :ok = Connection.reset_for_reuse(conn, :ro)

    assert Connection.autocommit?(conn), "the read transaction was rolled back"
    Connection.close(conn)
  end

  test "resetting an already-clean handle is a no-op that leaves it usable", %{path: path} do
    {:ok, conn} = Connection.open(path, tenant?: true, scope: :rw)
    assert :ok = Connection.reset_for_reuse(conn, :rw)
    assert count(conn) == 1
    Connection.close(conn)
  end
end
