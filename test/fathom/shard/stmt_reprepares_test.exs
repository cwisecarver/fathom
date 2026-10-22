defmodule Fathom.Shard.StmtRepreparesTest do
  @moduledoc """
  `fathom_stmt_reprepares(sql)` (expert review 2026-10-21 r7 #1): the extension counter over
  `sqlite3_stmt_status(SQLITE_STMTSTATUS_REPREPARE)`, the only way to see a re-prepare that happens
  INSIDE `sqlite3_step`.

  Symptom pinned: SQLite's flag pragmas (`PRAGMA foreign_keys = ON`, which Django sends first on
  every new connection and which fathom's pooled-reuse batch also runs) expire every prepared
  statement on the connection, whether or not the value changes. Each carried cached statement is
  then re-prepared (parse + plan) at its next step, on the dirty-IO thread, in a dispatch no counter
  sees. Invariants pinned here: (1) the counter reads 0 for a statement that was prepared once and
  merely re-stepped; (2) it reads > 0 after a flag-pragma assignment and a re-step; (3) NULL for a
  statement text that is not prepared; (4) the NULL-argument form sums over statements.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.DjangoSchema
  alias Fathom.Shard.Connection

  setup do
    path = Path.join(System.tmp_dir!(), "reprep_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    DjangoSchema.build!(path, wide: true, rows: 3)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, conn: conn, sql: DjangoSchema.wide_select_sql()}
  end

  defp reprepares(conn, sql) do
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT fathom_stmt_reprepares(?1)", [sql])
    n
  end

  test "a re-stepped statement without a flag pragma is never re-prepared", %{
    conn: conn,
    sql: sql
  } do
    assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])
    assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])
    assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])

    assert reprepares(conn, sql) == 0
  end

  test "PRAGMA foreign_keys = ON expires the cached statement: its next step re-prepares", %{
    conn: conn,
    sql: sql
  } do
    assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])
    assert reprepares(conn, sql) == 0

    # Django's per-connection pragma. The value is already ON; SQLite expires regardless.
    assert {:ok, _} = Connection.query(conn, "PRAGMA foreign_keys = ON", [])
    assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])

    assert reprepares(conn, sql) > 0,
           "the pragma did not re-prepare the carried statement; the counter (or the premise) is wrong"

    assert {:ok, %{rows: [[total]]}} =
             Connection.query(conn, "SELECT fathom_stmt_reprepares(NULL)", [])

    assert total >= reprepares(conn, sql)
  end

  test "NULL for a statement that is not prepared on this connection", %{conn: conn} do
    assert reprepares(conn, "SELECT 'never prepared'") == nil
  end
end
