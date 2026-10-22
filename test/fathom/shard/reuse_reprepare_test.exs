defmodule Fathom.Shard.ReuseRepreparesTest do
  @moduledoc """
  A pooled reuse must not make the handle's carried statements re-parse (expert review 2026-10-21
  r7 #13a).

  Symptom: `PRAGMA foreign_keys=ON` (rw) and `PRAGMA query_only=ON` (ro) emit OP_Expire
  unconditionally, so `reset_for_reuse/3` expired every statement the pooled handle carried and the
  stream's first step of each re-prepared it (29-column read 4.4 -> 21.5 us). Invariants pinned:
  (1) after a reuse the carried statement's `fathom_stmt_reprepares` is unchanged, for `:rw` and
  `:ro`; (2) the setting the PRAGMA guaranteed still holds: foreign keys ON after a stream turned them
  `OFF` (the one case where the flag really changes and the function path aborts into the PRAGMA
  fallback).
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.DjangoSchema
  alias Fathom.Shard.Connection

  setup do
    path = Path.join(System.tmp_dir!(), "reuse_rp_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    DjangoSchema.build!(path, wide: true, rows: 3)
    {:ok, path: path, sql: DjangoSchema.wide_select_sql()}
  end

  defp open!(path, scope) do
    {:ok, conn} = Connection.open(path, tenant?: true, scope: scope)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  defp reprepares(conn, sql) do
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT fathom_stmt_reprepares(?1)", [sql])
    n
  end

  for scope <- [:rw, :ro] do
    test "#{scope}: a reuse does not re-prepare the carried statement", %{path: path, sql: sql} do
      conn = open!(path, unquote(scope))
      assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])
      before = reprepares(conn, sql)

      assert :ok = Connection.reset_for_reuse(conn, unquote(scope))
      assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])

      assert reprepares(conn, sql) == before,
             "reset_for_reuse expired the carried statement; its next step re-parsed it"
    end
  end

  test "rw: foreign keys are ON again after a stream turned them OFF", %{path: path, sql: sql} do
    conn = open!(path, :rw)
    {:ok, _} = Connection.query(conn, "PRAGMA foreign_keys = OFF", [])
    assert {:ok, %{rows: [[0]]}} = Connection.query(conn, "PRAGMA foreign_keys", [])

    assert :ok = Connection.reset_for_reuse(conn, :rw)
    assert {:ok, %{rows: [[1]]}} = Connection.query(conn, "PRAGMA foreign_keys", [])
    assert {:ok, %{rows: [_]}} = Connection.query(conn, sql, [1])
  end
end
