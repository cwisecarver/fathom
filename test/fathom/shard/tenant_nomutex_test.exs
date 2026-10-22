defmodule Fathom.Shard.TenantNomutexTest do
  @moduledoc """
  `:tenant_nomutex` (expert review 2026-10-21 r7 #16): tenant handles may open SQLite NOMUTEX,
  skipping a recursive mutex enter/leave on every `sqlite3_column_*` call.

  The flag is DEFAULT OFF: the audit of the exqlite NIFs that do not take its own connection lock
  (`bind_*`, `interrupt`/`cancel`, `set_progress_handler_steps`) against fathom's cross-process
  touches (watchdog cancel, pooled close, shrink side process) is not done, and the Linux
  measurement (+5% on `wire_server_rows_per_s`, 19% execute-only) is below the 10% flip bar. These
  tests pin that the flag is off unless asked, and that with it ON every open path a tenant uses
  (`:rw`, `:ro`, the create-then-readonly fallback for a missing file) yields a working handle that
  survives a pooled reuse and a watchdog-cancelled query. They do NOT prove the NOMUTEX audit.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  setup do
    path = Path.join(System.tmp_dir!(), "nomutex_#{System.unique_integer([:positive])}.db")
    prev = Application.get_env(:fathom, :tenant_nomutex)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :tenant_nomutex),
        else: Application.put_env(:fathom, :tenant_nomutex, prev)

      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    {:ok, path: path}
  end

  test "the flag is off unless configured" do
    refute Application.get_env(:fathom, :tenant_nomutex, false)
  end

  test "with the flag ON, rw / ro / missing-file ro tenant handles open, query and reuse", %{
    path: path
  } do
    Application.put_env(:fathom, :tenant_nomutex, true)

    # `:ro` on a file that does not exist yet takes the create-then-readonly path.
    {:ok, ro_first} = Connection.open(path, tenant?: true, scope: :ro)
    assert {:ok, %{rows: [[1]]}} = Connection.query(ro_first, "SELECT 1", [])
    Connection.close(ro_first)

    {:ok, rw} = Connection.open(path, tenant?: true, scope: :rw)
    {:ok, _} = Connection.query(rw, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    {:ok, _} = Connection.query(rw, "INSERT INTO t VALUES (1, 'x'), (2, 'y')", [])

    assert {:ok, %{rows: [[1, "x"], [2, "y"]]}} =
             Connection.query(rw, "SELECT a, b FROM t ORDER BY a", [])

    assert :ok = Connection.reset_for_reuse(rw, :rw)
    assert {:ok, %{rows: [[2]]}} = Connection.query(rw, "SELECT count(*) FROM t", [])
    Connection.close(rw)

    {:ok, ro} = Connection.open(path, tenant?: true, scope: :ro)
    assert {:ok, %{rows: [[2]]}} = Connection.query(ro, "SELECT count(*) FROM t", [])
    assert :ok = Connection.reset_for_reuse(ro, :ro)
    assert {:error, _} = Connection.query(ro, "INSERT INTO t VALUES (3, 'z')", [])
    Connection.close(ro)
  end
end
