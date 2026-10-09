defmodule Fathom.Shard.InternalDeadlineTest do
  @moduledoc """
  Expert review 2026-10-08 #11: fathom's own durability work must not run under the tenant
  statement deadline.

  `:query_timeout_ms` (30 s in prod) bounds CLIENT SQL. It applied to every `Connection.query/4`,
  and the on-thread backstop was armed on every handle, so the durability snapshot (`VACUUM INTO`)
  and the pre-drop `quick_check` were cut off too — verified by execution with a 20 ms deadline on
  a ~58 MB shard. In prod that is a shard large or I/O-starved enough that its `quick_check` passes
  30 s: it can then never complete a periodic flush, and its drop keeps the local copy instead of
  uploading it.

  A 1 ms deadline over a multi-MB shard is the same shape at test speed: each operation takes far
  longer than the deadline, so it fails if any deadline reaches it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Connection, Integrity}

  setup do
    dir = Path.join(System.tmp_dir!(), "internal_deadline_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "shard.db")

    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE t (a INTEGER, b TEXT)")

    :ok =
      Connection.exec(
        c,
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100000) " <>
          "INSERT INTO t SELECT i, hex(randomblob(64)) FROM n"
      )

    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)

    prev = Application.get_env(:fathom, :query_timeout_ms)
    Application.put_env(:fathom, :query_timeout_ms, 1)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :query_timeout_ms, prev),
        else: Application.delete_env(:fathom, :query_timeout_ms)

      File.rm_rf!(dir)
    end)

    %{dir: dir, path: path}
  end

  test "the durability snapshot completes under a deadline far shorter than it takes", %{
    dir: dir,
    path: path
  } do
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    snap = Path.join(dir, "snap.db")
    assert :ok = Fathom.Shard.do_snapshot(conn, snap)
    assert File.stat!(snap).size > 1_000_000, "fixture: the snapshot copied almost nothing"
  end

  test "the integrity check completes under the same deadline", %{path: path} do
    assert :ok = Integrity.verify(path)
  end

  test "client SQL on a tenant handle is still bounded by it", %{path: path} do
    {:ok, conn} = Connection.open(path, tenant?: true)
    on_exit(fn -> Connection.close(conn) end)

    heavy =
      "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 90000000) " <>
        "SELECT count(*) FROM c"

    assert {:error, :query_timeout} = Connection.query(conn, heavy, [], deadline: true)
  end
end
