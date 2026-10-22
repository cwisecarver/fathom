defmodule Fathom.Shard.CheckinProbeTempTest do
  @moduledoc """
  The pooled-checkin probe must not open the connection's TEMP database (expert review 2026-10-21
  r7 #14).

  Symptom: `SELECT ... EXISTS(SELECT 1 FROM temp.sqlite_schema)` opens the temp btree/pager on a
  connection that never used it, +~95 KiB (+60%) per handle, invisible to every density arm; a
  handle re-lent within the pooled-idle shrink window, or a held stream from a pool hit, carries
  it. Invariants: (1) probing a handle that never touched temp leaves `fathom_cache_used()` flat;
  (2) a handle that did create a TEMP object is still refused by the pool (`{false, _}`), and one
  whose temp objects were dropped again is accepted (`{true, _}`).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  setup do
    path = Path.join(System.tmp_dir!(), "ckp_temp_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, conn: conn}
  end

  defp cache_used(conn) do
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT fathom_cache_used()", [])
    n
  end

  test "probing a handle that never used temp does not grow its page cache", %{conn: conn} do
    # Warm the statement the baseline read itself needs, so only the probe differs.
    _ = cache_used(conn)
    before = cache_used(conn)
    assert {true, true} = Connection.checkin_probe(conn)
    after_probe = cache_used(conn)

    assert after_probe - before < 1_024,
           "checkin_probe opened the temp database: #{before} -> #{after_probe} bytes"
  end

  test "a handle with a TEMP object is refused", %{conn: conn} do
    {:ok, _} = Connection.query(conn, "CREATE TEMP TABLE t(x)", [])
    assert {false, true} = Connection.checkin_probe(conn)
  end

  test "a handle whose temp objects were dropped is accepted", %{conn: conn} do
    {:ok, _} = Connection.query(conn, "CREATE TEMP TABLE t(x)", [])
    {:ok, _} = Connection.query(conn, "DROP TABLE temp.t", [])
    assert {true, true} = Connection.checkin_probe(conn)
  end
end
