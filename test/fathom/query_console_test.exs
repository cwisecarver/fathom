defmodule Fathom.QueryConsoleTest do
  @moduledoc """
  End-to-end coverage for the admin query console's front-door client
  (`Fathom.QueryConsole`, expert review 2026-07-14 #23).

  Every case drives a **real Hrana-over-HTTP request** through an in-process Filo
  listener (the same `Filo.Plug` + `Fathom.ShardExecutor` the production node
  runs), so it exercises the genuine client path — Host-subdomain routing,
  admission, the executor, the per-stream connection — not an internal side-door.

  Includes the AGENTS.md **shard-isolation gate**: this is a new request→shard
  caller, so a test proves a query addressed to shard A never resolves to or reads
  shard B's data.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.{QueryConsole, Shards}

  setup do
    {:ok, sup, port} = HranaClient.start_listener()
    endpoint = "http://127.0.0.1:#{port}"
    a = "qc_a_#{System.unique_integer([:positive])}"
    b = "qc_b_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for id <- [a, b] do
        Shards.drain(id, 5_000)
        rm_shard(id)
      end

      HranaClient.stop_listener(sup)
    end)

    {:ok, endpoint: endpoint, a: a, b: b}
  end

  test "round-trips a write and read through the front door", %{endpoint: ep, a: a} do
    assert {:ok, _} =
             QueryConsole.run(a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", endpoint: ep)

    assert {:ok, ins} = QueryConsole.run(a, "INSERT INTO t (v) VALUES ('hello')", endpoint: ep)
    assert ins.affected_row_count == 1
    # Hrana carries i64 (last_insert_rowid) as a string.
    assert ins.last_insert_rowid == "1"

    assert {:ok, sel} = QueryConsole.run(a, "SELECT id, v FROM t", endpoint: ep)
    assert sel.cols == ["id", "v"]
    assert sel.rows == [[1, "hello"]]
    assert sel.row_count == 1
    assert sel.truncated == false
    assert is_float(sel.latency_ms)
  end

  # Shard-isolation gate (AGENTS.md §Gates): a query addressed to A must never read B's file.
  test "a query to shard A never sees shard B's data", %{endpoint: ep, a: a, b: b} do
    for {id, tag} <- [{a, "A"}, {b, "B"}] do
      assert {:ok, _} = QueryConsole.run(id, "CREATE TABLE marker (who TEXT)", endpoint: ep)
      assert {:ok, _} = QueryConsole.run(id, "INSERT INTO marker VALUES ('#{tag}')", endpoint: ep)
    end

    assert {:ok, ra} = QueryConsole.run(a, "SELECT who FROM marker", endpoint: ep)
    assert {:ok, rb} = QueryConsole.run(b, "SELECT who FROM marker", endpoint: ep)

    # Each shard sees only its own tag — a routing leak A→B would show B's file
    # carrying A's row (or vice versa), or an over-count.
    assert ra.rows == [["A"]]
    assert rb.rows == [["B"]]
  end

  test "surfaces a Hrana error code instead of crashing the stream", %{endpoint: ep, a: a} do
    assert {:error, err} = QueryConsole.run(a, "SELECT * FROM does_not_exist", endpoint: ep)
    assert is_binary(err.code)
    assert is_binary(err.message) and err.message != ""
    assert is_float(err.latency_ms)
  end

  test "decodes native SQLite value types", %{endpoint: ep, a: a} do
    assert {:ok, r} =
             QueryConsole.run(a, "SELECT 42 AS i, 3.5 AS f, 'x' AS s, NULL AS n", endpoint: ep)

    assert r.cols == ["i", "f", "s", "n"]
    assert r.rows == [[42, 3.5, "x", nil]]
  end

  test "caps returned rows at :max_rows and flags truncation", %{endpoint: ep, a: a} do
    assert {:ok, _} = QueryConsole.run(a, "CREATE TABLE n (x INTEGER)", endpoint: ep)

    assert {:ok, _} =
             QueryConsole.run(
               a,
               "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 5) " <>
                 "INSERT INTO n (x) SELECT i FROM s",
               endpoint: ep
             )

    assert {:ok, r} = QueryConsole.run(a, "SELECT x FROM n ORDER BY x", endpoint: ep, max_rows: 2)
    assert length(r.rows) == 2
    assert r.row_count == 5
    assert r.truncated == true
  end

  defp rm_shard(id) do
    for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
        s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([dir, "#{id}.db"]) <> s)
    end
  end
end
