defmodule Fathom.Shard.ConnectionTest do
  @moduledoc """
  The per-stream exqlite connection wrapper. `Connection` is pure over a SQLite file path
  (no coordinator, registry, or Postgres), so these are plain unit tests over a temp file.

  Focus: `collect/3`'s result assembly. Phase-0 fix (docs/tpc-benchmark-plan.md) replaced the
  O(R²) `acc ++ rows` with an O(R) batch-accumulate + reverse + concat — so both the ordering
  invariant (batches must reassemble in arrival order) and the large-result cost are pinned here.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Connection

  setup do
    path = Path.join(System.tmp_dir!(), "conn_test_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

    on_exit(fn ->
      Connection.close(conn)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    %{conn: conn}
  end

  # Bulk-seed n rows in one statement via a recursive CTE. n is a test constant (never user
  # input), so inlining it is the same harness-controlled idiom the bench seeders use.
  defp seed(conn, n) do
    :ok =
      Connection.exec(conn, """
      WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < #{n})
      INSERT INTO t (id, v) SELECT i, 'row-' || i FROM seq
      """)
  end

  test "a large result reassembles in order with every row preserved", %{conn: conn} do
    # 20k rows spans hundreds of multi_step batches, so this exercises the batch-boundary
    # reassembly: a wrong reverse/concat would drop rows or reorder across batches.
    n = 20_000
    seed(conn, n)

    {:ok, %{columns: cols, rows: rows}} =
      Connection.query(conn, "SELECT id, v FROM t ORDER BY id", [])

    assert cols == ["id", "v"]
    assert length(rows) == n
    assert List.first(rows) == [1, "row-1"]
    assert List.last(rows) == [n, "row-#{n}"]
    # A mid-stream spot check pins ordering across a batch boundary (not just the ends).
    assert Enum.at(rows, 12_344) == [12_345, "row-12345"]
    # No dupes / gaps: the id column is exactly 1..n in order.
    assert Enum.map(rows, &hd/1) == Enum.to_list(1..n)
  end

  test "empty and single-row results are correct", %{conn: conn} do
    assert {:ok, %{rows: []}} = Connection.query(conn, "SELECT id FROM t", [])
    seed(conn, 1)
    assert {:ok, %{rows: [[1, "row-1"]]}} = Connection.query(conn, "SELECT id, v FROM t", [])
  end

  test "opens WAL + synchronous=FULL for per-commit local durability", %{conn: conn} do
    # Pin the durability choice: PRAGMA synchronous returns 2 (FULL), which fsyncs the WAL on
    # every commit. It's ~free for fathom's sharded, wire-bound model (see
    # docs/reviews/competitive-oltp-2026-07-10.md); the guarantee is the point, so a regression to
    # NORMAL (1) should trip this.
    assert {:ok, %{rows: [[2]]}} = Connection.query(conn, "PRAGMA synchronous", [])
    assert {:ok, %{rows: [["wal"]]}} = Connection.query(conn, "PRAGMA journal_mode", [])
  end

  # Expert review 2026-07-14 #2: SQLite defaults foreign_keys=OFF, but Django (>=2.2) assumes ON and
  # can't be relied on to replay `PRAGMA foreign_keys=ON` on every transparently re-created stream.
  # Without the server-side default a bad-FK insert SILENTLY succeeds (orphan rows) and
  # on_delete=CASCADE/PROTECT stops being enforced. Pins enforcement-on-by-default; pre-fix the
  # insert returns {:ok, _} and this fails.
  test "foreign keys are enforced by default on a fresh connection", %{conn: conn} do
    :ok = Connection.exec(conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))"
      )

    # pid 999 has no parent row → FK violation.
    assert {:error, _} = Connection.query(conn, "INSERT INTO child (id, pid) VALUES (1, 999)", [])
  end

  @tag :bench
  test "large-result collect stays O(R), not O(R²)", %{conn: conn} do
    # Regression guard for the Phase-0 fix. The old `acc ++ rows` copied the whole accumulator
    # on every multi_step batch → O(R²/batch); measured at 1385 ms for 200k rows here. The O(R)
    # batch-accumulate + concat assembles the same 200k rows in ~tens of ms. Order-of-magnitude
    # ceiling (per AGENTS.md hot-path guidance), not an exact latency: pre-fix blows 1s (verified
    # by reverting collect), the fix clears it by ~20×.
    n = 200_000
    seed(conn, n)

    {us, {:ok, %{rows: rows}}} =
      :timer.tc(fn -> Connection.query(conn, "SELECT id FROM t", []) end)

    assert length(rows) == n
    assert us < 1_000_000, "collect of #{n} rows took #{div(us, 1000)}ms — O(R²) regression?"
  end
end
