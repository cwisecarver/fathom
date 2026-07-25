defmodule Fathom.QueryBoundsTest do
  @moduledoc """
  Per-query resource bounds (expert review 2026-07-14 #26): a statement timeout, a max-result-rows
  cap, and a per-shard concurrent-stream cap — so one tenant's runaway query can't pin memory or
  wedge the shard un-drainable. All config-gated and off by default; these exercise them ON.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "qb_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for key <- [:query_timeout_ms, :query_max_rows, :max_checkouts_per_shard],
          do: Application.delete_env(:fathom, key)

      Shards.drain(shard, 2_000)

      for dir <- [@local_dir, @remote_dir],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp seed!(shard, sqls) do
    {:ok, h} = ShardExecutor.open(shard)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(h, stmt(s)) end)
    :ok = ShardExecutor.close(h)
  end

  test "a query past the statement timeout is interrupted with a 503", %{shard: shard} do
    Application.put_env(:fathom, :query_timeout_ms, 50)
    {:ok, h} = ShardExecutor.open(shard)

    # A long recursive CTE (bounded high so it terminates if interrupt ever failed) — the watchdog
    # interrupts it well before it finishes.
    slow =
      "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x < 100000000) " <>
        "SELECT count(*) FROM r"

    assert {:error, %Filo.Error{status: 503, code: "FILO_QUERY_TIMEOUT"}} =
             ShardExecutor.execute(h, stmt(slow))

    :ok = ShardExecutor.close(h)
  end

  # Expert review 2026-07-18 #13: a raise inside with_deadline (Sqlite3.bind raises ArgumentError on
  # a bad bind value) skipped the watchdog-stop, orphaning the watchdog to interrupt a LATER
  # statement on the reused connection `ms` later — a spurious FILO_QUERY_TIMEOUT on a healthy
  # query. The try/after now always stops it. The precise late-interrupt timing isn't
  # deterministically reproducible with a single query_timeout config (the orphan and a query's own
  # watchdog fire near-simultaneously), so this covers the previously-untested raise-under-deadline
  # path: a bad bind returns an error, and the connection stays healthy across the orphan's fire.
  test "a raising query under a statement timeout keeps the connection healthy (no orphan watchdog)",
       %{shard: shard} do
    Application.put_env(:fathom, :query_timeout_ms, 30)
    seed!(shard, ["CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (1)"])

    {:ok, h} = ShardExecutor.open(shard)

    # A tuple arg exqlite can't bind raises ArgumentError inside with_deadline → error, not a crash.
    assert {:error, _} = ShardExecutor.execute(h, stmt("SELECT v FROM t WHERE v = ?", [{:bad}]))

    # Past the 30ms deadline, so any orphaned watchdog has fired; subsequent queries must succeed.
    Process.sleep(50)
    assert {:ok, %{rows: [[1]]}} = ShardExecutor.execute(h, stmt("SELECT v FROM t"))

    :ok = ShardExecutor.close(h)
  end

  # Expert review 2026-07-24 #1: `PRAGMA busy_timeout=N` routes to sqlite3_busy_timeout(), which
  # REPLACES exqlite's custom busy handler with SQLite's default — a bare sqlite3OsSleep loop that
  # observes neither the interrupt flag nor exqlite's cancel flag. So a query blocked on the write
  # lock could not be cut short by the watchdog's Sqlite3.interrupt: it slept the FULL busy timeout
  # (5s) pinning a dirty-IO scheduler thread, then returned SQLITE_BUSY — never FILO_QUERY_TIMEOUT.
  # The invariant pinned here: :query_timeout_ms bounds LOCK WAITS, not just VDBE execution.
  # Pre-fix this takes ~5s and returns a SQLITE_BUSY-shaped error; post-fix ~200ms and a timeout.
  test "a query blocked on another stream's write lock honors the statement timeout", %{
    shard: shard
  } do
    seed!(shard, ["CREATE TABLE t (v INTEGER)"])

    # Holder: a second stream on the same shard takes the write lock and keeps it.
    {:ok, holder} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(holder, stmt("BEGIN IMMEDIATE"))
    {:ok, _} = ShardExecutor.execute(holder, stmt("INSERT INTO t VALUES (1)"))

    Application.put_env(:fathom, :query_timeout_ms, 200)
    {:ok, blocked} = ShardExecutor.open(shard)

    {elapsed_us, result} =
      :timer.tc(fn -> ShardExecutor.execute(blocked, stmt("INSERT INTO t VALUES (2)")) end)

    assert {:error, %Filo.Error{status: 503, code: "FILO_QUERY_TIMEOUT"}} = result

    # The load-bearing half: it must give up at the DEADLINE, not at the 5s busy timeout.
    assert elapsed_us < 2_000_000,
           "blocked write took #{div(elapsed_us, 1000)}ms; the 200ms deadline did not cut the " <>
             "busy wait short (busy handler is not cancellable)"

    {:ok, _} = ShardExecutor.execute(holder, stmt("ROLLBACK"))
    :ok = ShardExecutor.close(blocked)
    :ok = ShardExecutor.close(holder)
  end

  test "a normal query is unaffected by a generous timeout", %{shard: shard} do
    Application.put_env(:fathom, :query_timeout_ms, 30_000)
    seed!(shard, ["CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (1), (2)"])

    {:ok, h} = ShardExecutor.open(shard)
    assert {:ok, res} = ShardExecutor.execute(h, stmt("SELECT v FROM t ORDER BY v"))
    assert res.rows == [[1], [2]]
    :ok = ShardExecutor.close(h)
  end

  test "a result past the row cap errors with a 400 instead of materializing it", %{shard: shard} do
    seed!(shard, [
      "CREATE TABLE t (v INTEGER)",
      "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x < 50) " <>
        "INSERT INTO t SELECT x FROM r"
    ])

    Application.put_env(:fathom, :query_max_rows, 10)
    {:ok, h} = ShardExecutor.open(shard)

    assert {:error, %Filo.Error{status: 400, code: "FILO_RESULT_TOO_LARGE"}} =
             ShardExecutor.execute(h, stmt("SELECT v FROM t"))

    # A result within the cap is fine.
    assert {:ok, res} = ShardExecutor.execute(h, stmt("SELECT v FROM t LIMIT 5"))
    assert length(res.rows) == 5
    :ok = ShardExecutor.close(h)
  end

  test "the per-shard concurrent-stream cap refuses an over-cap open with a 503", %{shard: shard} do
    Application.put_env(:fathom, :max_checkouts_per_shard, 1)

    # Hold one stream open (one checkout); the second open is refused.
    {:ok, h1} = ShardExecutor.open(shard)

    assert {:error, %Filo.Error{status: 503, code: "FILO_SHARD_BUSY"}} = ShardExecutor.open(shard)

    # Freeing the first slot lets a new stream open again.
    :ok = ShardExecutor.close(h1)
    assert {:ok, h2} = ShardExecutor.open(shard)
    :ok = ShardExecutor.close(h2)
  end
end
