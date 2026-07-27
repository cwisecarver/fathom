defmodule Fathom.QueryBoundsTest do
  @moduledoc """
  Per-query resource bounds (expert review 2026-07-14 #26): a statement timeout, a max-result-rows
  cap, and a per-shard concurrent-stream cap — so one tenant's runaway query can't pin memory or
  wedge the shard un-drainable. All config-gated and off by default; these exercise them ON.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    shard = "qb_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for key <- [
            :query_timeout_ms,
            :query_max_rows,
            :max_checkouts_per_shard,
            :shard_cache_size_kb
          ],
          do: Application.delete_env(:fathom, key)

      Shards.drain(shard, 2_000)

      for dir <- [local_dir(), remote_dir()],
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

  # Expert review 2026-07-24 #8: the prepared-statement cache keyed entries by the caller's SQL
  # binary. filo reads a whole HTTP body / WS frame into one binary and Jason.decode's it, and Jason
  # extracts strings with binary_part/3 — so on any body >64 bytes the "sql" field is a SUB-BINARY
  # referencing the entire request body, and ERTS copies a >=64-byte sub-binary across a process
  # boundary as a reference rather than flattening it. Stashing it as a cache key therefore pinned
  # the whole originating body for the entry's life: up to 64 bodies per stream, freed only on the
  # holder's GC, and invisible in the density numbers because it lands in :erlang.memory(:binary)
  # rather than the process heap.
  #
  # Reproduced at the Connection level (the Hrana stack above it only supplies the sub-binary).
  # Pre-fix the 1 MiB parent stays referenced after GC; post-fix only the small copy survives.
  test "the statement cache does not pin the binary its SQL was sliced from", %{shard: shard} do
    path = Path.join(local_dir(), "#{shard}.db")
    File.mkdir_p!(local_dir())
    {:ok, conn} = Fathom.Shard.Connection.open(path)

    # Must be >=64 bytes or ERTS copies the slice onto the process heap instead of making a
    # sub-binary — which is precisely why this finding is about real ORM SQL. A Django
    # `SELECT "app_model"."id", ... FROM ... WHERE ...` is comfortably past that threshold.
    sql_text =
      "SELECT 1 AS a_deliberately_long_column_alias_so_this_statement_exceeds_the_64_byte_threshold"

    pad = :binary.copy("x", 1024 * 1024)
    body = pad <> sql_text <> pad

    # A sub-binary over `body`, exactly as Jason would hand back — not a copy.
    sql = :binary.part(body, byte_size(pad), byte_size(sql_text))

    # A sub-binary references its whole parent (ERTS over-allocates on append, so this is >= the
    # body's logical size, not equal to it).
    assert :binary.referenced_byte_size(sql) > 1_000_000,
           "precondition: sql must be a sub-binary holding a large parent, not a heap copy"

    {:ok, _} = Fathom.Shard.Connection.query(conn, sql, [])

    # Drop every local reference to the big parent; only the cache could still hold it.
    body = nil
    pad = nil
    sql = nil
    _ = {body, pad, sql}
    :erlang.garbage_collect()

    {:binary, refs} = Process.info(self(), :binary)
    largest = refs |> Enum.map(fn {_id, size, _refc} -> size end) |> Enum.max(fn -> 0 end)

    assert largest < 512 * 1024,
           "the statement cache still pins a #{div(largest, 1024)} KiB binary — the SQL key was " <>
             "not copied, so it holds the whole Hrana request body it was sliced from"

    :ok = Fathom.Shard.Connection.close(conn)
    File.rm(path)
  end

  # Expert review 2026-07-24 #17: column names are now cached alongside the prepared statement, so
  # they are read once at prepare instead of once per execution (Sqlite3.columns/2 is a dirty-IO
  # NIF dispatch plus a fresh binary per column, every time).
  #
  # THE HAZARD that makes this need invalidation: prepare_v2 transparently recompiles a cached
  # statement on a schema change, so the STATEMENT stays valid — but the cached column list does
  # not. Without the DDL purge, `SELECT *` after `ALTER TABLE ... ADD COLUMN` would report the
  # pre-ALTER columns while returning post-ALTER rows: a silently wrong result shape, which is
  # worse than the per-query cost being saved.
  test "a cached SELECT * reflects a column added by later DDL", %{shard: shard} do
    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (a INTEGER)"))
    {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t VALUES (1)"))

    # Prime the cache: this exact SQL now has its columns memoized.
    assert {:ok, %{cols: ["a"]}} = ShardExecutor.execute(h, stmt("SELECT * FROM t"))

    {:ok, _} = ShardExecutor.execute(h, stmt("ALTER TABLE t ADD COLUMN b TEXT"))

    assert {:ok, %{cols: ["a", "b"]}} = ShardExecutor.execute(h, stmt("SELECT * FROM t")),
           "the cached column list went stale across DDL — rows carry the new column but the " <>
             "reported shape does not"

    :ok = ShardExecutor.close(h)
  end

  # Expert review 2026-07-24 #18: last_insert_rowid was fetched on EVERY statement and then
  # discarded for every read-only one. It is ERL_NIF_DIRTY_JOB_IO_BOUND — a full dirty-scheduler
  # dispatch — so on a plain SELECT that was ~25% of the query's dirty-IO traffic spent on a value
  # nobody reads. The gate must not change what a client sees, which is what this pins: a write
  # still reports its rowid, and a read still reports none.
  test "the rowid is reported for writes and omitted for reads", %{shard: shard} do
    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))

    assert {:ok, %{last_insert_rowid: rowid}} =
             ShardExecutor.execute(h, stmt("INSERT INTO t (v) VALUES ('a')"))

    assert is_integer(rowid) and rowid > 0, "an INSERT must still report its rowid"

    # RETURNING has columns AND mutates, so it must keep reporting a rowid too.
    assert {:ok, %{last_insert_rowid: r2}} =
             ShardExecutor.execute(h, stmt("INSERT INTO t (v) VALUES ('b') RETURNING id"))

    assert is_integer(r2) and r2 > rowid

    # A read on the same connection, where sqlite3_last_insert_rowid still holds the write's value.
    assert {:ok, %{last_insert_rowid: nil}} = ShardExecutor.execute(h, stmt("SELECT v FROM t")),
           "a read must not surface the previous write's rowid"

    :ok = ShardExecutor.close(h)
  end

  # Expert review 2026-07-24 #29: the per-connection page cache was the one resource with no
  # declared ceiling. SQLite defaults to -2000 (~2 MiB PER CONNECTION) and fathom holds one
  # connection per Hrana stream for the stream's life, so at 30k held streams the tail is ~60 GB —
  # and none of the measured density regimes ever approached it, because their touched page set is
  # small. Those figures were a sample, not a bound.
  #
  # The default is deliberately SQLite's own value, so this is a knob and a declared ceiling rather
  # than a reduction — lowering it trades RAM for page re-reads on scan-heavy tenants, which is a
  # deployment decision.
  test "the page cache is declared at SQLite's default and is tunable", %{shard: shard} do
    {:ok, h} = ShardExecutor.open(shard)

    assert {:ok, %{rows: [[-2000]]}} = ShardExecutor.execute(h, stmt("PRAGMA cache_size")),
           "the shipped default must be byte-identical to SQLite's, so this changes no measurement"

    :ok = ShardExecutor.close(h)

    Application.put_env(:fathom, :shard_cache_size_kb, 500)
    {:ok, h2} = ShardExecutor.open(shard)

    # Negative = KiB in SQLite's convention. The sign is forced by the code, not the operator, so a
    # value can never be misread as a page count.
    assert {:ok, %{rows: [[-500]]}} = ShardExecutor.execute(h2, stmt("PRAGMA cache_size"))

    :ok = ShardExecutor.close(h2)
    Application.delete_env(:fathom, :shard_cache_size_kb)
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

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
