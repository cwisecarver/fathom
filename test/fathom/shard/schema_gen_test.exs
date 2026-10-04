defmodule Fathom.Shard.SchemaGenTest do
  @moduledoc """
  The stale cached COLUMN LIST across another connection's DDL (expert review 2026-08-26 #7).

  ## Why this is its own file, and NOT async

  These drive `Fathom.Shard.SchemaGen`, which is a NODE-GLOBAL counter by design (see its
  moduledoc for why per-shard would mean threading a shard id through `Connection`, which has
  callers with no shard identity at all). Any concurrently-running async test that executes DDL
  bumps the same counter.

  They started life in `connection_test.exs`, which is `async: true`, and the zero-row case failed
  once in a full-suite run while passing in isolation, under its own seed, and across five
  concurrent-DDL runs — i.e. a flake that could not be named. Rather than leave it, or paper over
  it with a retry, the tests moved to a file that does not share the counter with anything.

  `async: false` is the fix for the TEST, not for the product: the counter is meant to be shared,
  and over-invalidation across shards is explicitly the accepted trade.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  # Expert review 2026-08-26 #7, verified by execution in the audit and reproduced here.
  #
  # The statement cache stores the COLUMN LIST beside each prepared statement. `prepare_v2`
  # recompiles the statement transparently when the schema changes, so the statement survives DDL
  # — but the Elixir-side column list captured at prepare time does not, and nothing re-read it.
  # Review 2026-07-24 #17 purges the cache for DDL the SAME connection runs; DDL from a SIBLING
  # stream on the same shard was invisible.
  #
  # The result goes on the wire as a Filo.StmtResult, so a client zipping cols to values silently
  # mis-maps columns — Django's cursor.description reports 2 fields for a 3-wide row.
  test "a cached SELECT * re-reads its columns after ANOTHER connection's DDL" do
    path = Path.join(System.tmp_dir!(), "schemagen_#{System.unique_integer([:positive])}.db")

    {:ok, a} = Connection.open(path)
    {:ok, b} = Connection.open(path)

    on_exit(fn ->
      Connection.close(a)
      Connection.close(b)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    :ok = Connection.exec(a, "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")
    :ok = Connection.exec(a, "INSERT INTO t VALUES (1, 'hello')")

    # B caches `SELECT *` — two columns, two values.
    assert {:ok, %{columns: ["id", "x"], rows: [[1, "hello"]]}} =
             Connection.query(b, "SELECT * FROM t", [])

    # A changes the schema. In production this is a sibling Hrana stream, which is why B's own
    # purge-on-DDL cannot help.
    :ok = Connection.exec(a, "ALTER TABLE t ADD COLUMN y TEXT DEFAULT ('0')")

    # ShardExecutor bumps the generation on DDL; Connection.exec/2 is below that layer, so the
    # bump is explicit here. This is the seam the executor drives in production.
    Fathom.Shard.SchemaGen.bump()

    # THE ASSERTION. Pre-fix this returned ["id", "x"] with THREE values per row.
    assert {:ok, %{columns: cols, rows: [row]}} = Connection.query(b, "SELECT * FROM t", [])

    assert length(cols) == length(row),
           "the cached column list (#{inspect(cols)}) does not match the row width " <>
             "(#{inspect(row)}) — a client zipping the two silently mis-maps every column"

    assert cols == ["id", "x", "y"]
    assert row == [1, "hello", "0"]
  end

  # A DDL THAT CHANGED NOTHING MUST NOT INVALIDATE THE NODE (expert review 2026-10-01 perf #9).
  # The generation is node-global, so every bump makes every cached statement on every stream of
  # every tenant re-prepare on its next use. The executor bumped on ANY statement that LOOKS like
  # DDL, including `CREATE TABLE IF NOT EXISTS` on a table that exists and `CREATE TEMP TABLE`,
  # which is connection-local and invisible to every other connection. Frameworks issue both on
  # every boot or request, so one such tenant kept the whole node's statement cache cold.
  test "only DDL that changes the main schema bumps the node-wide generation" do
    id = "schemagen_exec_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Fathom.Shards.stop(id)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    {:ok, conn} = Fathom.ShardExecutor.open(id)
    on_exit(fn -> Fathom.ShardExecutor.close(conn) end)

    run = fn sql ->
      assert {:ok, _} = Fathom.ShardExecutor.execute(conn, %Filo.Stmt{sql: sql, args: []})
    end

    run.("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    before = Fathom.Shard.SchemaGen.current()

    run.("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
    run.("CREATE INDEX IF NOT EXISTS t_id ON t (id)")
    after_index = Fathom.Shard.SchemaGen.current()
    assert after_index > before, "a CREATE INDEX that did create one must still bump"

    run.("CREATE INDEX IF NOT EXISTS t_id ON t (id)")
    run.("DROP TABLE IF EXISTS no_such_table")
    run.("CREATE TEMP TABLE scratch (a INTEGER)")

    assert Fathom.Shard.SchemaGen.current() == after_index,
           "a DDL statement that left the main schema unchanged bumped the node-wide generation, " <>
             "forcing every cached statement on the node to re-prepare"

    run.("ALTER TABLE t ADD COLUMN y TEXT")
    assert Fathom.Shard.SchemaGen.current() > after_index, "a real schema change must bump"
  end

  # The zero-row case, which is why the tempting `length(hd(rows)) != length(cols)` guard was NOT
  # used: there is no row to measure, and the reported columns are still wrong.
  test "the stale-column fix also covers a SELECT * that returns no rows" do
    path = Path.join(System.tmp_dir!(), "schemagen0_#{System.unique_integer([:positive])}.db")

    {:ok, a} = Connection.open(path)
    {:ok, b} = Connection.open(path)

    on_exit(fn ->
      Connection.close(a)
      Connection.close(b)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    :ok = Connection.exec(a, "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")

    assert {:ok, %{columns: ["id", "x"], rows: []}} = Connection.query(b, "SELECT * FROM t", [])

    :ok = Connection.exec(a, "ALTER TABLE t ADD COLUMN y TEXT")
    Fathom.Shard.SchemaGen.bump()

    assert {:ok, %{columns: ["id", "x", "y"], rows: []}} =
             Connection.query(b, "SELECT * FROM t", []),
           "an empty SELECT * still reported the pre-DDL columns"
  end

  # DDL INSIDE AN EXPLICIT TRANSACTION (expert review 2026-10-01 perf #9, verified 2026-10-04). The
  # executor bumped the generation at the ALTER, BEFORE the COMMIT. A sibling reading in that window
  # cached the still-committed column list under the NEW generation, and the COMMIT bumped nothing,
  # so the sibling kept it: columns ["id", "x"] over rows [1, "hello", "0"] — the 2026-08-26 #7
  # mis-mapping, back through a transaction. Driven through the executor, which owns the bump.
  test "a sibling sees the columns a DDL transaction added once it commits" do
    shard = "schemagen_txn_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for base <- [
            Path.join([Fathom.Shard.data_dir(), "#{shard}.db"]),
            Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])
          ],
          suffix <- ["", "-wal", "-shm"],
          do: File.rm(base <> suffix)
    end)

    stmt = fn sql -> %Filo.Stmt{sql: sql, args: []} end
    {:ok, a} = Fathom.ShardExecutor.open(shard)
    {:ok, b} = Fathom.ShardExecutor.open(shard)

    {:ok, _} =
      Fathom.ShardExecutor.execute(a, stmt.("CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)"))

    {:ok, _} = Fathom.ShardExecutor.execute(a, stmt.("INSERT INTO t VALUES (1, 'hello')"))
    {:ok, _} = Fathom.ShardExecutor.execute(b, stmt.("SELECT * FROM t"))

    {:ok, _} = Fathom.ShardExecutor.execute(a, stmt.("BEGIN"))

    {:ok, _} =
      Fathom.ShardExecutor.execute(a, stmt.("ALTER TABLE t ADD COLUMN y TEXT DEFAULT ('0')"))

    # The sibling reads INSIDE the window: the change is not committed, so two columns is right.
    assert {:ok, %Filo.StmtResult{cols: mid_cols, rows: [mid_row]}} =
             Fathom.ShardExecutor.execute(b, stmt.("SELECT * FROM t"))

    assert length(mid_cols) == length(mid_row) and length(mid_row) == 2,
           "precondition: the sibling did not read the pre-commit schema inside the window"

    {:ok, _} = Fathom.ShardExecutor.execute(a, stmt.("COMMIT"))

    assert {:ok, %Filo.StmtResult{cols: cols, rows: [row]}} =
             Fathom.ShardExecutor.execute(b, stmt.("SELECT * FROM t"))

    assert length(cols) == length(row),
           "columns #{inspect(cols)} over row #{inspect(row)}: the sibling kept the column list " <>
             "it cached inside the DDL transaction"

    :ok = Fathom.ShardExecutor.close(a)
    :ok = Fathom.ShardExecutor.close(b)
  end
end
