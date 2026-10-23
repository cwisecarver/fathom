defmodule Fathom.Shard.NifDispatchTest do
  @moduledoc """
  Pins the number of DIRTY-scheduler NIF dispatches per statement and per fresh open (perf review
  2026-10-14 #7 and #21), and the invariants the removed dispatches must not break.

  Every `Sqlite3` NIF counted here is a dirty-scheduler hand-off. Before this change a point SELECT
  cost 3 (`multi_step`, `changes`, `reset`), a write 4 (`multi_step`, `changes`,
  `last_insert_rowid`, `reset`), a BEGIN/COMMIT 4, and a fresh tenant `:rw` open 23. The counts are
  DETERMINISTIC, so they are exact assertions rather than timings.

  The skipped calls were redundant for these reasons (each is tested below):

    * `reset`: exqlite's `multi_step` resets on DONE, BUSY and every error. Only a row-cap abort
      mid-statement and an exception need a manual reset. Proven by a second handle's
      `wal_checkpoint(TRUNCATE)` after every outcome: a statement left un-reset holds a read mark
      on the WAL and the TRUNCATE reports busy=1.
    * `changes` for a read-verb statement that returns columns: `wrote?/3` answers `false` and
      `to_stmt_result/2` reports 0 before either looks at it.
    * `last_insert_rowid` when `changes == 0`: `to_stmt_result/2` reports it only for `changes > 0`.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.ShardExecutor
  alias Fathom.Test.NifDispatchCounter, as: Counter
  alias Filo.{Stmt, StmtResult}

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp count(fun) do
    {result, counts} = Counter.measure(fun)
    {result, counts.total, counts.by_fun}
  end

  describe "per-statement dispatches through the Hrana executor" do
    setup do
      shard = "test_nifd_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        for base <- [
              Path.join(Fathom.Shard.data_dir(), "#{shard}.db"),
              Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}.db")
            ],
            suffix <- ["", "-wal", "-shm"],
            do: File.rm(base <> suffix)
      end)

      {:ok, conn} = ShardExecutor.open(shard)

      {:ok, _} =
        ShardExecutor.execute(
          conn,
          stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        )

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'a')"))
      on_exit(fn -> ShardExecutor.close(conn) end)
      %{conn: conn, shard: shard}
    end

    test "a point SELECT is exactly 1 dirty dispatch (was 3)", %{conn: conn} do
      sel = stmt("SELECT v FROM kv WHERE k = ?", [1])
      assert {:ok, _} = ShardExecutor.execute(conn, sel)

      {res, total, by_fun} = count(fn -> ShardExecutor.execute(conn, sel) end)

      assert {:ok, %StmtResult{rows: [["a"]], affected_row_count: 0, last_insert_rowid: nil}} =
               res

      assert {total, by_fun} == {1, %{multi_step: 1}}
    end

    # expert review 2026-10-17 #3: with replication ON, `ships_now?/3` evaluated
    # `Connection.autocommit?/1` (a dirty `transaction_status` NIF) before the cheap
    # `wrote? or ...` operand, so every read paid a second dispatch in prod (count was 2).
    test "a point SELECT is still exactly 1 dirty dispatch with replication ON", %{conn: conn} do
      prev = Application.get_env(:fathom, :replication_enabled)
      Application.put_env(:fathom, :replication_enabled, true)

      on_exit(fn ->
        if prev == nil,
          do: Application.delete_env(:fathom, :replication_enabled),
          else: Application.put_env(:fathom, :replication_enabled, prev)
      end)

      sel = stmt("SELECT v FROM kv WHERE k = ?", [1])
      assert {:ok, _} = ShardExecutor.execute(conn, sel)

      {res, total, by_fun} = count(fn -> ShardExecutor.execute(conn, sel) end)

      assert {:ok, %StmtResult{rows: [["a"]]}} = res
      assert {total, by_fun} == {1, %{multi_step: 1}}
    end

    test "SELECT after a write on the same handle still reports 0 changes / nil rowid", %{
      conn: conn
    } do
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (2, 'b')"))

      for sql <- [
            "SELECT v FROM kv WHERE k = 2",
            "VALUES (1), (2)",
            "EXPLAIN SELECT v FROM kv"
          ] do
        assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil, rows_written: 0}} =
                 ShardExecutor.execute(conn, stmt(sql))
      end
    end

    test "an INSERT is exactly 3 dirty dispatches (was 4) and reports changes + rowid", %{
      conn: conn
    } do
      ins = stmt("INSERT INTO kv VALUES (?, ?)", [2, "b"])
      # Warm the statement cache so the count is the steady state, not the first prepare.
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?, ?)", [3, "c"]))

      {res, total, by_fun} = count(fn -> ShardExecutor.execute(conn, ins) end)

      assert {:ok, %StmtResult{affected_row_count: 1, last_insert_rowid: 2, rows_written: 1}} =
               res

      assert {total, by_fun} == {3, %{multi_step: 1, changes: 1, last_insert_rowid: 1}}
    end

    test "BEGIN / COMMIT are exactly 1 dispatch each (was 4; 2 before expert review 2026-10-20-2027 #1)",
         %{
           shard: shard
         } do
      # A FRESH handle: sqlite3_changes() is 0 here. (On the writing handle a BEGIN inherits the
      # last write's changes()>0 and reports it, exactly as before this change.)
      {:ok, fresh} = ShardExecutor.open(shard)
      on_exit(fn -> ShardExecutor.close(fresh) end)

      {:ok, _} = ShardExecutor.execute(fresh, stmt("BEGIN"))
      {:ok, _} = ShardExecutor.execute(fresh, stmt("COMMIT"))

      {res, total, by_fun} = count(fn -> ShardExecutor.execute(fresh, stmt("BEGIN")) end)
      assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil}} = res
      assert {total, by_fun} == {1, %{multi_step: 1}}

      {res, total, _} = count(fn -> ShardExecutor.execute(fresh, stmt("COMMIT")) end)
      assert {:ok, %StmtResult{last_insert_rowid: nil}} = res
      assert total == 1
    end

    # expert review 2026-10-20-2027 #1: sqlite3_changes()/last_insert_rowid() keep the PREVIOUS
    # write's values across a control statement, so after `INSERT` of 3 rows a BEGIN / SAVEPOINT /
    # RELEASE / COMMIT reported affected_row_count 3 + a rowid on the wire, and paid `changes`
    # (+ `last_insert_rowid`) dirty dispatches to read them. Invariant: a transaction-control
    # statement reports 0 / nil and costs exactly its `multi_step`.
    test "control statements after a write report 0 / nil and cost 1 dispatch each", %{conn: conn} do
      {:ok, _} =
        ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (11, 'x'), (12, 'y'), (13, 'z')"))

      seq = ["BEGIN", "SAVEPOINT sp1", "RELEASE sp1", "ROLLBACK", "BEGIN", "COMMIT"]
      write = stmt("INSERT OR REPLACE INTO kv VALUES (11, 'x')")

      # One warm lap so every statement is in the cache: the count is the steady state.
      for sql <- seq do
        {:ok, _} = ShardExecutor.execute(conn, stmt(sql))
        {:ok, _} = ShardExecutor.execute(conn, write)
      end

      for sql <- seq do
        {res, total, by_fun} = count(fn -> ShardExecutor.execute(conn, stmt(sql)) end)

        assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil, rows_written: 0}} =
                 res,
               "#{sql} leaked the previous write's counters"

        assert {sql, total, by_fun} == {sql, 1, %{multi_step: 1}}
        # A write between control statements re-arms the inherited counters.
        {:ok, _} = ShardExecutor.execute(conn, write)
      end
    end

    # Expert review 2026-10-21 r7 #21. A PRAGMA never sets `sqlite3_changes()`, so a PRAGMA after
    # a write inherited the previous statement's counters on the wire ("PRAGMA foreign_keys = ON
    # changed 3 rows, rowid 13") and paid the `changes` + `last_insert_rowid` dispatches to read
    # them. Invariant: a PRAGMA (assignment or query) reports 0 / nil and costs its `multi_step`;
    # a DURABLE pragma still dirties the shard (classification is decided before `changes > 0`).
    test "a PRAGMA after a write reports 0 / nil and costs 1 dispatch", %{conn: conn} do
      write = stmt("INSERT OR REPLACE INTO kv VALUES (11, 'x')")
      assert {:ok, %StmtResult{affected_row_count: 1}} = ShardExecutor.execute(conn, write)

      for sql <- ["PRAGMA foreign_keys = ON", "PRAGMA user_version = 7"] do
        # Warm so the count is the steady state.
        {:ok, _} = ShardExecutor.execute(conn, stmt(sql))
        {:ok, _} = ShardExecutor.execute(conn, write)

        {res, total, by_fun} = count(fn -> ShardExecutor.execute(conn, stmt(sql)) end)

        assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil}} = res,
               "#{sql} leaked the previous write's counters"

        assert {sql, total, by_fun} == {sql, 1, %{multi_step: 1}}
        {:ok, _} = ShardExecutor.execute(conn, write)
      end
    end

    # Expert review 2026-10-21 r7 #18. `SchemaGen` is node-global, so ANOTHER shard's DDL moves
    # the generation for every cached statement on this connection even though this shard's schema
    # cookie did not. The stale-hit branch used to release + re-prepare + `columns` + memused-probe
    # (5-8 dirty dispatches and a parse per statement). SQLite's prepare_v3 statements recompile
    # themselves at step when THEIR schema moved, so the branch now keeps the statement and only
    # re-reads the columns after the step: `multi_step` + `columns` = 2. The same-shard pin
    # (2026-08-26 #7) proves the post-step read is still right when the schema DID change.
    test "another shard's DDL costs a cached statement 2 dispatches, not a re-prepare", %{
      conn: conn
    } do
      other = "test_nifd_other_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        for base <- [
              Path.join(Fathom.Shard.data_dir(), "#{other}.db"),
              Path.join(Fathom.Shard.Storage.Local.dir(), "#{other}.db")
            ],
            suffix <- ["", "-wal", "-shm"],
            do: File.rm(base <> suffix)
      end)

      {:ok, a} = ShardExecutor.open(other)
      on_exit(fn -> ShardExecutor.close(a) end)

      select = stmt("SELECT k, v FROM kv WHERE k = 1")
      {:ok, _} = ShardExecutor.execute(conn, select)
      {:ok, _} = ShardExecutor.execute(conn, select)

      {:ok, _} = ShardExecutor.execute(a, stmt("CREATE TABLE unrelated (x)"))

      {res, total, by_fun} = count(fn -> ShardExecutor.execute(conn, select) end)

      assert {:ok, %StmtResult{cols: [_, _]}} = res
      assert {total, by_fun} == {2, %{multi_step: 1, columns: 1}}

      # The refresh is one-shot: the next execution is the ordinary 1-dispatch hit.
      {_, total, _} = count(fn -> ShardExecutor.execute(conn, select) end)
      assert total == 1
    end

    test "INSERT ... RETURNING keeps affected_row_count and last_insert_rowid", %{conn: conn} do
      assert {:ok,
              %StmtResult{
                cols: ["k"],
                rows: [[7]],
                affected_row_count: 1,
                last_insert_rowid: 7,
                rows_written: 1
              }} =
               ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (7, 'g') RETURNING k"))
    end

    test "UPSERT (DO UPDATE and DO NOTHING) reports the right counters", %{conn: conn} do
      assert {:ok, %StmtResult{affected_row_count: 1, last_insert_rowid: 1}} =
               ShardExecutor.execute(
                 conn,
                 stmt(
                   "INSERT INTO kv VALUES (1, 'z') ON CONFLICT(k) DO UPDATE SET v = excluded.v"
                 )
               )

      # DO NOTHING changes no row: 0 affected, and the (discarded) rowid is not fetched at all.
      # (Run once first so the count is the cached steady state, not the prepare.)
      {:ok, _} =
        ShardExecutor.execute(
          conn,
          stmt("INSERT INTO kv VALUES (1, 'q') ON CONFLICT(k) DO NOTHING")
        )

      {res, total, by_fun} =
        count(fn ->
          ShardExecutor.execute(
            conn,
            stmt("INSERT INTO kv VALUES (1, 'q') ON CONFLICT(k) DO NOTHING")
          )
        end)

      assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil}} = res
      assert by_fun == %{multi_step: 1, changes: 1}
      assert total == 2
    end

    test "a trigger's writes do not leak into the statement's counters", %{conn: conn} do
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE log (id INTEGER PRIMARY KEY)"))

      {:ok, _} =
        ShardExecutor.execute(
          conn,
          stmt(
            "CREATE TRIGGER kv_ai AFTER INSERT ON kv BEGIN INSERT INTO log VALUES (NULL); " <>
              "INSERT INTO log VALUES (NULL); END"
          )
        )

      assert {:ok, %StmtResult{affected_row_count: 1, last_insert_rowid: 9}} =
               ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (9, 'i')"))
    end

    test "WITH ... INSERT ... RETURNING still dirties the shard (changes is not skipped)", %{
      conn: conn,
      shard: shard
    } do
      before = Fathom.Shard.WriteCounter.count(shard)

      # (affected_row_count is 0 for this shape both before and after this change: `dml?/1` is
      # leading-keyword only, so to_stmt_result reports a CTE-prefixed write as read-only. The
      # shard-dirtying classification is what depends on `changes`, and is asserted below.)
      assert {:ok, %StmtResult{cols: ["k"], rows: [[5]]}} =
               ShardExecutor.execute(
                 conn,
                 stmt(
                   "WITH src(x) AS (VALUES (5)) INSERT INTO kv SELECT x, 'w' FROM src RETURNING k"
                 )
               )

      assert Fathom.Shard.WriteCounter.count(shard) > before,
             "WITH ... INSERT RETURNING must be classified a write; its only signal is changes > 0"
    end
  end

  # expert review 2026-10-20-2027 #18: a pooled stream's CLOSE paid two dirty dispatches for two
  # facts (temp-schema probe `step` + `rollback_if_open`'s `transaction_status`); they are one
  # fused cached statement now. The abandoned-transaction rollback must still happen.
  describe "pooled stream close" do
    setup do
      prev = Application.get_env(:fathom, :connection_pool)
      prev_idle = Application.get_env(:fathom, :shard_idle_ms)
      Application.put_env(:fathom, :connection_pool, true)
      Application.put_env(:fathom, :shard_idle_ms, 60_000)
      shard = "test_nifd_pool_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        for {k, v} <- [connection_pool: prev, shard_idle_ms: prev_idle] do
          if v == nil,
            do: Application.delete_env(:fathom, k),
            else: Application.put_env(:fathom, k, v)
        end

        for base <- [
              Path.join(Fathom.Shard.data_dir(), "#{shard}.db"),
              Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}.db")
            ],
            suffix <- ["", "-wal", "-shm"],
            do: File.rm(base <> suffix)
      end)

      {:ok, keep} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(keep, stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY)"))
      on_exit(fn -> ShardExecutor.close(keep) end)
      %{shard: shard}
    end

    test "closing a clean pooled stream is exactly 1 dirty dispatch (was 2)", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("SELECT 1"))

      {:ok, warm} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(warm, stmt("SELECT 1"))
      # Warm the fused statement's cache entry on this handle; the steady state is what is pinned.
      _ = ShardExecutor.close(warm)

      {:ok, again} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(again, stmt("SELECT 1"))
      {_, total, by_fun} = count(fn -> ShardExecutor.close(again) end)
      ShardExecutor.close(conn)

      assert {total, by_fun} == {1, %{multi_step: 1}}
    end

    test "a stream closed mid-BEGIN is still rolled back before pooling", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1)"))
      :ok = ShardExecutor.close(conn)

      # The abandoned write was rolled back: nothing is held, and the row is gone.
      {:ok, other} = ShardExecutor.open(shard)

      assert {:ok, %StmtResult{rows: []}} =
               ShardExecutor.execute(other, stmt("SELECT k FROM kv"))

      assert {:ok, _} = ShardExecutor.execute(other, stmt("INSERT INTO kv VALUES (2)"))
      ShardExecutor.close(other)
    end

    test "the fused probe reports the temp schema and the autocommit flag", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      assert {true, true} = Connection.checkin_probe(elem(conn, 2))

      {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
      assert {true, false} = Connection.checkin_probe(elem(conn, 2))
      {:ok, _} = ShardExecutor.execute(conn, stmt("ROLLBACK"))

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TEMP TABLE t(x)"))
      assert {false, true} = Connection.checkin_probe(elem(conn, 2))
      ShardExecutor.close(conn)
    end
  end

  # Order-of-magnitude ceiling, not an exact latency (other agents load the machine). Measured
  # 2026-10-14: ~5.8 us -> ~2.7 us of NIF work per prepared point SELECT (100k iterations, median of
  # 7 alternating runs); the whole Connection.query is far below the 100 us ceiling either way.
  @tag :bench
  test "bench: a prepared point SELECT through Connection.query stays under 100 us" do
    path = Path.join(System.tmp_dir!(), "nifd_bench_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    :ok = Connection.exec(conn, "INSERT INTO t VALUES (1, 'a')")
    sql = "SELECT v FROM t WHERE id = ?"
    opts = [dml?: false, read?: true]
    {:ok, _} = Connection.query(conn, sql, [1], opts)

    n = 20_000

    {us, _} =
      :timer.tc(fn -> for _ <- 1..n, do: {:ok, _} = Connection.query(conn, sql, [1], opts) end)

    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    assert us / n < 100, "point SELECT took #{Float.round(us / n, 1)} us"
  end

  describe "fresh-handle open dispatches" do
    setup do
      path = Path.join(System.tmp_dir!(), "nifd_open_#{System.unique_integer([:positive])}.db")
      on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
      %{path: path}
    end

    test "a tenant :rw open is 18 dirty dispatches (was 23)", %{path: path} do
      {res, total, by_fun} = count(fn -> Connection.open(path, tenant?: true) end)
      assert {:ok, conn} = res
      Connection.close(conn)

      # The five fixed PRAGMAs are ONE execute, and since expert review 2026-10-19 #23 the two
      # engine-hardening PRAGMAs (trusted_schema, cell_size_check) are one more. Was 19 before that;
      # journal_mode, page_size and the extension's own setup are untouched.
      assert total == 18, "open dispatches: #{inspect(by_fun)}"
    end

    # PROD SHAPE (expert review 2026-10-22 r8 #18): prod sets `:query_timeout_ms` (30 s), which arms
    # the on-thread deadline backstop on every tenant handle. The test env leaves it unset, so the
    # count above never paid it and could not see it folded into the hardening batch.
    defp with_prod_deadline(fun) do
      prev = Application.fetch_env(:fathom, :query_timeout_ms)
      Application.put_env(:fathom, :query_timeout_ms, 30_000)

      try do
        fun.()
      after
        case prev do
          {:ok, v} -> Application.put_env(:fathom, :query_timeout_ms, v)
          :error -> Application.delete_env(:fathom, :query_timeout_ms)
        end
      end
    end

    test "prod shape: a tenant :rw open is 18 dirty dispatches with the backstop armed (was 19)",
         %{path: path} do
      {res, total, by_fun} =
        with_prod_deadline(fn -> count(fn -> Connection.open(path, tenant?: true) end) end)

      assert {:ok, conn} = res

      # The backstop rides the hardening batch (r8 #18 step 1) instead of its own execute. Prove
      # it was ARMED, not dropped: the extension answers the deadline it holds.
      assert {:ok, %{rows: [[30_000]]}} =
               Connection.query(conn, "SELECT fathom_backstop(0)", [], dml?: false)

      Connection.close(conn)
      assert total == 18, "open dispatches: #{inspect(by_fun)}"
    end

    test "prod shape: a tenant :ro open is 10 dirty dispatches (was 13)", %{path: path} do
      {:ok, c0} = Connection.open(path)
      Connection.close(c0)

      {res, total, by_fun} =
        with_prod_deadline(fn ->
          count(fn -> Connection.open(path, tenant?: true, scope: :ro) end)
        end)

      assert {:ok, conn} = res
      pragma = fn name -> Connection.query(conn, "PRAGMA #{name}", [], dml?: false) end

      # r8 #18 step 1: `cache_size` rides the hardening batch, `foreign_keys` is not set (FK
      # enforcement runs only in DML, which a read-only handle cannot execute), and the backstop
      # rides last. Everything the batch carries took effect:
      assert {:ok, %{rows: [[-2000]]}} = pragma.("cache_size")
      assert {:ok, %{rows: [[0]]}} = pragma.("trusted_schema")
      assert {:ok, %{rows: [[1]]}} = pragma.("cell_size_check")

      assert {:ok, %{rows: [[30_000]]}} =
               Connection.query(conn, "SELECT fathom_backstop(0)", [], dml?: false)

      Connection.close(conn)
      assert total == 10, "open dispatches: #{inspect(by_fun)}"
    end

    test "the batched PRAGMAs all took effect", %{path: path} do
      {:ok, conn} = Connection.open(path, tenant?: true)
      pragma = fn name -> Connection.query(conn, "PRAGMA #{name}", [], dml?: false) end

      assert {:ok, %{rows: [[2]]}} = pragma.("synchronous")
      # (wal_autocheckpoint reads back 0 on a tenant handle: the extension's own wal hook takes
      # the same slot, which is why the batch must stay BEFORE load_extension.)
      assert {:ok, %{rows: [[67_108_864]]}} = pragma.("journal_size_limit")
      assert {:ok, %{rows: [[1]]}} = pragma.("foreign_keys")
      assert {:ok, %{rows: [[-2000]]}} = pragma.("cache_size")
      Connection.close(conn)
    end
  end

  # expert review 2026-10-17 #6: with replication ON a DML statement needs changes, rowid AND the
  # autocommit flag; the extension's `fathom_conn_state()` returns all three in one statement step.
  describe "fathom_conn_state fast path (conn_state?: true)" do
    setup do
      path = Path.join(System.tmp_dir!(), "nifd_cs_#{System.unique_integer([:positive])}.db")
      {:ok, conn} = Connection.open(path, tenant?: true)
      :ok = Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

      on_exit(fn ->
        Connection.close(conn)
        for suffix <- ["", "-wal", "-shm"], do: File.rm(path <> suffix)
      end)

      %{conn: conn}
    end

    test "an INSERT is 2 dirty dispatches (was 4 with the executor's autocommit read)", %{
      conn: conn
    } do
      sql = "INSERT INTO t (v) VALUES (?)"
      {:ok, _} = Connection.query(conn, sql, ["warm"], conn_state?: true)

      {res, total, by_fun} =
        count(fn -> Connection.query(conn, sql, ["x"], conn_state?: true) end)

      assert {:ok, %{num_changes: 1, last_insert_rowid: 2, autocommit?: true}} = res
      assert {total, by_fun} == {2, %{multi_step: 2}}

      # Without the option: unchanged 3 dispatches, autocommit not reported.
      {res, total, by_fun} = count(fn -> Connection.query(conn, sql, ["y"], []) end)
      assert {:ok, %{num_changes: 1, last_insert_rowid: 3, autocommit?: nil}} = res
      assert {total, by_fun} == {3, %{multi_step: 1, changes: 1, last_insert_rowid: 1}}
    end

    test "autocommit? is SQLite's own answer inside a transaction and after it ends", %{
      conn: conn
    } do
      {:ok, _} = Connection.query(conn, "BEGIN", [], conn_state?: true)

      assert {:ok, %{autocommit?: false, num_changes: 1}} =
               Connection.query(conn, "INSERT INTO t (v) VALUES ('a')", [], conn_state?: true)

      assert {:ok, _} = Connection.query(conn, "COMMIT", [], conn_state?: true)

      assert {:ok, %{autocommit?: true, num_changes: 1}} =
               Connection.query(conn, "INSERT INTO t (v) VALUES ('b')", [], conn_state?: true)
    end

    test "INSERT ... RETURNING, a 0-row UPDATE, and a trigger's writes report the same counters",
         %{conn: conn} do
      :ok = Connection.exec(conn, "CREATE TABLE log (id INTEGER PRIMARY KEY)")

      :ok =
        Connection.exec(
          conn,
          "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NULL); END"
        )

      assert {:ok, %{rows: [[1]], num_changes: 1, last_insert_rowid: 1, autocommit?: true}} =
               Connection.query(conn, "INSERT INTO t (v) VALUES ('r') RETURNING id", [],
                 conn_state?: true
               )

      assert {:ok, %{num_changes: 0, last_insert_rowid: nil}} =
               Connection.query(conn, "UPDATE t SET v = 'z' WHERE id = 999", [],
                 conn_state?: true
               )
    end

    test "without the extension (a bare exqlite handle) it falls back to the 3 NIF calls" do
      {:ok, bare} = Sqlite3.open(":memory:")
      :ok = Sqlite3.execute(bare, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

      assert {:ok, %{num_changes: 1, last_insert_rowid: 1, autocommit?: nil}} =
               Connection.query(bare, "INSERT INTO t (v) VALUES ('a')", [], conn_state?: true)

      Sqlite3.close(bare)
    end

    test "a replicated-mode INSERT inside a transaction through the executor is 2 dispatches" do
      shard = "test_nifd_cs_#{System.unique_integer([:positive])}"
      {:ok, conn} = ShardExecutor.open(shard)

      on_exit(fn ->
        ShardExecutor.close(conn)

        for base <- [
              Path.join(Fathom.Shard.data_dir(), "#{shard}.db"),
              Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}.db")
            ],
            suffix <- ["", "-wal", "-shm"],
            do: File.rm(base <> suffix)
      end)

      # Replication is on and the writes are in an open transaction, so nothing ships (autocommit is
      # false); the dispatch count is the statement step plus the one state step.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY, v)"))

      # Replication goes on only after the autocommit DDL (it would try to ship to a quorum).
      prev = Application.get_env(:fathom, :replication_enabled)
      Application.put_env(:fathom, :replication_enabled, true)

      on_exit(fn ->
        if prev == nil,
          do: Application.delete_env(:fathom, :replication_enabled),
          else: Application.put_env(:fathom, :replication_enabled, prev)
      end)

      {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?, 'w')", [1]))

      {res, total, by_fun} =
        count(fn -> ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?, 'w')", [2])) end)

      assert {:ok, %StmtResult{affected_row_count: 1, last_insert_rowid: 2}} = res
      assert {total, by_fun} == {2, %{multi_step: 2}}
      {:ok, _} = ShardExecutor.execute(conn, stmt("ROLLBACK"))
    end

    # Expert review 2026-10-20 #27. A COMMIT that closes a transaction which wrote is a ship
    # candidate under replication, so the executor asked `autocommit?/1` (a dirty `transaction_status`
    # dispatch) AFTER the `changes` fetch, and a COMMIT inheriting changes()>0 also fetched the rowid:
    # multi_step + changes + last_insert_rowid + transaction_status = 4. The extension's
    # `fathom_conn_state()` returns all three in one step: 2.
    # Only the dispatch count is asserted; with no followers the ship itself is allowed to refuse.
    test "a replicated-mode COMMIT of a writing transaction is 2 dispatches (was 4)" do
      shard = "test_nifd_cm_#{System.unique_integer([:positive])}"
      {:ok, conn} = ShardExecutor.open(shard)

      on_exit(fn ->
        ShardExecutor.close(conn)

        for base <- [
              Path.join(Fathom.Shard.data_dir(), "#{shard}.db"),
              Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}.db")
            ],
            suffix <- ["", "-wal", "-shm"],
            do: File.rm(base <> suffix)
      end)

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY, v)"))

      prev = Application.get_env(:fathom, :replication_enabled)
      Application.put_env(:fathom, :replication_enabled, true)

      on_exit(fn ->
        if prev == nil,
          do: Application.delete_env(:fathom, :replication_enabled),
          else: Application.put_env(:fathom, :replication_enabled, prev)
      end)

      # One full transaction first, so BEGIN / INSERT / COMMIT are all in the statement cache and the
      # measured COMMIT is a cache hit.
      for k <- [1, 2] do
        {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?, 'w')", [k]))
        if k == 1, do: ShardExecutor.execute(conn, stmt("COMMIT"))
      end

      {_res, total, by_fun} = count(fn -> ShardExecutor.execute(conn, stmt("COMMIT")) end)

      # The ship (no followers here) may add its own non-dirty work, never a dirty dispatch.
      assert {total, by_fun} == {2, %{multi_step: 2}}
    end
  end

  # A statement left un-reset keeps its read transaction (a WAL read mark), which blocks a
  # `wal_checkpoint(TRUNCATE)` from any other handle. After every outcome, the statement must have
  # released it. (The reset was removed from the hot path: only the abandoned-`{:rows, _}` and the
  # exception paths reset by hand, everything else relies on `multi_step`'s own resets.)
  describe "no outcome leaves a statement holding a read transaction" do
    setup do
      path = Path.join(System.tmp_dir!(), "nifd_wal_#{System.unique_integer([:positive])}.db")
      {:ok, a} = Connection.open(path)
      {:ok, b} = Connection.open(path)
      :ok = Sqlite3.set_busy_timeout(b, 0)
      :ok = Connection.exec(a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")

      :ok =
        Connection.exec(a, """
        WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < 1500)
        INSERT INTO t (id, v) SELECT i, 'row-' || i FROM seq
        """)

      on_exit(fn ->
        Application.delete_env(:fathom, :query_max_rows)
        Application.delete_env(:fathom, :query_timeout_ms)
        Connection.close(a)
        Connection.close(b)
        for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
      end)

      %{a: a, b: b}
    end

    # The observer. busy is the first column of the checkpoint row; the WAL must be non-empty
    # for a held read mark to matter, which the seed insert above guarantees.
    defp truncate_busy(b) do
      assert {:ok, %{rows: [[busy, _log, _ckpt]]}} =
               Connection.query(b, "PRAGMA wal_checkpoint(TRUNCATE)", [], dml?: false)

      busy
    end

    # Same opts the executor passes for client SQL.
    defp client(conn, sql, args \\ []) do
      Connection.query(conn, sql, args, dml?: false, read?: true, deadline: true)
    end

    test "the harness discriminates: an un-reset mid-statement DOES block the checkpoint", %{
      a: a,
      b: b
    } do
      {:ok, st} = Sqlite3.prepare(a, "SELECT id FROM t")
      assert {:rows, _} = Sqlite3.multi_step(a, st, 10)
      assert truncate_busy(b) == 1, "precondition: a statement mid-execution must block TRUNCATE"
      :ok = Sqlite3.release(a, st)
      assert truncate_busy(b) == 0
    end

    test "cache hit, then DONE", %{a: a, b: b} do
      assert {:ok, %{row_count: 1}} = client(a, "SELECT v FROM t WHERE id = ?", [3])
      assert {:ok, %{row_count: 1}} = client(a, "SELECT v FROM t WHERE id = ?", [4])
      assert truncate_busy(b) == 0
    end

    test "a write", %{a: a, b: b} do
      assert {:ok, %{num_changes: 1}} =
               Connection.query(a, "INSERT INTO t VALUES (5000, 'x')", [], dml?: true)

      assert truncate_busy(b) == 0
    end

    test "a bind error", %{a: a, b: b} do
      assert {:ok, _} = client(a, "SELECT v FROM t WHERE id = ?", [1])
      assert {:error, _} = client(a, "SELECT v FROM t WHERE id = ?", [%{not: :bindable}])
      assert {:error, _} = client(a, "SELECT v FROM t WHERE id = ?", [])
      assert truncate_busy(b) == 0
      # and the statement is still usable afterwards
      assert {:ok, %{rows: [["row-1"]]}} = client(a, "SELECT v FROM t WHERE id = ?", [1])
    end

    test "too_many_rows abandoned mid-statement (the {:rows, _} branch)", %{a: a, b: b} do
      # 1500 rows > one 500-row multi_step chunk, so the cap trips on a {:rows, _} batch and the
      # statement is still mid-execution: only the manual reset releases it.
      Application.put_env(:fathom, :query_max_rows, 100)
      assert {:error, {:too_many_rows, 100}} = client(a, "SELECT * FROM t")
      assert truncate_busy(b) == 0
      Application.delete_env(:fathom, :query_max_rows)
      assert {:ok, %{row_count: 1500}} = client(a, "SELECT * FROM t")
    end

    test "too_many_rows on the final {:done, _} batch", %{a: a, b: b} do
      Application.put_env(:fathom, :query_max_rows, 10)
      assert {:error, {:too_many_rows, 10}} = client(a, "SELECT * FROM t WHERE id <= 50")
      assert truncate_busy(b) == 0
    end

    test "a constraint error", %{a: a, b: b} do
      assert {:error, _} =
               Connection.query(a, "INSERT INTO t VALUES (6000, NULL)", [], dml?: true)

      assert truncate_busy(b) == 0
    end

    test "query_timeout", %{a: a, b: b} do
      Application.put_env(:fathom, :query_timeout_ms, 20)

      assert {:error, :query_timeout} =
               client(
                 a,
                 "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) " <>
                   "SELECT count(*) FROM c"
               )

      Application.delete_env(:fathom, :query_timeout_ms)
      assert truncate_busy(b) == 0
    end

    test "busy", %{a: a, b: b} do
      :ok = Sqlite3.set_busy_timeout(a, 0)
      :ok = Connection.exec(b, "BEGIN IMMEDIATE")

      assert {:error, :busy} =
               Connection.query(a, "INSERT INTO t VALUES (7000, 'x')", [], dml?: true)

      :ok = Connection.exec(b, "COMMIT")
      assert truncate_busy(b) == 0
    end
  end
end
