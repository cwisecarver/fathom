defmodule Fathom.ShardExecutorCountersTest do
  @moduledoc """
  Expert review 2026-11-07 #68. The wire counters (`affected_row_count`, `last_insert_rowid`,
  `rows_written`) were wrong in two places:

    * a CTE-prefixed `INSERT/UPDATE/DELETE … RETURNING`: `dml?/1` is leading-keyword only, so the
      statement counted as a read and answered 0 / nil while SQLite changed N rows;
    * DDL right after a write: SQLite does not reset `sqlite3_changes()` / `last_insert_rowid()` for
      DDL, so `CREATE INDEX` after a 5-row INSERT reported `affected_row_count: 5` and the previous
      rowid.

  The CTE classification is by the CTE's MAIN statement verb, not by `changes > 0`: a read-only
  `WITH … SELECT` that follows a write on the same connection inherits that write's change count,
  which is the 2026-07-14 #42 defect from the other side, so it is pinned here too.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Stmt, StmtResult}

  setup do
    shard = "counters_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = run(h, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
    on_exit(fn -> ShardExecutor.close(h) end)
    %{h: h}
  end

  defp run(h, sql), do: ShardExecutor.execute(h, %Stmt{sql: sql})

  test "a CTE-prefixed INSERT … RETURNING reports its changes and rowid", %{h: h} do
    assert {:ok,
            %StmtResult{
              rows: [[7]],
              affected_row_count: 1,
              last_insert_rowid: 7,
              rows_written: 1
            }} =
             run(
               h,
               "WITH g(x) AS (VALUES (7)) INSERT INTO t(id, v) SELECT x, x FROM g RETURNING id"
             )
  end

  test "CTE-prefixed UPDATE and DELETE … RETURNING report their changes", %{h: h} do
    {:ok, _} = run(h, "INSERT INTO t(v) VALUES (1), (1), (1)")

    assert {:ok, %StmtResult{affected_row_count: 3, rows_written: 3}} =
             run(
               h,
               "WITH k(x) AS (SELECT 1) UPDATE t SET v = 2 WHERE v IN (SELECT x FROM k) RETURNING id"
             )

    assert {:ok, %StmtResult{affected_row_count: 3, rows_written: 3}} =
             run(
               h,
               "WITH k(x) AS (SELECT 2) DELETE FROM t WHERE v IN (SELECT x FROM k) RETURNING id"
             )
  end

  test "the CTE's main verb is found past recursive, column lists, MATERIALIZED, several CTEs and quoting",
       %{h: h} do
    cases = [
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) " <>
        "INSERT INTO t(v) SELECT n FROM r RETURNING id",
      "WITH a(x) AS NOT MATERIALIZED (SELECT 1), b(y) AS MATERIALIZED (SELECT x FROM a) " <>
        "INSERT INTO t(v) SELECT y FROM b RETURNING id",
      "WITH \"a)b\"(x) AS (SELECT ')' -- a comment with ) in it\n) " <>
        "INSERT INTO t(v) SELECT 1 FROM \"a)b\" RETURNING id",
      "/* lead */ WITH a AS (SELECT 1 AS x) REPLACE INTO t(id, v) SELECT x, x FROM a RETURNING id"
    ]

    for sql <- cases do
      assert {:ok, %StmtResult{affected_row_count: n, rows_written: n}} = run(h, sql),
             "#{sql} must report its changes"

      assert n >= 1, "#{sql} must report its changes"
    end
  end

  test "a read-only WITH … SELECT after a write reports no change count", %{h: h} do
    {:ok, %StmtResult{affected_row_count: 5}} =
      run(h, "INSERT INTO t(v) VALUES (1), (2), (3), (4), (5)")

    for sql <- [
          "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
          "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
          # A CTE whose NAME is a DML verb must not read as DML.
          "WITH \"insert\"(a) AS (SELECT 1) SELECT a FROM \"insert\"",
          "WITH x AS (SELECT 'insert') SELECT * FROM x"
        ] do
      assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil, rows_written: 0}} =
               run(h, sql),
             "#{sql} reported the inherited counters of the previous INSERT"
    end
  end

  test "DDL and ANALYZE right after a write report no changes and no rowid", %{h: h} do
    assert {:ok, %StmtResult{affected_row_count: 3, last_insert_rowid: rowid}} =
             run(h, "INSERT INTO t(v) VALUES (1), (2), (3)")

    assert is_integer(rowid), "fixture: the INSERT must have set a rowid"

    for sql <- [
          "CREATE INDEX t_v ON t(v)",
          "CREATE TABLE u (a INTEGER)",
          "ALTER TABLE u ADD COLUMN b TEXT",
          "DROP TABLE u",
          "ANALYZE",
          "REINDEX t_v"
        ] do
      # Re-arm the inherited counters before each DDL.
      assert {:ok, %StmtResult{affected_row_count: 1}} = run(h, "INSERT INTO t(v) VALUES (9)")

      assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil, rows_written: 0}} =
               run(h, sql),
             "#{sql} reported the previous write's counters"
    end
  end
end
