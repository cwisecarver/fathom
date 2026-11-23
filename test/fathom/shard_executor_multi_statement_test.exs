defmodule Fathom.ShardExecutorMultiStatementTest do
  @moduledoc """
  Expert review 2026-11-07 #19. `execute` and `describe` hand one SQL string to
  `sqlite3_prepare_v3`, which compiles the FIRST statement and returns the tail; exqlite discards
  the tail. So `"INSERT … ; INSERT …"` ran one insert and answered success, and `describe` of
  `"select 1; garbage"` succeeded. Both now refuse a string that holds more than one statement
  (sqld does too); `sequence` is the multi-statement request.

  What must NOT trip it: a trailing `;`, a trailing comment, a `;` inside a literal or a comment,
  and a `CREATE TRIGGER`, whose body is ONE statement that contains `;` (the splitter used for the
  authorization gate over-splits it, so the counter folds the body back in).
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Error, Stmt}

  setup do
    shard = "multi_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = run(h, "CREATE TABLE t (v INTEGER)")
    on_exit(fn -> ShardExecutor.close(h) end)
    %{h: h}
  end

  defp run(h, sql), do: ShardExecutor.execute(h, %Stmt{sql: sql})
  defp count(h), do: run(h, "SELECT count(*) FROM t") |> then(fn {:ok, r} -> hd(hd(r.rows)) end)

  test "execute refuses a second statement instead of silently dropping it", %{h: h} do
    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS", status: 400}} =
             run(h, "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")

    # Pre-fix: {:ok, _} and exactly one row.
    assert count(h) == 0, "the refused string must run NOTHING"

    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} =
             run(h, "PRAGMA foreign_keys = ON; BEGIN;")

    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} =
             run(h, "SELECT 1; -- not just a comment\nSELECT 2")
  end

  test "a trailing semicolon, trailing comment or empty statements are still one statement",
       %{h: h} do
    for sql <- [
          "INSERT INTO t VALUES (1);",
          "INSERT INTO t VALUES (2); -- trailing",
          "INSERT INTO t VALUES (3); /* trailing */",
          "INSERT INTO t VALUES (4);;  ;",
          "/* leading */ INSERT INTO t VALUES (5) ; -- t\n",
          "INSERT INTO t VALUES (6) -- ; not a separator"
        ] do
      assert {:ok, _} = run(h, sql), "#{inspect(sql)} is one statement"
    end

    assert count(h) == 6
  end

  # Perf audit 2026-11-19 #2: a string whose first `;` is followed only by ASCII whitespace skips the
  # scanner. The boundary is what matters: whitespace-only tails are one statement, and ANY other
  # byte after the `;` (a second statement, a comment, a non-ASCII space, a second `;`) must still
  # go through the scanner and be counted exactly as before.
  test "the one-statement fast path stops exactly at a non-whitespace tail", %{h: h} do
    for tail <- ["", " ", "\n", "\t\r\n \v\f", "      \n\n"] do
      assert {:ok, _} = run(h, "INSERT INTO t VALUES (1);" <> tail), "tail #{inspect(tail)}"
    end

    assert count(h) == 5

    for tail <- [" INSERT INTO t VALUES (9)", "\nSELECT 1", "\tSELECT 1;", " ; SELECT 1"] do
      assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} =
               run(h, "INSERT INTO t VALUES (9);" <> tail),
             "tail #{inspect(tail)}"
    end

    # A comment after the `;` is not a statement (scanner path), a second `;` after it is still none.
    assert {:ok, _} = run(h, "INSERT INTO t VALUES (2); -- trailing")
    assert {:ok, _} = run(h, "INSERT INTO t VALUES (3);  ;")
    # A U+00A0 (not ASCII whitespace) after the `;` takes the scanner, which trims it: still one.
    assert {:ok, _} = run(h, "INSERT INTO t VALUES (4);\u00A0")
    assert count(h) == 8
  end

  # Security review of the fast path (2026-11-20): the fast path keys on the first `;` BYTE, so a `;`
  # inside a quote or comment, or a byte just outside the blank set, must still reach the scanner.
  test "the fast path still hands quoted/commented first `;` and non-blank tails to the scanner",
       %{h: h} do
    for sql <- [
          "SELECT ';' ; SELECT 2",
          ~s(SELECT 1 AS "a;b"; SELECT 2),
          "SELECT 1 /* ; */; SELECT 2",
          "INSERT INTO t VALUES (1);\0SELECT 1",
          "SELECT 1;\x1F SELECT 1"
        ] do
      assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} = run(h, sql), inspect(sql)
    end

    # An unterminated quote/comment/bracket holding the only `;` is ONE statement: SQLite decides its
    # fate (an error for the quote and bracket, a plain SELECT for an unterminated comment), and the
    # multi-statement gate must not be the one to refuse it.
    for sql <- ["SELECT 'x; ", "SELECT [x; ", "SELECT 1 /* ; "] do
      res = run(h, sql)
      refute match?({:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}}, res), inspect(sql)
    end

    assert count(h) == 0
  end

  test "describe accepts a `;` with a whitespace tail and still refuses a second statement", %{
    h: h
  } do
    assert {:ok, _} = ShardExecutor.describe(h, "SELECT v FROM t; \n\t")

    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} =
             ShardExecutor.describe(h, "SELECT v FROM t;\nSELECT 1")
  end

  # Audit 2026-11-22 #1: statements hidden behind SQLite's `$name(...)` variable token. Unfixed, the
  # splitter saw ONE fragment, so `execute` ran the first statement and silently dropped the rest (the
  # 2026-11-07 #19 class) and `sequence` ran all three with the middle one never gated.
  test "statements hidden behind a $name(...) variable are counted and gated", %{h: h} do
    hidden = "SELECT $a('); INSERT INTO t VALUES (7); SELECT $b(')"

    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} = run(h, hidden)
    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} = ShardExecutor.describe(h, hidden)

    assert {:error, %Error{code: "FILO_STATEMENT_BLOCKED"}} =
             ShardExecutor.execute_sequence(
               h,
               "SELECT $a('); ATTACH DATABASE ':memory:' AS x; SELECT $b(')"
             )

    assert count(h) == 0, "nothing hidden may have run"
  end

  # Perf audit 2026-11-19 #2, hot-path floor (AGENTS.md: ratio, not an exact latency). Before the
  # fast path a trailing `;` on a ~1 KB statement cost about 8x the same statement without it
  # (4.0 vs 32.6 µs through `execute/2`, throwaway probe); after, the two share one path. A 3x
  # ceiling is loose against noise and still an order of magnitude under the old ratio.
  @tag :bench
  @tag timeout: 120_000
  test "a trailing `;` on a 1 KB statement costs no more than 3x the same statement without it",
       %{h: h} do
    cols = Enum.map_join(1..40, ", ", fn i -> "v AS col_#{i}_name_long" end)

    base =
      "SELECT " <>
        cols <>
        " FROM t WHERE v = 1 AND v IS NOT NULL AND v < 100 AND v > -5 ORDER BY v LIMIT 10"

    assert byte_size(base) > 900

    time = fn sql ->
      for _ <- 1..1_000, do: run(h, sql)
      t0 = System.monotonic_time(:microsecond)
      for _ <- 1..10_000, do: {:ok, _} = run(h, sql)
      (System.monotonic_time(:microsecond) - t0) / 10_000
    end

    without = time.(base)
    with_semi = time.(base <> ";")

    assert with_semi < without * 3,
           "a trailing `;` costs #{Float.round(with_semi, 1)} µs vs #{Float.round(without, 1)} µs without"
  end

  test "a semicolon inside a literal, identifier or comment is not a separator", %{h: h} do
    for sql <- [
          "SELECT ';'",
          "SELECT 'it''s; fine'",
          ~s(SELECT 1 AS "a;b"),
          "SELECT 1 /* ; */",
          "SELECT 1 -- ;\n"
        ] do
      assert {:ok, _} = run(h, sql), "#{inspect(sql)} is one statement"
    end
  end

  test "a CREATE TRIGGER (one statement with `;` inside its body) is accepted", %{h: h} do
    trigger = """
    CREATE TRIGGER t_ins AFTER INSERT ON t BEGIN
      UPDATE t SET v = v + 100 WHERE rowid = NEW.rowid;
      INSERT INTO t VALUES (CASE WHEN NEW.v > 1000 THEN 1 ELSE 2 END);
    END;
    """

    assert {:ok, _} = run(h, trigger)

    # …and a second statement after the trigger is still refused.
    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS"}} =
             run(h, "CREATE TRIGGER t_del AFTER DELETE ON t BEGIN SELECT 1; END; SELECT 2")
  end

  test "describe refuses a multi-statement string and accepts a trailing semicolon", %{h: h} do
    assert {:error, %Error{code: "FILO_MULTIPLE_STATEMENTS", status: 400}} =
             ShardExecutor.describe(h, "SELECT 1; SELECT garbage here")

    assert {:ok, %{cols: ["v"]}} = ShardExecutor.describe(h, "SELECT v FROM t;")
    assert {:ok, %{cols: ["v"]}} = ShardExecutor.describe(h, "SELECT v FROM t; -- trailing")
  end

  test "a blocked first statement still reports ITS error, not the multi-statement one", %{h: h} do
    assert {:error, %Error{code: code}} = run(h, "PRAGMA writable_schema = ON; SELECT 1")
    refute code == "FILO_MULTIPLE_STATEMENTS"
  end
end
