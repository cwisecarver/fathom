defmodule Fathom.Shard.RegexpStatementCacheTest do
  @moduledoc """
  A bound-parameter pattern (`WHERE col REGEXP ?`, what a Django queryset sends) must cost ONE
  regex compile per statement execution, however hard other patterns churn the node-wide compiled
  cache (`pyre::CACHE_CAP`, one LRU across all tenants) between its rows.

  The cache holds `CACHE_CAP` patterns; a pattern evicted from it recompiles (~1.6 ms for a
  Unicode-class pattern, so ~16 s over a 10k-row scan if it happened per row). `native/fathom_udf`
  keeps the compiled pattern in SQLite AUXDATA for the statement instead, so the cache is only
  consulted once per statement. This file proves it end to end through the loadable extension and
  `Fathom.Shard.Connection`: there is no Rust `#[test]` that can, because the crate is built with
  rusqlite's `loadable_extension` feature and cannot open a connection (see `wal_hook_test.exs`).

  The observable is `fathom_regexp_compiles()`: the calling connection's own count of compiles its
  `regexp` calls caused. A count, not wall-clock time, so a loaded machine cannot flake it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  # More distinct NON-constant patterns per row than `CACHE_CAP` (64): each row evaluates all of
  # them, so the LRU is round-robin over 70 patterns and every churn lookup misses; the hot bound
  # pattern, used once per row, is evicted between every two of its uses.
  @churn_patterns 70
  @rows 30

  setup do
    path = Path.join(System.tmp_dir!(), "fathom_rxstmt_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path, [])

    on_exit(fn ->
      Connection.close(conn)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    %{conn: conn}
  end

  defp rows!(conn, sql, params \\ []) do
    {:ok, %{rows: rows}} = Connection.query(conn, sql, params)
    rows
  end

  defp compiles(conn), do: conn |> rows!("SELECT fathom_regexp_compiles()") |> hd() |> hd()

  # Compiles `fun` caused on this connection.
  defp compiles_during(conn, fun) do
    before = compiles(conn)
    result = fun.()
    {result, compiles(conn) - before}
  end

  # A table with `x` and one pattern column per churn pattern; every row matches every pattern.
  defp churn_table!(conn, tag) do
    cols = for k <- 0..(@churn_patterns - 1), do: "p#{k}"

    rows!(conn, "CREATE TABLE r (x TEXT, #{Enum.map_join(cols, ", ", &"#{&1} TEXT")})")

    for _ <- 1..@rows do
      vals = for k <- 0..(@churn_patterns - 1), do: "'^(?:r|q#{tag}_#{k})ow$'"
      rows!(conn, "INSERT INTO r VALUES ('row', #{Enum.join(vals, ", ")})")
    end

    Enum.map_join(cols, " AND ", &"x REGEXP #{&1}")
  end

  test "the probe function exists, so this file is measuring something", %{conn: conn} do
    assert rows!(conn, "SELECT typeof(fathom_regexp_compiles())") == [["integer"]],
           "fathom_regexp_compiles() is missing: the extension did not load or is stale " <>
             "(mix compile.fathom_udf)"
  end

  test "a bound pattern compiles once per statement while other patterns churn the cache",
       %{conn: conn} do
    churn = churn_table!(conn, System.unique_integer([:positive]))
    hot = "^(?:r|zz#{System.unique_integer([:positive])})ow$"

    # The churn alone must be missing on every lookup, or the test cannot see the hot pattern's
    # compiles. (Churn patterns are not constant: SQLite gives them no auxdata, they go through the
    # node-wide cache one by one.)
    {[[count]], churn_only} =
      compiles_during(conn, fn -> rows!(conn, "SELECT count(*) FROM r WHERE #{churn}") end)

    assert count == @rows
    assert churn_only == @rows * @churn_patterns, "the churn must miss the cache on every lookup"

    # Same rows, same churn, plus the bound hot pattern. Pre-fix the hot pattern was evicted between
    # its uses and recompiled on EVERY row (`@rows` more compiles); with the statement's auxdata it
    # is compiled once.
    {[[count]], with_hot} =
      compiles_during(conn, fn ->
        rows!(conn, "SELECT count(*) FROM r WHERE x REGEXP ?1 AND #{churn}", [hot])
      end)

    assert count == @rows
    assert with_hot - churn_only == 1, "the bound pattern compiled #{with_hot - churn_only} times"
  end

  test "re-executing with a different bound pattern uses the new pattern, once per execution",
       %{conn: conn} do
    churn = churn_table!(conn, System.unique_integer([:positive]))
    # The churn goes FIRST so every row evaluates all of it even when the bound pattern matches
    # nothing (AND short-circuits left to right).
    sql = "SELECT count(*) FROM r WHERE #{churn} AND x REGEXP ?1"
    all = "^(?:r|ya#{System.unique_integer([:positive])})ow$"
    none = "^(?:z|yb#{System.unique_integer([:positive])})ow$"

    {[[count]], base} = compiles_during(conn, fn -> rows!(conn, sql, [all]) end)
    assert count == @rows
    assert base == @rows * @churn_patterns + 1, "first execution: churn misses + ONE hot compile"

    for {pattern, expected} <- [{none, 0}, {all, @rows}, {none, 0}] do
      {[[count]], n} = compiles_during(conn, fn -> rows!(conn, sql, [pattern]) end)
      assert count == expected, "pattern #{pattern}"
      assert n == base, "compiled #{n - @rows * @churn_patterns} bound-pattern times; expected 1"
    end
  end

  test "a pattern that differs per row is never reused from the row before", %{conn: conn} do
    rows!(conn, "CREATE TABLE pt (x TEXT, pat TEXT)")

    for {x, pat} <- [{"abc", "^a"}, {"abc", "^b"}, {"abc", "c$"}, {"abc", "^c"}, {"abc", "^a"}] do
      rows!(conn, "INSERT INTO pt VALUES (?1, ?2)", [x, pat])
    end

    # ^a, ^b, c$, ^c, ^a against "abc": match, no, match, no, match.
    assert rows!(conn, "SELECT count(*) FROM pt WHERE x REGEXP pat") == [[3]]

    assert rows!(conn, "SELECT pat FROM pt WHERE NOT (x REGEXP pat) ORDER BY rowid") ==
             [["^b"], ["^c"]]

    # A computed (non-constant) pattern, too: ^a, ^b, ^$ (last char of `c$`), ^c, ^a -> 2 match.
    assert rows!(conn, "SELECT count(*) FROM pt WHERE x REGEXP ('^' || substr(pat, -1))") ==
             [[2]]
  end

  test "the subject caps, errors and NULLs behave the same through the statement's pattern",
       %{conn: conn} do
    rows!(conn, "CREATE TABLE s (v TEXT)")
    rows!(conn, "INSERT INTO s VALUES ('ab'), (NULL), ('zz')")

    assert rows!(conn, "SELECT v FROM s WHERE v REGEXP ?1 ORDER BY rowid", ["^a"]) == [["ab"]]
    assert rows!(conn, "SELECT count(*) FROM s WHERE v REGEXP ?1", [nil]) == [[0]]

    # A pattern that does not compile fails the statement (and the next statement is unaffected).
    assert {:error, _} =
             Connection.query(conn, "SELECT v FROM s WHERE v REGEXP ?1", ["(unclosed"])

    assert rows!(conn, "SELECT v FROM s WHERE v REGEXP ?1 ORDER BY rowid", ["z$"]) == [["zz"]]

    # A subject over the plain cap (256 KiB) is refused on the row that has it, not silently
    # matched, even though the compiled pattern came from auxdata.
    rows!(conn, "INSERT INTO s VALUES (?1)", [String.duplicate("a", 262_145)])
    assert {:error, _} = Connection.query(conn, "SELECT count(*) FROM s WHERE v REGEXP ?1", ["a"])
  end
end
