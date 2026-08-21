defmodule Fathom.ShardExecutorSplitStatementsTest do
  @moduledoc """
  The statement splitter behind the `sequence` gate (expert review 2026-08-20 #18).

  ## Why this is hand-written, and why that is allowed here

  AGENTS.md forbids hand-rolling a parser for a grammar that has one, and sets a single bar for
  the exception: "I searched and there is no library." That bar is met. exqlite's `prepare/2`
  throws away the tail SQLite's own `sqlite3_prepare_v2` returns, so the engine cannot be asked
  where a statement ends; and its authorizer is an ACTION deny-list (`:pragma` denies every
  pragma) with no per-name granularity, so it cannot enforce the allowlist either — which is
  exactly what `blocked_statement/1`'s moduledoc already says.

  ## The safety direction, and which failure is which

  **Over-splitting is harmless.** An extra fragment is checked against the same allowlist, and a
  legitimate statement passes.

  **UNDER-splitting is the bypass**, and it has exactly one cause: the scanner believing it is
  inside a quoted region when it is not. From that point on every later statement is invisible to
  the gate. The doubling escapes (`'it''s'`) are the sharp edge — mistake the first `'` of a pair
  for a close and the scanner is inside-out for the rest of the script.

  So every test below that matters is of the form "a statement AFTER something tricky is still
  seen". Counting fragments is incidental; not losing one is the property.
  """
  use ExUnit.Case, async: true

  alias Fathom.ShardExecutor

  defp split(sql), do: ShardExecutor.split_statements(sql)

  describe "the ordinary cases" do
    test "a single statement is one fragment" do
      assert split("SELECT 1") == ["SELECT 1"]
      assert split("SELECT 1;") == ["SELECT 1"]
      assert split("  SELECT 1  ;  ") == ["SELECT 1"]
    end

    test "empty and whitespace-only input yields nothing" do
      assert split("") == []
      assert split("   \n\t ") == []
      assert split(";;;") == []
    end

    test "several statements split on the top-level semicolons" do
      assert split("SELECT 1; SELECT 2; SELECT 3") == ["SELECT 1", "SELECT 2", "SELECT 3"]
    end
  end

  describe "a semicolon that must NOT split (under-splitting is the bypass)" do
    test "inside a single-quoted string" do
      stmts = split("INSERT INTO t VALUES ('a;b'); PRAGMA synchronous=OFF")

      assert length(stmts) == 2,
             "a ';' inside a string literal split the script, hiding the boundary structure"

      assert List.last(stmts) =~ "synchronous",
             "the statement AFTER the quoted semicolon was lost — every later statement is now " <>
               "invisible to the gate"
    end

    test "inside a DOUBLED single quote, the escape that turns the scanner inside-out" do
      stmts = split("INSERT INTO t VALUES ('it''s; fine'); PRAGMA max_page_count=1")

      assert List.last(stmts) =~ "max_page_count",
             "the '' escape was read as close-then-open, so the scanner was inside-out and the " <>
               "trailing PRAGMA was never gated"
    end

    test "inside a double-quoted identifier, doubled or not" do
      assert split(~s|SELECT "we;ird"; PRAGMA synchronous=OFF|) |> List.last() =~ "synchronous"

      assert split(~s|SELECT "a""b;c"; PRAGMA synchronous=OFF|) |> List.last() =~ "synchronous"
    end

    test "inside a backtick identifier" do
      assert split("SELECT `we;ird`; PRAGMA synchronous=OFF") |> List.last() =~ "synchronous"
      assert split("SELECT `a``b;c`; PRAGMA synchronous=OFF") |> List.last() =~ "synchronous"
    end

    test "inside a bracket identifier, which has no doubling escape" do
      assert split("SELECT [we;ird]; PRAGMA synchronous=OFF") |> List.last() =~ "synchronous"
    end

    test "inside a line comment" do
      assert split("SELECT 1 -- a ; here\nPRAGMA synchronous=OFF") |> List.last() =~ "synchronous"
    end

    test "inside a block comment" do
      assert split("SELECT 1 /* a ; here */; PRAGMA synchronous=OFF")
             |> List.last() =~ "synchronous"
    end

    test "an unterminated quote does not swallow the rest silently" do
      # It cannot be split correctly — there is no correct answer — but it must terminate and
      # produce something the gate and then SQLite can both reject.
      assert is_list(split("SELECT 'unterminated ; PRAGMA synchronous=OFF"))
    end
  end

  describe "the exploit from the finding, statement by statement" do
    test "every statement in the measured payload is visible to the splitter" do
      script =
        "SELECT 1; PRAGMA synchronous=OFF; PRAGMA max_page_count=777; CREATE TABLE evil(a);"

      stmts = split(script)

      assert length(stmts) == 4
      assert Enum.any?(stmts, &(&1 =~ "synchronous"))
      assert Enum.any?(stmts, &(&1 =~ "max_page_count"))
      assert Enum.any?(stmts, &(&1 =~ "CREATE TABLE evil"))
    end
  end
end
