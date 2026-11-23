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

  # Expert review 2026-10-13 #33. A Hrana sequence is raw protobuf bytes, and Filo's decode does not
  # validate UTF-8. `do_split/3` and `quoted/3` matched only `<<c::utf8, _>>`, so one invalid byte
  # raised FunctionClauseError in the caller's stream. Pre-fix, every test below raised. The
  # quoted-region case also guards the bypass: an invalid byte inside a string must not desync the
  # scanner, so the statement after the string is still seen.
  describe "invalid UTF-8 in a script (expert review 2026-10-13 #33)" do
    test "an invalid byte at the top level splits without raising, and later statements are seen" do
      stmts = split(<<"SELECT 1; ", 0xFF, "; PRAGMA synchronous=OFF">>)

      assert List.last(stmts) =~ "synchronous",
             "the statement after an invalid top-level byte was lost"
    end

    test "an invalid byte inside a quoted string does not desync the scanner" do
      stmts = split(<<"INSERT INTO t VALUES ('a", 0xFE, "b'); PRAGMA max_page_count=777">>)

      assert length(stmts) == 2,
             "an invalid byte inside a string literal hid the statement boundary"

      assert List.last(stmts) =~ "max_page_count"
    end

    test "an invalid byte inside a double-quoted identifier and a comment is tolerated" do
      stmts = split(<<"SELECT \"c", 0xC3, "\" FROM t; -- note ", 0xA9, "\nSELECT 2">>)

      assert List.last(stmts) =~ "SELECT 2"
    end
  end

  # Audit 2026-11-22 #1. SQLite's tokenizer reads `$name(...)`, `:name(...)` and `@name(...)` as ONE
  # variable token whose parenthesised part swallows every byte up to whitespace or `)`, quotes and
  # comment openers included (sqlite3.c, CC_DOLLAR/CC_VARALPHA, no SQLITE_OMIT_TCL_VARIABLE). A scanner
  # that opens a string at that `'` is inside-out for the rest of the script: exactly the under-split
  # this module exists to prevent. Probed on the unfixed code first: `execute_sequence` ran the hidden
  # CREATE TABLE.
  describe "SQLite's TCL-style variable token" do
    test "a quote inside $name(...) does not open a string" do
      for sigil <- ["$", ":", "@"] do
        stmts = split("SELECT #{sigil}a('); CREATE TABLE evil(x); SELECT #{sigil}b(')")

        assert length(stmts) == 3, "sigil #{sigil}: #{inspect(stmts)}"
        assert Enum.at(stmts, 1) == "CREATE TABLE evil(x)"
      end
    end

    test "comment openers and brackets inside the parentheses are not comments or identifiers" do
      assert ["SELECT $a(--)", "PRAGMA synchronous=OFF", "SELECT $b(/*)"] =
               split("SELECT $a(--); PRAGMA synchronous=OFF; SELECT $b(/*)")

      assert ["SELECT $a([)", "DROP TABLE t", "SELECT $b(])"] =
               split("SELECT $a([); DROP TABLE t; SELECT $b(])")
    end

    test "a `;` inside the parentheses is part of the token, as in SQLite" do
      assert ["SELECT $a(x;y)", "SELECT 2"] = split("SELECT $a(x;y); SELECT 2")
    end

    test "whitespace before the `)` ends the token, so the quote after it IS a string again" do
      # SQLite makes this token illegal and stops with an error, so nothing after it runs; what matters
      # is that the splitter does not hide what comes after the string.
      assert ["SELECT $a( 'x;y')", "SELECT 2"] = split("SELECT $a( 'x;y'); SELECT 2")
    end

    test "`$` inside an identifier is an identifier character, not a variable start" do
      # Review of the first version of the fix: `t$x('a b;' TEXT)` is the identifier `t$x` followed by a
      # real string literal. Scanning `x(...)` as a TCL variable ended at the space, split at the `;`
      # INSIDE the literal and reopened a quote at its tail, hiding the PRAGMA after it.
      assert ["CREATE TABLE t$x('a b;' TEXT)", "PRAGMA writable_schema=ON", "SELECT 1"] =
               split("CREATE TABLE t$x('a b;' TEXT); PRAGMA writable_schema=ON; SELECT 1")

      assert ["SELECT abc$x('a;b')"] = split("SELECT abc$x('a;b')")
      assert ["SELECT a_1$b('x;y')", "SELECT 2"] = split("SELECT a_1$b('x;y'); SELECT 2")

      # ...while a `$` that starts a token (after a space, a comma, an operator) is still a variable.
      assert ["SELECT 1,$a('), (SELECT 2", "SELECT 3"] ==
               split("SELECT 1,$a('), (SELECT 2; SELECT 3")
    end

    test "a UTF-8 BOM before `$` is whitespace, unless it continues an identifier" do
      # Security review of the fix: the BOM's last byte (0xBF) is >= 0x80, an identifier character, but
      # SQLite's tokenizer reads a BOM that begins a token as whitespace (CC_BOM), so the `$` after it
      # starts a variable. Judged as an identifier tail it hid the middle statement.
      bom = <<0xEF, 0xBB, 0xBF>>
      tail = "('); CREATE TABLE evil(x); SELECT $b(')"

      for prefix <- ["SELECT ", "SELECT 1, ", "SELECT 1; SELECT ", ""] do
        stmts = split(prefix <> bom <> "$a" <> tail)
        assert Enum.any?(stmts, &(&1 == "CREATE TABLE evil(x)")), inspect(prefix)
      end

      # ...and a BOM inside a running identifier is part of it, so the `$` continues the identifier.
      assert ["CREATE TABLE t" <> _, "PRAGMA writable_schema=ON", "SELECT 1"] =
               split(
                 "CREATE TABLE t" <> bom <> "$x('a b;' TEXT); PRAGMA writable_schema=ON; SELECT 1"
               )
    end

    test "a variable that starts after every kind of token boundary still hides nothing" do
      rest = "('); CREATE TABLE e(x); SELECT $b(')"

      for lead <- [~s("q"), "[q]", "`q`", "/*c*/", "--c\n", "x'00'", "(", "1,", " "] do
        stmts = split("SELECT " <> lead <> "$a" <> rest)
        assert "CREATE TABLE e(x)" in stmts, inspect(lead)
      end

      for sigil <- ["#", "$", ":", "@"] do
        assert "CREATE TABLE e(x)" in split("SELECT #{sigil}a" <> rest), sigil
      end
    end

    test "name edge cases: $$( ::a( a::( non-ASCII, and `$(` with no name" do
      for name <- ["$", "::a", "a::", "\u00E9", "a$b"] do
        stmts = split("SELECT $#{name}" <> "('); CREATE TABLE e(x); SELECT $b(')")
        assert "CREATE TABLE e(x)" in stmts, name
      end

      assert ["SELECT $a((x))", "SELECT 2"] = split("SELECT $a((x)); SELECT 2")
    end

    test "ordinary variables and `::` names keep their old behaviour" do
      assert ["SELECT :a, $b, @c, ?1", "SELECT 2"] = split("SELECT :a, $b, @c, ?1; SELECT 2")
      assert ["SELECT $a::b", "SELECT 'x;y'"] = split("SELECT $a::b; SELECT 'x;y'")
      assert ["SELECT 'a:b(' , \"c\"", "SELECT 2"] = split("SELECT 'a:b(' , \"c\"; SELECT 2")
    end

    test "a statement hidden behind the token is still refused by the sequence gate" do
      # ATTACH is a blocked verb. Before the fix the gate saw one fragment (`select ...`) and passed it
      # to SQLite, which ran the hidden ATTACH (or its authorizer refused it, never the gate).
      assert Enum.any?(
               split("SELECT $a('); ATTACH DATABASE ':memory:' AS x; SELECT $b(')"),
               &String.starts_with?(&1, "ATTACH")
             )
    end
  end
end
