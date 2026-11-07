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
