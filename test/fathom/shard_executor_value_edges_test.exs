defmodule Fathom.ShardExecutorValueEdgesTest do
  @moduledoc """
  Expert review 2026-11-07 #69 (the `lib/` half; the `Filo.Value.decode` int64 range check lives in
  the sibling `../filo` repo and is a follow-up for the filo release).

    * an integer bind outside SQLite's signed 64-bit range raised inside `Sqlite3.bind` and reached
      the tenant as an opaque `argument error: 9223372036854775808`; it is now a typed
      `FILO_BIND_OUT_OF_RANGE` naming the parameter;
    * a REAL cell that is ±Infinity (`SELECT 9e999`, an overflowing `SUM`, a stored `1e999`) makes
      the NIF raise while building the row, so the whole `SELECT *` failed opaquely and an Inf stored
      in a table was unreadable; it is now a typed `FILO_NON_FINITE_REAL` naming the result columns
      (Hrana JSON cannot carry Infinity), and the statement is left reset so the stream goes on;
    * the `execute raised` warning was unthrottled, a log-flood lever for a tenant that finds any
      raising input; it now goes through `Fathom.LogLimiter`.

  The review described the bind failure as an `ErlangError` `{:badarg, _}` that `Connection.query/4`
  did not rescue. On this Elixir that exception is normalised to `ArgumentError` ("argument error:
  <value>"), which `query/4` already rescues, so the rescue was not what failed: the message was.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shard.Connection
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Error, Stmt, StmtResult}

  @int64_max 9_223_372_036_854_775_807
  @int64_min -9_223_372_036_854_775_808

  setup do
    shard = "valedge_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    {:ok, h} = ShardExecutor.open(shard)
    on_exit(fn -> ShardExecutor.close(h) end)
    %{h: h}
  end

  defp run(h, sql, args \\ [], named \\ []),
    do: ShardExecutor.execute(h, %Stmt{sql: sql, args: args, named_args: named})

  describe "integer binds" do
    test "the int64 boundaries bind; one past either end is a typed error", %{h: h} do
      assert {:ok, %StmtResult{rows: [[@int64_max]]}} = run(h, "SELECT ?", [@int64_max])
      assert {:ok, %StmtResult{rows: [[@int64_min]]}} = run(h, "SELECT ?", [@int64_min])

      for big <- [@int64_max + 1, @int64_min - 1, 10 ** 30] do
        assert {:error, %Error{code: "FILO_BIND_OUT_OF_RANGE", status: 400, message: msg}} =
                 run(h, "SELECT ?", [big])

        refute msg =~ "internal error"
        assert msg =~ "parameter 1"
      end
    end

    test "the offending parameter is named, positional or by name", %{h: h} do
      assert {:error, %Error{code: "FILO_BIND_OUT_OF_RANGE", message: msg}} =
               run(h, "SELECT ?, ?", [1, @int64_max + 1])

      assert msg =~ "parameter 2"

      assert {:error, %Error{code: "FILO_BIND_OUT_OF_RANGE", message: msg}} =
               run(h, "SELECT :a, :b", [], [{"a", 1}, {"b", @int64_max + 1}])

      assert msg =~ ~s(parameter "b")
    end

    test "an out-of-range bind does not log an 'execute raised' warning", %{h: h} do
      log = capture_log(fn -> run(h, "SELECT ?", [@int64_max + 1]) end)
      refute log =~ "execute raised"
    end

    test "an unrelated bind failure keeps its own message", %{h: h} do
      assert {:error, %Error{message: msg}} = run(h, "SELECT ?, ?", [1])
      assert msg =~ "expected 2 arguments"
    end
  end

  describe "non-finite REAL cells" do
    test "SELECT 9e999 is a typed error naming the column", %{h: h} do
      assert {:error, %Error{code: "FILO_NON_FINITE_REAL", status: 400, message: msg}} =
               run(h, "SELECT 9e999 AS big")

      assert msg =~ "big"
      assert msg =~ "Infinity"
    end

    test "-Infinity too, and an Inf stored in a table is reported rather than crashing", %{h: h} do
      assert {:error, %Error{code: "FILO_NON_FINITE_REAL"}} = run(h, "SELECT -9e999")

      {:ok, _} = run(h, "CREATE TABLE r (id INTEGER PRIMARY KEY, x REAL)")
      {:ok, _} = run(h, "INSERT INTO r(x) VALUES (1.5), (9e999)")

      assert {:error, %Error{code: "FILO_NON_FINITE_REAL", message: msg}} =
               run(h, "SELECT id, x FROM r")

      assert msg =~ "x"

      # The rest of the table is still readable by a query that avoids the cell.
      assert {:ok, %StmtResult{rows: [[1.5]]}} = run(h, "SELECT x FROM r WHERE x < 10")
    end

    test "the statement is reset, so the stream carries on and a DDL is not blocked", %{h: h} do
      {:ok, _} = run(h, "CREATE TABLE r (x REAL)")
      {:ok, _} = run(h, "INSERT INTO r VALUES (9e999)")

      for _ <- 1..3 do
        assert {:error, %Error{code: "FILO_NON_FINITE_REAL"}} = run(h, "SELECT x FROM r")
      end

      assert {:ok, %StmtResult{rows: [[1]]}} = run(h, "SELECT 1")

      # An unreset read statement would hold a read transaction and block this checkpoint-class DDL.
      assert {:ok, _} = run(h, "CREATE INDEX r_x ON r(x)")
    end

    test "Connection.query returns the typed reason, not a raise", %{h: h} do
      {_pid, _ref, conn, _id, _scope, _ver, _opts} = h

      assert {:error, {:non_finite_real, ["big"]}} =
               Connection.query(conn, "SELECT 9e999 AS big", [])
    end
  end

  describe "the `execute raised` warning" do
    test "is rate limited per node", %{h: h} do
      # A non-binary `sql` is not a request Filo can produce; it is the cheapest input that raises
      # inside `do_execute/2` (FunctionClauseError) and so reaches the rescue.
      log =
        capture_log(fn ->
          for _ <- 1..40, do: ShardExecutor.execute(h, %Stmt{sql: :not_sql})
        end)

      raised = log |> String.split("\n") |> Enum.count(&(&1 =~ "execute raised"))

      assert raised >= 1, "the first occurrence must still be logged"
      # LogLimiter admits 5 per 1 s window; the 40 calls span at most a couple of windows.
      assert raised <= 10, "40 raising requests logged #{raised} warnings"
    end

    test "still returns the fixed tenant-facing error", %{h: h} do
      capture_log(fn ->
        assert {:error, %Error{message: "internal error executing statement"}} =
                 ShardExecutor.execute(h, %Stmt{sql: :not_sql})
      end)
    end
  end
end
