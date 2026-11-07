defmodule Fathom.ShardExecutorScriptSchemaGenTest do
  @moduledoc """
  Expert review 2026-11-07 #20 and #79.

  #20: DDL that arrives through a Hrana `sequence` (`executescript`) never bumped the node-wide
  `SchemaGen`: `run_script` purged only ITS OWN connection's statement cache, so a sibling stream
  (or a pooled handle) that had cached `SELECT *` kept the old column list while SQLite recompiled
  the statement at step and returned wider rows: two column names over three-wide rows.

  #79: `bump_schema_gen/1` arms a per-connection `{ShardExecutor, :ddl_in_txn, conn}` key in the
  stream process's dictionary when DDL leaves a transaction open; `close/1` never deleted it, so a
  long-lived WebSocket process kept one entry (and the `conn` resource) per abandoned stream.

  Not async: `SchemaGen` is a node-global counter (see `schema_gen_test.exs`).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.SchemaGen
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Stmt, StmtResult}

  setup do
    shard = "scriptgen_#{System.unique_integer([:positive])}"
    prev_pool = Application.get_env(:fathom, :connection_pool)

    on_exit(fn ->
      if is_nil(prev_pool),
        do: Application.delete_env(:fathom, :connection_pool),
        else: Application.put_env(:fathom, :connection_pool, prev_pool)

      Shards.drain(shard, 2_000)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql}

  defp run!(h, sql) do
    {:ok, result} = ShardExecutor.execute(h, stmt(sql))
    result
  end

  defp seed!(h) do
    run!(h, "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")
    run!(h, "INSERT INTO t VALUES (1, 'hello')")
  end

  defp assert_widened(%StmtResult{cols: cols, rows: [row]}) do
    assert length(cols) == length(row),
           "columns #{inspect(cols)} over row #{inspect(row)}: the stream kept a column list " <>
             "cached before the script's DDL"

    assert cols == ["id", "x", "y"]
  end

  for pooled? <- [false, true] do
    test "a sibling re-reads its columns after a script's ALTER TABLE (pooled: #{pooled?})",
         %{shard: shard} do
      Application.put_env(:fathom, :connection_pool, unquote(pooled?))

      {:ok, writer} = ShardExecutor.open(shard)
      seed!(writer)

      {:ok, first} = ShardExecutor.open(shard)
      assert %StmtResult{cols: ["id", "x"], rows: [[1, "hello"]]} = run!(first, "SELECT * FROM t")

      sibling =
        if unquote(pooled?) do
          # The pooled handle carries the cached statement (and its column list) to the next stream.
          :ok = ShardExecutor.close(first)
          {:ok, reused} = ShardExecutor.open(shard)

          assert elem(reused, 2) == elem(first, 2),
                 "fixture: the next stream did not reuse the pooled handle"

          reused
        else
          first
        end

      assert :ok =
               ShardExecutor.execute_sequence(
                 writer,
                 "ALTER TABLE t ADD COLUMN y TEXT DEFAULT ('0');"
               )

      # Pre-fix this answered ["id","x"] over [1,"hello","0"].
      assert_widened(run!(sibling, "SELECT * FROM t"))

      :ok = ShardExecutor.close(sibling)
      :ok = ShardExecutor.close(writer)
    end
  end

  test "a script that FAILS after its DDL committed still invalidates siblings", %{shard: shard} do
    {:ok, writer} = ShardExecutor.open(shard)
    seed!(writer)
    {:ok, sibling} = ShardExecutor.open(shard)
    assert %StmtResult{cols: ["id", "x"]} = run!(sibling, "SELECT * FROM t")

    assert {:error, %Filo.Error{}} =
             ShardExecutor.execute_sequence(
               writer,
               "ALTER TABLE t ADD COLUMN y TEXT DEFAULT ('0'); SELECT * FROM no_such_table;"
             )

    assert_widened(run!(sibling, "SELECT * FROM t"))

    :ok = ShardExecutor.close(sibling)
    :ok = ShardExecutor.close(writer)
  end

  test "a script's DDL that is still uncommitted bumps again when its transaction ends",
       %{shard: shard} do
    {:ok, writer} = ShardExecutor.open(shard)
    seed!(writer)
    {:ok, sibling} = ShardExecutor.open(shard)
    run!(sibling, "SELECT * FROM t")

    assert :ok =
             ShardExecutor.execute_sequence(
               writer,
               "BEGIN; ALTER TABLE t ADD COLUMN y TEXT DEFAULT ('0');"
             )

    # Inside the window the committed schema is still two columns, and the sibling re-caches it.
    assert %StmtResult{cols: ["id", "x"], rows: [[1, "hello"]]} = run!(sibling, "SELECT * FROM t")

    run!(writer, "COMMIT")
    assert_widened(run!(sibling, "SELECT * FROM t"))

    :ok = ShardExecutor.close(sibling)
    :ok = ShardExecutor.close(writer)
  end

  test "a script that leaves the main schema unchanged does not bump the node-wide generation",
       %{shard: shard} do
    {:ok, h} = ShardExecutor.open(shard)
    seed!(h)

    before = SchemaGen.current()

    assert :ok =
             ShardExecutor.execute_sequence(
               h,
               "INSERT INTO t VALUES (2, 'b'); INSERT INTO t VALUES (3, 'c'); " <>
                 "CREATE TEMP TABLE scratch (a INTEGER);"
             )

    assert SchemaGen.current() == before,
           "a data-only script bumped the node-wide generation (every cached statement on the " <>
             "node re-prepared)"

    assert :ok = ShardExecutor.execute_sequence(h, "CREATE TABLE u (a INTEGER);")
    assert SchemaGen.current() > before, "a script that changed the main schema must bump"

    :ok = ShardExecutor.close(h)
  end

  test "close/1 deletes the :ddl_in_txn key of a stream that abandoned its DDL transaction",
       %{shard: shard} do
    {:ok, h} = ShardExecutor.open(shard)
    run!(h, "BEGIN")
    run!(h, "CREATE TABLE t (x)")

    assert Enum.any?(Process.get_keys(), &ddl_in_txn_key?/1),
           "fixture: the DDL inside a transaction did not arm the :ddl_in_txn key"

    :ok = ShardExecutor.close(h)

    refute Enum.any?(Process.get_keys(), &ddl_in_txn_key?/1),
           "close/1 left the per-connection :ddl_in_txn key in the process dictionary"
  end

  defp ddl_in_txn_key?({ShardExecutor, :ddl_in_txn, _conn}), do: true
  defp ddl_in_txn_key?(_), do: false
end
