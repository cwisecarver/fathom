defmodule Fathom.ShardExecutorNamedArgsTest do
  # Expert review 2026-10-20 #10: `ShardExecutor` read only `%Stmt{args: args}` and silently dropped
  # Hrana `named_args`, so every `:name` / `@name` / `$name` statement failed with a misleading
  # "expected N arguments, got 0". Invariant: a named argument binds to the parameter of that name
  # (with or without its prefix), mixes with positional ones, and a bad name is a clear error.
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.ShardExecutor
  alias Filo.{Error, Stmt, StmtResult}

  setup do
    shard = "named#{System.unique_integer([:positive])}"

    on_exit(fn ->
      _ = Fathom.Shards.drain(shard, 1_000)
      local = Path.join([Fathom.Shard.data_dir(), "#{shard}.db"])
      remote = Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])
      for base <- [local, remote], suffix <- ["", "-wal", "-shm"], do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp run(conn, sql, args, named),
    do: ShardExecutor.execute(conn, %Stmt{sql: sql, args: args, named_args: named})

  defp seeded(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = run(conn, "CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT, w TEXT)", [], [])
    conn
  end

  test "named args bind by name, with and without the prefix", %{shard: shard} do
    conn = seeded(shard)

    for {name_k, name_v} <- [{":k", ":v"}, {"k", "v"}] do
      assert {:ok, %StmtResult{affected_row_count: 1}} =
               run(
                 conn,
                 "INSERT OR REPLACE INTO kv (k, v) VALUES (:k, :v)",
                 [],
                 [{name_k, 7}, {name_v, "seven"}]
               )
    end

    assert {:ok, %StmtResult{rows: [["seven"]]}} =
             run(conn, "SELECT v FROM kv WHERE k = @key", [], [{"key", 7}])

    assert {:ok, %StmtResult{rows: [["seven"]]}} =
             run(conn, "SELECT v FROM kv WHERE k = $key", [], [{"$key", 7}])

    ShardExecutor.close(conn)
  end

  test "named and positional arguments mix", %{shard: shard} do
    conn = seeded(shard)

    # Positional `?` takes index 1; `:v` is index 2.
    assert {:ok, %StmtResult{affected_row_count: 1}} =
             run(conn, "INSERT INTO kv (k, v) VALUES (?, :v)", [1], [{":v", "one"}])

    assert {:ok, %StmtResult{rows: [["one"]]}} =
             run(conn, "SELECT v FROM kv WHERE k = ?", [1], [])

    ShardExecutor.close(conn)
  end

  test "a repeated named parameter binds every occurrence", %{shard: shard} do
    conn = seeded(shard)
    {:ok, _} = run(conn, "INSERT INTO kv (k, v, w) VALUES (1, 'a', 'a'), (2, 'a', 'b')", [], [])

    assert {:ok, %StmtResult{rows: [[1]]}} =
             run(conn, "SELECT k FROM kv WHERE v = :x AND w = :x", [], [{":x", "a"}])

    ShardExecutor.close(conn)
  end

  test "an unknown name is a clear error, not a crash or a silent NULL", %{shard: shard} do
    conn = seeded(shard)

    assert {:error, %Error{message: msg}} =
             run(conn, "SELECT v FROM kv WHERE k = :k", [], [{":nope", 1}])

    assert msg =~ "unknown named parameter"
    ShardExecutor.close(conn)
  end

  test "a missing parameter and an overlapping positional/named pair are errors", %{shard: shard} do
    conn = seeded(shard)

    assert {:error, %Error{message: m1}} =
             run(conn, "INSERT INTO kv (k, v) VALUES (:k, :v)", [], [{":k", 1}])

    assert m1 =~ "expected 2 arguments"

    # `?` is index 1 and so is `:k`: bound twice.
    assert {:error, %Error{message: m2}} =
             run(conn, "SELECT v FROM kv WHERE k = :k", [1], [{":k", 1}])

    assert m2 =~ "bound twice"
    ShardExecutor.close(conn)
  end

  test "values are bound, never interpolated", %{shard: shard} do
    conn = seeded(shard)
    evil = "x'); DROP TABLE kv; --"

    assert {:ok, _} = run(conn, "INSERT INTO kv (k, v) VALUES (1, :v)", [], [{":v", evil}])
    assert {:ok, %StmtResult{rows: [[^evil]]}} = run(conn, "SELECT v FROM kv", [], [])
    ShardExecutor.close(conn)
  end

  test "over the wire (Hrana WebSocket) named_args round-trip", %{shard: shard} do
    {:ok, sup, port} = HranaClient.start_listener()
    on_exit(fn -> HranaClient.stop_listener(sup) end)

    {:ok, client} = HranaClient.connect(port, shard)
    {:ok, client, _} = HranaClient.execute(client, "CREATE TABLE kv (k INTEGER, v TEXT)")

    {:ok, client, %{affected_row_count: 1}} =
      HranaClient.execute(client, "INSERT INTO kv VALUES (?, :v)", [5], [{":v", "five"}])

    assert {:ok, client, %{rows: [["five"]]}} =
             HranaClient.execute(client, "SELECT v FROM kv WHERE k = :k", [], [{"k", 5}])

    assert {:error, {:hrana_error, %{"message" => msg}}} =
             HranaClient.execute(client, "SELECT v FROM kv WHERE k = :k", [], [{":zz", 5}])

    assert msg =~ "unknown named parameter"
    HranaClient.close(client)
  end
end
