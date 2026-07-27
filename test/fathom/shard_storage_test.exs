defmodule Fathom.ShardStorageTest do
  # Exercises pull-on-wake / flush-on-idle (and that flush waits for connections
  # to drain) using the filesystem storage backend. Not async: shards are global.
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Stmt, StmtResult}

  setup do
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 50)

    shard = "store_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      for base <- [Path.join(local_dir(), "#{shard}.db"), Path.join(remote_dir(), "#{shard}.db")],
          suffix <- ["", "-wal", "-shm"],
          do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  test "a shard flushes to storage on idle and pulls it back on next wake", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    # Releasing the only connection lets the shard idle: flush, drop local, stop.
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 1000

    assert File.exists?(Path.join(remote_dir(), "#{shard}.db"))
    refute File.exists?(Path.join(local_dir(), "#{shard}.db"))

    # Next wake pulls the file back from storage and the data is there.
    {:ok, conn2} = ShardExecutor.open(shard)
    assert File.exists?(Path.join(local_dir(), "#{shard}.db"))

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE k = 1"))

    ShardExecutor.close(conn2)
  end

  test "the shard does not flush or stop while a connection is still checked out", %{shard: shard} do
    {:ok, a} = ShardExecutor.open(shard)
    {:ok, b} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(a, stmt("CREATE TABLE kv (v TEXT)"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    # Release A; B is still active, so the coordinator must stay up well past the
    # idle window (50ms) — it never arms the idle timer while a connection is out.
    :ok = ShardExecutor.close(a)
    refute_receive {:DOWN, ^ref, :process, ^coordinator, _}, 250

    assert {:ok, %StmtResult{}} = ShardExecutor.execute(b, stmt("INSERT INTO kv VALUES ('x')"))

    # Releasing the last connection lets it flush and stop.
    :ok = ShardExecutor.close(b)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 1000
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
