defmodule Fathom.Shard.ConnectionPoolIntegrationTest do
  @moduledoc """
  End-to-end connection pooling through the real request path (`Fathom.ShardExecutor` → the
  coordinator's per-shard `HandlePool`). The reuse observable is the SQLite connection reference
  itself: a stream that reuses a pooled handle gets back the exact `conn` the previous stream
  returned. Not async — shards are global and file-backed.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.HandlePool
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Stmt, StmtResult}

  setup do
    prev_pool = Application.get_env(:fathom, :connection_pool)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    # High idle so a shard does not drop (and drain its pool) between two sequential streams.
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    shard = "pool_it_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      restore(:connection_pool, prev_pool)
      restore(:shard_idle_ms, prev_idle)

      for base <- [
            Path.join(Fathom.Shard.data_dir(), "#{shard}.db"),
            Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}.db")
          ],
          suffix <- ["", "-wal", "-shm"],
          do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)
  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp pool_of(pid), do: :sys.get_state(pid).pool

  test "pooling ON: a closed stream's handle is pooled and the next stream reuses it", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, {pid, _ref, conn_a, _id, _sc, _v, _o} = h1} = ShardExecutor.open(shard)
    {:ok, %StmtResult{}} = ShardExecutor.execute(h1, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, %StmtResult{}} = ShardExecutor.execute(h1, stmt("INSERT INTO kv VALUES ('a')"))
    :ok = ShardExecutor.close(h1)

    assert HandlePool.count(pool_of(pid)) == 1,
           "the closed stream's handle was returned to the pool"

    {:ok, {^pid, _ref2, conn_b, _id2, _sc2, _v2, _o2} = h2} = ShardExecutor.open(shard)
    assert conn_b == conn_a, "the second stream reused the exact pooled handle"
    assert HandlePool.count(pool_of(pid)) == 0, "the handle was taken from the pool for the reuse"

    # It works and sees the committed write — reset_for_reuse rolls back only uncommitted state.
    assert {:ok, %StmtResult{rows: [["a"]]}} = ShardExecutor.execute(h2, stmt("SELECT v FROM kv"))
    :ok = ShardExecutor.close(h2)
  end

  test "pooling ON: an uncommitted write from the previous stream does not survive the reuse", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, h1} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h1, stmt("CREATE TABLE kv (v TEXT)"))
    # Leave a transaction open and uncommitted, then close (hands the handle back mid-txn).
    {:ok, _} = ShardExecutor.execute(h1, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(h1, stmt("INSERT INTO kv VALUES ('ghost')"))
    :ok = ShardExecutor.close(h1)

    {:ok, h2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: []}} = ShardExecutor.execute(h2, stmt("SELECT v FROM kv")),
           "reset_for_reuse rolled back the previous stream's open transaction"

    :ok = ShardExecutor.close(h2)
  end

  test "pooling ON: scope isolation — a :ro stream is NOT handed the :rw pooled handle", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, {pid, _r, conn_rw, _i, _s, _v, _o} = h1} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h1, stmt("CREATE TABLE kv (v TEXT)"))
    :ok = ShardExecutor.close(h1)
    assert HandlePool.count(pool_of(pid), :rw) == 1

    {:ok, {^pid, _r2, conn_ro, _i2, _s2, _v2, _o2} = h2} = ShardExecutor.open(shard, :ro)
    assert conn_ro != conn_rw, "a :ro stream must never reuse the :rw handle"
    assert HandlePool.count(pool_of(pid), :rw) == 1, "the :rw handle stayed in its own bucket"
    :ok = ShardExecutor.close(h2)
  end

  test "pooling OFF (default): the handle is closed on checkin, never pooled", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, false)

    {:ok, {pid, _r, conn_a, _i, _s, _v, _o} = h1} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h1, stmt("CREATE TABLE kv (v TEXT)"))
    :ok = ShardExecutor.close(h1)

    assert pool_of(pid) == nil, "pooling off ⇒ the coordinator holds no pool at all"

    {:ok, {^pid, _r2, conn_b, _i2, _s2, _v2, _o2} = h2} = ShardExecutor.open(shard)
    assert conn_b != conn_a, "each stream opens its own fresh handle when pooling is off"
    :ok = ShardExecutor.close(h2)
  end

  test "pooling ON: dropping the shard closes pooled handles (none outlive the lease)", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, {pid, _r, conn, _i, _s, _v, _o} = h1} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h1, stmt("CREATE TABLE kv (v TEXT)"))
    :ok = ShardExecutor.close(h1)
    assert HandlePool.count(pool_of(pid)) == 1

    mref = Process.monitor(pid)
    :ok = Shards.drain(shard)
    assert_receive {:DOWN, ^mref, :process, ^pid, _}, 5_000

    # close_pool/1 ran in terminate: the pooled handle is closed, so a statement on it now fails.
    assert {:error, _} = Sqlite3.execute(conn, "SELECT 1")
  end
end
