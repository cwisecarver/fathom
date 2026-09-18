defmodule Fathom.Shard.ConnectionPoolIntegrationTest do
  @moduledoc """
  End-to-end connection pooling through the real request path (`Fathom.ShardExecutor` → the
  coordinator's per-shard `HandlePool`).

  The invariant that shapes every test: **an idle shard holds ZERO pooled connections.** A pooled
  handle keeps a SQLite connection open, and the durability / position-stamp / flush-timer machinery
  assumes an idle shard's last stream already checkpointed+unlinked the WAL — so the pool is drained
  the instant `conns` hits 0 (`release/1`). Pooling therefore serves OVERLAPPING streams: a handle
  checked in while the shard is still busy is reused by the next stream. The reuse observable is the
  SQLite connection reference itself. Not async — shards are global and file-backed.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.HandlePool
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Stmt, StmtResult}

  setup do
    prev_pool = Application.get_env(:fathom, :connection_pool)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
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
  defp conn_of({_pid, _ref, conn, _id, _sc, _v, _o}), do: conn
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid

  test "pooling ON: a handle checked in while the shard is still busy is reused by the next stream",
       %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, %StmtResult{}} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, %StmtResult{}} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('a')"))

    # A SECOND stream keeps the shard busy, so closing A pools its handle instead of idle-draining.
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)

    :ok = ShardExecutor.close(ha)
    assert HandlePool.count(pool_of(pid)) == 1, "A's handle was pooled (the shard is still busy)"

    {:ok, hc} = ShardExecutor.open(shard)
    assert conn_of(hc) == conn_a, "the next stream reused A's pooled handle"
    assert HandlePool.count(pool_of(pid)) == 0

    assert {:ok, %StmtResult{rows: [["a"]]}} = ShardExecutor.execute(hc, stmt("SELECT v FROM kv"))
    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: an idle shard drains its pool — the durability invariant", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    # A is the ONLY stream — closing it takes the shard idle, which must drain the pool.
    :ok = ShardExecutor.close(ha)

    assert HandlePool.count(pool_of(pid)) == 0,
           "an idle shard must hold no pooled connection, or the next flush stamps the wrong position"

    {:ok, hb} = ShardExecutor.open(shard)
    assert conn_of(hb) != conn_a, "the drained handle was closed; the next stream opens fresh"
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: an uncommitted write from the reused stream does not survive", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('ghost')"))
    :ok = ShardExecutor.close(ha)

    {:ok, hc} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: []}} = ShardExecutor.execute(hc, stmt("SELECT v FROM kv")),
           "reset_for_reuse rolled back the reused handle's open transaction"

    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: scope isolation — a :ro stream is NOT handed the :rw pooled handle", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_rw = conn_of(ha)
    :ok = ShardExecutor.close(ha)
    assert HandlePool.count(pool_of(pid), :rw) == 1

    {:ok, hc} = ShardExecutor.open(shard, :ro)
    assert conn_of(hc) != conn_rw, "a :ro stream must never reuse the :rw handle"
    assert HandlePool.count(pool_of(pid), :rw) == 1, "the :rw handle stayed in its own bucket"
    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  test "pooling OFF (default): the handle is closed on checkin, never pooled", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, false)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    :ok = ShardExecutor.close(ha)

    assert pool_of(pid) == nil, "pooling off ⇒ the coordinator holds no pool at all"

    {:ok, hc} = ShardExecutor.open(shard)
    assert conn_of(hc) != conn_a, "each stream opens its own fresh handle when pooling is off"
    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: a busy shard terminating closes its pooled handles (none outlive the lease)",
       %{
         shard: shard
       } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    :ok = ShardExecutor.close(ha)
    assert HandlePool.count(pool_of(pid)) == 1, "A pooled while B keeps the shard busy"

    # Force-stop with B still checked out: the busy terminate clause runs close_pool/1.
    mref = Process.monitor(pid)
    Shards.stop(shard)
    assert_receive {:DOWN, ^mref, :process, ^pid, _}, 5_000

    assert {:error, _} = Sqlite3.execute(conn_a, "SELECT 1"),
           "close_pool/1 closed the pooled handle on terminate"
  end
end
