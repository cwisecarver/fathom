defmodule Fathom.Shard.PoolEvictionCloseTest do
  @moduledoc """
  Pool-EVICTION closes must not defer the durability flush of a busy shard (expert review
  2026-10-20 #22).

  ## The bug

  K concurrent streams on one shard against `max_per_scope: 1` evict a pooled handle on about a
  third of checkins (~570/s at K=16-32 on the rig). Each evicted handle was closed on the ORDERED
  close chain, which exists to order closes that may be the shard's LAST connection (those run the
  close-time checkpoint). An eviction close never is — `HandlePool.put/4` keeps the incoming handle
  — yet it went on the chain, and the `:durability_flush` deferred whenever ANY chain close was in
  flight (`closing_busy?/1`). With dirty-IO slow (2-30 ms per close) the chain served fewer closes
  per second than arrived: it grew without bound, and the hottest shard stopped flushing to object
  storage for the whole burst — object RPO no longer bounded by the flush interval.

  ## The fix under test

  Eviction closes from a pool that keeps a handle run in parallel (`side_closes`), still awaited by
  the next chain link and by terminate's `close_pool/1`. The flush on a busy shard no longer waits
  for them.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shard.{HandlePool, Storage}
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @keys [:connection_pool, :connection_pool_opts, :shard_idle_ms, :shard_pool_close_hook]

  setup do
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})
    Application.put_env(:fathom, :connection_pool, true)
    Application.put_env(:fathom, :connection_pool_opts, max_per_scope: 1, drain_grace_ms: 60_000)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    shard = "pool_evict_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      _ = Shards.stop(shard)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid

  defp count_deferrals do
    counts = :counters.new(1, [])
    handler = "evict-close-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush_close_deferred],
      fn _e, _m, _meta, _ -> :counters.add(counts, 1, 1) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    counts
  end

  test "a busy shard's flush is not deferred behind slow eviction closes", %{shard: shard} do
    # A schema, flushed, so the later write is the only dirt.
    {:ok, h0} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h0, stmt("CREATE TABLE t (a INTEGER)"))
    coordinator = pid_of(h0)

    # Four concurrent streams. One stays open for the rest of the test: the shard is BUSY, so no
    # close in flight can be its last connection and nothing about "idle ⇒ WAL gone" applies.
    hs = for _ <- 1..3, do: elem(ShardExecutor.open(shard), 1)
    {:ok, _} = ShardExecutor.execute(h0, stmt("INSERT INTO t VALUES (1)"))

    # Slow dirty-IO, as on the rig's bad mode: every close the coordinator spawns takes 1.5 s.
    Application.put_env(:fathom, :shard_pool_close_hook, fn _ -> Process.sleep(1_500) end)

    # Three checkins against a 1-handle pool: one pooled, two evicted.
    for h <- hs, do: :ok = ShardExecutor.close(h)
    st = :sys.get_state(coordinator)
    assert HandlePool.count(st.pool) == 1
    assert map_size(st.conns) == 1, "precondition: the shard is still busy"

    deferrals = count_deferrals()
    t0 = System.monotonic_time(:millisecond)
    assert :ok = Shards.flush(shard)
    took = System.monotonic_time(:millisecond) - t0

    # Pre-fix the two eviction closes were CHAINED (3 s) and every flush tick in that window
    # backed off; post-fix they run in parallel and the flush does not wait for them at all.
    assert :counters.get(deferrals, 1) == 0,
           "the flush was deferred behind eviction closes on a busy shard"

    assert took < 1_000, "the flush took #{took} ms"

    Application.delete_env(:fathom, :shard_pool_close_hook)
    :ok = ShardExecutor.close(h0)
  end

  test "terminate still waits for in-flight eviction closes before touching the files", %{
    shard: shard
  } do
    {:ok, h0} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h0, stmt("CREATE TABLE t (a INTEGER)"))
    coordinator = pid_of(h0)
    hs = for _ <- 1..2, do: elem(ShardExecutor.open(shard), 1)

    test = self()

    Application.put_env(:fathom, :shard_pool_close_hook, fn _ ->
      send(test, :closing)
      Process.sleep(300)
      send(test, :closed)
    end)

    for h <- hs, do: :ok = ShardExecutor.close(h)
    assert_receive :closing, 1_000
    assert map_size(:sys.get_state(coordinator).side_closes) == 1

    :ok = ShardExecutor.close(h0)
    ref = Process.monitor(coordinator)
    _ = Shards.stop(shard)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000

    # The eviction close finished before the coordinator was gone (close_pool/1 waited for it).
    assert_received :closed
  end
end
