defmodule Fathom.Shard.CoordinatorGcPolicyTest do
  @moduledoc """
  Expert review 2026-07-24 #9, moved into the default suite by 2026-11-07 #84 (it lived in
  `bench_test.exs`, which is `:bench`-tagged and run by neither `mix test` nor CI).

  Pins the coordinator's memory POLICY, not a post-GC byte count: a test that forces
  `:erlang.garbage_collect/1` measures the same figure with or without the policy (verified on the
  original change, 2 KiB vs 3 KiB), because forcing a sweep is precisely what the policy automates.
  The retained-heap win is measured by `fanout_kb_per_shard` in the real bench gate and by
  `chaos.sh density`; what belongs in a unit guard is that the flags are set.

  The `max_heap_size` half is the safety-critical one: a coordinator killed at a heap limit
  mid-`terminate/2` skips the final fence + checkpoint + PUT + lease release, a durability
  regression that can also strand a lease. It must stay unset.
  """
  use Fathom.ClusterShardCase, async: false

  test "a coordinator is spawned with the reclaiming GC policy and no heap-kill limit", %{
    shard: shard
  } do
    {:ok, pid} = Shards.ensure(shard)
    on_exit(fn -> Shards.drain(shard, 2_000) end)
    {:garbage_collection, gc} = Process.info(pid, :garbage_collection)

    assert Keyword.fetch!(gc, :fullsweep_after) == 0,
           "coordinators must full-sweep on every GC — it is the only thing that reclaims the old " <>
             "heap and returns pages, and retained heap dominates per-shard cost at 30k/node"

    # ERTS reports the limit as 0 (or a map with size 0) when unset.
    max_heap = Keyword.get(gc, :max_heap_size)
    size = if is_map(max_heap), do: Map.get(max_heap, :size, 0), else: max_heap || 0

    assert size == 0,
           "max_heap_size must stay UNSET on coordinators: a heap-limit kill mid-terminate/2 " <>
             "skips the final fence + checkpoint + PUT + lease release"
  end
end
