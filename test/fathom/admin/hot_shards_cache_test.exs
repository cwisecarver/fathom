defmodule Fathom.Admin.HotShardsCacheTest do
  @moduledoc """
  Expert review 2026-10-15 #14: `AdminShardsLive` recomputed `Fleet.hot_shards/0` (a load-window scan
  of `shard_load_samples`) on its own 5 s timer per open tab. Inside the TTL every tab must reuse one
  result, and at expiry exactly one caller computes (single-flight), like `migrations_page/1`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Admin.HotShardsCache

  setup do
    prev = Application.get_env(:fathom, :admin_hot_shards_cache_ms)
    Application.put_env(:fathom, :admin_hot_shards_cache_ms, 60_000)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :admin_hot_shards_cache_ms),
        else: Application.put_env(:fathom, :admin_hot_shards_cache_ms, prev)
    end)

    start_supervised!(HotShardsCache)
    :ok
  end

  defp counting(counter, value) do
    fn ->
      :counters.add(counter, 1, 1)
      value
    end
  end

  test "N sequential calls inside the TTL compute once" do
    counter = :counters.new(1, [])

    for _ <- 1..10, do: assert(HotShardsCache.get(counting(counter, [:hot])) == [:hot])

    assert :counters.get(counter, 1) == 1
  end

  test "TTL 0 recomputes every call" do
    Application.put_env(:fathom, :admin_hot_shards_cache_ms, 0)
    counter = :counters.new(1, [])

    for _ <- 1..3, do: HotShardsCache.get(counting(counter, :v))

    assert :counters.get(counter, 1) == 3
  end

  test "concurrent callers at expiry compute once; the rest serve the stale set" do
    stale_at = System.monotonic_time(:millisecond) - 120_000
    :ets.insert(:fathom_admin_hot_shards, {:page, {stale_at, :stale}})

    counter = :counters.new(1, [])
    me = self()

    compute = fn ->
      :counters.add(counter, 1, 1)
      send(me, {:computing, self()})

      receive do
        :release -> :fresh
      end
    end

    for _ <- 1..8, do: spawn_link(fn -> send(me, {:result, HotShardsCache.get(compute)}) end)

    assert_receive {:computing, winner}, 2_000
    for _ <- 1..7, do: assert_receive({:result, :stale}, 2_000)

    send(winner, :release)
    assert_receive {:result, :fresh}, 2_000
    assert :counters.get(counter, 1) == 1
  end

  test "without the owner process there is no table and it computes uncached" do
    stop_supervised!(HotShardsCache)
    counter = :counters.new(1, [])

    assert HotShardsCache.get(counting(counter, :x)) == :x
    assert HotShardsCache.get(counting(counter, :x)) == :x
    assert :counters.get(counter, 1) == 2
  end
end
