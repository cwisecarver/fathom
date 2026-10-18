defmodule Fathom.Shard.Storage.HeartbeatCacheTest do
  @moduledoc """
  Expert review 2026-10-17 #29. Symptom: a failover burst (many steals from one dead owner at once)
  all missed the TTL memo together and each issued the identical heartbeat GET. Invariant: concurrent
  `fetch/3` calls for one owner run the fetch ONCE; errors are neither cached nor allowed to leave a
  claim behind; a claim left by a killed winner does not stall later callers.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.HeartbeatCache

  setup do
    # Normally already running under the application supervisor.
    if Process.whereis(HeartbeatCache) == nil, do: start_supervised!({HeartbeatCache, []})
    HeartbeatCache.reset()
    :ok
  end

  defp counted(counter, value, sleep_ms) do
    fn ->
      :counters.add(counter, 1, 1)
      Process.sleep(sleep_ms)
      value
    end
  end

  test "concurrent fetches for one owner run the fetch once and share the value" do
    counter = :counters.new(1, [])
    value = {:ok, %{hb: 1}, 1234}

    results =
      1..20
      |> Enum.map(fn _ ->
        Task.async(fn ->
          HeartbeatCache.fetch("dead-node", 1_000, counted(counter, value, 100))
        end)
      end)
      |> Task.await_many(5_000)

    assert Enum.all?(results, &(&1 == value))
    assert :counters.get(counter, 1) == 1, "single-flight did not collapse the burst"
  end

  test "different owners do not share a flight" do
    counter = :counters.new(1, [])

    for owner <- ["a", "b", "c"] do
      Task.async(fn ->
        HeartbeatCache.fetch(owner, 1_000, counted(counter, {:ok, owner, 1}, 50))
      end)
    end
    |> Task.await_many(5_000)

    assert :counters.get(counter, 1) == 3
  end

  test "errors are not cached and release the claim" do
    counter = :counters.new(1, [])

    assert {:error, :boom} =
             HeartbeatCache.fetch("o", 1_000, counted(counter, {:error, :boom}, 0))

    assert {:ok, :v, 1} = HeartbeatCache.fetch("o", 1_000, counted(counter, {:ok, :v, 1}, 0))
    assert :counters.get(counter, 1) == 2
    # and the success is now memoised
    assert {:ok, :v, 1} = HeartbeatCache.fetch("o", 1_000, counted(counter, :unused, 0))
    assert :counters.get(counter, 1) == 2
  end

  test "a claim stranded by a killed winner is evicted, not waited on forever" do
    stale = System.monotonic_time(:millisecond) - 60_000
    :ets.insert(HeartbeatCache, {{:inflight, "o"}, self(), stale})

    {us, value} =
      :timer.tc(fn -> HeartbeatCache.fetch("o", 1_000, fn -> {:ok, :fresh, 1} end) end)

    assert value == {:ok, :fresh, 1}
    assert us < 500_000
  end

  test "ttl 0 (memo disabled) is a plain per-call read" do
    counter = :counters.new(1, [])
    HeartbeatCache.fetch("o", 0, counted(counter, {:ok, 1, 1}, 0))
    HeartbeatCache.fetch("o", 0, counted(counter, {:ok, 1, 1}, 0))
    assert :counters.get(counter, 1) == 2
  end
end
