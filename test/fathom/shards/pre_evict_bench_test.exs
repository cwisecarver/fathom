defmodule Fathom.Shards.PreEvictBenchTest do
  @moduledoc """
  Admitted-open latency AT the `:max_open_shards` cap (perf review 2026-10-21 #14).

  At the cap, every admitted cold open used to evict a victim INLINE first: drain it and wait for
  its whole `terminate/2`, which ends in the lock release (an S3 conditional DELETE), before its
  own cold open began. The background low-watermark evictor drops idle shards before the cap is
  reached, so an admission finds a free slot.

  In-process stub: `FaultyStorage` with an injected release latency (the DELETE round trip) and
  pull latency (the GET), so the inline drop's storage cost is visible — on the plain `Local`
  double a release is a local unlink and the effect is invisible. Arms alternate within one run so
  machine drift lands on both sides. `@tag :bench`: excluded from the default suite.

      mix test test/fathom/shards/pre_evict_bench_test.exs --include bench
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards
  alias Fathom.Shards.Lru
  alias Fathom.Test.FaultyStorage

  @moduletag :bench
  @moduletag :capture_log
  @moduletag timeout: 600_000

  @release_ms 25
  @pull_ms 25
  @fill 64
  @opens 40
  @rounds 3

  @env [
    :shard_storage,
    :faulty_before,
    :storage_pull_delay_ms,
    :max_open_shards,
    :shard_idle_ms,
    :pre_evict_headroom
  ]

  setup do
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    Lru.reset()

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Lru.reset()
    end)

    Application.put_env(:fathom, :shard_storage, FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :storage_pull_delay_ms, @pull_ms)

    Application.put_env(
      :fathom,
      :faulty_before,
      {:release, fn _ -> Process.sleep(@release_ms) end}
    )

    :ok
  end

  defp uniq, do: "preevb_#{System.unique_integer([:positive])}"

  defp open_idle(id) do
    {:ok, pid, ref, _path} = Shards.checkout(id)
    Fathom.Shard.checkin(pid, ref)
    _ = :sys.get_state(pid)
    id
  end

  defp stop_all(ids), do: Enum.each(ids, &Shards.stop/1)

  defp timed_opens(n) do
    for _ <- 1..n do
      id = uniq()
      {us, _} = :timer.tc(fn -> open_idle(id) end)
      {us, id}
    end
  end

  defp pct(sorted, p), do: Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))

  defp summary(samples) do
    s = Enum.sort(samples)
    %{p50: pct(s, 0.5), p90: pct(s, 0.9), p99: pct(s, 0.99), n: length(s)}
  end

  # One arm: fill to the cap with idle shards, then time `@opens` admitted cold opens.
  defp arm(:uncapped) do
    Application.put_env(:fathom, :max_open_shards, :infinity)
    filled = for _ <- 1..@fill, do: open_idle(uniq())
    {samples, ids} = Enum.unzip(timed_opens(@opens))
    stop_all(filled ++ ids)
    samples
  end

  defp arm({:capped, headroom}) do
    Lru.reset()
    Application.put_env(:fathom, :pre_evict_headroom, headroom)
    # Finite while filling, so the Lru records the fill (it is a no-op with no cap).
    Application.put_env(
      :fathom,
      :max_open_shards,
      Registry.count(Fathom.ShardRegistry) + 10 * @fill
    )

    filled = for _ <- 1..@fill, do: open_idle(uniq())
    # The cap sits exactly at the current population: the node is FULL of idle shards.
    Application.put_env(:fathom, :max_open_shards, Registry.count(Fathom.ShardRegistry))
    {samples, ids} = Enum.unzip(timed_opens(@opens))
    Application.put_env(:fathom, :max_open_shards, :infinity)
    # Evicted shards are already gone; `stop/1` on them is a no-op.
    stop_all(filled ++ ids)
    samples
  end

  test "admitted-open latency at the cap: inline eviction vs the background pre-evictor" do
    arms = [:uncapped, {:capped, 0}, {:capped, :auto}]

    results =
      for round <- 1..@rounds, arm <- arms, reduce: %{} do
        acc ->
          s = arm(arm)
          IO.puts("round #{round} #{inspect(arm)}: #{inspect(summary(s))}")
          Map.update(acc, arm, s, &(&1 ++ s))
      end

    for arm <- arms,
        do: IO.puts("ALL #{inspect(arm)} (us): #{inspect(summary(Map.fetch!(results, arm)))}")
  end
end
