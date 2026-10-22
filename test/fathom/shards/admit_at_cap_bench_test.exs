defmodule Fathom.Shards.AdmitAtCapBenchTest do
  @moduledoc """
  Concurrent cold admissions AT the `:max_open_shards` cap (expert review 2026-10-21 r7 #5, #7).
  `@tag :bench`: excluded from the default suite.

      mix test test/fathom/shards/admit_at_cap_bench_test.exs --include bench

  In-process: `FaultyStorage` with 25 ms injected on the lock release (the S3 DELETE every victim
  drop ends with) and on the pull, a node filled to the cap with idle shards, then 160 admissions of
  new ids at concurrency 16. The metrics collector is on, as in prod, so clean victims are known.

  Guards (order of magnitude, not exact latency):
    * `admit_at_cap_conc` (#5): with no pre-eviction headroom the population never exceeds
      cap + K, and every `evicted` event is a real victim (population delta + admissions). Before
      rv6 F10 this measured 352/340 over a cap of 256, and 160 events for 64-76 victims.
    * `cold_open_at_cap_k16` (#7): with the pre-evictor on (`:auto`), p50 stays within 1.3x of the
      uncapped p50 and under 15% of admissions evict inline. Before r7 #7: p50 52-54 ms vs 27-28
      uncapped, 81-90 of 160 inline; after: 29-31 ms, 3-16 inline.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards
  alias Fathom.Shards.Lru
  alias Fathom.Test.FaultyStorage

  @moduletag :bench
  @moduletag :capture_log
  @moduletag timeout: 300_000

  @fill 256
  @opens 160
  @k 16

  @env [
    :shard_storage,
    :faulty_before,
    :storage_pull_delay_ms,
    :max_open_shards,
    :shard_idle_ms,
    :pre_evict_headroom,
    :metrics_collector
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
    Application.put_env(:fathom, :storage_pull_delay_ms, 25)
    Application.put_env(:fathom, :metrics_collector, true)
    Application.put_env(:fathom, :faulty_before, {:release, fn _ -> Process.sleep(25) end})
    :ok
  end

  defp uniq, do: "admcap_#{System.unique_integer([:positive])}"

  defp open_idle(id) do
    case Shards.checkout(id) do
      {:ok, pid, ref, _path} ->
        Fathom.Shard.checkin(pid, ref)
        _ = :sys.get_state(pid)
        :ok

      {:error, _} = error ->
        error
    end
  end

  # `@opens` admissions at concurrency `@k`: {latencies_us, refusals, inline evictions}.
  defp timed_opens do
    counter = :counters.new(1, [])
    handler = "admit_at_cap_bench_#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :evicted],
      fn _, _, _, _ -> :counters.add(counter, 1, 1) end,
      nil
    )

    results =
      1..@opens
      |> Task.async_stream(
        fn _ ->
          id = uniq()
          {us, r} = :timer.tc(fn -> open_idle(id) end)
          {us, r, id}
        end,
        max_concurrency: @k,
        timeout: 60_000
      )
      |> Enum.map(fn {:ok, x} -> x end)

    :telemetry.detach(handler)
    ids = Enum.map(results, &elem(&1, 2))
    refused = Enum.count(results, fn {_, r, _} -> r != :ok end)
    {Enum.map(results, &elem(&1, 0)), refused, :counters.get(counter, 1), ids}
  end

  defp p50(samples), do: samples |> Enum.sort() |> Enum.at(div(length(samples), 2))

  defp arm(cap_mode) do
    Lru.reset()
    Application.put_env(:fathom, :max_open_shards, Shards.open_count() + 10 * @fill)
    filled = for _ <- 1..@fill, do: tap(uniq(), &(:ok = open_idle(&1)))

    cap =
      case cap_mode do
        :uncapped -> :infinity
        _ -> Shards.open_count()
      end

    Application.put_env(:fathom, :max_open_shards, cap)
    if cap_mode != :uncapped, do: Application.put_env(:fathom, :pre_evict_headroom, cap_mode)
    before = Shards.open_count()
    {samples, refused, inline, ids} = timed_opens()
    after_count = Shards.open_count()
    Application.put_env(:fathom, :max_open_shards, :infinity)
    Enum.each(filled ++ ids, &Shards.stop/1)

    %{
      p50: p50(samples),
      refused: refused,
      inline: inline,
      cap: cap,
      population_delta: after_count - before
    }
  end

  test "admit_at_cap_conc: the cap holds and every evicted event is a real victim" do
    r = arm(0)
    IO.puts("admit_at_cap_conc: #{inspect(r)}")
    assert r.refused == 0
    assert r.population_delta <= @k, "the soft cap leaked: #{inspect(r)}"
    # Each admission either evicted a victim or rode the population up.
    assert r.inline == @opens - r.population_delta
  end

  test "cold_open_at_cap_k16: the pre-evictor keeps concurrent admissions near uncapped latency" do
    uncapped = arm(:uncapped)
    auto = arm(:auto)
    IO.puts("cold_open_at_cap_k16: uncapped=#{inspect(uncapped)} auto=#{inspect(auto)}")
    assert auto.refused == 0
    assert auto.p50 <= 1.3 * uncapped.p50
    assert auto.inline < 0.15 * @opens
  end
end
