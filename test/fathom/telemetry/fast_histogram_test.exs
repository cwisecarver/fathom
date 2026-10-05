defmodule Fathom.Telemetry.FastHistogramTest do
  # Perf review 2026-10-01 #14: TelemetryMetricsPrometheus.Core records every distribution sample
  # under ONE ets key per metric, so the per-statement query histogram serialized every scheduler
  # on one hash bucket — -28% node query throughput on `mix fathom.scale --hotspots`. The two
  # request-path distributions moved to FastHistogram (lock-free :counters). These pin that the
  # move changed nothing a reader can see: same exposition text as Core for the same events (so
  # /metrics, the dashboard parser, Grafana and the alert rules read it unchanged), no lost
  # updates under concurrency, and a clean attach/detach lifecycle.
  #
  # async: false — the handlers are attached to the real [:fathom, …] events, which async tests
  # emit from the query path; a sync module runs alone, so the counts here are exact.
  use ExUnit.Case, async: false

  alias Fathom.Admin.PrometheusScrape
  alias Fathom.Telemetry.FastHistogram

  @query [:fathom, :shard, :query]
  @checkout_stop [:fathom, :shards, :checkout, :stop]

  defp hot_metrics, do: Enum.filter(Fathom.Telemetry.metrics(), &FastHistogram.handles?/1)

  defp start_fast(name \\ :"fast_hist_#{System.unique_integer([:positive])}") do
    start_supervised!({FastHistogram, metrics: hot_metrics(), name: name}, id: name)
    name
  end

  defp start_core do
    name = :"core_hist_#{System.unique_integer([:positive])}"

    start_supervised!(
      {TelemetryMetricsPrometheus.Core, metrics: hot_metrics(), name: name},
      id: name
    )

    # Core attaches its handlers in a message after start returns; sync before emitting.
    _ = :sys.get_state(name)
    name
  end

  defp ms(v), do: System.convert_time_unit(round(v * 1000), :microsecond, :native)
  defp query(v), do: :telemetry.execute(@query, %{duration: ms(v)}, %{})

  defp checkout(v, outcome),
    do: :telemetry.execute(@checkout_stop, %{duration: ms(v)}, %{outcome: outcome})

  # Bucket and count samples must match exactly; `_sum` is a float Core accumulates from floats
  # and FastHistogram from integer µs, so it is compared to the µs.
  defp normalize(text) do
    text
    |> PrometheusScrape.parse()
    |> Enum.map(fn {name, labels, v} ->
      if String.ends_with?(name, "_sum"),
        do: {name, labels, Float.round(v, 3)},
        else: {name, labels, v}
    end)
    |> Enum.sort()
  end

  test "renders the same series as TelemetryMetricsPrometheus.Core for the same events" do
    fast = start_fast()
    core = start_core()

    # Values on a bound (le is inclusive), between bounds, below the first, and past the last (+Inf).
    for v <- [0.05, 0.1, 0.3, 1, 1.5, 7, 49.999, 50, 4999, 5000, 7000], do: query(v)

    for {v, outcome} <- [{0.2, :ok}, {3, :ok}, {12, :held}, {6000, :unavailable}, {2, :error}],
        do: checkout(v, outcome)

    core_text = IO.iodata_to_binary(TelemetryMetricsPrometheus.Core.scrape(core))
    fast_text = FastHistogram.render(fast)

    assert fast_text != ""
    assert normalize(fast_text) == normalize(core_text)

    # Byte-level shape: HELP/TYPE headers and a trailing newline per family, like Core's export.
    assert fast_text =~ "# TYPE fathom_shard_query_duration histogram\n"
    assert fast_text =~ "# TYPE fathom_shards_checkout_stop_duration histogram\n"
    assert String.ends_with?(fast_text, "\n")
  end

  test "a value exactly on a bound lands in that bucket (le is inclusive)" do
    fast = start_fast()
    query(1)

    samples = PrometheusScrape.parse(FastHistogram.render(fast))
    # buckets/2, not value/3: value/3 skips bucket rows and would return its 0.0 default.
    cumulative = PrometheusScrape.buckets(samples, "fathom_shard_query_duration")
    at = fn bound -> Enum.find_value(cumulative, fn {b, c} -> b == bound && c end) end

    assert at.(0.5) == 0
    assert at.(1) == 1
    assert at.(:infinity) == 1
    assert PrometheusScrape.value(samples, "fathom_shard_query_duration_count") == 1
  end

  test "no samples are lost under concurrent writers, including a label set created mid-race" do
    fast = start_fast()
    procs = System.schedulers_online() * 2
    per = 2_000

    1..procs
    |> Task.async_stream(
      fn i ->
        # Every task races to create the same new `outcome` series on its first event.
        for _ <- 1..per do
          query(0.3)
          checkout(0.7, if(rem(i, 2) == 0, do: :ok, else: :held))
        end
      end,
      max_concurrency: procs,
      timeout: 60_000
    )
    |> Stream.run()

    samples = PrometheusScrape.parse(FastHistogram.render(fast))
    assert PrometheusScrape.value(samples, "fathom_shard_query_duration_count") == procs * per

    ok =
      PrometheusScrape.value(samples, "fathom_shards_checkout_stop_duration_count", %{
        "outcome" => "ok"
      })

    held =
      PrometheusScrape.value(samples, "fathom_shards_checkout_stop_duration_count", %{
        "outcome" => "held"
      })

    assert ok + held == procs * per
    assert ok == div(procs, 2) * per
  end

  test "a sample missing a tag is dropped and the handler stays attached" do
    fast = start_fast()
    :telemetry.execute(@checkout_stop, %{duration: ms(1)}, %{})
    :telemetry.execute(@query, %{}, %{})
    query(2)

    samples = PrometheusScrape.parse(FastHistogram.render(fast))

    assert PrometheusScrape.label_values(
             samples,
             "fathom_shards_checkout_stop_duration",
             "outcome"
           ) == []

    assert PrometheusScrape.value(samples, "fathom_shard_query_duration_count") == 1
  end

  test "stopping detaches the handlers and clears the series; a restart starts empty" do
    name = :"fast_hist_lifecycle_#{System.unique_integer([:positive])}"
    pid = start_fast(name)
    query(1)
    assert FastHistogram.render(name) =~ "fathom_shard_query_duration_count 1"

    :ok = stop_supervised(name)
    _ = pid
    assert FastHistogram.render(name) == ""

    refute Enum.any?(:telemetry.list_handlers(@query), &match?({FastHistogram, ^name, _}, &1.id))

    start_fast(name)
    assert FastHistogram.render(name) == ""
    query(1)
    assert FastHistogram.render(name) =~ "fathom_shard_query_duration_count 1"
  end

  # The split must be a partition: a hot metric on BOTH reporters would double the series in
  # /metrics, and one on neither would vanish from it.
  test "reporter_children hands each metric to exactly one reporter" do
    [{TelemetryMetricsPrometheus.Core, core_opts}, {FastHistogram, fast_opts}] =
      Fathom.Telemetry.reporter_children()

    core = Enum.map(core_opts[:metrics], & &1.name)
    fast = Enum.map(fast_opts[:metrics], & &1.name)

    assert Enum.sort(fast) ==
             Enum.sort([
               [:fathom, :shard, :query, :duration],
               [:fathom, :shards, :checkout, :stop, :duration]
             ])

    assert MapSet.disjoint?(MapSet.new(core), MapSet.new(fast))
    assert length(core) + length(fast) == length(Fathom.Telemetry.metrics())
  end

  test "Fathom.Telemetry.scrape/0 serves both reporters, each family once" do
    for child <- Fathom.Telemetry.reporter_children(), do: start_supervised!(child)
    _ = :sys.get_state(:fathom_metrics)

    query(1)
    checkout(1, :ok)
    :telemetry.execute([:fathom, :shards, :held_retry], %{wait_ms: 10}, %{aimed: true})

    text = Fathom.Telemetry.scrape()

    for family <-
          ~w(fathom_shard_query_duration fathom_shards_checkout_stop_duration fathom_shards_held_retry_wait_ms) do
      assert length(String.split(text, "# TYPE #{family} histogram")) == 2,
             "#{family} should appear exactly once"
    end
  end

  # Hot-path ceiling. Every scheduler emits the per-statement event at once, the shape of a busy
  # node. Measured 2026-10-05 (18 schedulers, prod): no handler ~15 ns/event of wall time, Core
  # ~8,000 ns (its one-key duplicate_bag), FastHistogram ~69 ns. The 1,000 ns ceiling has ~14x
  # headroom for FastHistogram and fails Core by ~8x, so a change that brings back a shared hot
  # key fails here.
  @tag :bench
  test "recording a query sample under all-scheduler contention stays under 1 µs/event" do
    fast = start_fast()
    procs = System.schedulers_online()
    per = 50_000

    {us, _} =
      :timer.tc(fn ->
        1..procs
        |> Task.async_stream(fn _ -> for _ <- 1..per, do: query(0.3) end,
          max_concurrency: procs,
          timeout: 120_000
        )
        |> Stream.run()
      end)

    ns_per_event = us * 1000 / (procs * per)

    samples = PrometheusScrape.parse(FastHistogram.render(fast))

    assert PrometheusScrape.value(samples, "fathom_shard_query_duration_count") == procs * per,
           "the bench recorded nothing: it is measuring an unattached handler"

    assert ns_per_event < 1_000,
           "#{Float.round(ns_per_event, 1)} ns/event across #{procs} schedulers (ceiling 1000)"
  end
end
