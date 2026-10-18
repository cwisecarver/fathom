defmodule Fathom.Telemetry.FastCounterTest do
  # Expert review 2026-10-17 #12: `pool_take` (per pooled stream open) and `replication.checkpoint`
  # (per covered commit) were Core counters, i.e. one contended `:ets.update_counter` key per label
  # set. They moved to FastCounter (lock-free :counters). These pin that a reader sees the same
  # exposition text as Core for the same events, that no increments are lost under concurrency, and
  # that the attach/detach lifecycle is clean.
  #
  # async: false — handlers attach to the real [:fathom, ...] events.
  use ExUnit.Case, async: false

  alias Fathom.Admin.PrometheusScrape
  alias Fathom.Telemetry.FastCounter
  alias Fathom.Telemetry.FastHistogram

  @pool_take [:fathom, :shard, :pool_take]
  @checkpoint [:fathom, :replication, :checkpoint]

  defp hot_metrics, do: Enum.filter(Fathom.Telemetry.metrics(), &FastCounter.handles?/1)

  defp start_fast(name \\ :"fast_ctr_#{System.unique_integer([:positive])}") do
    start_supervised!({FastCounter, metrics: hot_metrics(), name: name}, id: name)
    name
  end

  defp start_core do
    name = :"core_ctr_#{System.unique_integer([:positive])}"

    start_supervised!(
      {TelemetryMetricsPrometheus.Core, metrics: hot_metrics(), name: name},
      id: name
    )

    _ = :sys.get_state(name)
    name
  end

  defp normalize(text), do: text |> PrometheusScrape.parse() |> Enum.sort()

  test "the two hot counters are exactly the ones FastCounter claims" do
    assert hot_metrics() |> Enum.map(& &1.name) |> Enum.sort() == [
             [:fathom, :replication, :checkpoint, :count],
             [:fathom, :shard, :pool_take, :count]
           ]
  end

  test "renders the same series as TelemetryMetricsPrometheus.Core for the same events" do
    fast = start_fast()
    core = start_core()

    for r <- [:hit, :hit, :hit, :miss],
        do: :telemetry.execute(@pool_take, %{count: 1}, %{result: r})

    for v <- [:covered, :covered, :forced],
        do: :telemetry.execute(@checkpoint, %{}, %{verdict: v})

    core_text = IO.iodata_to_binary(TelemetryMetricsPrometheus.Core.scrape(core))
    fast_text = FastCounter.render(fast)

    assert normalize(fast_text) == normalize(core_text)
    assert fast_text =~ "# TYPE fathom_shard_pool_take_count counter\n"
    assert fast_text =~ ~s(fathom_shard_pool_take_count{result="hit"} 3)
    assert fast_text =~ ~s(fathom_replication_checkpoint_count{verdict="forced"} 1)
    assert String.ends_with?(fast_text, "\n")
  end

  test "no increments are lost under concurrent writers, including a label set created mid-race" do
    fast = start_fast()
    procs = System.schedulers_online() * 2
    per = 2_000

    1..procs
    |> Task.async_stream(
      fn _ ->
        for _ <- 1..per, do: :telemetry.execute(@pool_take, %{count: 1}, %{result: :hit})
      end,
      timeout: 30_000,
      max_concurrency: procs
    )
    |> Stream.run()

    samples = PrometheusScrape.parse(FastCounter.render(fast))
    assert PrometheusScrape.value(samples, "fathom_shard_pool_take_count") == procs * per
  end

  test "a sample missing its tag is dropped, and stopping detaches the handlers" do
    fast = start_fast()
    :telemetry.execute(@pool_take, %{count: 1}, %{})
    assert FastCounter.render(fast) == ""

    stop_supervised!(fast)
    handlers = :telemetry.list_handlers(@pool_take)
    refute Enum.any?(handlers, &match?({FastCounter, _, _}, &1.id))
  end

  # Expert review 2026-10-17 #4: commit_total / commit_stage moved onto FastHistogram.
  test "commit_total and commit_stage distributions are served by FastHistogram, not Core" do
    names =
      Fathom.Telemetry.metrics()
      |> Enum.filter(&FastHistogram.handles?/1)
      |> Enum.map(& &1.name)

    assert [:fathom, :replication, :commit_total, :duration] in names
    assert [:fathom, :replication, :commit_stage, :duration] in names

    fast = :"fast_hist_#{System.unique_integer([:positive])}"
    metrics = Enum.filter(Fathom.Telemetry.metrics(), &FastHistogram.handles?/1)
    start_supervised!({FastHistogram, metrics: metrics, name: fast}, id: fast)

    ms = fn v -> System.convert_time_unit(round(v * 1000), :microsecond, :native) end

    :telemetry.execute([:fathom, :replication, :commit_total], %{duration: ms.(3)}, %{
      outcome: :ok
    })

    :telemetry.execute([:fathom, :replication, :commit_stage], %{duration: ms.(7)}, %{
      stage: :build
    })

    text = FastHistogram.render(fast)
    assert text =~ ~s(fathom_replication_commit_total_duration_count{outcome="ok"} 1)
    assert text =~ ~s(fathom_replication_commit_stage_duration_count{stage="build"} 1)
  end
end
