defmodule Fathom.Admin.CollectorPanelsTest do
  @moduledoc """
  MetricsCollector end-to-end over a REAL in-process Prometheus reporter (expert review
  2026-10-15 #2/#6/#10/#15/#17/#23/#26). The reporter is off in test by default, so each test
  starts `Fathom.Telemetry.reporter_children/0` itself, drives gauges/histograms with
  `:telemetry.execute/3`, and ticks the collector by hand (`admin_tick_ms` is parked high).
  """
  use ExUnit.Case, async: false

  alias Fathom.Admin.{EventRing, Measurements, MetricsCollector}

  setup do
    prev = Application.get_env(:fathom, :admin_tick_ms)
    Application.put_env(:fathom, :admin_tick_ms, 3_600_000)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :admin_tick_ms, prev),
        else: Application.delete_env(:fathom, :admin_tick_ms)
    end)

    for child <- Fathom.Telemetry.reporter_children(), do: start_supervised!(child)
    # The Core reporter attaches its telemetry handlers AFTER start returns, so an event emitted
    # straight away is silently dropped (this made the gauge assertions below flake). Wait for them.
    await_handlers([[:fathom, :node, :vm_limits], [:fathom, :shard, :cold_open]])
    start_supervised!(EventRing)
    pid = start_supervised!(MetricsCollector)
    {:ok, pid: pid}
  end

  defp await_handlers(events, tries \\ 200) do
    cond do
      Enum.all?(events, &(:telemetry.list_handlers(&1) != [])) -> :ok
      tries == 0 -> flunk("reporter never attached handlers for #{inspect(events)}")
      true -> Process.sleep(10) && await_handlers(events, tries - 1)
    end
  end

  defp tick(pid) do
    send(pid, :tick)
    :sys.get_state(pid).current
  end

  defp cold_open(ms, warm?, n) do
    native = System.convert_time_unit(ms, :millisecond, :native)

    for _ <- 1..n do
      :telemetry.execute([:fathom, :shard, :cold_open], %{duration: native}, %{warm: warm?})
    end
  end

  # Symptom (review 2026-10-15 #6): the cold-open tile read the raw CUMULATIVE histogram, so it was
  # a lifetime percentile. After 200 fast opens at boot, a burst of slow S3 pulls barely moved
  # p50 and the tile could never show a current regression. Invariant: the published percentile
  # covers only the recent window.
  test "cold-open percentiles are windowed, not lifetime", %{pid: pid} do
    cold_open(2, true, 200)
    first = tick(pid)

    # No baseline yet => an empty window, NOT the lifetime distribution of the 200 opens above.
    assert first.cold_open_n == 0
    assert first.cold_open_p50_ms == 0.0

    cold_open(3_000, false, 5)
    second = tick(pid)

    assert second.cold_open_n == 5

    assert second.cold_open_p50_ms >= 1_000.0,
           "p50 #{second.cold_open_p50_ms} ms reflects the lifetime mix, not the 5 recent slow pulls"

    # Split by the warm tag: the slow pulls are warm=false and only those.
    assert second.cold_open_by_warm["false"].n == 5
    assert second.cold_open_by_warm["false"].p99 >= 1_000.0
    assert second.cold_open_by_warm["true"].n == 0
  end

  test "window_hist/4: oldest retained snapshot is the baseline; expiry; reset clamp" do
    b = fn counts -> [{1.0, counts[:one]}, {:infinity, counts[:inf]}] end
    snap = fn one, inf -> %{"all" => b.(one: one, inf: inf)} end

    {w0, h} = MetricsCollector.window_hist([], 0, snap.(10, 10), 60_000)
    assert w0["all"] == [{1.0, 0.0}, {:infinity, 0.0}]

    {w1, h} = MetricsCollector.window_hist(h, 30_000, snap.(10, 14), 60_000)
    assert w1["all"] == [{1.0, 0.0}, {:infinity, 4.0}]

    # Past the window the first snapshot expires; the baseline moves forward.
    {w2, _h} = MetricsCollector.window_hist(h, 70_000, snap.(10, 20), 60_000)
    assert w2["all"] == [{1.0, 0.0}, {:infinity, 6.0}]

    # A counter reset (cur < base) clamps to 0 instead of going negative.
    {w3, _} = MetricsCollector.window_hist([{0, snap.(50, 50)}], 1_000, snap.(5, 5), 60_000)
    assert w3["all"] == [{1.0, 0.0}, {:infinity, 0.0}]
  end

  test "node_health, vm and collector tick cost ride the broadcast (#2 #15 #17 #23)", %{pid: pid} do
    Measurements.vm_limits()
    Measurements.disk()
    Measurements.connections()
    Measurements.recorder()
    Measurements.memory_breakdown()
    # Two polls: utilisation needs a baseline sample.
    Measurements.schedulers()
    Measurements.schedulers()

    m = tick(pid)

    assert is_float(m.node_health.process_used_ratio)
    assert is_float(m.node_health.port_used_ratio)
    assert Map.has_key?(m.node_health.disks, "data")
    assert is_float(m.node_health.disks["data"].used_ratio)
    assert is_float(m.vm.run_queue.normal)
    assert is_float(m.vm.util.normal)
    assert m.vm.memory.processes > 0
    # #23: the collector reports what a tick costs.
    assert is_float(m.collector_tick_ms) and m.collector_tick_ms >= 0

    # #26: the ring carries the new series.
    point = MetricsCollector.ring_point(m)
    assert Enum.all?([:mem, :checkout, :flush_p99, :cold_p99, :dirty], &Map.has_key?(point, &1))
    assert point.mem == m.memory_bytes
  end

  test "admission refusals/evictions land in the ring with the shard id; success path does not",
       %{pid: pid} do
    before = EventRing.total(:admission)

    :telemetry.execute([:fathom, :shards, :at_capacity], %{count: 1}, %{shard_id: "refused-1"})
    :telemetry.execute([:fathom, :shards, :evicted], %{count: 1}, %{shard_id: "evicted-1"})

    :telemetry.execute([:fathom, :shards, :novel_rate_limited], %{count: 1}, %{
      shard_id: "novel-1"
    })

    # A successful checkout emits its own event; it must never write to the ring.
    :telemetry.execute([:fathom, :shards, :checkout, :stop], %{duration: 1}, %{outcome: "ok"})

    assert EventRing.total(:admission) == before + 3

    assert [
             %{kind: :novel_rate_limited, shard_id: "novel-1"},
             %{kind: :evicted, shard_id: "evicted-1"},
             %{kind: :at_capacity, shard_id: "refused-1"}
           ] = EventRing.recent(:admission, 10)

    # And the collector publishes the recent list + rates for the panel.
    m = tick(pid)
    assert Enum.map(m.admission.recent, & &1.shard_id) == ["novel-1", "evicted-1", "refused-1"]
    assert is_map(m.admission.rates)
  end

  # #17: the recorder hides a Postgres outage from the data path; stats/0 is how the Node-health
  # strip sees it. Read lock-free from ETS.
  test "Directory.Recorder.stats/0 reports buffer depth and a flush age without calling the recorder" do
    alias Fathom.Directory.Recorder

    # The test sandbox gives the recorder no DB connection, so its own flushes fail and re-buffer:
    # the very state (rows held, no recent successful flush) the Node-health cell exists to show.
    Recorder.record("rec_stats_#{System.unique_integer([:positive])}")

    assert %{buffered: buffered, flush_buffered: flush_buffered, last_flush_age_ms: age} =
             Recorder.stats()

    assert buffered >= 0 and flush_buffered >= 0
    assert is_integer(age) and age >= 0
  end

  test "scheduler_utils/1 averages per kind from :scheduler.utilization rows" do
    rows = [
      {:total, 0.5, "50%"},
      {:weighted, 0.4, "40%"},
      {:normal, 1, 0.2, "20%"},
      {:normal, 2, 0.4, "40%"},
      {:cpu, 3, 0.0, "0%"},
      {:io, 4, 1.0, "100%"}
    ]

    assert %{normal: n, dirty_cpu: +0.0, dirty_io: 1.0} = Measurements.scheduler_utils(rows)
    assert_in_delta n, 0.3, 1.0e-9
  end
end
