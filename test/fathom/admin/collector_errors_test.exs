defmodule Fathom.Admin.CollectorErrorsTest do
  @moduledoc """
  MetricsCollector's failure-signal series (admin-UI round 2): query errors by class + the error
  ratio, no-quorum by reason, Hrana auth rejections by reason, the 5 min event counts behind the
  Cluster page, and the RPO under-reporting coverage. Same harness as `CollectorPanelsTest`: a
  real in-process reporter, counters driven with `:telemetry.execute/3`, ticks by hand.
  """
  use ExUnit.Case, async: false

  alias Fathom.Admin.{EventRing, MetricsCollector}

  @events [
    [:fathom, :shard, :query, :error],
    [:fathom, :replication, :no_quorum],
    [:fathom, :hrana, :auth, :rejected],
    [:fathom, :shard, :flush, :failed],
    [:fathom, :shard, :lease, :superseded],
    [:fathom, :shard, :heartbeat, :lapsed]
  ]

  setup do
    prev = Application.get_env(:fathom, :admin_tick_ms)
    Application.put_env(:fathom, :admin_tick_ms, 3_600_000)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :admin_tick_ms, prev),
        else: Application.delete_env(:fathom, :admin_tick_ms)
    end)

    for child <- Fathom.Telemetry.reporter_children(), do: start_supervised!(child)
    await_handlers(@events)
    start_supervised!(EventRing)
    {:ok, pid: start_supervised!(MetricsCollector)}
  end

  defp await_handlers(events, tries \\ 200) do
    cond do
      Enum.all?(events, &(:telemetry.list_handlers(&1) != [])) -> :ok
      tries == 0 -> flunk("reporter never attached handlers for #{inspect(events)}")
      true -> Process.sleep(10) && await_handlers(events, tries - 1)
    end
  end

  # A blank scrape holds the diff baselines (no families exist until something is recorded), so
  # seed one sample of every counter family under test, then take the baseline tick.
  defp seed(pid) do
    emit([:fathom, :shard, :query, :error], %{class: "timeout"}, 1)
    emit([:fathom, :shard, :query, :error], %{class: "busy"}, 1)
    emit([:fathom, :replication, :no_quorum], %{reason: "followers_down"}, 1)
    emit([:fathom, :hrana, :auth, :rejected], %{reason: "wrong_shard"}, 1)
    emit([:fathom, :hrana, :auth, :rejected], %{reason: "expired"}, 1)
    emit([:fathom, :shard, :flush, :failed], %{}, 1)
    emit([:fathom, :shard, :lease, :superseded], %{}, 1)
    emit([:fathom, :shard, :heartbeat, :lapsed], %{}, 1)
    tick(pid)
  end

  defp tick(pid) do
    send(pid, :tick)
    :sys.get_state(pid).current
  end

  defp emit(event, meta, n),
    do: for(_ <- 1..n, do: :telemetry.execute(event, %{count: 1}, meta))

  test "query errors: per-class rate, 5 min window and the error ratio", %{pid: pid} do
    # Baseline tick: counters are diffed against the previous tick.
    seed(pid)

    emit([:fathom, :shard, :query, :error], %{class: "timeout"}, 3)
    emit([:fathom, :shard, :query, :error], %{class: "busy"}, 1)
    m = tick(pid)

    assert m.errors.rates["timeout"] > 0
    assert m.errors.rates["busy"] > 0
    assert m.errors.rates["too_large"] == 0.0
    assert m.errors.window["timeout"] == 3
    assert m.errors.window["busy"] == 1
    assert m.errors.total_rate > 0
    # Nothing was queried in the window, so every statement that ran failed.
    assert m.errors.ratio == 1.0

    # Quiet tick: the rate falls back to zero, the 5 min window keeps the count.
    m = tick(pid)
    assert m.errors.total_rate == 0.0
    assert m.errors.ratio == 0.0
    assert m.errors.window["timeout"] == 3
  end

  test "the ring point carries errors/s and the error ratio for the sparkline", %{pid: pid} do
    seed(pid)
    emit([:fathom, :shard, :query, :error], %{class: "timeout"}, 2)
    point = MetricsCollector.ring_point(tick(pid))
    assert point.err > 0
    assert point.err_ratio == 1.0
  end

  test "no-quorum by reason and auth rejections by reason", %{pid: pid} do
    seed(pid)
    emit([:fathom, :replication, :no_quorum], %{reason: "followers_down"}, 2)
    emit([:fathom, :hrana, :auth, :rejected], %{reason: "wrong_shard"}, 4)
    emit([:fathom, :hrana, :auth, :rejected], %{reason: "expired"}, 1)
    m = tick(pid)

    assert m.no_quorum.window == %{"followers_down" => 2}
    assert m.no_quorum.rates["followers_down"] > 0
    assert m.auth_rejected.window["wrong_shard"] == 4
    assert m.auth_rejected.window["expired"] == 1
    assert m.auth_rejected.rates["revoked"] == 0.0
  end

  test "events_5m counts the Cluster-page counters; clean node reads zeros", %{pid: pid} do
    clean = seed(pid)
    assert clean.events_5m.flush_failed == 0
    assert clean.events_5m.heartbeat_lapsed == 0

    emit([:fathom, :shard, :flush, :failed], %{}, 2)
    emit([:fathom, :shard, :lease, :superseded], %{}, 1)
    emit([:fathom, :shard, :heartbeat, :lapsed], %{}, 1)
    m = tick(pid)

    assert m.events_5m.flush_failed == 2
    assert m.events_5m.lease_superseded == 1
    assert m.events_5m.heartbeat_lapsed == 1
    assert m.events_5m.corrupt_flush == 0
  end

  test "rpo_under_reporting is set when watermark rows < open shards", %{pid: pid} do
    m = tick(pid)
    refute m.rpo_under_reporting

    # The coverage is refreshed by the (10 s throttled) RPO walk, not per tick.
    :sys.replace_state(pid, &%{&1 | rpo_cov: {1, 3}})
    m = tick(pid)
    assert m.rpo_under_reporting
    assert {m.rpo_rows, m.rpo_open} == {1, 3}
  end

  describe "window_counts/4" do
    test "windows against the oldest retained snapshot, clamps resets, drops expired" do
      k = {:events, :flush_failed}
      {w, h} = MetricsCollector.window_counts([], 0, %{k => 10.0}, 300_000)
      assert w[k] == 0.0

      {w, h} = MetricsCollector.window_counts(h, 1_000, %{k => 14.0}, 300_000)
      assert w[k] == 4.0

      # A counter reset (restart) cannot go negative.
      {w, h} = MetricsCollector.window_counts(h, 2_000, %{k => 1.0}, 300_000)
      assert w[k] == 0.0

      # The oldest snapshot (t=0, total 10) ages out: the baseline is now t=1000 (total 14).
      {w, _} = MetricsCollector.window_counts(h, 301_000, %{k => 20.0}, 300_000)
      assert w[k] == 6.0
    end
  end
end
