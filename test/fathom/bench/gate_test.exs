defmodule Fathom.Bench.GateTest do
  @moduledoc """
  The gate decision is what actually refuses a commit, so it gets real coverage —
  in the default suite, no bench run needed. Maps use string keys to mirror what
  `Jason.decode` produces from `perf_history.jsonl`.
  """
  use ExUnit.Case, async: true

  alias Fathom.Bench.Gate

  @parent %{
    "cold_open_p50_us" => 1000.0,
    "dir_resolve_p50_us" => 150.0,
    "copy_rows_per_s" => 8_000_000.0,
    "fanout_kb_per_shard" => 180.0,
    "hrana_rt_us" => nil,
    "host" => "darwin"
  }

  test "identical runs pass" do
    result = Gate.compare(@parent, @parent)
    assert result.verdict == :ok
    assert result.worst == 0.0
  end

  test "a latency metric regressing >= block blocks" do
    new = %{@parent | "cold_open_p50_us" => 1250.0}
    result = Gate.compare(@parent, new)
    assert result.verdict == :block
    assert result.worst == 25.0
  end

  test "throughput dropping (lower is worse) >= block blocks" do
    new = %{@parent | "copy_rows_per_s" => 6_000_000.0}
    result = Gate.compare(@parent, new)
    assert result.verdict == :block
    assert result.worst == 25.0
  end

  test "throughput improving is not a regression" do
    new = %{@parent | "copy_rows_per_s" => 12_000_000.0}
    result = Gate.compare(@parent, new)
    assert result.verdict == :ok
    # Improvement reads as a negative regression percent.
    copy = Enum.find(result.deltas, &(&1.metric == :copy_rows_per_s))
    assert copy.pct < 0
  end

  test "a regression between warn and block warns" do
    new = %{@parent | "cold_open_p50_us" => 1150.0}
    result = Gate.compare(@parent, new, 20, 10)
    assert result.verdict == :warn
    assert result.worst == 15.0
  end

  test "worst regression across metrics drives the verdict" do
    new = %{@parent | "dir_resolve_p50_us" => 160.0, "fanout_kb_per_shard" => 230.0}
    result = Gate.compare(@parent, new)
    # fanout +27.8% dominates dir_resolve +6.7%.
    assert result.verdict == :block
    assert result.worst > 27.0
  end

  test "a metric nil in either run is skipped, never treated as flat" do
    new = %{@parent | "dir_resolve_p50_us" => nil}
    result = Gate.compare(@parent, new)
    assert result.verdict == :ok
    dir = Enum.find(result.deltas, &(&1.metric == :dir_resolve_p50_us))
    assert dir.skipped
    assert dir.reason == "nil"
  end

  test "a zero parent value is skipped (no divide-by-zero)" do
    parent = %{@parent | "cold_open_p50_us" => 0}
    new = %{@parent | "cold_open_p50_us" => 500.0}
    result = Gate.compare(parent, new)
    cold = Enum.find(result.deltas, &(&1.metric == :cold_open_p50_us))
    assert cold.skipped
    assert cold.reason == "parent zero"
  end

  test "all metrics unmeasured yields no_data, not a silent pass" do
    blank = %{"host" => "darwin"}
    result = Gate.compare(blank, blank)
    assert result.verdict == :no_data
  end

  test "format renders a verdict line and per-metric rows" do
    new = %{@parent | "cold_open_p50_us" => 1250.0}
    report = Gate.compare(@parent, new) |> Gate.format("abc1234")
    assert report =~ "bench gate vs parent abc1234"
    assert report =~ "cold_open_p50_us"
    assert report =~ "BLOCK"
  end
end
