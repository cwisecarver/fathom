defmodule Fathom.Bench.GateTest do
  @moduledoc """
  The gate decision is what actually refuses a commit, so it gets real coverage —
  in the default suite, no bench run needed. Maps use string keys to mirror what
  `Jason.decode` produces from `perf_history.jsonl`.
  """
  use ExUnit.Case, async: true

  alias Fathom.Bench.Gate

  # Keys a history line carries that are provenance, not measurements.
  @bookkeeping ~w(log dirty host branch commit commit_full trials mix_env ts)

  @parent %{
    "cold_open_p50_us" => 1000.0,
    "dir_resolve_p50_us" => 150.0,
    "copy_keystone_rows_per_s" => 8_000_000.0,
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
    new = %{@parent | "copy_keystone_rows_per_s" => 6_000_000.0}
    result = Gate.compare(@parent, new)
    assert result.verdict == :block
    assert result.worst == 25.0
  end

  test "throughput improving is not a regression" do
    new = %{@parent | "copy_keystone_rows_per_s" => 12_000_000.0}
    result = Gate.compare(@parent, new)
    assert result.verdict == :ok
    # Improvement reads as a negative regression percent.
    copy = Enum.find(result.deltas, &(&1.metric == :copy_keystone_rows_per_s))
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

  describe "every measured metric is gated" do
    # THE GAP THIS CLOSES. Review #41.1 and #41.3 added `hrana_open_rt_us` and `flush_p50_us` to
    # the bench and verified they discriminate — but never added them to `@metrics`. For a day
    # they were measured, printed, and written to perf_history.jsonl while the gate silently
    # ignored them, so a regression in the WRITE PATH or in per-stream open could not block a
    # commit. Nothing caught that, because nothing asserted the two lists agree.
    #
    # Reading the real history file rather than a fixture is deliberate: the invariant is about
    # what the bench ACTUALLY emits, and a fixture would just be a second list to forget to
    # update. Adding a metric to the bench now fails here until it is a deliberate gate decision.
    test "the gated set matches what the bench emits, exactly" do
      measured =
        "scripts/perf_history.jsonl"
        |> File.stream!()
        |> Enum.reduce(nil, fn line, acc -> if String.trim(line) == "", do: acc, else: line end)
        |> Jason.decode!()
        |> Map.keys()
        |> Kernel.--(@bookkeeping)
        |> MapSet.new()

      gated = Gate.metrics() |> Enum.map(fn {m, _dir} -> to_string(m) end) |> MapSet.new()

      assert MapSet.difference(measured, gated) |> MapSet.to_list() == [],
             "the bench emits metrics the gate never compares — a regression in one of these " <>
               "cannot block a commit. Add it to Fathom.Bench.Gate's @metrics."

      assert MapSet.difference(gated, measured) |> MapSet.to_list() == [],
             "the gate compares metrics the bench no longer emits; they will silently skip " <>
               "forever. Remove them, or rename the series (see copy_keystone_rows_per_s)."
    end

    test "the write path and per-stream open are gated by name" do
      # Named explicitly as well as structurally: these two are #41's whole point, and a future
      # refactor of the check above must not quietly drop them.
      gated = Gate.metrics() |> Enum.map(&elem(&1, 0))
      assert :flush_p50_us in gated
      assert :hrana_open_rt_us in gated
    end
  end
end
