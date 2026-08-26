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

  # Lives here, not in bench_test.exs, because that whole module is @moduletag :bench and excluded
  # from the default suite — which is how this drift survived unnoticed. This needs no bench run.
  #
  # `mix fathom.bench --only <name>` resolves through String.to_existing_atom/1 against the TASK's
  # own @all_metrics literal, a second copy of Fathom.Bench's. A name added to Bench and not
  # mirrored there does not degrade gracefully: it raises "not an already existing atom" before the
  # run starts. Found 2026-08-10 when `--only served` crashed mid-investigation; :served,
  # :dir_recorder, :concurrent and :wire_encode had all drifted.
  test "the Mix task's --only allowlist covers every metric Fathom.Bench knows" do
    missing = Fathom.Bench.known_metrics() -- Mix.Tasks.Fathom.Bench.known_metrics()

    assert missing == [],
           "these metrics exist in Fathom.Bench but are unreachable via --only: " <>
             "#{inspect(missing)} — add them to Mix.Tasks.Fathom.Bench's @all_metrics"
  end

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

  # The vehicle used to be `fanout_kb_per_shard` at +27.8%. That stopped demonstrating anything on
  # 2026-08-10, when fanout was moved to a 50% band (it is fat-tailed — see Gate's @metrics), so
  # +27.8% became a legitimate :warn and this read as a behaviour change when it is a threshold
  # change. Switched to `cold_open_p50_us`, which is still gated at the default 20%: the claim
  # under test is that the WORST metric drives the verdict, not anything about which metric it is.
  test "worst regression across metrics drives the verdict" do
    new = %{@parent | "dir_resolve_p50_us" => 160.0, "cold_open_p50_us" => 1278.0}
    result = Gate.compare(@parent, new)
    # cold_open +27.8% dominates dir_resolve +6.7%.
    assert result.verdict == :block
    assert result.worst > 27.0
  end

  # Pins the per-metric override itself: the same +27.8% that blocks on a default-band metric above
  # must only WARN on a metric carrying a wider band, or the override is not in effect.
  #
  # THE VEHICLE HAS MOVED TWICE, and both moves were threshold changes rather than behaviour
  # changes — worth saying so the next reader does not read the churn as instability. It was
  # `fanout_kb_per_shard` until 2026-08-26, when fanout stopped carrying a numeric band at all
  # (it is `:watch` now, covered below). `cold_open_p99_us` still carries 50, so it demonstrates
  # the same mechanism.
  test "a metric with its own wider band uses it, not the default" do
    parent = Map.put(@parent, "cold_open_p99_us", 180.0)
    new = %{parent | "cold_open_p99_us" => 230.0}
    result = Gate.compare(parent, new)

    assert result.verdict == :warn,
           "cold_open_p99_us carries a 50% block band; +27.8% must not block"

    assert result.worst > 27.0
  end

  # `:watch` — reported, never gating. Demoted 2026-08-26 after the 50% band was measured to sit
  # UNDER the metric's own same-tree spread (identical trees span up to 56% in perf_history.jsonl),
  # so it refused clean commits twice on 2026-08-25 alone.
  describe "a :watch metric" do
    # 2.8x, far past any band the gate has ever carried, and it must still not block.
    test "never blocks, however far it moves" do
      new = %{@parent | "fanout_kb_per_shard" => 500.0}
      result = Gate.compare(@parent, new)

      assert result.verdict == :ok, "a watch-only metric reached the verdict"
      refute :fanout_kb_per_shard in result.blocked
    end

    # THE HALF THAT MATTERS MORE, and the one a naive demotion gets wrong. `commit_with_bench.sh`
    # PROMPTS on :warn and aborts when stdin is not a TTY, so leaving fanout in `worst` would still
    # stop an unattended commit — the same outage one door down. `worst` must ignore it entirely.
    test "is excluded from worst, so it cannot flip the verdict to :warn either" do
      new = %{@parent | "fanout_kb_per_shard" => 500.0}
      result = Gate.compare(@parent, new)

      assert result.worst == 0.0,
             "fanout leaked into worst; commit_with_bench.sh aborts on a warn without a TTY"
    end

    # Demoting it must not mean hiding it — it is still the only reading of whether the
    # coordinators' `fullsweep_after: 0` reclaims retained heap.
    test "is still measured and still reported" do
      new = %{@parent | "fanout_kb_per_shard" => 500.0}
      result = Gate.compare(@parent, new)
      d = Enum.find(result.deltas, &(&1.metric == :fanout_kb_per_shard))

      refute d.skipped
      assert d.pct > 170.0
      assert Gate.format(result, "abc1234") =~ "watch only"
    end

    # A GUARD, NOT A REPRODUCTION — it passes against the pre-demotion code too, because a +30%
    # cold_open blocked under the old band as well. The three tests above are the discriminating
    # ones (verified: all three fail with fanout restored to a 50% band). This one exists so a
    # future widening that accidentally swallowed neighbouring metrics would fail something.
    test "does not shield the metrics that do gate" do
      new = %{@parent | "fanout_kb_per_shard" => 500.0, "cold_open_p50_us" => 1300.0}
      result = Gate.compare(@parent, new)

      assert result.verdict == :block
      assert :cold_open_p50_us in result.blocked
    end
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

  describe "per-metric block thresholds" do
    # A threshold has to sit above the metric's measured noise floor or it reports noise as
    # regression. 20% was chosen for single-threaded p50s; the two p99 tails span ~37% across
    # their same-host history, and one duly blocked a commit at +22.73% with its own p50 flat.
    # The predictable end of a gate that cries wolf is somebody passing --skip by reflex.
    test "a metric with its own threshold does not block below it" do
      # 30% would BLOCK any default-threshold metric and must not block a 50% one. It still
      # WARNS — warn is global and deliberately left alone, so a tail drifting stays visible
      # in the report instead of being silently normalised away.
      parent = %{"cold_open_p99_us" => 1000.0, "host" => "darwin"}
      new = %{"cold_open_p99_us" => 1300.0, "host" => "darwin"}

      assert %{verdict: :warn, blocked: []} = Gate.compare(parent, new)
    end

    test "it still blocks past its own threshold" do
      # The failure the tail exists to catch is p50 flat while p99 DOUBLES; 50% leaves room.
      parent = %{"cold_open_p99_us" => 1000.0, "host" => "darwin"}
      new = %{"cold_open_p99_us" => 2000.0, "host" => "darwin"}

      assert %{verdict: :block, blocked: [:cold_open_p99_us]} = Gate.compare(parent, new)
    end

    test "a default-threshold metric still blocks at 20%" do
      # The relaxation must be scoped to the metrics that declare it, not global.
      parent = %{"cold_open_p50_us" => 1000.0, "host" => "darwin"}
      new = %{"cold_open_p50_us" => 1250.0, "host" => "darwin"}

      assert %{verdict: :block, blocked: [:cold_open_p50_us]} = Gate.compare(parent, new)
    end

    test "the report names the metric's own threshold so it is not invisible" do
      parent = %{"cold_open_p99_us" => 1000.0, "host" => "darwin"}
      new = %{"cold_open_p99_us" => 1300.0, "host" => "darwin"}

      report = parent |> Gate.compare(new) |> Gate.format("abc1234")
      assert report =~ "block >=50%", "a non-default threshold must be visible in the report"
    end
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

      # Entries are {metric, dir} or {metric, dir, block} — a metric may carry its own
      # threshold, so take the name positionally rather than pattern-matching an arity.
      gated = Gate.metrics() |> Enum.map(&(&1 |> elem(0) |> to_string())) |> MapSet.new()

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
