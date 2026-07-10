defmodule Fathom.Rebalancer.StatsTest do
  @moduledoc "Shared percentile/median helpers (fleet-relative hot bar, #2)."
  use ExUnit.Case, async: true

  alias Fathom.Rebalancer.Stats

  test "percentile interpolates — small-N p99 is not pinned to the max (#2)" do
    # 5 values: nearest-rank p99 = round(0.99*4) = 4 → the max (100). The old method returned
    # that, so mult × p99 ≥ max flagged nothing. Interpolation returns strictly below the max.
    p99 = Stats.percentile([1.0, 1.0, 1.0, 1.0, 100.0], 99)
    assert p99 < 100.0
    assert p99 > 1.0
  end

  test "edge cases" do
    assert Stats.percentile([], 99) == 0.0
    assert Stats.percentile([5.0], 99) == 5.0
    # p50 interpolates the two middle ranks.
    assert Stats.percentile([1.0, 3.0], 50) == 2.0
    assert Stats.percentile([2.0, 4.0, 9.0], 50) == 4.0
  end

  test "histogram buckets values into the fixed edges (#4)" do
    h = Stats.histogram([0.5, 3.0, 3.0, 150.0, 99_999.0])
    assert length(h) == Stats.bucket_count()
    assert Enum.sum(h) == 5
    # 0.5 → [0,1); 3.0×2 → [2,5); 150 → [100,200); 99999 → overflow [50000,+inf).
    edges = Stats.bucket_edges()
    at = fn lower -> Enum.at(h, Enum.find_index(edges, &(&1 == lower))) end
    assert at.(0.0) == 1
    assert at.(2.0) == 2
    assert at.(100.0) == 1
    assert at.(50000.0) == 1
  end

  test "percentile_from_histogram recovers the pooled percentile; sums are mergeable (#4)" do
    # Two nodes' histograms merge by element-wise add; the p99 of the pooled histogram is the
    # true fleet p99. 30 @ [2,5) + 70 @ [100,200): rank 99 lands in [100,200) → ≈ 198.6.
    a = Stats.histogram(List.duplicate(3.0, 30))
    b = Stats.histogram(List.duplicate(150.0, 70))
    pooled = Enum.zip_with(a, b, &+/2)

    p99 = Stats.percentile_from_histogram(pooled, 99)
    assert p99 > 190.0 and p99 < 200.0

    # Empty / all-zero ⇒ 0.0.
    assert Stats.percentile_from_histogram(Stats.histogram([]), 99) == 0.0
  end
end
