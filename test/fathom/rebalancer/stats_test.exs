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
    assert Stats.median([]) == 0.0
    assert Stats.median([1.0, 3.0]) == 2.0
    assert Stats.median([2.0, 4.0, 9.0]) == 4.0
  end
end
