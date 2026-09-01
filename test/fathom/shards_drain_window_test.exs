defmodule Fathom.ShardsDrainWindowTest do
  @moduledoc """
  Expert review 2026-08-31 #3 — the drain window handed to each coordinator must leave a POSITIVE
  wait-for-streams budget.

  `drain_pid/2` hands the coordinator `window - flush_grace` to wait for its streams before the
  flush grace. The old floor sat on the SLICE at 1_000 ms, BELOW the 5_000 ms grace, so beyond
  ~160 open shards the computed slice rounded down, the floor pinned it at 1_000, and the window
  minus the grace was NEGATIVE — every busy shard got a 0 ms drain and aborted `:busy`, falling to
  the unbounded supervisor teardown the drain exists to avoid. The floor now sits on the WINDOW at
  `grace + min_useful`, bounded by the remaining budget.

  Pure arithmetic, so the >160-shard regime is testable without standing up a fleet (the integration
  coverage in `shards_drain_all_test.exs` runs only 3 shards and cannot reach it).
  """
  use ExUnit.Case, async: true

  alias Fathom.Shards

  @grace 5_000
  @min_useful 1_000
  @concurrency 16

  test "a busy shard gets a positive drain window even with a huge open-shard count" do
    # 1_000 open shards, full 55 s budget. Pre-fix: slice floored at 1_000 (below the 5_000 grace),
    # so window == 1_000 and `window - grace` == -4_000 → a 0 ms drain for every busy shard.
    window = Shards.drain_window(55_000, 1_000, 63, @concurrency, @grace, @min_useful)

    assert window - @grace >= @min_useful,
           "the drain window collapsed to <= the flush grace (#{window}ms), so busy shards get " <>
             "0ms to drain and are hard-cut by the supervisor teardown"
  end

  test "the window never exceeds the remaining budget" do
    # The tail of the budget: `left` below the grace. The floor collapses to `left` (it cannot
    # invent budget) rather than overshooting.
    assert Shards.drain_window(3_000, 1_000, 63, @concurrency, @grace, @min_useful) <= 3_000
    assert Shards.drain_window(0, 1_000, 63, @concurrency, @grace, @min_useful) == 0
  end

  test "a small fleet still gets its full fair slice, not just the floor" do
    # 10 shards in one wave: the fair slice (~55 s) dominates the floor.
    window = Shards.drain_window(55_000, 10, 1, @concurrency, @grace, @min_useful)
    assert window >= @grace + @min_useful
    assert window <= 55_000
  end
end
