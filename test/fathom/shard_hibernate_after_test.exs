defmodule Fathom.ShardHibernateAfterTest do
  @moduledoc """
  Expert review 2026-10-17 #32. Symptom: at a ~30 s flush interval the coordinator's fixed 30 s
  `hibernate_after` equalled the durability tick, so a dirty-but-quiet shard hibernated (full-sweep
  GC) then woke on the tick, every interval. Invariant: `hibernate_after` must exceed the longest
  jittered gap between two ticks, while staying 30 s at small intervals.

  This pins the pure derivation; it does not run a coordinator across a 30 s interval (that would
  need a 30 s test). The live evidence is `process_info(pid, :garbage_collection)` over 3 intervals.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard

  test "small intervals keep the 30 s floor" do
    assert Shard.hibernate_after_ms(5_000, 0.25) == 30_000
    assert Shard.hibernate_after_ms(1, 0.25) == 30_000
  end

  test "a 30 s interval is exceeded by the longest jittered tick gap (+25%)" do
    assert Shard.hibernate_after_ms(30_000, 0.25) > round(30_000 * 1.25)
  end

  test "scales with the interval, and the jitter ratio is capped at 1.0" do
    assert Shard.hibernate_after_ms(60_000, 0.25) > 75_000
    assert Shard.hibernate_after_ms(60_000, 5.0) == Shard.hibernate_after_ms(60_000, 1.0)
  end

  test "no ratio / disabled interval fall back sanely" do
    assert Shard.hibernate_after_ms(30_000, 0) > 30_000
    assert Shard.hibernate_after_ms(0, 0.25) == 30_000
    assert Shard.hibernate_after_ms(nil, 0.25) == 30_000
  end
end
