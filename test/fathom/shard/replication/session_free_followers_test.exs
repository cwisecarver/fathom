defmodule Fathom.Shard.Replication.SessionFreeFollowersTest do
  @moduledoc """
  A follower whose reply was lost stops counting as busy once its push is older than the
  Session's `timeout()` (Jepsen 2026-10-28/29, the stuck-quorum runs).

  ## The defect

  `await_stragglers_if_short/3` waits, before each commit's first round, until a quorum of
  followers is FREE. It counted a follower busy whenever `state.inflight` had an entry for it,
  while the planner (`in_flight?/3`) treats an entry older than `timeout()` as stale, because an
  entry can outlive its push. A reply that never comes leaves the entry for good. With two such
  entries on a 3-follower shard every commit waited out its whole deadline for replies that were
  never coming, then failed `{:no_quorum, :timeout}` before sending anything: the shard refused
  writes for the rest of the run with all three followers up.

  ## What is pinned

  The free count agrees with the planner at every age: an entry younger than `timeout()` is busy,
  one at or past it is free, and a seed in flight is busy whatever its age.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Session

  @fleet_key {Fathom.Shard.Replication.Fleet, :shippers}
  @shippers [:f1, :f2, :f3]

  setup do
    prev = :persistent_term.get(@fleet_key, :unset)
    :persistent_term.put(@fleet_key, @shippers)

    on_exit(fn ->
      if prev == :unset,
        do: :persistent_term.erase(@fleet_key),
        else: :persistent_term.put(@fleet_key, prev)
    end)

    :ok
  end

  @now 1_000_000

  defp state(inflight_ages, seeding \\ []) do
    %{
      inflight: Map.new(inflight_ages, fn {s, _age} -> {s, %{offset: 4152}} end),
      inflight_at: Map.new(inflight_ages, fn {s, age} -> {s, @now - age} end),
      seeding: Map.new(seeding, &{&1, %{started_at: @now}})
    }
  end

  test "two lost replies no longer hold the shard below its quorum" do
    # The rig's shape: two followers with entries minutes old, whose shippers held no waiter.
    stale = Session.timeout() * 40
    assert Session.free_followers(state(f1: stale, f2: stale), @now) == 3
  end

  test "a push still within timeout() keeps its follower busy" do
    fresh = Session.timeout() - 1
    assert Session.free_followers(state(f1: fresh, f2: fresh), @now) == 1
  end

  test "the boundary is the planner's: busy below timeout(), free at it" do
    t = Session.timeout()
    assert Session.free_followers(state(f1: t - 1), @now) == 2
    assert Session.free_followers(state(f1: t), @now) == 3
  end

  test "a seed in flight keeps its follower busy at any age" do
    assert Session.free_followers(state([f1: Session.timeout() * 40], [:f1]), @now) == 2
    assert Session.free_followers(state([], [:f2, :f3]), @now) == 1
  end
end
