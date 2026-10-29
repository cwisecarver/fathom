defmodule Fathom.Shard.Replication.SessionFreeFollowersTest do
  @moduledoc """
  A follower whose reply was lost stops counting as busy once its push is older than
  `2 * timeout()` (Jepsen 2026-10-28/29, the stuck-quorum runs), and not before (CI 2026-10-29).

  ## The defect

  `await_stragglers_if_short/3` waits, before each commit's first round, until a quorum of
  followers is FREE. It counted a follower busy whenever `state.inflight` had an entry for it,
  while the planner (`in_flight?/3`) treats an entry older than `timeout()` as stale, because an
  entry can outlive its push. A reply that never comes leaves the entry for good. With two such
  entries on a 3-follower shard every commit waited out its whole deadline for replies that were
  never coming, then failed `{:no_quorum, :timeout}` before sending anything: the shard refused
  writes for the rest of the run with all three followers up.

  ## What is pinned

  An entry younger than `2 * timeout()` is busy, one at or past it is free, and a seed in flight
  is busy whatever its age.

  The bound is NOT the planner's `timeout()`. A commit that times out returns exactly `timeout()`
  after its pushes, so the next commit meets entries at that age whose shippers still hold the
  waiter. Freeing them at `timeout()` skipped the straggler wait and the commit failed
  `{:no_quorum, :impossible}` on `:already_in_flight` (`replication_seed_test`, "a commit waits for
  in-flight stragglers", red on CI and about 2 local runs in 5).
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

  test "a push just past timeout() is still busy: its waiter may be live" do
    # The previous commit timed out at exactly this age; its stragglers are slow, not lost.
    t = Session.timeout()
    assert Session.free_followers(state(f1: t, f2: t + 200), @now) == 1
  end

  test "the boundary is 2 * timeout(): busy below it, free at it" do
    t2 = 2 * Session.timeout()
    assert Session.free_followers(state(f1: t2 - 1), @now) == 2
    assert Session.free_followers(state(f1: t2), @now) == 3
  end

  test "a seed in flight keeps its follower busy at any age" do
    assert Session.free_followers(state([f1: Session.timeout() * 40], [:f1]), @now) == 2
    assert Session.free_followers(state([], [:f2, :f3]), @now) == 1
  end
end
