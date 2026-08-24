defmodule Fathom.Shard.StorageTest do
  # Expert review 2026-07-14 #19: mark_incarnation_dead/1 stored a SINGLE value in
  # :persistent_term, overwriting any prior. A node restarting several times fast (incarnations
  # X→Y→Z) then remembered only the LATEST proven-dead one — locks still held by the earlier,
  # equally-dead incarnations fell back to the slow lock-TTL liveness path (extra unavailability
  # up to TTL+margin per shard). The fix stores a SET, so every verified-dead predecessor is
  # treated as stealable. Invariant: marking X then Y leaves BOTH reported dead.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage

  @key {Storage, :dead_incarnations}

  setup do
    # :persistent_term is global; snapshot + restore so the test can't leak into others.
    prev = :persistent_term.get(@key, nil)

    on_exit(fn ->
      if prev,
        do: :persistent_term.put(@key, prev),
        else: :persistent_term.erase(@key)
    end)

    :ok
  end

  test "remembers EVERY dead incarnation, not just the latest" do
    x = "fathom@a#{System.unique_integer([:positive])}"
    y = "fathom@b#{System.unique_integer([:positive])}"

    refute Storage.incarnation_dead?(x)
    refute Storage.incarnation_dead?(y)

    :ok = Storage.mark_incarnation_dead(x)
    :ok = Storage.mark_incarnation_dead(y)

    # Pre-fix: marking y overwrote x, so x reported ALIVE and its still-held locks waited out
    # the lock-TTL fallback on the fast restart.
    assert Storage.incarnation_dead?(x), "earlier dead incarnation still remembered"
    assert Storage.incarnation_dead?(y), "latest dead incarnation remembered"
  end

  test "an unmarked owner is never dead (foreign / migrator owners can't match)" do
    refute Storage.incarnation_dead?("migrator@node@123")
    refute Storage.incarnation_dead?("some-other-node@xyz")
  end

  # THE SET ONLY EVER GROWS, so without this there was no way for a test to put it back.
  #
  # `mark_incarnation_dead/1` adds and never removes — correct for production, where the point is
  # that a fast-restarting node keeps remembering each of its own dead predecessors. In a suite it
  # meant any test that marked an owner changed a liveness rule for every test that ran afterwards
  # in the same BEAM. The quiescence half of `fast_steal_ok?/4` already had `reset_quiescence/0`;
  # this is the missing twin, and `incarnation_quiescence_test.exs` is the file that needed it.
  test "reset_incarnation_deaths/0 empties the set, so one test cannot leak into the next" do
    x = "fathom@d#{System.unique_integer([:positive])}"
    y = "fathom@e#{System.unique_integer([:positive])}"

    :ok = Storage.mark_incarnation_dead(x)
    :ok = Storage.mark_incarnation_dead(y)
    assert Storage.incarnation_dead?(x)
    assert Storage.incarnation_dead?(y)

    :ok = Storage.reset_incarnation_deaths()

    refute Storage.incarnation_dead?(x)
    refute Storage.incarnation_dead?(y)

    # The WHOLE set, not just the two owners named here — a reset that left anything behind would
    # still make the next test's verdict depend on this one.
    assert Storage.dead_incarnations() == MapSet.new()
  end

  test "re-marking an already-dead incarnation is idempotent and stays reported dead" do
    owner = "fathom@c#{System.unique_integer([:positive])}"

    :ok = Storage.mark_incarnation_dead(owner)
    # The membership-check guards the global-GC-triggering put; re-marking is a no-op write.
    :ok = Storage.mark_incarnation_dead(owner)

    assert Storage.incarnation_dead?(owner)
  end
end
