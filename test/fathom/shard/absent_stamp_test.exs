defmodule Fathom.Shard.AbsentStampTest do
  @moduledoc """
  The ranking half of "a takeover that finds NO object asks the fleet" (Jepsen round-8 herd run,
  2026-10-23, option A). A brand-new shard's owner can quorum-ack writes and die before its first
  object lands; the survivor found no object, an unrankable `nil` stamp skipped the fleet ask, and
  it served the shard EMPTY while peers held the writes. `Recovery.absent_stamp/0` ranks "nothing"
  as `{0, 0, 0}`. These pin, as pure functions, that it is ahead of nothing a replica cannot state,
  that it carries the ordinary responder floor, and that only a lease taken from another holder
  qualifies. The open-path tests are in `promote_on_open_test.exs`.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.PromoteOnOpen
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Storage

  @replica %{lineage: 3, wal_ordinal: 2, next_offset: 4096}

  describe "Promote.fresher?/2 against :absent" do
    test "a replica with a stated lineage and ordinal is ahead of no object" do
      assert Promote.fresher?(@replica, Recovery.absent_stamp())
    end

    test "nothing a replica cannot state ranks above no object" do
      absent = Recovery.absent_stamp()
      refute Promote.fresher?(nil, absent)
      refute Promote.fresher?(%{@replica | lineage: 0}, absent)
      refute Promote.fresher?(%{@replica | wal_ordinal: 0}, absent)
      refute Promote.fresher?(Map.put(@replica, :torn, true), absent)
    end

    test "an unstamped real object (nil) is still never overridable" do
      refute Promote.fresher?(@replica, nil)
    end
  end

  test "an absent stamp is rankable and carries the N - Q + 1 floor" do
    assert Recovery.rankable_stamp?(Recovery.absent_stamp())
    peers = [{"a", ~c"h", 1}, {"b", ~c"h", 2}, {"c", ~c"h", 3}]
    assert Recovery.answers_needed(peers, Recovery.absent_stamp(), quorum: 2) == 2
    assert Recovery.answers_needed(peers, nil, quorum: 2) == 0
  end

  describe "Recovery.recheck/4 for a decision made against no object" do
    test "an unchanged etag keeps the decision" do
      head = %{etag: "sentinel", position: nil}
      assert Recovery.recheck(head, head, @replica, :absent) == :ok
      assert Recovery.recheck(nil, nil, @replica, :absent) == :ok
    end

    test "an object published in between still declines it" do
      before = %{etag: "sentinel", position: nil}
      now = %{etag: "real", position: %{epoch: 9, wal_ordinal: 9, offset: 0}}
      assert {:error, {:object_moved, _, _}} = Recovery.recheck(before, now, @replica, :absent)
    end

    test "without the decided stamp a nil position declines, as before" do
      head = %{etag: "x", position: nil}
      assert {:error, {:object_advanced, nil}} = Recovery.recheck(head, head, @replica)
    end
  end

  describe "PromoteOnOpen.took_from_another_holder?/2" do
    test "a lock stolen from another owner qualifies" do
      assert PromoteOnOpen.took_from_another_holder?(%{prev_owner: "n2#1"}, "n1#1")
    end

    test "a fresh create or a reclaim with no previous owner does not" do
      refute PromoteOnOpen.took_from_another_holder?(%{owner: "n1#1"}, "n1#1")
    end

    test "a released marker qualifies only when it names another node" do
      marker = Storage.released_owner()

      assert PromoteOnOpen.took_from_another_holder?(
               %{prev_owner: marker, released_by: "n2#1"},
               "n1#2"
             )

      refute PromoteOnOpen.took_from_another_holder?(
               %{prev_owner: marker, released_by: "n1#1"},
               "n1#2"
             )

      refute PromoteOnOpen.took_from_another_holder?(%{prev_owner: marker}, "n1#2")
    end
  end
end
