defmodule Fathom.Shard.ReleasedByFloorTest do
  @moduledoc """
  A released lease marker names the holder whose answer the next opener's recovery floor must not
  count (Jepsen 2026-10-28, the first power-loss run, shard 15).

  ## The defect

  Recovery excludes the PREVIOUS OWNER from its `N - Q + 1` responder floor: a node that held the
  lease was never its own follower, so its "I hold nothing" says nothing about the tail it wrote
  (expert review 2026-10-13 #14). The previous owner came from the stolen lock's owner string, and a
  released marker's owner is the sentinel `"-released-"`, so a steal of a marker excluded nobody.

  Observed: fathom2 wrote (and a quorum acked) and lost S3. fathom3 stole from fathom2, was refused
  under quorum, and released CLEAN, leaving a marker. fathom3's retry then stole its own marker,
  excluded nobody, counted fathom2's empty answer as the third definite answer, met the floor
  without the one live acker (paused), and opened from the stored object over an acked write.

  ## What is pinned

    * a holder that wrote (or may have) names itself on its marker;
    * a clean holder names the holder it took the lease from, through any number of clean visitors;
    * the name survives the lock body's encode/decode, and a steal of the marker carries it;
    * `Storage.floor_owner/1` of that steal names the real writer, and recovery's floor then
      refuses to count that writer's empty answer: the incident's 3 answers become 2.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Storage

  @writer "nodeA#1"

  setup do
    shard = "relby_#{System.unique_integer([:positive])}"
    prev_repl = Application.get_env(:fathom, :replication_enabled)

    # Replicating, so a steal records a lineage claim and a release leaves a marker (with no claim a
    # clean release deletes the lock, which is the case nothing here is about).
    Application.put_env(:fathom, :replication_enabled, true)

    on_exit(fn ->
      if is_nil(prev_repl),
        do: Application.delete_env(:fathom, :replication_enabled),
        else: Application.put_env(:fathom, :replication_enabled, prev_repl)

      dir = Storage.Local.dir()
      for p <- Path.wildcard(Path.join(dir, "#{shard}*")), do: File.rm_rf(p)
    end)

    %{shard: shard}
  end

  describe "released_marker/2" do
    test "a holder that may have written names itself" do
      lease = %{owner: "nodeB#2", epoch: 4, expires_at_ms: 1, prev_owner: @writer}
      assert %{released_by: "nodeB#2"} = Storage.released_marker(lease, 9)

      dirty = Storage.with_dirty_release(lease)
      assert %{released_by: "nodeB#2", dirty: true} = Storage.released_marker(dirty, 9)
    end

    test "a clean holder names the holder it took the lease from" do
      lease = %{owner: "nodeB#2", epoch: 4, expires_at_ms: 1, prev_owner: @writer}

      assert %{released_by: @writer} =
               Storage.released_marker(Storage.with_clean_touch(lease), 9)
    end

    test "a clean holder that stole a marker passes its name on" do
      lease = %{
        owner: "nodeC#3",
        epoch: 5,
        expires_at_ms: 1,
        prev_owner: Storage.released_owner(),
        released_by: @writer
      }

      assert %{released_by: @writer} =
               Storage.released_marker(Storage.with_clean_touch(lease), 9)
    end

    test "a clean holder of a fresh lock names nobody" do
      lease = %{owner: "nodeB#2", epoch: 1, expires_at_ms: 1}
      marker = Storage.released_marker(Storage.with_clean_touch(lease), 9)
      refute Map.has_key?(marker, :released_by)
    end

    test "the name survives the lock body" do
      marker = Storage.released_marker(%{owner: @writer, epoch: 4, expires_at_ms: 1}, 9)
      assert {:ok, %{released_by: @writer}} = Storage.decode_lease(Storage.encode_lease(marker))
    end
  end

  describe "floor_owner/1" do
    test "the stolen owner, or the writer a stolen marker names" do
      assert Storage.floor_owner(%{prev_owner: @writer}) == @writer

      assert Storage.floor_owner(%{prev_owner: Storage.released_owner(), released_by: @writer}) ==
               @writer

      assert Storage.floor_owner(%{prev_owner: Storage.released_owner()}) == nil
      assert Storage.floor_owner(%{}) == nil
    end
  end

  # The incident's lease lifecycle against the real Local backend, ending at the floor count.
  test "steal, refuse clean, re-steal: the floor still excludes the writer", %{shard: shard} do
    # The writer's lock, long dead (no heartbeat, expired TTL), with a lineage claim.
    File.mkdir_p!(Storage.Local.dir())

    File.write!(
      Path.join(Storage.Local.dir(), shard <> ".lock"),
      Jason.encode!(%{"owner" => @writer, "epoch" => 3, "expires_at_ms" => 0, "claim" => 4})
    )

    # The taker steals it, then is refused under quorum and releases clean, as `abandon_open/5`
    # does for an `%UnderQuorum{}` refusal.
    assert {:ok, %{prev_owner: @writer} = lease} = Storage.acquire_lease(shard, "nodeC#7", 30_000)
    assert is_integer(lease[:claim]), "precondition: no claim, so no marker would be left"
    assert :ok = Storage.release_lease(shard, Storage.with_clean_touch(lease), lease[:claim])

    # The retry steals its own marker.
    assert {:ok, retry} = Storage.acquire_lease(shard, "nodeC#7", 30_000)
    assert retry.prev_owner == Storage.released_owner(), "precondition: the retry stole a marker"
    assert Storage.floor_owner(retry) == @writer

    # The incident's answers: the taker's own replica, one stale peer, and the writer's empty answer.
    # Counting the writer met the floor of 3; excluding it leaves 2, so the open waits for the
    # paused acker instead of opening over its write.
    local = %{lineage: 19, wal_ordinal: 74, next_offset: 78_312}

    answers = [
      {{"nodeD", "h", 1}, {:answered, %{lineage: 19, wal_ordinal: 75, next_offset: 346_112}}},
      {{"nodeA", "h", 1}, {:answered, nil}}
    ]

    opts = [self_node: "nodeC"]

    assert Recovery.answered(answers, local, [{:previous_owner, retry.prev_owner} | opts]) == 3,
           "precondition: excluding only the sentinel counts the writer, as before the fix"

    assert Recovery.answered(answers, local, [
             {:previous_owner, Storage.floor_owner(retry)} | opts
           ]) ==
             2
  end
end
