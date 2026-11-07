defmodule Fathom.Shard.Storage.StrandedReleaseTest do
  @moduledoc """
  `Storage.stranded_release/1` (expert review 2026-11-07 #26): what a node does with a lock that
  names it after an acquire of its own FAILED. Pure, and shared by every backend so they decide alike.

  The decision is deliberately NARROW. Deleting a lock turns the next acquire into a fresh create at
  epoch 1 with no steal-touch, and turning a node's own ordinary lock into a released marker hands a
  shard whose local copy may hold unflushed writes to the first peer to ask — so only the two shapes
  a failed acquire can be shown to have written are touched.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  @us "us@node#1"

  test "a plain first-create lock is deleted" do
    assert Storage.stranded_release(%{owner: @us, epoch: 1, expires_at_ms: 1}) == :delete
  end

  test "a lock that carries a claim or a higher epoch is KEPT, never deleted or marked" do
    # It may be this node's own lock from before the failed acquire (a coordinator that crashed
    # while the node stayed up), which is what keeps peers out of a dirty local copy.
    assert Storage.stranded_release(%{owner: @us, epoch: 1, expires_at_ms: 1, claim: 4}) == :keep
    assert Storage.stranded_release(%{owner: @us, epoch: 6, expires_at_ms: 1}) == :keep
  end

  describe "an interrupted steal (touch_pending)" do
    test "becomes a CLEAN released marker that names the previous holder, at the same epoch" do
      lock = %{
        owner: @us,
        epoch: 6,
        expires_at_ms: 1,
        claim: 9,
        touch_pending: true,
        prev_owner: "dead@node#7"
      }

      assert {:marker, marker} = Storage.stranded_release(lock)

      assert marker.owner == Storage.released_owner()
      assert marker.epoch == 6, "the epoch must not move, and must never fall back"
      assert marker.claim == 9, "the lineage claim must survive for the successor"

      assert marker.released_by == "dead@node#7",
             "recovery must still exclude the PREVIOUS holder"

      refute Map.has_key?(marker, :dirty)
    end

    test "carries a taken-over clean marker's touch provenance and last holder through" do
      lock = %{
        owner: @us,
        epoch: 6,
        expires_at_ms: 1,
        touch_pending: true,
        prev_owner: Storage.released_owner(),
        released_by: "visitor@node#3",
        touched_from: "E0",
        touched_to: "E1"
      }

      assert {:marker, marker} = Storage.stranded_release(lock)
      assert marker.released_by == "visitor@node#3"
      assert {marker.touched_from, marker.touched_to} == {"E0", "E1"}
    end
  end

  describe "Local.release_if_owned/2" do
    setup do
      shard = "stranded_#{System.unique_integer([:positive])}"
      on_exit(fn -> File.rm(Path.join(Local.dir(), shard <> ".lock")) end)
      %{shard: shard}
    end

    test "deletes a first-create lock of the caller", %{shard: shard} do
      {:ok, _} = Local.acquire_lease(shard, @us, 30_000)

      assert :ok = Local.release_if_owned(shard, @us)
      assert Local.lease_holder(shard) == :free
    end

    test "leaves a lock above epoch 1 exactly as it was", %{shard: shard} do
      {:ok, first} = Local.acquire_lease(shard, "a@node#1", 30_000)
      :ok = Local.release_lease_keeping_claim(shard, first, nil)
      {:ok, %{epoch: 2}} = Local.acquire_lease(shard, @us, 30_000)
      before = File.read!(Path.join(Local.dir(), shard <> ".lock"))

      assert :ok = Local.release_if_owned(shard, @us)
      assert File.read!(Path.join(Local.dir(), shard <> ".lock")) == before
    end
  end
end
