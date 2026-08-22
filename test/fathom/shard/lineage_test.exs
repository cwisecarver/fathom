defmodule Fathom.Shard.LineageTest do
  @moduledoc """
  The LINEAGE counter (expert review 2026-08-20 #8) — the number that fills a position stamp's
  `epoch` slot, and the reason it is no longer the lock epoch.

  ## The defect

  `release_lease` DELETES the lock object, so the next `acquire_lease` takes the optimistic
  `PUT If-None-Match: *` create path and starts again at **1**. The number therefore CLIMBED on
  crash-steals and RESET on every clean idle-drop, drain and rebalance handoff — while
  `Promote.fresher?/2` read it as the high-order component of a total order. A shard that had been
  dropped cleanly could be outranked by a replica holding a number from an earlier ownership, and
  that replica would be promoted over a perfectly good object.

  ## What is tested where

  This file covers the two layers that need no coordinator: the seed arithmetic
  (`Storage.next_lineage/1`) and the backend round trip. The end-to-end property — the stamped
  epoch rises across a real clean release/re-acquire while the LOCK epoch resets — lives in
  `ownership_cycle_position_test.exs`, beside the characterization it replaces.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage

  describe "next_lineage/1 — the seed arithmetic" do
    test "a shard nothing has ever flushed starts at 1" do
      assert Storage.next_lineage(nil) == 1
    end

    test "a stored lineage advances by exactly one" do
      assert Storage.next_lineage(%{lineage: 7, position: nil}) == 8
    end

    test "an object from BEFORE lineage tracking seeds from its position epoch" do
      # Pre-#8 objects carry a stamp whose epoch is the lock epoch and no lineage key at all.
      # Ignoring it would restart at 1 underneath everything that node already shipped.
      assert Storage.next_lineage(%{lineage: nil, position: %{epoch: 9, wal_gen: 0, offset: 0}}) ==
               10
    end

    test "takes the MAX of the two, not a preference — the rolling-upgrade hole" do
      # THE CASE THAT MAKES THIS max/2 RATHER THAN A FALLBACK CHAIN. During a rolling upgrade the
      # object is written alternately by nodes that stamp a lineage and nodes that still stamp the
      # lock epoch, and `flush/5`'s nil deliberately leaves an existing lineage in place — so the
      # two keys drift and EITHER can be larger.
      #
      # Seeding from the lineage alone would answer 4 here, below the 12 a not-yet-upgraded peer
      # has already shipped to its replicas. That replica would then outrank the object: the exact
      # promotion this counter exists to prevent, reintroduced by the migration to it.
      assert Storage.next_lineage(%{lineage: 3, position: %{epoch: 12, wal_gen: 0, offset: 0}}) ==
               13

      # ...and symmetrically, a stale position epoch never drags a good lineage back down.
      assert Storage.next_lineage(%{lineage: 20, position: %{epoch: 2, wal_gen: 0, offset: 0}}) ==
               21
    end

    test "a head with neither key still yields a usable 1" do
      assert Storage.next_lineage(%{lineage: nil, position: nil}) == 1
    end

    test "the result is always strictly greater than both inputs" do
      # The single property everything above is an instance of, asserted directly so a future
      # rewrite of the arithmetic cannot satisfy the examples and break the rule.
      for lineage <- [nil, 0, 1, 5, 999],
          epoch <- [nil, 0, 1, 5, 999] do
        pos = if epoch, do: %{epoch: epoch, wal_gen: 0, offset: 0}, else: nil
        next = Storage.next_lineage(%{lineage: lineage, position: pos})

        assert next > (lineage || 0),
               "next_lineage(#{inspect(lineage)}, #{inspect(epoch)}) = #{next} did not exceed the stored lineage"

        assert next > (epoch || 0),
               "next_lineage(#{inspect(lineage)}, #{inspect(epoch)}) = #{next} did not exceed the stored epoch"
      end
    end
  end

  describe "the backend round trip" do
    setup do
      root = Path.join(System.tmp_dir!(), "lineage_#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      prev = Application.get_env(:fathom, Fathom.Shard.Storage.Local)
      Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: root)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, Fathom.Shard.Storage.Local),
          else: Application.put_env(:fathom, Fathom.Shard.Storage.Local, prev)

        File.rm_rf(root)
      end)

      id = "lin_#{System.unique_integer([:positive])}"
      src = Path.join(root, "src.db")
      File.write!(src, "not a real database, but bytes are bytes for a flush")
      {:ok, id: id, src: src}
    end

    test "a flushed lineage reads back through object_head/1", %{id: id, src: src} do
      assert {:ok, _etag} = Storage.Local.flush(id, src, nil, nil, 4)
      assert {:ok, %{lineage: 4}} = Storage.Local.object_head(id)
    end

    test "nil LEAVES the previous lineage in place — the asymmetry with position", %{
      id: id,
      src: src
    } do
      # Position and lineage are deliberately NOT symmetric on nil. A stale POSITION describes
      # bytes that moved and would lose writes, so it is overwritten. The lineage describes
      # ownership history and must only ever go up — so a caller with nothing to claim (a migration
      # copy, a benchmark, a not-yet-upgraded flush path) leaves it alone rather than erasing it.
      #
      # Erasing it would reintroduce exactly the reset this counter exists to prevent, which is why
      # this is asserted rather than left to the reader of write_lineage/2.
      assert {:ok, etag} = Storage.Local.flush(id, src, nil, nil, 6)
      assert {:ok, _etag2} = Storage.Local.flush(id, src, etag, nil, nil)
      assert {:ok, %{lineage: 6}} = Storage.Local.object_head(id)
    end

    test "an object flushed with no lineage at all reads back nil, never 0", %{id: id, src: src} do
      # nil and 0 are different answers: 0 is a real stamped value, nil means "nothing has ever
      # claimed one". next_lineage/1 treats them identically today, but a consumer that invented
      # 0 from absence would let a fresh owner reuse a number a previous one already used.
      assert {:ok, _etag} = Storage.Local.flush(id, src, nil, nil, nil)
      assert {:ok, %{lineage: nil}} = Storage.Local.object_head(id)
    end

    test "the head of a shard with no object at all is nil, not a head full of nils", %{id: id} do
      assert {:ok, nil} = Storage.Local.object_head(id <> "_absent")
    end
  end
end
