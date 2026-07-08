defmodule Fathom.Rebalancer.OverridesTest do
  @moduledoc "The LB exception table CRUD (one pin per shard, upserted)."
  use Fathom.DataCase, async: true

  alias Fathom.Rebalancer.Overrides

  test "pin creates a pin; re-pin upserts (one override per shard)" do
    {:ok, o1} =
      Overrides.pin("hot_1", "fathom2",
        reason: "hot",
        q_per_s_at_pin: 500.0,
        from_node: "fathom1"
      )

    assert o1.pinned_node == "fathom2"
    assert o1.from_node == "fathom1"

    # Re-pinning the same shard elsewhere updates in place, not a second row.
    {:ok, o2} = Overrides.pin("hot_1", "fathom3")
    assert o2.id == o1.id
    assert o2.pinned_node == "fathom3"
    assert length(Overrides.all()) == 1
    assert Overrides.for_shard("hot_1").pinned_node == "fathom3"
  end

  test "unpin removes the pin (back to pure hash); idempotent" do
    {:ok, _} = Overrides.pin("hot_1", "fathom2")
    assert :ok = Overrides.unpin("hot_1")
    assert Overrides.for_shard("hot_1") == nil
    # No-op on an already-unpinned shard.
    assert :ok = Overrides.unpin("hot_1")
  end

  test "mark_failed retains the row as a cooldown record (finding #4)" do
    # Regression for #4: a failed handoff must NOT delete the override — deleting it left
    # the still-hot shard with no cooldown record, so it re-proposed every tick (thrash).
    # mark_failed keeps the row (Policy cooldown honors its updated_at) but stamps failed_at
    # so the renderer skips it and traffic returns to the source.
    {:ok, _} = Overrides.pin("hot_1", "fathom2", from_node: "fathom1")
    assert :ok = Overrides.mark_failed("hot_1")

    o = Overrides.for_shard("hot_1")
    assert o != nil, "row retained as a cooldown record, not deleted"
    assert o.failed_at != nil
    # Still in all/0 so the Policy sees it for cooldown; excluded from pinned_to (reverted).
    assert Enum.map(Overrides.all(), & &1.shard_id) == ["hot_1"]
    assert Overrides.pinned_to("fathom2") == []

    # Idempotent-ish no-op when there's no row to cool.
    assert :ok = Overrides.mark_failed("never_pinned")
  end

  test "a fresh pin clears a prior failed_at (re-attempt becomes an active pin)" do
    {:ok, _} = Overrides.pin("hot_1", "fathom2", from_node: "fathom1")
    :ok = Overrides.mark_failed("hot_1")
    assert Overrides.for_shard("hot_1").failed_at != nil

    {:ok, _} = Overrides.pin("hot_1", "fathom3", from_node: "fathom1")
    assert Overrides.for_shard("hot_1").failed_at == nil
    assert Overrides.pinned_to("fathom3") == ["hot_1"]
  end

  test "pin rejects an invalid shard_id at the write boundary (isolation gate, #14)" do
    # The shard-isolation gate at the LB-exception boundary: an id that would inject nginx
    # directives when rendered raw must be refused here, not just far upstream on the request
    # path. Overrides.pin is public, so this is enforced by Override.changeset.
    assert {:error, changeset} = Overrides.pin("evil; } server { deny all; } #", "fathom2")
    assert %{shard_id: ["is not a valid shard id"]} = errors_on(changeset)
    # Nothing was written.
    assert Overrides.all() == []

    # A dot (subdomain ambiguity / traversal) is also rejected.
    assert {:error, _} = Overrides.pin("a.b", "fathom2")
  end

  test "all is shard-sorted (stable render) and pinned_to filters by node" do
    {:ok, _} = Overrides.pin("hot_3", "fathom1")
    {:ok, _} = Overrides.pin("hot_1", "fathom2")
    {:ok, _} = Overrides.pin("hot_2", "fathom2")

    assert Enum.map(Overrides.all(), & &1.shard_id) == ["hot_1", "hot_2", "hot_3"]
    assert Enum.sort(Overrides.pinned_to("fathom2")) == ["hot_1", "hot_2"]
    assert Overrides.pinned_to("fathom1") == ["hot_3"]
  end
end
