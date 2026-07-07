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

  test "all is shard-sorted (stable render) and pinned_to filters by node" do
    {:ok, _} = Overrides.pin("hot_3", "fathom1")
    {:ok, _} = Overrides.pin("hot_1", "fathom2")
    {:ok, _} = Overrides.pin("hot_2", "fathom2")

    assert Enum.map(Overrides.all(), & &1.shard_id) == ["hot_1", "hot_2", "hot_3"]
    assert Enum.sort(Overrides.pinned_to("fathom2")) == ["hot_1", "hot_2"]
    assert Overrides.pinned_to("fathom1") == ["hot_3"]
  end
end
