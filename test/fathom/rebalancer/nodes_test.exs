defmodule Fathom.Rebalancer.NodesTest do
  @moduledoc "The per-node_key liveness registry (dead-node reconciler input, #1b)."
  use Fathom.DataCase, async: true

  alias Fathom.Rebalancer.{NodeBeat, Nodes}

  test "beat upserts one row per node_key; alive reflects the window (#1b)" do
    assert :ok = Nodes.beat("n1")
    assert :ok = Nodes.beat("n1")
    assert :ok = Nodes.beat("n2")

    alive = Nodes.alive(60_000)
    assert MapSet.member?(alive, "n1")
    assert MapSet.member?(alive, "n2")
    assert Repo.aggregate(NodeBeat, :count) == 2, "beat is an upsert, not an insert"

    # Backdate n2 past the window → it drops out of the live set (dead).
    from(n in NodeBeat, where: n.node_key == "n2")
    |> Repo.update_all(
      set: [last_seen_at: DateTime.add(DateTime.utc_now(), -120_000, :millisecond)]
    )

    alive2 = Nodes.alive(60_000)
    assert MapSet.member?(alive2, "n1")
    refute MapSet.member?(alive2, "n2")
  end
end
