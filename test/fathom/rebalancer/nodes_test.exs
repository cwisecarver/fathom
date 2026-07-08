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

  test "fleet_p99 is the median of live nodes' p99, guarded by min sample count (#2)" do
    :ok = Nodes.beat("n1", q_p99: 2.0, sample_count: 40)
    :ok = Nodes.beat("n2", q_p99: 4.0, sample_count: 40)

    # total 80 ≥ min 50 → median([2.0, 4.0]) = 3.0
    assert Nodes.fleet_p99(60_000, 50) == 3.0
    # min guard: total 80 < 200 → nil (untrusted; policy falls back)
    assert Nodes.fleet_p99(60_000, 200) == nil
  end

  test "fleet_p99 ignores stale nodes and nil-p99 nodes (#2)" do
    :ok = Nodes.beat("live", q_p99: 5.0, sample_count: 100)
    :ok = Nodes.beat("nop99", [])

    # Stale node with a p99 must not count.
    :ok = Nodes.beat("stale", q_p99: 99.0, sample_count: 100)

    from(n in NodeBeat, where: n.node_key == "stale")
    |> Repo.update_all(
      set: [last_seen_at: DateTime.add(DateTime.utc_now(), -120_000, :millisecond)]
    )

    # Only "live" contributes → median([5.0]) = 5.0
    assert Nodes.fleet_p99(60_000, 50) == 5.0
  end
end
