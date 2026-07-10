defmodule Fathom.Rebalancer.NodesTest do
  @moduledoc "The per-node_key liveness registry (dead-node reconciler input, #1b)."
  use Fathom.DataCase, async: true

  alias Fathom.Rebalancer.{NodeBeat, Nodes, Stats}

  # Publish a node's window from a real per-shard q/s list, so q_hist and sample_count are
  # consistent (never hand-set independently — finding #4).
  defp report(node_key, qps) do
    Nodes.beat(node_key,
      q_p99: Stats.percentile(qps, 99),
      sample_count: length(qps),
      q_hist: Stats.histogram(qps)
    )
  end

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

  test "a liveness-only beat/1 preserves the node's published q_hist/stats (#5)" do
    report("n1", List.duplicate(5.0, 100))
    # A plain beat (no stats) must refresh last_seen_at WITHOUT wiping the published histogram,
    # or the node drops out of the fleet-p99 bar for a window.
    :ok = Nodes.beat("n1")

    # The preserved [5,10) histogram → pooled p99 ≈ 9.95 (not nil, not reset).
    p99 = Nodes.fleet_p99(60_000, 1)
    assert p99 > 5.0 and p99 < 10.0
  end

  test "fleet_p99 is the TRUE pooled-distribution p99, not a mean of per-node p99s (#4)" do
    # n1: 30 shards @ 3 q/s (bucket [2,5)), n2: 70 shards @ 150 q/s (bucket [100,200)).
    report("n1", List.duplicate(3.0, 30))
    report("n2", List.duplicate(150.0, 70))

    # Pooled: [2,5)=30, [100,200)=70; total 100, p99 rank 99 lands in [100,200):
    # 100 + (99-30)/70 * 100 ≈ 198.6. A count-weighted MEAN of p99s would be
    # (3*30 + 150*70)/100 = 105.9 — so the pooled percentile is materially different (and higher,
    # because 70% of shards sit at 150), which is the point of #4.
    p99 = Nodes.fleet_p99(60_000, 50)
    assert p99 > 190.0 and p99 < 200.0, "pooled p99 ≈ 198.6, not the 105.9 count-weighted mean"

    # min-samples guard on the POOLED count: total 100 < 200 → nil (untrusted; no p99 move).
    assert Nodes.fleet_p99(60_000, 200) == nil
  end

  test "fleet_p99: a concentrated hotspot isn't dragged down by idle-load nodes (#4)" do
    # Regression for the 2026-07-08 rig observation, done right: a hotspot node plus two nodes
    # carrying only trickle load. The old median-over-all-nodes collapsed to ~0; summing the
    # histograms puts the p99 rank up in the hot bucket, so the idle trickle can't drag the bar.
    report("hot", List.duplicate(300.0, 60))
    report("idle1", List.duplicate(0.5, 100))
    report("idle2", List.duplicate(0.5, 100))

    # Pooled: [0,1)=200, [200,500)=60; p99 rank 257.4 lands in [200,500) → ≈ 487.
    p99 = Nodes.fleet_p99(60_000, 50)
    assert p99 > 200.0, "pooled p99 reflects the hot load, not dragged toward 0 by the trickle"
  end

  test "fleet_p99 ignores stale nodes and nodes with no published histogram (#4)" do
    report("live", List.duplicate(5.0, 100))
    # A liveness-only node (no q_hist) doesn't contribute.
    :ok = Nodes.beat("nohist")

    # A stale node's histogram must not count even though it has one.
    report("stale", List.duplicate(300.0, 100))

    from(n in NodeBeat, where: n.node_key == "stale")
    |> Repo.update_all(
      set: [last_seen_at: DateTime.add(DateTime.utc_now(), -120_000, :millisecond)]
    )

    # Only "live" ([5,10)=100) contributes → pooled p99 ≈ 9.95.
    p99 = Nodes.fleet_p99(60_000, 50)
    assert p99 > 5.0 and p99 < 10.0
  end
end
