defmodule Fathom.Rebalancer.Nodes do
  @moduledoc """
  The per-node_key liveness registry (finding #1b) — how the fleet knows which nodes are
  alive by their stable `Fathom.Rebalancer.node_key/0`. Each node `beat/1`s every reporter
  tick; the `RebalanceJob` reads `alive/1` to unpin overrides whose node has gone silent
  (a dead node), so a pinned hot shard re-homes instead of being stranded on a dead node.

  Assumes the B1 deployment posture where every node runs the load reporter (`:load_reporter`
  on) — the beat rides that tick — so a node that serves shards is also beating.
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Rebalancer.{NodeBeat, Stats}
  alias Fathom.Repo

  @doc """
  Records `node_key` as alive now, and optionally this window's full-distribution stats
  (`:q_p99`, `:sample_count`, `:q_hist` — findings #2/#4) and `:replication_address`.
  Upsert on node_key. Returns `:ok`.
  """
  @spec beat(String.t(), keyword()) :: :ok
  def beat(node_key, opts \\ []) do
    now = DateTime.utc_now()
    # A liveness-only `beat/1` (no stats) must NOT erase the node's last-published
    # q_p99/sample_count/q_hist (review 2026-07-09 #5): a plain beat with an unconditional
    # `set: [q_hist: nil, ...]` would NULL them and drop the node from the fleet-p99 bar.
    # Include the stats in the upsert ONLY when supplied.
    #
    # `:replication_address` joins that list for a sharper version of the same reason: NULLing it
    # does not just lose a gauge, it withdraws the node from A2's membership candidates. A beat
    # that forgot to carry it would silently shrink every shipper's follower set.
    stats = Keyword.take(opts, [:q_p99, :sample_count, :q_hist, :replication_address])

    Repo.insert!(
      struct(%NodeBeat{node_key: node_key, last_seen_at: now}, stats),
      on_conflict: [set: Keyword.put(stats, :last_seen_at, now)],
      conflict_target: :node_key
    )

    :ok
  end

  @doc "Every node beat row — the raw fleet roster (node_key, last_seen_at, q_p99, sample_count). O(nodes)."
  @spec all() :: [NodeBeat.t()]
  def all, do: Repo.all(NodeBeat)

  @doc """
  Replication endpoints published by nodes seen within `within_ms`, as `{node_key, address}`
  ordered by node_key — A2's membership candidates.

  Rows with a NULL `replication_address` are excluded: a node that does not listen, or runs a
  release predating the column, is not a candidate rather than an endpoint that would refuse.

  **Ordered, and the order is deterministic on purpose.** Every node computes its own follower set
  from this same list, and an unordered result would give each node a different set on each read —
  so a shard's replica set would drift with query planning rather than with the fleet. `node_key`
  is the stable identity the LB, the rebalancer and `Fleet.health/0` already key on.

  The staleness window is the caller's, and it is a *candidacy* filter applied when membership is
  recomputed — never a per-commit liveness filter. See `Fathom.Shard.Replication.Fleet`.
  """
  @spec replication_endpoints(non_neg_integer()) :: [{String.t(), String.t()}]
  def replication_endpoints(within_ms) do
    cutoff = DateTime.add(DateTime.utc_now(), -within_ms, :millisecond)

    from(n in NodeBeat,
      where: n.last_seen_at >= ^cutoff and not is_nil(n.replication_address),
      order_by: n.node_key,
      select: {n.node_key, n.replication_address}
    )
    |> Repo.all()
  end

  @doc "The set of node_keys seen within `within_ms` ago (the live fleet)."
  @spec alive(non_neg_integer()) :: MapSet.t()
  def alive(within_ms) do
    cutoff = DateTime.add(DateTime.utc_now(), -within_ms, :millisecond)

    from(n in NodeBeat, where: n.last_seen_at >= ^cutoff, select: n.node_key)
    |> Repo.all()
    |> MapSet.new()
  end

  @doc """
  The fleet hot bar (findings #2/#4): the **true pooled-distribution p99** of the fleet's
  per-shard q/s, computed by summing live nodes' published `q_hist` histograms element-wise and
  reading the p99 of the merged histogram (`Fathom.Rebalancer.Stats`). Returns `nil` when the
  pooled sample count is below `min_samples` (the bar isn't trustworthy yet, so the policy makes
  no p99-relative move — it does NOT fall back to a truncated-head percentile, review #4).

  This replaced a count-weighted *mean* of per-node p99s, which is not the pooled percentile and
  drifted with fleet composition (a busy node could pull the bar the wrong way). Summing the
  histograms is exact up to bucket resolution: an idle node contributes all-zero counts (can't
  drag the bar), and a busy node contributes its actual per-shard spread (not just its p99).
  """
  @spec fleet_p99(non_neg_integer(), non_neg_integer()) :: float() | nil
  def fleet_p99(within_ms, min_samples) do
    cutoff = DateTime.add(DateTime.utc_now(), -within_ms, :millisecond)

    pooled =
      Repo.all(
        from n in NodeBeat,
          where: n.last_seen_at >= ^cutoff and not is_nil(n.q_hist),
          select: n.q_hist
      )
      |> merge_histograms()

    total = Enum.sum(pooled)

    if total >= min_samples and total > 0 do
      Stats.percentile_from_histogram(pooled, 99)
    else
      nil
    end
  end

  # Element-wise sum of the live nodes' histograms. All share Stats.bucket_edges/0, so they're
  # equal-length; a defensive length filter drops any pre-schema/legacy vector rather than
  # silently truncating the sum via zip.
  defp merge_histograms(hists) do
    width = Stats.bucket_count()

    case Enum.filter(hists, &(length(&1) == width)) do
      [] -> []
      [first | rest] -> Enum.reduce(rest, first, fn h, acc -> Enum.zip_with(acc, h, &+/2) end)
    end
  end
end
