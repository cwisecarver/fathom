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

  alias Fathom.Rebalancer.NodeBeat
  alias Fathom.Repo

  @doc """
  Records `node_key` as alive now, and optionally this window's full-distribution stats
  (`:q_p99`, `:sample_count` — finding #2). Upsert on node_key. Returns `:ok`.
  """
  @spec beat(String.t(), keyword()) :: :ok
  def beat(node_key, opts \\ []) do
    now = DateTime.utc_now()
    q_p99 = opts[:q_p99]
    sample_count = opts[:sample_count]

    Repo.insert!(
      %NodeBeat{node_key: node_key, last_seen_at: now, q_p99: q_p99, sample_count: sample_count},
      on_conflict: [set: [last_seen_at: now, q_p99: q_p99, sample_count: sample_count]],
      conflict_target: :node_key
    )

    :ok
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
  The fleet hot bar (finding #2): a **count-weighted mean** of the full-distribution p99s of
  live nodes that are **actually serving load** (`sample_count > 0`), or `nil` if the fleet's
  total sample count is below `min_samples` (the bar isn't trustworthy yet, so the policy falls
  back to the floor / legacy path rather than acting on noise).

  Aggregating only over loaded nodes, weighted by their sample count, is the refinement from the
  2026-07-08 rig run: the earlier median over *all* live nodes collapsed toward 0 when a hotspot
  concentrated on a minority of nodes (the idle nodes each contributed a p99 of 0), so the
  p99-relative bar flagged nothing exactly when it mattered. A busier node now contributes
  proportionally more and idle nodes can't drag the bar down. This is an approximation of the
  true pooled-distribution p99 (a faithful merge would need per-node histograms, unwarranted for
  a fallback bar — the absolute floor is the recommended path).
  """
  @spec fleet_p99(non_neg_integer(), non_neg_integer()) :: float() | nil
  def fleet_p99(within_ms, min_samples) do
    cutoff = DateTime.add(DateTime.utc_now(), -within_ms, :millisecond)

    stats =
      Repo.all(
        from n in NodeBeat,
          where: n.last_seen_at >= ^cutoff and not is_nil(n.q_p99) and n.sample_count > 0,
          select: {n.q_p99, n.sample_count}
      )

    total = stats |> Enum.map(fn {_p99, c} -> c end) |> Enum.sum()

    if total >= min_samples and total > 0 do
      weighted = stats |> Enum.map(fn {p99, c} -> p99 * c end) |> Enum.sum()
      weighted / total
    else
      nil
    end
  end
end
