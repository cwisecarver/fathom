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

  @doc "Records `node_key` as alive now (upsert on node_key). Returns `:ok`."
  @spec beat(String.t()) :: :ok
  def beat(node_key) do
    now = DateTime.utc_now()

    Repo.insert!(
      %NodeBeat{node_key: node_key, last_seen_at: now},
      on_conflict: [set: [last_seen_at: now]],
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
end
