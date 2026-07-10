defmodule Fathom.Rebalancer.WarmLocations do
  @moduledoc """
  The warm-location signal (`shard_warm_locations`) — which nodes have a given hot shard
  warm-cached, so the rebalancer can prefer a handoff target that already holds it (Phase 2 C,
  folded into B1). Written per-node by `Fathom.Rebalancer.Reporter` (warmth is node-local, no
  BEAM cluster) and read by `Fathom.Rebalancer.RebalanceJob`.
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Rebalancer.WarmLocation
  alias Fathom.Repo

  @doc """
  Makes `node_key`'s advertised warm set exactly `shard_ids` (the intersection of the
  fleet-hot shards and this node's warm cache): upserts a fresh row per shard and drops this
  node's rows for shards no longer warm+hot. Returns `:ok`.
  """
  @spec publish(String.t(), [String.t()]) :: :ok
  def publish(node_key, shard_ids) when is_list(shard_ids) do
    # Isolation gate (review 2026-07-09 #6): insert_all bypasses the changeset, and a
    # shard_warm_locations row feeds affinity target selection — so drop any id ShardId
    # wouldn't accept (defense-in-depth; the ids come from validated ShardLoad samples).
    do_publish(node_key, Enum.filter(shard_ids, &Fathom.ShardId.valid?/1))
  end

  defp do_publish(node_key, []) do
    Repo.delete_all(from w in WarmLocation, where: w.node_key == ^node_key)
    :ok
  end

  defp do_publish(node_key, shard_ids) do
    now = DateTime.utc_now()

    # Retract advertisements for shards this node no longer warms (cooled out of the hot set,
    # or LRU-evicted from the warm cache).
    Repo.delete_all(
      from w in WarmLocation, where: w.node_key == ^node_key and w.shard_id not in ^shard_ids
    )

    rows = Enum.map(shard_ids, &%{node_key: node_key, shard_id: &1, updated_at: now})

    Repo.insert_all(WarmLocation, rows,
      on_conflict: {:replace, [:updated_at]},
      conflict_target: [:node_key, :shard_id]
    )

    :ok
  end

  @doc """
  The warm map for shards advertised within `within_ms`: `%{shard_id => MapSet(node_key)}`.
  A stale (unrefreshed) row — e.g. a dead node's — falls out of the window and is ignored.
  """
  @spec warm_nodes(non_neg_integer()) :: %{optional(String.t()) => MapSet.t()}
  def warm_nodes(within_ms) do
    cutoff = DateTime.add(DateTime.utc_now(), -within_ms, :millisecond)

    from(w in WarmLocation, where: w.updated_at >= ^cutoff, select: {w.shard_id, w.node_key})
    |> Repo.all()
    |> Enum.group_by(fn {s, _n} -> s end, fn {_s, n} -> n end)
    |> Map.new(fn {s, nodes} -> {s, MapSet.new(nodes)} end)
  end

  @doc "Deletes rows not refreshed within `older_than_ms` (dead-node leftovers). Returns count."
  @spec prune(non_neg_integer()) :: non_neg_integer()
  def prune(older_than_ms) do
    cutoff = DateTime.add(DateTime.utc_now(), -older_than_ms, :millisecond)
    {n, _} = Repo.delete_all(from w in WarmLocation, where: w.updated_at < ^cutoff)
    n
  end
end
