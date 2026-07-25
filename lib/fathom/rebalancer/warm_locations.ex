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
    valid_ids = Enum.filter(shard_ids, &Fathom.ShardId.valid?/1)
    now = DateTime.utc_now()

    # Upsert FIRST, stamping `now` on every kept/inserted row (a no-op for an empty set).
    rows = Enum.map(valid_ids, &%{node_key: node_key, shard_id: &1, updated_at: now})

    Repo.insert_all(WarmLocation, rows,
      on_conflict: {:replace, [:updated_at]},
      conflict_target: [:node_key, :shard_id]
    )

    # ...then retract exactly the rows this publish DIDN'T refresh — the ones this node no
    # longer warms (cooled out of the hot set, or LRU-evicted). An age sweep (`updated_at <
    # now`) is a constant 2-parameter query, not `shard_id not in ^valid_ids` — that bind list
    # grows with the warm∩hot set and can blow Postgres's 65535-parameter limit at fleet scale,
    # where the reporter's rescue would then silently drop every window (review #14). Safe
    # without a transaction: only this node's single Reporter publishes for this node_key, and
    # the sweep is node-scoped. Pairs with the [:updated_at] index (#13). Handles the empty set
    # too: nothing stamped `now`, so every row is older and gets swept.
    Repo.delete_all(
      from w in WarmLocation, where: w.node_key == ^node_key and w.updated_at < ^now
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
    # Same node-scoped shape as Rebalancer.Reporter's sample prune (expert review 2026-07-24 #27):
    # every node used to delete the whole expired set every window, so N-1 of N nodes did a full
    # index range scan and then blocked on row locks to delete nothing. Node-scoped deletes are
    # disjoint; the rare fleet sweep still reclaims a departed node's rows, with a much older cutoff
    # so it cannot race a live node's fresh ones.
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, -older_than_ms, :millisecond)
    key = Fathom.Rebalancer.node_key()

    {n, _} =
      Repo.delete_all(
        from w in WarmLocation, where: w.node_key == ^key and w.updated_at < ^cutoff
      )

    if sweep_now?() do
      stale = DateTime.add(now, -(older_than_ms * 3), :millisecond)
      Repo.delete_all(from w in WarmLocation, where: w.updated_at < ^stale)
    end

    n
  end

  # 1-in-N per call by default. `:rebalancer_fleet_sweep_odds` of 1 forces it every call, which is
  # how the departed-node reclaim is tested deterministically rather than by retrying a coin flip.
  defp sweep_now? do
    case Application.get_env(:fathom, :rebalancer_fleet_sweep_odds, 30) do
      1 -> true
      n when is_integer(n) and n > 1 -> :rand.uniform(n) == 1
      _ -> false
    end
  end
end
