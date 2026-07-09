defmodule Fathom.Rebalancer.LoadSamples do
  @moduledoc """
  Read side of the per-node load samples the `Fathom.Rebalancer.Reporter` publishes.
  The control plane reads a **short history** across all nodes to find persistently-hot
  shards (anti-flap) and their current serving node; the rebalance `Policy` consumes it.

  NB (expert review #15): `since/1`, `latest_per_shard/1`, and `prune/1` order/compare on
  `sampled_at` — each reporter's wall clock — so they assume the fleet is NTP-synced within
  a few seconds (small vs the ~10s window). See `Fathom.Rebalancer.LoadSample` for the skew
  failure modes; a single-server-clock ordering is a deferred fuller fix.
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Rebalancer.LoadSample
  alias Fathom.Repo

  @doc "Every sample newer than `ms` ago (default 120_000), newest first."
  @spec since(non_neg_integer()) :: [LoadSample.t()]
  def since(ms \\ 120_000) do
    cutoff = DateTime.add(DateTime.utc_now(), -ms, :millisecond)
    Repo.all(from s in LoadSample, where: s.sampled_at >= ^cutoff, order_by: [desc: s.sampled_at])
  end

  @doc """
  The most recent sample per shard within `ms` (default 120_000) — each shard's current
  rate and current owner (serving node). One row per shard_id, newest wins.
  """
  @spec latest_per_shard(non_neg_integer()) :: [LoadSample.t()]
  def latest_per_shard(ms \\ 120_000) do
    ms
    |> since()
    |> Enum.reduce(%{}, fn s, acc -> Map.put_new(acc, s.shard_id, s) end)
    |> Map.values()
  end

  @doc "Total current query rate per node_key (serving node) — the target-selection input."
  @spec node_load(non_neg_integer()) :: %{optional(String.t()) => float()}
  def node_load(ms \\ 120_000) do
    ms
    |> latest_per_shard()
    |> Enum.reduce(%{}, fn s, acc ->
      Map.update(acc, s.node_key, s.q_per_s, &(&1 + s.q_per_s))
    end)
  end

  @doc """
  Distinct shard_ids seen in any node's samples within `ms` — the recent fleet-hot set. The
  reporter intersects this with its local warm cache to publish the warm-location signal (the
  affinity-aware target input).
  """
  @spec recent_shard_ids(non_neg_integer()) :: [String.t()]
  def recent_shard_ids(ms \\ 120_000) do
    cutoff = DateTime.add(DateTime.utc_now(), -ms, :millisecond)

    Repo.all(
      from s in LoadSample, where: s.sampled_at >= ^cutoff, select: s.shard_id, distinct: true
    )
  end

  @doc "Deletes samples older than `ms` ago (ops/test helper; the reporter prunes too)."
  @spec prune(non_neg_integer()) :: {non_neg_integer(), nil}
  def prune(ms) do
    cutoff = DateTime.add(DateTime.utc_now(), -ms, :millisecond)
    Repo.delete_all(from s in LoadSample, where: s.sampled_at < ^cutoff)
  end
end
