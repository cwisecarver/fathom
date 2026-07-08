defmodule Fathom.Rebalancer.LoadSample do
  @moduledoc """
  One per-node ShardLoad rate sample — the `shard_load_samples` row a
  `Fathom.Rebalancer.Reporter` publishes each window for a hot shard on its node.

  `node_key` is the reporting node's stable rebalancer key (`Fathom.Rebalancer.node_key/0`),
  which is also the shard's **current serving node** in the LB-partition model (a shard is
  served where its subdomain hashes / is pinned). The rebalancer reads a short history of
  these across all nodes to find persistently-hot shards and their current node — the data
  the request path never persists.

  ## Clock assumption (expert review #15)

  `sampled_at` is each reporter's **wall clock** at publish time. The reporter's *window*
  math is skew-immune (it diffs its own snapshots with a monotonic clock — see
  `Fathom.Rebalancer.Reporter`), but the cross-node reads that compare `sampled_at` —
  `LoadSamples.since/1` + `latest_per_shard/1` (which resolves a shard seen by two nodes
  mid-remap by the newest `sampled_at`) and the pruning cutoff — assume the fleet's clocks
  are **NTP-synced within a few seconds** (small vs the ~10s window). Under material skew a
  fast-clock node's rows can win the "newest" tie or survive pruning longer, mis-attributing
  a shard's current node or biasing load off a slow-clock node. Keep NTP healthy on
  rebalancer nodes; a fuller fix (order/prune on a single server clock) is deferred.
  """
  use Ecto.Schema

  @type t :: %__MODULE__{}

  schema "shard_load_samples" do
    field :node_key, :string
    field :shard_id, :string
    field :q_per_s, :float
    field :rows_read_per_s, :float
    field :checkouts_per_s, :float
    field :window_s, :float
    field :sampled_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end
end
