defmodule Fathom.Rebalancer.LoadSample do
  @moduledoc """
  One per-node ShardLoad rate sample — the `shard_load_samples` row a
  `Fathom.Rebalancer.Reporter` publishes each window for a hot shard on its node.

  `node_key` is the reporting node's stable rebalancer key (`Fathom.Rebalancer.node_key/0`),
  which is also the shard's **current serving node** in the LB-partition model (a shard is
  served where its subdomain hashes / is pinned). The rebalancer reads a short history of
  these across all nodes to find persistently-hot shards and their current node — the data
  the request path never persists.
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
