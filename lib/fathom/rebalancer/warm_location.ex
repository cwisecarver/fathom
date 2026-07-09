defmodule Fathom.Rebalancer.WarmLocation do
  @moduledoc """
  One row of the warm-location signal (`shard_warm_locations`) — "node `node_key` has hot
  shard `shard_id` warm-cached." Published per reporter window for the intersection of
  fleet-hot shards and this node's `Fathom.Shard.WarmFollower` cache, and read by the
  rebalancer to prefer a handoff target that already holds the shard warm (affinity-aware
  placement; Phase 2 C folded into B1).
  """
  use Ecto.Schema

  @type t :: %__MODULE__{}
  @primary_key false

  schema "shard_warm_locations" do
    field :node_key, :string
    field :shard_id, :string
    field :updated_at, :utc_datetime_usec
  end
end
