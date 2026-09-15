defmodule Fathom.Rebalancer.WarmLocation do
  @moduledoc """
  One row of the warm-location signal (`shard_warm_locations`) — "node `node_key` holds hot
  shard `shard_id` warm." Published per reporter window for the intersection of fleet-hot shards
  and this node's **A2 replica set** (`Fathom.Shard.Replication.Follower.replica_shard_ids/1`), and
  read by the rebalancer to prefer a handoff target that already holds the shard warm (affinity-aware
  placement; Phase 2 C folded into B1). The signal was re-sourced from the A2 replica set on
  2026-09-14 when the `WarmFollower` read cache was retired — a replica holder already has recent
  bytes, so a handoff there is a peer-pull rather than a full cold S3 pull.
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
