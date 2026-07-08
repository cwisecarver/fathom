defmodule Fathom.Rebalancer.NodeBeat do
  @moduledoc """
  One per-node_key liveness row (`rebalancer_nodes`) — the signal the dead-node reconciler
  keys on (finding #1b). `node_key` is the stable `Fathom.Rebalancer.node_key/0` a shard can
  be pinned to; `last_seen_at` is refreshed every reporter tick. Distinct from the S3
  heartbeat, which is keyed by the boot-scoped lease owner (`node()#incarnation`), not the
  node_key the LB exception table references.
  """
  use Ecto.Schema

  @type t :: %__MODULE__{}
  @primary_key {:node_key, :string, autogenerate: false}

  schema "rebalancer_nodes" do
    field :last_seen_at, :utc_datetime_usec
  end
end
