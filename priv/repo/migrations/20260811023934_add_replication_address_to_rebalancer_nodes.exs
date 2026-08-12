defmodule Fathom.Repo.Migrations.AddReplicationAddressToRebalancerNodes do
  use Ecto.Migration

  # Where this node accepts replication frames, as `host:port` — what A2 membership resolves to a
  # `Shipper` endpoint. It lives on the EXISTING roster rather than in a new table because
  # `rebalancer_nodes` already is the fleet roster: one row per `Fathom.Rebalancer.node_key/0`,
  # refreshed by the same beat, already read by `Fleet.health/0` for follower liveness. A second
  # roster would need its own writer, its own staleness window, and its own way to disagree.
  #
  # NULLABLE on purpose, and it is the safe direction: a node that does not listen has no address
  # to publish, and a node running an older release publishes none either. Membership treats NULL
  # as "not a candidate", so a rolling upgrade shrinks the candidate set rather than pointing
  # shippers at endpoints that refuse — and the guarded swap in Fleet refuses to shrink the live
  # set below quorum+1 regardless.
  def change do
    alter table(:rebalancer_nodes) do
      add :replication_address, :string
    end
  end
end
