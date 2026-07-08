defmodule Fathom.Repo.Migrations.CreateRebalancerNodes do
  use Ecto.Migration

  # Per-node_key liveness beat (finding #1b). The S3 heartbeat is keyed by the boot-scoped
  # lease owner (node()#incarnation), NOT the stable node_key the LB exception table pins
  # to, so there's no existing node_key-keyed liveness signal. Each node upserts its
  # node_key + last_seen_at every reporter tick; the RebalanceJob reconciler unpins overrides
  # whose pinned_node hasn't beaten recently (a dead node), letting the shard re-home.
  def change do
    create table(:rebalancer_nodes, primary_key: false) do
      add :node_key, :string, null: false, primary_key: true
      add :last_seen_at, :utc_datetime_usec, null: false
    end
  end
end
