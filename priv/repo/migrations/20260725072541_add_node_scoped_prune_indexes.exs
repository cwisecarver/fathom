defmodule Fathom.Repo.Migrations.AddNodeScopedPruneIndexes do
  use Ecto.Migration

  # Expert review 2026-07-24 #27. Both rebalancer observation tables were pruned FLEET-WIDE from
  # every node every reporting window (default 10s), so on an N-node fleet each window issued N
  # deletes of the same expired rows: one useful, N-1 acquiring row locks, blocking, re-checking and
  # deleting zero — each after a full index range scan. The waste grew linearly with node count,
  # which is the opposite of the horizontal additivity the density work established.
  #
  # The prunes are now node-scoped (disjoint per node ⇒ no cross-node contention) with a rare
  # fleet-wide sweep to reclaim a departed node's rows. These indexes are what make the node-scoped
  # predicate an index read rather than a scan-and-filter.
  #
  # `shard_warm_locations (node_key, updated_at)` also fixes a second, pre-existing mismatch:
  # `WarmLocations.publish/2`'s retract sweep filters `node_key == ... AND updated_at < now`, which
  # matches essentially every row of that node — so the `[:updated_at]`-only index was a poor fit
  # and the planner fell back to the unique index prefix plus a filter.
  #
  # CONCURRENTLY: these are high-churn insert/delete tables, and the rebalancer reports on a 10s
  # cadence, so a blocking build would stall reporting across the fleet.
  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    create index(:shard_load_samples, [:node_key, :sampled_at],
             name: :shard_load_samples_node_key_sampled_at_index,
             concurrently: true
           )

    create index(:shard_warm_locations, [:node_key, :updated_at],
             name: :shard_warm_locations_node_key_updated_at_index,
             concurrently: true
           )
  end
end
