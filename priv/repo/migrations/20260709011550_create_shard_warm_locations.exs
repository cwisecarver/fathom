defmodule Fathom.Repo.Migrations.CreateShardWarmLocations do
  use Ecto.Migration

  # Affinity-aware handoff target (Phase 2 C, folded into B1). Each node advertises which
  # fleet-HOT shards it has warm-cached (`Fathom.Shard.WarmFollower`) but doesn't own, so the
  # rebalancer can prefer a handoff target that already holds the shard warm (a cheap 304 at
  # handoff instead of a full S3 body pull). Bounded to hot shards (the only handoff
  # candidates), not the whole warm set, and refreshed/pruned each reporter window like
  # `shard_load_samples`. Per-node because warm state is node-local (no BEAM cluster).
  def change do
    create table(:shard_warm_locations, primary_key: false) do
      add :node_key, :string, null: false
      add :shard_id, :string, null: false
      add :updated_at, :utc_datetime_usec, null: false
    end

    # Upsert target (one row per node/shard) + the policy's read (warm nodes for a shard).
    create unique_index(:shard_warm_locations, [:node_key, :shard_id])
    create index(:shard_warm_locations, [:shard_id])
  end
end
