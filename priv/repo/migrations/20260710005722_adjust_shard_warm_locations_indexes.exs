defmodule Fathom.Repo.Migrations.AdjustShardWarmLocationsIndexes do
  use Ecto.Migration

  # Review 2026-07-09 #13/#14: no query filters shard_warm_locations by :shard_id — warm_nodes/1
  # and prune/1 both filter :updated_at (grouping by shard in Elixir), and the age-based retract
  # sweep (#14) filters `updated_at < now`. So [:shard_id] is pure write overhead (the exact
  # unused-index mistake #18 fixed for shard_load_samples) while :updated_at — read every reporter
  # and rebalance tick — is a full scan. Drop the former, index the latter. The unique
  # [:node_key, :shard_id] correctly serves the upsert + node-scoped deletes and is untouched.
  def change do
    drop index(:shard_warm_locations, [:shard_id])
    create index(:shard_warm_locations, [:updated_at])
  end
end
