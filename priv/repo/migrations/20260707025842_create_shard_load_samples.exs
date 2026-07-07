defmodule Fathom.Repo.Migrations.CreateShardLoadSamples do
  use Ecto.Migration

  # Per-node ShardLoad rate samples, published off the hot path by
  # Fathom.Rebalancer.Reporter (one row per (owner, shard) per reporter window).
  # The rebalancer reads a short history of these to find persistently-hot shards.
  # `owner` is the reporting node's identity (Fathom.Shard.Heartbeat.owner/0), which
  # is also the shard's current serving node — Postgres has no other node column.
  def change do
    create table(:shard_load_samples) do
      add :owner, :string, null: false
      add :shard_id, :string, null: false
      add :q_per_s, :float, null: false, default: 0.0
      add :rows_read_per_s, :float, null: false, default: 0.0
      add :checkouts_per_s, :float, null: false, default: 0.0
      add :window_s, :float, null: false, default: 0.0
      add :sampled_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    # The rebalancer reads recent samples per shard (anti-flap streak + current rate)
    # and prunes old ones by time.
    create index(:shard_load_samples, [:shard_id, :sampled_at])
    create index(:shard_load_samples, [:sampled_at])
  end
end
