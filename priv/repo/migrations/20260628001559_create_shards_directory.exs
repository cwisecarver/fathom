defmodule Fathom.Repo.Migrations.CreateShardsDirectory do
  use Ecto.Migration

  def change do
    create table(:shards) do
      add :shard_id, :string, null: false
      # The schema version this shard's data currently sits at (0 = brand-new,
      # never migrated). HEAD is tracked elsewhere; a shard with version < HEAD
      # is a laggard the rollout will migrate.
      add :schema_version, :integer, null: false, default: 0
      # active | migrating | retired | migration_failed
      add :status, :string, null: false, default: "active"
      add :last_active_at, :utc_datetime_usec, null: false
      # When set (retired shards): the cutoff after which the old shard may be
      # dropped and revert is no longer possible.
      add :retain_until, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:shards, [:shard_id])
    # Drives the rollout sweep: laggards ordered by recency of use.
    create index(:shards, [:schema_version, :last_active_at])
  end
end
