defmodule Fathom.Repo.Migrations.CreateShardMigrations do
  use Ecto.Migration

  def change do
    # The registry of released shard-schema versions. The transform for each
    # version is code (a migration module keyed by version); this table records
    # which versions exist and, by its max, the fleet's HEAD.
    create table(:shard_migrations) do
      add :version, :integer, null: false
      add :name, :string, null: false

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    create unique_index(:shard_migrations, [:version])
  end
end
