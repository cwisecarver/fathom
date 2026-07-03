defmodule Fathom.Repo.Migrations.AddStatementsToShardMigrations do
  use Ecto.Migration

  def change do
    # The captured SQL for a version (the DDL + django_migrations bookkeeping
    # Django emitted), replayed verbatim per shard. One element per statement,
    # in order.
    alter table(:shard_migrations) do
      add :statements, {:array, :text}, null: false, default: []
    end
  end
end
