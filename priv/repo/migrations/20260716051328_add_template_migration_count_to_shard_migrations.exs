defmodule Fathom.Repo.Migrations.AddTemplateMigrationCountToShardMigrations do
  use Ecto.Migration

  # Expert review #32: record the template's django_migrations count at each captured version, so a
  # post-revert consistency check can tell whether the template's linear migration graph was left
  # AHEAD of the (yanked-away) fleet — the drift that wedges the next `makemigrations`. Nullable and
  # additive: pre-existing releases keep NULL (the drift check skips them, documented).
  def change do
    alter table(:shard_migrations) do
      add :template_migration_count, :integer
    end
  end
end
