defmodule Fathom.Repo.Migrations.AddRequiresReviewToShardMigrations do
  use Ecto.Migration

  # Expert review #1: a captured version whose buffer contains template-literal DATA migrations
  # (INSERT/UPDATE/DELETE on non-django_migrations tables) is flagged requires_review, which caps
  # the fleet HEAD below it (Migrator.head/0) so the rollout can't replay the dangerous DML
  # fleet-wide until an operator reviews and approves it. Additive, defaults false → existing
  # releases are unaffected.
  def change do
    alter table(:shard_migrations) do
      add :requires_review, :boolean, default: false, null: false
    end
  end
end
