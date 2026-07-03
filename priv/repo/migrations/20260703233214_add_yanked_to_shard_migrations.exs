defmodule Fathom.Repo.Migrations.AddYankedToShardMigrations do
  use Ecto.Migration

  # Expert review #12: a revert flipped shard pointers but left the Release row, so
  # `max(version)` (the fleet HEAD) never dropped — every reverted shard was
  # immediately a laggard and the hourly reconcile re-applied the reverted-from
  # version. A yanked release is excluded from HEAD and can never be applied again.
  def change do
    alter table(:shard_migrations) do
      add :yanked, :boolean, default: false, null: false
    end
  end
end
