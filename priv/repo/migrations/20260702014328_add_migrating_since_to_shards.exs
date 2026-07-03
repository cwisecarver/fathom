defmodule Fathom.Repo.Migrations.AddMigratingSinceToShards do
  use Ecto.Migration

  # When a shard entered `migrating`, so the reconcile sweep can time out a migration
  # whose Oban job was lost (finding #20). A plain timestamp column (nullable, no default),
  # so adding it is metadata-only — no table rewrite. It is *not* updated_at: a migrating
  # shard still receiving traffic bumps updated_at via the directory recorder, which would
  # reset the staleness clock; migrating_since is only ever touched by the migration
  # lifecycle, so it is a reliable "migrating since" signal.
  def up do
    alter table(:shards) do
      add :migrating_since, :utc_datetime_usec
    end

    # Backfill any shard already mid-migration at deploy time so the reclaim can time it
    # out (updated_at is the best available estimate of when it entered `migrating`).
    execute(
      "UPDATE shards SET migrating_since = updated_at WHERE status = 'migrating' AND migrating_since IS NULL"
    )
  end

  def down do
    alter table(:shards) do
      remove :migrating_since
    end
  end
end
