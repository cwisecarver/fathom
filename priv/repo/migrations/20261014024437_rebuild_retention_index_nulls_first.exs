defmodule Fathom.Repo.Migrations.RebuildRetentionIndexNullsFirst do
  @moduledoc """
  Expert review 2026-10-13 #27: `sample_for_retention/1` could not use its index.

  The query orders by `last_retention_at ASC NULLS FIRST, last_snapshot_at ASC`, but
  `shards_last_retention_at_active_index` (20261009044407, already applied, never edited) is a single
  column `ASC NULLS LAST`, so Postgres sorted every active, snapshotted row each run. It was also
  created without CONCURRENTLY on the hottest write table (the defect 20261010050845 fixed for the
  snapshot rotation).

  The replacement is the composite matching the ORDER BY exactly, partial on the query's whole
  WHERE (`sample_for_retention/1` must match that predicate textually-equivalently or the planner
  will not use it), built CONCURRENTLY. The old index is dropped CONCURRENTLY after the new one
  exists, so there is never a window without one.
  """
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    create index(
             :shards,
             ["last_retention_at ASC NULLS FIRST", "last_snapshot_at ASC"],
             where: "status = 'active' AND last_snapshot_at IS NOT NULL",
             name: :shards_retention_rotation_active_index,
             concurrently: true
           )

    drop_if_exists index(:shards, [:last_retention_at],
                     name: :shards_last_retention_at_active_index,
                     concurrently: true
                   )
  end

  def down do
    create index(:shards, [:last_retention_at],
             where: "status = 'active'",
             name: :shards_last_retention_at_active_index,
             concurrently: true
           )

    drop_if_exists index(:shards, [],
                     name: :shards_retention_rotation_active_index,
                     concurrently: true
                   )
  end
end
