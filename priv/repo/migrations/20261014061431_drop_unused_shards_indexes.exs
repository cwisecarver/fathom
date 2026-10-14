defmodule Fathom.Repo.Migrations.DropUnusedShardsIndexes do
  @moduledoc """
  Perf review 2026-10-14 #5: two `shards` indexes with no reader, on the table's hottest write path.

  Every Recorder touch and flush write is a NON-HOT update (`last_active_at` / `updated_at` are
  indexed), so each unread index is one wasted index entry per write, plus bloat and autovacuum.
  Measured on 1M rows: dropping both took the touch write from 727 to 590 B WAL/row (-19%), 299 to
  270 ms per 20k rows, and freed 37 MB of index per 1M rows.

    * `shards_active_last_active_at_index` (20260702011500). Its only reader, `active_recent/1`,
      was deleted with the WarmFollower 2026-09-14. `laggards` uses the composite
      `shards_active_schema_version_last_active_at_index` (it filters on `schema_version`), and
      `flush_lag_report`'s predicate (`status != 'deleted'`) does not imply the partial
      `status = 'active'` predicate, so it cannot use this index either.
    * `shards_last_snapshot_at_active_index` (20260805044751). Superseded by
      `shards_snapshot_rotation_active_index` (20261010050845) and
      `shards_retention_rotation_active_index` (20261014024437), which match the two sweeps'
      ORDER BYs exactly.

  `test/fathom/directory_test.exs` pins, by EXPLAIN, that every remaining reader keeps its index.

  CONCURRENTLY: no write-blocking lock on a large live table (so no DDL transaction and no
  migration advisory lock). The id -> shard_id primary key switch is a separate, parked change.
  """
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    execute("DROP INDEX CONCURRENTLY IF EXISTS shards_active_last_active_at_index")
    execute("DROP INDEX CONCURRENTLY IF EXISTS shards_last_snapshot_at_active_index")
  end

  # Recreates exactly what the original migrations created, concurrently (a rollback on a live
  # fleet must not block writes either).
  def down do
    create index(:shards, [:last_active_at],
             where: "status = 'active'",
             name: :shards_active_last_active_at_index,
             concurrently: true
           )

    create index(:shards, [:last_snapshot_at],
             where: "status = 'active'",
             name: :shards_last_snapshot_at_active_index,
             concurrently: true
           )
  end
end
