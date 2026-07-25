defmodule Fathom.Repo.Migrations.AddDeletedUpdatedAtIndexToShards do
  use Ecto.Migration

  # Expert review 2026-07-24 #30. `Fathom.Tenants.Tombstones` refreshed by loading EVERY id ever
  # deleted, on every node, every 5 minutes — so both the Postgres read and the receiving process's
  # heap scaled with cumulative lifetime deletions rather than with anything current. At a million
  # lifetime deletions that is a full-table read plus a million-element list materialized per node
  # per refresh.
  #
  # The refresh is now incremental (`Directory.deleted_shard_ids_since/1`), and this is the index
  # that makes it an index read rather than a scan of every tombstoned row. Partial on the deleted
  # status so it holds only tombstones, ordered by `updated_at` so the "changed since" predicate is
  # a range scan; INCLUDE carries the id so it never touches the heap.
  #
  # CONCURRENTLY, like the other `shards` indexes — this table is written on every Recorder flush.
  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    create index(:shards, [:updated_at],
             where: "status = 'deleted'",
             include: [:shard_id],
             name: :shards_deleted_updated_at_index,
             concurrently: true
           )
  end
end
