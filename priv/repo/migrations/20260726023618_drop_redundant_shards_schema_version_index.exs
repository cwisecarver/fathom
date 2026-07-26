defmodule Fathom.Repo.Migrations.DropRedundantShardsSchemaVersionIndex do
  use Ecto.Migration

  # Expert review 2026-07-24 #35. `20260628001559` created a NON-partial
  # (schema_version, last_active_at) index. `20260702011500` then added
  # `shards_active_schema_version_last_active_at_index` — same columns, `WHERE status =
  # 'active'` — which supersedes it, because every schema_version predicate in lib/ also
  # filters status = 'active' (verified: laggards, count_laggards, shards_at_version and the
  # keyset sweep in directory.ex — four predicates, all four scoped to active). So the
  # non-partial index serves no read and is pure write overhead.
  #
  # It sits on the worst table in the system for that. `Directory.Recorder.record_batch/1`
  # updates `last_active_at` on every flush cycle, and `last_active_at` appears in three of
  # the five indexes, so HOT updates are already impossible — every flush writes a new heap
  # tuple plus an entry in EVERY index. Removing one of five is ~20% fewer index-entry writes
  # and ~20% less index bloat and autovacuum load on the directory's hottest write path. Same
  # mistake `20260708012318` fixed for shard_load_samples and `20260710005722` for
  # shard_warm_locations.
  #
  # CONCURRENTLY so dropping it on a large live table takes no write-blocking lock on the
  # control plane during a deploy (hence no surrounding transaction and no advisory lock).
  @disable_ddl_transaction true
  @disable_migration_lock true

  # Before running this against a real fleet, confirm disuse rather than trusting the analysis:
  #   SELECT indexrelname, idx_scan FROM pg_stat_user_indexes WHERE relname = 'shards';
  # idx_scan should be 0 (or flat since the partial index shipped) for
  # shards_schema_version_last_active_at_index.
  def up do
    execute("DROP INDEX CONCURRENTLY IF EXISTS shards_schema_version_last_active_at_index")
  end

  # Reversible: recreate exactly what 20260628001559 created, so a rollback restores the
  # original shape. Concurrently here too — a rollback on a live fleet must not block writes.
  def down do
    create index(:shards, [:schema_version, :last_active_at], concurrently: true)
  end
end
