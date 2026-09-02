defmodule Fathom.Repo.Migrations.AddBulkShardUniqueIndexToObanJobs do
  use Ecto.Migration

  # Expert review 2026-08-31 #26. `Fathom.BulkEnqueue.unique/1` dedups per (worker, shard_id) with a
  # SELECT-then-`Oban.insert_all`, which is NOT atomic and had no DB constraint behind it — two
  # concurrent callers (the hourly ReconcileJob rollout racing an :async migrate-on-touch enqueue, or
  # revert/3 racing revert_stranded/0) could each see a shard un-queued and both insert a per-shard
  # job. The S3 lease makes the second a harmless no-op, but the "unique" guarantee was only
  # best-effort. This partial UNIQUE index is the race backstop: `Oban.insert_all` merges
  # `on_conflict: :nothing` (see Oban.Engines.Basic.insert_all_jobs/3), so a duplicate is silently
  # skipped by the DB and the returned count stays accurate.
  #
  # SCOPED to exactly the three workers BulkEnqueue is used with — each is genuinely (worker,
  # shard_id)-unique (one in-flight migration / revert / revoke per shard). It must NOT be global:
  # RetirementJob shares (worker, shard_id) across DIFFERENT retained VERSIONS of one shard, and a
  # global unique index would wrongly block retiring a shard's second version. A NEW BulkEnqueue
  # caller whose uniqueness key is (worker, shard_id) must be ADDED to this predicate; one keyed by
  # anything else must NOT be. The existing non-unique
  # `oban_jobs_worker_shard_id_live_index` still serves the dedup SELECT (and RetirementJob's own
  # revert_in_flight? scan) for ALL workers.
  #
  # CONCURRENTLY so it can be added to a busy oban_jobs table without blocking job processing.
  @disable_ddl_transaction true
  @disable_migration_lock true

  @states "'scheduled','available','executing','retryable','suspended'"
  @workers "'Fathom.Migrator.ShardMigrationJob','Fathom.Migrator.RevertJob','Fathom.HranaAuth.RevokeJob'"

  def up do
    execute("""
    CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS oban_jobs_bulk_shard_unique_index
      ON oban_jobs (worker, (args->>'shard_id'))
      WHERE state IN (#{@states}) AND worker IN (#{@workers})
    """)
  end

  def down do
    execute("DROP INDEX CONCURRENTLY IF EXISTS oban_jobs_bulk_shard_unique_index")
  end
end
