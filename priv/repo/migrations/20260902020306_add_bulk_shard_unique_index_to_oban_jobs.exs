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

  # DROP-FIRST, and NO `IF NOT EXISTS` on the CREATE (review 2026-08-31 self-review #1). A
  # `CREATE UNIQUE INDEX CONCURRENTLY` that fails to build — e.g. two live rows already share a
  # (worker, shard_id), the exact race this index exists to close — leaves an INVALID index behind
  # AND does not record the migration version, so `mix ecto.migrate` re-runs it. With `IF NOT EXISTS`
  # the re-run sees the invalid leftover, skips silently, and records success: the index now EXISTS
  # but enforces nothing, and nobody is told. So: drop any leftover (a no-op on a clean DB, and it
  # clears an invalid one from a prior failed attempt), then create without `IF NOT EXISTS` so a
  # genuine build failure RAISES and fails the migration loudly. On the retry after the operator
  # clears the duplicate rows, the drop removes the invalid index and the create rebuilds cleanly.
  #
  # Deliberately does NOT auto-cancel the duplicate live jobs: mutating rows in oban_jobs from a
  # migration is riskier than surfacing the collision, and a duplicate is a harmless no-op at
  # runtime (the S3 lease makes the second job idle). No unit test — `CONCURRENTLY` cannot run inside
  # the test sandbox's per-test transaction; `bulk_enqueue_test.exs` proves the index enforces
  # uniqueness once built.
  def up do
    execute("DROP INDEX CONCURRENTLY IF EXISTS oban_jobs_bulk_shard_unique_index")

    execute("""
    CREATE UNIQUE INDEX CONCURRENTLY oban_jobs_bulk_shard_unique_index
      ON oban_jobs (worker, (args->>'shard_id'))
      WHERE state IN (#{@states}) AND worker IN (#{@workers})
    """)
  end

  def down do
    execute("DROP INDEX CONCURRENTLY IF EXISTS oban_jobs_bulk_shard_unique_index")
  end
end
