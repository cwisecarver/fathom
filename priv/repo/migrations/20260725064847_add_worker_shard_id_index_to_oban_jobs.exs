defmodule Fathom.Repo.Migrations.AddWorkerShardIdIndexToObanJobs do
  use Ecto.Migration

  # Expert review 2026-07-24 #20. Four hand-written queries filter Oban jobs by the shard id inside
  # `args`, and NONE of them could use an index:
  #
  #   * Migrator.enqueue_unique_chunk/1  — once per 5,000-shard chunk of every rollout/fleet revert
  #   * Migrator.RetirementJob.revert_in_flight?/2 — once per RetirementJob perform, i.e. once per
  #     shard per cutover
  #   * Rebalancer.RebalanceJob.handoff_in_flight?/1 — per dead-pin candidate, every rebalance tick
  #   * Tenants.cancel_pending_jobs/1 — per tenant delete
  #
  # Oban's own index on `args` is GIN/jsonb_ops, which supports containment (@>, ?, ?|) but NOT the
  # `->>` text-extraction operator these use. (Oban's internal uniqueness check uses `args @> $1`,
  # which is why IT is fast and these are not.) There is no index on `worker` either, so the planner
  # fell back to scanning by `state` and then filtering every row with a JSONB parse.
  #
  # During a fleet rollout the live job set is hundreds of thousands to millions of rows, so a
  # 1M-shard rollout meant 200 chunk-dedup queries each scanning every live job — and then 1M
  # RetirementJobs each doing the same scan a week later.
  #
  # `(args->>'shard_id')` is immutable (jsonb ->> text), so it is index-legal. The partial predicate
  # keeps the index the size of the LIVE job set rather than the Pruner's 7-day retention. The state
  # list is the union of Migrator's @unique_states, RebalanceJob's @handoff_live_states, and the
  # tighter list in Tenants — all subsets, so all four queries are served.
  #
  # CONCURRENTLY so it can be added to a busy oban_jobs table without blocking job processing.
  @disable_ddl_transaction true
  @disable_migration_lock true

  @states "'scheduled','available','executing','retryable','suspended'"

  def up do
    execute("""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS oban_jobs_worker_shard_id_live_index
      ON oban_jobs (worker, (args->>'shard_id'))
      WHERE state IN (#{@states})
    """)
  end

  def down do
    execute("DROP INDEX CONCURRENTLY IF EXISTS oban_jobs_worker_shard_id_live_index")
  end
end
