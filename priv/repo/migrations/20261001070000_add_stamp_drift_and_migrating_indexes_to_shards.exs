defmodule Fathom.Repo.Migrations.AddStampDriftAndMigratingIndexesToShards do
  use Ecto.Migration

  # Expert review 2026-10-01 perf #29. Three control-plane predicates over `shards` had no index,
  # so each was a sequential scan of the hottest control-plane table:
  #
  #   * `count_stamp_drift/0` (`active AND last_verify_status IN (…)`) and
  #   * `stamp_drift_checked/0` (`active AND last_verified_at IS NOT NULL`), both on every call of
  #     `Migrator.status/0` — the deploy gate a CI job polls until convergence;
  #   * `reclaim_stale_migrating/1` (`status = 'migrating' AND migrating_since < cutoff`), hourly.
  #
  # All three are PARTIAL, so each holds only the rows its predicate selects: almost none for the
  # drift statuses and `migrating`, and only restore-drilled rows for `last_verified_at`. That keeps
  # them nearly free to maintain under the Recorder's per-second upserts.
  #
  # CONCURRENTLY: `shards` is written every second by the Recorder on every node, so a blocking
  # build would stall that for the length of the build.
  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    create index(:shards, [:shard_id],
             name: :shards_active_stamp_drift_index,
             where:
               "status = 'active' AND last_verify_status IN ('schema_mismatch', 'ledger_mismatch')",
             concurrently: true
           )

    create index(:shards, [:last_verified_at],
             name: :shards_active_verified_index,
             where: "status = 'active' AND last_verified_at IS NOT NULL",
             concurrently: true
           )

    create index(:shards, [:migrating_since],
             name: :shards_migrating_since_index,
             where: "status = 'migrating'",
             concurrently: true
           )
  end
end
