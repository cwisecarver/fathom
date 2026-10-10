defmodule Fathom.Repo.Migrations.AddLastSnapshotAttemptAtToShards do
  @moduledoc """
  Expert review 2026-10-10 #14: the scheduled-snapshot rotation's own attempt stamp.

  `sample_for_snapshot/1` ordered by `last_snapshot_at NULLS FIRST`, and that column is stamped
  only on SUCCESS. A shard whose snapshot fails every time (a corrupt object, a permanent store
  error) therefore never leaves the head of the order and holds one of the per-run slots forever;
  with `:snapshot_schedule_sample` poison shards the scheduler snapshots nothing. Retention got the
  same fix on 2026-10-08 via `last_retention_at`.

  This column is stamped on every ATTEMPT (before the copy, so a timeout-killed task counts too) and
  the rotation orders by it, NULLs first: a failing shard is retried once per cycle, not every run.

  Fix-review R3-4: the index is a COMPOSITE that matches `sample_for_snapshot/1`'s
  `ORDER BY last_snapshot_attempt_at ASC NULLS FIRST, last_snapshot_at ASC NULLS FIRST` exactly (a
  single-column ASC NULLS LAST index cannot serve a NULLS FIRST order), and is built CONCURRENTLY —
  hence no DDL transaction and no migration lock — so it does not block writes on a large live
  directory (same convention as `20260725055213_*`).

  Expert review 2026-10-10 #P2: the index is also PARTIAL on the query's whole "needs a snapshot"
  predicate. The two-column `last_flushed_at > last_snapshot_at` test cannot be an index condition,
  so with only `status = 'active'` in the predicate the planner walked the ordered index and
  filtered; dormant clean shards (attempted, nothing changed since) stayed at the head of the walk
  forever. With the predicate in the index they are simply not in it. `sample_for_snapshot/1`'s
  WHERE must match this predicate textually-equivalently or the planner will not use the index.
  """
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    alter table(:shards) do
      add :last_snapshot_attempt_at, :utc_datetime_usec
    end

    create index(
             :shards,
             ["last_snapshot_attempt_at ASC NULLS FIRST", "last_snapshot_at ASC NULLS FIRST"],
             where:
               "status = 'active' AND last_flushed_at IS NOT NULL AND " <>
                 "(last_snapshot_at IS NULL OR last_flushed_at > last_snapshot_at)",
             name: :shards_snapshot_rotation_active_index,
             concurrently: true
           )
  end
end
