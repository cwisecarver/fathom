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
  """
  use Ecto.Migration

  def change do
    alter table(:shards) do
      add :last_snapshot_attempt_at, :utc_datetime_usec
    end

    create index(:shards, [:last_snapshot_attempt_at],
             where: "status = 'active'",
             name: :shards_last_snapshot_attempt_at_active_index
           )
  end
end
