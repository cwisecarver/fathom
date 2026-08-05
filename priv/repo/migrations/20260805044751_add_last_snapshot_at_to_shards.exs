defmodule Fathom.Repo.Migrations.AddLastSnapshotAtToShards do
  @moduledoc """
  Expert review 2026-08-01 #18: the scheduled-snapshot rotation weight.

  Without this column, "which shards need a snapshot?" can only be answered with a `list_snapshots`
  call **per shard** — an S3 LIST per tenant per run, which at a million shards costs more than the
  snapshots themselves. With it the question is one indexed Postgres query, the same shape
  `sample_for_drill/1` already uses for the restore drill.
  """
  use Ecto.Migration

  def change do
    alter table(:shards) do
      add :last_snapshot_at, :utc_datetime_usec
    end

    # The scheduler orders by "least-recently-snapshotted first", NULLs first — a shard that has
    # never been snapshotted is the highest priority. Partial on `active` because the scheduler
    # never considers any other status.
    create index(:shards, [:last_snapshot_at],
             where: "status = 'active'",
             name: :shards_last_snapshot_at_active_index
           )
  end
end
