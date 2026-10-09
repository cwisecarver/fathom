defmodule Fathom.Repo.Migrations.AddLastRetentionAtToShards do
  @moduledoc """
  Expert review 2026-10-08 #19: the snapshot-retention sweep's own rotation key.

  `RetentionJob` swept the N active shards with the OLDEST `last_snapshot_at` and recorded nothing
  per sweep. That stamp moves only when a shard is snapshotted again, so the head of the order was
  permanently the coldest, once-snapshotted shards — which have nothing to expire — and every run
  re-swept the same N. The shards actually accumulating hourly `-auto` snapshots are the hot ones,
  stamped most recently, so they sorted last and, once more than N shards had snapshots, were never
  reached. Storage grew without bound while the job reported `dropped: 0`.

  The sweep now stamps this column on every shard it sweeps and orders by it, NULLs first, so the
  rotation visits every snapshotted shard once per ceil(count / N) runs.
  """
  use Ecto.Migration

  def change do
    alter table(:shards) do
      add :last_retention_at, :utc_datetime_usec
    end

    # Partial on `active`, like the snapshot rotation's index: the sweep never considers another
    # status.
    create index(:shards, [:last_retention_at],
             where: "status = 'active'",
             name: :shards_last_retention_at_active_index
           )
  end
end
