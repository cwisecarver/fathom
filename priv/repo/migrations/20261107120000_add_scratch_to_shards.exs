defmodule Fathom.Repo.Migrations.AddScratchToShards do
  @moduledoc """
  Expert review 2026-11-07 #15: PROVENANCE for the restore drill's scratch forks.

  `ErasureSweepJob.sweep_scratch_forks/0` purged (objects, snapshots, and the directory row) every
  row older than 24 h whose id matched `^restoredrill[0-9]+$`, and the rollout/drill sweeps excluded
  the same shape. The shape is only a RESERVATION, enforced at `Tenants.provision/2` and `fork/3` —
  a stray request or the token mint can still birth a row with it — so a live tenant with such an id
  was destroyed a day after it was created. The drill's own fork now writes `scratch = true` at birth
  and every scratch-aware query keys on that column instead of the name.

  NOT NULL DEFAULT false: a constant default is metadata-only on Postgres 11+ (no table rewrite), and
  a node on the previous release inserts rows without the column, which read false.

  NO BACKFILL, on purpose. A leaked drill row that predates this migration is indistinguishable from
  an organic tenant that has the same shape, and marking the latter destroys a live tenant — the bug
  being fixed. Such a row (finite, and only from a drill killed before this release) is a normal row
  from here on; remove one by hand with `Fathom.Directory.hard_delete/1` plus
  `Fathom.Shard.Storage.purge_shard/1` if one is found.

  The partial index serves the sweep's `scratch AND status <> 'deleted' AND inserted_at < cutoff`
  from the handful of marked rows instead of a regex scan of the whole table. Built CONCURRENTLY (no
  DDL transaction, no migration lock) so it cannot block writes on the hottest table.
  """
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    alter table(:shards) do
      add :scratch, :boolean, null: false, default: false
    end

    create index(:shards, [:inserted_at],
             where: "scratch",
             name: :shards_scratch_inserted_at_index,
             concurrently: true
           )
  end
end
