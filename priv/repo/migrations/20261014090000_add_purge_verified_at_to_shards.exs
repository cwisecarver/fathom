defmodule Fathom.Repo.Migrations.AddPurgeVerifiedAtToShards do
  @moduledoc """
  Perf review 2026-10-14 #15: the stamp that lets `ErasureSweepJob` stop re-LISTing every tombstone
  every hour. A clean storage probe of a `deleted` row stamps `purge_verified_at`; the sweep then
  re-probes only on a decaying schedule measured from the deletion (see `Directory.deleted_page/4`).
  NULL = never verified clean. Control-plane (Postgres) column, not a shard migration.

  The index serves the sweep's keyset walk (`status = 'deleted'`, newest `updated_at` first) from
  the index in order: the existing `shards_deleted_index` is on `shard_id` only, so every page
  sorted the whole window. `purge_verified_at` is INCLUDEd so the due-filter is evaluated without a
  heap fetch per row. Partial (almost every row is not deleted) and built CONCURRENTLY.
  """
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    alter table(:shards) do
      add :purge_verified_at, :utc_datetime_usec
    end

    create index(:shards, [desc: :updated_at, desc: :shard_id],
             include: [:purge_verified_at],
             where: "status = 'deleted'",
             name: :shards_deleted_sweep_index,
             concurrently: true
           )
  end
end
