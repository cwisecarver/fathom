defmodule Fathom.Repo.Migrations.AddPromotionSnapshotAtToShards do
  @moduledoc """
  Expert review 2026-10-12 #8: the stamp that lets the always-on ReconcileJob expire `-promoexp`
  snapshots.

  `PromoteOnOpen` takes a full-copy pre-promotion snapshot, and the 14-day expiry (9b55f62) ran only
  inside `RetentionJob`, which is off by default and samples only active rows that a scheduler has
  snapshotted. So in the default configuration every promotion still leaked a copy forever.

  `promotion_snapshot_at` is stamped (asynchronously, off the open path) when such a snapshot is
  taken. ReconcileJob sweeps rows whose stamp is older than the expiry, any status, and clears it.
  NULL = no pending promotion snapshot. This is a control-plane (Postgres) column, not a shard
  migration.

  The index is PARTIAL on `IS NOT NULL` (almost every row is NULL) and built CONCURRENTLY, like the
  other directory indexes.
  """
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    alter table(:shards) do
      add :promotion_snapshot_at, :utc_datetime_usec
    end

    create index(:shards, [:promotion_snapshot_at],
             where: "promotion_snapshot_at IS NOT NULL",
             name: :shards_promotion_snapshot_at_index,
             concurrently: true
           )
  end
end
