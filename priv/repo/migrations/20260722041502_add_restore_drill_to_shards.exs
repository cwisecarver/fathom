defmodule Fathom.Repo.Migrations.AddRestoreDrillToShards do
  use Ecto.Migration

  # Expert review #24: the RestoreDrillJob samples least-recently-verified shards and integrity-
  # checks their durable objects, so a bad stored object on a dormant tenant's cold tail is caught by
  # a drill instead of when the tenant returns. `last_verified_at` drives the least-recently-verified
  # sampling weight (ASC NULLS FIRST, so never-verified shards go first); `last_verify_status` makes a
  # failing shard queryable durably (it survives the telemetry window). Both nullable — old rows are
  # simply unverified. Additive, so an in-flight fleet tolerates it (readers ignore the new columns).
  #
  # Deliberately NO index on `last_verified_at`: the drill's sampling query orders by it, but the
  # drill is gated-off + daily + bounded, whereas `resolve`/`record_batch` upsert into `shards` on
  # every checkout — an index would tax that hot path (~15% dir_resolve) for a query most deployments
  # never run. The daily sort is fine at typical scale; an operator running the drill against millions
  # of active shards can add `create index(:shards, [:last_verified_at])` then.
  def change do
    alter table(:shards) do
      add :last_verified_at, :utc_datetime_usec
      add :last_verify_status, :string
    end
  end
end
