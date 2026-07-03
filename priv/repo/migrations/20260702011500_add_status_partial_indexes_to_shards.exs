defmodule Fathom.Repo.Migrations.AddStatusPartialIndexesToShards do
  use Ecto.Migration

  # `status` was in no index, so every `status = 'active'`-filtered query sequentially
  # scanned the whole `shards` table — linear in fleet size (millions at target scale).
  # Build the indexes CONCURRENTLY so adding them to a large live table doesn't take a
  # write-blocking lock on the control plane during a deploy (requires no surrounding
  # transaction and no migration advisory lock).
  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    # active_recent/1 (Fathom.Shard.WarmFollower, every :warm_poll_ms per node):
    #   status = 'active' AND last_active_at IS NOT NULL ORDER BY last_active_at DESC LIMIT n.
    # A partial index on last_active_at (scanned backwards for the DESC + LIMIT) turns that
    # per-poll full scan into a bounded index read. Must be in place before :warm_follower
    # ships on by default.
    create index(:shards, [:last_active_at],
             where: "status = 'active'",
             name: :shards_active_last_active_at_index,
             concurrently: true
           )

    # laggards/count_laggards (hourly reconcile + every rollout sweep) and shards_at_version
    # (fleet revert): schema_version (range or equality) AND status = 'active', with laggards
    # also ORDER BY last_active_at DESC. A partial variant of the existing
    # (schema_version, last_active_at) composite, scoped to active rows, so these stop
    # near-full-table sorting the retired/migrating rows they always discard.
    create index(:shards, [:schema_version, :last_active_at],
             where: "status = 'active'",
             name: :shards_active_schema_version_last_active_at_index,
             concurrently: true
           )
  end
end
