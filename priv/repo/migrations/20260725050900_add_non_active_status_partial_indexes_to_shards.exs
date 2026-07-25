defmodule Fathom.Repo.Migrations.AddNonActiveStatusPartialIndexesToShards do
  use Ecto.Migration

  # Expert review 2026-07-24 #12. 20260702011500 added partial indexes for `status = 'active'`,
  # but Postgres's `predicate_implied_by` cannot derive `status = 'active'` from
  # `status = 'deleted'`, so EVERY non-active status predicate still sequentially scans the whole
  # `shards` table — millions of rows at target scale, and this table is updated on every
  # Directory.Recorder flush so its real bloat is typically 2–4× its logical size.
  #
  # The callers are not occasional. `Fathom.Tenants.Tombstones` and `Fathom.Tenants.Suspensions`
  # each refresh every 5 minutes ON EVERY NODE, so a 20-node fleet ran 8 full-table scans per
  # minute; the admin overview adds two more every 5 s per connected viewer. That volume of
  # scanning evicts the Recorder's hot index pages from shared_buffers, which is how it surfaces
  # as a `dir_resolve_p50_us` regression with no code change behind it.
  #
  # Three EQUALITY-predicate partial indexes rather than one `status <> 'active'` index:
  # equality-to-equality implication is unambiguous in the planner's predtest, and three indexes
  # each holding only the rows in that state are a few KB in a healthy fleet — collectively far
  # less write maintenance on the hot path than one index covering every non-active row.
  #
  # CONCURRENTLY (hence no DDL transaction and no migration advisory lock) so adding these to a
  # large live table never takes a write-blocking lock on the control plane during a deploy.
  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    # Directory.deleted_shard_ids/0 — Tombstones boot load + 5-min refresh, per node. The re-mint
    # guard depends on this completing; a scan slow enough to trip its retry/backoff leaves the
    # 410 admission gate stale.
    create index(:shards, [:shard_id],
             where: "status = 'deleted'",
             name: :shards_deleted_index,
             concurrently: true
           )

    # Directory.suspended_shard_ids/0 — Suspensions boot load + 5-min refresh, per node. Same
    # shape, backing the 403 gate.
    create index(:shards, [:shard_id],
             where: "status = 'suspended'",
             name: :shards_suspended_index,
             concurrently: true
           )

    # Directory.count_failed/0 and failed_shards/0 — the admin overview's failure tile (every 5 s
    # per connected viewer) and the operator's triage list.
    create index(:shards, [:shard_id],
             where: "status = 'migration_failed'",
             name: :shards_migration_failed_index,
             concurrently: true
           )
  end
end
