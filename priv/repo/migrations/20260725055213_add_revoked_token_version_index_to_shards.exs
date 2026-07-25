defmodule Fathom.Repo.Migrations.AddRevokedTokenVersionIndexToShards do
  use Ecto.Migration

  # Expert review 2026-07-24 #5. `Fathom.HranaAuth.Revocations` used to do a Postgres point-read
  # per shard per TTL, inline on the Hrana stream-open path — a rate that scaled with SHARD COUNT
  # rather than with revocation events (~3,300 reads/s at 100k shards/node, each taking a repo
  # connection). It now bulk-loads the revoked set instead, via `Directory.revoked_floors/0`.
  #
  # `token_version` defaults to 1 and is only raised by revoke/rotate and the reconcile sweep, so
  # this partial index holds a tiny fraction of the fleet — a few KB in a healthy deployment —
  # while making the bulk query an index-only scan instead of a full table scan.
  #
  # INCLUDE carries the two columns the query selects, so it never touches the heap. CONCURRENTLY
  # (hence no DDL transaction and no migration advisory lock) so it can be added to a large live
  # table without a write-blocking lock on the control plane.
  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    create index(:shards, [:shard_id],
             where: "token_version > 1",
             include: [:token_version, :token_version_bumped_at],
             name: :shards_revoked_token_version_index,
             concurrently: true
           )
  end
end
