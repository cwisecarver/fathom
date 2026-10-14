defmodule Fathom.Repo.Migrations.AddTokenVersionChangedAtToShards do
  @moduledoc """
  Expert review 2026-10-13 #10: an incremental cursor for the Revocations bulk refresh.

  `Directory.revoked_floors/0` read EVERY shard whose `token_version` had ever been raised, on every
  node, every 30 s, so after a fleet-wide `revoke_issued_before` it was O(fleet x nodes). The panel
  suggested filtering on `token_version_bumped_at`, but that is the WRONG column: a hard revoke
  stamps it `nil` (grace off), so a revoke would be invisible to the incremental read. This column
  is stamped (on the Postgres clock) by EVERY path that raises `token_version`, revoke, rotate and
  the DR reconcile alike.

  Existing rows stay NULL: the Revocations process does a full keyset-paged reload at boot and
  daily, which covers every row raised before this migration. The index is partial on
  `token_version > 1`, so it holds only ever-raised shards, and built CONCURRENTLY (no DDL
  transaction, no migration lock) so it cannot block writes on the hottest table.
  """
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    alter table(:shards) do
      add :token_version_changed_at, :utc_datetime_usec
    end

    create index(:shards, [:token_version_changed_at],
             where: "token_version > 1",
             name: :shards_token_version_changed_at_index,
             concurrently: true
           )
  end
end
