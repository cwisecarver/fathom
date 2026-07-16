defmodule Fathom.Repo.Migrations.AddTokenVersionBumpedAtToShards do
  @moduledoc """
  Zero-downtime Hrana-token rotation (expert review 2026-07-14 #24). Records WHEN a shard's
  `token_version` was last raised so `Fathom.HranaAuth` can honor a two-generation grace window
  after a `rotate/1` (accept the previous version for a bit) while a `revoke/1` (which clears this
  to NULL) stays immediate.

  Nullable + additive, so it's backward-compatible: old code never reads it, and a NULL means
  "no grace" (the safe default — the previous version is not accepted).
  """
  use Ecto.Migration

  def change do
    alter table(:shards) do
      add :token_version_bumped_at, :utc_datetime_usec
    end
  end
end
