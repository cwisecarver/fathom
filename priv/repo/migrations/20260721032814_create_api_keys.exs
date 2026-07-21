defmodule Fathom.Repo.Migrations.CreateApiKeys do
  use Ecto.Migration

  # Scoped, revocable, per-identity control-plane API keys (expert review #8). Replaces the single
  # shared BasicAuth secret for /api: each key has an actor `name`, a `scope`, and is stored as a
  # SHA-256 hash (never plaintext). Revoke by stamping `revoked_at` — no restart.
  def change do
    create table(:api_keys) do
      add :name, :string, null: false
      add :scope, :string, null: false, default: "read"
      add :token_hash, :string, null: false
      add :revoked_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:api_keys, [:token_hash])
    create index(:api_keys, [:revoked_at])
  end
end
