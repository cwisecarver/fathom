defmodule Fathom.Repo.Migrations.AddTokenVersionToShards do
  use Ecto.Migration

  # Expert review #31: a per-shard revocation counter. A Hrana token carries the
  # token_version it was minted at; revoking a shard's credentials bumps this, so
  # every outstanding token for THAT shard stops verifying — without the fleet-wide
  # SECRET_KEY_BASE rotation that also logs out the dashboard. Starts at 1.
  def change do
    alter table(:shards) do
      add :token_version, :integer, default: 1, null: false
    end
  end
end
