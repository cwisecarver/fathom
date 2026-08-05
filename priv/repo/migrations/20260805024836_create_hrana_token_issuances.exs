defmodule Fathom.Repo.Migrations.CreateHranaTokenIssuances do
  use Ecto.Migration

  # Expert review 2026-08-01 #37. The per-shard token lifecycle was well built — mint, zero-downtime
  # rotate with a grace window, immediate revoke, a read-only scope — but there was **no record of
  # which tokens were ever issued**. Hrana tokens are stateless `Phoenix.Token`s and only a per-shard
  # `token_version` FLOOR is persisted, so nothing answered "what is outstanding, for whom, minted
  # when, with what scope". `mix fathom.token <shard>` minted and printed; nothing was written down.
  #
  # This is the ledger. It stores the CLAIMS, never the secret — stricter than `api_keys`, which
  # persists a SHA-256 hash. Here not even a hash is warranted: a Hrana token is verified by
  # signature, not by lookup, so anything derived from the secret would add a credential to steal
  # while answering no question the claims cannot.
  #
  # Deliberately append-only, and best-effort at the write site: a ledger outage must never fail a
  # mint. An incomplete ledger under-reports what is outstanding, which is the safe direction for
  # the bulk revoke built on it — it revokes less than it might, never more.
  def change do
    create table(:hrana_token_issuances) do
      add :shard_id, :string, null: false
      # The revocation floor the token embeds, so a sweep can tell whether a shard's floor has
      # already moved past everything the ledger lists for it.
      add :token_version, :integer, null: false
      # "rw" | "ro" — the #24 scope claim, so an audit distinguishes a read-only credential.
      add :scope, :string, null: false, default: "rw"

      # Free-text provenance: the mix task, the control-plane API, a test. NOT authenticated — an
      # audit breadcrumb, never an authorization input.
      add :actor, :string

      # When the token was minted, distinct from inserted_at so a replayed/backfilled row can carry
      # the real issuance instant.
      add :minted_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec)
    end

    # The bulk-revoke sweep asks "which shards have a token issued before <cutoff>". Time leads
    # because the cutoff is the selective predicate — a fleet has many shards and the sweep wants a
    # range scan, not a per-shard probe.
    create index(:hrana_token_issuances, [:minted_at])
    create index(:hrana_token_issuances, [:shard_id, :minted_at])
  end
end
