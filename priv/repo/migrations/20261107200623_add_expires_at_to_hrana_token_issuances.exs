defmodule Fathom.Repo.Migrations.AddExpiresAtToHranaTokenIssuances do
  use Ecto.Migration

  # Expert review 2026-11-07 #33. `Ledger.stream_issued_before/2` selected "ever minted", but its
  # moduledoc and `revoke_issued_before/2`'s contract are "a token from that mint STILL VERIFIES":
  # every mint wrote a row with no expiry, so a token whose max-age had long passed — and every 60 s
  # console mint (`console:<user>` actors) — stayed "outstanding" until a floor bump. Under the prod
  # config `HRANA_AUTH=required` (which REFUSES to boot without a finite `HRANA_TOKEN_MAX_AGE`) every
  # tenant minted before the cutoff therefore stayed in the sweep, and the "laptop lost Feb 15"
  # scalpel degraded into a fleet-wide revoke.
  #
  # The instant the token stops verifying, recorded at mint time from the SAME computation that
  # puts the `"e"` claim in the token (`signed_at + max_age`, with the console's per-token ttl
  # back-dating already applied), so it never disagrees with what `Phoenix.Token.verify/4` will
  # accept. NULL means "does not expire" (`:hrana_token_max_age` is `:infinity`) OR "minted before
  # this column existed": both keep today's over-select, which is the safe direction for an
  # incident tool — wide on blast radius, never missing a live token.
  #
  # Nullable and unindexed: the selective predicate is still `minted_at`, and the expiry is a
  # filter on the rows that range scan already returns.
  def change do
    alter table(:hrana_token_issuances) do
      add :expires_at, :utc_datetime_usec
    end
  end
end
