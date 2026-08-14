defmodule Fathom.HranaAuth.Issuance do
  @moduledoc """
  One row per Hrana token minted (expert review 2026-08-01 #37).

  Stores the token's **claims**, never the secret. A Hrana token is a stateless `Phoenix.Token`
  verified by signature, not by lookup, so persisting anything derived from the secret would add a
  credential to steal while answering no question the claims cannot. That is stricter than
  `Fathom.ApiKeys.ApiKey`, which keeps a SHA-256 hash because control-plane keys ARE looked up.

  Append-only: rotation and revocation move the per-shard `token_version` floor, they do not edit
  history. "Is this issuance still valid?" is answered by comparing its `token_version` against the
  shard's current floor, which is what `Fathom.HranaAuth.Ledger.outstanding/1` does.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @scopes ~w(rw ro)

  @type t :: %__MODULE__{}

  schema "hrana_token_issuances" do
    field :shard_id, :string
    field :token_version, :integer
    field :scope, :string, default: "rw"
    field :actor, :string
    field :minted_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec)
  end

  @doc "Valid scope claims: `rw` (full) and `ro` (read-only, the #24 `\"sc\"` claim)."
  def scopes, do: @scopes

  def changeset(issuance, attrs) do
    issuance
    |> cast(attrs, [:shard_id, :token_version, :scope, :actor, :minted_at])
    |> validate_required([:shard_id, :token_version, :minted_at])
    |> validate_inclusion(:scope, @scopes)
    # `actor` is provenance for a human reading an audit, not an authorization input. Bounded so a
    # caller cannot write an unbounded blob into the control plane through a mint.
    |> validate_length(:actor, max: 200)
  end
end
