defmodule Fathom.ApiKeys.ApiKey do
  @moduledoc """
  A scoped, revocable control-plane API key (expert review #8). Stored as a SHA-256 `token_hash` —
  the plaintext token is shown once at mint and never persisted. `scope` is `read < manage < destroy`.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @scopes ~w(read manage destroy)

  @type t :: %__MODULE__{}

  schema "api_keys" do
    field :name, :string
    field :scope, :string, default: "read"
    field :token_hash, :string
    field :revoked_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec)
  end

  @doc "The valid scopes, least- to most-privileged."
  def scopes, do: @scopes

  def changeset(key, attrs) do
    key
    |> cast(attrs, [:name, :scope, :token_hash, :revoked_at])
    |> validate_required([:name, :scope, :token_hash])
    |> validate_inclusion(:scope, @scopes)
    |> unique_constraint(:token_hash)
  end
end
