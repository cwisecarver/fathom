defmodule Fathom.ApiKeys do
  @moduledoc """
  Scoped, revocable, per-identity control-plane API keys (expert review #8) — the replacement for a
  single shared BasicAuth secret guarding `/api`.

  A key carries an actor `name` (so audit events attribute an action, #9), a `scope`
  (`read < manage < destroy`), and is stored only as a **SHA-256 hash** — the plaintext token is
  returned once at mint and never persisted. `authenticate/1` hashes a presented token and resolves it
  to `{name, scope}` iff the key exists and is not revoked. Revocation stamps `revoked_at` and takes
  effect on the next request — no restart (each `/api` request, a low-volume control-plane path, reads
  through to Postgres). Mint/list/revoke via `mix fathom.apikey`.
  """
  import Ecto.Query

  alias Fathom.ApiKeys.ApiKey
  alias Fathom.Repo

  @token_bytes 32
  @token_prefix "fathom_"
  @scope_rank %{"read" => 1, "manage" => 2, "destroy" => 3}

  @doc """
  Mints a new key named `name` with `scope`. Returns `{:ok, token, api_key}` — `token` is the
  plaintext secret, shown ONCE (only its hash is stored). `{:error, changeset}` on invalid input.
  """
  @spec mint(String.t(), String.t() | atom()) ::
          {:ok, String.t(), ApiKey.t()} | {:error, Ecto.Changeset.t()}
  def mint(name, scope \\ "read") do
    token = generate_token()

    case %ApiKey{}
         |> ApiKey.changeset(%{name: name, scope: to_string(scope), token_hash: hash(token)})
         |> Repo.insert() do
      {:ok, key} -> {:ok, token, key}
      {:error, changeset} -> {:error, changeset}
    end
  end

  @doc "Resolves a presented token to `{:ok, %{name, scope}}` iff it maps to a live (non-revoked) key."
  @spec authenticate(term()) :: {:ok, %{name: String.t(), scope: String.t()}} | :error
  def authenticate(token) when is_binary(token) do
    token_hash = hash(token)

    case Repo.one(from k in ApiKey, where: k.token_hash == ^token_hash and is_nil(k.revoked_at)) do
      nil -> :error
      key -> {:ok, %{name: key.name, scope: key.scope}}
    end
  end

  def authenticate(_), do: :error

  @doc "All keys (never the plaintext token), newest first."
  @spec list() :: [ApiKey.t()]
  def list, do: Repo.all(from k in ApiKey, order_by: [desc: k.inserted_at])

  @doc "Revokes key `id` (idempotent-ish). Takes effect on the next request."
  @spec revoke(term()) :: {:ok, ApiKey.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def revoke(id) do
    case Repo.get(ApiKey, id) do
      nil -> {:error, :not_found}
      key -> key |> ApiKey.changeset(%{revoked_at: DateTime.utc_now()}) |> Repo.update()
    end
  end

  @doc "Whether `actor_scope` grants at least `required` (read < manage < destroy)."
  @spec scope_at_least?(String.t() | atom(), String.t() | atom()) :: boolean()
  def scope_at_least?(actor_scope, required) do
    Map.get(@scope_rank, to_string(actor_scope), 0) >=
      Map.get(@scope_rank, to_string(required), 99)
  end

  defp generate_token,
    do:
      @token_prefix <> Base.url_encode64(:crypto.strong_rand_bytes(@token_bytes), padding: false)

  defp hash(token), do: :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
end
