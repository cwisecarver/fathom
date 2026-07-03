defmodule Fathom.HranaAuth do
  @moduledoc """
  In-app bearer-token auth for the Hrana data path — the host side of Filo's
  `:authorize` seam (wired in `Fathom.Application`'s Hrana listener).

  A credential is a `Phoenix.Token` signed with the endpoint's `secret_key_base`
  that grants access to exactly **one shard** (the canonical, downcased id).
  Clients present it as libSQL's `authToken`, which arrives two ways — an
  `Authorization: Bearer` header on Hrana-over-HTTP, or the `hello` message's
  `jwt` field on Hrana-over-WebSocket (django-libsql's path; the token is NOT in
  the upgrade headers, which is why this couldn't be a plain pre-plug) — and Filo
  hands both to `authorize/2` before any stream opens.

  Gated by `config :fathom, :hrana_auth`:

    * `:disabled` (default) — no in-app credential; the trust boundary is the
      network (LB-only reachability, `HRANA_BIND_IP`), as documented in
      `docs/deploy-cluster.md`.
    * `:required` — every stream open must present a token for the shard it
      names. Prod opts in with `HRANA_AUTH=required` (config/runtime.exs); a boot
      guard (`check_config!/0`) refuses `:required` without a usable secret.
      Any other configured value fails closed to `:required`.

  Tokens don't expire by default (they are database credentials — revoke by
  rotating `SECRET_KEY_BASE`); set `config :fathom, :hrana_token_max_age` (or
  `HRANA_TOKEN_MAX_AGE`, seconds) to enforce expiry. Mint with
  `mix fathom.token <shard>` in dev, or `Fathom.HranaAuth.token_for/1` from a
  release's remote console.
  """

  @salt "fathom hrana shard"

  # Both refusals are 401 with the same code; the message distinguishes "you sent
  # nothing" (an unconfigured client) from "what you sent doesn't grant this shard"
  # (bad signature, expired, or a foreign shard's token — deliberately not told apart,
  # so a probe can't distinguish a wrong token from a wrong shard).
  @missing %Filo.Error{message: "missing bearer token", code: "AUTH_REQUIRED", status: 401}
  @unauthorized %Filo.Error{message: "unauthorized", code: "AUTH_REQUIRED", status: 401}

  @doc """
  Filo's `:authorize` callback: may `token` open a stream on `shard_id`?

  `shard_id` is the resolved open-arg (`Fathom.ShardExecutor.shard_from_conn/1`'s
  result). Returns `:ok` or `{:error, %Filo.Error{status: 401}}`.
  """
  @spec authorize(String.t() | nil, String.t() | nil) :: :ok | {:error, Filo.Error.t()}
  # No shard resolved: pass through so `ShardExecutor.open(nil)` refuses with its
  # clearer 400 (the fail-closed posture, finding #26) instead of a misleading 401 —
  # nothing can open on a nil shard regardless.
  def authorize(nil, _token), do: :ok

  def authorize(shard_id, token) do
    case mode() do
      :disabled -> :ok
      :required -> verify(shard_id, token)
    end
  end

  defp verify(_shard_id, nil), do: {:error, @missing}

  defp verify(shard_id, token) when is_binary(token) do
    with {:ok, granted} <- Phoenix.Token.verify(secret!(), @salt, token, max_age: max_age()),
         {:ok, ^granted} <- Fathom.ShardId.cast(shard_id) do
      :ok
    else
      # Bad signature/expiry, a token for a different shard, or an id that doesn't
      # cast (defense-in-depth: even if open/1's validation ever regressed, nothing
      # unauthorized gets past here). One opaque refusal for all of them.
      _ -> {:error, @unauthorized}
    end
  end

  defp verify(_shard_id, _token), do: {:error, @unauthorized}

  @doc """
  Mints a bearer token granting access to `shard_id` (canonicalized).

  `opts` pass through to `Phoenix.Token.sign/4` (e.g. `:signed_at`, for tests).
  """
  @spec token_for(term(), keyword()) :: {:ok, String.t()} | {:error, :invalid_shard_id}
  def token_for(shard_id, opts \\ []) do
    case Fathom.ShardId.cast(shard_id) do
      {:ok, canonical} -> {:ok, Phoenix.Token.sign(secret!(), @salt, canonical, opts)}
      :error -> {:error, :invalid_shard_id}
    end
  end

  @doc """
  Boot guard (called from `Fathom.Application.start/2`): refuse to start with a
  nonsense `:hrana_auth` value, or with auth required but no usable signing
  secret — better a loud boot failure than a node that 401s all traffic (or,
  worse, one that a typo silently left open).
  """
  @spec check_config!() :: :ok
  def check_config! do
    configured = Application.get_env(:fathom, :hrana_auth, :disabled)

    if configured not in [:required, :disabled] do
      raise "config error: :hrana_auth must be :required or :disabled, got #{inspect(configured)}"
    end

    if configured == :required and byte_size(secret!()) < 32 do
      raise "config error: :hrana_auth is :required but secret_key_base is too short " <>
              "(need >= 32 bytes) — generate one with: mix phx.gen.secret"
    end

    :ok
  end

  # Anything that isn't exactly :disabled requires a token — a typo in the mode must
  # deny traffic (loud, recoverable), never silently open the fleet.
  defp mode do
    case Application.get_env(:fathom, :hrana_auth, :disabled) do
      :disabled -> :disabled
      _ -> :required
    end
  end

  # The endpoint's secret_key_base straight from config (Phoenix.Token key-derives
  # per salt, so sharing the secret with cookie signing is fine); no dependency on
  # the endpoint process, so this works in mix tasks and before Edge starts.
  defp secret! do
    Application.get_env(:fathom, FathomWeb.Endpoint, [])[:secret_key_base] ||
      raise "config error: :hrana_auth needs a secret_key_base under " <>
              "config :fathom, FathomWeb.Endpoint (prod sets it from SECRET_KEY_BASE)"
  end

  defp max_age, do: Application.get_env(:fathom, :hrana_token_max_age, :infinity)
end
