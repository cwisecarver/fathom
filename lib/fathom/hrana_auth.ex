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

  ## Revocation and signing secret (expert review #31)

  A token embeds the shard's `token_version` at mint time.
  `Fathom.HranaAuth.revoke/1` bumps that version in the directory, so every
  outstanding token for **that one shard** stops verifying — no fleet-wide
  `SECRET_KEY_BASE` rotation (which used to be the only lever and also logged out
  the dashboard). The current floor is read through the
  `Fathom.HranaAuth.Revocations` cache, so verification stays off the Postgres hot
  path and a Postgres outage fails **open** on the version check only (a valid
  signature still opens; revocation converges within the cache TTL).

  Signing uses a **dedicated** secret — `config :fathom, :hrana_token_secret`
  (`HRANA_TOKEN_SECRET`) — separate from the web endpoint's `secret_key_base`, so a
  data-path secret rotation never touches web sessions/CSRF and vice versa. It falls
  back to `secret_key_base` when unset (backward compatible), with a boot warning.

  Tokens don't expire by default; set `config :fathom, :hrana_token_max_age` (or
  `HRANA_TOKEN_MAX_AGE`, seconds) to bound exposure. Mint with
  `mix fathom.token <shard>` in dev, or `Fathom.HranaAuth.token_for/1` from a
  release's remote console.
  """

  require Logger

  alias Fathom.{Directory, ShardId}
  alias Fathom.HranaAuth.Revocations

  @salt "fathom hrana shard"

  # How long after a graceful rotate the PREVIOUS token version keeps verifying (#24) — long
  # enough for the tenant to deploy the new token. Configurable via `:hrana_rotation_grace_ms`.
  @default_rotation_grace_ms 3_600_000

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
    with {:ok, %{"s" => granted, "v" => version} = payload} <-
           Phoenix.Token.verify(secret!(), @salt, token, max_age: max_age()),
         {:ok, ^granted} <- ShardId.cast(shard_id),
         true <- version_ok?(version, Revocations.floor_info(granted)) do
      # Stash the token's scope for the stream open that follows in THIS process (#24); the executor
      # reads it into the handle so it can enforce read-only per statement. Missing claim ⇒ rw.
      stash_scope(decode_scope(Map.get(payload, "sc")))
      :ok
    else
      # Bad signature/expiry, a token for a different shard, a revoked (stale-version)
      # token, or an id that doesn't cast (defense-in-depth). One opaque refusal for
      # all of them — a probe can't tell a wrong token from a wrong/revoked shard.
      _ -> {:error, @unauthorized}
    end
  end

  defp verify(_shard_id, _token), do: {:error, @unauthorized}

  # Whether a token's embedded `version` clears the shard's revocation floor, honoring the
  # graceful-rotation grace window (#24): the CURRENT floor always verifies, and the PREVIOUS
  # version (`floor - 1`) verifies too while a rotate's `bumped_at` is within the grace window —
  # so mint-new → deploy → the old auto-hardens out with no outage. A hard revoke sets `bumped_at`
  # to nil (no grace), and a floor-read outage with no last-known-good value is `:unavailable`
  # (fail-closed refuse, round-2 #25 — not a term-ordering accident).
  defp version_ok?(_version, :unavailable), do: false

  defp version_ok?(version, {floor, bumped_at}) do
    version >= floor or (version == floor - 1 and within_grace?(bumped_at))
  end

  defp within_grace?(%DateTime{} = bumped_at),
    do: DateTime.diff(DateTime.utc_now(), bumped_at, :millisecond) < rotation_grace_ms()

  defp within_grace?(_), do: false

  defp rotation_grace_ms,
    do: Application.get_env(:fathom, :hrana_rotation_grace_ms, @default_rotation_grace_ms)

  # Explicit map — never String.to_atom on the claim (it's from a signed token, but atom-exhaustion
  # hygiene is unconditional). Any unknown/missing value is the safe default: full access is only
  # granted by the absence of a claim / an explicit rw, and `ro` is the only restriction.
  defp decode_scope("ro"), do: :ro
  defp decode_scope(_), do: :rw

  @scope_key {__MODULE__, :stream_scope}

  @doc false
  @spec stash_scope(:rw | :ro) :: :ok
  def stash_scope(scope) do
    Process.put(@scope_key, scope)
    :ok
  end

  @doc """
  Read + clear the scope `authorize/2` stashed for the stream open running in THIS process (#24),
  defaulting to `:rw` (auth disabled ⇒ no token ⇒ full access; the trust boundary is the network).
  Called by `Fathom.ShardExecutor.open/1`.
  """
  @spec take_scope() :: :rw | :ro
  def take_scope, do: Process.delete(@scope_key) || :rw

  @doc """
  Mints a bearer token granting access to `shard_id` (canonicalized).

  `opts` pass through to `Phoenix.Token.sign/4` (e.g. `:signed_at`, for tests).
  """
  @spec token_for(term(), keyword()) :: {:ok, String.t()} | {:error, :invalid_shard_id}
  def token_for(shard_id, opts \\ []) do
    # `:scope` (`:rw` default | `:ro`, #24) is consumed here; the rest pass to Phoenix.Token.sign.
    {scope, sign_opts} = Keyword.pop(opts, :scope, :rw)

    case ShardId.cast(shard_id) do
      {:ok, canonical} ->
        # Embed the shard's current revocation version (expert review #31); a later
        # revoke/1 bumps the floor above this, so this token stops verifying. A
        # directory-unreachable mint (the `mix fathom.token` task runs with config
        # only, no Repo) defaults to version 1 — the floor is also read fail-open, so
        # a v1 token works until a revoke actually bumps the floor above 1.
        version = current_token_version(canonical)
        payload = put_scope(%{"s" => canonical, "v" => version}, scope)
        {:ok, Phoenix.Token.sign(secret!(), @salt, payload, sign_opts)}

      :error ->
        {:error, :invalid_shard_id}
    end
  end

  # A `ro` token carries an `"sc"` claim; a full (`rw`) token carries none — absence reads as `rw`,
  # so every already-outstanding token stays full-access (backward-compatible).
  defp put_scope(payload, :ro), do: Map.put(payload, "sc", "ro")
  defp put_scope(payload, _rw), do: payload

  @doc """
  Revokes every outstanding token for `shard_id` (expert review #31): bumps the
  directory revocation version, refreshes this node's cache so the floor takes
  effect immediately here, and pushes the new floor fleet-wide over Postgres
  LISTEN/NOTIFY (round-2 #24 — no BEAM cluster, so PubSub can't cross nodes; a
  lost notify still converges within the cache TTL). Returns the new version, or
  `{:error, :invalid_shard_id}`.
  """
  @spec revoke(term()) :: {:ok, pos_integer()} | {:error, :invalid_shard_id}
  def revoke(shard_id) do
    case ShardId.cast(shard_id) do
      {:ok, canonical} ->
        # canonical passed ShardId.cast, so a changeset refusal here means the
        # directory's own validation disagrees — surface it as the same error.
        case Directory.bump_token_version(canonical) do
          {:ok, version} ->
            # No bump instant ⇒ no grace: a revoke kills the previous version immediately.
            Revocations.put(canonical, version, nil)
            notify_revocation(canonical, version, nil)
            {:ok, version}

          {:error, _changeset} ->
            {:error, :invalid_shard_id}
        end

      :error ->
        {:error, :invalid_shard_id}
    end
  end

  @doc """
  Zero-downtime rotation for `shard_id` (#24): raises the shard's token version — stamping the
  rotate instant so the PREVIOUS version keeps verifying for the grace window
  (`:hrana_rotation_grace_ms`, default 1h) — refreshes this node's cache, pushes the change
  fleet-wide, and mints + returns a NEW token at the new version. Deploy the returned token within
  the grace window and the old auto-hardens out with no per-tenant outage — unlike `revoke/1`,
  which is immediate. `opts` pass through to the mint. Returns `{:ok, token}` or
  `{:error, :invalid_shard_id}`.
  """
  @spec rotate(term(), keyword()) :: {:ok, String.t()} | {:error, :invalid_shard_id}
  def rotate(shard_id, opts \\ []) do
    case ShardId.cast(shard_id) do
      {:ok, canonical} ->
        case Directory.rotate_token(canonical) do
          {:ok, version} ->
            bumped_at = DateTime.utc_now()
            Revocations.put(canonical, version, bumped_at)
            notify_revocation(canonical, version, bumped_at)
            # token_for reads the now-raised directory version, so it mints at the new version.
            token_for(canonical, opts)

          {:error, _changeset} ->
            {:error, :invalid_shard_id}
        end

      :error ->
        {:error, :invalid_shard_id}
    end
  end

  # Best-effort fleet push (round-2 #24): a notify failure must never fail the revoke/rotate — the
  # directory bump is the durable truth and the TTL converges. `bumped_at` (ISO or nil) carries the
  # rotation grace instant so other nodes honor it too.
  defp notify_revocation(shard_id, version, bumped_at) do
    Oban.Notifier.notify(Oban, :fathom_revocations, %{
      shard_id: shard_id,
      version: version,
      bumped_at: encode_bumped_at(bumped_at)
    })

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp encode_bumped_at(nil), do: nil
  defp encode_bumped_at(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

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

    # The boot warning the moduledoc has promised all along (round-2 #36): running
    # :required on the secret_key_base FALLBACK silently couples the data-path
    # credential to web session/CSRF signing — a routine web SECRET_KEY_BASE
    # rotation then invalidates every outstanding Hrana token, fleet-wide.
    if configured == :required and is_nil(Application.get_env(:fathom, :hrana_token_secret)) do
      Logger.warning(
        "hrana_auth is :required with no :hrana_token_secret — tokens are signing with " <>
          "the web secret_key_base, so rotating it will invalidate EVERY Hrana token. " <>
          "Set HRANA_TOKEN_SECRET to decouple the data path from web sessions."
      )
    end

    # No default expiry (#24): a bearer token minted with no max_age lives forever, so a leaked
    # credential's exposure grows unbounded. Warn loudly at boot; with graceful rotation
    # (`HranaAuth.rotate/1`) a finite max_age no longer means a rotation outage.
    if configured == :required and max_age() == :infinity do
      Logger.warning(
        "hrana_auth is :required but :hrana_token_max_age is unset (tokens NEVER expire). " <>
          "Set HRANA_TOKEN_MAX_AGE (seconds) to bound credential-leak exposure — rotation is " <>
          "now zero-downtime (HranaAuth.rotate/1), so a finite expiry is safe."
      )
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

  # The token-signing secret: a DEDICATED :hrana_token_secret when set (expert
  # review #31 — decouples the data-path credential from web session/CSRF signing),
  # falling back to the endpoint's secret_key_base for backward compatibility. No
  # dependency on the endpoint process, so this works in mix tasks and before Edge
  # starts.
  defp secret! do
    Application.get_env(:fathom, :hrana_token_secret) ||
      Application.get_env(:fathom, FathomWeb.Endpoint, [])[:secret_key_base] ||
      raise "config error: :hrana_auth needs :hrana_token_secret (or a " <>
              "secret_key_base under config :fathom, FathomWeb.Endpoint)"
  end

  defp max_age, do: Application.get_env(:fathom, :hrana_token_max_age, :infinity)

  # Read the shard's mint-time version, defaulting to 1 when the directory can't be
  # reached (the mix-task path has no Repo; the floor is read fail-open to match).
  defp current_token_version(canonical) do
    Directory.token_version(canonical) || 1
  rescue
    _ -> 1
  catch
    :exit, _ -> 1
  end
end
