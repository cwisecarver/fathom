defmodule Fathom.Shard.Replication.FrameAuth do
  @moduledoc """
  HMAC authentication for A2 replication frames (expert review 2026-08-20 #3, tier 3).

  ## What this closes

  Before it, **a reachable replication port was equivalent to write access to every shard on the
  node**. The listener accepts any TCP connection and applies whatever WAL frames arrive; the only
  control was `REPLICATION_BIND_IP`, i.e. network reachability. That is a real control and it
  stays, but it is the kind that fails silently: one misconfigured security group, one container
  on a shared bridge network, and there is nothing else in the way.

  Tier 2 of the finding proposed a peer-IP allowlist instead. It was skipped deliberately: the
  fleet's endpoints are HOSTNAMES and a peer presents a source IP, so an allowlist needs a
  resolution cache with an invalidation story, a full-mesh topology assumption that fails CLOSED
  (a write outage) against an asymmetric fleet, and source-IP fidelity that NAT and bridge
  networks do not provide. An HMAC authenticates the PEER rather than its address and is immune to
  all three.

  ## What it does NOT do, stated plainly

  **The signature covers the frame HEADER, not the payload.** So it authenticates that a holder of
  the fleet secret constructed *this header* — the shard id, epoch, generation, salt and offset —
  and it does not attest the WAL bytes that follow. An attacker who can modify bytes in flight on
  an established connection can still alter a payload under a valid header.

  That is a deliberate trade, not an oversight: a push payload is up to `REPLICATION_MAX_PUSH_BYTES`
  (1 MiB) and hashing it would put a megabyte of SHA-256 on the commit path, which is the hot path
  A2 is already the 4x cost of. The threat this is sized for is an unauthenticated peer opening a
  socket and injecting frames — which it stops completely, because such a peer cannot produce a
  valid header at all. It is not sized for an on-path attacker who can rewrite an authenticated
  peer's traffic; that is what TLS would be for, and there is none here.

  ## The two gates, and why there are two

  `:replication_sign_frames` (`REPLICATION_SIGN_FRAMES`) makes this node SIGN what it sends.
  `:replication_hmac_required` (`REPLICATION_HMAC_REQUIRED`) makes it REFUSE what arrives unsigned.

  They are separate for the same reason `REPLICATION_ENABLED` and `REPLICATION_LISTEN` are separate,
  and the rollout is the same shape: **signing must be on fleet-wide BEFORE any node requires it**,
  or a node that has flipped `required` refuses every frame from a peer that has not, and every
  shard that peer replicates loses quorum.

  There is no third "permissive" value — no accept-if-absent-but-log mode. A verification mode that
  accepts what it cannot verify is not a control, and the ones that ship as temporary do not get
  turned off. The transition is carried by the two booleans and the deploy order, not by a
  degraded checking mode.

  ## Key material

  Derived from `:replication_hmac_secret` (`REPLICATION_HMAC_SECRET`) when set, otherwise from
  `:hrana_token_secret` (`HRANA_TOKEN_SECRET`) — so a fleet that already distributes one shared
  secret does not have to distribute a second, while an operator who wants the two rotated
  independently can have that.

  **The configured secret is never used directly.** It is run through one HMAC with a fixed
  domain-separation label, so the replication key cannot mint a Hrana token and a leaked
  replication key does not hand over the token-signing secret. The derived key is cached in
  `:persistent_term` because it is computed on every frame on the commit path.
  """

  @label "fathom.replication.frame.v1"
  @cache_key {__MODULE__, :derived_key}

  # Truncated to 128 bits. A full SHA-256 tag costs 32 bytes on every frame for no meaningful gain
  # here — 128 bits is far beyond forgeable, and the frames are small and numerous.
  @tag_bytes 16

  @doc "Whether this node signs the frames it sends."
  @spec signing?() :: boolean()
  def signing?, do: Application.get_env(:fathom, :replication_sign_frames, false) == true

  @doc """
  Whether this node REFUSES frames that did not arrive signed and valid.

  Read on the receive path for every frame, so it is a plain config read rather than anything that
  can block.
  """
  @spec required?() :: boolean()
  def required?, do: Application.get_env(:fathom, :replication_hmac_required, false) == true

  @doc "Length in bytes of the tag `sign/1` produces."
  @spec tag_bytes() :: pos_integer()
  def tag_bytes, do: @tag_bytes

  @doc """
  The authentication tag for `material`, or `nil` when no key is configured.

  `nil` rather than a raise: signing is enabled by config and the key comes from a DIFFERENT
  config, so the two can disagree. A raise here would be inside the commit path, taking writes
  down for a misconfiguration that `check_replication_frame_auth!/0` refuses at boot anyway.
  """
  @spec sign(iodata()) :: binary() | nil
  def sign(material) do
    case key() do
      nil -> nil
      k -> binary_part(:crypto.mac(:hmac, :sha256, k, material), 0, @tag_bytes)
    end
  end

  @doc """
  Whether `tag` authenticates `material`.

  Constant-time comparison via `:crypto.hash_equals/2` — a byte-at-a-time `==` on a MAC leaks the
  position of the first differing byte, which is the textbook way a tag gets forged one byte at a
  time by an attacker who can retry.
  """
  @spec valid?(iodata(), binary()) :: boolean()
  def valid?(material, tag) when is_binary(tag) do
    case sign(material) do
      nil -> false
      expected -> byte_size(tag) == @tag_bytes and :crypto.hash_equals(expected, tag)
    end
  end

  def valid?(_material, _tag), do: false

  @doc """
  Whether key material is configured at all — what the boot guard asks.
  """
  @spec key_configured?() :: boolean()
  def key_configured?, do: key() != nil

  @doc """
  Drop the cached derived key. For tests and for a secret rotation that changes config at runtime;
  the cache is otherwise permanent for the life of the node.
  """
  @spec forget_key() :: :ok
  def forget_key do
    :persistent_term.erase(@cache_key)
    :ok
  end

  # Cached because this is on the commit path. The cache is keyed on nothing — a node does not
  # change its fleet secret without a restart, and `forget_key/0` exists for the case that it does.
  defp key do
    case :persistent_term.get(@cache_key, :miss) do
      :miss ->
        derived = derive()
        :persistent_term.put(@cache_key, derived)
        derived

      cached ->
        cached
    end
  end

  defp derive do
    case configured_secret() do
      secret when is_binary(secret) and byte_size(secret) > 0 ->
        :crypto.mac(:hmac, :sha256, secret, @label)

      _ ->
        nil
    end
  end

  # The Hrana secret is the FALLBACK, not the default, and the order matters: an operator who sets
  # REPLICATION_HMAC_SECRET wants the two rotated independently, so it must win.
  defp configured_secret do
    Application.get_env(:fathom, :replication_hmac_secret) ||
      Application.get_env(:fathom, :hrana_token_secret)
  end
end
