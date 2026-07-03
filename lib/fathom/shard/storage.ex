defmodule Fathom.Shard.Storage do
  @moduledoc """
  Durable, bottomless storage for shard database files. The `Fathom.Shard`
  coordinator **pulls** a shard's SQLite file from storage when it wakes and
  **flushes** it back (then drops the local copy) when the shard goes idle, so a
  node only holds the working set on local disk.

  This is a behaviour with a pluggable backend, selected by config:

      config :fathom, :shard_storage, Fathom.Shard.Storage.Local   # default

  `Fathom.Shard.Storage.Local` (a filesystem object store, the default) is what
  dev and tests use; `Fathom.Shard.Storage.S3` is the production backend.

  Both callbacks key by `shard_id`. `pull/2` writes the remote object to
  `local_path`; if the shard has no object yet (a brand-new shard) it returns
  `:ok` without creating a file. `flush/2` uploads `local_path` to the object for
  `shard_id`.

  ## Cross-node single-writer leasing

  A shard's file is one durable object, but SQLite/WAL only serializes writers on
  one shared file on one machine — nothing in `pull`/`flush` stops two nodes from
  each pulling their own copy, accepting writes, and flushing, with the last flush
  silently clobbering the other's. So storage also owns a per-shard **lease**: the
  `Fathom.Shard` coordinator acquires it before pulling, renews it for the shard's
  lifetime, and releases it on flush. A monotonic **epoch** stamped on the lease is
  the fencing token — a node that loses its lease (GC pause, partition) self-fences
  and never flushes over a newer owner.

  The lease is a `t:lease/0` (`owner`, `epoch`, `expires_at_ms`). `acquire_lease/3`
  returns `{:error, {:held, owner}}` when another owner holds a live lease;
  `renew_lease/3` returns `{:error, :superseded}` once the lease has been taken
  over. Backends MUST enforce mutual exclusion with a conditional/atomic write
  (the `Local` backend uses an `O_EXCL` lock file; `S3` uses conditional PUTs) and
  MUST **fail closed** on a transient lookup error — never fall back to an
  unconditional overwrite, which silently steals a live owner's lease.

  ## Node heartbeat (liveness, separate from ownership)

  Renewing every shard's lock individually is `active_shards / (ttl/3)` writes per
  second per node — a PUT storm at scale (millions of shards ⇒ ~100k PUT/s/node,
  see `mix fathom.scale --lease-rps`). So **liveness is a single per-node object**,
  not per-shard: a node renews one `t:heartbeat/0` at `heartbeat/<owner>` via
  `renew_heartbeat/2` (driven by `Fathom.Shard.Heartbeat`), and a shard lock no
  longer carries a meaningful per-lock TTL — its owner is *live* iff that owner's
  heartbeat is fresh. So `acquire_lease/3`'s steal decision reads the current
  owner's heartbeat (not `lock.expires_at_ms`): steal only if the heartbeat is
  missing or expired **past `steal_margin_ms/0`** (a clock-skew guard — see that
  function), and **fail closed** on a heartbeat read error (don't steal on
  uncertainty). `check_lease/2` is the read-only fence the coordinator uses before
  a flush to confirm it still owns the shard (it pairs with the holder's
  locally-confirmed heartbeat validity — see `Fathom.Shard.Heartbeat`). The lock's
  `expires_at_ms` is retained for wire/decode compatibility and debugging, but is
  no longer the liveness signal.
  """

  @typedoc """
  A shard lease. `owner` identifies the holding node, `epoch` is the monotonic
  fencing token (bumped each time an expired lease is stolen), and `expires_at_ms`
  is the wall-clock expiry in `System.system_time(:millisecond)`.
  """
  @type lease :: %{owner: String.t(), epoch: non_neg_integer(), expires_at_ms: integer()}

  @typedoc """
  A node's liveness heartbeat. `owner` identifies the node, `expires_at_ms` is the
  wall-clock expiry in `System.system_time(:millisecond)`. A shard whose lock names
  `owner` is live iff `owner`'s heartbeat is fresh.
  """
  @type heartbeat :: %{owner: String.t(), expires_at_ms: integer()}

  # Pull the stored object to `local_path`, returning its current etag (or `nil` for a
  # brand-new shard with no object — nothing is written). The etag lets the coordinator fence
  # its first flush without a second round-trip (finding #15).
  @callback pull(shard_id :: String.t(), local_path :: Path.t()) ::
              {:ok, String.t() | nil} | {:error, term()}

  # Unconditional write of `local_path` to the shard's object. For callers that don't fence
  # the live object (migration version copies, benchmarks); the coordinator uses `flush/3`.
  @callback flush(shard_id :: String.t(), local_path :: Path.t()) :: :ok | {:error, term()}

  # Fenced flush: write `local_path` only if the stored object still matches `expected_etag`
  # (`If-Match`), or — for a brand-new shard (`expected_etag == nil`) — only if no object
  # exists yet (`If-None-Match: *`). `{:error, :superseded}` (a 412) means the object changed
  # under us (a stealer flushed) → the caller must self-fence and NOT clobber. On success
  # returns the new object etag so the caller can fence its next flush. Puts the fencing token
  # on the data write itself, closing the check-then-PUT window (finding #15).
  @callback flush(
              shard_id :: String.t(),
              local_path :: Path.t(),
              expected_etag :: String.t() | nil
            ) ::
              {:ok, String.t()} | {:error, :superseded} | {:error, term()}

  # The stored object's current etag WITHOUT transferring the body (an S3 HEAD; the Local
  # double hashes the file). `{:ok, nil}` when no object exists. Used on a warm restart — where
  # the coordinator kept its local copy and skipped the pull — to learn the etag its first
  # fenced flush must match.
  @callback object_etag(shard_id :: String.t()) :: {:ok, String.t() | nil} | {:error, term()}

  # Conditional pull, keyed on the caller's currently-held `etag` (an opaque store
  # value captured from a prior pull). The warm-standby freshness check: a warm cache
  # may lag the owner's latest flush, so before serving it we confirm it equals the
  # store's current object.
  #
  #   * `{:ok, :unchanged}` — the object's etag matches `etag`; nothing written, the
  #     caller's existing local copy is current (a store 304). No byte transfer.
  #   * `{:ok, {:written, new_etag}}` — the object differs (or `etag` was `nil`); fresh
  #     bytes were written to `local_path`, `new_etag` is the current object's etag
  #     (may be `nil` if the store returned none).
  #   * `{:ok, :absent}` — no object exists (a brand-new shard); nothing written.
  #
  # `nil` `etag` degrades to an unconditional pull (always `:written` or `:absent`),
  # which is how a follower captures the initial etag.
  @callback pull_if_changed(
              shard_id :: String.t(),
              local_path :: Path.t(),
              etag :: String.t() | nil
            ) ::
              {:ok, :unchanged}
              | {:ok, {:written, String.t() | nil}}
              | {:ok, :absent}
              | {:error, term()}

  @callback acquire_lease(shard_id :: String.t(), owner :: String.t(), ttl_ms :: pos_integer()) ::
              {:ok, lease()} | {:error, {:held, String.t()}} | {:error, term()}
  @callback renew_lease(shard_id :: String.t(), lease :: lease(), ttl_ms :: pos_integer()) ::
              {:ok, lease()} | {:error, :superseded} | {:error, term()}
  @callback release_lease(shard_id :: String.t(), lease :: lease()) :: :ok | {:error, term()}

  # Read-only fence: confirm `lease` is still the live lock for `shard_id` (owner +
  # epoch unchanged) without writing. `:superseded` once another owner/epoch holds it.
  @callback check_lease(shard_id :: String.t(), lease :: lease()) ::
              :ok | {:error, :superseded} | {:error, term()}

  # Per-node liveness heartbeat (one object per node, not per shard).
  @callback renew_heartbeat(owner :: String.t(), ttl_ms :: pos_integer()) ::
              {:ok, heartbeat()} | {:error, term()}
  @callback read_heartbeat(owner :: String.t()) ::
              {:ok, heartbeat()} | :not_found | {:error, term()}
  @callback clear_heartbeat(owner :: String.t()) :: :ok | {:error, term()}

  # Versioned copies for blue/green migration: the live object stays
  # `<shard_id>`, and the migrator keeps prior versions under `<shard_id>@<version>`
  # for the retention window so a revert is a copy-back.
  @callback retain(shard_id :: String.t(), version :: non_neg_integer()) ::
              :ok | {:error, term()}
  @callback restore(shard_id :: String.t(), version :: non_neg_integer()) ::
              :ok | {:error, term()}
  @callback drop_version(shard_id :: String.t(), version :: non_neg_integer()) ::
              :ok | {:error, term()}

  @doc "Pulls the shard's stored file to `local_path`, returning its etag (`nil` if absent)."
  @spec pull(String.t(), Path.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def pull(shard_id, local_path), do: backend().pull(shard_id, local_path)

  @doc "Unconditional flush of `local_path` (unfenced — see `flush/3` for the coordinator)."
  @spec flush(String.t(), Path.t()) :: :ok | {:error, term()}
  def flush(shard_id, local_path), do: backend().flush(shard_id, local_path)

  @doc "Fenced flush: writes only if the stored object still matches `expected_etag`. See callback."
  @spec flush(String.t(), Path.t(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :superseded} | {:error, term()}
  def flush(shard_id, local_path, expected_etag),
    do: backend().flush(shard_id, local_path, expected_etag)

  @doc "The stored object's current etag (`nil` if absent) without transferring the body."
  @spec object_etag(String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def object_etag(shard_id), do: backend().object_etag(shard_id)

  @doc """
  Conditional pull keyed on the caller's held `etag` — the warm-standby freshness
  check. Returns `{:ok, :unchanged}` (store object matches `etag`; nothing written),
  `{:ok, {:written, new_etag}}` (fresh bytes written to `local_path`), or
  `{:ok, :absent}` (no object). A `nil` `etag` is an unconditional pull that captures
  the current etag. See the callback docs.
  """
  @spec pull_if_changed(String.t(), Path.t(), String.t() | nil) ::
          {:ok, :unchanged}
          | {:ok, {:written, String.t() | nil}}
          | {:ok, :absent}
          | {:error, term()}
  def pull_if_changed(shard_id, local_path, etag),
    do: backend().pull_if_changed(shard_id, local_path, etag)

  @doc """
  Acquires `shard_id`'s lease for `owner` with a `ttl_ms` window. Returns
  `{:error, {:held, owner}}` if another owner holds a live lease, or
  `{:error, reason}` on a transient store error (the caller must NOT proceed —
  see the module doc on failing closed).
  """
  @spec acquire_lease(String.t(), String.t(), pos_integer()) ::
          {:ok, lease()} | {:error, {:held, String.t()}} | {:error, term()}
  def acquire_lease(shard_id, owner, ttl_ms),
    do: backend().acquire_lease(shard_id, owner, ttl_ms)

  @doc """
  Renews `lease` on `shard_id`, extending its expiry by `ttl_ms`. Returns
  `{:error, :superseded}` once another owner has taken the lease (the holder must
  self-fence), or `{:error, reason}` on a transient store error (retry, don't
  fence — a transient blip is not loss of ownership).
  """
  @spec renew_lease(String.t(), lease(), pos_integer()) ::
          {:ok, lease()} | {:error, :superseded} | {:error, term()}
  def renew_lease(shard_id, lease, ttl_ms),
    do: backend().renew_lease(shard_id, lease, ttl_ms)

  @doc "Releases `lease` on `shard_id` (no-op if we no longer hold it)."
  @spec release_lease(String.t(), lease()) :: :ok | {:error, term()}
  def release_lease(shard_id, lease), do: backend().release_lease(shard_id, lease)

  @doc """
  Read-only fence: returns `:ok` if `lease` is still the live lock for `shard_id`
  (same owner + epoch), `{:error, :superseded}` if another owner/epoch has taken it,
  or `{:error, reason}` on a transient store error. Unlike `renew_lease/3` this does
  not write — liveness is the node heartbeat, so the flush fence only needs to
  confirm ownership hasn't changed.
  """
  @spec check_lease(String.t(), lease()) :: :ok | {:error, :superseded} | {:error, term()}
  def check_lease(shard_id, lease), do: backend().check_lease(shard_id, lease)

  @doc """
  Renews this node's liveness heartbeat (`owner`), extending its expiry by `ttl_ms`.
  One object per node — the cost is O(nodes), not O(shards). Driven by
  `Fathom.Shard.Heartbeat`.
  """
  @spec renew_heartbeat(String.t(), pos_integer()) :: {:ok, heartbeat()} | {:error, term()}
  def renew_heartbeat(owner, ttl_ms), do: backend().renew_heartbeat(owner, ttl_ms)

  @doc "Reads `owner`'s heartbeat (the liveness signal `acquire_lease/3` consults to steal)."
  @spec read_heartbeat(String.t()) :: {:ok, heartbeat()} | :not_found | {:error, term()}
  def read_heartbeat(owner), do: backend().read_heartbeat(owner)

  @doc "Clears `owner`'s heartbeat (clean node shutdown so its shards are immediately stealable)."
  @spec clear_heartbeat(String.t()) :: :ok | {:error, term()}
  def clear_heartbeat(owner), do: backend().clear_heartbeat(owner)

  @doc "Copies the shard's live object to its `version` (retains the old version for revert)."
  @spec retain(String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def retain(shard_id, version), do: backend().retain(shard_id, version)

  @doc "Copies the shard's `version` object back to live (the revert step)."
  @spec restore(String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def restore(shard_id, version), do: backend().restore(shard_id, version)

  @doc "Deletes the shard's `version` object (idempotent; retirement)."
  @spec drop_version(String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def drop_version(shard_id, version), do: backend().drop_version(shard_id, version)

  @doc false
  @spec encode_lease(lease()) :: binary()
  def encode_lease(%{owner: owner, epoch: epoch, expires_at_ms: exp}),
    do: Jason.encode!(%{"owner" => owner, "epoch" => epoch, "expires_at_ms" => exp})

  @doc false
  @spec decode_lease(binary()) :: {:ok, lease()} | :error
  def decode_lease(body) do
    case Jason.decode(body) do
      {:ok, %{"owner" => owner, "epoch" => epoch, "expires_at_ms" => exp}}
      when is_binary(owner) and is_integer(epoch) and is_integer(exp) ->
        {:ok, %{owner: owner, epoch: epoch, expires_at_ms: exp}}

      _ ->
        :error
    end
  end

  @doc false
  @spec encode_heartbeat(heartbeat()) :: binary()
  def encode_heartbeat(%{owner: owner, expires_at_ms: exp}),
    do: Jason.encode!(%{"owner" => owner, "expires_at_ms" => exp})

  @doc false
  @spec decode_heartbeat(binary()) :: {:ok, heartbeat()} | :error
  def decode_heartbeat(body) do
    case Jason.decode(body) do
      {:ok, %{"owner" => owner, "expires_at_ms" => exp}}
      when is_binary(owner) and is_integer(exp) ->
        {:ok, %{owner: owner, expires_at_ms: exp}}

      _ ->
        :error
    end
  end

  @doc false
  @spec now_ms() :: integer()
  def now_ms, do: System.system_time(:millisecond)

  @default_steal_margin_ms 5_000

  @doc """
  How long past a heartbeat's expiry an owner must stay silent before a peer may
  steal its shards (`config :fathom, :steal_margin_ms`, default
  #{@default_steal_margin_ms}ms).

  The steal decision compares two nodes' wall clocks — the reader's `now` against the
  owner's heartbeat `expires_at_ms` — so this margin absorbs inter-node clock skew: a
  peer steals only once the heartbeat is expired by MORE than the margin, so a wrongful
  steal requires skew greater than the heartbeat's remaining life PLUS this margin. It
  must exceed the fleet's max inter-node clock skew — run NTP and monitor skew (a
  sustained `fathom.shard.lease.superseded` rate is the tripwire). Cost: a hard crash
  (an owner that dies without clearing its heartbeat) delays failover by up to this
  margin beyond the lease TTL; a clean shutdown clears the heartbeat and is unaffected.
  """
  @spec steal_margin_ms() :: non_neg_integer()
  def steal_margin_ms,
    do: Application.get_env(:fathom, :steal_margin_ms, @default_steal_margin_ms)

  @doc """
  Write `body` to `path` atomically: write a sibling temp file, then `File.rename/2` it into
  place (atomic on POSIX within a filesystem — the temp sits in `path`'s dir). A crash or a
  concurrent reader (a warm-cache promotion mid-rewrite) therefore sees either the whole old
  file or the whole new one, never a torn/half-written object (findings #24/#28). The temp is
  cleaned up on a write or rename error.
  """
  @spec atomic_write(Path.t(), iodata()) :: :ok | {:error, term()}
  def atomic_write(path, body), do: with_atomic_temp(path, &File.write(&1, body))

  @doc "Like `atomic_write/2` but copies `src`'s bytes into `dst` (temp + rename)."
  @spec atomic_copy(Path.t(), Path.t()) :: :ok | {:error, term()}
  def atomic_copy(src, dst), do: with_atomic_temp(dst, &File.cp(src, &1))

  # Materialize into a sibling temp, then atomically rename into `dst`; drop the temp on error.
  defp with_atomic_temp(dst, produce) do
    File.mkdir_p!(Path.dirname(dst))
    tmp = "#{dst}.tmp.#{System.unique_integer([:positive])}"

    with :ok <- produce.(tmp),
         :ok <- File.rename(tmp, dst) do
      :ok
    else
      {:error, _} = err ->
        File.rm(tmp)
        err
    end
  end

  defp backend, do: Application.get_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
end
