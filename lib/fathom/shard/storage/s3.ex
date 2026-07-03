defmodule Fathom.Shard.Storage.S3 do
  @moduledoc """
  S3 (or any S3-compatible store: Tigris, MinIO, R2) shard storage, signing with
  `Req`'s built-in `aws_sigv4` — no extra AWS dependency.

      config :fathom, :shard_storage, Fathom.Shard.Storage.S3

      config :fathom, Fathom.Shard.Storage.S3,
        bucket: "fathom-shards",
        region: "us-east-1",
        prefix: "shards/",                 # optional key prefix
        access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
        secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
        token: System.get_env("AWS_SESSION_TOKEN"),   # optional (STS)
        endpoint: nil,                     # optional override for S3-compatible stores
        path_style: false,                 # true for path-style stores (MinIO, R2)
        pool_size: 200,                    # connections per pool in the dedicated S3 Finch pool
        pool_count: 1                      # number of pools (total conns = pool_size * pool_count)

  Each shard is one object at `<prefix><shard_id>.db`. `pull/2` returns the object's
  etag (`{:ok, nil}` for a missing object — a brand-new shard — without creating a file).

  Addressing defaults to AWS **virtual-hosted** style (`bucket.s3.region…`). Stores
  that need **path-style** (`endpoint/bucket/key`) — MinIO, Cloudflare R2 — set
  `path_style: true` alongside an `endpoint`.

  The per-shard lease is a second object at `<prefix><shard_id>.lock`, mutated with
  **conditional writes** (`If-None-Match: *` to create, `If-Match: <etag>` to
  steal/renew) so concurrent acquirers get a 412 and only one wins. A transient
  lookup error **fails closed** (`{:error, {:transient_lookup, _}}`) rather than
  falling back to an unconditional PUT — an unconditional overwrite would silently
  steal a live owner's lease (the classic distributed-lease split-brain). S3
  conditional writes are supported on AWS S3 and most compatible stores; verify
  the target store before relying on the fence.

  > Not yet exercised against a real bucket — the `Local` backend proves the lease
  > mechanics in tests; a live S3 round-trip is tracked separately.
  """
  @behaviour Fathom.Shard.Storage

  alias Fathom.Shard.Storage

  # Dedicated Finch pool for all S3 traffic. Warming (node startup / failover)
  # pulls many shards from S3 at once; Req's default pool tops out near 50
  # conns/host, which caps warming throughput well below bandwidth. The pool is
  # config-driven on two axes: `pool_size` (connections per nimble_pool) and
  # `pool_count` (number of pools). Total connections to the S3 host = size *
  # count; raising `pool_count` spreads concurrent checkouts across more pool
  # processes, which cuts the checkout contention a single large pool hits under
  # a bursty warming fan-out. Started by the app supervision tree (and by the
  # bench's setup_s3); `req/0` routes through it whenever it's running.
  @finch __MODULE__.Finch
  @default_pool_size 200
  @default_pool_count 1

  @doc """
  Name of the dedicated Finch pool that carries all S3 traffic.
  """
  @spec finch_name() :: atom()
  def finch_name, do: @finch

  @doc """
  Child spec for the dedicated S3 Finch pool, for the supervision tree.

  Two knobs, both via `config :fathom, Fathom.Shard.Storage.S3`:

    * `pool_size` (default #{@default_pool_size}) — connections per nimble_pool.
    * `pool_count` (default #{@default_pool_count}) — number of pools.

  Total connections to the S3 host = `pool_size * pool_count`. Req's default
  ~50-conn pool caps warming throughput (many shards pulled from S3 at once on
  startup/failover); a larger pool lifts that ceiling, and raising `pool_count`
  spreads concurrent checkouts across more pool processes to cut the checkout
  contention one big pool hits under a bursty fan-out. Idle pools hold no
  connections, so it costs nothing when the backend is `Local` or S3 is idle.
  """
  @spec finch_child_spec() :: {module(), keyword()}
  def finch_child_spec do
    {Finch, name: @finch, pools: %{default: [size: pool_size(), count: pool_count()]}}
  end

  defp pool_size, do: config()[:pool_size] || @default_pool_size
  defp pool_count, do: config()[:pool_count] || @default_pool_count

  @impl true
  def pull(shard_id, local_path) do
    case Req.get(req(), url: object_path(shard_id)) do
      {:ok, %{status: 200, body: body, headers: h}} ->
        # Atomic local write so a concurrent reader never sees a half-downloaded file (#24).
        # Return the object's etag so the coordinator can fence its first flush (#15).
        case Storage.atomic_write(local_path, body) do
          :ok -> {:ok, etag(h)}
          err -> err
        end

      # Brand-new shard — no object, nothing written, no etag to fence against yet.
      {:ok, %{status: 404}} ->
        {:ok, nil}

      {:ok, %{status: status}} ->
        {:error, {:s3_get_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def flush(shard_id, local_path) do
    with {:ok, body} <- File.read(local_path),
         {:ok, %{status: status}} <- Req.put(req(), url: object_path(shard_id), body: body) do
      if status in 200..299, do: :ok, else: {:error, {:s3_put_status, status}}
    end
  end

  @impl true
  def flush(shard_id, local_path, expected_etag) do
    # If-Match the etag we last saw (or If-None-Match:* for a brand-new shard), so the PUT
    # only lands if the object hasn't changed under us. A 412 means a stealer flushed in the
    # window since our fence check → superseded, don't clobber (finding #15). Return the new
    # object etag for the next flush's fence.
    cond_headers =
      if expected_etag, do: [{"if-match", expected_etag}], else: [{"if-none-match", "*"}]

    with {:ok, body} <- File.read(local_path),
         {:ok, resp} <-
           Req.put(req(), url: object_path(shard_id), body: body, headers: cond_headers) do
      case resp.status do
        s when s in 200..299 -> {:ok, etag(resp.headers)}
        412 -> {:error, :superseded}
        s -> {:error, {:s3_put_status, s}}
      end
    end
  end

  @impl true
  def object_etag(shard_id) do
    case Req.head(req(), url: object_path(shard_id)) do
      {:ok, %{status: 200, headers: h}} -> {:ok, etag(h)}
      {:ok, %{status: 404}} -> {:ok, nil}
      {:ok, %{status: status}} -> {:error, {:s3_head_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Conditional GET with `If-None-Match: <etag>` — the warm-standby freshness check. A
  # `304` means the cached copy still equals the current object (no body transferred);
  # a `200` carries the fresh bytes; a `404` is a brand-new shard. A `nil` etag omits
  # the header, so it's an unconditional GET that captures the current etag.
  @impl true
  def pull_if_changed(shard_id, local_path, etag) do
    headers = if etag, do: [{"if-none-match", etag}], else: []

    case Req.get(req(), url: object_path(shard_id), headers: headers) do
      {:ok, %{status: 304}} ->
        {:ok, :unchanged}

      {:ok, %{status: 200, body: body, headers: h}} ->
        # Atomic so a warm-cache rewrite can't be read half-old/half-new by a promotion (#24).
        case Storage.atomic_write(local_path, body) do
          :ok -> {:ok, {:written, etag(h)}}
          err -> err
        end

      {:ok, %{status: 404}} ->
        {:ok, :absent}

      {:ok, %{status: status}} ->
        {:error, {:s3_get_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- versioned copies (blue/green migration) ---

  @impl true
  def retain(shard_id, version) do
    copy_object(db_key(shard_id), version_key(shard_id, version))
  end

  @impl true
  def restore(shard_id, version) do
    copy_object(version_key(shard_id, version), db_key(shard_id))
  end

  @impl true
  def drop_version(shard_id, version) do
    case Req.delete(req(), url: url_path(version_key(shard_id, version))) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Server-side copy: the destination is the request URL; the source is the
  # bucket-qualified key in x-amz-copy-source (S3/MinIO copy without download).
  defp copy_object(src_key, dst_key) do
    source = "/" <> fetch!(config(), :bucket) <> "/" <> src_key

    case Req.put(req(), url: url_path(dst_key), headers: [{"x-amz-copy-source", source}]) do
      {:ok, %{status: status}} when status in 200..299 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_copy_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  # --- leasing ---

  @impl true
  def acquire_lease(shard_id, owner, ttl_ms) do
    now = Storage.now_ms()

    # Optimistic single-request create: the common cold-open has no prior lock (a
    # clean idle/drain releases it), so try to create it directly with
    # `If-None-Match: *` and skip the read round-trip. This cuts the cold-open from
    # 3 S3 requests (GET lock + PUT lock + GET db) to 2. Only a `412` — a lock
    # already exists (crash-recovery / contention) — falls back to read-then-resolve,
    # which costs the same as before. The fence is unchanged: still create-only /
    # conditional, never an unconditional overwrite.
    case create_lock(shard_id, %{owner: owner, epoch: 1, expires_at_ms: now + ttl_ms}) do
      {:ok, _lease} = ok -> ok
      :exists -> acquire_existing(shard_id, owner, ttl_ms, now)
      {:error, _reason} = error -> error
    end
  end

  # A lock already exists: read it and reclaim (ours), refuse (live, theirs), or
  # steal (expired, theirs, epoch+1). A lock that vanished between the failed create
  # and this read (the holder just released) is retried as a fresh create.
  defp acquire_existing(shard_id, owner, ttl_ms, now) do
    case get_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: epoch}, etag} ->
        put_lock(shard_id, %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms},
          if_match: etag
        )

      # Someone else holds it — liveness is *their heartbeat* (with the lock's own TTL as the
      # fallback when they run no heartbeat — see owner_live?/3).
      {:ok, %{owner: other, epoch: epoch, expires_at_ms: lock_exp}, etag} ->
        case owner_live?(other, now, lock_exp) do
          :live ->
            {:error, {:held, other}}

          :dead ->
            put_lock(shard_id, %{owner: owner, epoch: epoch + 1, expires_at_ms: now + ttl_ms},
              if_match: etag
            )

          # Fail closed: don't steal a possibly-live owner on a heartbeat read blip.
          {:error, reason} ->
            {:error, {:transient_lookup, reason}}
        end

      :not_found ->
        case create_lock(shard_id, %{owner: owner, epoch: 1, expires_at_ms: now + ttl_ms}) do
          {:ok, _lease} = ok -> ok
          # Someone else created it in the gap — they hold it now.
          :exists -> {:error, {:held, :unknown}}
          {:error, _reason} = error -> error
        end

      # Fail closed: a transient lookup error must NOT fall through to an
      # unconditional write that would steal a live owner's lease.
      {:error, reason} ->
        {:error, {:transient_lookup, reason}}
    end
  end

  # Is `other` still alive? Its heartbeat object is the primary source of truth. Goes
  # through the Storage dispatch (resolves to this backend in prod) so a faulty
  # backend can inject a heartbeat-read failure for the fail-closed test. The
  # steal_margin absorbs inter-node clock skew (see Storage.steal_margin_ms/0): only
  # steal once the heartbeat is expired by more than the margin.
  defp owner_live?(other, now, lock_expires_at_ms) do
    margin = Storage.steal_margin_ms()

    case Storage.read_heartbeat(other) do
      {:ok, %{expires_at_ms: exp}} when now <= exp + margin -> :live
      {:ok, _stale} -> :dead
      # No heartbeat object at all (`heartbeat_server: false` legacy mode, or the owner's
      # heartbeat was cleared): fall back to the lock's OWN TTL for liveness (finding #11).
      # Without this, a live owner that renews its lock per-shard (the legacy fence) looks
      # instantly dead and any contender steals it. The heartbeat stays primary; this is the
      # pre-heartbeat lease-TTL fence, applied only when there is no heartbeat to consult.
      :not_found -> if now <= lock_expires_at_ms + margin, do: :live, else: :dead
      {:error, reason} -> {:error, reason}
    end
  end

  # Create the lock only if it does not exist (`If-None-Match: *`). `:exists` on a
  # 412 — no extra read, the caller decides what to do next.
  defp create_lock(shard_id, lease) do
    case Req.put(req(),
           url: lock_path(shard_id),
           body: Storage.encode_lease(lease),
           headers: [{"if-none-match", "*"}]
         ) do
      {:ok, %{status: status}} when status in 200..299 -> {:ok, lease}
      {:ok, %{status: 412}} -> :exists
      {:ok, %{status: status}} -> {:error, {:s3_put_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def renew_lease(shard_id, %{owner: owner, epoch: epoch}, ttl_ms) do
    now = Storage.now_ms()

    case get_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}, etag} ->
        case put_lock(shard_id, %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms},
               if_match: etag
             ) do
          {:ok, _} = ok -> ok
          {:error, {:held, _}} -> {:error, :superseded}
          {:error, :precondition_failed} -> {:error, :superseded}
          err -> err
        end

      {:ok, _other, _etag} ->
        {:error, :superseded}

      :not_found ->
        {:error, :superseded}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def release_lease(shard_id, %{owner: owner, epoch: epoch}) do
    case get_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}, etag} ->
        # Conditional delete (If-Match the object we just read): a stall between this read and
        # the delete could let a stealer write its own lock in the gap, and an unconditional
        # delete would remove THAT lock (finding #22). A 412 means the lock is no longer the
        # one we read — someone else's now — so leave it and no-op.
        case Req.delete(req(), url: lock_path(shard_id), headers: [{"if-match", etag}]) do
          {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
          {:ok, %{status: 412}} -> :ok
          {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
          {:error, reason} -> {:error, reason}
        end

      _ ->
        :ok
    end
  end

  @impl true
  def check_lease(shard_id, %{owner: owner, epoch: epoch}) do
    case get_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}, _etag} -> :ok
      {:ok, _other, _etag} -> {:error, :superseded}
      :not_found -> {:error, :superseded}
      {:error, reason} -> {:error, reason}
    end
  end

  # --- node heartbeat ---
  #
  # One object per node at `<prefix>heartbeats/<owner>`. Only this node writes its
  # own heartbeat, so the PUT is unconditional (no contention to fence).

  @impl true
  def renew_heartbeat(owner, ttl_ms) do
    hb = %{owner: owner, expires_at_ms: Storage.now_ms() + ttl_ms}

    case Req.put(req(), url: heartbeat_path(owner), body: Storage.encode_heartbeat(hb)) do
      {:ok, %{status: status}} when status in 200..299 -> {:ok, hb}
      {:ok, %{status: status}} -> {:error, {:s3_put_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def read_heartbeat(owner) do
    case Req.get(req(), url: heartbeat_path(owner)) do
      {:ok, %{status: 200, body: body}} ->
        case Storage.decode_heartbeat(body) do
          {:ok, hb} -> {:ok, hb}
          :error -> {:error, :corrupt_heartbeat}
        end

      {:ok, %{status: 404}} ->
        :not_found

      {:ok, %{status: status}} ->
        {:error, {:s3_get_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def clear_heartbeat(owner) do
    case Req.delete(req(), url: heartbeat_path(owner)) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_lock(shard_id) do
    case Req.get(req(), url: lock_path(shard_id)) do
      {:ok, %{status: 200, body: body, headers: headers}} ->
        case Storage.decode_lease(body) do
          {:ok, lease} -> {:ok, lease, etag(headers)}
          :error -> {:error, :corrupt_lock}
        end

      {:ok, %{status: 404}} ->
        :not_found

      {:ok, %{status: status}} ->
        {:error, {:s3_get_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Conditional update of an existing lock (`If-Match: <etag>`), for reclaim / steal
  # / renew. Lock *creation* goes through `create_lock/2`.
  defp put_lock(shard_id, lease, if_match: etag) do
    case Req.put(req(),
           url: lock_path(shard_id),
           body: Storage.encode_lease(lease),
           headers: [{"if-match", etag}]
         ) do
      {:ok, %{status: status}} when status in 200..299 ->
        {:ok, lease}

      # Another node wrote between our read and our conditional write.
      {:ok, %{status: 412}} ->
        case get_lock(shard_id) do
          {:ok, %{owner: owner}, _etag} -> {:error, {:held, owner}}
          _ -> {:error, :precondition_failed}
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_put_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp etag(headers) do
    case headers["etag"] || headers["ETag"] do
      [value | _] -> value
      value when is_binary(value) -> value
      _ -> nil
    end
  end

  defp object_path(shard_id), do: url_path(db_key(shard_id))
  defp lock_path(shard_id), do: url_path(prefix() <> shard_id <> ".lock")
  defp heartbeat_path(owner), do: url_path(prefix() <> "heartbeats/" <> owner)

  defp db_key(shard_id), do: prefix() <> shard_id <> ".db"

  defp version_key(shard_id, version),
    do: prefix() <> shard_id <> "@" <> to_string(version) <> ".db"

  # Virtual-hosted style carries the bucket in the host; path-style carries it in
  # the URL path (MinIO, R2).
  defp url_path(key) do
    if path_style?(), do: "/" <> fetch!(config(), :bucket) <> "/" <> key, else: "/" <> key
  end

  defp path_style?, do: config()[:path_style] == true

  defp req do
    config = config()

    Req.new(
      [
        base_url: base_url(config),
        aws_sigv4: [
          access_key_id: fetch!(config, :access_key_id),
          secret_access_key: fetch!(config, :secret_access_key),
          token: config[:token],
          service: :s3,
          region: region(config)
        ]
      ] ++ finch_opt() ++ req_plug_opt(config)
    )
  end

  # Test seam only: route requests through a `Plug`/`Req.Test` stub instead of the network,
  # so the conditional-lease semantics (If-Match / If-None-Match on PUT and DELETE) can be
  # asserted without a live S3. `nil` (production) leaves Req on its normal transport.
  defp req_plug_opt(config) do
    case config[:req_plug] do
      nil -> []
      plug -> [plug: plug]
    end
  end

  # Route through the dedicated, larger pool when it's running (the app
  # supervises it; the bench starts it in setup_s3). Fall back to Req's default
  # Finch if it isn't started, so Storage.S3 still works standalone (iex,
  # one-off scripts) without a hard dependency on the supervision tree.
  defp finch_opt do
    if Process.whereis(@finch), do: [finch: @finch], else: []
  end

  defp base_url(config) do
    cond do
      config[:endpoint] -> config[:endpoint]
      config[:path_style] == true -> "https://s3.#{region(config)}.amazonaws.com"
      true -> "https://#{fetch!(config, :bucket)}.s3.#{region(config)}.amazonaws.com"
    end
  end

  defp region(config), do: config[:region] || "us-east-1"
  defp prefix, do: config()[:prefix] || ""
  defp config, do: Application.get_env(:fathom, __MODULE__, [])

  defp fetch!(config, key) do
    config[key] ||
      raise "Fathom.Shard.Storage.S3 is missing #{inspect(key)} — set config :fathom, " <>
              "Fathom.Shard.Storage.S3, #{key}: ..."
  end
end
