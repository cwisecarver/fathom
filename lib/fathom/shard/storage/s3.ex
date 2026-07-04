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

  # Streaming transfer tuning (expert review #20): bodies are streamed in chunks in
  # both directions instead of materialized whole in the BEAM, and a single PUT is
  # refused past S3's 5 GB single-request ceiling (previously it failed opaquely).
  @stream_chunk 1024 * 1024
  @max_single_put 5 * 1024 * 1024 * 1024

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

  @doc """
  Boot-time self-test that the configured store **enforces** HTTP conditional writes
  (expert review #16). Every safety property in the system — lease mutual exclusion,
  the flush fence, conditional release — is load-bearing on the store returning 412
  for a failed `If-Match` / `If-None-Match` PUT. A store that ignores the headers
  returns 200, so two concurrent acquirers both "win" and every fenced flush
  "succeeds": silent, error-free split-brain with zero signal until data is lost
  (AWS S3 only added If-Match-on-PUT in late 2024; S3-compatibles vary). Raises —
  refusing the boot — if the store doesn't enforce them, or if the probe can't
  reach the store at all (fail closed: an unverifiable fence is not a fence).
  """
  @spec verify_conditional_writes!() :: :ok
  def verify_conditional_writes! do
    key = prefix() <> "fence-probe/" <> to_string(node())

    # Seed the probe object unconditionally.
    probe_status!(key, [], 200..299, "probe PUT")

    # A PUT fenced with a wrong etag MUST be refused.
    probe_status!(key, [{"if-match", ~s("fathom-bogus-etag")}], [412], "If-Match enforcement")

    # A create-only PUT against an existing object MUST be refused.
    probe_status!(key, [{"if-none-match", "*"}], [412], "If-None-Match enforcement")

    _ = Req.delete(req(), url: url_path(key))
    :ok
  end

  defp probe_status!(key, headers, expected, label) do
    case Req.put(req(), url: url_path(key), body: "fathom-fence-probe", headers: headers) do
      {:ok, %{status: s}} ->
        if s in expected do
          :ok
        else
          raise "shard storage fence self-test failed (#{label}): expected " <>
                  "#{inspect(expected)}, got #{s}. This store does not enforce the " <>
                  "conditional writes the single-writer lease and flush fence depend on — " <>
                  "refusing to boot (expert review #16)."
        end

      {:error, reason} ->
        raise "shard storage fence self-test unreachable (#{label}): #{inspect(reason)}. " <>
                "Refusing to boot with an unverified fence (expert review #16)."
    end
  end

  @impl true
  def pull(shard_id, local_path) do
    # Streamed to disk, never buffered whole in the BEAM (expert review #20): the
    # correlated moments — warming bursts, failover, N concurrent pulls — used to
    # hold (transfers x shard size) resident. The body is MD5-verified against an
    # MD5-shaped etag as it streams (expert review #37), then fsynced and renamed
    # into place atomically (#24/#17).
    case download(object_path(shard_id), local_path) do
      {:ok, etag} -> {:ok, etag}
      {:error, _} = error -> error
      :absent -> {:ok, nil}
    end
  end

  @impl true
  def flush(shard_id, local_path) do
    with {:ok, size, md5} <- stat_and_md5(local_path),
         {:ok, %{status: status}} <-
           Req.put(req(),
             url: object_path(shard_id),
             body: File.stream!(local_path, @stream_chunk),
             headers: [
               {"content-length", Integer.to_string(size)},
               {"content-md5", md5}
             ]
           ) do
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

    with {:ok, size, md5} <- stat_and_md5(local_path),
         {:ok, resp} <-
           Req.put(req(),
             url: object_path(shard_id),
             body: File.stream!(local_path, @stream_chunk),
             headers: [
               {"content-length", Integer.to_string(size)},
               {"content-md5", md5} | cond_headers
             ]
           ) do
      case resp.status do
        s when s in 200..299 -> {:ok, etag(resp.headers)}
        412 -> {:error, :superseded}
        s -> {:error, {:s3_put_status, s}}
      end
    end
  end

  # Steal-time data-object etag invalidation (expert review #3): a conditional
  # server-side self-copy — same bytes, new etag. S3 requires the REPLACE metadata
  # directive for a self-copy. 404 (no object yet — a brand-new shard was stolen) is
  # fine: there is nothing a zombie flush could clobber that a nil-etag fence
  # doesn't already refuse. A 412 means the object changed between our read and the
  # copy — re-running the touch once covers the benign race.
  defp touch_object(shard_id, attempt \\ 1) do
    key = db_key(shard_id)

    case object_etag(shard_id) do
      {:ok, nil} ->
        :ok

      {:ok, etag} ->
        source = "/" <> fetch!(config(), :bucket) <> "/" <> key

        case Req.put(req(),
               url: url_path(key),
               headers: [
                 {"x-amz-copy-source", source},
                 {"x-amz-metadata-directive", "REPLACE"},
                 {"x-amz-copy-source-if-match", etag}
               ]
             ) do
          {:ok, %{status: s}} when s in 200..299 -> :ok
          {:ok, %{status: 412}} when attempt < 2 -> touch_object(shard_id, attempt + 1)
          {:ok, %{status: s}} -> {:error, {:s3_touch_status, s}}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Integrity (expert review #37): TLS/TCP catch wire corruption, but nothing caught
  # corruption introduced BEFORE the socket — a torn read off a bad disk, a buggy
  # proxy, or an S3-compatible store bug. Content-MD5 on every data PUT makes the
  # store reject a torn upload; downloads verify the streamed body's MD5 against the
  # returned etag when it's the MD5-shaped single-part form (32 hex chars —
  # multipart/encrypted etags are not MD5s and are skipped).
  #
  # Uploads hash the file in a chunked pass (Content-MD5 is a header, so it must be
  # known before the body streams; the page cache makes the second pass cheap) and
  # refuse a single PUT past the 5 GB ceiling (expert review #20).
  defp stat_and_md5(path) do
    with {:ok, %{size: size}} <- File.stat(path) do
      if size > Application.get_env(:fathom, :s3_max_single_put, @max_single_put) do
        {:error, {:object_too_large, size}}
      else
        md5 =
          path
          |> File.stream!(@stream_chunk)
          |> Enum.reduce(:crypto.hash_init(:md5), &:crypto.hash_update(&2, &1))
          |> :crypto.hash_final()
          |> Base.encode64()

        {:ok, size, md5}
      end
    end
  rescue
    e in File.Error -> {:error, e.reason}
  end

  defp verify_md5(_digest, nil), do: :ok

  defp verify_md5(digest, etag) do
    plain = String.trim(etag, ~s("))

    if md5_etag?(plain) and
         Base.encode16(digest, case: :lower) != String.downcase(plain) do
      {:error, :checksum_mismatch}
    else
      :ok
    end
  end

  defp md5_etag?(plain), do: plain =~ ~r/\A[0-9a-fA-F]{32}\z/

  # Streamed GET straight to a sibling temp of `local_path` (expert review #20):
  # chunks are written to disk and MD5-hashed as they arrive — the whole object is
  # never resident in the BEAM. On 200 the digest is verified (#37) and the temp is
  # fsynced + renamed into place (#24/#17). Returns {:ok, etag} | :absent |
  # :unchanged (with allow_304) | {:error, reason}.
  # Bounded transient retries, each with a FRESH temp + fd (expert review #1). The
  # download MUST run with `retry: false`: Req's default `:safe_transient` retry
  # re-runs the request against the SAME still-open fd on a mid-body transport error,
  # appending the retry's body after the first attempt's partial bytes — and because
  # the streamed-chunk MD5 restarts per Response, the digest covers only the final
  # attempt and CERTIFIES the `partial₁ ++ full₂` corruption (defeating #37's integrity
  # check on exactly the tear it exists to catch). We instead retry the whole download
  # ourselves so every attempt writes a clean temp.
  @download_attempts 3

  defp download(url, local_path, headers \\ [], opts \\ []) do
    File.mkdir_p!(Path.dirname(local_path))
    do_download(url, local_path, headers, opts, @download_attempts)
  end

  defp do_download(url, local_path, headers, opts, attempts_left) do
    tmp = "#{local_path}.dl.#{System.unique_integer([:positive])}"
    {:ok, fd} = File.open(tmp, [:write, :raw, :binary])

    into = fn {:data, chunk}, {req, resp} ->
      if resp.status == 200 do
        :ok = IO.binwrite(fd, chunk)
        md5 = resp.private[:fathom_md5] || :crypto.hash_init(:md5)
        resp = Req.Response.put_private(resp, :fathom_md5, :crypto.hash_update(md5, chunk))
        {:cont, {req, resp}}
      else
        {:cont, {req, resp}}
      end
    end

    result = Req.get(req(), url: url, headers: headers, into: into, retry: false)
    :ok = File.close(fd)

    case result do
      {:ok, %{status: 200} = resp} ->
        digest = :crypto.hash_final(resp.private[:fathom_md5] || :crypto.hash_init(:md5))

        case verify_md5(digest, etag(resp.headers)) do
          :ok ->
            case Storage.promote_temp(tmp, local_path) do
              :ok -> {:ok, etag(resp.headers)}
              {:error, _} = error -> file_error(tmp, error)
            end

          # A torn transfer produced bytes that don't match the object's etag — a
          # transient corruption, not a permanent one; retry the whole download with a
          # fresh temp rather than failing the pull outright (expert review #1).
          {:error, :checksum_mismatch} = error ->
            File.rm(tmp)
            retry_or(error, url, local_path, headers, opts, attempts_left)
        end

      {:ok, %{status: 304}} ->
        File.rm(tmp)
        if opts[:allow_304], do: :unchanged, else: {:error, {:s3_get_status, 304}}

      {:ok, %{status: 404}} ->
        File.rm(tmp)
        :absent

      {:ok, %{status: status}} ->
        File.rm(tmp)
        {:error, {:s3_get_status, status}}

      # A transport error (possibly mid-body, so the temp may hold partial bytes):
      # drop the partial temp and retry the whole download with a fresh one.
      {:error, reason} ->
        File.rm(tmp)
        retry_or({:error, reason}, url, local_path, headers, opts, attempts_left)
    end
  end

  defp retry_or(error, url, local_path, headers, opts, attempts_left) do
    if attempts_left > 1 do
      do_download(url, local_path, headers, opts, attempts_left - 1)
    else
      error
    end
  end

  defp file_error(tmp, error) do
    File.rm(tmp)
    error
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

    # Streamed + verified like pull/2 (expert reviews #20/#37); a 304 transfers no body.
    case download(object_path(shard_id), local_path, headers, allow_304: true) do
      {:ok, new_etag} -> {:ok, {:written, new_etag}}
      :unchanged -> {:ok, :unchanged}
      :absent -> {:ok, :absent}
      {:error, _} = error -> error
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
            case put_lock(
                   shard_id,
                   %{owner: owner, epoch: epoch + 1, expires_at_ms: now + ttl_ms},
                   if_match: etag
                 ) do
              {:ok, lease} ->
                # Expert review #3: invalidate the DATA object's etag at steal time,
                # BEFORE the new owner pulls/serves. The old owner may be stalled
                # inside a fenced flush it already passed the fence for (whole-VM
                # pause) — the steal never touched the data object, so its late
                # `If-Match` PUT would still land AFTER our pull, leaving us serving
                # a copy missing its final acknowledged writes and self-fencing away
                # our own on the first flush. A conditional self-copy changes the
                # etag without moving bytes, so the zombie's PUT deterministically
                # 412s and IT self-fences instead.
                case touch_object(shard_id) do
                  :ok ->
                    {:ok, Map.put(lease, :took_over, true)}

                  # Fail closed: an un-fenced steal is not a steal. Release our
                  # fresh lock claim implicitly by not serving (the caller refuses
                  # the open and retries).
                  {:error, reason} ->
                    {:error, {:transient_lookup, {:touch_failed, reason}}}
                end

              other ->
                other
            end

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
      # Verify the heartbeat body's owner matches (expert review round-2 #3, defense in
      # depth): with the per-owner key this always holds, but never trust a mismatched
      # body to declare `other` live.
      {:ok, %{owner: ^other, expires_at_ms: exp}} ->
        if now <= exp + margin, do: :live, else: :dead

      # A heartbeat object that isn't `other`'s — treat as no signal and fall back to the
      # lock's own TTL, same as :not_found.
      {:ok, _mismatch} ->
        if now <= lock_expires_at_ms + margin, do: :live, else: :dead

      # No heartbeat object at all (`heartbeat_server: false` legacy mode, or the owner's
      # heartbeat was cleared): fall back to the lock's OWN TTL for liveness (finding #11).
      # Without this, a live owner that renews its lock per-shard (the legacy fence) looks
      # instantly dead and any contender steals it. The heartbeat stays primary; this is the
      # pre-heartbeat lease-TTL fence, applied only when there is no heartbeat to consult.
      :not_found ->
        if now <= lock_expires_at_ms + margin, do: :live, else: :dead

      {:error, reason} ->
        {:error, reason}
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
  # Percent-encode the owner (expert review round-2 #3): the incarnation-qualified owner
  # is `node()#<nonce>`, and Req parses the URL with URI.parse — a raw `#` is a fragment
  # delimiter, never transmitted, so every incarnation of a node name collided on
  # `heartbeats/<node>` with the nonce silently stripped (voiding #6's boot-scoped
  # identity on S3 while the Local double, using `#` as a legal filename char, passed).
  # encode_www_form makes the whole owner one safe path segment.
  defp heartbeat_path(owner),
    do: url_path(prefix() <> "heartbeats/" <> URI.encode_www_form(owner))

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
