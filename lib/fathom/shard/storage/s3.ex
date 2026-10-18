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

  require Logger

  alias Fathom.Shard.Storage.S3.ListPage
  alias Fathom.Shard.Storage.S3.UploadsPage

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Codec

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
  # Under S3's ~20s idle close, so fathom is always the side that retires a pooled connection
  # (expert review 2026-07-24 #14).
  @default_conn_max_idle_time :timer.seconds(15)
  @default_connect_timeout 3_000

  # Streaming transfer tuning (expert review #20): bodies are streamed in chunks in
  # both directions instead of materialized whole in the BEAM, and a single PUT is
  # refused past S3's 5 GB single-request ceiling (previously it failed opaquely).
  @stream_chunk 1024 * 1024
  @max_single_put 5 * 1024 * 1024 * 1024

  # Object metadata carrying the body's MD5 as lowercase base16 hex, independent of the etag
  # form (expert review 2026-07-14 #17). A steal-time touch moves the etag to invalidate a zombie's
  # If-Match — to a multipart form, or to the MD5 of re-nonced bytes that are no longer the bytes
  # a flush PUT — which defeats the etag-MD5 download check for the new owner's first pull; this
  # metadata survives the touch. See verify_integrity/3.
  @md5_meta "x-amz-meta-fathom-md5"

  # How much of the shard's history the object's bytes contain (Phase 2 A2 promote-on-open).
  # User metadata on the SAME PUT — S3 charges per request and ingress is free, so a stamp that
  # cost its own request would make every durability flush more expensive to buy a failover-only
  # benefit. Absent on any object written before stamping existed, which readers must treat as
  # "unknown" (⇒ never overridable), never as "empty".
  @pos_meta "x-amz-meta-fathom-pos"

  # Marks a placeholder object created by the brand-new steal path (round-2 #7). Lives up here with
  # the other metadata keys, and ABOVE `carry_meta/1`, because that function's deny list needs it —
  # `--warnings-as-errors` enforces definition-before-use for attributes.
  @sentinel_meta "x-amz-meta-fathom-sentinel"

  # The shard's LINEAGE counter (expert review 2026-08-20 #8) — a per-shard integer that only ever
  # increases, one step per ownership. Deliberately its own key rather than being read back out of
  # `@pos_meta`: the position stamp is `nil` on the ordinary graceful-drop path (an empty WAL both
  # before and after the flush is genuinely ambiguous), so a lineage derived from it would be
  # missing exactly where it is needed.
  @lineage_meta "x-amz-meta-fathom-lineage"

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
    * `conn_max_idle_time` (default #{@default_conn_max_idle_time}ms) — retire an idle pooled
      connection before S3 does, so a checkout never races the server's FIN.
    * `connect_timeout` (default #{@default_connect_timeout}ms) — TCP connect budget.

  Total connections to the S3 host = `pool_size * pool_count`. Req's default
  ~50-conn pool caps warming throughput (many shards pulled from S3 at once on
  startup/failover); a larger pool lifts that ceiling, and raising `pool_count`
  spreads concurrent checkouts across more pool processes to cut the checkout
  contention one big pool hits under a bursty fan-out. Idle pools hold no
  connections, so it costs nothing when the backend is `Local` or S3 is idle.
  """
  @spec finch_child_spec() :: {module(), keyword()}
  def finch_child_spec do
    {Finch,
     name: @finch,
     pools: %{
       default: [
         size: pool_size(),
         count: pool_count(),
         # Expert review 2026-07-24 #14. Finch defaults this to :infinity, so a pooled connection
         # was never proactively retired and the SERVER became the closing side. A checkout that
         # lands ahead of the server's FIN in the pool's mailbox then hands out a dead socket.
         #
         # That failure is asymmetric here, which is why it matters: Req's default :safe_transient
         # retries GET/HEAD only, `download/4` deliberately rolls its own retries, and the flush PUT
         # is retried by NOBODY — so a raced :closed aborts the idle drop, the coordinator keeps the
         # local file AND the lease, and the tenant's RPO window extends by a whole backoff.
         #
         # 15s is comfortably under S3's ~20s idle close, making fathom always the closing side —
         # the same discipline as the LB's `keepalive_timeout` vs Bandit's idle close.
         conn_max_idle_time: conn_max_idle_time(),
         # A hung connect otherwise sits for Finch's default before anything reacts.
         conn_opts: [transport_opts: [timeout: connect_timeout()]]
       ]
     }}
  end

  defp pool_size, do: config()[:pool_size] || @default_pool_size
  defp pool_count, do: config()[:pool_count] || @default_pool_count
  defp conn_max_idle_time, do: config()[:conn_max_idle_time] || @default_conn_max_idle_time
  defp connect_timeout, do: config()[:connect_timeout] || @default_connect_timeout

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

    # A fenced PUT against a MISSING key must also be refused: AWS answers 404 NoSuchKey, others
    # 412. `flush/6` maps both to :superseded (expert review 2026-10-12 #22); a store answering 2xx
    # here would let a fenced flush resurrect a purged object.
    probe_status!(
      key <> ".absent",
      [{"if-match", ~s("fathom-bogus-etag")}],
      [404, 412],
      "If-Match on a missing key"
    )

    # Round-2 #4: the steal fence is only real if a touch produces a GENUINELY new
    # etag — on MD5-etag stores a plain self-copy does NOT rotate (same bytes, same
    # MD5). And a NEW etag is not enough: it must be one the key has never had, or two
    # steals bring a zombie's etag back (expert review 2026-10-12 N1). Two consecutive
    # touches of the identical probe bytes must yield three distinct etags.
    probe_rotation!(key)

    # The large-object touch's own primitives (expert review 2026-10-13 #3): UploadPartCopy must
    # honour a source If-Match and a source range, and CompleteMultipartUpload must honour If-Match.
    # A store that ignores any of them makes the fence a silent no-op for exactly the largest shards.
    probe_multipart_fence!(key)

    # The lock-release fence (`release_lease/2`) is a conditional DELETE, and the whole lock-leak
    # fix (`resolve_412_release`, finding #22) rests on the store answering 412 to a stale-etag
    # DELETE. Conditional DELETE support DIFFERS across AWS general-purpose/directory buckets, R2,
    # Tigris and MinIO: a store that IGNORES the precondition lets a zombie's release delete a LIVE
    # owner's lock (two writers, silent clobber), and one that REJECTS every conditional DELETE
    # leaks every lock (no failover, migration or rebalance can ever hand off a shard). Nothing else
    # in this self-test exercises DELETE, so probe both directions and refuse boot on either failure
    # (expert review 2026-09-05 #8). NOTE: this is what makes the boot answer "does the deployed
    # store support conditional DELETE?", which the module was never verified against a real bucket.
    #
    # TEST-STORE ESCAPE HATCH (`:allow_weak_conditional_delete`, env
    # SHARD_STORAGE_ALLOW_WEAK_CONDITIONAL_DELETE, DEFAULT OFF — prod stays strict). A store that does
    # not implement conditional DELETE AT ALL — MinIO returns 204, ignoring `If-Match`, which also
    # DELETES the probe object so the matching-etag probe below then fails "object vanished" — cannot
    # pass this section, so a chaos rig built on MinIO could never boot after #8. When the flag is on,
    # the WHOLE conditional-DELETE probe is skipped with a loud warning: the lock-release fence (#22)
    # is genuinely NOT enforced by such a store, which is acceptable ONLY in a test rig and NEVER in
    # production. Scenarios that do not exercise it (replication lineage #4/#12, commit-ack ownership
    # #5, clock skew #16) are unaffected.
    if allow_weak_conditional_delete?() do
      Logger.warning(
        "shard storage: SKIPPING the conditional-DELETE fence self-test — " <>
          ":allow_weak_conditional_delete is on. The lock-release fence (#22) is NOT enforced by " <>
          "this store (e.g. MinIO answers a conditional DELETE 204, not 412). Acceptable ONLY for a " <>
          "test rig; NEVER set this in production."
      )

      # The store ignores the precondition, so clean the probe object up unconditionally.
      _ = Req.delete(req(), url: url_path(key))
    else
      probe_delete_status!(
        key,
        [{"if-match", ~s("fathom-bogus-etag")}],
        [412],
        "conditional DELETE enforcement (a stale-etag release MUST be refused 412)"
      )

      case head_object(key) do
        {:ok, real_etag, _sentinel?, _meta} when is_binary(real_etag) ->
          probe_delete_status!(
            key,
            [{"if-match", real_etag}],
            200..299,
            "conditional DELETE with the matching etag MUST succeed (else every lock leaks)"
          )

        other ->
          raise "shard storage fence self-test failed: the probe object vanished before the " <>
                  "conditional-DELETE probe (#{inspect(other)}) — refusing to boot (expert review #8)."
      end

      # The valid probe above already removed the object; this is a best-effort belt in case a store
      # answered the probe 2xx without actually deleting.
      _ = Req.delete(req(), url: url_path(key))
    end

    :ok
  end

  # Test-store escape hatch for the conditional-DELETE probe above (expert review 2026-09-05 #8).
  # DEFAULT OFF, so production refuses to boot on a store without conditional DELETE exactly as
  # before; only a test rig on a limited store (MinIO) turns it on.
  defp allow_weak_conditional_delete?,
    do: Application.get_env(:fathom, :allow_weak_conditional_delete, false) == true

  # The probe object is 18 raw bytes, so this exercises the small-object touch twice: the first
  # re-encodes `:none` -> zstd + nonce, the second replaces the nonce. Two GET+PUTs of a few dozen
  # bytes. The large-object split touch is not run (that would mean uploading >= 6 MiB per boot);
  # its primitives are probed by `probe_multipart_fence!/1` with a single 4-byte part instead.
  defp probe_rotation!(key) do
    # The probe deliberately keeps its own HEAD-based verification (it is a boot-time
    # self-test of the STORE, so it must not trust the write response's claimed etag the
    # way the steal path now does — review 2026-07-23 #13).
    with {:ok, e0, _, carry0, size0} when is_binary(e0) <- head_sized(key),
         {:ok, _claimed} <- rotate_etag(key, e0, carry0, size0),
         {:ok, e1, _, carry1, size1} when is_binary(e1) <- head_sized(key),
         {:ok, _claimed} <- rotate_etag(key, e1, carry1, size1),
         {:ok, e2} when is_binary(e2) <- head_etag(key),
         [_, _, _] <- Enum.uniq([e0, e1, e2]) do
      :ok
    else
      other ->
        raise "shard storage fence self-test failed: two steal-time touches did not rotate " <>
                "the object etag to two NEW etags (#{inspect(other)}). On this store the " <>
                "zombie-flush fence would be a silent no-op — refusing to boot " <>
                "(expert review round-2 #4, 2026-10-12 N1)."
    end
  end

  # Exercises the split touch's primitives WITHOUT a 6 MiB upload: only a non-last part must reach
  # 5 MiB, so a single 4-byte part copy completes legitimately. Uses its own key so the rotation
  # probe's object is untouched; always cleaned up (object and any open upload).
  defp probe_multipart_fence!(probe_key) do
    key = probe_key <> ".mp"
    bogus = ~s("fathom-bogus-etag")
    source = "/" <> fetch!(config(), :bucket) <> "/" <> key
    probe_status!(key, [], 200..299, "multipart probe seed")

    try do
      with {:ok, etag} when is_binary(etag) <- head_etag(key),
           {:ok, upload_id} <- create_multipart(key, []) do
        try do
          multipart_fence_checks!(key, upload_id, source, etag, bogus)
        after
          abort_multipart(key, upload_id)
        end
      else
        other ->
          raise "shard storage fence self-test failed (multipart probe setup): " <>
                  "#{inspect(other)} — refusing to boot (expert review 2026-10-13 #3)."
      end
    after
      _ = Req.delete(req(), url: url_path(key))
    end

    :ok
  end

  defp multipart_fence_checks!(key, upload_id, source, etag, bogus) do
    unless match?(
             {:error, :touch_precondition},
             upload_part_copy(key, upload_id, source, bogus, 1, {0, 3})
           ) do
      raise "shard storage fence self-test failed (UploadPartCopy x-amz-copy-source-if-match " <>
              "enforcement): a bogus precondition was not refused. The large-object steal-touch " <>
              "fence would be a silent no-op — refusing to boot (expert review 2026-10-13 #3)."
    end

    part_etag =
      case upload_part_copy(key, upload_id, source, etag, 1, {0, 3}) do
        {:ok, part_etag} ->
          part_etag

        other ->
          raise "shard storage fence self-test failed (UploadPartCopy with a valid source " <>
                  "range and etag): #{inspect(other)} — refusing to boot (expert review 2026-10-13 #3)."
      end

    unless match?(
             {:error, :touch_precondition},
             complete_multipart(key, upload_id, [{1, part_etag}], bogus)
           ) do
      raise "shard storage fence self-test failed (CompleteMultipartUpload If-Match " <>
              "enforcement): a bogus precondition was not refused. A zombie flush landing " <>
              "mid-touch would be silently overwritten — refusing to boot " <>
              "(expert review 2026-10-13 #3)."
    end

    with {:ok, _} <- complete_multipart(key, upload_id, [{1, part_etag}], etag),
         {:ok, _, _, _, 4} <- head_sized(key) do
      :ok
    else
      other ->
        raise "shard storage fence self-test failed (a valid ranged multipart self-copy must " <>
                "complete to the 4 bytes copied, honouring x-amz-copy-source-range): " <>
                "#{inspect(other)} — refusing to boot (expert review 2026-10-13 #3)."
    end
  end

  defp head_etag(key) do
    case Req.head(req(), url: url_path(key)) do
      {:ok, %{status: 200, headers: h}} -> {:ok, etag(h)}
      {:ok, %{status: s}} -> {:error, {:s3_head_status, s}}
      {:error, reason} -> {:error, reason}
    end
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

  # DELETE sibling of probe_status!/4 — the release fence is a conditional DELETE, not a PUT
  # (expert review 2026-09-05 #8).
  defp probe_delete_status!(key, headers, expected, label) do
    case Req.delete(req(), url: url_path(key), headers: headers) do
      {:ok, %{status: s}} ->
        if s in expected do
          :ok
        else
          raise "shard storage fence self-test failed (#{label}): expected " <>
                  "#{inspect(expected)}, got #{s}. This store does not enforce the conditional " <>
                  "DELETE the lock-release fence depends on — refusing to boot (expert review #8)."
        end

      {:error, reason} ->
        raise "shard storage fence self-test unreachable (#{label}): #{inspect(reason)}. " <>
                "Refusing to boot with an unverified fence (expert review #8)."
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
      {:ok, etag} ->
        {:ok, etag}

      # A steal-time brand-new sentinel (round-2 #7): the shard is brand-new — no local file
      # is written — but the first flush must still fence with the SENTINEL's etag (If-Match
      # replaces it; the zombie's If-None-Match:* create then 412s).
      #
      # `{:absent, etag}`, NOT `{:ok, etag}` (expert review 2026-08-01 #24). Collapsing it to
      # `{:ok, _}` told every caller "bytes are at local_path" when none were, and the
      # pull-then-open consumers duly opened the missing path — which CREATES an empty
      # database. The fence etag is preserved, so round-2 #7's property is unchanged; only the
      # "were bytes written" answer becomes honest.
      {:sentinel, etag} ->
        {:absent, etag}

      {:error, _} = error ->
        error

      :absent ->
        {:absent, nil}
    end
  end

  @impl true
  def flush(shard_id, local_path) do
    with_body(local_path, fn body_path, size, md5, md5_hex, enc_headers ->
      with {:ok, %{status: status}} <-
             Req.put(req(),
               url: object_path(shard_id),
               body: File.stream!(body_path, @stream_chunk),
               headers:
                 [
                   {"content-length", Integer.to_string(size)},
                   {"content-md5", md5},
                   # Etag-form-independent integrity hash (expert review 2026-07-14 #17). Always
                   # the UNCOMPRESSED database's hash, so an object's identity doesn't depend on
                   # how it was stored (#38).
                   {@md5_meta, md5_hex}
                 ] ++ enc_headers
             ) do
        if status in 200..299, do: :ok, else: {:error, {:s3_put_status, status}}
      end
    end)
  end

  @impl true
  def flush(shard_id, local_path, expected_etag, position \\ nil, lineage \\ nil, opts \\ []) do
    # If-Match the etag we last saw (or If-None-Match:* for a brand-new shard), so the PUT
    # only lands if the object hasn't changed under us. A 412 means a stealer flushed in the
    # window since our fence check → superseded, don't clobber (finding #15). Return the new
    # object etag for the next flush's fence.
    cond_headers =
      if expected_etag, do: [{"if-match", expected_etag}], else: [{"if-none-match", "*"}]

    # Resolved BEFORE the body is prepared, so the HEAD (when one is needed at all) does not sit
    # inside the streaming upload's callback where a reader would miss it.
    {header, carried} = carried_lineage_header(shard_id, expected_etag, lineage)

    with_body(local_path, fn body_path, size, md5, md5_hex, enc_headers ->
      # The caller's chance to record "I am about to PUT bytes hashing to md5_hex" durably, BEFORE
      # the object can change (expert review 2026-10-10 #6). A caller that cannot record it must
      # not stop the flush — the recording only buys a crash-recovery adoption.
      with :ok <- run_pre_put(opts, md5_hex),
           {:ok, resp} <-
             Req.put(req(),
               url: object_path(shard_id),
               body: File.stream!(body_path, @stream_chunk),
               headers:
                 [
                   {"content-length", Integer.to_string(size)},
                   {"content-md5", md5},
                   # Etag-form-independent integrity hash (expert review 2026-07-14 #17), over the
                   # UNCOMPRESSED bytes (#38).
                   {@md5_meta, md5_hex} | cond_headers
                 ] ++
                   enc_headers ++
                   pos_meta_header(position) ++ header
             ) do
        case resp.status do
          # REPORTS WHAT IT CARRIED (expert review 2026-08-26 #33). The caller caches it and hands
          # it back as `{:carried, _}` next time, so only the FIRST flush of a coordinator's life
          # pays the HEAD above.
          s when s in 200..299 -> {:ok, etag(resp.headers), carried}
          412 -> {:error, :superseded}
          # A FENCED (If-Match) PUT to a key that no longer exists: AWS answers 404 NoSuchKey
          # rather than 412 (expert review 2026-10-12 #22). The fence did its job — the object we
          # fenced on is gone (purge/delete) — so it is the same self-fence verdict as 412; the
          # coordinator's `reconcile_superseded` re-checks the lock and either resyncs to an
          # absent object (lock still ours) or self-fences. Not for the create-only
          # (If-None-Match) form, where a 404 is a real error (missing bucket).
          404 when expected_etag != nil -> {:error, :superseded}
          s -> {:error, {:s3_put_status, s}}
        end
      end
    end)
  end

  # The `:pre_put` flush option. The fun is awaited (it is a durable local write); a result other
  # than `:ok` is logged and ignored, because losing the intent only re-opens the old, quarantining
  # behaviour for a crash in the gap — it must never fail a durability flush.
  defp run_pre_put(opts, md5_hex) do
    case Keyword.get(opts, :pre_put) do
      nil ->
        :ok

      fun ->
        case fun.(md5_hex) do
          :ok -> :ok
          other -> Logger.warning("flush pre_put hook did not record: #{inspect(other)}")
        end

        :ok
    end
  end

  # Prepares the PUT body for `local_path`, honouring `:shard_object_encoding` (#38), and hands
  # the callback everything the upload needs. Cleans up the compressed temp on every path.
  #
  # The two hashes are deliberately over DIFFERENT bytes:
  #   * `content-md5` (the wire body)  -> the bytes actually being PUT, compressed or not.
  #   * `@md5_meta`   (the identity)   -> ALWAYS the uncompressed database. That is what makes
  #     `verify_integrity/3` work unchanged and keeps an object's identity independent of how it
  #     happened to be stored.
  #
  # The 5 GiB single-PUT ceiling (#37) is checked against the body actually uploaded, which is the
  # compressed size when encoding is on — compression genuinely raises that ceiling.
  defp with_body(local_path, fun) do
    case Codec.encoding() do
      :none ->
        with {:ok, size, md5, md5_hex} <- stat_and_md5(local_path) do
          fun.(local_path, size, md5, md5_hex, [])
        end

      encoding ->
        # The plaintext hash must NOT go through stat_and_md5: that applies the single-PUT
        # ceiling, and the plaintext is not what gets PUT.
        #
        # AND THE COMPRESSED BODY IS PROVED TO DECODE TO THAT HASH BEFORE IT IS PUT (expert review
        # 2026-10-08 #15). Uncompressed, `content-md5` and `@md5_meta` are one digest over one byte
        # string, so the store's BadDigest check verified the object's identity end to end.
        # Compressed, they are over different reads of the file — the hash read, then the compress
        # read — and nothing tied the uploaded body to the hash it is labelled with. A codec defect
        # (a9ce967 records the `{:continue, remainder, out}` case losing bytes in development) or a
        # file that changed between the two reads produced an object every pull then refuses as a
        # checksum mismatch, and on an idle drop or drain the local copy was deleted on the PUT's
        # success. zstd decodes several times faster than it encodes, so this costs a fraction of the
        # compress it guards.
        #
        # ONE READ OF THE PLAINTEXT (perf review 2026-10-14 #6). `compress_to_temp_hashed/2` hashes
        # the plaintext as it feeds the compressor and the body as it writes it, so the old
        # hash-read and the body re-hash are gone. What the old hash-then-compress gap (the
        # `between_hash_and_compress` seam) caught was a file that changed between two reads; with
        # one read the label cannot disagree with the body that way, so that check is replaced by
        # stat-before/stat-after around the single read (the `stat_and_md5/1` guard, which also
        # catches a torn read the old two-read compare would have caught only by luck of timing).
        # THE DECODE PROOF IS INLINE (expert review 2026-10-17 #20). `compress_to_temp_hashed/2` feeds
        # each compressed piece to a decoder as it writes it, and refuses the body if the decoded MD5
        # is not the plaintext MD5. So a body that would not decode to its label never reaches this
        # `with`, and the temp file is not re-read to prove it.
        with {:ok, %{size: psize, mtime: pmtime}} <- File.stat(local_path),
             {:ok, tmp, size, md5, plain_hex} <-
               Codec.compress_to_temp_hashed(local_path, encoding) do
          try do
            with :ok <- after_compress(local_path),
                 :ok <- source_unchanged(local_path, psize, pmtime),
                 :ok <- check_single_put(size) do
              fun.(tmp, size, Base.encode64(md5), plain_hex, Codec.upload_headers(encoding))
            end
          after
            File.rm(tmp)
          end
        end
    end
  end

  # Test seam: runs after the single compress read, before the source-changed check (the race the
  # check exists to catch). Unset in every real config — one env read per compressed flush.
  defp after_compress(path) do
    case Application.get_env(:fathom, :s3_after_compress_hook) do
      nil -> :ok
      hook -> hook.(path)
    end
  end

  defp source_unchanged(path, size, mtime) do
    case File.stat(path) do
      {:ok, %{size: ^size, mtime: ^mtime}} -> :ok
      {:ok, _changed} -> {:error, {:source_changed_during_upload, path}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_single_put(size) do
    if size > Application.get_env(:fathom, :s3_max_single_put, @max_single_put),
      do: {:error, {:object_too_large, size}},
      else: :ok
  end

  # Steal-time data-object etag invalidation (expert review #3, fixed for real
  # stores by round-2 #4): the fence needs a GENUINELY new etag, but on an
  # MD5-etag store (every non-multipart, non-KMS object — exactly what fathom's
  # single-PUT uploads produce) a plain self-copy of identical bytes yields the
  # SAME etag, making the whole zombie fence a silent no-op in production.
  #
  # And a NEW etag is not enough: it must be one the key has NEVER had, or two
  # steals bring a zombie's etag back (expert review 2026-10-12 N1). See rotate_etag/4
  # for how each touch gets one (random-boundary split copy >= 6 MiB, re-nonced
  # bytes below). A response that does not prove rotation is CONFIRMED with a HEAD,
  # and a non-rotating store fails the touch closed (an un-fenced steal is not a
  # steal); verify_conditional_writes!/0 also probes two consecutive touches at boot.
  #
  # 404 (a never-flushed brand-new shard was stolen): the old "nothing a zombie
  # flush could clobber" reasoning was the WRONG direction (round-2 #7) — the
  # zombie's brand-new fence is If-None-Match:*, which succeeds precisely when no
  # object exists, so its stalled create-only PUT would land AFTER our steal and
  # the stealer's own first flush (also If-None-Match:*) would then 412 and
  # self-fence away its accepted writes. Create a SENTINEL at the data key
  # (create-only, so a just-landed real flush is never clobbered): the zombie's
  # PUT now deterministically 412s, and the stealer's pull recognizes the
  # sentinel as brand-new while carrying its etag as the first flush's fence.
  # A 412 means the object changed between our read and the copy —
  # re-running the touch once covers the benign race.
  # Returns `{:ok, %{pre: etag_or_nil, post: etag}}` — the SOURCE etag the touch
  # If-Matched (nil for a brand-new sentinel create) and the object's post-touch
  # etag (round-2 #6): the takeover revalidation adopts the post-touch etag for a
  # warm local file ONLY when `pre` equals its provenance sidecar, so a fork
  # laundered through the touch is quarantined instead of clobbered.
  defp touch_object(shard_id, attempt \\ 1) do
    key = db_key(shard_id)

    case head_sized(key) do
      {:ok, nil, _, _, _} ->
        case create_sentinel(key) do
          {:ok, sentinel_etag} ->
            {:ok, %{pre: nil, post: sentinel_etag}}

          # Lost the create race — a REAL object just landed (the zombie's flush,
          # or a concurrent writer): re-run the touch against it.
          {:error, :sentinel_exists} when attempt < 2 ->
            touch_object(shard_id, attempt + 1)

          {:error, _} = error ->
            error
        end

      # A PRIOR steal's sentinel (that stealer died before its first flush): a
      # form-rotation copy would strip the sentinel metadata and the next pull
      # would open the placeholder as shard bytes. Refresh it instead — a fresh
      # nonced body rotates the MD5 etag (fencing the previous stealer's zombie
      # If-Match) while keeping the sentinel semantics intact.
      {:ok, etag, true, _, _} ->
        case refresh_sentinel(key, etag) do
          {:ok, post} ->
            with {:ok, post} <- resolved_post(shard_id, etag, post),
                 do: {:ok, %{pre: etag, post: post}}

          {:error, :touch_precondition} when attempt < 2 ->
            touch_object(shard_id, attempt + 1)

          {:error, _} = error ->
            error
        end

      {:ok, etag, false, carry, size} ->
        case rotate_etag(key, etag, carry, size) do
          {:ok, post} ->
            with {:ok, post} <- resolved_post(shard_id, etag, post),
                 do: {:ok, %{pre: etag, post: post}}

          {:error, :touch_precondition} when attempt < 2 ->
            touch_object(shard_id, attempt + 1)

          {:error, _} = error ->
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Re-rotate the object's etag when it is STILL `expected` (expert review 2026-10-13 #17, the
  # `Storage.retouch/2` callback). A flush of byte-identical content (VACUUM INTO plus zstd is
  # deterministic) can land back on the steal-touch's PRE etag, which is the one a zombie's late
  # `If-Match` PUT carries — silently reopening the fence the steal closed. Guarded twice so it can
  # only ever rotate OUR bytes: the HEAD must read `expected`, and the rotation itself is
  # If-Matched on it, so a zombie PUT that lands in between 412s the touch (`:touch_precondition`)
  # rather than being laundered into a fresh etag. A sentinel (never-flushed placeholder) is not
  # something a flush produces, so it is `:moved`.
  @impl true
  def retouch(shard_id, expected) when is_binary(expected) do
    key = db_key(shard_id)

    case head_sized(key) do
      {:ok, ^expected, false, carry, size} ->
        with {:ok, post} <- rotate_etag(key, expected, carry, size),
             do: resolved_post(shard_id, expected, post)

      {:ok, _other, _sentinel?, _carry, _size} ->
        {:error, :moved}

      {:error, _} = error ->
        error
    end
  end

  # The post-touch etag, WITHOUT the confirm-rotation HEAD when the touch's own write
  # response already proved it (review 2026-07-23 #13): the sentinel PUT returns the new
  # etag in its response header, and both copy forms return it in their response body —
  # a follow-up HEAD re-fetched what the write response had already said, one more
  # serialized RTT per steal. The round-2 #4 fail-closed stance is unchanged: a response
  # that did NOT prove rotation (no etag, or an unmoved one) falls back to the HEAD-based
  # confirm_rotation, which fails the steal on an unrotated etag exactly as before.
  defp resolved_post(_shard_id, old_etag, post) when is_binary(post) and post != old_etag,
    do: {:ok, post}

  defp resolved_post(shard_id, old_etag, _unproven), do: confirm_rotation(shard_id, old_etag)

  # HEAD with sentinel awareness: {:ok, etag_or_nil, sentinel?, carry_headers}.
  #
  # The fourth element is EVERY user metadata header the touch must re-send, because a
  # self-copy/multipart-copy with the REPLACE directive drops all of it unless it is sent again.
  #
  # IT IS A LIST, NOT ONE HAND-PICKED KEY, and that is the actual fix rather than a detail. It
  # used to be the integrity md5 alone (#12/#17). `x-amz-meta-fathom-pos` — the position stamp A2
  # compares a replica against — was added later and nobody added it here, so **every steal-touch
  # silently erased it**. An unstamped object is never overridable by design, so promote-on-open
  # and survivor selection both went inert at precisely the moment they exist for: a takeover.
  # Nothing failed, nothing logged, and the shard just recovered to the last flush.
  #
  # Found on the rig 2026-08-12 by `chaos.sh rpo`, which measured the stamp present before the kill
  # and gone after. The unit suite could not see it: `Storage.Local` and `Fathom.Test.FaultyStorage`
  # keep a lock/metadata map in place across a touch, so "REPLACE drops user metadata" is a
  # property only the real backend has.
  defp head_object(key) do
    with {:ok, etag, sentinel?, carry, _size} <- head_sized(key),
         do: {:ok, etag, sentinel?, carry}
  end

  # `head_object/1` plus the stored size (`nil` when the store does not report it), which picks the
  # steal-touch's path (expert review 2026-10-12 N1).
  defp head_sized(key) do
    case Req.head(req(), url: url_path(key)) do
      {:ok, %{status: 200, headers: h}} ->
        {:ok, etag(h), sentinel_response?(h), carry_meta(h), content_length(h)}

      {:ok, %{status: 404}} ->
        {:ok, nil, false, [], nil}

      {:ok, %{status: s}} ->
        {:error, {:s3_head_status, s}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp content_length(headers) do
    with v when is_binary(v) <- header_value(headers, "content-length"),
         {n, ""} when n >= 0 <- Integer.parse(String.trim(v)) do
      n
    else
      _ -> nil
    end
  end

  @doc """
  The user metadata headers a REPLACE copy must re-send, taken off a GET/HEAD response.

  Public so it can be tested directly. The bug this closes was an omission from a list, and the
  only cheap way to catch the next omission is to assert the list's contents — the behaviour it
  protects (S3 dropping metadata on a self-copy) is not something either test double reproduces.

  Absent keys are skipped rather than sent empty: a legacy object with no md5 must not gain a
  fabricated one, and an object with no position must stay unstamped rather than acquire a stamp
  that claims an ordering nobody established.
  """
  # Markers that describe the OBJECT'S ROLE rather than its contents, and must therefore never
  # survive a copy. Keep this list short and justify every entry: everything here is a key the
  # carry-by-default rule below would otherwise get right.
  @never_carried [@sentinel_meta]

  @spec carry_meta(map() | keyword()) :: [{String.t(), String.t()}]
  def carry_meta(headers) do
    # EVERY USER KEY, NOT A LIST (expert review 2026-08-20 #8's implementation, 2026-08-22).
    #
    # This was `for key <- [@md5_meta, @pos_meta]`, and AGENTS.md already describes the fix as
    # carrying "every user key rather than a hand-picked one (the failure mode is an omission from
    # a list)" — the code had not caught up with the lesson. The list IS how the original bug
    # happened: `x-amz-meta-fathom-pos` was added after the md5 and nobody added it here, so every
    # steal-touch silently erased the stamp, and promote-on-open and survivor selection were both
    # inert at exactly the moment they exist for. Nothing failed and nothing logged.
    #
    # Found again while adding the lineage key: a list would have made the identical omission
    # available a third time. Enumerating instead means a key added tomorrow is carried by
    # construction.
    # ALLOW BY DEFAULT, with a small explicit deny list — not "carry everything", and the
    # difference matters. `shard_storage_touch_meta_test` already pinned that the SENTINEL marker
    # must NOT ride along: a touch that carried it would turn a real object into something the
    # pull path reads as a placeholder. That test blocked the first version of this change and was
    # right to.
    #
    # The two failure modes are not symmetric, which is why the default flipped. Forgetting to ADD
    # a key to an allow-list is silent: the stamp just vanishes and recovery quietly stops working,
    # for months. Forgetting to EXCLUDE one is loud: a sentinel where it does not belong breaks a
    # pull immediately and visibly.
    for {k, v} <- normalize_headers(headers),
        String.starts_with?(k, "x-amz-meta-"),
        k not in @never_carried,
        do: {k, v}
  end

  # Headers arrive as a map or a keyword list depending on the caller, and header names are
  # case-insensitive per RFC 9110 — so downcase before matching the prefix.
  defp normalize_headers(headers) do
    for {k, v} <- headers do
      key = k |> to_string() |> String.downcase()
      {key, value_of(v)}
    end
  end

  defp value_of([v | _]), do: to_string(v)
  defp value_of(v), do: to_string(v)

  # The fence is only real if the etag actually moved (round-2 #4): a store whose
  # touch doesn't rotate would leave the zombie's If-Match valid — fail the steal
  # closed instead of serving unfenced.
  defp confirm_rotation(shard_id, old_etag) do
    case object_etag(shard_id) do
      {:ok, new_etag} when not is_nil(new_etag) and new_etag != old_etag -> {:ok, new_etag}
      {:ok, unmoved} -> {:error, {:touch_no_rotation, unmoved}}
      {:error, reason} -> {:error, reason}
    end
  end

  # A lineage that will not parse is treated as ABSENT, never as 0. Zero is a real value meaning
  # "nothing has been stamped", and inventing it from a corrupt header would let a fresh owner
  # reuse a number a previous one already used.
  defp parse_lineage(nil), do: nil

  defp parse_lineage(raw) do
    case Integer.parse(String.trim(raw)) do
      {n, ""} when n >= 0 -> n
      _ -> nil
    end
  end

  # THE STEAL-TOUCH MUST PRODUCE AN ETAG THE KEY HAS NEVER HAD (expert review 2026-10-12 N1).
  #
  # What this replaced alternated copy forms: a single-form (MD5) etag was touched with a one-part
  # multipart self-copy (`md5(md5)-1`), a multipart one with a plain self-copy (back to `md5`).
  # Each touch moved the etag, but for IDENTICAL BYTES there are only those two etags, so two
  # consecutive steals went E0 -> E1 -> E0. A zombie that flushed at E0 and stalled across both
  # steals then had its late `If-Match: E0` PUT ACCEPTED — an unfenced write past the fence.
  # Metadata cannot help: user metadata does not enter an S3 etag.
  #
  # So the touch now always produces a fresh etag, by object size:
  #
  #   * >= @split_touch_min: a TWO-PART multipart self-copy (UploadPartCopy with byte ranges — the
  #     store copies server-side, nothing is transferred) split at a RANDOM boundary. A multipart
  #     etag is md5(md5(part1) <> md5(part2))-2, so a different boundary is a different etag, and
  #     it is never the single-form etag a flush PUT leaves. Part 1 must be >= 5 MiB (S3's minimum
  #     for every part but the last), so the boundary is drawn from [5 MiB, size): size - 5 MiB
  #     choices. The floor is 6 MiB rather than 5 so there are always >= 2^20 of them: the chance
  #     that any one later touch repeats a given touch's boundary is <= 2^-20 (~1e-6), and for a
  #     64 MiB shard ~1.7e-8. Large objects pay nothing extra over the old touch: still three
  #     requests' worth (create, part copies, complete), no bytes through fathom.
  #
  #   * smaller: there is no multipart copy with more than one part, and one part has exactly one
  #     etag, so the only way to a new etag is new stored BYTES. GET the object (If-Match the etag
  #     being touched), append a zstd skippable frame carrying a random nonce (replacing the
  #     previous touch's), and PUT it back If-Match. A zstd decoder skips the frame, so the decoded
  #     database and `x-amz-meta-fathom-md5` are unchanged; the etag is md5(new bytes), a repeat
  #     with probability ~2^-128. An object stored `:none` or `:zlib` is re-encoded to zstd on the
  #     way (every reader decodes by the object's own marker, so a changed encoding is invisible to
  #     them), and the decoded plaintext is checked against the object's digest first. COST: a
  #     small-object steal now moves the object through fathom twice (GET + PUT, up to ~6 MiB
  #     each way), which adds to failover latency; the old touch moved no bytes.
  @min_part_bytes 5 * 1024 * 1024
  @split_touch_min @min_part_bytes + 1024 * 1024

  # `size` is the object's stored size off the HEAD. A store that does not report it gets no
  # touch, and so no steal: guessing a path could pick a multipart split the store then refuses.
  defp rotate_etag(key, etag, carry, size) when is_integer(size) and size >= @split_touch_min,
    do: split_copy_touch(key, etag, carry, size)

  defp rotate_etag(key, etag, _carry, size) when is_integer(size), do: nonce_touch(key, etag)

  defp rotate_etag(_key, _etag, _carry, _size), do: {:error, :touch_unknown_size}

  # The small-object touch: GET exactly the version at `etag`, re-nonce it, PUT it back fenced on
  # that same etag. The metadata re-sent is the GET's own (the same version's), for the reason
  # `head_object/1` gives: a PUT keeps no `x-amz-meta-*` it is not sent.
  defp nonce_touch(key, etag) do
    # `raw: true`: the EXACT stored bytes — no HTTP-level decompression or body decoding by Req.
    case Req.get(req(), url: url_path(key), headers: [{"if-match", etag}], raw: true) do
      {:ok, %{status: 200, body: body, headers: h}} ->
        with {:ok, new_body, meta} <- nonced_body(IO.iodata_to_binary(body), h, carry_meta(h)) do
          put_touched(key, etag, new_body, meta)
        end

      # Changed (412) or gone (404) since the HEAD: the caller re-runs the touch once.
      {:ok, %{status: s}} when s in [404, 412] ->
        {:error, :touch_precondition}

      {:ok, %{status: s}} ->
        {:error, {:s3_touch_get_status, s}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp nonced_body(body, headers, carry) do
    case Codec.decoder(meta_enc(headers)) do
      # Already zstd: only the nonce changes. The original frames are untouched, so the body is not
      # re-decoded here (that would be a full decompress on every failover); the pull decodes and
      # digests it as always.
      {:ok, :zstd} ->
        {:ok, Codec.renonce(body), carry}

      {:ok, from} ->
        reencoded_body(body, from, headers, carry)

      # An encoding this node cannot read: it cannot re-encode it either. Fail the touch closed.
      {:error, _} = error ->
        error
    end
  end

  # `:none`/`:zlib` -> zstd + nonce. The plaintext digest must match what the object claims BEFORE
  # it is restamped (the metadata digest when present, else the MD5 etag of a raw single-PUT
  # object), so a touch never launders a torn or foreign body into one carrying a fresh digest. A
  # raw object with neither is the legacy unverifiable case the pull already serves as-is.
  defp reencoded_body(body, from, headers, carry) do
    with {:ok, zbody, plain_hex} <- Codec.reencode_zstd(body, from),
         :ok <- plain_matches_object(plain_hex, from, headers),
         new_body = Codec.renonce(zbody),
         # The new encoding is proved to decode to the digest it is labelled with before it is
         # stored, as every compressed flush is (expert review 2026-10-08 #15).
         :ok <- Codec.verify_binary_decodes_to(new_body, :zstd, plain_hex) do
      meta =
        Enum.reject(carry, fn {k, _} -> k in [@md5_meta, Codec.meta_header()] end) ++
          [{@md5_meta, plain_hex} | Codec.upload_headers(:zstd)]

      {:ok, new_body, meta}
    end
  end

  defp plain_matches_object(plain_hex, from, headers) do
    case {meta_md5(headers), from} do
      {nil, :none} ->
        verify_md5(Base.decode16!(plain_hex, case: :lower), etag(headers))

      {nil, enc} ->
        {:error, {:missing_plain_digest, Atom.to_string(enc)}}

      {hex, _} ->
        if String.downcase(hex) == plain_hex, do: :ok, else: {:error, :checksum_mismatch}
    end
  end

  defp put_touched(key, etag, body, meta) do
    content_md5 = :md5 |> :crypto.hash(body) |> Base.encode64()

    case Req.put(req(),
           url: url_path(key),
           body: body,
           headers: [{"if-match", etag}, {"content-md5", content_md5} | meta]
         ) do
      {:ok, %{status: s, headers: h}} when s in 200..299 -> {:ok, etag(h)}
      # A fenced PUT to a key deleted since the GET is 404 on AWS (see `flush/6`).
      {:ok, %{status: s}} when s in [404, 412] -> {:error, :touch_precondition}
      {:ok, %{status: s}} -> {:error, {:s3_touch_put_status, s}}
      {:error, reason} -> {:error, reason}
    end
  end

  # The etag an XML copy/complete response body reports, in the same quoted form the etag
  # header carries (confirm_rotation/object_etag compare header-form etags), or nil.
  defp body_etag(body) do
    case copy_part_etag(body) do
      {:ok, bare} -> ~s(") <> bare <> ~s(")
      _ -> nil
    end
  end

  # The large-object touch: a two-part multipart self-copy split at a random boundary (see above).
  # Both part copies are fenced on `etag`, so a flush landing mid-touch fails the touch (412) rather
  # than mixing two versions' bytes into one object.
  defp split_copy_touch(key, etag, carry, size) do
    source = "/" <> fetch!(config(), :bucket) <> "/" <> key
    cut = split_point(size)

    case create_multipart(key, carry) do
      {:ok, upload_id} ->
        with {:ok, p1} <- upload_part_copy(key, upload_id, source, etag, 1, {0, cut - 1}),
             {:ok, p2} <- upload_part_copy(key, upload_id, source, etag, 2, {cut, size - 1}),
             {:ok, post} <- complete_multipart(key, upload_id, [{1, p1}, {2, p2}], etag) do
          {:ok, post}
        else
          {:error, _} = error ->
            abort_multipart(key, upload_id)
            error
        end

      {:error, _} = error ->
        error
    end
  end

  # Uniform over [@min_part_bytes, size): part 1 is at least S3's 5 MiB minimum, part 2 at least one
  # byte. From the CSPRNG, so two nodes (or a node restarted with the same seed) do not draw in step;
  # the modulo bias over a 64-bit draw is below 2^-30 for any object under 16 GiB.
  @doc false
  @spec split_point(pos_integer()) :: pos_integer()
  def split_point(size) when size > @min_part_bytes do
    <<r::unsigned-64>> = :crypto.strong_rand_bytes(8)
    @min_part_bytes + rem(r, size - @min_part_bytes)
  end

  # CreateMultipartUpload is where a multipart object's user metadata is set (parts carry none), so
  # the whole `carry` list is threaded here — the completed object then HEADs with every key the
  # source had, integrity md5 AND position stamp.
  defp create_multipart(key, carry) do
    case Req.post(req(),
           url: url_path(key) <> "?uploads",
           body: "",
           headers: carry
         ) do
      {:ok, %{status: s, body: body}} when s in 200..299 ->
        case Regex.run(~r|<UploadId>([^<]+)</UploadId>|, to_string(body)) do
          [_, id] -> {:ok, id}
          _ -> {:error, {:s3_multipart_no_upload_id, body}}
        end

      {:ok, %{status: s}} ->
        {:error, {:s3_multipart_status, s}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp upload_part_copy(key, upload_id, source, etag, part, {first, last}) do
    case Req.put(req(),
           url:
             url_path(key) <>
               "?partNumber=#{part}&uploadId=#{URI.encode_www_form(upload_id)}",
           headers: [
             {"x-amz-copy-source", source},
             {"x-amz-copy-source-if-match", etag},
             {"x-amz-copy-source-range", "bytes=#{first}-#{last}"}
           ]
         ) do
      {:ok, %{status: s, body: body}} when s in 200..299 ->
        body = to_string(body)

        cond do
          not copy_body_ok?(body) ->
            {:error, {:s3_touch_error_body, body}}

          true ->
            copy_part_etag(body)
        end

      {:ok, %{status: 412}} ->
        {:error, :touch_precondition}

      {:ok, %{status: s}} ->
        {:error, {:s3_part_copy_status, s}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The quote around the ETag value arrives in whatever XML escaping the store
  # chose: AWS emits &quot;, MinIO emits the numeric entity &#34;, and a literal
  # " is legal inside element content. Missing one of these fails the boot fence
  # probe on a store whose conditional writes are actually fine (the chaos rig
  # caught MinIO's &#34;).
  @doc false
  def copy_part_etag(body) do
    case Regex.run(~r|<ETag>(?:&quot;\|&#34;\|")?([0-9a-fA-F-]+)|, body) do
      [_, part_etag] -> {:ok, part_etag}
      _ -> {:error, {:s3_part_copy_no_etag, body}}
    end
  end

  # CompleteMultipartUpload is the CLASSIC 200-with-<Error>-body case (#8) —
  # copy_body_ok? guards it like the other copies. Returns `{:ok, new_etag_or_nil}`: the
  # CompleteMultipartUploadResult body carries the completed object's etag (review
  # 2026-07-23 #13 — the caller previously re-fetched it with a confirm HEAD).
  #
  # `if_match` fences the Complete on the etag the touch started from (expert review 2026-10-13
  # #3). The part copies are each fenced on it, but a zombie's `If-Match: E0` flush that lands
  # AFTER a part was read and BEFORE the Complete succeeds (E0 is still current) would otherwise
  # be published over by the stale bytes at a new etag: the zombie's acked flush erased. With the
  # precondition the Complete is 412 (the object moved) or 404 (it was deleted), both mapped to
  # `:touch_precondition`, so `touch_object` re-HEADs and re-splits against the zombie's bytes.
  defp complete_multipart(key, upload_id, parts, if_match) do
    body =
      "<CompleteMultipartUpload>" <>
        Enum.map_join(parts, "", fn {n, part_etag} ->
          "<Part><PartNumber>#{n}</PartNumber>" <> ~s(<ETag>"#{part_etag}"</ETag></Part>)
        end) <> "</CompleteMultipartUpload>"

    case Req.post(req(),
           url: url_path(key) <> "?uploadId=#{URI.encode_www_form(upload_id)}",
           body: body,
           headers: [{"if-match", if_match}]
         ) do
      {:ok, %{status: s, body: resp}} when s in 200..299 ->
        resp = to_string(resp)

        if copy_body_ok?(resp),
          do: {:ok, body_etag(resp)},
          else: {:error, {:s3_touch_error_body, resp}}

      {:ok, %{status: s}} when s in [404, 412] ->
        {:error, :touch_precondition}

      {:ok, %{status: s}} ->
        {:error, {:s3_complete_status, s}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # An abort that fails leaves server-side copies of tenant data behind until the bucket's
  # AbortIncompleteMultipartUpload lifecycle rule or `purge_shard` reaps them (expert review
  # 2026-10-13 #21), so a failure is logged rather than swallowed. The caller's own error stands.
  defp abort_multipart(key, upload_id) do
    case Req.delete(req(), url: url_path(key) <> "?uploadId=#{URI.encode_www_form(upload_id)}") do
      {:ok, %{status: s}} when s in 200..299 or s == 404 ->
        :ok

      other ->
        Logger.warning(
          "shard storage: could not abort multipart upload #{upload_id} on #{key} " <>
            "(#{inspect(abort_failure(other))}); it stays until the AbortIncompleteMultipartUpload " <>
            "lifecycle rule or a purge reaps it"
        )

        :error
    end
  end

  defp abort_failure({:ok, %{status: s}}), do: {:status, s}
  defp abort_failure({:error, reason}), do: reason

  # --- brand-new steal sentinel (round-2 #7) ---

  @sentinel_body "fathom-brand-new-sentinel"

  defp create_sentinel(key) do
    case put_sentinel(key, [{"if-none-match", "*"}]) do
      {:ok, %{status: s, headers: h}} when s in 200..299 -> {:ok, etag(h)}
      {:ok, %{status: 412}} -> {:error, :sentinel_exists}
      {:ok, %{status: s}} -> {:error, {:s3_sentinel_status, s}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Returns `{:ok, new_etag_or_nil}` — the refreshed sentinel's etag from the PUT response
  # header (nil if the store omitted it; the caller then confirms via HEAD).
  defp refresh_sentinel(key, etag) do
    case put_sentinel(key, [{"if-match", etag}]) do
      {:ok, %{status: s, headers: h}} when s in 200..299 -> {:ok, etag(h)}
      {:ok, %{status: 412}} -> {:error, :touch_precondition}
      {:ok, %{status: s}} -> {:error, {:s3_sentinel_status, s}}
      {:error, reason} -> {:error, reason}
    end
  end

  # A fresh nonce per write so every sentinel carries a NEW MD5 etag — a refresh
  # must rotate (same-body PUTs etag identically on MD5 stores).
  defp put_sentinel(key, cond_headers) do
    nonce = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    Req.put(req(),
      url: url_path(key),
      body: @sentinel_body <> ":" <> nonce,
      headers: [{@sentinel_meta, "1"} | cond_headers]
    )
  end

  defp sentinel_response?(headers), do: headers[@sentinel_meta] != nil

  # Integrity (expert review #37): TLS/TCP catch wire corruption, but nothing caught
  # corruption introduced BEFORE the socket — a torn read off a bad disk, a buggy
  # proxy, or an S3-compatible store bug. Content-MD5 on every data PUT makes the
  # store reject a torn upload; downloads verify the streamed body's MD5 against the
  # x-amz-meta-fathom-md5 metadata (etag-form-independent — #17) when present, else the
  # returned etag when it's the MD5-shaped single-part form (32 hex chars —
  # multipart/encrypted etags are not MD5s and are skipped).
  #
  # Uploads hash the file in a chunked pass (Content-MD5 is a header, so it must be
  # known before the body streams; the page cache makes the second pass cheap) and
  # refuse a single PUT past the 5 GB ceiling (expert review #20). Returns both the
  # base64 form (the Content-MD5 header wire format) and the base16 hex form (the
  # metadata + download-comparison form).
  defp stat_and_md5(path) do
    with {:ok, %{size: size, mtime: mtime}} <- File.stat(path) do
      if size > Application.get_env(:fathom, :s3_max_single_put, @max_single_put) do
        {:error, {:object_too_large, size}}
      else
        digest =
          path
          |> File.stream!(@stream_chunk)
          |> Enum.reduce(:crypto.hash_init(:md5), &:crypto.hash_update(&2, &1))
          |> :crypto.hash_final()

        # The upload reads this file a SECOND time to stream the body, and the two reads were
        # treated as identical (expert review 2026-08-01 #44). Re-stat and refuse if the file
        # moved underneath us: `content-length` and `content-md5` are computed here and describe
        # the FIRST read, so a changed file means the PUT declares one thing and sends another.
        #
        # S3 does reject that (BadDigest), so this is not a correctness backstop — it is an
        # honest error. `{:source_changed_during_upload, path}` says the flush source was written
        # while being uploaded; a BadDigest from the store says the credentials or the proxy are
        # broken. Those need completely different operator responses, and the window is real on
        # `upload_for_drop/1`, where the source is the LIVE database rather than a VACUUM temp.
        case File.stat(path) do
          {:ok, %{size: ^size, mtime: ^mtime}} ->
            {:ok, size, Base.encode64(digest), Base.encode16(digest, case: :lower)}

          {:ok, _changed} ->
            {:error, {:source_changed_during_upload, path}}

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  rescue
    e in File.Error -> {:error, e.reason}
  end

  # The download integrity gate (expert review 2026-07-14 #17). The etag-MD5 check
  # (verify_md5/2) only fires when the etag is the MD5-shaped single-part form, but the
  # steal-time fence ROTATES the etag (rotate_etag/touch_object) to invalidate a zombie's
  # If-Match — so right after a steal the object carries a `...-N` etag, or the MD5 of re-nonced
  # compressed bytes, and the new owner's failover cold-open pull would run with NO content check
  # until the next flush. The `x-amz-meta-fathom-md5` metadata carries the body's MD5
  # independent of the etag's shape and survives the rotation, so when it's present we verify
  # against IT regardless of etag form. Falls back to the etag-MD5 check for objects with no
  # metadata (older flushes, or the unfenced copy paths); a no-op when neither is available.
  # An ENCODED object must carry `@md5_meta` (expert review 2026-10-08 #23). Without it the
  # fallback below compares the DECODED bytes' digest with the etag, which is the MD5 of the stored
  # COMPRESSED bytes — so the pull fails every time on a single-PUT etag, and on a multipart etag
  # (which `verify_md5/2` skips) the decoded bytes are not checked at all. Every fathom write that
  # sets the marker also sets the digest; an object that has one without the other came from
  # something else, and is refused rather than served unverified.
  defp verify_decoded(digest, headers) do
    case {meta_enc(headers), meta_md5(headers)} do
      {enc, nil} when is_binary(enc) and enc != "" ->
        {:error, {:missing_plain_digest, enc}}

      {_enc, meta_hex} ->
        verify_integrity(digest, meta_hex, etag(headers))
    end
  end

  defp verify_integrity(digest, nil, etag), do: verify_md5(digest, etag)

  defp verify_integrity(digest, meta_hex, _etag) do
    if Base.encode16(digest, case: :lower) == String.downcase(meta_hex),
      do: :ok,
      else: {:error, :checksum_mismatch}
  end

  # The x-amz-meta-fathom-md5 value off a GET/HEAD response (list- or bare-valued), or nil.
  defp meta_md5(headers) do
    header_value(headers, @md5_meta)
  end

  defp header_value(headers, key) do
    case headers[key] do
      [v | _] when is_binary(v) -> v
      v when is_binary(v) -> v
      _ -> nil
    end
  end

  defp pos_meta_header(nil), do: []
  defp pos_meta_header(pos), do: [{@pos_meta, Storage.encode_position(pos)}]

  # `nil` LINEAGE MEANS "LEAVE THE PREVIOUS VALUE ALONE", AND A PUT CANNOT DO THAT BY ITSELF
  # (expert review 2026-08-24 #11, found independently by two panels).
  #
  # The `c:flush/5` callback doc is explicit: "`nil` writes no lineage … the backends take `nil`
  # as leave-any-previous-value-alone. Erasing a shard's lineage would reintroduce exactly the
  # reset the key exists to prevent." `Storage.Local` honours that for free — its lineage is a
  # separate sidecar and `write_lineage(_, nil)` is a no-op. S3 stores it as user metadata ON the
  # object, and a `PUT Object` replaces the object wholesale: every `x-amz-meta-*` key not
  # re-sent is gone. So the two backends implemented OPPOSITE behaviours for the same argument,
  # and every flush passing `nil` silently reset the counter on real S3.
  #
  # Reachable on the default path, not an exotic one: `lineage_to_store(:disabled) -> nil` on any
  # node with replication off, plus the `:unknown` case, `Promote.install_and_publish/3`, every
  # per-shard schema migration and revert, and every snapshot restore. Afterwards
  # `Storage.next_lineage/1` falls back to `position.epoch` — the LOCK epoch under `:disabled` —
  # so a shard sitting at lineage 50 is reseeded at 2 by one drain onto a non-replicating node.
  # A peer holding a pre-migration replica stamped higher then outranks the object in
  # `Promote.fresher?/2`, and a later failover rolls the tenant's schema and data back.
  #
  # This is the same failure class `carry_meta/1` already fixed for the TOUCH path — that one was
  # hardened to carry every user key across a copy, but a PUT still drops what it is not
  # explicitly told to write, and the flush path never got the equivalent.
  #
  # COST, since the panel flagged the mechanism as open and unmeasured. The HEAD is paid only
  # when we are overwriting an EXISTING object (`expected_etag` present) AND have no lineage of
  # our own to write. A brand-new shard PUTs `If-None-Match: *`, has no metadata to inherit, and
  # pays nothing — so cold open is untouched, which matters because `open_lineage/1` deliberately
  # skips its HEAD when replication is off precisely to keep `cold_open_p50_us` unchanged. On the
  # flush path one HEAD sits beside a checkpoint/VACUUM INTO and a full-object PUT, which dominate
  # it. Measured through the bench gate rather than argued: see the commit.
  #
  # The POSITION stamp is deliberately NOT carried — erasing it is correct, and `pos_meta_header/1`
  # keeps its own semantics.
  #
  # RETURNS `{headers, carried}` (expert review 2026-08-26 #33). `carried` is what the object will
  # carry after this PUT — an integer, or `:none` for "it carries no lineage". The caller stashes
  # it and hands it straight back as `{:carried, _}` on the next flush, which is what removes the
  # HEAD from every flush after the first.
  #
  # THE HEAD WAS PURE WASTE ON THE DEFAULT CONFIG. With replication off `lineage_to_store/1` is
  # `nil` and the third clause fires on every flush of an existing shard, forever — and on a fleet
  # that never enabled replication the object carries no lineage, so the HEAD returns nothing and
  # the header is empty. The cost is not bytes (the old note argued that): it is a request and a
  # serialized RTT inserted AFTER `recheck_before_put/1` and before the PUT, partially undoing
  # review 2026-08-01 #28, whose whole purpose was shrinking that window.
  #
  # SOUND UNDER THE SINGLE-WRITER LEASE: while this node holds it nobody else writes the object,
  # so a cached value cannot go stale downward — which is the only dangerous direction, since the
  # lineage must never decrease. The caller drops the cache on the two paths where `state.etag`
  # moves for a reason we did not cause, and a dropped cache falls back to the HEAD.
  defp carried_lineage_header(_shard_id, _expected_etag, n) when is_integer(n),
    do: {[{@lineage_meta, Integer.to_string(n)}], n}

  # Told what the object carries, so no read at all. `:none` writes no header, which is exactly
  # what the HEAD-then-empty path did.
  defp carried_lineage_header(_shard_id, _expected_etag, {:carried, n}) when is_integer(n),
    do: {[{@lineage_meta, Integer.to_string(n)}], n}

  defp carried_lineage_header(_shard_id, _expected_etag, {:carried, :none}), do: {[], :none}

  # A brand-new object has no metadata to inherit, so cold open pays nothing.
  defp carried_lineage_header(_shard_id, nil, nil), do: {[], :none}

  defp carried_lineage_header(shard_id, _expected_etag, nil) do
    case object_head(shard_id) do
      {:ok, %{lineage: n}} when is_integer(n) -> {[{@lineage_meta, Integer.to_string(n)}], n}
      _ -> {[], :none}
    end
  end

  # A materialized download temp is empty (or unstattable). Empty is refused rather than promoted
  # (#6); unstattable is refused too — the safe direction for a file we are about to hand SQLite.
  defp empty_body?(path) do
    case File.stat(path) do
      {:ok, %{size: 0}} -> true
      {:ok, _} -> false
      _ -> true
    end
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
    Storage.ensure_dir(Path.dirname(local_path))
    do_download(url, local_path, headers, opts, @download_attempts)
  end

  defp do_download(url, local_path, headers, opts, attempts_left) do
    tmp = "#{local_path}.dl.#{System.unique_integer([:positive])}"
    # The temp file/fd is created LAZILY on the first 200 data chunk (review 2026-07-23
    # #15b): a 304/404 has no body, and the eager open cost every (since-removed)
    # warm-follower revalidation a create/open/close/rm disk cycle per 304 — pure churn
    # at O(cached)/poll; a missing object still benefits. The fd rides this process's dictionary (the `into` fun runs in the
    # calling process) so the after-block can close it on ANY exit, including a transport
    # error where Req returns no response to carry it.
    fd_key = {__MODULE__, :dl_fd, tmp}

    # try/after (expert review round-2 #27): `:ok = IO.binwrite` RAISES on
    # ENOSPC/EIO mid-stream, which previously skipped both the close and the rm —
    # leaking the fd and stranding a shard-sized temp nothing reaps. The after
    # unwinds on any exception; a successful promote renames tmp away first, so its
    # rm is a harmless enoent. (An external brutal kill still can't unwind — the
    # age-gated temp reaper at coordinator open / follower refresh covers that.)
    try do
      # The inflate context (#38) lives in the process dictionary alongside the fd, for the same
      # reason: the after-block must be able to release it on ANY exit, including a transport
      # error that returns no response to carry it.
      z_key = {__MODULE__, :dl_z, tmp}

      into = fn {:data, chunk}, {req, resp} ->
        if resp.status == 200 do
          fd =
            case Process.get(fd_key) do
              nil ->
                # Buffered (expert review 2026-10-01 perf #35): over TLS each chunk is one record,
                # <= 16 KiB, so an unbuffered fd paid one write syscall per record — a 5 MiB pull
                # was 320 writes, 10.8 ms of handler work vs 6.1 ms with a 1 MiB buffer, and this
                # runs between socket reads. A buffered write's error surfaces at close instead,
                # which is why the close below is a hard `:ok =` match: an unflushed buffer must
                # fail the pull, never reach promote with a short file (the MD5 is over the bytes
                # handed to binwrite, so it would not catch a lost write by itself).
                {:ok, fd} =
                  File.open(tmp, [:write, :raw, :binary, {:delayed_write, 1_048_576, 50}])

                Process.put(fd_key, fd)
                fd

              fd ->
                fd
            end

          # Decode-always (#38): the object's own marker decides, never this node's setting, so a
          # fleet can roll the encoding flag back without orphaning objects. An unrecognised
          # marker resolves to an error and is caught below — we must never write bytes we can't
          # interpret into a file SQLite will be handed.
          decoded =
            case Process.get(z_key, :unset) do
              :unset ->
                d = Codec.decoder(meta_enc(resp.headers))
                Process.put(z_key, decode_state(d))
                Process.get(z_key)

              state ->
                state
            end

          case decoded do
            {:error, _} = err ->
              {:halt, {req, Req.Response.put_private(resp, :fathom_enc_error, err)}}

            z ->
              # Each decoded piece is written as the codec yields it, never collected per chunk —
              # a chunk of highly compressible data decodes to tens of MB (expert review 2026-10-08
              # #2; see Codec.inflate_into/4).
              case Codec.inflate_into(z, chunk, &write_plain(&2, fd, &1), resp) do
                {:ok, resp} ->
                  {:cont, {req, resp}}

                # A body that does not decode: torn or corrupt in transit, handled like a
                # checksum mismatch below (retry the whole download), never written.
                {:error, _} = err ->
                  {:halt, {req, Req.Response.put_private(resp, :fathom_decode_error, err)}}
              end
          end
        else
          {:cont, {req, resp}}
        end
      end

      # `params` is empty for every pull but `pull_object_version/3`'s (`versionId`); Req's
      # put_params step query-encodes it before sigv4 signs the request, so a version id is never
      # spliced into the URL by hand.
      result =
        Req.get(req(),
          url: url,
          params: Keyword.get(opts, :params, []),
          headers: headers,
          into: into,
          retry: false
        )

      # A buffered write's error surfaces HERE (see the open above), and it used to be a hard
      # `:ok =` match — fail-closed, but as a raise: it skipped the retry below and crashed a
      # coordinator open that called this outside a rescue (expert review 2026-10-08 #26). Now it is
      # an error result like any other torn transfer, retried with a fresh temp.
      fd = Process.delete(fd_key)

      case close_download_temp(fd) do
        :ok ->
          handle_download_result(result, {url, local_path, headers, opts, attempts_left},
            tmp: tmp,
            opened?: fd != nil
          )

        {:error, reason} ->
          retry_or(
            {:error, {:temp_close_failed, reason}},
            url,
            local_path,
            headers,
            opts,
            attempts_left
          )
      end
    after
      case Process.delete(fd_key) do
        nil -> :ok
        fd -> _ = File.close(fd)
      end

      # Release the inflate context on every exit path, exactly like the fd (#38).
      Codec.release(Process.delete({__MODULE__, :dl_z, tmp}))

      File.rm(tmp)
    end
  end

  @doc false
  @spec close_download_temp(term()) :: :ok | {:error, term()}
  def close_download_temp(nil), do: :ok
  def close_download_temp(fd), do: File.close(fd)

  defp handle_download_result(result, {url, local_path, headers, opts, attempts_left}, temp) do
    tmp = Keyword.fetch!(temp, :tmp)

    case result do
      # An object marked with an encoding this node cannot perform (#38). FAIL THE PULL — the
      # temp holds nothing usable and handing raw bytes to SQLite as a database is precisely
      # the correctness incident the marker exists to prevent. Not retryable: the marker will
      # be the same next time. Upgrade the node instead.
      # The body would not decode under its own marker — corrupt or torn in transit. Same
      # handling as a checksum mismatch: a fresh temp and the whole download again.
      {:ok, %{private: %{fathom_decode_error: {:error, reason}}}} ->
        Logger.warning("shard object at #{url} did not decode (#{inspect(reason)}); retrying")

        retry_or(
          {:error, {:undecodable_body, reason}},
          url,
          local_path,
          headers,
          opts,
          attempts_left
        )

      {:ok, %{private: %{fathom_enc_error: {:error, reason}}}} ->
        Logger.error(
          "shard object at #{url} is stored with an encoding this node cannot decode " <>
            "(#{inspect(reason)}); refusing the pull rather than serving undecodable bytes " <>
            "as a database. Upgrade this node to a build that supports it."
        )

        {:error, reason}

      {:ok, %{status: 200} = resp} ->
        cond do
          # A brand-new steal sentinel (round-2 #7): not shard bytes — never
          # promote it into place. The after-block drops the temp; the caller
          # carries the sentinel's etag as the brand-new shard's first-flush fence.
          sentinel_response?(resp.headers) ->
            {:sentinel, etag(resp.headers)}

          true ->
            # A 200 with an empty body never opened the temp — materialize the empty
            # file so the promote below behaves exactly as the eager-open code did.
            unless Keyword.fetch!(temp, :opened?), do: File.touch(tmp)

            # A zero-length body is NEVER a fathom object: every flush writes a non-empty
            # database AND an @md5_meta hash, and a brand-new shard is a 404 (:absent), not an
            # empty 200. Without this floor a foreign/corrupt object carrying a multipart-shaped
            # etag and no metadata makes verify_integrity a no-op (verify_md5 skips non-MD5
            # etags), and the empty temp is fsynced + promoted as a valid empty SQLite database.
            # The tenant is then served an empty db and the next flush PUTs the empty file over
            # the real stored object — an unrecoverable wipe (expert review 2026-08-31 #6).
            if empty_body?(tmp) do
              Logger.error(
                "shard object at #{url} returned a zero-length body; refusing to promote it " <>
                  "as an empty database (no integrity metadata to verify it against)"
              )

              {:error, :empty_object}
            else
              digest = :crypto.hash_final(resp.private[:fathom_md5] || :crypto.hash_init(:md5))

              # Prefer the etag-form-independent metadata hash (#17); fall back to the etag-MD5
              # check when the object predates the metadata or came from an unfenced copy path.
              case verify_decoded(digest, resp.headers) do
                :ok ->
                  case Storage.promote_temp(tmp, local_path) do
                    :ok -> {:ok, etag(resp.headers)}
                    {:error, _} = error -> error
                  end

                # A torn transfer produced bytes that don't match the object's etag — a
                # transient corruption, not a permanent one; retry the whole download with a
                # fresh temp rather than failing the pull outright (expert review #1).
                {:error, :checksum_mismatch} = error ->
                  retry_or(error, url, local_path, headers, opts, attempts_left)

                # Encoded but carrying no plaintext digest (see verify_decoded/2): nothing to
                # check the decoded bytes against, and a retry reads the same headers.
                {:error, {:missing_plain_digest, _}} = error ->
                  error
              end
            end
        end

      {:ok, %{status: 304}} ->
        if opts[:allow_304], do: :unchanged, else: {:error, {:s3_get_status, 304}}

      {:ok, %{status: 404}} ->
        :absent

      {:ok, %{status: status}} ->
        {:error, {:s3_get_status, status}}

      # A transport error (possibly mid-body, so the temp may hold partial bytes):
      # drop the partial temp and retry the whole download with a fresh one.
      {:error, reason} ->
        retry_or({:error, reason}, url, local_path, headers, opts, attempts_left)
    end
  end

  # Write decoded bytes to the temp and fold them into the plaintext digest. The digest is over the
  # DECODED bytes, so it still means "this database's hash" and matches @md5_meta whether or not
  # the object was compressed.
  defp write_plain(resp, fd, plain) do
    :ok = IO.binwrite(fd, plain)
    md5 = resp.private[:fathom_md5] || :crypto.hash_init(:md5)
    Req.Response.put_private(resp, :fathom_md5, :crypto.hash_update(md5, plain))
  end

  # `nil` for the raw path, a decode context for a decodable encoding, or the error tuple
  # itself so the `into` fun can halt on it.
  defp decode_state({:ok, encoding}), do: Codec.init(encoding)
  defp decode_state({:error, _} = error), do: error

  # The object's encoding marker off a GET/HEAD response (list- or bare-valued), or nil.
  defp meta_enc(headers) do
    case headers[Codec.meta_header()] do
      [v | _] when is_binary(v) -> v
      v when is_binary(v) -> v
      _ -> nil
    end
  end

  defp retry_or(error, url, local_path, headers, opts, attempts_left) do
    if attempts_left > 1 do
      do_download(url, local_path, headers, opts, attempts_left - 1)
    else
      error
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

  # Same HEAD shape as object_etag/1. A 404 is `{:ok, nil}` — no object means no position, which
  # reads identically to an unstamped one and lands on the same safe answer.
  @impl true
  def object_position(shard_id) do
    case Req.head(req(), url: object_path(shard_id)) do
      {:ok, %{status: 200, headers: h}} ->
        {:ok, Storage.parse_position(header_value(h, @pos_meta))}

      {:ok, %{status: 404}} ->
        {:ok, nil}

      {:ok, %{status: status}} ->
        {:error, {:s3_head_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The same HEAD again, read for BOTH facts at once. `object_etag/1` and `object_position/1` were
  # always the identical request; issuing it twice is what let the etag and the stamp describe two
  # different versions of the object, with a peer query and a database transfer in between.
  @impl true
  def object_head(shard_id) do
    case Req.head(req(), url: object_path(shard_id)) do
      {:ok, %{status: 200, headers: h}} ->
        {:ok,
         %{
           etag: etag(h),
           position: Storage.parse_position(header_value(h, @pos_meta)),
           lineage: parse_lineage(header_value(h, @lineage_meta)),
           md5: meta_md5(h)
         }}

      {:ok, %{status: 404}} ->
        {:ok, nil}

      {:ok, %{status: status}} ->
        {:error, {:s3_head_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Ranged GET of the object's first @header_range bytes, stream-decoded just far enough to reach
  # the SQLite header (expert review 2026-10-10 R3-5). The fork path used to pull the WHOLE object
  # to a scratch file to read four bytes. The object is codec-encoded, so a raw byte range cannot
  # be read directly: zstd emits output per block (<=128 KiB decoded, so a 256 KiB compressed
  # prefix always yields one) and zlib per input chunk. A truncated prefix is not a decode error
  # (see Codec.init/1); we only read the first 100 decoded bytes, and the full-object md5 is not
  # checked here — this is a version read for stamping a fresh fork, not a data path.
  @header_range 262_144
  @impl true
  def object_user_version(shard_id) do
    result =
      Req.get(req(),
        url: object_path(shard_id),
        headers: [{"range", "bytes=0-#{@header_range - 1}"}],
        decode_body: false
      )

    case result do
      {:ok, %{status: status, headers: h, body: body}} when status in [200, 206] ->
        # A steal sentinel is a placeholder, not a database: no object, as pull/2 reports it.
        if sentinel_response?(h),
          do: {:absent, nil},
          else: header_version(body, meta_enc(h))

      {:ok, %{status: 404}} ->
        {:absent, nil}

      {:ok, %{status: status}} ->
        {:error, {:s3_ranged_get_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp header_version(body, marker) when is_binary(body) do
    with {:ok, enc} <- Codec.decoder(marker) do
      prefix = binary_part(body, 0, min(byte_size(body), @header_range))
      ctx = Codec.init(enc)

      try do
        # Keep only the first 100 decoded bytes; a high-ratio prefix never accumulates.
        keep = fn piece, acc ->
          need = 100 - byte_size(acc)

          if need <= 0 do
            acc
          else
            bin = IO.iodata_to_binary(piece)
            acc <> binary_part(bin, 0, min(need, byte_size(bin)))
          end
        end

        case Codec.inflate_into(ctx, prefix, keep, <<>>) do
          {:ok, <<"SQLite format 3\0", _::binary-size(44), v::signed-big-32, _::binary>>} ->
            {:ok, v}

          {:ok, other} when is_binary(other) ->
            {:error, {:header_unreachable, byte_size(other)}}

          {:error, reason} ->
            {:error, {:header_unreachable, reason}}
        end
      after
        Codec.release(ctx)
      end
    end
  end

  # --- versioned copies (blue/green migration) ---

  # Refuse a server-side copy whose SOURCE is a steal sentinel (expert review 2026-08-01 #25).
  #
  # `touch_object/2` plants a sentinel AT THE DATA KEY on a steal of a never-flushed shard, and
  # CopyObject's default metadata directive is COPY — so every copy primitive would duplicate the
  # placeholder verbatim and report `:ok`. That is the same root cause as #24 (a sentinel read as
  # real bytes) on a different set of consumers, and #24's rating is why this one was deferred:
  # the panel called the trigger "Low likelihood (needs a steal of a never-flushed shard)" and the
  # chaos rig hit exactly that state THREE TIMES in one 180s soak.
  #
  # The sharpest case is `retain/2`, which is the migration's PRE-MIGRATION BACKUP
  # (`Migrator.ShardMigration`): a sentinel source meant retain copied nothing, returned `:ok`, and
  # the migration proceeded believing it had a rollback.
  #
  # Only the sentinel is intercepted. A genuinely MISSING source (404) still falls through to the
  # copy and keeps its existing error shape, because that shape is load-bearing —
  # `ShardMigration.classify_fork_error/1` maps `{:s3_copy_status, 404}` to `:no_template_snapshot`.
  #
  # NOT the review's recommended fix. It proposed, as the "simplest robust alternative", moving the
  # sentinel off the data key entirely (e.g. `<shard>.newlock`). That would reintroduce the bug
  # round-2 #7 added the sentinel to close: the zombie's stalled first flush is a create-only
  # `PUT If-None-Match:*` AT THE DATA KEY, which succeeds precisely when no object exists there. The
  # sentinel only fences it by occupying that key. Moving it is not an alternative; it is a revert.
  defp copy_unless_sentinel_source(src_key, dst_key) do
    case head_object(src_key) do
      {:ok, _etag, true, _} -> {:error, :no_source}
      {:ok, _etag, _not_sentinel, _} -> copy_object(src_key, dst_key)
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def retain(shard_id, version) do
    copy_unless_sentinel_source(db_key(shard_id), version_key(shard_id, version))
  end

  @impl true
  def restore(shard_id, version) do
    copy_unless_sentinel_source(version_key(shard_id, version), db_key(shard_id))
  end

  @impl true
  def version_present?(shard_id, version) do
    # A HEAD on the version key, like object_etag/1. Best-effort: anything but a 200 (a 404, a
    # non-2xx, or a transport error) reads as absent so a release-time warning never fails a
    # release (expert review 2026-08-31 #11).
    case Req.head(req(), url: url_path(version_key(shard_id, version))) do
      {:ok, %{status: 200}} -> true
      _ -> false
    end
  end

  # Same HEAD as version_present?/2, keeping the failure: only a 404 is `:absent`. Anything else (a
  # 5xx, a throttle, a transport error) is `{:error, _}`, so a DR reconcile never clears a pointer
  # to a copy it merely could not reach.
  @impl true
  def version_state(shard_id, version) do
    case Req.head(req(), url: url_path(version_key(shard_id, version))) do
      {:ok, %{status: 200}} -> :present
      {:ok, %{status: 404}} -> :absent
      {:ok, %{status: status}} -> {:error, {:s3_head_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def restore(shard_id, version, expected_etag) do
    # Fenced restore (expert review 2026-07-14 #4): the revert counterpart of the fenced flush/3.
    # A plain server-side CopyObject to live is UNCONDITIONAL, and S3 cannot carry an If-Match on
    # a CopyObject DESTINATION — so a steal landing between the migrator's read-only fence and this
    # copy-back would be clobbered. Mirror the forward flush instead: stream the version bytes to a
    # temp, then conditional-PUT them to live via flush/3 (If-Match the live etag the migrator
    # captured at pull). A 412 → :superseded → the revert aborts without clobbering the new owner.
    tmp = restore_temp(shard_id, version)

    try do
      case download(url_path(version_key(shard_id, version)), tmp) do
        {:ok, _etag} ->
          case flush(shard_id, tmp, expected_etag) do
            {:ok, _new_etag, _carried} -> :ok
            {:error, _} = error -> error
          end

        :absent ->
          {:error, :version_absent}

        # A version key holding a brand-new sentinel is not restorable bytes.
        {:sentinel, _etag} ->
          {:error, :version_absent}

        {:error, _} = error ->
          error
      end
    after
      File.rm(tmp)
    end
  end

  defp restore_temp(shard_id, version) do
    Path.join(
      Fathom.Shard.TempReaper.scratch_dir(),
      "fathom_s3_restore_#{shard_id}@#{version}_#{System.unique_integer([:positive])}.db"
    )
  end

  @impl true
  def drop_version(shard_id, version) do
    case Req.delete(req(), url: url_path(version_key(shard_id, version))) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def fork_from(template_id, version, dst_shard_id) do
    # Fork-from-template (finding #10): server-side copy of the retained
    # `<template>@<version>` snapshot to the new tenant's live object — same
    # CopyObject as retain/restore (a missing snapshot is a 404 copy status the
    # caller classifies as :no_template_snapshot).
    #
    # A version key can only hold a sentinel if a pre-#25 `retain/2` put one there; guarding
    # anyway, because that state is already durable in any bucket this ran against.
    copy_unless_sentinel_source(version_key(template_id, version), db_key(dst_shard_id))
  end

  @impl true
  def drop_live(shard_id) do
    case Req.delete(req(), url: object_path(shard_id)) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def fork_shard(src_id, dst_id) do
    # Fork a live shard to a new id (#14): HEAD the dst first (never clobber a tenant), then HEAD the
    # src, then CopyObject src.db → dst.db. Two heads + a copy is fine — a fork is a rare operator/API
    # action, not a hot path.
    # The dst HEAD is sentinel-aware (expert review 2026-08-01 #25). `head_etag/1` returns a plain
    # `{:ok, etag}` for ANY 200, so a dst holding only a steal sentinel read as "destination taken"
    # and returned `:dst_exists` FOREVER — permanently poisoning that tenant id, recoverable only by
    # an operator who knows to purge it. `Tenants.fork/3` could inflict this on itself: its
    # `fork_into_leased_dst/2` acquires the dst lease first, and a stale dst lock routes through the
    # steal path whose `touch_object` plants the sentinel, so the very next `fork_shard/2` in the
    # same function failed on a placeholder it had just created.
    #
    # A sentinel dst is treated as ABSENT and overwritten. That is safe for the property the
    # sentinel exists to hold (round-2 #7): a zombie's create-only `PUT If-None-Match:*` still 412s
    # against the forked bytes exactly as it did against the placeholder — the key stays occupied.
    # And the caller already holds the dst lease while this runs.
    case head_object(db_key(dst_id)) do
      {:ok, nil, _, _} -> fork_after_dst_check(src_id, dst_id)
      {:ok, _etag, true, _} -> fork_after_dst_check(src_id, dst_id)
      {:ok, _etag, _not_sentinel, _} -> {:error, :dst_exists}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fork_after_dst_check(src_id, dst_id) do
    # `copy_unless_sentinel_source/2` HEADs the src and refuses a sentinel; a 404 there returns
    # `{:ok, nil, false, nil}`, which would fall through to a copy that 404s — so keep the explicit
    # absent branch and its `:no_source`, which is the documented contract callers match on.
    case head_object(db_key(src_id)) do
      {:ok, nil, _, _} -> {:error, :no_source}
      {:ok, _etag, true, _} -> {:error, :no_source}
      {:ok, _etag, _not_sentinel, _} -> copy_object(db_key(src_id), db_key(dst_id))
      {:error, reason} -> {:error, reason}
    end
  end

  # --- point-in-time snapshots (#12) ---

  @impl true
  def snapshot(shard_id, snapshot_id) do
    # A sentinel live object is not snapshottable bytes (#25) — a "successful" snapshot of a
    # placeholder is worse than a failed one, because the operator stops looking.
    copy_unless_sentinel_source(db_key(shard_id), snapshot_key(shard_id, snapshot_id))
  end

  @doc """
  Downloads one specific S3 object VERSION of `shard_id`'s live object to `local_path` (expert
  review 2026-10-08 #22) — the one-command path behind `mix fathom.shard pull --version-id`.

  A bucket version is usually compressed (`x-amz-meta-fathom-enc`), so the raw bytes are not a
  SQLite file. This goes through the SAME decode + plaintext-digest verification + atomic promote as
  `pull/2`, including its refusal of an encoded object with no `x-amz-meta-fathom-md5`. Not part of
  the `Storage` behaviour: object versions are an S3 bucket feature with no `Local` counterpart.
  """
  @spec pull_object_version(String.t(), String.t(), Path.t()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def pull_object_version(shard_id, version_id, local_path)
      when is_binary(version_id) and version_id != "" do
    case download(object_path(shard_id), local_path, [], params: [versionId: version_id]) do
      {:ok, etag} -> {:ok, etag}
      # A steal sentinel is a placeholder, not shard bytes — nothing was written.
      {:sentinel, _etag} -> {:error, :sentinel_version}
      :absent -> {:error, :no_such_version}
      {:error, _} = error -> error
    end
  end

  @impl true
  def pull_snapshot(shard_id, snapshot_id, local_path) do
    case download(url_path(snapshot_key(shard_id, snapshot_id)), local_path) do
      {:ok, etag} -> {:ok, etag}
      # Same contract as pull/2 — never claim bytes were written when none were
      # (expert review 2026-08-01 #24).
      {:sentinel, etag} -> {:absent, etag}
      :absent -> {:absent, nil}
      {:error, _} = error -> error
    end
  end

  @impl true
  def restore_snapshot_from_file(shard_id, local_path, expected_etag) do
    # The fenced snapshot restore. Split out (when it was the promotion half of a by-id restore) so a caller that already holds the
    # snapshot's bytes does not re-download them (expert review 2026-08-26 #25). Identical fence:
    # flush/3 conditional-PUTs with If-Match on the live etag the caller captured, so a writer that
    # raced in after the drain aborts the restore with :superseded rather than being clobbered.
    #
    # The caller owns local_path — this does NOT delete it.
    case flush(shard_id, local_path, expected_etag) do
      {:ok, _new_etag, _carried} -> :ok
      {:error, _} = error -> error
    end
  end

  @impl true
  def drop_snapshot(shard_id, snapshot_id) do
    case Req.delete(req(), url: url_path(snapshot_key(shard_id, snapshot_id))) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def purge_shard(shard_id) do
    with :ok <- purge_shard_page(shard_id, nil), do: abort_shard_uploads(shard_id, nil)
  end

  # In-progress multipart uploads are invisible to ListObjectsV2, so a steal-touch killed between
  # create and complete leaves server-side copies of the tenant's data that the purge above never
  # sees (expert review 2026-10-13 #21). List them, apply the same delimiter rule as objects
  # (`acme` never aborts `acme2`'s), and abort each. Fail closed: an erasure that could not look is
  # not an erasure.
  #
  # The listing prefix is the shard's EXACT data key, not `<prefix><shard_id>`. The only multipart
  # upload fathom ever creates is the large-object steal-touch's, at `db_key/1`, so that is complete
  # coverage; and a broader prefix is not safe to rely on: the live-MinIO test showed MinIO's
  # ListMultipartUploads returns NOTHING for a partial-key prefix (`leasetest/s3_ab` finds no upload
  # at `leasetest/s3_ab12.db`; only the whole key matches), which would have made the purge a silent
  # no-op there. Revisit if another multipart creator is added.
  defp abort_shard_uploads(shard_id, markers) do
    with {:ok, page} <- list_uploads(db_key(shard_id), markers),
         :ok <- abort_uploads(shard_upload_targets(page.uploads, shard_id)) do
      case page.next do
        nil -> :ok
        next -> abort_shard_uploads(shard_id, next)
      end
    end
  end

  defp list_uploads(list_prefix, markers) do
    params =
      [{"uploads", ""}, {"prefix", list_prefix}] ++
        case markers do
          {key_marker, id_marker} -> [{"key-marker", key_marker}, {"upload-id-marker", id_marker}]
          nil -> []
        end

    case Req.get(req(), url: url_path(""), params: params) do
      {:ok, %{status: 200, body: body}} ->
        case UploadsPage.parse(body) do
          {:ok, _page} = ok -> ok
          {:error, reason} -> {:error, {:s3_list_uploads_parse, reason}}
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_list_uploads_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp shard_upload_targets(uploads, shard_id) do
    for %{key: key, upload_id: id} <- uploads, shard_object_key?(key, shard_id), do: {key, id}
  end

  defp abort_uploads(targets) do
    Enum.reduce_while(targets, :ok, fn {key, id}, :ok ->
      case Req.delete(req(), url: url_path(key) <> "?uploadId=#{URI.encode_www_form(id)}") do
        {:ok, %{status: status}} when status in 200..299 or status == 404 -> {:cont, :ok}
        {:ok, %{status: status}} -> {:halt, {:error, {:s3_abort_upload_status, status}}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  # Full tenant erasure (#15): ListObjectsV2 over `<prefix><shard_id>` (a broad
  # prefix that can also surface sibling ids like `<shard_id>2.db`), then delete
  # only the keys whose id-delimiter is `.` or `@` — so purging `acme` never deletes
  # `acme2`. Paginated; per-object DELETEs are issued as each page lands (fathom's
  # thesis is small shards with few versions/snapshots, so the per-shard object
  # count is tiny — no DeleteObjects batching needed, matching drop_* elsewhere).
  defp purge_shard_page(shard_id, token) do
    with {:ok, page} <- list_page(prefix() <> shard_id, token),
         :ok <- delete_keys(shard_object_keys(page.entries, shard_id)) do
      case page.next do
        nil -> :ok
        next -> purge_shard_page(shard_id, next)
      end
    end
  end

  # Any object left for the id? Two one-key LISTs on the delimiter-qualified prefixes (`<id>.` and
  # `<id>@`), which is exactly `shard_object_key?/2`'s rule, so a sibling id (`acme2.db`) can never
  # match and no paging past siblings is needed (expert review 2026-10-12 #7).
  @impl true
  def any_shard_object?(shard_id) do
    Enum.reduce_while([".", "@"], {:ok, false}, fn delim, _acc ->
      case list_page(prefix() <> shard_id <> delim, nil, 1) do
        {:ok, %{entries: [_ | _]}} -> {:halt, {:ok, true}}
        {:ok, _} -> {:cont, {:ok, false}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # One ListObjectsV2 page for `list_prefix`, parsed by `ListPage` (expert review 2026-10-10 #25):
  # entity-decoded keys, and an error — never a short "complete" listing — when the response says
  # it is truncated but gives no token to continue with.
  defp list_page(list_prefix, token, max_keys \\ nil) do
    params =
      [{"list-type", "2"}, {"prefix", list_prefix}] ++
        if(token, do: [{"continuation-token", token}], else: []) ++
        if(max_keys, do: [{"max-keys", Integer.to_string(max_keys)}], else: [])

    case Req.get(req(), url: url_path(""), params: params) do
      {:ok, %{status: 200, body: body}} ->
        case ListPage.parse(body) do
          {:ok, _page} = ok -> ok
          {:error, reason} -> {:error, {:s3_list_parse, reason}}
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_list_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp shard_object_keys(entries, shard_id) do
    for %{key: key} <- entries, shard_object_key?(key, shard_id), do: key
  end

  # Same delimiter rule as Local's shard_object?/2, on the prefix-qualified key: the
  # char after `<prefix><shard_id>` must be `.` or `@` (never a bare-prefix match).
  defp shard_object_key?(key, shard_id) do
    case String.replace_prefix(key, prefix() <> shard_id, "") do
      "." <> _ -> true
      "@" <> _ -> true
      _ -> false
    end
  end

  defp delete_keys(keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case Req.delete(req(), url: url_path(key)) do
        {:ok, %{status: status}} when status in 200..299 or status == 404 -> {:cont, :ok}
        {:ok, %{status: status}} -> {:halt, {:error, {:s3_delete_status, status}}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @impl true
  def put_tombstone(shard_id) do
    # A durable tombstone marker under the `tombstones/` key prefix (#6) — a namespace distinct from
    # every `<prefix><shard_id>…` object, so `purge_shard`'s prefix LIST never sees it and full erasure
    # leaves it standing. Empty body; the key is the fact. Survives a Postgres directory restore.
    case Req.put(req(), url: url_path(tombstone_key(shard_id)), body: "") do
      {:ok, %{status: status}} when status in 200..299 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_put_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def tombstoned_ids, do: tombstoned_ids_page(nil, [])

  # Paginated ListObjectsV2 over `<prefix>tombstones/`, stripping the scan prefix to recover each
  # deleted shard id (same shape as purge_shard_page / list_snapshots).
  #
  # `acc` is held REVERSED and each page is prepended with `Enum.reverse/2`, then reversed once at the
  # end. The old `acc ++ ids` copied the whole accumulator on every page, so N pages cost O(N^2) list
  # cells (expert review 2026-10-13 #20). The result order is unchanged.
  defp tombstoned_ids_page(token, acc) do
    scan_prefix = prefix() <> "tombstones/"

    with {:ok, page} <- list_page(scan_prefix, token) do
      acc = Enum.reverse(tombstone_ids(page.entries, scan_prefix), acc)

      case page.next do
        nil -> {:ok, Enum.reverse(acc)}
        next -> tombstoned_ids_page(next, acc)
      end
    end
  end

  defp tombstone_ids(entries, scan_prefix) do
    for %{key: key} <- entries,
        id = String.replace_prefix(key, scan_prefix, ""),
        id != key and id != "",
        do: id
  end

  defp tombstone_key(shard_id), do: prefix() <> "tombstones/" <> shard_id

  @impl true
  def put_token_floor(shard_id, version) do
    case Req.put(req(),
           url: url_path(token_floor_key(shard_id)),
           body: Integer.to_string(version)
         ) do
      {:ok, %{status: status}} when status in 200..299 -> :ok
      {:ok, %{status: status}} -> {:error, {:s3_put_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def read_token_floor(shard_id) do
    case Req.get(req(), url: url_path(token_floor_key(shard_id))) do
      {:ok, %{status: 200, body: body}} -> {:ok, parse_token_floor(body)}
      {:ok, %{status: 404}} -> {:ok, nil}
      {:ok, %{status: status}} -> {:error, {:s3_get_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp token_floor_key(shard_id), do: prefix() <> "tokenfloors/" <> shard_id

  defp parse_token_floor(body) do
    case Integer.parse(String.trim(to_string(body))) do
      {n, _} when n >= 0 -> n
      _ -> nil
    end
  end

  @impl true
  def list_snapshots(shard_id), do: list_snapshots(shard_id, nil, [])

  # ListObjectsV2 over the `<shard>@snap-` prefix (reusing the same paginated scan as
  # stored_usage/0), parsing each object's snapshot id + byte size.
  defp list_snapshots(shard_id, token, acc) do
    snap_prefix = prefix() <> shard_id <> "@snap-"

    with {:ok, page} <- list_page(snap_prefix, token) do
      entries = snapshot_entries(page.entries, snap_prefix)

      case page.next do
        nil -> {:ok, Enum.sort_by(acc ++ entries, & &1.id, :desc)}
        next -> list_snapshots(shard_id, next, acc ++ entries)
      end
    end
  end

  defp snapshot_entries(entries, snap_prefix) do
    for %{key: key, size: size} when is_integer(size) <- entries,
        String.starts_with?(key, snap_prefix) and String.ends_with?(key, ".db") do
      id = key |> String.replace_prefix(snap_prefix, "") |> String.replace_suffix(".db", "")
      %{id: id, bytes: size}
    end
  end

  # Server-side copy: the destination is the request URL; the source is the
  # bucket-qualified key in x-amz-copy-source (S3/MinIO copy without download).
  defp copy_object(src_key, dst_key) do
    source = "/" <> fetch!(config(), :bucket) <> "/" <> src_key

    case Req.put(req(), url: url_path(dst_key), headers: [{"x-amz-copy-source", source}]) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        if copy_body_ok?(body), do: :ok, else: {:error, {:s3_copy_error_body, body}}

      {:ok, %{status: status}} ->
        {:error, {:s3_copy_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A CopyObject can return 200 OK then stream an `<Error>` XML body when the copy
  # fails AFTER response headers are sent (expert review #8) — so status alone is not
  # proof of success. A real success carries `<CopyObjectResult>`; treat an `<Error>`
  # body as failure so the steal-touch fence (touch_object) and the migration
  # retain/restore backups can't silently no-op (a lost fence is split-brain; a
  # lost backup is an unrecoverable revert). An empty / non-XML 2xx body (some
  # S3-compatible stores) is taken as success. NOTE: CopyObject also caps at 5 GB per
  # request; a multipart-copy path for >5 GB shards is deferred (fathom's thesis is
  # millions of SMALL shards — see the gated CopyObject work, round-2 #4).
  defp copy_body_ok?(body) when is_binary(body), do: not String.contains?(body, "<Error")
  defp copy_body_ok?(_), do: true

  # --- leasing ---

  @impl true
  def lease_holder(shard_id) do
    now = Storage.now_ms()

    case get_lock(shard_id) do
      {:ok, %{owner: other, expires_at_ms: lock_exp}, _etag} ->
        case owner_live?(shard_id, other, now, lock_exp) do
          :live -> {:held, other}
          :dead -> :free
          {:error, reason} -> {:error, reason}
        end

      :not_found ->
        :free

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A released marker left by a DIRTY release (expert review 2026-10-13 #7): see
  # `Storage.with_dirty_release/1`. A read error is returned, not swallowed, so the caller fails closed.
  @impl true
  def released_dirty?(shard_id) do
    case get_lock(shard_id) do
      {:ok, %{dirty: true}, _etag} -> true
      {:ok, _lease, _etag} -> false
      :not_found -> false
      {:error, reason} -> {:error, reason}
    end
  end

  # The steal instant on the CALLER's clock, or `nil` when it cannot be computed. Shares
  # `stealable_at/4` with `lease_stealable_at/1` — and the clock normalisation with it — so the
  # instant an acquire hands back and the instant a later probe would read are the same number.
  # See #13 for why the raw value cannot be returned: it is on S3's Date clock when the store
  # sends one, and diffing that against `System.system_time/1` bakes the skew into the answer.
  defp steal_instant(shard_id, other, now, lock_exp) do
    case stealable_at(shard_id, other, now, lock_exp) do
      {:ok, at, ref_now} -> at + (now - ref_now)
      _ -> nil
    end
  end

  @impl true
  def lease_stealable_at(shard_id) do
    now = Storage.now_ms()

    case get_lock(shard_id) do
      {:ok, %{owner: other, expires_at_ms: lock_exp}, _etag} ->
        case stealable_at(shard_id, other, now, lock_exp) do
          # Normalise onto the CALLER's clock. `stealable_at/3` compares against S3's Date when
          # the store sends one (#13), so the instant it returns is on that clock — handing it
          # back raw would have the caller diff it against `System.system_time/1` and bake the
          # skew straight into the answer, which is the thing #13 exists to keep out.
          {:ok, at, ref_now} -> {:held, other, at + (now - ref_now)}
          {:error, reason} -> {:error, reason}
        end

      :not_found ->
        :free

      {:error, reason} ->
        {:error, reason}
    end
  end

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
      # A same-owner reclaim is a takeover from a previous coordinator of THIS node that may have
      # shipped under its claim, so it claims above it like a steal (expert review 2026-09-29 #3).
      # An unreadable claim fails closed BEFORE the write (2026-10-12 #21; see
      # `Storage.with_takeover_claim/3`), so the existing lock and its claim survive for the retry.
      {:ok, %{owner: ^owner, epoch: epoch} = existing, etag} ->
        with {:ok, reclaimed} <-
               Storage.with_takeover_claim(
                 %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms},
                 shard_id,
                 Map.get(existing, :claim)
               ) do
          put_lock(shard_id, reclaimed, if_match: etag)
        end

      # Someone else holds it — liveness is *their heartbeat* (with the lock's own TTL as the
      # fallback when they run no heartbeat — see owner_live?/3).
      {:ok, %{owner: other, epoch: epoch, expires_at_ms: lock_exp} = dead_lock, etag} ->
        case owner_live?(shard_id, other, now, lock_exp) do
          :live ->
            # CARRIES THE STEAL INSTANT (expert review 2026-08-26 #23). `owner_live?/4` and
            # `stealable_at/4` are the same rule read two ways, and this branch has just paid for
            # the reads both need — so handing the instant back saves `Shards.held_retry/4` a
            # whole `lease_stealable_at/1` round trip on the first held error. `nil` when it
            # cannot be computed, which the caller already treats as "poll instead of aim".
            {:error, {:held, other, steal_instant(shard_id, other, now, lock_exp)}}

          :dead ->
            # The new lock records the lineage this owner claims, strictly above anything the dead
            # owner could have shipped under (expert review 2026-09-29 #3). Read BEFORE the PUT so it
            # rides the steal write itself — no extra lock write per takeover.
            # Fails closed BEFORE the write when the claim is unreadable (2026-10-12 #21).
            with {:ok, stolen} <-
                   Storage.with_takeover_claim(
                     %{owner: owner, epoch: epoch + 1, expires_at_ms: now + ttl_ms},
                     shard_id,
                     Map.get(dead_lock, :claim)
                   ) do
              case put_lock(shard_id, stolen, if_match: etag) do
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
                    {:ok, %{pre: pre, post: post}} ->
                      # Thread the touch's lineage through the lease (round-2 #6):
                      # the takeover revalidation needs the SOURCE etag the touch
                      # If-Matched to tell "our own lineage, moved no bytes" from a
                      # zombie flush laundered into the touched object.
                      # The touch's ORIGIN chains through a clean released marker's provenance
                      # (expert review 2026-10-12 #10), so a holder whose local copy predates an
                      # intermediate clean visitor's touch is recognised as unforked.
                      {:ok,
                       lease
                       |> Map.put(:took_over, true)
                       # Who we stole from, so promote-on-open can exclude that node from the
                       # recovery responder floor (expert review 2026-10-13 #14).
                       |> Map.put(:prev_owner, other)
                       |> Map.put(:touch_pre_etag, pre)
                       |> Map.put(:touch_post_etag, post)
                       |> Map.put(:touch_origin_etag, Storage.touch_origin(dead_lock, pre))}

                    # Fail closed: an un-fenced steal is not a steal — but the epoch+1
                    # lock write already LANDED, and leaving it would let our own next
                    # checkout RECLAIM it (same owner + epoch) with no `took_over`:
                    # the zombie fence and the takeover revalidation both skipped for a
                    # takeover that was never fenced (expert review round-2 #20).
                    #
                    # Roll back to the dead owner's identity but at epoch + 2, NEVER back to
                    # `epoch` (expert review 2026-08-01 #10). The old rollback wrote `epoch`
                    # after `epoch + 1` had been durably visible — moving the fencing token
                    # BACKWARD, and the epoch's monotonicity is the single invariant the
                    # whole single-writer design rests on. Concretely it let the zombie's
                    # lease `{other, epoch}` match `check_lease/2` again, so it could
                    # conclude it was still the owner; and `touch_object` can fail AFTER its
                    # rotation copy landed (the confirm-rotation HEAD erroring transiently),
                    # leaving the data object's etag rotated — fencing the zombie's If-Match
                    # — while the lock said the zombie still owned it.
                    #
                    # NOT a delete, though that is the tidier-looking fix and what the review
                    # first proposed: with no lock object the next `acquire_lease` takes the
                    # `:not_found` branch, which is a fresh `create_lock` at **epoch 1**. That
                    # is both a bigger backward jump and a takeover with NO steal-touch —
                    # exactly the unfenced reclaim round-2 #20 added this rollback to prevent
                    # (`S3StealTouchRollbackTest` pins it).
                    #
                    # `epoch + 2` satisfies both at once: strictly greater than anything an
                    # observer saw, so monotonic; a foreign owner, so our retry re-enters the
                    # FULL steal path (epoch bump + touch) instead of the same-owner reclaim;
                    # and `{other, epoch}` no longer matches, so the zombie reads superseded.
                    # Best-effort — if the rollback itself fails, the etag data-fence still
                    # backstops a clobber.
                    {:error, reason} ->
                      # Keeps the dead owner's recorded claim (#3), so the retry's takeover still
                      # goes above it.
                      restore_lock(
                        shard_id,
                        stolen,
                        Map.merge(
                          %{owner: other, epoch: epoch + 2, expires_at_ms: lock_exp},
                          Map.take(dead_lock, [:claim])
                        )
                      )

                      {:error, {:transient_lookup, {:touch_failed, reason}}}
                  end

                other ->
                  other
              end
            end

          # Fail closed: don't steal a possibly-live owner on a heartbeat read blip.
          {:error, reason} ->
            {:error, {:transient_lookup, reason}}
        end

      :not_found ->
        case create_lock(shard_id, %{owner: owner, epoch: 1, expires_at_ms: now + ttl_ms}) do
          # RE-CREATED, and marked so (expert review 2026-10-17 #1): the lock existed when our first
          # create 412'd and is gone now, so its holder released in between — possibly right after a
          # final flush that the open's speculative pull (issued before our first create) missed.
          # The open re-checks the pulled etag for this lease only (`Shard.cold_fresh_check/5`);
          # the uncontended first-try create stays 1 RTT.
          {:ok, lease} -> {:ok, Map.put(lease, :recreated, true)}
          # Someone else created it in the gap — they hold it now.
          # Neither owner nor instant is known here: the winner's lock was written in the gap and
          # has not been read. `nil` is "no instant", which routes the caller to its ordinary
          # backoff rather than to a fabricated deadline.
          :exists -> {:error, {:held, :unknown, nil}}
          {:error, _reason} = error -> error
        end

      # AN UNDECODABLE LOCK IS ITS OWN, LOUD ERROR (expert review 2026-10-15 #23, D14). It used to
      # ride the generic clause below as `{:transient_lookup, :corrupt_lock}`, which reads as "retry
      # shortly" -- but nothing retried will ever fix a lock body that does not decode, and the
      # migrator treats `:transient_lookup` as retryable forever. NOT auto-repaired on either backend:
      # the body carried the epoch and the claim, so a rewrite from here would have to guess them.
      # The remedy is the operator task `mix fathom.lock.repair` (`repair_corrupt_lock/2`).
      {:error, :corrupt_lock} ->
        report_corrupt_lock(shard_id)
        {:error, :corrupt_lock}

      # Fail closed: a transient lookup error must NOT fall through to an
      # unconditional write that would steal a live owner's lease.
      {:error, reason} ->
        {:error, {:transient_lookup, reason}}
    end
  end

  defp report_corrupt_lock(shard_id) do
    Logger.error(
      "shard #{shard_id}: lock object #{lock_path(shard_id)} is undecodable -- the shard stays " <>
        "CLOSED. It is NOT auto-repaired: the lock's epoch and claim are lost with it. Inspect " <>
        "the bucket, then run `mix fathom.lock.repair #{shard_id}` (docs/runbooks/lock-repair.md)."
    )

    :telemetry.execute([:fathom, :storage, :lock_corrupt], %{count: 1}, %{shard_id: shard_id})
  end

  @doc """
  Operator repair of an UNDECODABLE lock (`mix fathom.lock.repair`, expert review 2026-10-15 #23).

  `build` receives the raw lock bytes and returns the lease to write. The write is `If-Match` the
  etag of the corrupt body we just read, so a concurrent writer that fixed or replaced the lock wins
  and the repair reports `{:error, :precondition_failed}` rather than clobbering it. Refuses with
  `{:error, :lock_not_corrupt}` unless the lock really is undecodable, `{:error, :no_lock}` if absent.
  """
  @spec repair_corrupt_lock(String.t(), (binary() -> {:ok, Storage.lease()} | {:error, term()})) ::
          {:ok, Storage.lease()} | {:error, term()}
  def repair_corrupt_lock(shard_id, build) do
    case Req.get(req(), url: lock_path(shard_id), decode_body: false) do
      {:ok, %{status: 200, body: body, headers: headers}} when is_binary(body) ->
        with :error <- corrupt_or_error(Storage.decode_lease(body)),
             {:ok, marker} <- build.(body) do
          case etag(headers) do
            etag when is_binary(etag) -> put_lock(shard_id, marker, if_match: etag)
            _ -> {:error, :no_lock_etag}
          end
        end

      {:ok, %{status: 404}} ->
        {:error, :no_lock}

      {:ok, %{status: status}} ->
        {:error, {:s3_get_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp corrupt_or_error(:error), do: :error
  defp corrupt_or_error({:ok, _}), do: {:error, :lock_not_corrupt}

  # Is `other` still alive? Its heartbeat object is the primary source of truth. Goes
  # through the Storage dispatch (resolves to this backend in prod) so a faulty
  # backend can inject a heartbeat-read failure for the fail-closed test. The
  # steal_margin absorbs inter-node clock skew (see Storage.steal_margin_ms/0): only
  # steal once the heartbeat is expired by more than the margin.
  defp owner_live?(shard_id, other, now, lock_expires_at_ms) do
    case stealable_at(shard_id, other, now, lock_expires_at_ms) do
      {:ok, at, ref_now} -> if ref_now <= at, do: :live, else: :dead
      {:error, _} = error -> error
    end
  end

  # The instant `other`'s hold becomes stealable, plus the reference clock it should be compared
  # against (S3's own Date when the store sent one — see the skew reasoning below). THE single
  # source of the liveness rule: both `owner_live?/3` (is ref_now past it) and
  # `lease_stealable_at/1` (how long until it) derive from this, so a caller PREDICTING a steal
  # cannot disagree with the code PERFORMING it.
  #
  # That divergence was a real bug: `Shards.holder_stealable_soon?/2` predicted from the heartbeat
  # ALONE while this required both signals lapsed (#12), so a checkout could hold and retry its
  # whole crash-failover budget waiting for a steal that could not happen yet, then return the
  # error it would have returned immediately.
  defp stealable_at(shard_id, other, now, lock_expires_at_ms) do
    margin = Storage.steal_margin_ms()

    case cached_read_heartbeat_dated(other) do
      # Verify the heartbeat body's owner matches (expert review round-2 #3, defense in
      # depth): with the per-owner key this always holds, but never trust a mismatched
      # body to declare `other` live.
      {:ok, %{owner: ^other, expires_at_ms: exp}, s3_now} ->
        # Compare against S3's OWN clock (its response Date), not this reader's local clock (#13). A
        # reader whose clock stepped forward (VM live-migration, a bad NTP step, a hypervisor pause
        # resumed with a jumped RTC) would otherwise see a live owner's fresh heartbeat as expired and
        # wrongfully steal — the victim then self-fences and QUARANTINES its acked writes, turning a
        # skew event into data loss. S3's Date is a clock both sides share. Fall back to the local
        # `now` only when the store returned no Date. (Owner-clock skew — the owner stamping `exp`
        # wrong — is unaddressed here; the frozen-vs-advancing double-read is the follow-up for it.)
        maybe_emit_skew(other, now, s3_now)

        # `max/2` is #12: the heartbeat lapsing alone does not make the OWNER dead, never
        # consulting the lock's TTL even if the owner renewed it seconds ago. That was exactly
        # backwards from the `:not_found` branch below, which DOES fall back to the lock TTL.
        #
        # And the asymmetry is reachable by design: when the Heartbeat GenServer dies abnormally
        # it deliberately LEAVES its object behind, and coordinators degrade to the legacy
        # per-shard renew fence — a node that is healthy, serving, and renewing every lock it
        # holds. Under the old logic a single process failure (or a `heartbeat_server: false`
        # config migration) made that node's ENTIRE keyspace slice instantly stealable, with
        # per-shard loss up to a flush interval and a `.fenced.<ts>` quarantine each, while it
        # kept serving. A stale-but-present object was strictly WORSE than an absent one.
        #
        # An owner is dead only when BOTH its heartbeat and its lock TTL have lapsed.
        {:ok, max(exp, lock_expires_at_ms) + margin, s3_now || now}

      # A heartbeat object that isn't `other`'s — treat as no signal and fall back to the
      # lock's own TTL, same as :not_found.
      {:ok, _mismatch, s3_now} ->
        {:ok, lock_expires_at_ms + margin, s3_now || now}

      # No heartbeat object at all (`heartbeat_server: false` legacy mode, or the owner's
      # heartbeat was cleared): fall back to the lock's OWN TTL for liveness (finding #11).
      # Without this, a live owner that renews its lock per-shard (the legacy fence) looks
      # instantly dead and any contender steals it. The heartbeat stays primary; this is the
      # pre-heartbeat lease-TTL fence, applied only when there is no heartbeat to consult.
      # Exception (round-2 #34): this node's PROVEN-DEAD previous incarnation (heartbeat
      # verified stale/frozen and cleared) — its recently-renewed locks must not block
      # the restarted node for TTL+margin. Exact owner match only. Stealable at 0 ⇒ now.
      {:not_found, s3_now} ->
        # NOW ALSO REQUIRES THE RENEWAL PROBE (expert review 2026-08-20 #10). A heartbeat proof
        # says the heartbeat OBJECT stopped; it does not say the process did. See
        # `Storage.fast_steal_ok?/4` for why the finding's own condition was rejected.
        if Storage.fast_steal_ok?(other, shard_id, lock_expires_at_ms, s3_now || now),
          do: {:ok, 0, s3_now || now},
          else: {:ok, lock_expires_at_ms + margin, s3_now || now}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Observability for clock skew (#13): the difference between this reader's local clock and S3's
  # response Date, so operators can watch skew on a metric rather than out-of-band NTP monitoring — a
  # large skew is what would (pre-#13) have driven wrongful steals. No-op when the store sent no Date.
  defp maybe_emit_skew(_owner, _local_now, nil), do: :ok

  defp maybe_emit_skew(owner, local_now, s3_now) do
    :telemetry.execute([:fathom, :shard, :clock_skew], %{skew_ms: local_now - s3_now}, %{
      owner: owner
    })
  end

  # Create the lock only if it does not exist (`If-None-Match: *`). `:exists` on a
  # 412 — no extra read, the caller decides what to do next. The PUT response's etag is
  # carried in the lease (client-side only — encode_lease never serializes it) so
  # renew/release can fence conditionally WITHOUT re-reading the lock: the old shape
  # threw the etag away and re-GET'd the object we ourselves just wrote, one whole RTT
  # per drain/renew (review 2026-07-23 #6 — the measured ~3.5-RTT drain's third trip).
  defp create_lock(shard_id, lease) do
    case Req.put(req(),
           url: lock_path(shard_id),
           body: Storage.encode_lease(lease),
           headers: [{"if-none-match", "*"}]
         ) do
      {:ok, %{status: status, headers: headers}} when status in 200..299 ->
        {:ok, put_lock_etag(lease, headers)}

      {:ok, %{status: 412}} ->
        :exists

      {:ok, %{status: status}} ->
        {:error, {:s3_put_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Attach the lock object's etag from a successful lock-PUT response. Some stores could
  # omit the header — then the lease simply has no cached etag and renew/release take the
  # legacy read-then-fence path.
  defp put_lock_etag(lease, headers) do
    case etag(headers) do
      etag when is_binary(etag) -> Map.put(lease, :lock_etag, etag)
      _ -> lease
    end
  end

  # A renew rewrites the lock body with only the claim, and the returned lease is what the
  # coordinator keeps in `state.lease`. The steal's touch keys live ONLY in that in-memory lease
  # (they are not part of the lock body), so a legacy-mode coordinator that renewed lost them and
  # `release_clean`'s `Storage.with_clean_touch/1` then attached no provenance: the previous
  # holder's acked warm copy later read as a fork (expert review 2026-10-13 N2, same class as
  # 975b80c7). Carried here, in the one place every renew path (renew task, flush fence, lapse
  # revalidation) goes through. Per-op cost: one three-key `Map.take` on a successful renew.
  @impl true
  def renew_lease(shard_id, lease, ttl_ms) do
    case do_renew_lease(shard_id, lease, ttl_ms) do
      {:ok, renewed} ->
        {:ok,
         Map.merge(
           renewed,
           Map.take(lease, [:touch_pre_etag, :touch_post_etag, :touch_origin_etag])
         )}

      other ->
        other
    end
  end

  defp do_renew_lease(shard_id, %{owner: owner, epoch: epoch} = lease, ttl_ms) do
    now = Storage.now_ms()

    # Carries the claim this lease wrote (#3): a renew rewrites the whole lock body, and dropping it
    # would let a later successor fall back to the fresh-create bound and collide with our lineage.
    renewed =
      Map.merge(
        %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms},
        Map.take(lease, [:claim])
      )

    case lease do
      # Fast path (review 2026-07-23 #6): we cached the lock's etag when WE last wrote it
      # (create/put/renew all return it), so renew is one conditional PUT — 1 RTT, not the
      # legacy GET-then-PUT's 2.
      #
      # A 412 does NOT mean "another writer" (expert review 2026-08-24 #8). The comment here
      # used to say it "can only be another writer", and that is false for a node racing
      # ITSELF: in legacy mode a coordinator has two independent producers of renew PUTs
      # against the same cached `lock_etag` — the `:renew_lease` timer, and the durability
      # flush task's own `Fence.check`, which takes `Fence.legacy/2` and renews too. The flush
      # fences first and replies only after the whole VACUUM INTO + PUT completes, so its
      # refreshed etag is held inside the task while `state.lease` stays stale for the entire
      # flush — seconds at real S3. Any timer tick landing in that window 412s against our own
      # write, and the coordinator read `:superseded` as a steal, self-fenced, and
      # `quarantine_fenced!`d a dirty local WITHOUT flushing it: every un-flushed acked write
      # lost, plus a false split-brain alarm, with no peer involved.
      %{lock_etag: etag} when is_binary(etag) ->
        case put_lock(shard_id, renewed, if_match: etag) do
          {:ok, _} = ok -> ok
          # The re-read lock names OUR OWN owner string — `owner` carries this node's boot
          # incarnation, so this cannot be another node. Confirm the epoch and retry.
          {:error, {:held, ^owner, _at}} -> renew_after_own_write(shard_id, renewed, owner, epoch)
          {:error, {:held, _, _at}} -> {:error, :superseded}
          {:error, :precondition_failed} -> {:error, :superseded}
          err -> err
        end

      # Legacy path — a lease with no cached etag (decoded from an old lock body, or the
      # store omitted the header): read the lock to learn it, then fence the PUT.
      _ ->
        case get_lock(shard_id) do
          {:ok, %{owner: ^owner, epoch: ^epoch}, etag} ->
            case put_lock(shard_id, renewed, if_match: etag) do
              {:ok, _} = ok -> ok
              {:error, {:held, _, _at}} -> {:error, :superseded}
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
  end

  # A renew that 412'd against a lock still naming our own `{owner, epoch}` lost a race with
  # OURSELVES, not with a peer — the flush fence's renew and the renew timer both write this
  # object (expert review 2026-08-24 #8). Re-read to learn the etag our other writer just left,
  # and re-issue the renew fenced on it, so the TTL actually advances rather than the caller
  # being told it was superseded.
  #
  # SAFE AGAINST A GENUINE TAKEOVER, which is the whole question. `owner` carries this node's
  # boot incarnation, so a different node cannot match it; and a steal always BUMPS the epoch
  # (`acquire_lease`'s steal path), so a lock at our owner and our epoch cannot be a peer's. If
  # either differs we fall through to `:superseded` exactly as before. Bounded: one extra
  # GET + PUT, no loop — a second 412 here means the object really is moving under us.
  defp renew_after_own_write(shard_id, renewed, owner, epoch) do
    case get_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}, fresh_etag} ->
        case put_lock(shard_id, renewed, if_match: fresh_etag) do
          {:ok, _} = ok -> ok
          _ -> {:error, :superseded}
        end

      _ ->
        {:error, :superseded}
    end
  end

  @impl true
  # Fast path (review 2026-07-23 #6): the lease carries the etag of the lock object WE last
  # wrote, so release is one conditional DELETE — this was the drain path's third RTT (the
  # GET below existed only to re-learn an etag the lock PUT had already returned). The fence
  # semantics are identical: If-Match our own write, and a 412 means the lock is no longer
  # ours (a stealer wrote in the gap — finding #22) so leave it and no-op.
  def release_lease(shard_id, %{lock_etag: etag} = lease) when is_binary(etag) do
    case Req.delete(req(), url: lock_path(shard_id), headers: [{"if-match", etag}]) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      # A 412 means "the lock is not the object I wrote". That is TWO different situations and
      # collapsing them to :ok is how a lock gets stranded silently — see resolve_412_release/2.
      {:ok, %{status: 412}} -> resolve_412_release(shard_id, lease)
      {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

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

  # The release that keeps the holder's lineage claim (expert review 2026-10-12 #3 — see
  # `Storage.release_lease/3`): the SAME conditions as `release_lease/2` — If-Match the lock we last
  # wrote, the same 412 disambiguation — but a conditional PUT of the released marker where that
  # path DELETEs. A lock that is no longer ours is left alone, as there.
  @impl true
  def release_lease_keeping_claim(shard_id, %{lock_etag: etag} = lease, claim)
      when is_binary(etag) do
    marker = Storage.released_marker(lease, claim)

    case put_lock(shard_id, marker, if_match: etag) do
      {:ok, _} ->
        :ok

      # `put_lock/3` folds its 412 into these two (it re-reads the holder). Either way "the lock is
      # not the object I wrote" — the same two situations `resolve_412_release/2` tells apart.
      {:error, :precondition_failed} ->
        resolve_412_marker(shard_id, lease, marker)

      {:error, {:held, _, _}} ->
        resolve_412_marker(shard_id, lease, marker)

      {:error, _} = error ->
        error
    end
  end

  def release_lease_keeping_claim(shard_id, %{owner: owner, epoch: epoch} = lease, claim) do
    case get_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}, etag} ->
        put_marker(shard_id, Storage.released_marker(lease, claim), etag)

      _ ->
        :ok
    end
  end

  defp resolve_412_marker(shard_id, lease, marker) do
    Storage.resolve_stale_release(
      shard_id,
      lease,
      fn -> read_own_lock(shard_id, lease) end,
      fn fresh_etag -> put_marker(shard_id, marker, fresh_etag) end
    )
  end

  defp read_own_lock(shard_id, %{owner: owner}) do
    case get_lock(shard_id) do
      {:ok, %{owner: ^owner}, etag} -> {:ours, etag}
      {:ok, _other, _etag} -> :not_ours
      :not_found -> :not_ours
      {:error, reason} -> {:error, reason}
    end
  end

  # Raced again between the read and this write ⇒ someone else's now; leave it, as the delete does.
  defp put_marker(shard_id, marker, etag) do
    case put_lock(shard_id, marker, if_match: etag) do
      {:ok, _} -> :ok
      {:error, :precondition_failed} -> :ok
      {:error, {:held, _, _}} -> :ok
      {:error, _} = error -> error
    end
  end

  # The backend half of the conditional-release 412: report the FACT (is the lock still ours, at
  # what etag) and perform the retry delete. The DECISION lives in
  # `Fathom.Shard.Storage.resolve_stale_release/4` so every etag-carrying backend makes it
  # identically and the default test suite can exercise it — see that function for why a 412 is
  # two different situations.
  defp resolve_412_release(shard_id, %{owner: owner} = lease) do
    Storage.resolve_stale_release(
      shard_id,
      lease,
      fn ->
        case get_lock(shard_id) do
          {:ok, %{owner: ^owner}, etag} -> {:ours, etag}
          {:ok, _other, _etag} -> :not_ours
          :not_found -> :not_ours
          {:error, reason} -> {:error, reason}
        end
      end,
      fn fresh_etag ->
        case Req.delete(req(), url: lock_path(shard_id), headers: [{"if-match", fresh_etag}]) do
          {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
          # Raced again — someone took it between the read and this delete. Theirs now; leave it.
          {:ok, %{status: 412}} -> :ok
          {:ok, %{status: status}} -> {:error, {:s3_delete_status, status}}
          {:error, reason} -> {:error, reason}
        end
      end
    )
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
  # /2 keeps the two-element contract (the store clock is a /3-only concern — expert review
  # 2026-09-05 #16), so it strips the third element /3 now carries. /3 does the work.
  def renew_heartbeat(owner, ttl_ms) do
    case renew_heartbeat(owner, ttl_ms, []) do
      {:ok, hb, _store_now} -> {:ok, hb}
      other -> other
    end
  end

  @impl true
  def renew_heartbeat(owner, ttl_ms, opts) do
    hb = %{owner: owner, expires_at_ms: Storage.now_ms() + ttl_ms}

    # BOUNDED PER ATTEMPT (expert review 2026-08-26 #14). `build_req/1` sets `connect_timeout` but
    # neither `receive_timeout` (Req default 15 000 ms) nor `pool_timeout` (Finch default
    # 5 000 ms), so one stalled attempt could occupy ~23 s against the heartbeat's 10 s cadence and
    # 30 s TTL — losing two renewals and putting the whole node at `:not_valid` from nothing worse
    # than pool contention. The caller owns the number because only it knows its cadence.
    #
    # Both halves are needed and they bound different things: `pool_timeout` is how long we wait
    # for a CONNECTION (the one a warming or failover burst exhausts first, which is the actual
    # trigger this finding is about), `receive_timeout` how long we wait for the RESPONSE.
    timeouts =
      case Keyword.get(opts, :budget_ms) do
        ms when is_integer(ms) and ms > 0 -> [receive_timeout: ms, pool_timeout: ms]
        _ -> []
      end

    case Req.put(
           req(),
           [url: heartbeat_path(owner), body: Storage.encode_heartbeat(hb)] ++ timeouts
         ) do
      # Return the store's Date so the caller can detect OWNER-CLOCK SKEW (expert review
      # 2026-09-05 #16). #13 fixed the reader (a stealer compares against S3's Date); this closes
      # the owner side — `hb.expires_at_ms` was stamped from `Storage.now_ms()`, this node's wall
      # clock, and a lagging clock makes a peer read the lease stealable while our own monotonic
      # deadline still says :ok. `s3_date_ms/1` is `nil` when the header is absent/unparseable, and
      # the caller then skips skew detection rather than guessing.
      {:ok, %{status: status} = resp} when status in 200..299 -> {:ok, hb, s3_date_ms(resp)}
      {:ok, %{status: status}} -> {:error, {:s3_put_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def read_heartbeat(owner) do
    case read_heartbeat_dated(owner) do
      {:ok, hb, _s3_now} -> {:ok, hb}
      {:not_found, _s3_now} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  # Memoized heartbeat read (review 2026-07-23 #13): a mass failover steals many shards from
  # the SAME dead owner, and every steal re-read the identical heartbeat/<owner> object —
  # ~N redundant GETs of one object on the takeover critical path. Cache the raw read per
  # owner for @hb_cache_ttl_ms. Staleness bound: the cached verdict (including its s3_now
  # clock sample) is at most the TTL old, which must stay well inside steal_margin_ms
  # (default 5000 ms) — the margin absorbs exactly this class of skew. Errors are never
  # cached, so the fail-closed no-steal-on-blip behavior stays per-call.
  # DERIVED from the steal margin, not a hardcoded 1000 (expert review 2026-08-01 #40). The
  # safety condition — "the TTL must stay well inside `steal_margin_ms`" — was stated in the
  # HeartbeatCache moduledoc and enforced nowhere, while `steal_margin_ms` is operator-tunable
  # and can be set to 500 ms or 0. Deriving it makes the condition STRUCTURAL: ttl is always
  # margin/5, so it cannot be violated by config and the boot assertion the review asked for
  # would be dead code. Capped at the original 1 s so the default behaviour is unchanged.
  #
  # NOT the review's "cheaper still" alternative — cache only the raw read and re-stamp `ref_now`
  # with a fresh local clock. That is BACKWARDS, and in the dangerous direction. The cached value
  # is `{hb, s3_now}` TOGETHER, so a hit replays a self-consistent comparison. Re-stamping a fresh
  # `now` against a STALE `exp` makes `ref_now <= exp + margin` progressively more likely to be
  # FALSE as the entry ages — so an owner that was alive at read time and has renewed since would
  # be declared dead and stolen from. That is a wrongful steal of a live node, which is precisely
  # what #13's whole store-clock apparatus exists to prevent; the review's claim that a stale
  # entry "can only ever produce a more conservative (`:live`) verdict" has the sign inverted.
  #
  # The residual behaviour the finding correctly names — a cached `:dead` can be replayed for an
  # owner that recovered inside the window — is bounded by this TTL, which is the reason to shrink
  # it with the margin rather than to re-stamp it.
  @doc false
  # Public (@doc false) only so the derivation itself is testable: the property that matters is
  # the RELATIONSHIP to the margin, and a test that re-derives the formula locally would pass no
  # matter what this does.
  def hb_cache_ttl_ms do
    case Storage.steal_margin_ms() do
      margin when is_integer(margin) and margin > 0 -> min(1_000, div(margin, 5))
      _ -> 0
    end
  end

  defp cached_read_heartbeat_dated(owner) do
    # Single-flight (expert review 2026-10-17 #29): concurrent steals from one dead owner share one GET.
    Fathom.Shard.Storage.HeartbeatCache.fetch(owner, hb_cache_ttl_ms(), fn ->
      read_heartbeat_dated(owner)
    end)
  end

  # read_heartbeat, also returning S3's own clock (its response `Date` header, epoch-ms, or nil if
  # absent/unparseable) so the steal decision can compare against a SHARED clock instead of this
  # reader's local one (#13). owner_live?/3 is the only caller that needs the date.
  defp read_heartbeat_dated(owner) do
    case Req.get(req(), url: heartbeat_path(owner)) do
      {:ok, %{status: 200, body: body} = resp} ->
        case Storage.decode_heartbeat(body) do
          {:ok, hb} -> {:ok, hb, s3_date_ms(resp)}
          :error -> {:error, :corrupt_heartbeat}
        end

      {:ok, %{status: 404} = resp} ->
        {:not_found, s3_date_ms(resp)}

      {:ok, %{status: status}} ->
        {:error, {:s3_get_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Epoch-ms of an S3 response's `Date` header (the store's own clock), or nil when absent/unparseable.
  # HTTP dates are RFC 1123 in GMT; `:httpd_util.convert_request_date/1` parses them without inets running.
  defp s3_date_ms(resp) do
    with date when is_binary(date) <- date_header(resp),
         {{y, mo, d}, {h, mi, s}} <- :httpd_util.convert_request_date(String.to_charlist(date)),
         {:ok, naive} <- NaiveDateTime.new(y, mo, d, h, mi, s) do
      naive |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:millisecond)
    else
      _ -> nil
    end
  end

  defp date_header(resp) do
    case resp.headers["date"] do
      [v | _] when is_binary(v) -> v
      v when is_binary(v) -> v
      _ -> nil
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
      {:ok, %{status: status, headers: headers}} when status in 200..299 ->
        {:ok, put_lock_etag(lease, headers)}

      # Another node wrote between our read and our conditional write.
      {:ok, %{status: 412}} ->
        case get_lock(shard_id) do
          {:ok, %{owner: owner}, _etag} -> {:error, {:held, owner, nil}}
          _ -> {:error, :precondition_failed}
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_put_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Roll a failed steal's lock write back to the prior (dead) owner's content
  # (expert review round-2 #20), so the next acquire re-enters the steal path —
  # epoch bump + data-object touch — instead of RECLAIMING an unfenced takeover.
  # Conditional on the lock still being exactly our failed-steal write; any
  # concurrent change wins and the rollback no-ops.
  # Conditionally rewrite a lock WE wrote, only while it is still ours. The caller supplies the
  # replacement content; see the `touch_failed` branch for why it is the previous owner at
  # `epoch + 2` rather than at `epoch` (monotonicity) or absent (an unfenced epoch-1 reclaim).
  # Bounded so a TRANSIENT put failure does not strand the failed-steal lock (expert review
  # 2026-09-18 #21). The old code did `_ = put_lock(...)` — one attempt, result discarded — so a
  # correlated brownout that failed BOTH the touch and this rollback left the lock at our own
  # `{owner, epoch+1}`. The IMMEDIATE outer checkout retry then reads its OWN owner and takes the
  # same-owner RECLAIM path (re-PUT, no steal-touch, no `took_over`), skipping the zombie data-etag
  # fence and the takeover revalidation. Retrying the rollback closes the common transient case:
  # once it lands `{other, epoch+2}`, the retry re-enters the FULL steal path instead.
  @restore_lock_attempts 3

  defp restore_lock(shard_id, ours, replacement),
    do: restore_lock(shard_id, ours, replacement, @restore_lock_attempts)

  defp restore_lock(shard_id, ours, replacement, attempts) do
    case get_lock(shard_id) do
      {:ok, %{owner: o, epoch: e}, etag} when o == ours.owner and e == ours.epoch ->
        case put_lock(shard_id, replacement, if_match: etag) do
          {:ok, _} ->
            :ok

          # A concurrent write already replaced our stranded lock with a foreign owner (412): the
          # next acquire re-enters the steal path anyway, so there is nothing left to roll back.
          {:error, {:held, _, _}} ->
            :ok

          {:error, :precondition_failed} ->
            :ok

          # A transient storage error — the correlated-brownout case this guard exists for. Retry;
          # leaving our own `{owner, epoch+1}` in place is what lets the immediate outer retry reclaim
          # it unfenced.
          {:error, _reason} when attempts > 1 ->
            restore_lock(shard_id, ours, replacement, attempts - 1)

          {:error, reason} = err ->
            Logger.error(
              "shard #{shard_id}: could NOT roll back a failed steal-touch lock after " <>
                "#{@restore_lock_attempts} attempts (#{inspect(reason)}). The lock is stranded at " <>
                "our own epoch+1 and the next checkout would RECLAIM it UNFENCED; the data-object " <>
                "etag fence is the remaining backstop (expert review 2026-09-18 #21)."
            )

            err
        end

      _ ->
        :ok
    end
  end

  defp etag(headers) do
    case headers["etag"] || headers["ETag"] do
      [value | _] -> value
      value when is_binary(value) -> value
      _ -> nil
    end
  end

  # ListObjectsV2 over the shard prefix, summing live `<shard>.db` objects (excluding `.lock`,
  # retained `@version` copies, `heartbeats/`, and probes), paginated via the continuation token.
  # Expensive at fleet scale — the dashboard polls it slowly + caches; prefer S3 Inventory /
  # CloudWatch in production (see Fathom.Shard.Storage.stored_usage/0). This backend is not yet
  # exercised against a real bucket (see moduledoc), so the XML scan is deliberately best-effort.
  @impl true
  def stored_usage, do: list_usage(nil, 0, 0)

  defp list_usage(token, count, bytes) do
    with {:ok, page} <- list_page(prefix(), token) do
      {count, bytes} = tally_list(page.entries, count, bytes)

      case page.next do
        nil -> {count, bytes}
        next -> list_usage(next, count, bytes)
      end
    end
  end

  defp tally_list(entries, count, bytes) do
    Enum.reduce(entries, {count, bytes}, fn
      %{key: key, size: size}, {c, b} when is_integer(size) ->
        if live_db_object?(key), do: {c + 1, b + size}, else: {c, b}

      _no_size, acc ->
        acc
    end)
  end

  defp live_db_object?(key),
    do: String.ends_with?(key, ".db") and not String.contains?(key, "@")

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

  defp snapshot_key(shard_id, snapshot_id),
    do: prefix() <> shard_id <> "@snap-" <> snapshot_id <> ".db"

  # Virtual-hosted style carries the bucket in the host; path-style carries it in
  # the URL path (MinIO, R2).
  defp url_path(key) do
    if path_style?(), do: "/" <> fetch!(config(), :bucket) <> "/" <> key, else: "/" <> key
  end

  defp path_style?, do: config()[:path_style] == true

  # The base Req struct is invariant per config, but every S3 call rebuilt it — config
  # read, sigv4 options, response-step append, plus finch_opt's Process.whereis — which
  # is measurable allocation churn at exactly the fan-out moments (warming bursts,
  # follower polls, mass failover; review 2026-07-23 #25). Cache it in :persistent_term
  # keyed by the config term: a config change (tests swap req_plug/buckets freely) misses
  # the key and rebuilds, so correctness never depends on invalidation. The term is
  # written once per distinct config (bounded; tests write a handful), so persistent_term
  # GC churn is a non-issue. finch_opt's pool detection folds into the cached build:
  # whether our dedicated pool is up is decided at supervision-tree boot, not per call.
  defp req do
    config = config()
    key = {__MODULE__, :base_req, config, finch_running?()}

    case :persistent_term.get(key, nil) do
      nil ->
        req = build_req(config)
        :persistent_term.put(key, req)
        req

      req ->
        req
    end
  end

  defp build_req(config) do
    Req.new(
      [
        base_url: base_url(config),
        aws_sigv4: [
          access_key_id: fetch!(config, :access_key_id),
          secret_access_key: fetch!(config, :secret_access_key),
          token: config[:token],
          service: :s3,
          region: region(config)
        ],
        # Req's `exp_backoff` starts at 1000 ms, sized for public-internet APIs. Against a
        # same-region S3 whose entire cold-open budget is ~1 RTT (~26 ms at 10 ms one-way), one
        # transient blip on a retried GET/HEAD was a ~40× p99 spike (expert review 2026-07-24 #14).
        #
        # This only reshapes the delay for retries Req ALREADY performs (:safe_transient ⇒
        # GET/HEAD). It deliberately does NOT widen retry coverage: `flush/3`'s PUT stays
        # unretried at this layer because it is `If-Match`-fenced, and a blind re-issue would
        # replay a conditional write with a stale etag. The coordinator's own backoff is the
        # correct retry tier for a flush.
        retry_delay: &__MODULE__.retry_delay/1
      ] ++ finch_opt() ++ req_plug_opt(config)
    )
    |> Req.Request.prepend_request_steps(fathom_s3_start: &__MODULE__.meter_start/1)
    |> Req.Request.append_response_steps(fathom_s3_meter: &__MODULE__.meter/1)
    |> Req.Request.append_error_steps(fathom_s3_meter_error: &__MODULE__.meter_error/1)
  end

  @doc false
  # Retry backoff for the GET/HEAD retries Req already performs, scaled to a same-region object
  # store rather than to a public-internet API (expert review 2026-07-24 #14).
  @spec retry_delay(non_neg_integer()) :: non_neg_integer()
  def retry_delay(attempt), do: Enum.at([50, 200, 800], attempt, 800)

  @doc false
  # S3-op meter, attached as a Req response step in req/0 — the single choke point every
  # `Req.*` S3 call routes through. Emits one `[:fathom, :s3, :op]` telemetry event per response
  # tagged by HTTP method (low cardinality: get/put/head/delete), carrying the transferred byte
  # count (Prometheus counter + sum → the S3 cost / rate-limit-headroom panel). A response step
  # that raised would fail the S3 request, so the body is fully guarded and the event is a free
  # no-op when no reporter is attached (test / metrics off).
  def meter({request, %Req.Response{} = response}) do
    :telemetry.execute(
      [:fathom, :s3, :op],
      Map.put(duration(request), :count, 1) |> Map.put(:bytes, op_bytes(request, response)),
      %{op: request.method, status_class: status_class(response.status)}
    )

    {request, response}
  rescue
    _ -> {request, response}
  end

  def meter({request, other}), do: {request, other}

  @doc false
  # Request step: stamp a monotonic start so the response/error steps can emit a duration (Req
  # exposes no per-request timing). Re-stamped on each retry attempt, so the duration is the
  # final attempt's. Admin-UI observability review 2026-10-15 #22.
  def meter_start(request),
    do: Req.Request.put_private(request, :fathom_s3_t0, System.monotonic_time())

  @doc false
  # Error step: a transport failure (no response — timeout, closed, refused) was previously
  # invisible to the meter. Emits `status_class: "error"` with the duration; the exception
  # passes through untouched so retry/caller behaviour is unchanged.
  def meter_error({request, exception}) do
    :telemetry.execute(
      [:fathom, :s3, :op],
      Map.put(duration(request), :count, 1) |> Map.put(:bytes, 0),
      %{op: request.method, status_class: "error"}
    )

    {request, exception}
  rescue
    _ -> {request, exception}
  end

  defp duration(%{private: %{fathom_s3_t0: t0}}),
    do: %{duration: System.monotonic_time() - t0}

  defp duration(_), do: %{}

  # Bounded HTTP status class for the s3.op tag.
  defp status_class(s) when s in 200..299, do: "2xx"
  defp status_class(s) when s in 300..399, do: "3xx"
  defp status_class(s) when s in 400..499, do: "4xx"
  defp status_class(s) when s in 500..599, do: "5xx"
  defp status_class(_), do: "error"

  # Payload size in whichever direction the op moved: the response body for a GET, the request
  # body for a PUT/POST. S3's PUT response carries `content-length: 0`, so take the larger of the
  # two rather than preferring one side (else PUT byte volume reads 0).
  defp op_bytes(request, response) do
    max(
      header_int(Req.Request.get_header(request, "content-length")),
      header_int(Req.Response.get_header(response, "content-length"))
    )
  end

  defp header_int([v | _]) when is_binary(v) do
    case Integer.parse(v) do
      {n, _} -> n
      :error -> 0
    end
  end

  defp header_int(_), do: 0

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
    if finch_running?(), do: [finch: @finch], else: []
  end

  # Deliberately still checked per call (as part of req/0's cache key): it is a ~100ns
  # registry lookup, and keying on it keeps the cached struct correct across pool
  # start/stop (tests, iex standalone) without invalidation logic. The saved cost is the
  # Req.new/sigv4/step-append allocation churn, which dominated.
  defp finch_running?, do: Process.whereis(@finch) != nil

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
