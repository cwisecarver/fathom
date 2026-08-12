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
  # form (expert review 2026-07-14 #17). A steal-time fence rotates the etag between single-part
  # and multipart form to invalidate a zombie's If-Match, which disabled the etag-MD5 download
  # check for the new owner's first pull; this metadata survives the rotation. See
  # verify_integrity/3.
  @md5_meta "x-amz-meta-fathom-md5"

  # How much of the shard's history the object's bytes contain (Phase 2 A2 promote-on-open).
  # User metadata on the SAME PUT — S3 charges per request and ingress is free, so a stamp that
  # cost its own request would make every durability flush more expensive to buy a failover-only
  # benefit. Absent on any object written before stamping existed, which readers must treat as
  # "unknown" (⇒ never overridable), never as "empty".
  @pos_meta "x-amz-meta-fathom-pos"

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

    # Round-2 #4: the steal fence is only real if a touch produces a GENUINELY new
    # etag — on MD5-etag stores a plain self-copy does NOT rotate (same bytes, same
    # MD5). Probe BOTH alternating copy forms (single→multipart, multipart→single)
    # and refuse boot on a store where either fails to move the etag.
    probe_rotation!(key, "single→multipart touch rotation")
    probe_rotation!(key, "multipart→single touch rotation")

    _ = Req.delete(req(), url: url_path(key))
    :ok
  end

  defp probe_rotation!(key, label) do
    # The probe deliberately keeps its own HEAD-based verification (it is a boot-time
    # self-test of the STORE, so it must not trust the copy response's claimed etag the
    # way the steal path now does — review 2026-07-23 #13).
    with {:ok, etag} when not is_nil(etag) <- head_etag(key),
         {:ok, _claimed} <- rotate_etag(key, etag, []),
         {:ok, new_etag} when not is_nil(new_etag) and new_etag != etag <- head_etag(key) do
      :ok
    else
      other ->
        raise "shard storage fence self-test failed (#{label}): the steal-time touch " <>
                "did not rotate the object etag (#{inspect(other)}). On this store the " <>
                "zombie-flush fence would be a silent no-op — refusing to boot " <>
                "(expert review round-2 #4)."
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
  def flush(shard_id, local_path, expected_etag, position \\ nil) do
    # If-Match the etag we last saw (or If-None-Match:* for a brand-new shard), so the PUT
    # only lands if the object hasn't changed under us. A 412 means a stealer flushed in the
    # window since our fence check → superseded, don't clobber (finding #15). Return the new
    # object etag for the next flush's fence.
    cond_headers =
      if expected_etag, do: [{"if-match", expected_etag}], else: [{"if-none-match", "*"}]

    with_body(local_path, fn body_path, size, md5, md5_hex, enc_headers ->
      with {:ok, resp} <-
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
                 ] ++ enc_headers ++ pos_meta_header(position)
             ) do
        case resp.status do
          s when s in 200..299 -> {:ok, etag(resp.headers)}
          412 -> {:error, :superseded}
          s -> {:error, {:s3_put_status, s}}
        end
      end
    end)
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

      :zlib ->
        # The plaintext hash must NOT go through stat_and_md5: that applies the single-PUT
        # ceiling, and the plaintext is not what gets PUT.
        with {:ok, plain_hex} <- file_md5_hex(local_path),
             {:ok, tmp} <- Codec.compress_to_temp(local_path) do
          try do
            with {:ok, size, md5, _} <- stat_and_md5(tmp) do
              fun.(tmp, size, md5, plain_hex, Codec.upload_headers(:zlib))
            end
          after
            File.rm(tmp)
          end
        end
    end
  end

  # The file's MD5 in hex, with no size ceiling applied (see with_body/2).
  defp file_md5_hex(path) do
    digest =
      path
      |> File.stream!(@stream_chunk)
      |> Enum.reduce(:crypto.hash_init(:md5), &:crypto.hash_update(&2, &1))
      |> :crypto.hash_final()

    {:ok, Base.encode16(digest, case: :lower)}
  rescue
    e in File.Error -> {:error, e.reason}
  end

  # Steal-time data-object etag invalidation (expert review #3, fixed for real
  # stores by round-2 #4): the fence needs a GENUINELY new etag, but on an
  # MD5-etag store (every non-multipart, non-KMS object — exactly what fathom's
  # single-PUT uploads produce) a plain self-copy of identical bytes yields the
  # SAME etag, making the whole zombie fence a silent no-op in production.
  #
  # The rotation that works for identical bytes at ANY size: ALTERNATE COPY FORMS.
  # A single-form etag (32-hex MD5) is touched via a one-part MULTIPART copy —
  # its etag is md5(part-md5s)-1, a different form entirely; a multipart-form
  # etag ("...-N") is touched via a plain CopyObject — back to the single MD5
  # form. Either direction provably moves the etag. The rotation is then
  # CONFIRMED with a HEAD, and a non-rotating store fails the touch closed (an
  # un-fenced steal is not a steal); verify_conditional_writes!/0 also probes
  # both directions at boot.
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

    case head_object(key) do
      {:ok, nil, _, _} ->
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
      {:ok, etag, true, _} ->
        case refresh_sentinel(key, etag) do
          {:ok, post} ->
            with {:ok, post} <- resolved_post(shard_id, etag, post),
                 do: {:ok, %{pre: etag, post: post}}

          {:error, :touch_precondition} when attempt < 2 ->
            touch_object(shard_id, attempt + 1)

          {:error, _} = error ->
            error
        end

      {:ok, etag, false, md5} ->
        case rotate_etag(key, etag, md5) do
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
    case Req.head(req(), url: url_path(key)) do
      {:ok, %{status: 200, headers: h}} -> {:ok, etag(h), sentinel_response?(h), carry_meta(h)}
      {:ok, %{status: 404}} -> {:ok, nil, false, []}
      {:ok, %{status: s}} -> {:error, {:s3_head_status, s}}
      {:error, reason} -> {:error, reason}
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
  @spec carry_meta(map() | keyword()) :: [{String.t(), String.t()}]
  def carry_meta(headers) do
    for key <- [@md5_meta, @pos_meta],
        value = header_value(headers, key),
        do: {key, value}
  end

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

  defp multipart_etag_form?(etag), do: etag |> String.trim(~s(")) |> String.contains?("-")

  defp rotate_etag(key, etag, carry) do
    if multipart_etag_form?(etag),
      do: single_copy_touch(key, etag, carry),
      else: multipart_copy_touch(key, etag, carry)
  end

  # Plain server-side self-copy: produces a single-form (MD5) etag. Used when the
  # object currently carries a multipart-form etag, so the form flip IS the
  # rotation. S3 requires the REPLACE metadata directive for a self-copy — which drops ALL user
  # metadata, so `carry` re-sends every key the object had. Sending back only the integrity md5 is
  # what silently erased A2's position stamp on every takeover — see `head_object/1`.
  # Returns `{:ok, new_etag_or_nil}` — the CopyObjectResult body carries the new etag
  # (nil if unparseable; the caller then confirms via HEAD).
  defp single_copy_touch(key, etag, carry) do
    source = "/" <> fetch!(config(), :bucket) <> "/" <> key

    case Req.put(req(),
           url: url_path(key),
           headers:
             [
               {"x-amz-copy-source", source},
               {"x-amz-metadata-directive", "REPLACE"},
               {"x-amz-copy-source-if-match", etag}
             ] ++ carry
         ) do
      {:ok, %{status: s, body: body}} when s in 200..299 ->
        body = to_string(body)

        if copy_body_ok?(body),
          do: {:ok, body_etag(body)},
          else: {:error, {:s3_touch_error_body, body}}

      {:ok, %{status: 412}} ->
        {:error, :touch_precondition}

      {:ok, %{status: s}} ->
        {:error, {:s3_touch_status, s}}

      {:error, reason} ->
        {:error, reason}
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

  # One-part multipart self-copy: Complete's etag is md5(part-md5s)-1 — never the
  # single MD5 form, so it rotates even for identical bytes. A single-part
  # multipart upload has no minimum part size, so this works for any shard.
  defp multipart_copy_touch(key, etag, carry) do
    source = "/" <> fetch!(config(), :bucket) <> "/" <> key

    case create_multipart(key, carry) do
      {:ok, upload_id} ->
        with {:ok, part_etag} <- upload_part_copy(key, upload_id, source, etag),
             {:ok, post} <- complete_multipart(key, upload_id, part_etag) do
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

  defp upload_part_copy(key, upload_id, source, etag) do
    case Req.put(req(),
           url: url_path(key) <> "?partNumber=1&uploadId=#{URI.encode_www_form(upload_id)}",
           headers: [
             {"x-amz-copy-source", source},
             {"x-amz-copy-source-if-match", etag}
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
  defp complete_multipart(key, upload_id, part_etag) do
    body =
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>" <>
        ~s(<ETag>"#{part_etag}"</ETag></Part></CompleteMultipartUpload>)

    case Req.post(req(),
           url: url_path(key) <> "?uploadId=#{URI.encode_www_form(upload_id)}",
           body: body
         ) do
      {:ok, %{status: s, body: resp}} when s in 200..299 ->
        resp = to_string(resp)

        if copy_body_ok?(resp),
          do: {:ok, body_etag(resp)},
          else: {:error, {:s3_touch_error_body, resp}}

      {:ok, %{status: s}} ->
        {:error, {:s3_complete_status, s}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp abort_multipart(key, upload_id) do
    _ = Req.delete(req(), url: url_path(key) <> "?uploadId=#{URI.encode_www_form(upload_id)}")
    :ok
  end

  # --- brand-new steal sentinel (round-2 #7) ---

  @sentinel_meta "x-amz-meta-fathom-sentinel"
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
  # steal-time fence ROTATES a single-part etag to MULTIPART form (rotate_etag/touch_object)
  # to invalidate a zombie's If-Match — so right after a steal the object carries a `...-N`
  # etag and the new owner's failover cold-open pull would run with NO content check until the
  # next single-PUT flush. The `x-amz-meta-fathom-md5` metadata carries the body's MD5
  # independent of the etag's shape and survives the rotation, so when it's present we verify
  # against IT regardless of etag form. Falls back to the etag-MD5 check for objects with no
  # metadata (older flushes, or the unfenced copy paths); a no-op when neither is available.
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
    # The temp file/fd is created LAZILY on the first 200 data chunk (review 2026-07-23
    # #15b): a 304/404 has no body, and the eager open cost every warm-follower
    # revalidation a create/open/close/rm disk cycle per 304 — pure churn at
    # O(cached)/poll. The fd rides this process's dictionary (the `into` fun runs in the
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
                {:ok, fd} = File.open(tmp, [:write, :raw, :binary])
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
              plain = Codec.inflate(z, chunk)
              :ok = IO.binwrite(fd, plain)
              # The digest is over the DECODED bytes, so it still means "this database's hash"
              # and matches @md5_meta whether or not the object was compressed.
              md5 = resp.private[:fathom_md5] || :crypto.hash_init(:md5)

              resp =
                Req.Response.put_private(resp, :fathom_md5, :crypto.hash_update(md5, plain))

              {:cont, {req, resp}}
          end
        else
          {:cont, {req, resp}}
        end
      end

      result = Req.get(req(), url: url, headers: headers, into: into, retry: false)

      case Process.get(fd_key) do
        nil -> :ok
        fd -> :ok = File.close(fd)
      end

      case result do
        # An object marked with an encoding this node cannot perform (#38). FAIL THE PULL — the
        # temp holds nothing usable and handing raw bytes to SQLite as a database is precisely
        # the correctness incident the marker exists to prevent. Not retryable: the marker will
        # be the same next time. Upgrade the node instead.
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
              if Process.get(fd_key) == nil, do: File.touch(tmp)
              digest = :crypto.hash_final(resp.private[:fathom_md5] || :crypto.hash_init(:md5))

              # Prefer the etag-form-independent metadata hash (#17); fall back to the etag-MD5
              # check when the object predates the metadata or came from an unfenced copy path.
              case verify_integrity(digest, meta_md5(resp.headers), etag(resp.headers)) do
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
    after
      case Process.delete(fd_key) do
        nil -> :ok
        fd -> _ = File.close(fd)
      end

      # Release the inflate context on every exit path, exactly like the fd (#38).
      case Process.delete({__MODULE__, :dl_z, tmp}) do
        z when is_reference(z) -> Codec.finish(z)
        _ -> :ok
      end

      File.rm(tmp)
    end
  end

  # `nil` for the raw path, an inflate context for a decodable encoding, or the error tuple
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
      # A brand-new sentinel (round-2 #7) is nothing to warm: the follower treats
      # it as absent and drops any stale cache entry.
      {:sentinel, _etag} -> {:ok, :absent}
      {:error, _} = error -> error
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
            {:ok, _new_etag} -> :ok
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
      System.tmp_dir!(),
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

  @impl true
  def restore_snapshot(shard_id, snapshot_id) do
    copy_unless_sentinel_source(snapshot_key(shard_id, snapshot_id), db_key(shard_id))
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
  def restore_snapshot(shard_id, snapshot_id, expected_etag) do
    # Fenced snapshot restore (expert review 2026-07-18 #2): the snapshot counterpart of the fenced
    # migration restore/3. S3 can't carry an If-Match on a CopyObject DESTINATION, so mirror the
    # forward flush: download the snapshot bytes to a temp, then conditional-PUT them to live via
    # flush/3 (If-Match the live etag the caller captured). A 412 → :superseded aborts without
    # clobbering a writer that raced in after the drain.
    tmp = restore_temp(shard_id, snapshot_id)

    try do
      case download(url_path(snapshot_key(shard_id, snapshot_id)), tmp) do
        {:ok, _etag} ->
          case flush(shard_id, tmp, expected_etag) do
            {:ok, _new_etag} -> :ok
            {:error, _} = error -> error
          end

        :absent ->
          {:error, :snapshot_absent}

        {:sentinel, _etag} ->
          {:error, :snapshot_absent}

        {:error, _} = error ->
          error
      end
    after
      File.rm(tmp)
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
  def purge_shard(shard_id), do: purge_shard_page(shard_id, nil)

  # Full tenant erasure (#15): ListObjectsV2 over `<prefix><shard_id>` (a broad
  # prefix that can also surface sibling ids like `<shard_id>2.db`), then delete
  # only the keys whose id-delimiter is `.` or `@` — so purging `acme` never deletes
  # `acme2`. Paginated; per-object DELETEs are issued as each page lands (fathom's
  # thesis is small shards with few versions/snapshots, so the per-shard object
  # count is tiny — no DeleteObjects batching needed, matching drop_* elsewhere).
  defp purge_shard_page(shard_id, token) do
    params =
      [{"list-type", "2"}, {"prefix", prefix() <> shard_id}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    case Req.get(req(), url: url_path(""), params: params) do
      {:ok, %{status: 200, body: body}} ->
        case delete_keys(shard_object_keys(body, shard_id)) do
          :ok ->
            case next_token(body) do
              nil -> :ok
              next -> purge_shard_page(shard_id, next)
            end

          {:error, _} = err ->
            err
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_list_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp shard_object_keys(xml, shard_id) when is_binary(xml) do
    ~r{<Key>(.*?)</Key>}s
    |> Regex.scan(xml)
    |> Enum.flat_map(fn [_, key] -> if shard_object_key?(key, shard_id), do: [key], else: [] end)
  end

  defp shard_object_keys(_body, _shard_id), do: []

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
  defp tombstoned_ids_page(token, acc) do
    scan_prefix = prefix() <> "tombstones/"

    params =
      [{"list-type", "2"}, {"prefix", scan_prefix}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    case Req.get(req(), url: url_path(""), params: params) do
      {:ok, %{status: 200, body: body}} ->
        ids = tombstone_ids_from_xml(body, scan_prefix)

        case next_token(body) do
          nil -> {:ok, acc ++ ids}
          next -> tombstoned_ids_page(next, acc ++ ids)
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_list_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp tombstone_ids_from_xml(xml, scan_prefix) when is_binary(xml) do
    ~r{<Key>(.*?)</Key>}s
    |> Regex.scan(xml)
    |> Enum.flat_map(fn [_, key] ->
      id = String.replace_prefix(key, scan_prefix, "")
      if id != key and id != "", do: [id], else: []
    end)
  end

  defp tombstone_ids_from_xml(_body, _scan_prefix), do: []

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

    params =
      [{"list-type", "2"}, {"prefix", snap_prefix}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    case Req.get(req(), url: url_path(""), params: params) do
      {:ok, %{status: 200, body: body}} ->
        entries = snapshot_entries(body, snap_prefix)

        case next_token(body) do
          nil -> {:ok, Enum.sort_by(acc ++ entries, & &1.id, :desc)}
          next -> list_snapshots(shard_id, next, acc ++ entries)
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_list_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp snapshot_entries(xml, snap_prefix) when is_binary(xml) do
    ~r{<Contents>.*?<Key>(.*?)</Key>.*?<Size>(\d+)</Size>.*?</Contents>}s
    |> Regex.scan(xml)
    |> Enum.flat_map(fn [_, key, size] ->
      if String.starts_with?(key, snap_prefix) and String.ends_with?(key, ".db") do
        id = key |> String.replace_prefix(snap_prefix, "") |> String.replace_suffix(".db", "")
        [%{id: id, bytes: String.to_integer(size)}]
      else
        []
      end
    end)
  end

  defp snapshot_entries(_body, _prefix), do: []

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
        case owner_live?(other, now, lock_exp) do
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

  @impl true
  def lease_stealable_at(shard_id) do
    now = Storage.now_ms()

    case get_lock(shard_id) do
      {:ok, %{owner: other, expires_at_ms: lock_exp}, _etag} ->
        case stealable_at(other, now, lock_exp) do
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
            stolen = %{owner: owner, epoch: epoch + 1, expires_at_ms: now + ttl_ms}

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
                    {:ok,
                     lease
                     |> Map.put(:took_over, true)
                     |> Map.put(:touch_pre_etag, pre)
                     |> Map.put(:touch_post_etag, post)}

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
                    restore_lock(shard_id, stolen, %{
                      owner: other,
                      epoch: epoch + 2,
                      expires_at_ms: lock_exp
                    })

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
    case stealable_at(other, now, lock_expires_at_ms) do
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
  defp stealable_at(other, now, lock_expires_at_ms) do
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
        if Storage.incarnation_dead?(other),
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

  @impl true
  def renew_lease(shard_id, %{owner: owner, epoch: epoch} = lease, ttl_ms) do
    now = Storage.now_ms()
    renewed = %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms}

    case lease do
      # Fast path (review 2026-07-23 #6): we cached the lock's etag when WE last wrote it
      # (create/put/renew all return it), so renew is one conditional PUT — 1 RTT, not the
      # legacy GET-then-PUT's 2. A 412 means the lock changed under us, which can only be
      # another writer: superseded, exactly what the legacy read would have concluded.
      %{lock_etag: etag} when is_binary(etag) ->
        case put_lock(shard_id, renewed, if_match: etag) do
          {:ok, _} = ok -> ok
          {:error, {:held, _}} -> {:error, :superseded}
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
    case Fathom.Shard.Storage.HeartbeatCache.get(owner, hb_cache_ttl_ms()) do
      {:hit, value} ->
        value

      :miss ->
        case read_heartbeat_dated(owner) do
          {:error, _} = error ->
            error

          value ->
            Fathom.Shard.Storage.HeartbeatCache.put(owner, value)
            value
        end
    end
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
          {:ok, %{owner: owner}, _etag} -> {:error, {:held, owner}}
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
  defp restore_lock(shard_id, ours, replacement) do
    case get_lock(shard_id) do
      {:ok, %{owner: o, epoch: e}, etag} when o == ours.owner and e == ours.epoch ->
        _ = put_lock(shard_id, replacement, if_match: etag)
        :ok

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
    params =
      [{"list-type", "2"}, {"prefix", prefix()}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    case Req.get(req(), url: url_path(""), params: params) do
      {:ok, %{status: 200, body: body}} ->
        {count, bytes} = tally_list(body, count, bytes)

        case next_token(body) do
          nil -> {count, bytes}
          next -> list_usage(next, count, bytes)
        end

      {:ok, %{status: status}} ->
        {:error, {:s3_list_status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp tally_list(xml, count, bytes) when is_binary(xml) do
    ~r{<Contents>.*?<Key>(.*?)</Key>.*?<Size>(\d+)</Size>.*?</Contents>}s
    |> Regex.scan(xml)
    |> Enum.reduce({count, bytes}, fn [_, key, size], {c, b} ->
      if live_db_object?(key), do: {c + 1, b + String.to_integer(size)}, else: {c, b}
    end)
  end

  defp tally_list(_body, count, bytes), do: {count, bytes}

  defp next_token(xml) when is_binary(xml) do
    case Regex.run(~r{<NextContinuationToken>(.*?)</NextContinuationToken>}s, xml) do
      [_, token] -> token
      _ -> nil
    end
  end

  defp next_token(_body), do: nil

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
    |> Req.Request.append_response_steps(fathom_s3_meter: &__MODULE__.meter/1)
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
      %{count: 1, bytes: op_bytes(request, response)},
      %{op: request.method}
    )

    {request, response}
  rescue
    _ -> {request, response}
  end

  def meter({request, other}), do: {request, other}

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
