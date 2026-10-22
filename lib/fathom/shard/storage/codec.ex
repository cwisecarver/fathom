defmodule Fathom.Shard.Storage.Codec do
  @moduledoc """
  Compression for stored shard objects (expert review 2026-07-24 #38; zstd 2026-10-05).

  Every shard object was stored and transferred as a raw SQLite file. ORM row data compresses
  ~2–3.5×, which comes off every flush PUT, every cold-open GET, every warm re-pull, and the
  at-rest bill.

  **`:zstd` (level 3) is the default** (expert review 2026-10-01 perf #31). Measured on a 4.2 MB
  shard: zlib-6 75 ms to compress (58 MB/s), ratio 1.89; zstd-3 8.7 ms (504 MB/s), ratio 1.92,
  and zstd decodes ~2.7× faster. `:zstd` is in OTP 28+, which is fathom's floor. `:zlib` stays
  writable and is always decodable; `:none` stores raw bytes.

  ## Where this pays, and where it does not

  **Not** on single-shard cold-open: that path is RTT-bound at fathom's shard sizes
  (`docs/reviews/latency-cost-2026-07-23.md` measured ~1 RTT with the body essentially free), so
  there is ~0 win there for a small shard on a fat pipe. It pays on **aggregate-bandwidth-bound**
  work — mass warming and failover (`warm_s3_shards_per_s`), steady-state PUT volume for
  write-hot shards, cross-region transfer, and storage cost.

  And the CPU is not free. The chaos rig measures as CPU-saturated (2026-07-25: ~20 runnable
  processes on 12 normal schedulers, host at ~95% of 12 vCPUs), so on a loaded node this trades a
  resource that is scarce for one that may not be. That is why this defaulted to `:none` while
  the only codec was zlib-6; zstd-3 costs ~1/9 of the CPU for the same ratio, which is what made
  it the default.

  ## The contract

  * **Decode always, encode optionally.** A node decodes any marked object regardless of its own
    setting, so a fleet can roll the flag forward and back without a flag day, and mixed-version
    nodes interoperate.
  * **Marker-tagged, fail closed.** An object carries `x-amz-meta-fathom-enc`. A node that does
    not recognise the marker must FAIL THE PULL, never hand raw bytes to SQLite as a database.
    That is the one way this becomes a correctness incident rather than a slow path.
  * **The integrity digest is over the UNCOMPRESSED bytes.** `x-amz-meta-fathom-md5` keeps
    meaning "this database's hash", so `verify_integrity/3` is unchanged and an object's identity
    does not depend on how it was stored. `content-md5` covers the compressed wire body.
  * **Application-layer, never HTTP `Content-Encoding`.** The etag/`If-Match` fence is over the
    exact stored bytes; a store or CDN that transformed encodings would move the etag out from
    under the fence. We store opaque bytes.
  """

  @enc_meta "x-amz-meta-fathom-enc"
  @zlib_marker "zlib"
  @zstd_marker "zstd"
  # zlib level 6: the knee. Level 9 costs materially more CPU for a few percent on SQLite pages,
  # and CPU is the contended resource on a loaded node.
  @level 6
  # zstd level 3 (zstd's own default): ratio 1.92 vs 1.82 at level 1 for +50% CPU, still ~9x
  # cheaper than zlib-6 (perf review 2026-10-01 #31).
  @zstd_level 3
  # 256 KiB, not 1 MiB (expert review 2026-10-19 #29). The zstd/md5 NIF calls run on normal
  # schedulers; probe (37 MB, zstd-3 + frame checksum, 3 rounds): 1 MiB 2.42-2.45 ms/MB with a
  # 2.6 ms max call, 256 KiB 2.47-2.54 ms/MB (within ~2%) with a 0.7-1.0 ms max call, 128 KiB
  # 2.46-2.51 ms/MB with 0.35-0.4 ms. 256 KiB keeps the call count 4x lower than 128 KiB.
  @chunk 256 * 1024

  @doc "The metadata header carrying an object's encoding marker."
  def meta_header, do: @enc_meta

  @doc """
  The encoding this node WRITES with: `:zstd` (default), `:zlib`, or `:none` (raw bytes).

  Reading is unaffected by this — see `decoder/1`.
  """
  @spec encoding() :: :none | :zlib | :zstd
  def encoding do
    case Application.get_env(:fathom, :shard_object_encoding, :zstd) do
      :zlib -> :zlib
      :none -> :none
      _ -> :zstd
    end
  end

  @doc """
  Resolves an object's marker to a decoder.

  `nil` (an object written before this existed, or by a node with encoding off) is raw — that is
  the backward-compatible case, not an error. An UNRECOGNISED marker is an error and the caller
  must fail the pull: serving bytes we cannot interpret as a database is the failure mode this
  whole design exists to prevent.
  """
  @spec decoder(String.t() | nil) ::
          {:ok, :none | :zlib | :zstd} | {:error, {:unknown_object_encoding, String.t()}}
  def decoder(nil), do: {:ok, :none}
  def decoder(""), do: {:ok, :none}
  def decoder(@zlib_marker), do: {:ok, :zlib}
  def decoder(@zstd_marker), do: {:ok, :zstd}
  def decoder(other), do: {:error, {:unknown_object_encoding, other}}

  @doc """
  Compresses `path` with `encoding` (`:zlib` or `:zstd`) to a sibling temp file, returning
  `{:ok, tmp_path}`. See `compress_to_temp_hashed/2` for the single-pass variant that also
  returns the digests and size.
  """
  @spec compress_to_temp(Path.t(), :zlib | :zstd) :: {:ok, Path.t()} | {:error, term()}
  def compress_to_temp(path, encoding) do
    with {:ok, tmp, _size, _body_md5, _plain_hex} <- compress_to_temp_hashed(path, encoding),
         do: {:ok, tmp}
  end

  @doc """
  `compress_to_temp/2` that hashes in the same pass (perf review 2026-10-14 #6).

  Returns `{:ok, tmp_path, body_size, body_md5, plain_md5_hex}`: the size and raw MD5 digest of the
  compressed file as written (what `content-md5` covers), and the lowercase-hex MD5 of the
  plaintext chunks exactly as they were fed to the compressor (what `x-amz-meta-fathom-md5`
  covers). The plaintext is read ONCE and the compressed body is never re-read to hash it; the
  plaintext hash is of the very bytes compressed, so it cannot disagree with the body because the
  file changed between a hashing read and a compressing read.

  Streamed both ways: a shard object is up to gigabytes and must never be materialized whole in
  the BEAM.

  The `body_hash` argument picks the BODY digest (expert review 2026-10-20 #29): `:md5` (default;
  `content-md5`, 16 raw bytes) or `:crc32` (`x-amz-checksum-crc32`, 4 big-endian bytes, ~40x
  cheaper per MB). The plaintext identity hash is MD5 either way.
  """
  @spec compress_to_temp_hashed(Path.t(), :zlib | :zstd, :md5 | :crc32) ::
          {:ok, Path.t(), non_neg_integer(), binary(), String.t()} | {:error, term()}
  def compress_to_temp_hashed(path, encoding, body_hash \\ :md5) do
    case compress_to_temp_digests(path, encoding, body_hash, false) do
      {:ok, tmp, size, body, plain_hex, _no_sha} -> {:ok, tmp, size, body, plain_hex}
      {:error, _} = error -> error
    end
  end

  @doc """
  `compress_to_temp_hashed/3` that can ALSO hash the plaintext with SHA-256 in the same pass
  (`plain_sha256? = true`; expert review 2026-10-20-2027 #23). Returns
  `{:ok, tmp, body_size, body_digest, plain_md5_hex, plain_sha256_hex | nil}`.

  The MD5 stays: it is the object's IDENTITY (`x-amz-meta-fathom-md5`, the PUT intents `Fork`
  adopts on). The SHA-256 exists only so the PULL can verify the decoded bytes with a cheaper hash
  than MD5 on CPUs with SHA extensions; it costs the upload one more plaintext hash pass.
  """
  @spec compress_to_temp_digests(Path.t(), :zlib | :zstd, :md5 | :crc32, boolean()) ::
          {:ok, Path.t(), non_neg_integer(), binary(), String.t(), String.t() | nil}
          | {:error, term()}
  def compress_to_temp_digests(path, encoding, body_hash, plain_sha256?) do
    tmp = "#{path}.z.#{System.unique_integer([:positive])}"
    ctx = compressor(encoding)
    dctx = init(encoding)
    corrupt = Application.get_env(:fathom, :codec_corrupt_hook)

    try do
      File.open!(tmp, [:write, :raw, :binary], fn out ->
        # Every compressed piece does three things in one pass: it goes to disk, into the body
        # digest (`content-md5`), and into a decoder whose output is DISCARDED. The codec's own
        # integrity check does the proving: the zstd frame checksum (xxh64 of the plaintext,
        # verified when the frame ends) and zlib's adler32, so a body that would not decode to the
        # database it is labelled as is refused HERE, before anything is PUT (expert review
        # 2026-10-08 #15, moved inline by 2026-10-17 #20). The decode-side MD5 over the output was
        # a third plaintext-sized hash pass (~1 ms/MB of ~4.5) made redundant by the frame checksum
        # (expert review 2026-10-19 #8). This decodes the same bytes that are written, so it needs
        # no second read of the file, and the output is never collected.
        write = fn piece, {size, h} ->
          piece = corrupt_piece(corrupt, piece)
          :ok = IO.binwrite(out, piece)
          decode_piece(dctx, piece)
          {size + IO.iodata_length(piece), body_hash_update(h, piece)}
        end

        sha = if plain_sha256?, do: :crypto.hash_init(:sha256)

        {plain, sha, body} =
          Enum.reduce(
            File.stream!(path, @chunk),
            {:crypto.hash_init(:md5), sha, {0, body_hash_init(body_hash)}},
            fn chunk, {plain, sha, body} ->
              {:crypto.hash_update(plain, chunk), sha && :crypto.hash_update(sha, chunk),
               write.(compress_chunk(ctx, chunk), body)}
            end
          )

        {size, body_h} = write.(compress_finish(ctx), body)
        plain_hex = Base.encode16(:crypto.hash_final(plain), case: :lower)
        sha_hex = sha && Base.encode16(:crypto.hash_final(sha), case: :lower)

        {size, body_hash_final(body_h), plain_hex, sha_hex}
      end)
      |> then(fn {size, body_md5, plain_hex, sha_hex} ->
        {:ok, tmp, size, body_md5, plain_hex, sha_hex}
      end)
    rescue
      e ->
        Fathom.FS.rm(tmp)
        {:error, e}
    catch
      {:decode_failed, reason} ->
        Fathom.FS.rm(tmp)
        {:error, reason}
    after
      release(dctx)
      compress_close(ctx)
    end
  end

  defp body_hash_init(:md5), do: {:md5, :crypto.hash_init(:md5)}
  defp body_hash_init(:crc32), do: {:crc32, 0}

  defp body_hash_update({:md5, ctx}, piece), do: {:md5, :crypto.hash_update(ctx, piece)}
  defp body_hash_update({:crc32, crc}, piece), do: {:crc32, :erlang.crc32(crc, piece)}

  defp body_hash_final({:md5, ctx}), do: :crypto.hash_final(ctx)
  defp body_hash_final({:crc32, crc}), do: <<crc::32>>

  # Test seam (expert review 2026-10-17 #20): a corrupting codec step, applied to each compressed piece
  # before it is written, hashed or decoded, so the body on disk and the content-md5 agree with each
  # other and only the decode proof can refuse them. Unset in every real config.
  defp corrupt_piece(nil, piece), do: piece
  defp corrupt_piece(hook, piece), do: hook.(piece)

  # One compressed piece through the decoder, its output dropped. A body that does not decode (or
  # whose frame checksum fails, raised by the NIF at the end of the frame) is thrown out of the file
  # write, which removes the temp, so nothing is PUT.
  defp decode_piece(dctx, piece) do
    case inflate_into(dctx, piece, fn _out, acc -> acc end, :ok) do
      {:ok, :ok} -> :ok
      {:error, reason} -> throw({:decode_failed, reason})
    end
  end

  @doc """
  Decodes the compressed file at `path` as `encoding` and checks its plaintext MD5 is `expected_hex`.

  `:ok`, or `{:error, {:compressed_body_mismatch, actual_hex}}` / `{:error, {:zstd | :zlib, _}}`.
  The upload path no longer runs this: `compress_to_temp_hashed/2` proves the same property inline,
  over the pieces as they are produced, so the file is not read a second time (expert review
  2026-10-17 #20). It stays as the file-level oracle the codec tests check against. Streamed, in
  bounded pieces, like the download path.
  """
  @spec verify_decodes_to(Path.t(), :zlib | :zstd, String.t()) :: :ok | {:error, term()}
  def verify_decodes_to(path, encoding, expected_hex),
    do: verify_chunks(File.stream!(path, @chunk), encoding, expected_hex)

  @doc """
  `verify_decodes_to/3` for a body held in memory (the steal-touch's re-encoded small object).
  """
  @spec verify_binary_decodes_to(binary(), :zlib | :zstd, String.t()) :: :ok | {:error, term()}
  def verify_binary_decodes_to(body, encoding, expected_hex),
    do: verify_chunks([body], encoding, expected_hex)

  defp verify_chunks(chunks, encoding, expected_hex) do
    ctx = init(encoding)

    try do
      chunks
      |> Enum.reduce_while({:ok, :crypto.hash_init(:md5)}, fn chunk, {:ok, md5} ->
        case inflate_into(ctx, chunk, &:crypto.hash_update(&2, &1), md5) do
          {:ok, _} = ok -> {:cont, ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, md5} ->
          case Base.encode16(:crypto.hash_final(md5), case: :lower) do
            ^expected_hex -> :ok
            actual -> {:error, {:compressed_body_mismatch, actual}}
          end

        {:error, _} = error ->
          error
      end
    after
      release(ctx)
    end
  end

  # --- the steal-touch nonce (expert review 2026-10-12 N1) ---
  #
  # A steal must leave the data object at an etag that key has NEVER had, or a zombie holding an
  # old etag can land a fenced flush past the fence. For an object too small to split into a
  # multipart copy (S3's 5 MiB part minimum), the only way to a new MD5 etag is new stored BYTES —
  # so the touch appends a zstd SKIPPABLE FRAME carrying 16 random bytes. RFC 8878 §3.1.2: a
  # decoder skips frames with magic 0x184D2A50..0x184D2A5F, so the decoded database (and its
  # `x-amz-meta-fathom-md5`) is unchanged while the etag moves. One magic value is ours, and the
  # payload starts with a fixed tag, so a later touch can recognise and REPLACE the previous nonce
  # instead of growing the object by 32 bytes per steal.
  @nonce_magic 0x184D2A5F
  @nonce_tag "fathomtn"
  @nonce_bytes 16
  @nonce_payload byte_size(@nonce_tag) + @nonce_bytes
  @nonce_frame 8 + @nonce_payload

  @doc """
  `zstd_body` with any trailing touch nonce replaced by a fresh one (expert review 2026-10-12 N1).

  The input must be a zstd-encoded object body; the output decodes to exactly the same bytes.
  """
  @spec renonce(binary()) :: binary()
  def renonce(zstd_body) when is_binary(zstd_body) do
    payload = @nonce_tag <> :crypto.strong_rand_bytes(@nonce_bytes)

    strip_nonce(zstd_body) <>
      <<@nonce_magic::little-32, @nonce_payload::little-32, payload::binary>>
  end

  defp strip_nonce(body) do
    keep = byte_size(body) - @nonce_frame

    case body do
      <<head::binary-size(^keep), @nonce_magic::little-32, @nonce_payload::little-32, @nonce_tag,
        _::binary-size(@nonce_bytes)>>
      when keep >= 0 ->
        head

      _ ->
        body
    end
  end

  @doc """
  Re-encodes an in-memory object body stored as `from` (`:none` or `:zlib`) to zstd.

  Returns `{:ok, zstd_body, plain_md5_hex}`. The plaintext streams from the decoder straight into
  the compressor and is never collected, so a small zlib body of a highly compressible database
  (a 200 MiB `zeroblob` is ~1000:1) never materializes its plaintext; only the zstd output, which
  is about as small as the input, is held. The steal-touch uses it to move a `:none`/`:zlib` object
  onto the encoding that can carry a nonce (expert review 2026-10-12 N1).
  """
  @spec reencode_zstd(binary(), :none | :zlib) :: {:ok, binary(), String.t()} | {:error, term()}
  def reencode_zstd(body, from) when from in [:none, :zlib] do
    dctx = init(from)
    cctx = compressor(:zstd)

    try do
      feed = fn piece, {md5, out} ->
        {:crypto.hash_update(md5, piece), [out, compress_chunk(cctx, piece)]}
      end

      case inflate_into(dctx, body, feed, {:crypto.hash_init(:md5), []}) do
        {:ok, {md5, out}} ->
          {:ok, IO.iodata_to_binary([out, compress_finish(cctx)]),
           Base.encode16(:crypto.hash_final(md5), case: :lower)}

        {:error, _} = error ->
          error
      end
    after
      release(dctx)
      compress_close(cctx)
    end
  end

  defp compressor(:zlib) do
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, @level)
    {:zlib, z}
  end

  defp compressor(:zstd) do
    # `checksumFlag`: the frame carries a 32-bit xxh64 of the plaintext, which the decoder verifies
    # at the frame's end. That is what lets `compress_to_temp_hashed/2` prove the body decodes
    # without a third MD5 pass (expert review 2026-10-19 #8; probe: +4 bytes, same compress time,
    # a flipped byte fails with "Restored data doesn't match checksum"). Frames written without it
    # (every object before this change) still decode; the flag is a property of the frame, not of
    # the decoder.
    {:ok, c} = :zstd.context(:compress, %{compressionLevel: @zstd_level, checksumFlag: true})
    {:zstd, c}
  end

  defp compress_chunk({:zlib, z}, chunk), do: :zlib.deflate(z, chunk)

  defp compress_chunk({:zstd, c}, chunk), do: zstd_stream(c, chunk, [])

  defp compress_finish({:zlib, z}) do
    out = :zlib.deflate(z, "", :finish)
    :zlib.deflateEnd(z)
    out
  end

  defp compress_finish({:zstd, c}) do
    {:done, out} = :zstd.finish(c, "")
    out
  end

  defp compress_close({:zlib, z}), do: :zlib.close(z)
  defp compress_close({:zstd, c}), do: :zstd.close(c)

  @doc """
  Opens a streaming decode context, or `nil` for the raw path.

  Returned as an opaque handle threaded through `inflate_into/4` → `release/1` so the download path
  can decode chunk-by-chunk as they arrive, without buffering the object.

  `inflate_into/4` hands out everything decodable from the bytes it has been given; there is no
  held-back tail to collect at the end (measured 2026-10-05 for zstd: 0 bytes from
  `:zstd.finish/2` after a complete frame, at chunk sizes from 1 B to 1 MiB, provided the
  remainder loop below runs). A TRUNCATED body is not reported by either codec — the download's
  plaintext MD5 over every byte written is what fails it.

  DO NOT DROP THAT PLAINTEXT HASH FOR zstd ON THE STRENGTH OF THE FRAME CHECKSUM (expert review
  2026-10-21 #23, parked; probed on OTP 29). OTP's `:zstd` exposes no frame-completion signal:
  decompress `stream/2` never returns `{:done, _}`, `finish/2` answers `{:done, _}` for a frame cut
  in half, and even one-shot `decompress/1` returns `{:ok, ""}` for that half frame. The xxh64 is
  verified only when its 4-byte trailer arrives, so a frame missing its last 1-4 bytes decodes to
  the full plaintext UNVERIFIED, and a body bit-flip plus a stripped trailer decodes to plaintext
  of the right length with wrong bytes, silently: a `page_count x page_size` length check passes
  it too. (A flip with the trailer present does fail: "Restored data doesn't match checksum".)
  Revisit only if OTP surfaces `ZSTD_decompressStream`'s "frame complete" return.
  """
  @spec init(:none | :zlib | :zstd) :: nil | {:zlib | :zstd, term()}
  def init(:none), do: nil

  def init(:zlib) do
    z = :zlib.open()
    :ok = :zlib.inflateInit(z)
    {:zlib, z}
  end

  def init(:zstd) do
    {:ok, c} = :zstd.context(:decompress)
    {:zstd, c}
  end

  @doc """
  Decodes one received chunk, handing each piece of decoded output to `fun.(piece, acc)` as soon
  as the codec produces it. The raw path hands the chunk over untouched.

  STREAMED, NEVER COLLECTED (expert review 2026-10-08 #2, measured). This used to return the whole
  chunk's decoded output as one iodata for the caller to write. That is unbounded: the ratio is
  the data's, not the codec's, and ordinary tenant data reaches it — a database holding a 200 MiB
  `zeroblob` compressed 1244:1 at zstd-3, so one 16 KiB TLS record decoded to 19.8 MB and one
  1 MiB plain-HTTP read to the whole 210 MB, per pull, times however many pulls a mass warm or
  failover runs at once. Handing pieces out as they come bounds what a pull holds to one codec
  output buffer (zstd ~128 KiB, zlib's `safeInflate` 16 KiB) whatever the ratio.

  A body that does not decode is `{:error, reason}`, never a raise: the caller treats it like a
  torn transfer and retries the whole download with a fresh temp. Only the codec call is guarded —
  a raise from `fun` (a failed write) propagates, so it is not mistaken for a corrupt body.
  """
  @spec inflate_into(nil | {:zlib | :zstd, term()}, iodata(), (iodata(), acc -> acc), acc) ::
          {:ok, acc} | {:error, term()}
        when acc: var
  def inflate_into(nil, chunk, fun, acc), do: {:ok, fun.(chunk, acc)}
  def inflate_into({:zlib, z}, chunk, fun, acc), do: zlib_each(z, chunk, fun, acc)
  def inflate_into({:zstd, c}, chunk, fun, acc), do: zstd_each(c, chunk, fun, acc)

  # `safeInflate/2` returns at most one output buffer per call: `{:continue, out}` means more output
  # is pending for the input already given (call again with `[]`), `{:finished, out}` means that
  # input is used up — NOT that the stream ended; the next chunk continues it (probed 2026-10-08).
  defp zlib_each(z, data, fun, acc) do
    case guard(:zlib, fn -> :zlib.safeInflate(z, data) end) do
      {:ok, {:continue, out}} -> zlib_each(z, [], fun, fun.(out, acc))
      {:ok, {:finished, out}} -> {:ok, fun.(out, acc)}
      {:ok, {:need_dictionary, _adler, _out}} -> {:error, {:zlib, :need_dictionary}}
      {:error, _} = error -> error
    end
  end

  # `:zstd.stream/2` stops when its output buffer fills (`{:continue, remainder, out}`); the
  # remainder must be fed back in or those bytes are silently lost. `{:continue, out}` means the
  # whole input was consumed.
  defp zstd_each(c, data, fun, acc) do
    case guard(:zstd, fn -> :zstd.stream(c, data) end) do
      {:ok, {:continue, remainder, out}} -> zstd_each(c, remainder, fun, fun.(out, acc))
      {:ok, {:continue, out}} -> {:ok, fun.(out, acc)}
      {:error, _} = error -> error
    end
  end

  # The NIFs RAISE on a malformed body (zstd: `{:zstd_error, "Unknown frame descriptor"}`, zlib: a
  # `data_error`). Turn that into an error tuple at the call, and only there.
  defp guard(codec, call) do
    {:ok, call.()}
  rescue
    e -> {:error, {codec, Exception.message(e)}}
  catch
    _, reason -> {:error, {codec, reason}}
  end

  # `:zstd.stream/2` may stop before consuming its whole input (`{:continue, remainder, out}`) when
  # its output buffer fills; the remainder must be fed back in or those bytes are silently lost.
  # Only `{:continue, out}` means everything was consumed.
  defp zstd_stream(c, data, acc) do
    case :zstd.stream(c, data) do
      {:continue, remainder, out} -> zstd_stream(c, remainder, [acc, out])
      {:continue, out} -> [acc, out]
    end
  end

  @doc "Releases a decode context on any exit path. Safe on `nil` and on anything else."
  @spec release(term()) :: :ok
  def release({:zlib, z}) do
    # inflateEnd raises on an incomplete stream (a torn transfer); the MD5 has already judged it.
    try do
      :zlib.inflateEnd(z)
    catch
      _, _ -> :ok
    end

    :zlib.close(z)
    :ok
  catch
    _, _ -> :ok
  end

  def release({:zstd, c}) do
    :zstd.close(c)
    :ok
  catch
    _, _ -> :ok
  end

  def release(_), do: :ok

  @doc """
  The metadata headers to attach to an upload for `encoding`.

  Raw uploads carry NO marker, so an object written with encoding off is byte-identical to one
  written before this module existed.
  """
  @spec upload_headers(:none | :zlib | :zstd) :: [{String.t(), String.t()}]
  def upload_headers(:none), do: []
  def upload_headers(:zlib), do: [{@enc_meta, @zlib_marker}]
  def upload_headers(:zstd), do: [{@enc_meta, @zstd_marker}]
end
