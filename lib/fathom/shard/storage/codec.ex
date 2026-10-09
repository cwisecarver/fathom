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
  @chunk 1024 * 1024

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
  `{:ok, tmp_path}`.

  Streamed both ways: a shard object is up to gigabytes and must never be materialized whole in
  the BEAM.
  """
  @spec compress_to_temp(Path.t(), :zlib | :zstd) :: {:ok, Path.t()} | {:error, term()}
  def compress_to_temp(path, encoding) do
    tmp = "#{path}.z.#{System.unique_integer([:positive])}"
    ctx = compressor(encoding)

    try do
      File.open!(tmp, [:write, :raw, :binary], fn out ->
        path
        |> File.stream!(@chunk)
        |> Enum.each(fn chunk -> :ok = IO.binwrite(out, compress_chunk(ctx, chunk)) end)

        :ok = IO.binwrite(out, compress_finish(ctx))
      end)

      {:ok, tmp}
    rescue
      e ->
        File.rm(tmp)
        {:error, e}
    after
      compress_close(ctx)
    end
  end

  @doc """
  Decodes the compressed file at `path` as `encoding` and checks its plaintext MD5 is `expected_hex`.

  `:ok`, or `{:error, {:compressed_body_mismatch, actual_hex}}` / `{:error, {:zstd | :zlib, _}}`.
  The upload path runs it before PUTting a compressed body (expert review 2026-10-08 #15), so a
  body that would not decode to the database it is labelled as is never stored. Streamed, in
  bounded pieces, like the download path.
  """
  @spec verify_decodes_to(Path.t(), :zlib | :zstd, String.t()) :: :ok | {:error, term()}
  def verify_decodes_to(path, encoding, expected_hex) do
    ctx = init(encoding)

    try do
      path
      |> File.stream!(@chunk)
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

  defp compressor(:zlib) do
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, @level)
    {:zlib, z}
  end

  defp compressor(:zstd) do
    {:ok, c} = :zstd.context(:compress, %{compressionLevel: @zstd_level})
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
