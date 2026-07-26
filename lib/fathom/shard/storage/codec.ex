defmodule Fathom.Shard.Storage.Codec do
  @moduledoc """
  Optional compression for stored shard objects (expert review 2026-07-24 #38).

  Every shard object was stored and transferred as a raw SQLite file. ORM row data compresses
  ~2.5–3.5× with `:zlib` at level 6 (in OTP, no new dependency), which comes off every flush PUT,
  every cold-open GET, every warm re-pull, and the at-rest bill.

  ## Where this pays, and where it does not

  **Not** on single-shard cold-open: that path is RTT-bound at fathom's shard sizes
  (`docs/reviews/latency-cost-2026-07-23.md` measured ~1 RTT with the body essentially free), so
  there is ~0 win there for a small shard on a fat pipe. It pays on **aggregate-bandwidth-bound**
  work — mass warming and failover (`warm_s3_shards_per_s`), steady-state PUT volume for
  write-hot shards, cross-region transfer, and storage cost.

  And the CPU is not free. The chaos rig measures as CPU-saturated (2026-07-25: ~20 runnable
  processes on 12 normal schedulers, host at ~95% of 12 vCPUs), so on a loaded node this trades a
  resource that is scarce for one that may not be. **That is why it defaults to `:none`** and why
  the honest way to turn it on is to measure `warm_s3_shards_per_s` under `S3_FAKE_RATE_KBPS`
  first.

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
  # Level 6: the knee. Level 9 costs materially more CPU for a few percent on SQLite pages, and
  # CPU is the contended resource on a loaded node.
  @level 6
  @chunk 1024 * 1024

  @doc "The metadata header carrying an object's encoding marker."
  def meta_header, do: @enc_meta

  @doc """
  The encoding this node WRITES with. `:none` (default) stores raw bytes.

  Reading is unaffected by this — see `decoder/1`.
  """
  @spec encoding() :: :none | :zlib
  def encoding do
    case Application.get_env(:fathom, :shard_object_encoding, :none) do
      :zlib -> :zlib
      _ -> :none
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
          {:ok, :none | :zlib} | {:error, {:unknown_object_encoding, String.t()}}
  def decoder(nil), do: {:ok, :none}
  def decoder(""), do: {:ok, :none}
  def decoder(@zlib_marker), do: {:ok, :zlib}
  def decoder(other), do: {:error, {:unknown_object_encoding, other}}

  @doc """
  Compresses `path` to a sibling temp file, returning `{:ok, tmp_path}`.

  Streamed both ways: a shard object is up to gigabytes and must never be materialized whole in
  the BEAM.
  """
  @spec compress_to_temp(Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def compress_to_temp(path) do
    tmp = "#{path}.z.#{System.unique_integer([:positive])}"
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, @level)

      File.open!(tmp, [:write, :raw, :binary], fn out ->
        path
        |> File.stream!(@chunk)
        |> Enum.each(fn chunk ->
          :ok = IO.binwrite(out, :zlib.deflate(z, chunk))
        end)

        :ok = IO.binwrite(out, :zlib.deflate(z, "", :finish))
      end)

      :zlib.deflateEnd(z)
      {:ok, tmp}
    rescue
      e ->
        File.rm(tmp)
        {:error, e}
    after
      :zlib.close(z)
    end
  end

  @doc """
  Opens a streaming inflate context, or `nil` for the raw path.

  Returned as an opaque handle threaded through `inflate/2` + `finish/1` so the download path can
  inflate chunk-by-chunk as they arrive, without buffering the object.
  """
  @spec init(:none | :zlib) :: nil | :zlib.zstream()
  def init(:none), do: nil

  def init(:zlib) do
    z = :zlib.open()
    :ok = :zlib.inflateInit(z)
    z
  end

  @doc "Inflates one chunk. Returns iodata; the raw path passes the chunk through untouched."
  @spec inflate(nil | :zlib.zstream(), iodata()) :: iodata()
  def inflate(nil, chunk), do: chunk
  def inflate(z, chunk), do: :zlib.inflate(z, chunk)

  @doc "Finishes and releases an inflate context. Safe on `nil`."
  @spec finish(nil | :zlib.zstream()) :: :ok
  def finish(nil), do: :ok

  def finish(z) do
    # inflateEnd raises when the stream is incomplete — a TORN transfer. The caller treats that
    # like any other integrity failure (retry the whole download), so it is caught here rather
    # than crashing the pull.
    try do
      :zlib.inflateEnd(z)
    catch
      _, _ -> :ok
    end

    :zlib.close(z)
    :ok
  end

  @doc """
  The metadata headers to attach to an upload for `encoding`.

  Raw uploads carry NO marker, so an object written with encoding off is byte-identical to one
  written before this module existed.
  """
  @spec upload_headers(:none | :zlib) :: [{String.t(), String.t()}]
  def upload_headers(:none), do: []
  def upload_headers(:zlib), do: [{@enc_meta, @zlib_marker}]
end
