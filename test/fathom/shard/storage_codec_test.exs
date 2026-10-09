defmodule Fathom.Shard.Storage.CodecTest do
  @moduledoc """
  The stored-object compression codec (expert review 2026-07-24 #38; zstd default since
  2026-10-05, perf review 2026-10-01 #31).

  The properties that matter here are safety ones, not compression ratio: decode-always so a
  fleet can roll the flag back, and fail-closed on a marker this node doesn't understand — the
  one path where this feature could hand SQLite bytes that aren't a database.
  """
  # NOT async: two tests here mutate `:shard_object_encoding`, which is global application env
  # that the S3 backend reads. An async module doing that races every other async test.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.Codec

  defp tmp(name),
    do: Path.join(System.tmp_dir!(), "codec_#{name}_#{System.unique_integer([:positive])}")

  # The download path's shape: decode chunk by chunk (16 KiB, a TLS record), then release.
  defp stream_inflate(path, decoder) do
    z = Codec.init(decoder)

    body =
      path
      |> File.stream!(16 * 1024)
      |> Enum.reduce([], fn chunk, acc ->
        {:ok, acc} = Codec.inflate_into(z, chunk, &[&2, &1], acc)
        acc
      end)

    :ok = Codec.release(z)
    IO.iodata_to_binary(body)
  end

  for enc <- [:zlib, :zstd] do
    test "#{enc}: round-trips a file through compress + streaming decode" do
      src = tmp("src")
      # Repetitive, like real SQLite pages — and big enough to cross many 16 KiB chunks.
      body = String.duplicate("the quick brown fox jumps over the lazy dog. ", 60_000)
      File.write!(src, body)
      on_exit(fn -> File.rm(src) end)

      assert {:ok, z} = Codec.compress_to_temp(src, unquote(enc))
      on_exit(fn -> File.rm(z) end)

      assert File.stat!(z).size < File.stat!(src).size,
             "compression didn't shrink obviously-compressible input"

      assert stream_inflate(z, unquote(enc)) == body
    end

    test "#{enc}: an incompressible multi-MB file round-trips byte for byte" do
      src = tmp("rand")
      body = :crypto.strong_rand_bytes(3 * 1024 * 1024 + 17)
      File.write!(src, body)
      on_exit(fn -> File.rm(src) end)

      assert {:ok, z} = Codec.compress_to_temp(src, unquote(enc))
      on_exit(fn -> File.rm(z) end)
      assert stream_inflate(z, unquote(enc)) == body
    end

    test "#{enc}: an empty file round-trips" do
      src = tmp("empty")
      File.write!(src, "")
      on_exit(fn -> File.rm(src) end)

      assert {:ok, z} = Codec.compress_to_temp(src, unquote(enc))
      on_exit(fn -> File.rm(z) end)
      assert stream_inflate(z, unquote(enc)) == ""
    end

    # A body that is not what its marker says must come back as an error the download can retry,
    # never a raise out of the HTTP callback and never bytes written to the temp.
    test "#{enc}: a body that does not decode is an error, not a raise" do
      z = Codec.init(unquote(enc))
      on_exit(fn -> Codec.release(z) end)

      assert {:error, {unquote(enc), _}} =
               Codec.inflate_into(z, String.duplicate("not compressed ", 100), &[&2, &1], [])
    end
  end

  # Expert review 2026-10-08 #2, measured: decode output is bounded by the DATA's ratio, not the
  # codec's, and ordinary tenant data reaches it — a database holding a 200 MiB zeroblob compressed
  # 1244:1, so one received chunk decoded to tens or hundreds of MB that the pull held at once
  # before writing any of it. Times the pulls a mass warm or failover runs concurrently, that is a
  # node OOM from one tenant's data. The invariant: no single piece handed to the writer exceeds a
  # codec output buffer, whatever the ratio and however large the received chunk.
  @max_piece 256 * 1024

  for enc <- [:zlib, :zstd] do
    test "#{enc}: a highly compressible body is handed out in bounded pieces, not per chunk" do
      src = tmp("zeros")
      # 64 MiB of zeros: compresses to a few KB (zstd) or ~64 KB (zlib), so a single 1 MiB read
      # is the whole object — the plain-HTTP shape that decoded all 210 MB at once in the panel.
      body = :binary.copy(<<0>>, 64 * 1024 * 1024)
      File.write!(src, body)
      on_exit(fn -> File.rm(src) end)

      {:ok, z} = Codec.compress_to_temp(src, unquote(enc))
      on_exit(fn -> File.rm(z) end)

      stream = Codec.init(unquote(enc))
      on_exit(fn -> Codec.release(stream) end)

      {total, largest} =
        z
        |> File.stream!(1024 * 1024)
        |> Enum.reduce({0, 0}, fn chunk, acc ->
          {:ok, acc} =
            Codec.inflate_into(
              stream,
              chunk,
              fn piece, {t, l} ->
                n = IO.iodata_length(piece)
                {t + n, max(l, n)}
              end,
              acc
            )

          acc
        end)

      assert total == byte_size(body)

      assert largest <= @max_piece,
             "a #{largest}-byte piece reached the writer at once (bound #{@max_piece})"
    end
  end

  test "zstd is the default encoding" do
    prev = Application.get_env(:fathom, :shard_object_encoding)
    on_exit(fn -> restore(prev) end)
    Application.delete_env(:fathom, :shard_object_encoding)
    assert Codec.encoding() == :zstd
  end

  # Decode-always: reading must NOT depend on this node's own encoding setting, or rolling the
  # flag back would make every object written while it was on unreadable.
  test "decoding is independent of what this node writes" do
    prev = Application.get_env(:fathom, :shard_object_encoding)
    on_exit(fn -> restore(prev) end)

    Application.put_env(:fathom, :shard_object_encoding, :none)
    assert Codec.encoding() == :none
    assert Codec.decoder("zlib") == {:ok, :zlib}
    assert Codec.decoder("zstd") == {:ok, :zstd}

    Application.put_env(:fathom, :shard_object_encoding, :zlib)
    assert Codec.encoding() == :zlib
    assert Codec.decoder(nil) == {:ok, :none}
  end

  # THE safety property. An object marked with an encoding this node cannot perform must fail the
  # pull. Handing the raw (still-compressed, or otherwise-encoded) bytes to SQLite as a database
  # is the one way this feature turns into a correctness incident.
  test "an unrecognised marker fails closed" do
    assert {:error, {:unknown_object_encoding, "zstd-v9"}} = Codec.decoder("zstd-v9")
    assert {:error, {:unknown_object_encoding, "gzip"}} = Codec.decoder("gzip")
    assert {:error, {:unknown_object_encoding, "aes256"}} = Codec.decoder("aes256")
  end

  # A missing marker is the pre-existing / encoding-off case, and must stay a plain raw read.
  test "a missing or empty marker reads raw, not as an error" do
    assert Codec.decoder(nil) == {:ok, :none}
    assert Codec.decoder("") == {:ok, :none}
  end

  test "an unencoded upload carries no marker at all" do
    assert Codec.upload_headers(:none) == []
    assert [{header, "zlib"}] = Codec.upload_headers(:zlib)
    assert [{^header, "zstd"}] = Codec.upload_headers(:zstd)
    assert header == Codec.meta_header()
  end

  # A torn compressed transfer must not decode to the whole file. Neither codec REPORTS the tear,
  # so the download's plaintext MD5 is what fails it — this pins that the bytes differ for it.
  for enc <- [:zlib, :zstd] do
    test "#{enc}: decoding a truncated stream does not yield the whole file" do
      src = tmp("torn")
      File.write!(src, :crypto.strong_rand_bytes(400_000))
      on_exit(fn -> File.rm(src) end)

      {:ok, z} = Codec.compress_to_temp(src, unquote(enc))
      on_exit(fn -> File.rm(z) end)

      full = File.read!(z)
      torn = binary_part(full, 0, div(byte_size(full), 2))

      stream = Codec.init(unquote(enc))
      {:ok, partial} = Codec.inflate_into(stream, torn, &[&2, &1], [])
      :ok = Codec.release(stream)

      refute IO.iodata_to_binary(partial) == File.read!(src),
             "a half transfer must not decode to the whole file"
    end
  end

  defp restore(nil), do: Application.delete_env(:fathom, :shard_object_encoding)
  defp restore(v), do: Application.put_env(:fathom, :shard_object_encoding, v)
end
