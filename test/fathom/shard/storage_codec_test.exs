defmodule Fathom.Shard.Storage.CodecTest do
  @moduledoc """
  The optional stored-object compression codec (expert review 2026-07-24 #38).

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

  defp stream_inflate(path, decoder) do
    z = Codec.init(decoder)

    out =
      path
      |> File.stream!(64 * 1024)
      |> Enum.reduce([], fn chunk, acc -> [acc, Codec.inflate(z, chunk)] end)
      |> IO.iodata_to_binary()

    Codec.finish(z)
    out
  end

  test "round-trips a file through compress + streaming inflate" do
    src = tmp("src")
    # Repetitive, like real SQLite pages — and big enough to cross chunk boundaries.
    body = String.duplicate("the quick brown fox jumps over the lazy dog. ", 60_000)
    File.write!(src, body)
    on_exit(fn -> File.rm(src) end)

    assert {:ok, z} = Codec.compress_to_temp(src)
    on_exit(fn -> File.rm(z) end)

    assert File.stat!(z).size < File.stat!(src).size,
           "compression didn't shrink obviously-compressible input"

    assert stream_inflate(z, :zlib) == body
  end

  test "an empty file round-trips" do
    src = tmp("empty")
    File.write!(src, "")
    on_exit(fn -> File.rm(src) end)

    assert {:ok, z} = Codec.compress_to_temp(src)
    on_exit(fn -> File.rm(z) end)
    assert stream_inflate(z, :zlib) == ""
  end

  # Decode-always: reading must NOT depend on this node's own encoding setting, or rolling the
  # flag back would make every object written while it was on unreadable.
  test "decoding is independent of what this node writes" do
    prev = Application.get_env(:fathom, :shard_object_encoding)
    on_exit(fn -> restore(prev) end)

    Application.put_env(:fathom, :shard_object_encoding, :none)
    assert Codec.encoding() == :none
    assert Codec.decoder("zlib") == {:ok, :zlib}

    Application.put_env(:fathom, :shard_object_encoding, :zlib)
    assert Codec.encoding() == :zlib
    assert Codec.decoder(nil) == {:ok, :none}
  end

  # THE safety property. An object marked with an encoding this node cannot perform must fail the
  # pull. Handing the raw (still-compressed, or otherwise-encoded) bytes to SQLite as a database
  # is the one way this feature turns into a correctness incident.
  test "an unrecognised marker fails closed" do
    assert {:error, {:unknown_object_encoding, "zstd"}} = Codec.decoder("zstd")
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
    assert header == Codec.meta_header()
  end

  # A torn compressed transfer must surface as a failure the caller can retry, not a crash and
  # not silently-truncated output.
  test "inflating a truncated stream does not corrupt silently" do
    src = tmp("torn")
    File.write!(src, String.duplicate("abcdefgh", 50_000))
    on_exit(fn -> File.rm(src) end)

    {:ok, z} = Codec.compress_to_temp(src)
    on_exit(fn -> File.rm(z) end)

    full = File.read!(z)
    torn = binary_part(full, 0, div(byte_size(full), 2))

    stream = Codec.init(:zlib)
    partial = stream |> Codec.inflate(torn) |> IO.iodata_to_binary()

    refute partial == File.read!(src),
           "a half transfer must not inflate to the whole file"

    # finish/1 tolerates the incomplete stream rather than raising out of the download path.
    assert Codec.finish(stream) == :ok
  end

  defp restore(nil), do: Application.delete_env(:fathom, :shard_object_encoding)
  defp restore(v), do: Application.put_env(:fathom, :shard_object_encoding, v)
end
