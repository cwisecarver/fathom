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

  # Expert review 2026-10-08 #15: the upload path proves a compressed body decodes to the plaintext
  # hash it will be labelled with before PUTting it.
  for enc <- [:zlib, :zstd] do
    test "#{enc}: verify_decodes_to accepts the right hash and refuses any other" do
      src = tmp("verify")
      File.write!(src, String.duplicate("row data ", 50_000))
      on_exit(fn -> File.rm(src) end)

      {:ok, z} = Codec.compress_to_temp(src, unquote(enc))
      on_exit(fn -> File.rm(z) end)

      right = Base.encode16(:crypto.hash(:md5, File.read!(src)), case: :lower)
      wrong = Base.encode16(:crypto.hash(:md5, "something else"), case: :lower)

      assert :ok = Codec.verify_decodes_to(z, unquote(enc), right)

      assert {:error, {:compressed_body_mismatch, ^right}} =
               Codec.verify_decodes_to(z, unquote(enc), wrong)

      # A body that does not decode at all is an error too, never a raise.
      bad = tmp("garbage")
      File.write!(bad, String.duplicate("not compressed ", 1000))
      on_exit(fn -> File.rm(bad) end)
      assert {:error, {unquote(enc), _}} = Codec.verify_decodes_to(bad, unquote(enc), right)
    end
  end

  # Expert review 2026-10-17 #20: the decode proof is inline in `compress_to_temp_hashed/2`, over the
  # pieces as they are produced, instead of a second read of the finished file. A codec step that
  # corrupts the compressed bytes must be refused THERE, with no temp file left behind, and a clean run
  # must still hand back the body and its digests. The corruption goes in before the bytes are written,
  # hashed or decoded, so the body on disk and its content-md5 agree, and only the decode can refuse it.
  for enc <- [:zlib, :zstd] do
    test "#{enc}: a compressed body that would not decode to its plaintext digest is refused inline" do
      src = tmp("inline_corrupt")
      File.write!(src, String.duplicate("row data ", 50_000))
      on_exit(fn -> File.rm(src) end)

      Application.put_env(:fathom, :codec_corrupt_hook, fn piece ->
        case IO.iodata_to_binary(piece) do
          bin when byte_size(bin) < 9 ->
            bin

          bin ->
            mid = div(byte_size(bin), 2)
            <<head::binary-size(^mid), b, tail::binary>> = bin
            <<head::binary, Bitwise.bxor(b, 0xFF), tail::binary>>
        end
      end)

      on_exit(fn -> Application.delete_env(:fathom, :codec_corrupt_hook) end)

      assert {:error, _} = Codec.compress_to_temp_hashed(src, unquote(enc))

      leftovers =
        src
        |> Path.dirname()
        |> File.ls!()
        |> Enum.filter(&String.starts_with?(&1, Path.basename(src) <> ".z."))

      assert leftovers == [], "a refused body left its temp file behind: #{inspect(leftovers)}"
    end

    test "#{enc}: a clean compress_to_temp_hashed returns a body that decodes to its plaintext digest" do
      src = tmp("inline_clean")
      File.write!(src, String.duplicate("row data ", 50_000))
      on_exit(fn -> File.rm(src) end)

      assert {:ok, z, size, _body_md5, plain_hex} =
               Codec.compress_to_temp_hashed(src, unquote(enc))

      on_exit(fn -> File.rm(z) end)

      assert plain_hex == Base.encode16(:crypto.hash(:md5, File.read!(src)), case: :lower)
      assert size == File.stat!(z).size
      assert :ok = Codec.verify_decodes_to(z, unquote(enc), plain_hex)
    end
  end

  # Expert review 2026-10-19 #8: the inline decode proof no longer hashes the decoded output; the
  # zstd frame checksum is the only thing that catches a flip inside a RAW block (incompressible
  # data is stored verbatim, so the decoder sees nothing structurally wrong). Without
  # `checksumFlag` this body is accepted and PUT with a plaintext label it does not decode to.
  test "zstd: a flipped byte inside an incompressible (raw-block) body is refused by the frame checksum" do
    src = tmp("raw_block")
    File.write!(src, :crypto.strong_rand_bytes(600_000))
    on_exit(fn -> File.rm(src) end)

    Application.put_env(:fathom, :codec_corrupt_hook, fn piece ->
      bin = IO.iodata_to_binary(piece)

      if byte_size(bin) < 1_000 do
        bin
      else
        mid = div(byte_size(bin), 2)
        <<head::binary-size(^mid), b, tail::binary>> = bin
        <<head::binary, Bitwise.bxor(b, 0xFF), tail::binary>>
      end
    end)

    on_exit(fn -> Application.delete_env(:fathom, :codec_corrupt_hook) end)

    assert {:error, {:zstd, msg}} = Codec.compress_to_temp_hashed(src, :zstd)
    assert msg =~ "checksum"
  end

  test "zstd: frames written before the checksum flag (and with it) both still decode" do
    plain = String.duplicate("old frame ", 20_000)

    for flag <- [false, true] do
      {:ok, c} = :zstd.context(:compress, %{compressionLevel: 3, checksumFlag: flag})
      {:done, frame} = :zstd.finish(c, plain)
      :zstd.close(c)
      path = tmp("oldframe")
      File.write!(path, frame)
      on_exit(fn -> File.rm(path) end)
      assert stream_inflate(path, :zstd) == plain
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

  # --- the steal-touch nonce (expert review 2026-10-12 N1) ---
  #
  # A small object's steal-touch appends a zstd skippable frame with a random nonce, so the
  # STORED bytes (and so the MD5 etag) change while the DECODED database does not. Every S3 read
  # decodes through `inflate_into/4` (the streamed download behind pull, pull_snapshot, version
  # restore, fork's cold-open, export and the restore drill) or through it on a ranged prefix
  # (`object_user_version`), so these pin that the decoder skips the frame at every chunk
  # boundary — including a boundary that splits the frame's own header.

  defp zstd_of(plain), do: plain |> :zstd.compress() |> IO.iodata_to_binary()

  defp inflate_chunks(body, chunk) do
    z = Codec.init(:zstd)

    out =
      for <<piece::binary-size(^chunk) <- pad_chunks(body, chunk)>>, reduce: [] do
        acc ->
          {:ok, acc} = Codec.inflate_into(z, piece, &[&2, &1], acc)
          acc
      end

    rest = binary_part(body, div(byte_size(body), chunk) * chunk, rem(byte_size(body), chunk))
    {:ok, out} = Codec.inflate_into(z, rest, &[&2, &1], out)
    :ok = Codec.release(z)
    IO.iodata_to_binary(out)
  end

  defp pad_chunks(body, chunk), do: binary_part(body, 0, div(byte_size(body), chunk) * chunk)

  test "a renonced zstd body decodes to the same bytes at every chunk size" do
    plain = String.duplicate("SQLite page bytes, mostly repetitive. ", 20_000)
    nonced = plain |> zstd_of() |> Codec.renonce() |> Codec.renonce()

    for chunk <- [1, 3, 7, 32, 4096, 16 * 1024, byte_size(nonced)] do
      assert inflate_chunks(nonced, chunk) == plain, "chunk size #{chunk}"
    end

    hex = Base.encode16(:crypto.hash(:md5, plain), case: :lower)
    assert :ok = Codec.verify_binary_decodes_to(nonced, :zstd, hex)

    path = tmp("nonced")
    File.write!(path, nonced)
    on_exit(fn -> File.rm(path) end)
    assert :ok = Codec.verify_decodes_to(path, :zstd, hex)
  end

  test "renonce changes the stored bytes every time and replaces, never stacks, the nonce" do
    base = zstd_of("db")
    n1 = Codec.renonce(base)
    n2 = Codec.renonce(n1)
    n3 = Codec.renonce(n2)

    assert length(Enum.uniq([base, n1, n2, n3])) == 4, "every touch must change the stored bytes"
    assert byte_size(n1) == byte_size(base) + 32
    assert byte_size(n3) == byte_size(n1), "a later touch replaces the previous nonce"
    assert binary_part(n3, 0, byte_size(base)) == base, "the database frames are untouched"
  end

  test "a body that merely ends like a nonce frame is never truncated by renonce" do
    # Not our tag: a foreign skippable frame (another magic value) must be kept intact.
    foreign = <<0x184D2A50::little-32, 24::little-32, "othertag", 0::128>>
    body = zstd_of("db") <> foreign
    assert binary_part(Codec.renonce(body), 0, byte_size(body)) == body
  end

  for from <- [:none, :zlib] do
    test "#{from}: reencode_zstd yields a zstd body of the same plaintext and its digest" do
      plain = :crypto.strong_rand_bytes(70_000) <> String.duplicate("x", 300_000)
      body = if unquote(from) == :zlib, do: :zlib.compress(plain), else: plain

      assert {:ok, zbody, hex} = Codec.reencode_zstd(body, unquote(from))
      assert hex == Base.encode16(:crypto.hash(:md5, plain), case: :lower)
      assert inflate_chunks(Codec.renonce(zbody), 16 * 1024) == plain
    end
  end

  test "reencode_zstd reports an undecodable zlib body as an error" do
    assert {:error, {:zlib, _}} = Codec.reencode_zstd("not zlib at all", :zlib)
  end
end
