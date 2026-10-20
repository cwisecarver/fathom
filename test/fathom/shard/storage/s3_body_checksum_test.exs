defmodule Fathom.Shard.Storage.S3BodyChecksumTest do
  # Expert review 2026-10-20 #29: the compressed PUT's transit check moves from content-md5
  # (MD5 over the compressed body, ~1.2 ms/MB) to x-amz-checksum-crc32 (~0.03 ms/MB), but ONLY when a
  # boot probe shows the store enforces it — a store that ignores the header would silently drop the
  # integrity gate. Invariants: probe rejects-wrong/accepts-right => :crc32; any other answer =>
  # MD5 fallback; the uncompressed (:none) path is untouched.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.{Codec, S3}

  setup do
    dir = Path.join(System.tmp_dir!(), "s3crc_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    prev_enc = Application.get_env(:fathom, :shard_object_encoding)
    prev_s3 = Application.get_env(:fathom, S3)
    S3.reset_body_hash_kind()

    on_exit(fn ->
      File.rm_rf!(dir)
      S3.reset_body_hash_kind()

      if prev_enc,
        do: Application.put_env(:fathom, :shard_object_encoding, prev_enc),
        else: Application.delete_env(:fathom, :shard_object_encoding)

      if prev_s3,
        do: Application.put_env(:fathom, S3, prev_s3),
        else: Application.delete_env(:fathom, S3)
    end)

    %{dir: dir}
  end

  defp put_s3_config(plug) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug
    )
  end

  defp etag(body),
    do: ~s(") <> Base.encode16(:crypto.hash(:md5, body), case: :lower) <> ~s(")

  # A store that ENFORCES x-amz-checksum-crc32: 400 BadDigest on a mismatch.
  defp enforcing_store(test_pid) do
    fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      send(test_pid, {:put, conn.request_path, conn.req_headers, body})

      case Plug.Conn.get_req_header(conn, "x-amz-checksum-crc32") do
        [given] ->
          if given == Base.encode64(<<:erlang.crc32(body)::32>>) do
            conn |> Plug.Conn.put_resp_header("etag", etag(body)) |> Plug.Conn.send_resp(200, "")
          else
            Plug.Conn.send_resp(conn, 400, "BadDigest")
          end

        _ ->
          conn |> Plug.Conn.put_resp_header("etag", etag(body)) |> Plug.Conn.send_resp(200, "")
      end
    end
  end

  # A store that ignores the header and says 200 to anything.
  defp lax_store(test_pid) do
    fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      send(test_pid, {:put, conn.request_path, conn.req_headers, body})
      conn |> Plug.Conn.put_resp_header("etag", etag(body)) |> Plug.Conn.send_resp(200, "")
    end
  end

  defp flush_file(dir, encoding) do
    Application.put_env(:fathom, :shard_object_encoding, encoding)
    local = Path.join(dir, "s.db")
    File.write!(local, String.duplicate("row-data-0123456789 ", 20_000))
    assert {:ok, _, _} = S3.flush("s", local, nil)
  end

  defp last_put do
    # The flush PUT is the last one sent; earlier mailbox entries are probe PUTs.
    case drain_puts([]) do
      [{headers, body} | _] -> {Map.new(headers), body}
      [] -> flunk("no PUT received")
    end
  end

  defp drain_puts(acc) do
    receive do
      {:put, _path, headers, body} -> drain_puts([{headers, body} | acc])
    after
      0 -> acc
    end
  end

  test "default (probe not run): compressed PUT keeps content-md5", %{dir: dir} do
    put_s3_config(enforcing_store(self()))
    assert S3.body_hash_kind() == :md5
    flush_file(dir, :zstd)
    {headers, body} = last_put()
    assert headers["content-md5"] == Base.encode64(:crypto.hash(:md5, body))
    refute Map.has_key?(headers, "x-amz-checksum-crc32")
  end

  test "probe against an enforcing store selects crc32; the PUT carries the body's CRC32", %{
    dir: dir
  } do
    put_s3_config(enforcing_store(self()))
    assert S3.probe_body_checksum!() == :crc32
    assert S3.body_hash_kind() == :crc32

    # The probe sent a wrong checksum (refused) and a right one (accepted).
    assert_received {:put, _, _, _}
    assert_received {:put, _, _, _}

    flush_file(dir, :zstd)
    {headers, body} = last_put()
    assert headers["x-amz-checksum-crc32"] == Base.encode64(<<:erlang.crc32(body)::32>>)
    refute Map.has_key?(headers, "content-md5")
    # The identity digest is still the plaintext MD5.
    assert headers["x-amz-meta-fathom-md5"] =~ ~r/^[0-9a-f]{32}$/
  end

  test "probe against a store that ignores the header falls back to MD5", %{dir: dir} do
    put_s3_config(lax_store(self()))
    assert S3.probe_body_checksum!() == :md5

    flush_file(dir, :zstd)
    {headers, body} = last_put()
    assert headers["content-md5"] == Base.encode64(:crypto.hash(:md5, body))
    refute Map.has_key?(headers, "x-amz-checksum-crc32")
  end

  test "probe against a store that refuses even a correct checksum falls back to MD5" do
    put_s3_config(fn conn ->
      {:ok, _body, conn} = Plug.Conn.read_body(conn)
      Plug.Conn.send_resp(conn, 400, "InvalidRequest")
    end)

    assert S3.probe_body_checksum!() == :md5
  end

  test "the uncompressed path keeps content-md5 even when crc32 was selected", %{dir: dir} do
    put_s3_config(enforcing_store(self()))
    assert S3.probe_body_checksum!() == :crc32
    flush_file(dir, :none)
    {headers, body} = last_put()
    assert headers["content-md5"] == Base.encode64(:crypto.hash(:md5, body))
    refute Map.has_key?(headers, "x-amz-checksum-crc32")
  end

  test "Codec.compress_to_temp_hashed/3 :crc32 digest is the big-endian CRC32 of the body file",
       %{
         dir: dir
       } do
    src = Path.join(dir, "src.db")
    File.write!(src, String.duplicate("abc123", 100_000))
    assert {:ok, tmp, size, digest, _hex} = Codec.compress_to_temp_hashed(src, :zstd, :crc32)
    body = File.read!(tmp)
    assert size == byte_size(body)
    assert digest == <<:erlang.crc32(body)::32>>
  end
end
