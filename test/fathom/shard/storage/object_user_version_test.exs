defmodule Fathom.Shard.Storage.ObjectUserVersionTest do
  @moduledoc """
  `Storage.object_user_version/1` — expert review 2026-10-10 R3-5.

  Symptom: `Tenants.fork` read the copy's schema version by pulling the WHOLE object to a scratch
  file just to read four header bytes. S3 objects are zstd/zlib-encoded, so a raw byte range of the
  object cannot be read directly; the fix is a ranged GET of the first 256 KiB, stream-decoded just
  far enough to reach the SQLite header. Invariants pinned here: the S3 path asks for a RANGE (never
  the whole object), decodes every codec, treats 404/sentinel as absent, and reports a prefix that
  never decodes to 100 bytes as `:header_unreachable` (the caller's cue to fall back to a pull).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.{Codec, Local, S3}

  @range 262_144

  # A file with a valid SQLite header (magic + user_version at offset 60) followed by `pad` bytes
  # of incompressible-ish filler, so the encoded object is bigger than the ranged prefix.
  defp fake_db(version, pad) do
    header =
      <<"SQLite format 3\0", 0::size(352), version::signed-big-32, 0::size(288)>>

    header <> :crypto.strong_rand_bytes(pad)
  end

  defp tmp(name),
    do: Path.join(System.tmp_dir!(), "ouv_#{name}_#{System.unique_integer([:positive])}")

  describe "Local" do
    setup do
      dir = tmp("local")
      File.mkdir_p!(dir)
      prev = Application.get_env(:fathom, Local)
      Application.put_env(:fathom, Local, dir: dir)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, Local, prev),
          else: Application.delete_env(:fathom, Local)

        File.rm_rf(dir)
      end)
    end

    test "reads the header of a stored object; absent for a missing one; error for a non-database" do
      src = tmp("src")
      File.write!(src, fake_db(7, 4096))
      on_exit(fn -> File.rm(src) end)
      :ok = Local.flush("ouv-local", src)

      assert {:ok, 7} = Local.object_user_version("ouv-local")
      assert {:absent, nil} = Local.object_user_version("ouv-nope")

      File.write!(src, String.duplicate("not a database", 20))
      :ok = Local.flush("ouv-junk", src)
      assert {:error, _} = Local.object_user_version("ouv-junk")
    end
  end

  describe "S3 (req_plug seam, encoded fixtures)" do
    setup do
      prev = Application.get_env(:fathom, S3)
      on_exit(fn -> restore(prev) end)
      %{prev: prev}
    end

    defp restore(nil), do: Application.delete_env(:fathom, S3)
    defp restore(prev), do: Application.put_env(:fathom, S3, prev)

    defp put_store(plug) do
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

    # A store holding `object` (already encoded) that HONOURS the Range header like S3 does, and
    # reports each request's range to the test.
    defp ranged_store(object, enc_marker, test_pid) do
      fn conn ->
        range = Plug.Conn.get_req_header(conn, "range")
        send(test_pid, {:range, conn.method, range})

        body =
          case range do
            ["bytes=0-" <> last] ->
              binary_part(object, 0, min(byte_size(object), String.to_integer(last) + 1))

            _ ->
              object
          end

        conn =
          if enc_marker,
            do: Plug.Conn.put_resp_header(conn, Codec.meta_header(), enc_marker),
            else: conn

        Plug.Conn.send_resp(conn, if(range == [], do: 200, else: 206), body)
      end
    end

    defp encode(plain, enc) do
      src = tmp("plain")
      File.write!(src, plain)
      {:ok, z} = Codec.compress_to_temp(src, enc)
      bin = File.read!(z)
      File.rm(src)
      File.rm(z)
      bin
    end

    for enc <- [:zstd, :zlib] do
      test "#{enc}: a ranged prefix of an object much bigger than the range yields the version" do
        # 1 MiB of random filler: the encoded object is far larger than the 256 KiB range, so a
        # whole-object decode is impossible from the prefix — the version must come from the head.
        object = encode(fake_db(42, 1_048_576), unquote(enc))
        assert byte_size(object) > @range

        put_store(ranged_store(object, Atom.to_string(unquote(enc)), self()))

        assert {:ok, 42} = S3.object_user_version("ouv-s3")
        assert_received {:range, "GET", ["bytes=0-" <> last]}

        assert String.to_integer(last) + 1 == @range,
               "must ask for a bounded prefix, not the object"
      end
    end

    test "a raw (unmarked) object" do
      put_store(ranged_store(fake_db(5, 2048), nil, self()))
      assert {:ok, 5} = S3.object_user_version("ouv-raw")
    end

    test "a negative user_version round-trips as signed" do
      put_store(ranged_store(fake_db(-3, 2048), nil, self()))
      assert {:ok, -3} = S3.object_user_version("ouv-neg")
    end

    test "404 is absent" do
      put_store(fn conn -> Plug.Conn.send_resp(conn, 404, "") end)
      assert {:absent, nil} = S3.object_user_version("ouv-404")
    end

    test "a steal sentinel is absent, not a database" do
      put_store(fn conn ->
        conn
        |> Plug.Conn.put_resp_header("x-amz-meta-fathom-sentinel", "1")
        |> Plug.Conn.send_resp(206, "")
      end)

      assert {:absent, nil} = S3.object_user_version("ouv-sentinel")
    end

    test "an unknown encoding marker fails closed" do
      put_store(ranged_store(fake_db(1, 512), "brotli", self()))
      assert {:error, {:unknown_object_encoding, "brotli"}} = S3.object_user_version("ouv-enc")
    end

    test "a prefix that never decodes to 100 bytes is :header_unreachable (caller falls back)" do
      object = binary_part(encode(fake_db(9, 4096), :zstd), 0, 12)
      put_store(ranged_store(object, "zstd", self()))
      assert {:error, {:header_unreachable, _}} = S3.object_user_version("ouv-short")
    end

    test "a non-200/206/404 status is an error" do
      put_store(fn conn -> Plug.Conn.send_resp(conn, 403, "") end)
      assert {:error, {:s3_ranged_get_status, 403}} = S3.object_user_version("ouv-403")
    end
  end
end
