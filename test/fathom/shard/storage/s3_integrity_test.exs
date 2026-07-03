defmodule Fathom.Shard.Storage.S3IntegrityTest do
  # Expert review #37: flushes PUT the shard bytes with no integrity header, and pulls
  # never verified the body against the object's etag. TLS/TCP catch wire corruption,
  # but nothing caught corruption introduced BEFORE the socket (torn read off a bad
  # disk, buggy proxy, store bug) — a corrupted upload became the durable truth and a
  # corrupted download became the served shard, discovered only after the good copy
  # was gone. Invariants: every data PUT carries Content-MD5; a pulled body that
  # mismatches an MD5-shaped etag is an error, never written; non-MD5 (multipart)
  # etags skip verification rather than false-positive.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  @good "shard-bytes"

  setup do
    dir = Path.join(System.tmp_dir!(), "s3int_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp put_s3_config(plug) do
    prev = Application.get_env(:fathom, S3)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)
  end

  defp md5_etag(body),
    do: ~s(") <> Base.encode16(:crypto.hash(:md5, body), case: :lower) <> ~s(")

  test "data PUTs carry a Content-MD5 of the body", %{dir: dir} do
    test_pid = self()

    put_s3_config(fn conn ->
      send(test_pid, {:content_md5, Plug.Conn.get_req_header(conn, "content-md5")})

      conn
      |> Plug.Conn.put_resp_header("etag", md5_etag(@good))
      |> Plug.Conn.send_resp(200, "")
    end)

    local = Path.join(dir, "s.db")
    File.write!(local, @good)

    assert :ok = S3.flush("s", local)
    expected = Base.encode64(:crypto.hash(:md5, @good))
    assert_receive {:content_md5, [^expected]}

    assert {:ok, _} = S3.flush("s", local, nil)
    assert_receive {:content_md5, [^expected]}
  end

  test "a pulled body that mismatches an MD5 etag errors and writes nothing", %{dir: dir} do
    put_s3_config(fn conn ->
      # The store claims the MD5 of the GOOD bytes but serves corrupted ones.
      conn
      |> Plug.Conn.put_resp_header("etag", md5_etag(@good))
      |> Plug.Conn.send_resp(200, "corrupted-bytes")
    end)

    local = Path.join(dir, "pull.db")

    assert {:error, :checksum_mismatch} = S3.pull("s", local)
    refute File.exists?(local), "a corrupted body must never be written"

    assert {:error, :checksum_mismatch} = S3.pull_if_changed("s", Path.join(dir, "w.db"), nil)
  end

  test "a matching MD5 etag pulls normally; non-MD5 etags skip verification", %{dir: dir} do
    put_s3_config(fn conn ->
      case Plug.Conn.request_url(conn) =~ "multi" do
        # Multipart-style etag: not an MD5 — must not false-positive.
        true ->
          conn
          |> Plug.Conn.put_resp_header("etag", ~s("abc123-2"))
          |> Plug.Conn.send_resp(200, @good)

        false ->
          conn
          |> Plug.Conn.put_resp_header("etag", md5_etag(@good))
          |> Plug.Conn.send_resp(200, @good)
      end
    end)

    ok = Path.join(dir, "ok.db")
    assert {:ok, _etag} = S3.pull("s", ok)
    assert File.read!(ok) == @good

    multi = Path.join(dir, "multi.db")
    assert {:ok, _etag} = S3.pull("multi", multi)
    assert File.read!(multi) == @good
  end
end
