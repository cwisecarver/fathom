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

    assert {:ok, _, _} = S3.flush("s", local, nil)
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

  # Round-2 expert review #1: the streamed download ran with Req's default
  # `:safe_transient` retry, which re-runs the request over the SAME still-open fd on a
  # mid-body transport error — appending the retry's body after the first attempt's
  # partial bytes, while the per-Response streamed MD5 restarts and certifies the
  # `partial ++ full` result. The download now runs `retry: false` and retries the
  # WHOLE transfer itself with a fresh temp, treating a torn transfer (checksum
  # mismatch) as retryable rather than fatal. The invariant: a transient bad transfer
  # is recovered and the promoted file's bytes match the etag — never a certified-corrupt
  # concatenation.
  test "a torn transfer is retried with a fresh temp and recovers the correct bytes",
       %{dir: dir} do
    attempts = start_supervised!({Agent, fn -> 0 end})

    put_s3_config(fn conn ->
      n = Agent.get_and_update(attempts, &{&1 + 1, &1 + 1})
      # Attempt 1: the store's etag claims the GOOD MD5 but the body is torn/corrupt.
      # Attempt 2+: the clean body.
      body = if n == 1, do: "torn-partial-garbage", else: @good

      conn
      |> Plug.Conn.put_resp_header("etag", md5_etag(@good))
      |> Plug.Conn.send_resp(200, body)
    end)

    local = Path.join(dir, "torn.db")

    # Pre-fix: attempt 1's mismatch was fatal ({:error, :checksum_mismatch}, no file).
    assert {:ok, _etag} = S3.pull("s", local)
    assert File.read!(local) == @good, "the recovered file must be the clean bytes, not a tear"
    assert Agent.get(attempts, & &1) == 2, "must have retried exactly once"
  end

  # Expert review #20: a single PUT past S3's 5 GB ceiling used to fail opaquely
  # mid-transfer; it is now refused up front with an explicit error. (Ceiling
  # shrunk via config so the test doesn't need a 5 GB file.)
  test "a flush past the single-PUT ceiling is refused explicitly", %{dir: dir} do
    put_s3_config(fn conn -> Plug.Conn.send_resp(conn, 200, "") end)
    Application.put_env(:fathom, :s3_max_single_put, 4)
    on_exit(fn -> Application.delete_env(:fathom, :s3_max_single_put) end)

    local = Path.join(dir, "big.db")
    File.write!(local, "way-more-than-four-bytes")

    assert {:error, {:object_too_large, _}} = S3.flush("big", local)
    assert {:error, {:object_too_large, _}} = S3.flush("big", local, nil)
  end

  # Expert review 2026-07-14 #17: download MD5 verification only fired when the etag was the
  # MD5-shaped single-part form. But the steal-time fence ROTATES a single-part etag to
  # MULTIPART form (rotate_etag/touch_object) to invalidate a zombie's If-Match, so right after
  # a steal the object carries a `...-N` etag and the new owner's failover cold-open pull ran
  # with NO content check. A flush now records the body's MD5 in x-amz-meta-fathom-md5 metadata,
  # verified on download regardless of etag shape. Invariants: the metadata is written on flush;
  # a multipart-etag object with correct metadata verifies; a corrupt body under correct
  # metadata fails EVEN THOUGH its etag is the would-be-skipped multipart form.
  defp md5_hex(body), do: Base.encode16(:crypto.hash(:md5, body), case: :lower)

  test "flush records the body md5 as x-amz-meta-fathom-md5 metadata", %{dir: dir} do
    test_pid = self()

    put_s3_config(fn conn ->
      send(test_pid, {:meta, Plug.Conn.get_req_header(conn, "x-amz-meta-fathom-md5")})

      conn
      |> Plug.Conn.put_resp_header("etag", md5_etag(@good))
      |> Plug.Conn.send_resp(200, "")
    end)

    local = Path.join(dir, "flush_meta.db")
    File.write!(local, @good)
    expected = md5_hex(@good)

    assert :ok = S3.flush("s", local)
    assert_receive {:meta, [^expected]}

    assert {:ok, _, _} = S3.flush("s", local, nil)
    assert_receive {:meta, [^expected]}
  end

  test "download verifies against md5 metadata even when the etag is multipart-shaped",
       %{dir: dir} do
    put_s3_config(fn conn ->
      conn
      # Multipart-form etag — verify_md5 alone would SKIP the check (post-steal state).
      |> Plug.Conn.put_resp_header("etag", ~s("deadbeefcafebabe-3"))
      |> Plug.Conn.put_resp_header("x-amz-meta-fathom-md5", md5_hex(@good))
      |> Plug.Conn.send_resp(200, @good)
    end)

    local = Path.join(dir, "meta_ok.db")
    assert {:ok, _etag} = S3.pull("s", local)
    assert File.read!(local) == @good
  end

  test "download fails a corrupt body under correct md5 metadata (multipart etag)",
       %{dir: dir} do
    put_s3_config(fn conn ->
      # Multipart etag (etag-MD5 check would skip) but the metadata pins the GOOD md5; the
      # body is corrupt — the metadata check must still catch it.
      conn
      |> Plug.Conn.put_resp_header("etag", ~s("deadbeefcafebabe-3"))
      |> Plug.Conn.put_resp_header("x-amz-meta-fathom-md5", md5_hex(@good))
      |> Plug.Conn.send_resp(200, "corrupted-bytes")
    end)

    local = Path.join(dir, "meta_bad.db")
    assert {:error, :checksum_mismatch} = S3.pull("s", local)
    refute File.exists?(local), "a corrupt body must never be promoted"
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
