defmodule Fathom.Shard.Storage.S3FenceProbeTest do
  # Expert review #16: every safety property in the system — lease mutual exclusion,
  # the flush fence, conditional release — is load-bearing on the store ENFORCING
  # conditional writes (412 on a failed If-Match / If-None-Match PUT). A store that
  # ignores the headers returns 200, so two concurrent acquirers both "win" and every
  # fenced flush "succeeds": silent split-brain with zero signal until data is lost.
  # Round-2 #4 extends the probe: the steal-time touch must also produce a GENUINELY
  # new etag (both alternating copy forms), else the zombie fence is a silent no-op.
  # The invariant: the boot probe passes on an enforcing+rotating store and raises
  # (refusing the boot) on a lax, non-rotating, or unreachable one.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

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

  # A lax store: accepts every PUT, silently ignoring the conditional headers.
  defp lax_store(conn),
    do: Plug.Conn.send_resp(conn, if(conn.method == "DELETE", do: 204, else: 200), "")

  # Enforces conditional writes but its touch NEVER rotates the etag — the round-2
  # #4 hazard class (an exotic store where copies of identical bytes always etag
  # identically, in every form).
  defp non_rotating_store(conn) do
    q = conn.query_string || ""
    same = ~s("same-forever")

    cond do
      conn.method == "DELETE" ->
        Plug.Conn.send_resp(conn, 204, "")

      conn.method == "HEAD" ->
        conn |> Plug.Conn.put_resp_header("etag", same) |> Plug.Conn.send_resp(200, "")

      conn.method == "POST" and q =~ "uploads" ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>"
        )

      conn.method == "POST" and q =~ "uploadId" ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<CompleteMultipartUploadResult><ETag>&quot;same-forever&quot;</ETag></CompleteMultipartUploadResult>"
        )

      conn.method == "PUT" and q =~ "uploadId" ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<CopyPartResult><ETag>&quot;same-forever&quot;</ETag></CopyPartResult>"
        )

      Plug.Conn.get_req_header(conn, "if-match") not in [[], [same]] ->
        Plug.Conn.send_resp(conn, 412, "")

      Plug.Conn.get_req_header(conn, "if-none-match") == ["*"] ->
        Plug.Conn.send_resp(conn, 412, "")

      true ->
        conn |> Plug.Conn.put_resp_header("etag", same) |> Plug.Conn.send_resp(200, "")
    end
  end

  test "passes against a store that enforces conditional writes and rotates the touch" do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})
    put_s3_config(fn conn -> S3EtagStore.serve(conn, store) end)

    assert S3.verify_conditional_writes!() == :ok
  end

  test "refuses to boot against a store that ignores conditional writes" do
    put_s3_config(&lax_store/1)

    assert_raise RuntimeError, ~r/does not enforce/, fn ->
      S3.verify_conditional_writes!()
    end
  end

  # Round-2 #4: an enforcing store whose touch does not rotate the etag would make
  # the zombie fence a silent no-op — the boot probe must catch it.
  test "refuses to boot against a store whose touch does not rotate the etag" do
    put_s3_config(&non_rotating_store/1)

    assert_raise RuntimeError, ~r/did not rotate/, fn ->
      S3.verify_conditional_writes!()
    end
  end

  test "fails closed when the store is unreachable" do
    put_s3_config(fn _conn -> raise "connection refused" end)

    assert_raise RuntimeError, fn -> S3.verify_conditional_writes!() end
  end
end
