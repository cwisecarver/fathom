defmodule Fathom.Shard.Storage.S3FenceProbeTest do
  # Expert review #16: every safety property in the system — lease mutual exclusion,
  # the flush fence, conditional release — is load-bearing on the store ENFORCING
  # conditional writes (412 on a failed If-Match / If-None-Match PUT). A store that
  # ignores the headers returns 200, so two concurrent acquirers both "win" and every
  # fenced flush "succeeds": silent split-brain with zero signal until data is lost.
  # The moduledoc said "verify the target store before relying on the fence" but
  # nothing did. The invariant: the boot probe passes on an enforcing store and
  # raises (refusing the boot) on a lax or unreachable one.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

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

  # A store that honors conditional writes: unconditional PUT lands; a PUT carrying
  # a wrong If-Match or a create-only If-None-Match against the existing object 412s.
  defp enforcing_store(conn) do
    cond do
      conn.method == "DELETE" ->
        Plug.Conn.send_resp(conn, 204, "")

      Plug.Conn.get_req_header(conn, "if-match") != [] ->
        Plug.Conn.send_resp(conn, 412, "")

      Plug.Conn.get_req_header(conn, "if-none-match") != [] ->
        Plug.Conn.send_resp(conn, 412, "")

      true ->
        conn
        |> Plug.Conn.put_resp_header("etag", ~s("probe-etag"))
        |> Plug.Conn.send_resp(200, "")
    end
  end

  # A lax store: accepts every PUT, silently ignoring the conditional headers.
  defp lax_store(conn),
    do: Plug.Conn.send_resp(conn, if(conn.method == "DELETE", do: 204, else: 200), "")

  test "passes against a store that enforces conditional writes" do
    put_s3_config(&enforcing_store/1)
    assert S3.verify_conditional_writes!() == :ok
  end

  test "refuses to boot against a store that ignores conditional writes" do
    put_s3_config(&lax_store/1)

    assert_raise RuntimeError, ~r/does not enforce/, fn ->
      S3.verify_conditional_writes!()
    end
  end

  test "fails closed when the store is unreachable" do
    put_s3_config(fn _conn -> raise "connection refused" end)

    assert_raise RuntimeError, fn -> S3.verify_conditional_writes!() end
  end
end
