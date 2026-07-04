defmodule Fathom.Shard.Storage.S3HeartbeatKeyTest do
  # Round-2 expert review #3: the incarnation-qualified owner is `node()#<nonce>`, and
  # heartbeat_path embedded it raw in the request URL. Req parses the URL with URI.parse,
  # where `#` delimits a FRAGMENT that HTTP never transmits — so renew/read/clear for
  # every incarnation of a node name collapsed onto `heartbeats/<node>` with the nonce
  # silently stripped, voiding #6's boot-scoped identity on S3 while the Local double
  # (using `#` as a legal filename char) passed. The invariant: the owner is
  # percent-encoded, so two incarnations of one node name hit DISTINCT keys and the `#`
  # reaches the server (in the path, not a dropped fragment).
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  setup do
    test = self()

    plug = fn conn ->
      # Report the exact request path (Plug has already stripped any URI fragment, so a
      # raw `#` would show up as a truncated path here).
      send(test, {:req_path, conn.request_path})

      case conn.method do
        "GET" -> Plug.Conn.send_resp(conn, 404, "")
        _ -> Plug.Conn.send_resp(conn, 200, "")
      end
    end

    prev = Application.get_env(:fathom, S3)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      prefix: "shards/",
      req_plug: plug
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    :ok
  end

  test "the heartbeat key percent-encodes the owner so the incarnation nonce survives" do
    {:ok, _} = S3.renew_heartbeat("fathom@10.0.0.1#aaaa1111", 30_000)
    assert_received {:req_path, path_a}

    {:ok, _} = S3.renew_heartbeat("fathom@10.0.0.1#bbbb2222", 30_000)
    assert_received {:req_path, path_b}

    # The nonce reaches the server encoded, not stripped as a URI fragment.
    assert path_a =~ "aaaa1111",
           "the nonce must be in the transmitted path, not a dropped fragment"

    assert path_b =~ "bbbb2222"

    # Two incarnations of the SAME node name map to DISTINCT keys.
    assert path_a != path_b, "each incarnation must have its own heartbeat object"

    # And the shared prefix is `heartbeats/<node>...`, with the node name encoded too.
    assert path_a =~ "heartbeats/fathom%4010.0.0.1%23aaaa1111"
  end
end
