defmodule Fathom.Shard.Storage.S3LeaseEtagTest do
  # Review 2026-07-23 #6: the drain path's third RTT was a lock GET inside release_lease that
  # existed only to re-learn the etag of the lock object WE ourselves last wrote — create_lock
  # and put_lock received it in the PUT response and threw it away. The lease now carries
  # `:lock_etag` (client-side only; encode_lease never serializes it), so release is a single
  # conditional DELETE and renew a single conditional PUT. These pin the request shapes: the
  # fast paths must issue NO lock GET, and the fence (If-Match) must carry the cached etag.
  # The legacy no-etag paths keep their read-then-fence shape (s3_release_fence_test pins it).
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  @etag_v1 "\"lock-etag-v1\""
  @etag_v2 "\"lock-etag-v2\""

  setup do
    test = self()

    plug = fn conn ->
      send(test, {:req, conn.method, Plug.Conn.get_req_header(conn, "if-match")})

      case conn.method do
        "PUT" ->
          conn
          |> Plug.Conn.put_resp_header("etag", @etag_v2)
          |> Plug.Conn.send_resp(200, "")

        "DELETE" ->
          Plug.Conn.send_resp(conn, 204, "")

        _other ->
          Plug.Conn.send_resp(conn, 500, "unexpected #{conn.method}")
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
      req_plug: plug
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    :ok
  end

  test "release_lease with a cached lock etag is ONE conditional DELETE — no lock GET" do
    lease = %{owner: "a@node", epoch: 1, expires_at_ms: 1, lock_etag: @etag_v1}
    assert :ok = S3.release_lease("shard_x", lease)

    assert_received {:req, "DELETE", if_match}
    assert if_match == [@etag_v1]
    refute_received {:req, "GET", _}, "the fast release must not re-read the lock (#6)"
  end

  test "renew_lease with a cached lock etag is ONE conditional PUT, and re-caches the new etag" do
    lease = %{owner: "a@node", epoch: 3, expires_at_ms: 1, lock_etag: @etag_v1}
    assert {:ok, renewed} = S3.renew_lease("shard_x", lease, 30_000)

    assert_received {:req, "PUT", if_match}
    assert if_match == [@etag_v1]
    refute_received {:req, "GET", _}, "the fast renew must not re-read the lock (#6)"

    # The renewed lease carries the NEW lock etag (each renew rewrites the object), so the
    # next renew/release stays on the 1-RTT path.
    assert renewed.lock_etag == @etag_v2
    assert renewed.owner == "a@node" and renewed.epoch == 3
  end

  test "a fresh acquire caches the created lock's etag in the lease" do
    assert {:ok, lease} = S3.acquire_lease("shard_x", "a@node", 30_000)
    assert lease.lock_etag == @etag_v2
    assert_received {:req, "PUT", _}
  end
end
