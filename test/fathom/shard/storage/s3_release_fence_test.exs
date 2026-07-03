defmodule Fathom.Shard.Storage.S3ReleaseFenceTest do
  # Finding #22: release_lease deletes the lock object, but the delete was unconditional. A
  # stall between reading the lock (to confirm it's ours) and issuing the DELETE lets a stealer
  # write its own lock in the gap — and an unconditional DELETE then removes THAT lock. The fix
  # fences the delete with If-Match on the etag we read, so a 412 (the object changed) is a
  # no-op. The pure network race isn't reproducible in the default suite, so we pin the
  # observable guarantee via a Req plug stub: the DELETE the backend issues carries If-Match.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3

  @etag "\"lock-etag-v1\""

  setup do
    test = self()

    plug = fn conn ->
      case conn.method do
        "GET" ->
          # get_lock reads the current lock + its etag.
          body = Storage.encode_lease(%{owner: "a@node", epoch: 1, expires_at_ms: 1})

          conn
          |> Plug.Conn.put_resp_header("etag", @etag)
          |> Plug.Conn.send_resp(200, body)

        "DELETE" ->
          # Report whether the release DELETE was fenced with If-Match.
          send(test, {:delete_if_match, Plug.Conn.get_req_header(conn, "if-match")})
          Plug.Conn.send_resp(conn, 204, "")
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

  test "release_lease issues a DELETE fenced with If-Match on the lock etag" do
    assert :ok = S3.release_lease("shard_x", %{owner: "a@node", epoch: 1, expires_at_ms: 1})

    assert_received {:delete_if_match, if_match}
    assert if_match != [], "release DELETE must be fenced with If-Match (finding #22)"
  end
end
