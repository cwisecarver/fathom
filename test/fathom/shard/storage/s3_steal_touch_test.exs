defmodule Fathom.Shard.Storage.S3StealTouchTest do
  # Expert review #3: the steal protocol never touched the DATA object, so an old
  # owner stalled inside a fenced flush it had already passed the fence for (whole-VM
  # pause) could land its PUT after the new owner's pull — the If-Match fence
  # arbitrated in favor of the ZOMBIE. The invariant: a steal invalidates the data
  # object's etag (a conditional self-copy, same bytes) before the new owner serves,
  # so the zombie's late PUT deterministically 412s and IT self-fences.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3

  @lock_etag ~s("lock-v1")
  @data_etag ~s("data-v1")

  setup do
    test = self()

    plug = fn conn ->
      key = conn.request_path

      cond do
        # Optimistic create of the lock: it already exists.
        conn.method == "PUT" and String.ends_with?(key, ".lock") and
            Plug.Conn.get_req_header(conn, "if-none-match") == ["*"] ->
          Plug.Conn.send_resp(conn, 412, "")

        # Read the existing lock: a dead foreign owner (stale TTL, no heartbeat).
        conn.method == "GET" and String.ends_with?(key, ".lock") ->
          body =
            Storage.encode_lease(%{
              owner: "dead@node#old",
              epoch: 5,
              expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
            })

          conn
          |> Plug.Conn.put_resp_header("etag", @lock_etag)
          |> Plug.Conn.send_resp(200, body)

        # The dead owner has no heartbeat object.
        String.contains?(key, "heartbeats/") ->
          Plug.Conn.send_resp(conn, 404, "")

        # The steal PUT of the lock (If-Match on the read etag).
        conn.method == "PUT" and String.ends_with?(key, ".lock") ->
          send(test, {:lock_put, Plug.Conn.get_req_header(conn, "if-match")})

          conn
          |> Plug.Conn.put_resp_header("etag", ~s("lock-v2"))
          |> Plug.Conn.send_resp(200, "")

        # The touch's HEAD of the data object.
        conn.method == "HEAD" ->
          conn
          |> Plug.Conn.put_resp_header("etag", @data_etag)
          |> Plug.Conn.send_resp(200, "")

        # The touch itself: a conditional self-copy.
        conn.method == "PUT" ->
          send(
            test,
            {:touch,
             %{
               source: Plug.Conn.get_req_header(conn, "x-amz-copy-source"),
               directive: Plug.Conn.get_req_header(conn, "x-amz-metadata-directive"),
               if_match: Plug.Conn.get_req_header(conn, "x-amz-copy-source-if-match")
             }}
          )

          conn
          |> Plug.Conn.put_resp_header("etag", ~s("data-v2-touched"))
          |> Plug.Conn.send_resp(200, "")
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

  test "a steal touches the data object's etag before the new owner serves" do
    assert {:ok, lease} = S3.acquire_lease("shard_x", "new@node#inc2", 30_000)

    assert lease.epoch == 6, "a steal must bump the epoch"
    assert lease[:took_over] == true, "the caller must know to revalidate its pull"

    assert_received {:lock_put, [@lock_etag]}

    assert_received {:touch,
                     %{source: ["/b/shard_x.db"], directive: ["REPLACE"], if_match: [@data_etag]}}
  end
end
