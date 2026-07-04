defmodule Fathom.Shard.Storage.S3StealTouchRollbackTest do
  @moduledoc """
  Round-2 expert review #20: a steal writes the epoch+1 lock BEFORE the data-object
  touch, so a failed touch (fail-closed, refuses to serve) left the fresh
  `{us, epoch+1}` lock behind. The same node's next checkout then matched the
  RECLAIM path — same owner + epoch — and got the lease back with no `took_over`
  and no re-touch: the zombie fence and the takeover revalidation were both skipped
  for a takeover that was never fenced (stale reads + accepted-write loss; the etag
  data-fence backstops the outright clobber). The invariant: a failed steal-touch
  rolls the lock back to the dead owner's content, so the retry redoes the FULL
  steal + touch.

  Driven against a stateful req_plug standing in for the S3 store (the Local double
  has no steal-touch — this is an S3-only protocol path).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  @shard "s20"
  @old_owner "old@node"
  @error_body ~s(<?xml version="1.0"?><Error><Code>InternalError</Code><Message>boom</Message></Error>)
  @ok_copy_body ~s(<?xml version="1.0"?><CopyObjectResult><ETag>"db-2"</ETag></CopyObjectResult>)

  setup do
    prev = Application.get_env(:fathom, S3)

    # The store: a lock held by a DEAD owner (expired past the steal margin, no
    # heartbeat object), a data object at etag db-1, and a touch that fails once.
    old_lock = %{
      "owner" => @old_owner,
      "epoch" => 3,
      "expires_at_ms" => System.system_time(:millisecond) - 60_000
    }

    store =
      start_supervised!(
        {Agent,
         fn ->
           %{
             lock: %{body: Jason.encode!(old_lock), etag: ~s("lock-1")},
             seq: 1,
             touches: 0,
             db_etag: ~s("db-1")
           }
         end}
      )

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> serve(conn, store) end
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    %{store: store}
  end

  test "a failed steal-touch rolls the lock back so the retry redoes the full steal",
       %{store: store} do
    # First acquire: the steal write lands (epoch 4), then the touch fails → refused.
    assert {:error, {:transient_lookup, {:touch_failed, _}}} =
             S3.acquire_lease(@shard, "new@node", 30_000)

    # The failed steal's lock write must be rolled back to the dead owner's content —
    # pre-fix it stayed {new@node, 4}, and the next acquire RECLAIMED it unfenced.
    lock = Agent.get(store, & &1.lock)
    assert lock != nil, "the lock must not be deleted (that would skip the steal entirely)"
    assert %{"owner" => @old_owner, "epoch" => 3} = Jason.decode!(lock.body)

    # The retry re-enters the FULL steal path: epoch bump AND a (now successful)
    # touch — so the lease carries took_over and the takeover gets revalidated.
    assert {:ok, %{took_over: true, epoch: 4}} = S3.acquire_lease(@shard, "new@node", 30_000)

    assert Agent.get(store, & &1.touches) == 2,
           "the retry must redo the data-object touch, not skip it via reclaim"
  end

  # --- a minimal stateful S3 double (lock object + data object + heartbeats) ---

  defp serve(%Plug.Conn{method: method, request_path: path} = conn, store) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    case {method, path} do
      {"GET", "/b/" <> @shard <> ".lock"} ->
        case Agent.get(store, & &1.lock) do
          nil ->
            Plug.Conn.send_resp(conn, 404, "")

          %{body: b, etag: e} ->
            conn |> Plug.Conn.put_resp_header("etag", e) |> Plug.Conn.send_resp(200, b)
        end

      {"PUT", "/b/" <> @shard <> ".lock"} ->
        put_lock_resp(conn, store, body)

      {"HEAD", "/b/" <> @shard <> ".db"} ->
        e = Agent.get(store, & &1.db_etag)
        conn |> Plug.Conn.put_resp_header("etag", e) |> Plug.Conn.send_resp(200, "")

      {"PUT", "/b/" <> @shard <> ".db"} ->
        # The steal-time touch (server-side self-copy). Fails once, then succeeds.
        touches =
          Agent.get_and_update(store, fn s -> {s.touches + 1, %{s | touches: s.touches + 1}} end)

        if touches == 1 do
          Plug.Conn.send_resp(conn, 200, @error_body)
        else
          Agent.update(store, fn s -> %{s | db_etag: ~s("db-#{touches}")} end)
          Plug.Conn.send_resp(conn, 200, @ok_copy_body)
        end

      {"GET", "/b/heartbeats/" <> _} ->
        # The dead owner has no heartbeat: liveness falls back to the lock's own
        # (long-expired) TTL ⇒ :dead ⇒ stealable.
        Plug.Conn.send_resp(conn, 404, "")

      _ ->
        Plug.Conn.send_resp(conn, 500, "unexpected #{method} #{path}")
    end
  end

  defp put_lock_resp(conn, store, body) do
    if_none_match = Plug.Conn.get_req_header(conn, "if-none-match")
    if_match = Plug.Conn.get_req_header(conn, "if-match")

    Agent.get_and_update(store, fn s ->
      cond do
        if_none_match == ["*"] and s.lock != nil ->
          {412, s}

        if_none_match == ["*"] ->
          {200, %{s | lock: %{body: body, etag: ~s("lock-#{s.seq + 1}")}, seq: s.seq + 1}}

        if_match != [] and (s.lock == nil or hd(if_match) != s.lock.etag) ->
          {412, s}

        true ->
          {200, %{s | lock: %{body: body, etag: ~s("lock-#{s.seq + 1}")}, seq: s.seq + 1}}
      end
    end)
    |> then(&Plug.Conn.send_resp(conn, &1, ""))
  end
end
