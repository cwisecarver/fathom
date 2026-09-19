defmodule Fathom.Shard.Storage.S3StealTouchRollbackRetryTest do
  @moduledoc """
  Expert review 2026-09-18 #21: the DOUBLE failure. A steal writes the epoch+1 lock, the data-object
  touch fails (fail-closed), and then the rollback PUT that should restore the dead owner at epoch+2
  ALSO fails — a correlated storage brownout. Pre-fix `restore_lock/3` did one PUT and discarded the
  result (`_ = put_lock(...)`), so the lock was stranded at our own `{new@node, epoch+1}`. The
  IMMEDIATE outer checkout retry then read its OWN owner and took the same-owner RECLAIM path — no
  steal-touch, no `took_over` — skipping both the zombie data-etag fence and the takeover
  revalidation.

  The invariant: a rollback whose first PUT hits a transient error is RETRIED, so the lock still
  lands at the dead owner's identity at epoch+2 and the retry redoes the full fenced steal.

  Driven against a stateful req_plug S3 double (steal-touch is an S3-only protocol path — the Local
  double has none). Mirrors `s3_steal_touch_rollback_test.exs` but injects a transient failure on the
  first ROLLBACK put.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  @shard "s21"
  @old_owner "old@node"
  @new_owner "new@node"
  @error_body ~s(<?xml version="1.0"?><Error><Code>InternalError</Code><Message>boom</Message></Error>)

  setup do
    prev = Application.get_env(:fathom, S3)

    old_lock = %{
      "owner" => @old_owner,
      "epoch" => 3,
      "expires_at_ms" => System.system_time(:millisecond) - 60_000
    }

    store =
      start_supervised!({Agent,
       fn ->
         %{
           lock: %{body: Jason.encode!(old_lock), etag: ~s("lock-1")},
           seq: 1,
           db_etag: ~s("db-1"),
           # The touch always fails here (we exercise the rollback, not the retry-steal), and the
           # FIRST rollback PUT (owner == old_owner) fails transiently, then succeeds.
           rollback_puts: 0
         }
       end})

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

  test "a transient rollback-put failure is retried, so the lock is not stranded self-owned",
       %{store: store} do
    assert {:error, {:transient_lookup, {:touch_failed, _}}} =
             S3.acquire_lease(@shard, @new_owner, 30_000)

    lock = Agent.get(store, & &1.lock)
    assert lock != nil, "the lock must not be deleted"
    decoded = Jason.decode!(lock.body)

    # Pre-fix: the single rollback PUT failed and was discarded → lock stranded at {new@node, 4},
    # which the next checkout would reclaim UNFENCED. Post-fix: the rollback is retried, so it lands
    # at the dead owner at epoch+2 and the retry re-enters the full steal path.
    assert decoded["owner"] == @old_owner,
           "the failed steal's lock was left self-owned (#{decoded["owner"]}) — an unfenced reclaim"

    assert decoded["epoch"] == 5,
           "the rollback must keep the dead owner at epoch+2 (monotonic), got #{decoded["epoch"]}"

    assert Agent.get(store, & &1.rollback_puts) >= 2,
           "the transient first rollback PUT must have been retried"
  end

  # --- a minimal stateful S3 double ---

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
        # The steal-time touch always fails: we are exercising the rollback, not the retry-steal.
        Plug.Conn.send_resp(conn, 200, @error_body)

      {"GET", "/b/heartbeats/" <> _} ->
        # The dead owner has no heartbeat ⇒ liveness falls back to the expired lock TTL ⇒ stealable.
        Plug.Conn.send_resp(conn, 404, "")

      _ ->
        Plug.Conn.send_resp(conn, 500, "unexpected #{method} #{path}")
    end
  end

  defp put_lock_resp(conn, store, body) do
    if_none_match = Plug.Conn.get_req_header(conn, "if-none-match")
    if_match = Plug.Conn.get_req_header(conn, "if-match")
    owner = body |> Jason.decode!() |> Map.get("owner")

    status =
      Agent.get_and_update(store, fn s ->
        cond do
          # A create attempt (if-none-match: *) must 412 while a lock exists, so acquire falls to the
          # steal path rather than minting a fresh epoch-1 lock.
          if_none_match == ["*"] and s.lock != nil ->
            {412, s}

          # Inject ONE transient failure on the rollback PUT (owner == the dead owner): the write
          # the fix must retry. The lock is left unchanged, exactly as a real 5xx would.
          owner == @old_owner and s.rollback_puts == 0 ->
            {500, %{s | rollback_puts: 1}}

          if_match != [] and (s.lock == nil or hd(if_match) != s.lock.etag) ->
            {412, s}

          true ->
            bump = if owner == @old_owner, do: s.rollback_puts + 1, else: s.rollback_puts

            {200,
             %{
               s
               | lock: %{body: body, etag: ~s("lock-#{s.seq + 1}")},
                 seq: s.seq + 1,
                 rollback_puts: bump
             }}
        end
      end)

    Plug.Conn.send_resp(conn, status, "")
  end
end
