defmodule Fathom.Shard.Storage.S3CorruptLockTest do
  @moduledoc """
  Expert review 2026-10-15 #23 (D14): an UNDECODABLE S3 lock fails closed with its own loud error,
  never `{:transient_lookup, _}` (which reads as "retry shortly" and which the migrator retries
  forever), and is never auto-repaired. `Local` must answer the same input the same way
  (`local_lock_integrity_test.exs`); the operator remedy is `mix fathom.lock.repair`
  (`test/mix/tasks/fathom_lock_repair_test.exs`). Driven against a stateful `:req_plug` S3 double.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Mix.Tasks.Fathom.Lock.Repair

  @shard "s23corrupt"
  @garbage ~s({"owner": "node_x", "epoch": 7, "expires_at_)

  setup do
    prev = Application.get_env(:fathom, S3)
    prev_backend = Application.get_env(:fathom, :shard_storage)

    store =
      start_supervised!({Agent, fn -> %{lock: %{body: @garbage, etag: ~s("c1")}, puts: 0} end})

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> serve(conn, store) end
    )

    Application.put_env(:fathom, :shard_storage, S3)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)

      if prev_backend,
        do: Application.put_env(:fathom, :shard_storage, prev_backend),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    %{store: store}
  end

  test "acquire returns :corrupt_lock (not :transient_lookup), logs loudly, writes nothing",
       %{store: store} do
    :telemetry_test.attach_event_handlers(self(), [[:fathom, :storage, :lock_corrupt]])

    log =
      capture_log(fn ->
        assert {:error, :corrupt_lock} = S3.acquire_lease(@shard, "node_a", 30_000)
      end)

    assert log =~ "mix fathom.lock.repair #{@shard}"
    assert_received {[:fathom, :storage, :lock_corrupt], _ref, %{count: 1}, %{shard_id: @shard}}
    assert Agent.get(store, & &1.lock.body) == @garbage
    assert Agent.get(store, & &1.puts) == 0, "a corrupt lock must not be overwritten by acquire"
  end

  test "repair writes a marker at a strictly higher epoch, and acquire gets past the refusal",
       %{store: store} do
    assert {:ok, marker} = Repair.repair(@shard)

    # Salvaged epoch 7 from the damaged bytes; the marker goes to 7 + 2, never back.
    assert marker.epoch == 9
    assert marker.owner == Storage.released_owner()
    assert {:ok, %{epoch: 9}} = Storage.decode_lease(Agent.get(store, & &1.lock.body))

    # The marker is an ordinary expired foreign lock, so the next acquire takes the steal path.
    # (The steal-touch needs the data object, which this double does not serve; only the passage
    # beyond the corrupt-lock refusal is asserted.)
    refute match?({:error, :corrupt_lock}, S3.acquire_lease(@shard, "node_a", 30_000))
  end

  test "repair refuses a decodable lock and leaves it alone", %{store: store} do
    good = Jason.encode!(%{owner: "node_a", epoch: 3, expires_at_ms: 1})
    Agent.update(store, &put_in(&1.lock.body, good))

    assert {:error, :lock_not_corrupt} = Repair.repair(@shard)
    assert Agent.get(store, & &1.lock.body) == good
    assert Agent.get(store, & &1.puts) == 0
  end

  test "repair is If-Match the corrupt etag: a concurrent fix wins", %{store: store} do
    swapped = Jason.encode!(%{owner: "n", epoch: 50, expires_at_ms: 1})
    Agent.update(store, &Map.put(&1, :swap_on_put, swapped))

    assert {:error, _} = Repair.repair(@shard)
    assert {:ok, %{epoch: 50}} = Storage.decode_lease(Agent.get(store, & &1.lock.body))
  end

  test "repair with --epoch-floor goes above the floor when nothing is salvageable",
       %{store: store} do
    Agent.update(store, &put_in(&1.lock.body, <<0, 1, 2>>))
    assert {:ok, %{epoch: 1002}} = Repair.repair(@shard, epoch_floor: 1000)
  end

  defp serve(%Plug.Conn{method: method, request_path: path} = conn, store) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    case {method, path} do
      {"GET", "/b/" <> @shard <> ".lock"} ->
        %{body: b, etag: e} = Agent.get(store, & &1.lock)
        conn |> Plug.Conn.put_resp_header("etag", e) |> Plug.Conn.send_resp(200, b)

      {"PUT", "/b/" <> @shard <> ".lock"} ->
        put_lock(conn, store, body)

      {"GET", "/b/heartbeats/" <> _} ->
        Plug.Conn.send_resp(conn, 404, "")

      _ ->
        Plug.Conn.send_resp(conn, 500, "unexpected #{method} #{path}")
    end
  end

  defp put_lock(conn, store, body) do
    if_none_match = Plug.Conn.get_req_header(conn, "if-none-match")
    if_match = Plug.Conn.get_req_header(conn, "if-match")

    status =
      Agent.get_and_update(store, fn s ->
        cond do
          if_none_match == ["*"] ->
            {412, s}

          # Simulate a concurrent writer replacing the lock before our conditional PUT lands.
          s[:swap_on_put] ->
            {412,
             s
             |> put_in([:lock], %{body: s.swap_on_put, etag: ~s("c2")})
             |> Map.delete(:swap_on_put)}

          if_match == [s.lock.etag] ->
            {200, %{s | lock: %{body: body, etag: ~s("c#{s.puts + 10}")}, puts: s.puts + 1}}

          true ->
            {412, s}
        end
      end)

    Plug.Conn.send_resp(conn, status, "")
  end
end
