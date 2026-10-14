defmodule Fathom.Shard.ReplicationShipperConnectTest do
  @moduledoc """
  A Shipper dials OFF its GenServer loop (expert review 2026-10-13 #13).

  `connect/1` used to run inline: up to 5 s in `:gen_tcp.connect` plus 5 s in the handshake. A peer
  that accepts and then says nothing (the handshake half of a black hole; a dropped SYN blocks the
  first half the same way) pinned the Shipper, so `push/2` casts queued instead of taking the
  immediate `sock: nil` reject, and `connected?/1` timed out, leaving the Session to wait that
  follower out to the commit deadline instead of subtracting it from the quorum.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Shipper

  setup do
    prev = Application.get_env(:fathom, :replication_conn_nonce)
    # The nonce handshake is what makes a silent peer block the dialer: it waits for the hello.
    Application.put_env(:fathom, :replication_conn_nonce, true)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_conn_nonce),
        else: Application.put_env(:fathom, :replication_conn_nonce, prev)
    end)

    me = self()

    listener =
      spawn(fn ->
        {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
        {:ok, port} = :inet.port(listen)
        send(me, {:silent_port, self(), port})
        {:ok, _sock} = :gen_tcp.accept(listen)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:silent_port, ^listener, port}, 2_000
    on_exit(fn -> Process.exit(listener, :kill) end)
    %{port: port}
  end

  test "a peer that never completes the handshake does not pin the Shipper", %{port: port} do
    ship = :"shipper_connect_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Shipper, name: ship, host: ~c"127.0.0.1", port: port}, id: ship)

    t0 = System.monotonic_time(:millisecond)
    refute Shipper.connected?(pid)

    assert System.monotonic_time(:millisecond) - t0 < 1_500,
           "connected?/1 blocked behind the inline connect"

    t1 = System.monotonic_time(:millisecond)

    Shipper.push(pid, %Push{
      shard_id: "acme",
      epoch: 1,
      wal_gen: 1,
      salt1: 0,
      offset: 0,
      payload: "x"
    })

    assert_receive {:repl_reply, ^pid, {:reject, "acme", :disconnected, 0}}, 1_500

    assert System.monotonic_time(:millisecond) - t1 < 1_000,
           "a push to an unreachable follower queued instead of rejecting at once"
  end
end
