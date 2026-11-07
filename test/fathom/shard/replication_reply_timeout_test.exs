defmodule Fathom.Shard.ReplicationReplyTimeoutTest do
  @moduledoc """
  A shipper link that stops answering is dropped even when its socket stays healthy.

  Tier 0, 2026-11-06: followers reached through toxiproxy were still refusing pushes
  `:already_in_flight` 21 s after a partition healed. The proxy acknowledged every byte, so the
  primary's socket never errored and `TCP_USER_TIMEOUT` (which watches the kernel's own
  unacknowledged data) could not fire; the waiter for each shard sat until the proxy's upstream
  socket gave up on its retransmit backoff.

  The fake follower here is that proxy: it completes the handshake, reads every frame, and answers
  only the shards it is told to. The kernel acknowledges everything, exactly as through toxiproxy.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.{Push, SeedBegin}
  alias Fathom.Shard.Replication.Shipper

  @timeout 300

  setup do
    prev = Application.get_env(:fathom, :replication_reply_timeout_ms)
    Application.put_env(:fathom, :replication_reply_timeout_ms, @timeout)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_reply_timeout_ms),
        else: Application.put_env(:fathom, :replication_reply_timeout_ms, prev)
    end)

    :ok
  end

  # A follower that answers pushes for the shards in `answer` and swallows everything else.
  defp peer!(answer) do
    me = self()

    pid =
      spawn(fn ->
        {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
        {:ok, port} = :inet.port(l)
        send(me, {:peer_port, self(), port})
        accept(l, answer, me)
      end)

    assert_receive {:peer_port, ^pid, port}, 2_000
    on_exit(fn -> Process.exit(pid, :kill) end)
    port
  end

  defp accept(l, answer, me) do
    {:ok, sock} = :gen_tcp.accept(l)
    send(me, :peer_connected)
    :ok = Protocol.handshake_accept(sock, 5_000)
    serve(sock, answer)
    # The shipper redials after a drop; accept that connection too.
    accept(l, answer, me)
  end

  defp serve(sock, answer) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, frame} ->
        with {:ok, %Push{shard_id: s, offset: o, payload: p}} <- Protocol.decode(frame),
             true <- s in answer do
          :gen_tcp.send(sock, Protocol.encode_ack(s, o + byte_size(p)))
        end

        serve(sock, answer)

      {:error, _} ->
        :ok
    end
  end

  defp shipper!(port) do
    name = :"reply_timeout_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Shipper, name: name, host: ~c"127.0.0.1", port: port}, id: name)
    assert_receive :peer_connected, 2_000
    wait_connected(pid, 50)
    pid
  end

  defp wait_connected(pid, 0), do: flunk("shipper #{inspect(pid)} never connected")

  defp wait_connected(pid, n) do
    unless Shipper.connected?(pid) do
      receive after: (20 -> :ok)
      wait_connected(pid, n - 1)
    end
  end

  defp push(pid, shard),
    do:
      Shipper.push(pid, %Push{
        shard_id: shard,
        epoch: 1,
        wal_gen: 1,
        salt1: 0,
        offset: 0,
        payload: "x"
      })

  test "a push the follower never answers frees its shard once the link has been silent" do
    pid = shipper!(peer!([]))
    t0 = System.monotonic_time(:millisecond)
    push(pid, "stuck")

    # Before the watchdog this waited forever: no reply, no socket error, and every later push for
    # "stuck" refused :already_in_flight.
    assert_receive {:repl_reply, ^pid, {:reject, "stuck", :disconnected, 0}}, 3_000
    waited = System.monotonic_time(:millisecond) - t0
    assert waited >= @timeout, "the link was dropped before the timeout (#{waited} ms)"

    # The shipper redials: the shard can reach this follower again.
    assert_receive :peer_connected, 2_000
  end

  test "a link still answering other shards is not dropped for one slow shard" do
    pid = shipper!(peer!(["live"]))
    push(pid, "slow")

    # Keep the link busy with a shard the follower answers, for well past the timeout.
    for _ <- 1..8 do
      push(pid, "live")
      assert_receive {:repl_reply, ^pid, {:ack, "live", _}}, 1_000
      receive after: (div(@timeout, 3) -> :ok)
    end

    refute_received {:repl_reply, ^pid, {:reject, "slow", :disconnected, 0}},
                    "a follower that is answering other shards was treated as dead"
  end

  test "an outstanding seed alone does not trip it" do
    pid = shipper!(peer!([]))

    Shipper.seed_begin(pid, %SeedBegin{
      shard_id: "seeding",
      epoch: 1,
      wal_gen: 1,
      salt1: 0,
      wal_offset: 0,
      db_size: 0,
      wal_size: 0
    })

    refute_receive {:repl_reply, ^pid, _}, @timeout * 3
  end

  describe "reply_stalled?/4" do
    test "fires at the boundary, not before" do
      refute Shipper.reply_stalled?(1_299, 1_000, 1, 300)
      assert Shipper.reply_stalled?(1_300, 1_000, 1, 300)
    end

    test "never fires with nothing awaited, no progress mark, or the check disabled" do
      refute Shipper.reply_stalled?(10_000, 0, 0, 300)
      refute Shipper.reply_stalled?(10_000, nil, 1, 300)
      refute Shipper.reply_stalled?(10_000, 0, 1, 0)
    end
  end
end
