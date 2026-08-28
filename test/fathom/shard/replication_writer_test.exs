defmodule Fathom.Shard.ReplicationWriterTest do
  @moduledoc """
  The per-link writer process — expert review 2026-08-26 #19.

  ## The defect

  `Shipper` is both the writer and the READER of its socket, and `:gen_tcp.send/2` blocks until the
  peer's receive window drains — up to `send_timeout`, 5 000 ms. So a send inside `handle_cast/2`
  left incoming acks **for every other shard on that link** sitting unread in the shipper's mailbox:
  a shard whose ack had already arrived could not complete its quorum until an unrelated shard's
  send unblocked, and `Budget.release/2` was delayed with it, so the node-wide byte budget stayed
  claimed for the stall.

  ## The fixture

  `Fathom.Test.PausablePeer` cannot express this. It holds the follower's REPLIES while still
  forwarding pushes upstream, which models a slow answer — a peer that reads and does not talk. What
  is needed here is the opposite: a peer that **stops reading**, so its receive window fills and the
  primary's send blocks. `black_hole!/1` in `replication_transport_test` does not do it either; it
  reads and discards, which never fills anything.

  So: a deaf peer. It accepts, sets `active: false` and never calls `recv`, and can still push
  frames DOWN the socket, which is what makes the ack observable while a send is stuck.

  ## Why it discriminates

  Against the unfixed shipper the send blocks inside `handle_cast/2`, so the `{:tcp, _, ack}` for
  the OTHER shard is never decoded and `assert_receive` times out. Probed: it fails at the
  `:blocked_ack` assertion.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Shipper

  # Big enough that a handful of them exceed any default socket buffer, small enough that filling
  # the window costs milliseconds rather than seconds.
  @payload_bytes 512 * 1024

  # A peer that ACCEPTS and never reads. `active: false` with no `recv` is what fills the window;
  # everything else here exists so the test can still send a reply DOWN the same socket.
  defp deaf_peer! do
    test = self()

    pid =
      spawn(fn ->
        # A TINY receive buffer, and it is what makes the stall reachable at all. The first draft
        # used the defaults and 12 MB of pushes: nothing blocked, the test passed against the
        # UNFIXED shipper, and it was measuring nothing. macOS auto-tunes the receive window well
        # past that. Pinning `recbuf`/`buffer` small closes the window after a few frames.
        {:ok, listen} =
          :gen_tcp.listen(0, [
            :binary,
            packet: 4,
            active: false,
            reuseaddr: true,
            nodelay: true,
            recbuf: 1024,
            buffer: 1024
          ])

        {:ok, port} = :inet.port(listen)
        send(test, {:deaf_port, self(), port})
        {:ok, sock} = :gen_tcp.accept(listen)
        send(test, {:deaf_accepted, self()})
        deaf_loop(sock, listen)
      end)

    assert_receive {:deaf_port, ^pid, port}, 2_000
    on_exit(fn -> Process.exit(pid, :kill) end)
    {pid, port}
  end

  # Deliberately no `recv`: reading is exactly what this peer must not do.
  defp deaf_loop(sock, listen) do
    receive do
      {:reply, frame} ->
        :ok = :gen_tcp.send(sock, frame)
        deaf_loop(sock, listen)

      :close ->
        :gen_tcp.close(sock)
        :gen_tcp.close(listen)
    end
  end

  defp push(shard_id, payload) do
    %Push{
      shard_id: shard_id,
      epoch: 1,
      wal_gen: 0,
      salt1: 7,
      offset: 0,
      payload: payload
    }
  end

  test "an ack for one shard is processed while another shard's send is blocked" do
    {peer, port} = deaf_peer!()

    name = :"writer_#{System.unique_integer([:positive])}"
    shipper = start_supervised!({Shipper, name: name, id: name, host: ~c"127.0.0.1", port: port})
    assert Shipper.connected?(shipper), "the shipper never connected to the deaf peer"
    assert_receive {:deaf_accepted, ^peer}, 2_000

    # Grab the socket while the shipper is still responsive. After the flood it is not — pre-fix
    # `:sys.get_state/1` on it would itself block, which is the defect.
    sock = :sys.get_state(shipper).sock

    # Register a waiter for shard "b" FIRST, while the window is still open, so the ack below has
    # somewhere to land. Its own frame is small.
    Shipper.push(shipper, push("b", "small"))

    # Now fill the peer's receive window. ONE SHARD ID PER PUSH, which the first draft got wrong:
    # the shipper holds a single waiter per shard, so 24 pushes for the same id are 1 send and 23
    # `:already_in_flight` rejects — half a megabyte on the wire, nowhere near a stall. Distinct
    # ids make all 24 real sends.
    payload = :binary.copy(<<0xAB>>, @payload_bytes)

    for i <- 1..24, do: Shipper.push(shipper, push("a#{i}", payload))

    # PRECONDITION, and it is the reason this test is worth anything. `send_pend` is bytes sitting
    # in the port's output queue because the peer will not take them — i.e. a send that is blocked
    # right now. Read from the TEST process off the socket's own stats, so it works whether the
    # blocked party is the shipper (unfixed) or the writer (fixed). Without this the first draft
    # asserted a happy path against a link that was never stalled.
    await_stall!(sock, 200)

    # Drain any rejects so they cannot be confused with the assertion below.
    flush_rejects()

    # THE ASSERTION. The peer answers for "b" while "a" is stuck. A shipper blocked inside
    # `:gen_tcp.send/2` cannot decode this, because it is the same process that reads the socket.
    send(peer, {:reply, IO.iodata_to_binary(Protocol.encode_ack("b", 5))})

    assert_receive {:repl_reply, ^name, {:ack, "b", 5}}, 1_000, ":blocked_ack"

    send(peer, :close)
  end

  # Bounded poll on an OS-level condition, which is the one thing `Process.monitor` cannot express.
  # Same shape as `flush_position_test`'s `settle/2`, and it FLUNKS rather than proceeding: a test
  # that reaches the assertion without a stall is measuring the happy path.
  defp await_stall!(_sock, 0) do
    flunk(
      "the socket never blocked, so nothing was head-of-line blocked and this test would pass " <>
        "against the unfixed shipper. Raise @payload_bytes or lower the peer's recbuf."
    )
  end

  defp await_stall!(sock, tries) do
    case :inet.getstat(sock, [:send_pend]) do
      {:ok, [send_pend: n]} when n > 0 ->
        :ok

      _ ->
        Process.sleep(5)
        await_stall!(sock, tries - 1)
    end
  end

  # A second, cheaper property from the same change, and the one that is invisible until it bites:
  # `send_timeout_close: true` closes the socket on a timed-out send, so the shipper receives BOTH
  # the writer's `{:send_failed, …}` and the socket's `{:tcp_closed, _}`. With a bare `_` on the
  # `tcp_closed` clause, `drop/2` ran twice and armed TWO reconnect timers — two sockets, one
  # leaked, and the leaked one the `active: true` reader for a link nobody drains.
  #
  # Structural rather than behavioural: reproducing it needs a send to time out AND a reconnect to
  # land in the same window, and the guard is a one-line change that looks equivalent without it.
  test "the tcp_closed handler matches the CURRENT socket, not any socket" do
    source = File.read!("lib/fathom/shard/replication/shipper.ex")

    assert source =~ "def handle_info({:tcp_closed, sock}, %{sock: sock} = state)",
           "the {:tcp_closed, _} handler stopped matching the current socket. With a bare " <>
             "wildcard, a timed-out send delivers both {:send_failed, _} and {:tcp_closed, _}, " <>
             "drop/2 runs twice, two reconnect timers fire, and one active: true reader socket " <>
             "is leaked per occurrence on a long-lived link."

    assert source =~ "def handle_info({:send_failed, writer, reason}, %{writer: writer} = state)",
           "the writer's failure report stopped being matched against the CURRENT writer. A " <>
             "straggler from a previous incarnation — one still blocked in send when its socket " <>
             "was closed — would then tear down the socket that replaced it."
  end

  defp flush_rejects do
    receive do
      {:repl_reply, _, {:reject, _, _, _}} -> flush_rejects()
    after
      0 -> :ok
    end
  end
end
