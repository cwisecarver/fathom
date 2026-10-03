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

  alias Fathom.Shard.Replication.Budget
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

  # EXPERT REVIEW 2026-10-01 #17. The shipper released each push's byte reservation when it handed
  # the frame to the writer, so on a stalled link every byte queued behind the blocked send was
  # invisible to `Budget`: `queued/0` read ~0, `:overloaded` never fired, and the memory the budget
  # exists to bound grew in the writer's mailbox. The invariant: a frame stays counted until its
  # send returns, and a dropped link's stranded frames stop counting.
  describe "the byte budget follows the frame into the writer (#17)" do
    test "frames stuck behind a blocked send are still counted" do
      {peer, port} = deaf_peer!()

      name = :"writer_budget_#{System.unique_integer([:positive])}"

      shipper =
        start_supervised!({Shipper, name: name, id: name, host: ~c"127.0.0.1", port: port})

      assert Shipper.connected?(shipper)
      assert_receive {:deaf_accepted, ^peer}, 2_000
      sock = :sys.get_state(shipper).sock

      payload = :binary.copy(<<0xCD>>, @payload_bytes)
      for i <- 1..24, do: Shipper.push(shipper, push("q#{i}", payload))

      await_stall!(sock, 200)
      # Every push has left the SHIPPER's mailbox: whatever is still counted is in the writer.
      _ = :sys.get_state(shipper)
      flush_rejects()

      # Unfixed: 0 — the shipper had already released every one of these.
      assert Budget.queued(name) >= 4 * @payload_bytes,
             "only #{Budget.queued(name)} bytes counted while the link is stalled with frames " <>
               "queued in the writer: the budget is released before the send"

      # The link drops. The frames queued in the dead writer will never be sent, so they must stop
      # counting — or a link that keeps timing out ratchets the node toward refusing everything.
      ref = Process.monitor(:sys.get_state(shipper).writer)
      send(peer, :close)
      assert_receive {:DOWN, ^ref, :process, _, _}, 10_000
      _ = :sys.get_state(shipper)

      assert Budget.queued(name) == 0,
             "#{Budget.queued(name)} bytes still charged to a dropped link's dead writer"
    end

    test "a frame that was sent is released" do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen)

      # Accepts and reads (and discards) everything, so every send completes.
      reader =
        spawn(fn ->
          {:ok, s} = :gen_tcp.accept(listen)
          :ok = Protocol.handshake_accept(s, Protocol.handshake_timeout_ms())
          :ok = :inet.setopts(s, active: true)
          drain_forever()
        end)

      on_exit(fn -> Process.exit(reader, :kill) end)

      name = :"writer_budget_ok_#{System.unique_integer([:positive])}"

      shipper =
        start_supervised!({Shipper, name: name, id: name, host: ~c"127.0.0.1", port: port})

      assert Shipper.connected?(shipper)

      payload = :binary.copy(<<0xEF>>, 4096)
      for i <- 1..10, do: Shipper.push(shipper, push("ok#{i}", payload))

      # Precondition: the pushes really reached the writer (a waiter each), not a reject.
      assert map_size(:sys.get_state(shipper).waiters) == 10

      assert await_budget_zero(name, 200) == 0,
             "sent frames were never released: the budget would ratchet up on a healthy link"
    end
  end

  # EXPERT REVIEW 2026-10-01 #6. `seed_chunk/5` was a cast, so a seeder `pread` the whole database
  # into the shipper's mailbox while the shipper sent one chunk at a time: a 2 GB tenant was 2 GB
  # resident on the primary, uncounted. The invariant: a seed chunk is not accepted until the
  # previous one is on the wire, so a stalled link stops the seeder reading instead of buffering.
  test "a seed chunk does not return while the link is stalled (#6)" do
    {peer, port} = deaf_peer!()

    name = :"writer_seed_#{System.unique_integer([:positive])}"
    shipper = start_supervised!({Shipper, name: name, id: name, host: ~c"127.0.0.1", port: port})
    assert Shipper.connected?(shipper)
    assert_receive {:deaf_accepted, ^peer}, 2_000
    sock = :sys.get_state(shipper).sock

    chunk = :binary.copy(<<0x5E>>, @payload_bytes)

    seeder =
      Task.async(fn ->
        for seq <- 0..23, do: Shipper.seed_chunk(shipper, "seedme", :db, seq, chunk)
      end)

    await_stall!(sock, 200)

    # Unfixed: the 24 casts return at once and the task finishes with the link still stalled.
    assert Task.yield(seeder, 300) == nil,
           "the seeder finished while the link was stalled: every chunk was buffered in the " <>
             "shipper instead of waiting for the wire"

    {:message_queue_len, queued} = Process.info(shipper, :message_queue_len)
    assert queued <= 1, "#{queued} seed chunks are queued in the shipper's mailbox"

    send(peer, :close)
    Task.shutdown(seeder, :brutal_kill)
  end

  defp drain_forever do
    receive do
      _ -> drain_forever()
    end
  end

  defp await_budget_zero(name, 0), do: Budget.queued(name)

  defp await_budget_zero(name, tries) do
    case Budget.queued(name) do
      0 ->
        0

      _ ->
        Process.sleep(5)
        await_budget_zero(name, tries - 1)
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
