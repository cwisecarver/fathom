defmodule Fathom.Shard.RecoveryQueryPoolTest do
  @moduledoc """
  Survivor selection's position ask over pooled connections (expert review 2026-10-21 #9).

  Every rankable cold open used to dial a fresh connection to every peer: connect, the nonce
  handshake, one query, close. That is 3 round trips where 1 would do, one TIME_WAIT socket per ask,
  and in a failover wave one follower-cap slot per concurrent ask. `Recovery.QueryPool` keeps a few
  authenticated connections per peer. These tests pin what the pool must and must not do:

    * **Behaviour pins.** These fail against the fresh-dial code, which opens one connection per
      ask. Asks reuse one connection, and a burst of concurrent asks opens at most `pool_size`
      connections per peer, which is what keeps a wave inside the follower's connection cap.
    * **Safety guards.** These cover a peer restart, a late reply, a stray frame, a timeout and an
      authentication failure. The fresh-dial code passes them by construction, because it never
      reuses a socket. Here they guard the one thing a reused socket adds: an answer read on it must
      be the reply to THIS ask, and a dead or untrusted socket must cost a redial or a silent
      answer, never a wrong one.

  Each scripted peer runs the real `Protocol` handshake and frame sealing, with signing, required
  verification and the per-connection nonce all on (the prod defaults). It answers each query with
  `next_offset = conn * 1000 + query`, so every answer names the connection it came from.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{FrameAuth, Follower, Protocol, Recovery}
  alias Fathom.Shard.Replication.Recovery.QueryPool

  @secret "recovery-query-pool-test-secret-long-enough-to-be-real"
  @keys [
    :replication_sign_frames,
    :replication_hmac_required,
    :replication_hmac_secret,
    :replication_conn_nonce,
    :replication_recovery_pool_size,
    :replication_recovery_pool_idle_ms
  ]

  setup do
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      FrameAuth.forget_key()
    end)

    Application.put_env(:fathom, :replication_hmac_secret, @secret)
    Application.put_env(:fathom, :replication_sign_frames, true)
    Application.put_env(:fathom, :replication_hmac_required, true)
    Application.put_env(:fathom, :replication_conn_nonce, true)
    FrameAuth.forget_key()
    Recovery.init_table()
    :ok
  end

  defp start_pool(size) do
    Application.put_env(:fathom, :replication_recovery_pool_size, size)
    start_supervised!(QueryPool)
  end

  # -- a scripted peer -----------------------------------------------------------------------

  # `script.(shard_id, conn_no, query_no)` returns the actions to take for one query:
  # `:answer` (a definite position naming this conn and query), `{:sleep, ms}`, `{:raw, iodata}`
  # (sent as is), `:stray` (an extra answer nobody asked for), or `:close`.
  defp start_peer(script, port \\ 0) do
    test = self()

    acceptor =
      spawn(fn ->
        {:ok, lsock} =
          :gen_tcp.listen(port, [
            :binary,
            packet: 4,
            active: false,
            reuseaddr: true,
            # Room for a burst of overflow dials (rv6 F2): the default backlog of 5 resets them.
            backlog: 256,
            ip: {127, 0, 0, 1}
          ])

        {:ok, bound} = :inet.port(lsock)
        send(test, {:peer_port, self(), bound, lsock})
        accept_loop(lsock, script, test, 1)
      end)

    {port, lsock} =
      receive do
        {:peer_port, ^acceptor, p, l} -> {p, l}
      after
        5_000 -> flunk("scripted peer did not start")
      end

    on_exit(fn -> Process.exit(acceptor, :kill) end)
    %{acceptor: acceptor, lsock: lsock, port: port, endpoint: {"peer#{port}", "127.0.0.1", port}}
  end

  defp accept_loop(lsock, script, test, n) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        send(test, {:accepted, n})
        handler = spawn_link(fn -> receive(do: (:go -> serve(sock, script, test, n, 1))) end)
        _ = :gen_tcp.controlling_process(sock, handler)
        send(handler, :go)
        accept_loop(lsock, script, test, n + 1)

      {:error, _} ->
        :ok
    end
  end

  # Never crashes on a socket error: handlers are linked to the acceptor, and one dying would take
  # the listener (and every other connection) down with it.
  defp serve(sock, script, test, conn, q) do
    if Protocol.handshake_accept(sock, 5_000) == :ok,
      do: serve_loop(sock, script, test, conn, q),
      else: send(test, {:peer_saw_close, conn})
  end

  defp serve_loop(sock, script, test, conn, q) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, bytes} ->
        case Protocol.decode(bytes) do
          {:ok, {:position_query, shard}} ->
            send(test, {:query, conn, q, shard})

            if run(script.(shard, conn, q), sock, shard, conn, q, test) == :closed,
              do: :ok,
              else: serve_loop(sock, script, test, conn, q + 1)

          other ->
            send(test, {:peer_refused, conn, other})
            :gen_tcp.close(sock)
        end

      {:error, _} ->
        send(test, {:peer_saw_close, conn})
    end
  end

  defp run([], _sock, _shard, _conn, _q, _test), do: :ok

  defp run([action | rest], sock, shard, conn, q, test) do
    case action do
      :answer ->
        _ = :gen_tcp.send(sock, Protocol.encode_position(shard, pos(conn * 1000 + q)))

      :stray ->
        _ = :gen_tcp.send(sock, Protocol.encode_position(shard, pos(9_999)))
        send(test, {:stray_sent, conn})

      {:sleep, ms} ->
        Process.sleep(ms)

      {:raw, bytes} ->
        _ = :gen_tcp.send(sock, bytes)

      :close ->
        :gen_tcp.close(sock)
    end

    if action == :close, do: :closed, else: run(rest, sock, shard, conn, q, test)
  end

  defp pos(off), do: %{lineage: 7, wal_gen: 1, salt1: 11, next_offset: off}

  defp answered_at(off),
    do: {:ok, {:answered, %{epoch: 7, lineage: 7, wal_gen: 1, salt1: 11, next_offset: off}}}

  defp ask(endpoint, shard, timeout_ms \\ 2_000, connect_ms \\ 2_000),
    do: QueryPool.ask(endpoint, shard, timeout_ms, connect_ms, true)

  defp accepted_count do
    Stream.repeatedly(fn ->
      receive do
        {:accepted, n} -> n
      after
        0 -> nil
      end
    end)
    |> Enum.take_while(& &1)
    |> length()
  end

  defp start_follower do
    root = Path.join(System.tmp_dir!(), "qpool_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)
    name = :"qpool_f#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: root}, id: name)
    {:ok, port} = Follower.port(pid)
    {name, port}
  end

  # A rankable object stamp, so `best_replica/3` asks the peers.
  defp stamp, do: %{epoch: 9, wal_gen: 4, wal_ordinal: 5, offset: 0}

  # -- behaviour pins (fail against the fresh-dial code) --------------------------------------

  describe "behaviour pins through Recovery.best_replica/3" do
    test "repeated cold opens reuse one connection to the peer" do
      start_pool(4)
      {surv, _} = start_follower()
      peer = start_peer(fn _s, _c, _q -> [:answer] end)
      id = "qpool_reuse_#{System.unique_integer([:positive])}"

      for _ <- 1..5 do
        # N = 1, Q = 1: this node's own answer meets the floor and the peer is still asked.
        assert :none =
                 Recovery.best_replica(id, stamp(),
                   follower: surv,
                   peers: [peer.endpoint],
                   quorum: 1
                 )
      end

      for q <- 1..5, do: assert_received({:query, 1, ^q, ^id})
      # The fresh-dial code accepted 5 connections here, one per ask.
      assert accepted_count() == 1
    end

    # This test used to assert `accepted_count() <= 2`: a burst queued behind the pool's slots and
    # never dialed past them. That queue is what answered a healthy peer `:silent` (expert review
    # 2026-10-21 rv6 F2), so an ask that finds every slot busy now dials fresh, as the pre-pool code
    # did. What stays bounded is the POOLED connections, the long-lived ones that hold the
    # follower's connection cap between waves.
    test "a burst of concurrent opens is fully answered and keeps at most pool_size slots" do
      start_pool(2)
      {surv, _} = start_follower()
      peer = start_peer(fn _s, _c, _q -> [:answer] end)

      results =
        1..40
        |> Task.async_stream(
          fn i ->
            Recovery.best_replica("qpool_burst_#{i}", stamp(),
              follower: surv,
              peers: [peer.endpoint],
              quorum: 1
            )
          end,
          max_concurrency: 40,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, r} -> r end)

      assert Enum.all?(results, &(&1 == :none))
      assert :ets.info(QueryPool.Slots, :size) <= 2
    end
  end

  # -- safety guards ---------------------------------------------------------------------------

  describe "pooled ask" do
    test "answers come back in order on one socket" do
      start_pool(1)
      peer = start_peer(fn _s, _c, _q -> [:answer] end)

      assert answered_at(1001) == ask(peer.endpoint, "a")
      assert answered_at(1002) == ask(peer.endpoint, "b")
      assert answered_at(1003) == ask(peer.endpoint, "a")
      assert accepted_count() == 1
    end

    test "a real Follower verifies every query on a reused, nonce-bound connection" do
      start_pool(1)
      {_name, port} = start_follower()
      endpoint = {"real", "127.0.0.1", port}

      # The follower holds nothing for these shards: a definite "nothing", three times on one
      # connection. A follower that failed to verify the second frame would close it, and the
      # answer would come back silent.
      for i <- 1..3 do
        assert {:ok, {:answered, nil}} == ask(endpoint, "qpool_real_#{i}")
      end
    end

    test "a peer restart between asks costs a redial, not an answer" do
      start_pool(1)
      peer = start_peer(fn _s, _c, _q -> [:answer] end)
      assert answered_at(1001) == ask(peer.endpoint, "r")

      ref = Process.monitor(peer.acceptor)
      # The listen socket closes asynchronously after its owner dies, so the acceptor's DOWN alone
      # does not free the port: re-listening on it raced the close and failed with `eaddrinuse`
      # ("scripted peer did not start"). Wait for the port's own DOWN.
      lref = :erlang.monitor(:port, peer.lsock)
      Process.exit(peer.acceptor, :kill)
      assert_receive {:DOWN, ^ref, :process, _, :killed}
      assert_receive {:DOWN, ^lref, :port, _, _}

      # Same host and port, new process: the pooled socket is dead.
      restarted = start_peer(fn _s, _c, _q -> [:answer] end, peer.port)
      assert restarted.endpoint == peer.endpoint
      # The restarted peer numbers its connections from 1 again.
      assert answered_at(1001) == ask(peer.endpoint, "r")
    end

    test "a reused socket that dies mid-ask is redialed once inside the deadline" do
      start_pool(1)
      # Conn 1 answers its first query, then takes the second and hangs up without replying.
      peer =
        start_peer(fn
          _s, 1, 2 -> [:close]
          _s, _c, _q -> [:answer]
        end)

      assert answered_at(1001) == ask(peer.endpoint, "d")
      assert answered_at(2001) == ask(peer.endpoint, "d")
      assert accepted_count() == 2
    end

    test "a FRESH socket that dies is silent: no retry loop against a peer that refuses us" do
      start_pool(1)
      peer = start_peer(fn _s, _c, _q -> [:close] end)

      assert {:ok, :silent} == ask(peer.endpoint, "x")
      assert accepted_count() == 1
      assert {:ok, :silent} == ask(peer.endpoint, "x")
      assert accepted_count() == 1
    end

    test "a late reply is never read as the next ask's answer" do
      start_pool(1)

      # Conn 1's first reply comes 300 ms late, after the asker gave up.
      peer =
        start_peer(fn
          _s, 1, 1 -> [{:sleep, 300}, :answer]
          _s, _c, _q -> [:answer]
        end)

      {elapsed, first} = :timer.tc(fn -> ask(peer.endpoint, "late", 100, 100) end, :millisecond)
      assert {:ok, :silent} == first
      assert elapsed < 300

      # Same shard, same slot. Reading conn 1's late reply here would return 1001.
      assert answered_at(2001) == ask(peer.endpoint, "late")
    end

    test "a stray frame on an idle pooled socket discards it" do
      start_pool(1)

      peer =
        start_peer(fn
          _s, 1, 1 -> [:answer, :stray]
          _s, _c, _q -> [:answer]
        end)

      assert answered_at(1001) == ask(peer.endpoint, "s")
      assert_receive {:stray_sent, 1}
      # Reading the stray here would return 9999. Nothing a follower sends is unsolicited, so the
      # socket is not trusted again.
      assert answered_at(2001) == ask(peer.endpoint, "s")
    end

    test "a peer that never answers is silent within the caller's budget, then redialed" do
      start_pool(1)

      peer =
        start_peer(fn
          _s, 1, _q -> []
          _s, _c, _q -> [:answer]
        end)

      {elapsed, first} = :timer.tc(fn -> ask(peer.endpoint, "t", 150, 100) end, :millisecond)
      assert {:ok, :silent} == first
      assert elapsed < 1_000
      assert answered_at(2001) == ask(peer.endpoint, "t")
    end

    test "a reply that fails verification is silent and closes the socket" do
      start_pool(1)
      shard = "auth"
      # An UNSIGNED "I hold nothing" while signatures are required: decode refuses it.
      unsigned =
        <<2::8, 10::8, 0::8, byte_size(shard)::16, 0::64, 0::64, 0::64, 0::64>> <> shard

      peer =
        start_peer(fn
          _s, 1, _q -> [{:raw, unsigned}]
          _s, _c, _q -> [:answer]
        end)

      assert {:ok, :silent} == ask(peer.endpoint, shard)
      assert answered_at(2001) == ask(peer.endpoint, shard)
      assert accepted_count() == 2
    end

    test "an idle pooled socket is closed, so a quiet fleet holds no query connections" do
      Application.put_env(:fathom, :replication_recovery_pool_idle_ms, 50)
      start_pool(1)
      peer = start_peer(fn _s, _c, _q -> [:answer] end)

      assert answered_at(1001) == ask(peer.endpoint, "i")
      assert_receive {:peer_saw_close, 1}, 2_000
    end

    # HEAD-OF-LINE BLOCKING (expert review 2026-10-21 rv6 F2). Asks used to queue behind a busy
    # slot (`phash2(shard, pool_size)`, one ask at a time), and an ask that waited past its budget
    # answered `:silent` without touching the network. With a 100 ms peer, 30 concurrent asks on 2
    # slots need 1.5 s of serial service against a 500 ms budget, so most of them came back silent:
    # a healthy peer read as gone, which under quorum can refuse an open. A busy slot must never
    # hold an ask: the ask takes an idle slot or is handed back `:unavailable`, and the caller
    # dials fresh exactly as the pre-pool code did.
    test "concurrent asks beyond pool_size never wait behind a busy slot" do
      start_pool(2)
      peer = start_peer(fn _s, _c, _q -> [{:sleep, 100}, :answer] end)
      {_k, host, port} = peer.endpoint

      answers =
        1..30
        |> Task.async_stream(
          fn i ->
            shard = "hol_#{i}"

            case ask(peer.endpoint, shard, 250, 250) do
              {:ok, answer} ->
                answer

              :unavailable ->
                {:ok, sock} = Recovery.connect_peer(host, port)
                {answer, _} = Recovery.exchange(sock, shard, 250)
                :gen_tcp.close(sock)
                answer
            end
          end,
          max_concurrency: 30,
          timeout: 5_000
        )
        |> Enum.map(fn {:ok, a} -> a end)

      assert Enum.count(answers, &(&1 == :silent)) == 0
    end

    # A peer partitioned after the handshake (expert review 2026-10-21 rv6 F8) first shows up as a
    # recv timeout on a reused socket, which marks nothing. The dial that follows must mark it even
    # when it lost a millisecond of its connect bound: the old rule was `bound == connect_ms`, so
    # 1999 of 2000 ms read as "cut short" and the peer was never skipped. A dial clipped well short
    # (half the bound) still proves nothing and must not mark.
    test "a dial timeout marks the peer when it ran about its full connect bound" do
      alias Fathom.Shard.Replication.Recovery.QueryPool.Slot

      assert Slot.full_dial?(2_000, 2_000)
      assert Slot.full_dial?(1_999, 2_000)
      assert Slot.full_dial?(1_800, 2_000)
      refute Slot.full_dial?(1_799, 2_000)
      refute Slot.full_dial?(1_000, 2_000)
      refute Slot.full_dial?(0, 2_000)
    end

    test "with the pool off, asks dial fresh exactly as before" do
      start_pool(0)
      peer = start_peer(fn _s, _c, _q -> [:answer] end)
      assert :unavailable == ask(peer.endpoint, "off")

      {surv, _} = start_follower()

      for _ <- 1..3 do
        assert :none =
                 Recovery.best_replica("off", stamp(),
                   follower: surv,
                   peers: [peer.endpoint],
                   quorum: 1
                 )
      end

      assert accepted_count() == 3
    end
  end
end
