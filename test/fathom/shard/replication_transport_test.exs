defmodule Fathom.Shard.ReplicationTransportTest do
  @moduledoc """
  The A2 transport end to end — real sockets, real files, four followers.
  See `docs/a2-quorum-replication.md`.

  `replication_test.exs` covers the pure decisions. This covers the shells around them: that a
  push actually crosses a socket, lands in a follower's `-wal` at the right offset, comes back as
  an ack, and that `ship_quorum/4` stops at Q rather than at N.

  Each follower gets its own `:replication_dir`, which is what a real deployment has (one
  directory per node). Sharing one would let a bug that writes to the wrong path pass.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Shipper

  @payload :binary.copy(<<0xCD>>, 512)

  setup do
    prev_dir = Application.get_env(:fathom, :replication_dir)
    root = Path.join(System.tmp_dir!(), "repl_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    Application.put_env(:fathom, :replication_dir, root)

    on_exit(fn ->
      if prev_dir,
        do: Application.put_env(:fathom, :replication_dir, prev_dir),
        else: Application.delete_env(:fathom, :replication_dir)

      File.rm_rf(root)
    end)

    %{root: root}
  end

  # One follower listener, with its OWN directory — four followers sharing one would all write the
  # same file per shard, and the test would pass while proving nothing about independent replicas.
  # `start_supervised!` so each is torn down between tests; a leaked listener keeps its ETS table.
  defp start_follower!(name) do
    dir = Path.join(Application.get_env(:fathom, :replication_dir), to_string(name))
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    {:ok, port} = Follower.port(pid)
    {pid, port}
  end

  defp start_follower_named!(name) do
    {_pid, port} = start_follower!(name)
    port
  end

  defp start_shipper!(name, port) do
    pid = start_supervised!({Shipper, name: name, host: ~c"127.0.0.1", port: port}, id: name)
    # The connect happens in handle_continue; make sure it landed before anything is shipped, or
    # the first push races it and gets a :disconnected reject that looks like a transport bug.
    assert Shipper.connected?(pid), "shipper never connected to 127.0.0.1:#{port}"
    pid
  end

  # Every follower at the same position — the steady-state case. `ship_quorum/3` takes a
  # per-follower push so a laggard can be sent a different (larger) delta; this helper is the
  # common case where they all get the same one.
  defp fanout(shippers, push), do: for(s <- shippers, do: {s, push})

  # THE STRAGGLER CLASS. `:already_in_flight` was the TOP reject on the 2026-08-17 rig runs —
  # 8,781 and 18,226 on two nodes at 512 tenants — and NOTHING covered it. It is also
  # pre-existing rather than a side effect of the catch-up loop: the same runs with the per-push
  # cap disabled produced 21,442 and 9,175, so the loop is not the cause.
  # See `docs/reviews/a2-feedback-loop-fixed-2026-08-17.md`.
  #
  # The mechanism this pins: `ship_quorum/4` deliberately returns at the Q-th ack — that early
  # return is A2's entire measured value (2-of-4 at 1.6 ms against 4-of-4 at 134 ms). But the
  # straggler's push is STILL OUTSTANDING in its shipper, which holds exactly one waiter per
  # shard, so the very next commit for that shard is refused before it reaches the socket.
  #
  # `Shipper`'s moduledoc justifies that refusal with "a shard has exactly one writer, so there is
  # never more than one push in flight for it... a second push is a bug in the caller." That
  # premise is FALSE in the presence of an early quorum return: the session is correctly
  # serialized per shard and still produces a second push while the first is unanswered. The
  # refusal is not catching a caller bug, it is reporting a straggler.
  describe "a straggler left behind by the early quorum return" do
    test "is refused :already_in_flight on the next commit, before its push reaches the socket" do
      # 2 followers that ack + 1 black hole that never will. q=2, so the quorum is satisfied by
      # the two live ones and the black hole is left mid-flight — the exact production shape.
      {shippers, _holes} = mixed_fleet!("aif", 2, 1)
      [live1, live2, straggler] = shippers

      push1 = %Push{
        shard_id: "acme",
        epoch: 1,
        wal_gen: 1,
        salt1: 1,
        offset: 0,
        payload: @payload
      }

      assert {:ok, acked, []} = Replication.ship_quorum(fanout(shippers, push1), 2, 2_000)

      assert length(acked) == 2, "the quorum should have been carried by the two live followers"
      refute straggler in acked, "the black hole cannot have acked — the fixture is wrong"

      # The straggler received the bytes; it simply has not answered. Assert that rather than
      # assume it, or a fixture that never delivered would produce the same reject for the wrong
      # reason and this test would prove nothing.
      assert_receive {:black_hole_push, _}, 2_000

      # The next commit for the SAME shard, exactly as a hammering tenant produces it.
      push2 = %Push{push1 | offset: byte_size(@payload)}

      assert {:ok, _acked2, rejects} =
               Replication.ship_quorum(fanout(shippers, push2), 2, 2_000)

      # `start_shipper!` hands back a pid, and `ship_quorum/4` keys replies by pid too.
      assert {^straggler, :already_in_flight, 0} =
               Enum.find(rejects, fn {from, _r, _at} -> from == straggler end),
             "expected the straggler to be refused :already_in_flight, got #{inspect(rejects)}"

      # AND THE COST, which is the reason this matters rather than being cosmetic: the refusal is
      # a `Quorum.reject`, so it counts against the commit. Here q=2 of n=3 survives on the two
      # live followers — but one more reject from any real cause (`:disconnected`,
      # `:stale_wal_gen`, both present in volume on the same rig runs) makes it `:impossible`.
      assert length(rejects) == 1
      assert live1 != nil and live2 != nil
    end

    test "the straggler's push never reached the socket, so the work building it was wasted" do
      # The refusal happens on DEQUEUE in the shipper, after the session has already read the
      # delta off disk, encoded it and queued it. So a refused push is not free: it is a
      # `Wal.read_delta` plus an encode plus a mailbox slot, spent to be thrown away — and under
      # the 2026-08-17 numbers that happened ~18k times in one run on one node.
      {shippers, _holes} = mixed_fleet!("aif2", 2, 1)
      [_l1, _l2, straggler] = shippers

      p = %Push{shard_id: "acme", epoch: 1, wal_gen: 1, salt1: 1, offset: 0, payload: @payload}

      assert {:ok, _, []} = Replication.ship_quorum(fanout(shippers, p), 2, 2_000)
      assert_receive {:black_hole_push, _}, 2_000

      # Second push to the straggler alone: it must be refused without another frame crossing the
      # wire. If the black hole reports a push here, the guard let it through and the mechanism is
      # not what this test claims.
      Shipper.push(straggler, %Push{p | offset: byte_size(@payload)})
      assert_receive {:repl_reply, _, {:reject, "acme", :already_in_flight, 0}}, 2_000
      refute_receive {:black_hole_push, _}, 300
    end
  end

  # THE FIXTURE THIS CLASS ALWAYS LACKED — see `Fathom.Test.PausablePeer`.
  #
  # `black_hole!/1` above models a peer that is GONE. A straggler is a peer that is LATE, and until
  # now nothing could express one: `Follower` answers from an unlinked `Task`, so it cannot be
  # suspended. That gap is why `replication_seed_test.exs:553` ("a quiet shard catches a laggard up
  # without waiting on it") has been an unreproducible CI flake since 2026-08-14 — the diagnosis
  # needed a follower that receives a push and withholds its reply.
  # These were :flaky-tagged for one commit while THREE real bugs in `PausablePeer` were found and
  # fixed: it accepted exactly ONE connection, so any shipper reconnect completed the handshake
  # against the listen backlog and then sat unaccepted with every later frame vanishing; frames
  # arriving before the `{:accepted, _}` notification matched no clause and were dropped
  # (`controlling_process/2` then `send/2` does not order that notification against
  # kernel-delivered `{:tcp, _, _}`); and the seed scenario's setup did not allow for the extra hop.
  #
  # Kept as a comment rather than deleted because each of those three presented as the PRODUCT
  # failing — a proxied follower that simply never converged — which is indistinguishable from the
  # bug this fixture exists to study. If these flake again, suspect the fixture's socket handling
  # first: the same scenarios against a direct `Follower` are stable.
  describe "a LATE follower (pausable peer)" do
    test "holds its reply, so the next push for that shard is refused before it reaches the wire" do
      port = start_follower_named!(:pp_follower)
      peer = start_supervised!({Fathom.Test.PausablePeer, upstream_port: port, notify: self()})
      Follower.seed(:pp_follower, "acme", 1, 1, 0, 0)

      ship = start_shipper!(:pp_ship, Fathom.Test.PausablePeer.port(peer))

      p1 = %Push{shard_id: "acme", epoch: 1, wal_gen: 1, salt1: 1, offset: 0, payload: @payload}

      # PAUSED: the follower still RECEIVES and applies the bytes — only the answer is withheld.
      # That is the whole difference from a black hole, and it is what makes this a straggler.
      :ok = Fathom.Test.PausablePeer.pause(peer)
      Shipper.push(ship, p1)

      assert_receive {:peer_frame, :to_follower, _}, 2_000
      assert_receive {:peer_frame, :to_primary, _}, 2_000, "the follower never answered at all"
      assert Fathom.Test.PausablePeer.held(peer) == 1, "the reply should be held, not delivered"

      # The primary has had no answer, so the shipper still holds this shard's single waiter.
      Shipper.push(ship, %Push{p1 | offset: byte_size(@payload)})
      assert_receive {:repl_reply, _, {:reject, "acme", :already_in_flight, 0}}, 2_000

      # And it was refused BEFORE the socket — nothing new crossed to the follower.
      refute_receive {:peer_frame, :to_follower, _}, 300

      # Releasing settles the first push normally, proving the peer is a real follower throughout
      # and that the fixture is not simply eating traffic.
      assert {:ok, 1} = Fathom.Test.PausablePeer.release(peer)
      assert_receive {:repl_reply, _, {:ack, "acme", next}}, 2_000
      assert next == byte_size(@payload)
    end

    test "once released, the shard accepts pushes again — the fixture is not a one-way trapdoor" do
      port = start_follower_named!(:pp2_follower)
      peer = start_supervised!({Fathom.Test.PausablePeer, upstream_port: port, notify: self()})
      Follower.seed(:pp2_follower, "acme", 1, 1, 0, 0)
      ship = start_shipper!(:pp2_ship, Fathom.Test.PausablePeer.port(peer))

      p = %Push{shard_id: "acme", epoch: 1, wal_gen: 1, salt1: 1, offset: 0, payload: @payload}

      :ok = Fathom.Test.PausablePeer.pause(peer)
      Shipper.push(ship, p)
      assert_receive {:peer_frame, :to_primary, _}, 2_000
      assert {:ok, 1} = Fathom.Test.PausablePeer.release(peer)
      assert_receive {:repl_reply, _, {:ack, "acme", _}}, 2_000

      # A second, contiguous push now behaves exactly as against a direct follower.
      Shipper.push(ship, %Push{p | offset: byte_size(@payload)})
      assert_receive {:repl_reply, _, {:ack, "acme", next}}, 2_000
      assert next == byte_size(@payload) * 2
    end
  end

  # A peer that accepts a connection, answers NOTHING ever, and whose death is a real socket death.
  #
  # THIS IS NOT A CONVENIENCE, IT IS THE ONLY WAY TO REACH THE PATH. A node dying has to reach
  # `Shipper.drop/2` through `tcp_closed`, and a real `Follower` cannot be made to do that from a
  # test: it hands every accepted socket to an UNLINKED `Task` (follower.ex:235), so killing or
  # stopping the `Follower` GenServer leaves the connection alive and serving. It then answers
  # `:internal` from the rescue at follower.ex:677 — a courtesy reply, the opposite of what a
  # SIGKILLed container does. Measured while writing these: `stop_supervised!` on a follower
  # produces `internal: 0` rejects and never a `:disconnected` one, so a test built that way would
  # exercise graceful shutdown while claiming to test node loss. (That path is real too, and has
  # its own test below — it is just a different path.)
  #
  # It also reports each push it receives to `test_pid`, so a kill can be ordered against the send
  # with no `Process.sleep`.
  defp black_hole!(test_pid) do
    me = self()

    pid =
      spawn(fn ->
        {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: true, reuseaddr: true])
        {:ok, port} = :inet.port(listen)
        send(me, {:black_hole_port, self(), port})
        {:ok, sock} = :gen_tcp.accept(listen)
        black_hole_loop(sock, listen, test_pid)
      end)

    assert_receive {:black_hole_port, ^pid, port}, 2_000
    on_exit(fn -> Process.exit(pid, :kill) end)
    {port, pid}
  end

  defp black_hole_loop(sock, listen, test_pid) do
    receive do
      {:tcp, ^sock, _bytes} ->
        send(test_pid, {:black_hole_push, self()})
        black_hole_loop(sock, listen, test_pid)

      :die ->
        # A dead node closes its LISTEN port as well as the connection. Without the second close
        # the shipper's 500 ms reconnect would find the listener still up, connect, and sit on a
        # socket nothing ever accepts — so the reject the quorum is waiting for would never come
        # and an `:impossible` test would fail as a timeout for a reason that is pure fixture.
        :gen_tcp.close(sock)
        :gen_tcp.close(listen)
    end
  end

  # `live` followers that really ack, plus `dead` black holes that never will — each with its own
  # connected shipper, which is the state a primary is in the instant before some of its peers
  # die. Returns `{all_shippers, black_hole_pids}`.
  defp mixed_fleet!(tag, live, dead) do
    acking =
      for i <- 1..live do
        name = :"#{tag}f#{i}"
        {_pid, port} = start_follower!(name)
        Follower.seed(name, "acme", 1, 1, 0, 0)
        start_shipper!(:"#{tag}s#{i}", port)
      end

    holes =
      for i <- 1..dead do
        {port, pid} = black_hole!(self())
        {pid, start_shipper!(:"#{tag}h#{i}", port)}
      end

    {acking ++ for({_pid, ship} <- holes, do: ship), for({pid, _ship} <- holes, do: pid)}
  end

  # All of them, with nothing shipped in between — "simultaneous" is the property under test, and
  # a loop with work between the kills is the staggered case `chaos.sh soak` already covers.
  #
  # Waits for the DOWN rather than sleeping, so the sockets are provably closed before anything is
  # shipped.
  defp kill_peers!(pids) do
    refs = for pid <- pids, do: {pid, Process.monitor(pid)}
    for pid <- pids, do: send(pid, :die)
    for {pid, ref} <- refs, do: assert_receive({:DOWN, ^ref, :process, ^pid, _}, 2_000)
    :ok
  end

  defp push(shard, opts \\ []) do
    %Push{
      salt1: 0,
      shard_id: shard,
      epoch: Keyword.get(opts, :epoch, 1),
      wal_gen: Keyword.get(opts, :wal_gen, 1),
      offset: Keyword.get(opts, :offset, 0),
      payload: Keyword.get(opts, :payload, @payload)
    }
  end

  describe "one follower, one shipper" do
    test "a push crosses the wire, lands in the WAL, and is acked" do
      port = start_follower_named!(:f_solo)
      ship = start_shipper!(:s_solo, port)
      Follower.seed(:f_solo, "acme", 1, 1, 0, 0)

      Shipper.push(ship, push("acme"))

      assert_receive {:repl_reply, ^ship, {:ack, "acme", 512}}, 2_000
      assert File.read!(Follower.wal_path(:f_solo, "acme")) == @payload
      assert Follower.state_of(:f_solo, "acme").next_offset == 512
    end

    test "successive deltas append in order" do
      port = start_follower_named!(:f_seq)
      ship = start_shipper!(:s_seq, port)
      Follower.seed(:f_seq, "acme", 1, 1, 0, 0)

      Shipper.push(ship, push("acme", offset: 0, payload: "aaa"))
      assert_receive {:repl_reply, ^ship, {:ack, "acme", 3}}, 2_000

      Shipper.push(ship, push("acme", offset: 3, payload: "bbb"))
      assert_receive {:repl_reply, ^ship, {:ack, "acme", 6}}, 2_000

      assert File.read!(Follower.wal_path(:f_seq, "acme")) == "aaabbb"
    end

    test "a gap is refused with the follower's real offset, and nothing is written" do
      port = start_follower_named!(:f_gap)
      ship = start_shipper!(:s_gap, port)
      Follower.seed(:f_gap, "acme", 1, 1, 0, 0)

      Shipper.push(ship, push("acme", offset: 0, payload: "aaa"))
      assert_receive {:repl_reply, ^ship, {:ack, "acme", 3}}, 2_000

      # Skip ahead. Accepting this is the corruption the offset field exists to prevent.
      Shipper.push(ship, push("acme", offset: 9_999, payload: "zzz"))
      assert_receive {:repl_reply, ^ship, {:reject, "acme", :offset_mismatch, 3}}, 2_000

      assert File.read!(Follower.wal_path(:f_gap, "acme")) == "aaa",
             "a rejected push still wrote to the WAL"
    end

    test "an unseeded shard is refused rather than fabricated from a fragment" do
      port = start_follower_named!(:f_unseeded)
      ship = start_shipper!(:s_unseeded, port)

      Shipper.push(ship, push("never-seeded"))
      assert_receive {:repl_reply, ^ship, {:reject, "never-seeded", :unknown_shard, 0}}, 2_000
      refute File.exists?(Follower.wal_path(:f_unseeded, "never-seeded"))
    end

    test "one connection carries several shards" do
      port = start_follower_named!(:f_multi)
      ship = start_shipper!(:s_multi, port)
      Follower.seed(:f_multi, "acme", 1, 1, 0, 0)
      Follower.seed(:f_multi, "beta", 1, 1, 0, 0)

      Shipper.push(ship, push("acme", payload: "AAAA"))
      Shipper.push(ship, push("beta", payload: "BB"))

      assert_receive {:repl_reply, ^ship, {:ack, "acme", 4}}, 2_000
      assert_receive {:repl_reply, ^ship, {:ack, "beta", 2}}, 2_000

      # The isolation that matters: shard A's bytes never reach shard B's file.
      assert File.read!(Follower.wal_path(:f_multi, "acme")) == "AAAA"
      assert File.read!(Follower.wal_path(:f_multi, "beta")) == "BB"
    end
  end

  describe "ship_quorum/4 across four followers" do
    setup do
      ships =
        for i <- 1..4 do
          {_f, port} = start_follower!(:"qf#{i}")
          # Seeded per follower: each has its own ETS table and its own directory, exactly as four
          # real nodes would.
          Follower.seed(:"qf#{i}", "acme", 1, 1, 0, 0)
          start_shipper!(:"qs#{i}", port)
        end

      %{ships: ships}
    end

    test "returns as soon as Q have acked", %{ships: ships} do
      # Returns WHO acked, not just that the quorum was met: the caller must advance only those
      # followers, since advancing a rejecter would leave the primary believing it holds bytes it
      # refused.
      assert {:ok, acked, []} = Replication.ship_quorum(fanout(ships, push("acme")), 2, 2_000)
      assert length(acked) >= 2
    end

    test "Q = N is refused at the call, not at a timeout", %{ships: ships} do
      # Same guard as Quorum.new/2, reached through the public entry point.
      assert_raise ArgumentError, ~r/must be < 4 followers/, fn ->
        Replication.ship_quorum(fanout(ships, push("acme")), 4, 500)
      end
    end

    test "a dead follower subtracts from the quorum instead of stalling it" do
      # Two live followers, two shippers pointed at a port nobody is listening on. With q=2 the
      # quorum is still reachable, so this must SUCCEED rather than wait for the dead ones.
      live =
        for i <- 5..6 do
          {_f, port} = start_follower!(:"lf#{i}")
          Follower.seed(:"lf#{i}", "acme", 1, 1, 0, 0)
          start_shipper!(:"ls#{i}", port)
        end

      dead =
        for i <- 7..8,
            do:
              start_supervised!({Shipper, name: :"ds#{i}", host: ~c"127.0.0.1", port: 1},
                id: :"ds#{i}"
              )

      assert {:ok, acked, rejects} =
               Replication.ship_quorum(fanout(live ++ dead, push("acme")), 2, 2_000)

      assert length(acked) == 2, "only the live followers should be counted as acked"
      assert Enum.all?(rejects, fn {_s, r, _at} -> r == :disconnected end)
    end

    test "reports :impossible rather than burning the timeout when too few can ack" do
      # Three of four cannot be reached, so a quorum of 2 can never form. The measured point of
      # failing fast: a commit blocked on an unreachable quorum is an outage wearing latency.
      {_f, port} = start_follower!(:if1)
      Follower.seed(:if1, "acme", 1, 1, 0, 0)
      live = start_shipper!(:is1, port)

      dead =
        for i <- 1..3,
            do:
              start_supervised!({Shipper, name: :"id#{i}", host: ~c"127.0.0.1", port: 1},
                id: :"id#{i}"
              )

      started = System.monotonic_time(:millisecond)

      # The third element is the per-follower reject list, which Session turns into seeds.
      assert {:error, {:no_quorum, :impossible, rejects}} =
               Replication.ship_quorum(fanout([live | dead], push("acme")), 2, 5_000)

      assert length(rejects) == 3,
             "every unreachable follower should be reported, got #{inspect(rejects)}"

      elapsed = System.monotonic_time(:millisecond) - started

      assert elapsed < 1_000,
             "took #{elapsed}ms — it waited for the timeout instead of proving the quorum " <>
               "unreachable, which is the whole point of Quorum's :impossible branch"
    end
  end

  # `push/2` is a cast, so nothing structurally bounds a shipper's mailbox: one socket per peer node
  # carries every shard, and if writes arrive faster than that socket drains, the queue grows and
  # every queued message holds a WAL-frame payload.
  #
  # This is not hypothetical. Sampling DURING a 1024-tenant rig ramp on 2026-08-15 caught a node at
  # total=45,409 MB with binary=44,541 MB against 111 MB of process memory, with this exact process
  # holding **25,866 queued messages**; across the run the deepest queue was a shipper in 109 of 143
  # samples, and binary memory tracked queue depth at 0.8–1.8 MB per message. The mailbox WAS the
  # memory, which is why `fullsweep_after: 0` reduced the OOM without preventing it — those binaries
  # are live, not garbage.
  describe "a saturated link refuses instead of queueing without bound" do
    test "a push past :replication_max_queue is rejected as :overloaded, not buffered" do
      # A peer that accepts the connection and then reads nothing, so the shipper's socket buffer
      # fills and its mailbox is the only place work can accumulate — the rig's condition, minus
      # the rig.
      {port, hole} = black_hole!(self())
      ship = start_shipper!(:overload_ship, port)

      # Bound it hard so the test states its own precondition rather than shipping megabytes.
      prev = Application.get_env(:fathom, :replication_max_queue)
      Application.put_env(:fathom, :replication_max_queue, 5)
      on_exit(fn -> restore_max_queue(prev) end)

      # Suspend the shipper so casts pile up in its mailbox deterministically — this is the
      # producer-outpaces-consumer state, made explicit instead of raced for.
      :erlang.suspend_process(ship)
      for i <- 1..40, do: Shipper.push(ship, push_for("ovl_#{i}"))
      :erlang.resume_process(ship)

      replies = drain_replies(40, 2_000)
      overloaded = for {:reject, id, :overloaded, _} <- replies, do: id

      assert overloaded != [],
             "a mailbox past the cap must reject as :overloaded; got #{inspect(Enum.uniq(Enum.map(replies, fn {_, _, r, _} -> r end)))}"

      # The bound has to be EFFECTIVE, not merely present: nearly all of the burst is refused, and
      # only a handful are accepted onto the wire. Those few stay pending on purpose — the black
      # hole never answers — which is exactly why "every push got a reply" is the wrong assertion
      # here and this one is the right one.
      assert length(overloaded) >= 30,
             "the bound must shed most of a burst, not a token few; overloaded=#{length(overloaded)}/40"

      send(hole, :die)
    end

    # The other direction: with the bound disabled, the same burst is absorbed silently. This is
    # what the rig hit — nothing refuses, the mailbox grows, and the memory goes with it.
    test "with :replication_max_queue 0 the same burst is absorbed, refusing nothing" do
      {port, hole} = black_hole!(self())
      ship = start_shipper!(:noovl_ship, port)

      prev = Application.get_env(:fathom, :replication_max_queue)
      Application.put_env(:fathom, :replication_max_queue, 0)
      on_exit(fn -> restore_max_queue(prev) end)

      :erlang.suspend_process(ship)
      for i <- 1..40, do: Shipper.push(ship, push_for("noovl_#{i}"))
      :erlang.resume_process(ship)

      replies = drain_replies(40, 1_000)
      overloaded = for {:reject, id, :overloaded, _} <- replies, do: id

      assert overloaded == [],
             "with the bound off nothing may be refused as :overloaded — that is the unbounded " <>
               "behaviour this cap exists to end, and it must stay reachable to reproduce it"

      send(hole, :die)
    end
  end

  defp restore_max_queue(nil), do: Application.delete_env(:fathom, :replication_max_queue)
  defp restore_max_queue(v), do: Application.put_env(:fathom, :replication_max_queue, v)

  defp push_for(shard_id) do
    %Push{
      shard_id: shard_id,
      epoch: 1,
      wal_gen: 1,
      salt1: 0,
      offset: 0,
      payload: :binary.copy(<<0>>, 256)
    }
  end

  defp drain_replies(n, timeout), do: drain_replies(n, timeout, [])
  defp drain_replies(0, _timeout, acc), do: Enum.reverse(acc)

  defp drain_replies(n, timeout, acc) do
    receive do
      {:repl_reply, _ship, reply} -> drain_replies(n - 1, timeout, [reply | acc])
    after
      timeout -> Enum.reverse(acc)
    end
  end

  describe "several connected followers dying at once" do
    # WHY THIS IS SEPARATE FROM THE DEAD-FOLLOWER TESTS ABOVE. Those build their dead followers as
    # shippers pointed at `port: 1` — nothing ever listened there, so `state.sock` is `nil` and
    # `handle_cast` rejects synchronously with no socket in the picture. A real node dies the other
    # way round: it was connected and healthy, and the rejection has to come out of
    # `Shipper.drop/2` on a `tcp_closed`. Nothing covered that path, in process or on the rig, and
    # `chaos.sh soak` cannot reach it either — it kills one node at a time on a 25 s timer, so with
    # q=2 of 4 followers the quorum was reachable in every run ever done.
    #
    # n = 4 followers, q = 2 throughout, matching the chaos rig's five-node layout: each node ships
    # to its four peers and needs two acks. So `n - q` = 2 is the survivable loss and `n - q + 1`
    # = 3 is the one that must fail closed.

    test "n - q dying at once still commits, and does not wait on the dead ones" do
      {ships, holes} = mixed_fleet!(:sv, 2, 2)
      kill_peers!(holes)

      started = System.monotonic_time(:millisecond)

      assert {:ok, acked, _rejects} =
               Replication.ship_quorum(fanout(ships, push("acme")), 2, 5_000)

      elapsed = System.monotonic_time(:millisecond) - started

      # Exactly two: only two followers are alive, and `Quorum.settle/1` returns at Q.
      assert length(acked) == 2, "the survivors must be the ones counted, got #{inspect(acked)}"

      # The rejects are deliberately NOT asserted on here. `collect/5` returns the moment the
      # quorum is reached, so whether the dead followers' rejects have been drained by then is a
      # race — pinning it would fail intermittently while proving nothing extra.
      assert elapsed < 1_000,
             "took #{elapsed}ms — the commit waited on followers that were already gone"
    end

    test "n - q + 1 dying at once is :impossible, not a burned timeout" do
      {ships, holes} = mixed_fleet!(:un, 1, 3)
      kill_peers!(holes)

      started = System.monotonic_time(:millisecond)

      assert {:error, {:no_quorum, :impossible, rejects}} =
               Replication.ship_quorum(fanout(ships, push("acme")), 2, 5_000)

      elapsed = System.monotonic_time(:millisecond) - started

      # Deterministic, unlike the successful arm: `:impossible` needs `n - rejected < q`, which at
      # n=4, q=2 is only true once three have rejected.
      assert length(rejects) == 3, "got #{inspect(rejects)}"

      assert elapsed < 1_000,
             "took #{elapsed}ms — a commit blocked on an unreachable quorum is an outage wearing " <>
               "latency, and the operator needs it as an error instead"
    end

    test "dying while HOLDING the pushes rejects them instead of stranding the commit" do
      # The two above kill before the push, so each shipper gets to notice on its own terms and
      # answer from `sock: nil`. This one kills DURING: the frames are on the wire and every
      # shipper is holding a waiter for one when its peer goes away. That is `Shipper.drop/2`'s
      # waiter loop (shipper.ex:250), and it is the only path a mid-commit node failure takes.
      {ships, holes} = mixed_fleet!(:fl, 1, 3)

      task =
        Task.async(fn -> Replication.ship_quorum(fanout(ships, push("acme")), 2, 5_000) end)

      # No sleep: the peers themselves report that they are holding a push, so the kill is ordered
      # against the send rather than against a guessed duration.
      for _ <- 1..3, do: assert_receive({:black_hole_push, _}, 2_000)

      started = System.monotonic_time(:millisecond)
      for pid <- holes, do: send(pid, :die)

      assert {:error, {:no_quorum, :impossible, rejects}} = Task.await(task, 5_000)
      elapsed = System.monotonic_time(:millisecond) - started

      assert length(rejects) == 3, "got #{inspect(rejects)}"

      assert Enum.all?(rejects, fn {_s, r, _at} -> r == :disconnected end),
             "a death mid-flight is a disconnect, not a refusal, got #{inspect(rejects)}"

      assert elapsed < 1_000,
             "took #{elapsed}ms from the kill — the in-flight waiters were stranded until the " <>
               "timeout instead of being failed by drop/2"
    end

    test "a graceful shutdown REFUSES rather than going quiet" do
      # The other direction, and a genuinely different mechanism — found by writing the tests
      # above wrong. A follower stopping cleanly keeps serving its accepted connections from an
      # unlinked `Task` whose ETS table has just gone, so `handle_push/2`'s rescue
      # (follower.ex:677) answers `:internal` instead of letting the primary wait out its timeout.
      #
      # This is the rolling-deploy shape, not the node-loss one, and it must stay distinguishable:
      # if it ever starts reporting `:disconnected` then either the rescue was removed or the
      # sockets became linked, and the two failure modes have stopped being tellable apart.
      for i <- 1..4 do
        name = :"gsf#{i}"
        {_pid, port} = start_follower!(name)
        Follower.seed(name, "acme", 1, 1, 0, 0)
        start_shipper!(:"gss#{i}", port)
      end

      ships = for i <- 1..4, do: Process.whereis(:"gss#{i}")
      for i <- 1..3, do: stop_supervised!(:"gsf#{i}")

      assert {:error, {:no_quorum, :impossible, rejects}} =
               Replication.ship_quorum(fanout(ships, push("acme")), 2, 5_000)

      assert Enum.all?(rejects, fn {_s, r, _at} -> r == :internal end),
             "a clean stop should refuse, not disconnect, got #{inspect(rejects)}"
    end
  end

  # #20 and #21 — the replication listener's exposure to ordinary and hostile network events. The
  # port is unauthenticated by design and binds every interface unless REPLICATION_BIND_IP is set.
  describe "the listener bounds what a peer can make it allocate (#20)" do
    # THE FINDING'S NUMBER WAS WRONG AND ITS DIRECTION WAS RIGHT, which is worth recording because
    # the obvious probe passes either way. It claimed `<<0xFFFFFFFF::32>>` forces a ~4 GiB
    # allocation. Measured on this OTP against a bare `packet: 4` listener, that exact value is
    # ALREADY refused with :emsgsize — the driver has its own sanity bound near the 32-bit ceiling.
    #
    #     declared     no packet_size    packet_size: 5MB
    #     8 MiB        accepted (waits)  :emsgsize
    #     1 GiB        accepted (waits)  :emsgsize
    #     4 GiB        :emsgsize         :emsgsize
    #
    # So the hazard is real but sits BELOW that ceiling: a peer declaring 1 GiB is accepted and the
    # driver waits, buffering toward it. Testing with 0xFFFFFFFF would have passed against the
    # unfixed code and proved nothing.
    test "a declared length far above any real frame is refused, not buffered" do
      dir = Path.join(System.tmp_dir!(), "pkt_#{System.unique_integer([:positive])}")
      name = :"pkt_f#{System.unique_integer([:positive])}"
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      on_exit(fn -> File.rm_rf(dir) end)

      # 64 MiB: above our derived cap (max(seed chunk, max push) + slack ≈ 4 MiB) and well below
      # the driver's own ceiling, so this is exactly the band packet_size adds.
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
      :ok = :gen_tcp.send(sock, <<64 * 1024 * 1024::32>>)
      :ok = :gen_tcp.send(sock, "dribble")

      # A bounded listener refuses the frame and closes the connection (serve/3's {:error, _}
      # branch). An unbounded one sits waiting for 64 MiB of body that never comes.
      assert {:error, :closed} = :gen_tcp.recv(sock, 0, 2_000),
             "the listener accepted a 64 MiB declared frame and is buffering toward it — " <>
               "packet_size is the only bound on what `packet: 4` allocates before any body byte " <>
               "arrives, and it is a security control on an unauthenticated port"

      :gen_tcp.close(sock)

      # Still alive, and a legitimate frame still works — the bound did not break real traffic.
      assert {:ok, ^port} = Follower.port(pid)

      {:ok, s2} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 2_000)

      :ok = :gen_tcp.send(s2, Fathom.Shard.Replication.Protocol.encode_position_query("acme"))
      assert {:ok, _} = :gen_tcp.recv(s2, 0, 2_000)
      :gen_tcp.close(s2)
    end
  end

  # #21 — OTP hygiene in the accept path.
  #
  # HONESTY ABOUT COVERAGE. The specific trigger the finding names — `controlling_process/2`
  # answering {:error, :closed} because the peer reset between accept and handoff — needs a race
  # this suite cannot force: connecting and aborting with SO_LINGER 0 does NOT reliably produce it,
  # and a test that passes either way is worse than none. So the bare `=` match is rescued on the
  # argument (an ordinary peer reset must not be a MatchError in a spawn_link'ed loop whose owner
  # does not trap exits), and what IS pinned here is the second half: with trap_exit set, the
  # listener no longer dies silently when its accept loop does.
  describe "the accept loop's exit is handled, not fatal-by-default (#21)" do
    test "a dead accept loop stops the listener cleanly instead of leaving a bound port" do
      dir = Path.join(System.tmp_dir!(), "exit_#{System.unique_integer([:positive])}")
      name = :"exit_f#{System.unique_integer([:positive])}"
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      on_exit(fn -> File.rm_rf(dir) end)

      # The accept loop is the process blocked in :gen_tcp.accept on this listener's socket.
      #
      # POLLED, NOT SAMPLED ONCE (CI, OTP 29, run 33230391419). `Follower.port/1` returning proves
      # the loop was SPAWNED — `handle_continue(:accept, …)` runs before any call is served — but
      # not that it has been SCHEDULED far enough to be parked in `:prim_inet.accept0`. A freshly
      # spawned process still shows its own entry function, so a single sample on a 2-core runner
      # found no match and the fixture refused to continue. It was right to: the assertion below
      # would have been measuring nothing. What was missing is that "spawned" and "parked" are
      # different instants, and only the second one is identifiable this way.
      #
      # Bounded and it FLUNKS — never a sleep-and-hope. A loop that never reaches `:prim_inet` is a
      # real defect, and this still says so.
      loop = await_accept_loop!(pid, 200)

      assert is_pid(loop), "could not find the accept loop; the fixture proves nothing"

      ref = Process.monitor(pid)
      Process.exit(loop, :kill)

      assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 2_000

      assert match?({:accept_loop_exited, _}, reason) or match?({:shutdown, _}, reason),
             "the listener died from the linked exit rather than handling it, so terminate/2 " <>
               "never ran and the listen socket was left bound with nothing accepting " <>
               "(got #{inspect(reason)})"

      # The port is genuinely released — terminate/2 ran.
      assert {:ok, l} = :gen_tcp.listen(port, [:binary, reuseaddr: true, active: false])
      :gen_tcp.close(l)
    end

    defp await_accept_loop!(_pid, 0), do: nil

    defp await_accept_loop!(pid, tries) do
      found =
        Process.info(pid, :links)
        |> elem(1)
        |> Enum.find(fn p ->
          is_pid(p) and
            match?({:current_function, {:prim_inet, _, _}}, Process.info(p, :current_function))
        end)

      if is_pid(found) do
        found
      else
        Process.sleep(5)
        await_accept_loop!(pid, tries - 1)
      end
    end
  end

  # SHIPPER IDENTITY (expert review 2026-08-20 #24).
  #
  # A caller records what it expects back keyed by the term it used to ADDRESS a shipper. Every
  # reply must therefore name that same term. Before this fix replies always carried `self()` and
  # the three primary-side modules normalised names with `Process.whereis(name) || name` — which
  # returns `nil` for a shipper mid-restart under the DynamicSupervisor or a node mid-`Membership`
  # swap, both of which AGENTS.md records as routine under load. The expectation was then filed
  # under the ATOM while the ack arrived under the PID: a good ack scored as `:offset_mismatch`,
  # and with N=3/Q=2 two of those drive `Quorum.settle/1` to `:impossible` and fail the tenant's
  # write with `FILO_NO_QUORUM` for no reason.
  describe "a shipper answers under the identity its caller addresses it by (#24)" do
    test "a named shipper's ack names the NAME, not its pid", ctx do
      %{root: _} = ctx
      port = start_follower_named!(:idf1)
      name = :"id_ship_#{System.unique_integer([:positive])}"

      pid =
        start_supervised!(
          {Shipper, name: name, id: name, host: ~c"127.0.0.1", port: port},
          id: name
        )

      assert Shipper.connected?(pid)

      p = %Push{
        shard_id: "acme",
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        offset: 0,
        payload: @payload
      }

      # Addressed by NAME — which is how `Fleet.shippers/0` publishes them, and therefore how
      # every real commit addresses them.
      Shipper.push(name, p)

      # The VERDICT is beside the point (this follower has never been seeded, so it is a
      # deterministic `:unknown_shard`); the IDENTITY is the whole test.
      assert_receive {:repl_reply, ^name, {:reject, "acme", :unknown_shard, 0}},
                     2_000,
                     "the reply did not name the shipper the way the caller addressed it; the " <>
                       "primary's expectation map and the answer would key on different things"
    end

    test "a locally refused push is reported under the same identity as a real reply" do
      # The budget refusal is produced in the CALLER's process, not the shipper's, so it is the
      # one reply path that cannot read `state.id`. It must still name the caller's own term or
      # the refusal is counted against nobody and the quorum waits out its deadline.
      prev = Application.get_env(:fathom, :replication_max_queue_bytes)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, :replication_max_queue_bytes),
          else: Application.put_env(:fathom, :replication_max_queue_bytes, prev)
      end)

      port = start_follower_named!(:idf2)
      name = :"id_full_#{System.unique_integer([:positive])}"

      pid =
        start_supervised!(
          {Shipper, name: name, id: name, host: ~c"127.0.0.1", port: port},
          id: name
        )

      assert Shipper.connected?(pid)

      # `Budget.queued/0` sums over the shippers Fleet publishes, so the fleet view is part of
      # what makes the refusal reachable at all.
      Fathom.Shard.Replication.Fleet.publish([{to_string(name), "127.0.0.1", port, name}])
      on_exit(fn -> Fathom.Shard.Replication.Fleet.publish([]) end)

      # A cap of one byte: any payload is over budget, so `push/2` takes the local-refusal path.
      Application.put_env(:fathom, :replication_max_queue_bytes, 1)

      Shipper.push(name, %Push{
        shard_id: "acme",
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        offset: 0,
        payload: @payload
      })

      assert_receive {:repl_reply, ^name, {:reject, "acme", :overloaded, 0}}, 2_000
    end
  end

  # The primary half of the same contract, and the half that cannot be reached with a real
  # `Shipper` — it is now the side that was fixed, so it always answers correctly. See
  # `Fathom.Test.IdentityShipper`.
  describe "ship_quorum keys expectations on the caller's term (#24)" do
    test "an ack that names the term the caller used is counted" do
      [a, b] = start_identity_shippers([:name, :name])

      assert {:ok, acked, []} = Replication.ship_quorum(fanout([a, b], identity_push()), 1, 1_000)
      assert acked != []
      assert Enum.all?(acked, &(&1 in [a, b])), "the quorum tallied an identity nobody addressed"
    end

    test "an ack that names its PID while addressed by NAME is NOT counted" do
      # This is the pre-fix world reproduced deliberately: the shipper answers by pid while the
      # caller addressed it by name. It must NOT satisfy the quorum — an identity the primary
      # cannot match is an unanswered push, and silently accepting it would let any process that
      # can guess a shard id vote in a quorum.
      [a, b] = start_identity_shippers([:pid, :pid])

      assert {:error, {:no_quorum, _, _}} =
               Replication.ship_quorum(fanout([a, b], identity_push()), 1, 300)
    end
  end

  defp start_identity_shippers(modes) do
    for {mode, i} <- Enum.with_index(modes) do
      name = :"iq_#{System.unique_integer([:positive])}_#{i}"
      reply_as = if mode == :pid, do: :pid, else: name
      pid = Fathom.Test.IdentityShipper.start(name, reply_as)
      on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
      name
    end
  end

  defp identity_push do
    %Push{shard_id: "acme", epoch: 1, wal_gen: 0, salt1: 0, offset: 0, payload: @payload}
  end
end
