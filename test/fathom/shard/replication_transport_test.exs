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
end
