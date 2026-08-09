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

  defp push(shard, opts \\ []) do
    %Push{
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
      Follower.seed(:f_solo, "acme", 1, 1, 0)

      Shipper.push(ship, push("acme"))

      assert_receive {:repl_reply, ^ship, {:ack, "acme", 512}}, 2_000
      assert File.read!(Follower.wal_path(:f_solo, "acme")) == @payload
      assert Follower.state_of(:f_solo, "acme").next_offset == 512
    end

    test "successive deltas append in order" do
      port = start_follower_named!(:f_seq)
      ship = start_shipper!(:s_seq, port)
      Follower.seed(:f_seq, "acme", 1, 1, 0)

      Shipper.push(ship, push("acme", offset: 0, payload: "aaa"))
      assert_receive {:repl_reply, ^ship, {:ack, "acme", 3}}, 2_000

      Shipper.push(ship, push("acme", offset: 3, payload: "bbb"))
      assert_receive {:repl_reply, ^ship, {:ack, "acme", 6}}, 2_000

      assert File.read!(Follower.wal_path(:f_seq, "acme")) == "aaabbb"
    end

    test "a gap is refused with the follower's real offset, and nothing is written" do
      port = start_follower_named!(:f_gap)
      ship = start_shipper!(:s_gap, port)
      Follower.seed(:f_gap, "acme", 1, 1, 0)

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
      Follower.seed(:f_multi, "acme", 1, 1, 0)
      Follower.seed(:f_multi, "beta", 1, 1, 0)

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
          Follower.seed(:"qf#{i}", "acme", 1, 1, 0)
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
          Follower.seed(:"lf#{i}", "acme", 1, 1, 0)
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
      Follower.seed(:if1, "acme", 1, 1, 0)
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
end
