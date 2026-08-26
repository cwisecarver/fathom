defmodule Fathom.Shard.ReplicationMembershipTest do
  @moduledoc """
  The guarded swap — A2 Layer 3. See `Fathom.Shard.Replication.Membership`.

  **What is actually being protected.** `Session.ship_planned/4` reads `Fleet.shippers/0` on every
  commit and derives `n` from its length; `Quorum.new/2` raises when `q >= n`, inside a tenant's
  write. Before membership could change, `Fleet.validate_quorum!/0` checking `q < n` once at boot
  was sufficient. It no longer is, so the refusal below is the replacement guarantee, not a
  nicety — every test here that asserts "the previous set stays live" is asserting that a tenant's
  write does not start raising.

  Uses the DataCase sandbox because roster mode reads Postgres; the static-mode tests do not need
  it but share the setup rather than splitting the file.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Rebalancer.Nodes
  alias Fathom.Shard.Replication.{Fleet, Membership}

  setup do
    prev =
      Map.new(
        [
          :replication_enabled,
          :replication_listen,
          :replication_followers,
          :replication_quorum,
          :replication_membership,
          :replication_membership_poll_ms,
          :node_key
        ],
        &{&1, Application.get_env(:fathom, &1)}
      )

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Fleet.publish([])
    end)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_listen, false)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :node_key, "self")
    # Long enough that no test races the timer; every test drives swaps with refresh/0.
    Application.put_env(:fathom, :replication_membership_poll_ms, 3_600_000)
    :ok
  end

  # Closed ports on purpose: membership is about WHICH followers are in the set, and a shipper
  # that cannot connect is still a member (that is the whole liveness-is-not-membership rule).
  defp followers(n) do
    for i <- 1..n, do: {"f#{i}", "127.0.0.1", 9100 + i}
  end

  defp start_membership! do
    Application.put_env(:fathom, :replication_followers, followers(3))
    start_supervised!(Fleet)
    :ok
  end

  defp keys, do: Fleet.running() |> Enum.map(&elem(&1, 0)) |> Enum.sort()

  describe "static source" do
    test "publishes the configured list and keeps shippers/0 in step with running/0" do
      start_membership!()

      assert keys() == ~w(f1 f2 f3)
      assert length(Fleet.shippers()) == 3

      names = Enum.map(Fleet.running(), &elem(&1, 3))
      assert Enum.sort(names) == Enum.sort(Fleet.shippers())
    end

    test "every published shipper name resolves to a live process" do
      start_membership!()

      for name <- Fleet.shippers() do
        assert is_pid(Process.whereis(name)),
               "#{name} is published to the commit path but has no process — a commit would " <>
                 "call into a dead name"
      end
    end
  end

  describe "the guarded swap" do
    test "a set smaller than quorum+1 is REFUSED and the previous set stays live" do
      start_membership!()
      assert keys() == ~w(f1 f2 f3)

      # q=2 needs 3. Two would leave n=2, q=2 → `q >= n` inside the next commit.
      Application.put_env(:fathom, :replication_followers, followers(2))

      assert {:refused, :static_list_too_small} = Membership.refresh()
      assert keys() == ~w(f1 f2 f3), "the refused set was applied anyway"
      assert length(Fleet.shippers()) == 3
    end

    test "quorum+1 exactly is accepted — the boundary is >=, not >" do
      start_membership!()
      Application.put_env(:fathom, :replication_quorum, 2)
      Application.put_env(:fathom, :replication_followers, followers(3))

      assert {:ok, _} = Membership.refresh()
      assert keys() == ~w(f1 f2 f3)
    end

    test "growing the set starts the new shipper and keeps the existing ones" do
      start_membership!()
      before = Map.new(Fleet.running(), fn {k, _h, _p, name} -> {k, Process.whereis(name)} end)

      Application.put_env(:fathom, :replication_followers, followers(4))
      assert {:ok, _} = Membership.refresh()

      assert keys() == ~w(f1 f2 f3 f4)

      # The three survivors keep their sockets. An index-derived shipper name would have restarted
      # them all; the name is derived from node_key precisely so it does not.
      for {k, pid} <- before do
        now = Fleet.running() |> Enum.find(&(elem(&1, 0) == k)) |> elem(3) |> Process.whereis()
        assert now == pid, "#{k}'s shipper was restarted by an unrelated membership change"
      end
    end

    test "shrinking to a still-legal set stops only the departed shipper" do
      Application.put_env(:fathom, :replication_followers, followers(4))
      start_supervised!(Fleet)
      assert keys() == ~w(f1 f2 f3 f4)

      gone = Fleet.running() |> Enum.find(&(elem(&1, 0) == "f4")) |> elem(3)
      gone_pid = Process.whereis(gone)

      Application.put_env(:fathom, :replication_followers, followers(3))
      assert {:ok, _} = Membership.refresh()

      assert keys() == ~w(f1 f2 f3)
      refute Enum.member?(Fleet.shippers(), gone), "a departed shipper is still published"

      ref = Process.monitor(gone_pid)
      assert_receive {:DOWN, ^ref, :process, ^gone_pid, _}, 2_000
    end

    # THIS DOES NOT PROVE THE SWAP ORDERING, and must not be read as if it did.
    #
    # `Membership.apply_set!/1` starts new shippers, THEN publishes, THEN stops departed ones.
    # Inverting that to publish-then-start was tried against this suite and **all 12 tests still
    # passed**, because every assertion here runs after the swap has completed — by which point
    # both orders have reached the same state. Catching the difference needs a reader observing
    # the intermediate state, i.e. a commit racing the swap, which is not deterministically
    # reproducible.
    #
    # What it DOES catch is a name published that was never started at all (verified: removing the
    # start loop fails this test and two others). The ordering itself rests on the argument in
    # `Membership`'s moduledoc, not on this test.
    test "no published name is ever dead after a swap" do
      start_membership!()
      Application.put_env(:fathom, :replication_followers, followers(5))
      assert {:ok, _} = Membership.refresh()

      Application.put_env(:fathom, :replication_followers, followers(3))
      assert {:ok, _} = Membership.refresh()

      for name <- Fleet.shippers(), do: assert(is_pid(Process.whereis(name)))
    end
  end

  # `:persistent_term` outlives the tree that wrote it, and until 2026-08-26 nothing retracted the
  # published set when Fleet stopped. `Fleet.init/1` clears on START and says why; there was no
  # matching clear on STOP, so a stopped Fleet left dead shipper names published for every later
  # reader of `shippers/0` and `running/0`.
  #
  # It surfaced as a CI failure two commits away from here: `Recovery.peers/0` was non-empty for
  # every test that followed a replication test, so promote-on-open silently took the fleet branch.
  # Note this file's own `setup` has published `[]` in `on_exit` all along — a workaround for a leak
  # nobody had named, which is exactly why the tests in THIS file never saw it.
  describe "shutdown retracts the published set" do
    test "an orderly stop clears both keys" do
      start_membership!()
      assert length(Fleet.shippers()) == 3
      assert length(Fleet.running()) == 3

      :ok = stop_supervised(Fleet)

      assert Fleet.running() == [], "running/0 still names shippers from a stopped Fleet"
      assert Fleet.shippers() == [], "shippers/0 still names shippers from a stopped Fleet"
    end

    # THE GUARD, and the reason `terminate/2` is not simply "clear it". `terminate/2` also runs when
    # a callback raises, and a crash blanks nothing on purpose: emptying `shippers/0` mid-flight
    # drops `n` to 0 while this process restarts, and `q >= n` raises INSIDE a tenant's write — the
    # failure `validate_quorum!/0` exists to keep off the commit path. `init/1` republishes the set
    # moments later, so leaving it standing is both safe and correct.
    #
    # Driven by calling the callback directly rather than by crashing the live process: a real crash
    # is restarted by the supervisor, which republishes, so the assertion would be racing its own
    # recovery and would pass whether or not the guard existed.
    test "a crash reason leaves the set standing; only a deliberate teardown retracts" do
      start_membership!()
      published = Fleet.running()
      assert length(published) == 3

      assert Membership.terminate(:boom, %{}) == :ok

      assert Fleet.running() == published,
             "a crash retracted the set and would strand live commits"

      assert Membership.terminate({:shutdown, :db_connection_closed}, %{}) == :ok
      assert Fleet.running() == [], "a {:shutdown, reason} teardown should retract"
    end
  end

  describe "roster source" do
    setup do
      Application.put_env(:fathom, :replication_membership, :roster)
      :ok
    end

    test "derives the set from published endpoints and excludes this node" do
      Application.put_env(:fathom, :replication_followers, followers(3))

      for k <- ~w(peer1 peer2 peer3), do: Nodes.beat(k, replication_address: "10.0.0.1:9100")
      # This node beats too; shipping to yourself is not membership.
      Nodes.beat("self", replication_address: "10.0.0.9:9100")

      start_supervised!(Fleet)

      assert keys() == ~w(peer1 peer2 peer3)
      refute Enum.member?(keys(), "self")
    end

    # The fallback is what keeps REPLICATION_FOLLOWERS meaningful rather than dead config, and it
    # is the path a fresh fleet and a rolling upgrade both take.
    test "falls back to the configured list when the roster cannot supply quorum+1" do
      Application.put_env(:fathom, :replication_followers, followers(3))
      Nodes.beat("peer1", replication_address: "10.0.0.1:9100")

      start_supervised!(Fleet)

      assert keys() == ~w(f1 f2 f3), "one roster entry is below q+1; the static floor should hold"
    end

    test "a node with no published address is not a candidate" do
      Application.put_env(:fathom, :replication_followers, followers(3))
      for k <- ~w(peer1 peer2), do: Nodes.beat(k, replication_address: "10.0.0.1:9100")
      # Beating without an address (older release, or not listening) must not count toward q+1.
      Nodes.beat("peer3")

      start_supervised!(Fleet)

      assert keys() == ~w(f1 f2 f3), "a nil-address node was counted as a candidate"
    end

    # One malformed stored row must not take a running node's whole replica set with it — unlike
    # REPLICATION_FOLLOWERS, this value was not typed by an operator standing at the boot.
    test "a malformed stored address is skipped, not raised on" do
      Application.put_env(:fathom, :replication_followers, followers(3))
      for k <- ~w(peer1 peer2 peer3), do: Nodes.beat(k, replication_address: "10.0.0.1:9100")
      Nodes.beat("peer4", replication_address: "not-an-endpoint")

      start_supervised!(Fleet)

      assert keys() == ~w(peer1 peer2 peer3)
    end

    test "a roster change is picked up on refresh" do
      Application.put_env(:fathom, :replication_followers, followers(3))
      for k <- ~w(peer1 peer2 peer3), do: Nodes.beat(k, replication_address: "10.0.0.1:9100")

      start_supervised!(Fleet)
      assert keys() == ~w(peer1 peer2 peer3)

      Nodes.beat("peer4", replication_address: "10.0.0.4:9100")
      assert {:ok, _} = Membership.refresh()

      assert keys() == ~w(peer1 peer2 peer3 peer4)
    end
  end
end
