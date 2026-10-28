defmodule Fathom.Shard.ReplicationRecoveryGuardTest do
  @moduledoc """
  The prod boot guard for peer recovery over a follower SUBSET (`Fleet.recovery_peers_problem/0`).

  Jepsen 2026-10-27 lost quorum-acked writes on a follower ring because a takeover asked only its own
  followers, so recovery's responder floor was computed over the wrong set (`Recovery.peers/0`). A node
  cannot tell from its own config whether its follower list is every other node, so a node that could
  run recovery over a subset must say it does not: `REPLICATION_RECOVERY_PEERS`, an explicit
  `REPLICATION_RECOVERY_SYMMETRIC=true`, roster membership, or peer recovery switched off.

  The guard is a pure read of application config, so most of these never start the supervisor. One
  test does, because `Fleet.init/1` is where the verdict becomes a refusal to boot.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Fleet

  @env [
    :replication_recovery_guard,
    :replication_enabled,
    :replication_listen,
    :replication_recover_from_peers,
    :replication_promote_on_open,
    :replication_recovery_peers,
    :replication_recovery_symmetric,
    :replication_membership,
    :replication_followers,
    :replication_quorum
  ]

  setup do
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    # The prod shape that the guard exists for: every gate on, three followers of a bigger fleet, and
    # none of the escape hatches.
    Application.put_env(:fathom, :replication_recovery_guard, true)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_listen, true)
    Application.put_env(:fathom, :replication_recover_from_peers, true)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.delete_env(:fathom, :replication_recovery_peers)
    Application.delete_env(:fathom, :replication_recovery_symmetric)
    Application.put_env(:fathom, :replication_membership, :static)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(:fathom, :replication_followers, [
      {"f2", "127.0.0.1", 1},
      {"f3", "127.0.0.1", 2},
      {"f4", "127.0.0.1", 3}
    ])

    :ok
  end

  test "a prod node with a follower list and no recovery peers is refused, with the way out" do
    assert message = Fleet.recovery_peers_problem()
    assert message =~ "REPLICATION_RECOVERY_PEERS"
    assert message =~ "REPLICATION_RECOVERY_SYMMETRIC"
    assert message =~ "3 REPLICATION_FOLLOWERS"
  end

  describe "each way out clears the verdict" do
    test "naming every other node" do
      Application.put_env(:fathom, :replication_recovery_peers, [{"f5", "127.0.0.1", 4}])
      assert Fleet.recovery_peers_problem() == nil
    end

    test "asserting a full mesh" do
      Application.put_env(:fathom, :replication_recovery_symmetric, true)
      assert Fleet.recovery_peers_problem() == nil
    end

    test "roster membership, which ships to every advertising node" do
      Application.put_env(:fathom, :replication_membership, :roster)
      assert Fleet.recovery_peers_problem() == nil
    end

    test "switching peer recovery off" do
      Application.put_env(:fathom, :replication_recover_from_peers, false)
      assert Fleet.recovery_peers_problem() == nil
    end

    test "switching promote-on-open off" do
      Application.put_env(:fathom, :replication_promote_on_open, false)
      assert Fleet.recovery_peers_problem() == nil
    end
  end

  describe "nothing to guard" do
    test "a node that does not ship" do
      Application.put_env(:fathom, :replication_enabled, false)
      assert Fleet.recovery_peers_problem() == nil
    end

    test "a node that is not listening never runs recovery" do
      Application.put_env(:fathom, :replication_listen, false)
      assert Fleet.recovery_peers_problem() == nil
    end

    test "dev and test, where the guard is off" do
      Application.put_env(:fathom, :replication_recovery_guard, false)
      assert Fleet.recovery_peers_problem() == nil
    end
  end

  test "the verdict is a boot failure: Fleet refuses to start" do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: message}, _}} = Fleet.start_link([])
    assert message =~ "REPLICATION_RECOVERY_PEERS"
  end
end
