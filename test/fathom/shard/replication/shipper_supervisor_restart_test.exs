defmodule Fathom.Shard.Replication.ShipperSupervisorRestartTest do
  @moduledoc """
  Audit 2026-11-22 #12. The shipper `DynamicSupervisor` is restarted EMPTY by `Fleet` if it dies, and
  `Membership` only starts shippers when the desired set changes, so the published shipper names stayed
  dead for good: every cast went to an unregistered name and every replicated commit waited out its
  deadline, until the node restarted.

  The supervisor is killed outright here (its restart budget is not what is under test; the budget only
  makes the death rarer). The invariant: after it comes back, every published shipper is running again.
  """
  use ExUnit.Case, async: false

  import Fathom.Test.ReplicationCase

  alias Fathom.Shard.Replication.{Fleet, Shipper}
  alias Fathom.Test.Await

  test "Membership restarts the shippers when their supervisor is restarted empty" do
    root = Path.join(System.tmp_dir!(), "shipsup_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(root) end)

    followers = start_followers!(root, 3, "shipsup")
    enable_replication!(followers, quorum: 2, connected: :all)

    names = Fleet.shippers()
    assert length(names) == 3
    assert Enum.all?(names, &is_pid(Process.whereis(&1))), "fixture: shippers must be registered"

    old = Process.whereis(Fleet.shippers_supervisor())
    ref = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old, :killed}, 2_000

    Await.until(
      fn ->
        match?(pid when is_pid(pid) and pid != old, Process.whereis(Fleet.shippers_supervisor()))
      end,
      "Fleet never restarted the shipper supervisor"
    )

    Await.until(
      fn -> Enum.all?(Fleet.shippers(), &is_pid(Process.whereis(&1))) end,
      "the published shippers stayed dead after their supervisor was restarted"
    )

    await_connected!(:all)
    assert Enum.all?(Fleet.shippers(), &Shipper.connected?/1)
  end
end
