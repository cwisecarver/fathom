defmodule Fathom.Rebalancer.CommandPollerTest do
  @moduledoc """
  The per-node handoff executor. Not async — it drains real shard coordinators (global
  Registry + files) and shares the sandbox connection (DataCase async: false ⇒ shared).
  """
  use Fathom.DataCase, async: false

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{CommandPoller, Commands}
  alias Fathom.Shards

  setup do
    shard = "poll_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for dir <- ["fathom_shards", "fathom_remote_test"], suffix <- ["", "-wal", "-shm"] do
        File.rm(Path.join([System.tmp_dir!(), dir, "#{shard}.db"]) <> suffix)
      end
    end)

    %{shard: shard, node: Rebalancer.node_key()}
  end

  test "a drain command releases the shard on this node and marks the command done",
       %{shard: shard, node: node} do
    # Open the shard here (starts a coordinator), then release the connection so drain
    # isn't blocked by an in-flight checkout.
    {:ok, pid, ref, _path} = Shards.checkout(shard)
    Fathom.Shard.checkin(pid, ref)
    down = Process.monitor(pid)

    {:ok, cmd} = Commands.issue(shard, node, "drain")
    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 1

    # The coordinator stopped (lease released) and the command is done.
    assert_receive {:DOWN, ^down, :process, ^pid, _reason}, 5_000
    assert Commands.get(cmd.id).status == "done"
  end

  test "only commands addressed to this node are executed", %{shard: shard} do
    {:ok, cmd} = Commands.issue(shard, "some_other_node", "drain")
    start_supervised!(CommandPoller)

    assert CommandPoller.poll_now() == 0, "a command for another node is left untouched"
    assert Commands.get(cmd.id).status == "pending"
  end

  test "a warm command is best-effort: an un-flushed shard still marks done",
       %{shard: shard, node: node} do
    # No stored object for this shard, so warm_now can't fetch — but warm is only an
    # optimization (the target cold-opens correctly), so the command still completes.
    {:ok, cmd} = Commands.issue(shard, node, "warm")
    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 1

    done = Commands.get(cmd.id)
    assert done.status == "done"
    assert done.detail =~ "warm"
  end
end
