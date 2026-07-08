defmodule Fathom.Rebalancer.CommandPollerTest do
  @moduledoc """
  The per-node handoff executor. Not async — it drains real shard coordinators (global
  Registry + files) and shares the sandbox connection (DataCase async: false ⇒ shared).
  """
  use Fathom.DataCase, async: false

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{CommandPoller, Commands, Overrides}
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
    # A real drain is always preceded by a pin (pin_and_flip runs before drain is issued);
    # the poller re-checks the pin before draining (#7), so pin the shard first.
    {:ok, _} = Overrides.pin(shard, node, reason: "test")

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

  test "a drain for a reverted shard is skipped + cancelled; the source is not drained (#7)",
       %{shard: shard, node: node} do
    # Regression for #7: the handoff reverted (pin marked failed) but a pending drain
    # remained. The poller must NOT drain the now-serving source — it cancels the command
    # instead. (Covers the race where the poller reaches the drain before cancel_pending.)
    {:ok, _} = Overrides.pin(shard, node, reason: "test")
    :ok = Overrides.mark_failed(shard)

    {:ok, pid, ref, _path} = Shards.checkout(shard)
    Fathom.Shard.checkin(pid, ref)
    down = Process.monitor(pid)

    {:ok, cmd} = Commands.issue(shard, node, "drain")
    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 1

    assert Commands.get(cmd.id).status == "cancelled"
    # The coordinator was NOT drained (still alive serving the source).
    refute_receive {:DOWN, ^down, :process, ^pid, _}, 500
    assert Process.alive?(pid)
  end

  test "a Postgres outage makes a poll tick a no-op without crashing (#13)", %{shard: shard} do
    import ExUnit.CaptureLog

    {:ok, _} = Commands.issue(shard, Rebalancer.node_key(), "warm")
    pid = start_supervised!(CommandPoller)

    # Cut the poller off from Postgres: pending_for raises. do_poll must drop the tick
    # (rescue + catch :exit) and the poller must survive.
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    log = capture_log(fn -> assert CommandPoller.poll_now() == 0 end)
    assert log =~ "command poller tick"
    assert Process.alive?(pid)
  end

  test "only commands addressed to this node are executed", %{shard: shard} do
    {:ok, cmd} = Commands.issue(shard, "some_other_node", "drain")
    start_supervised!(CommandPoller)

    assert CommandPoller.poll_now() == 0, "a command for another node is left untouched"
    assert Commands.get(cmd.id).status == "pending"
  end

  test "a poll batch executes all commands concurrently off the poller (#8)", %{node: node} do
    # Multiple commands in one batch all complete via a single poll (the Task.Supervisor
    # async_stream), so a slow drain can't head-of-line-block the warms behind it.
    ids =
      for i <- 1..3 do
        {:ok, c} =
          Commands.issue("warmbatch_#{i}_#{System.unique_integer([:positive])}", node, "warm")

        c.id
      end

    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 3

    for id <- ids, do: assert(Commands.get(id).status == "done")
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
