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
      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          suffix <- ["", "-wal", "-shm"] do
        File.rm(Path.join([dir, "#{shard}.db"]) <> suffix)
      end
    end)

    %{shard: shard, node: Rebalancer.node_key()}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp wait_until(fun, tries \\ 400) do
    cond do
      fun.() -> :ok
      tries <= 0 -> flunk("condition never met")
      true -> Process.sleep(5) && wait_until(fun, tries - 1)
    end
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

  defp touch_loop(shard, parent) do
    outcome =
      case Shards.checkout(shard) do
        {:ok, pid, ref, _path} ->
          Fathom.Shard.checkin(pid, ref)
          :opened

        {:error, reason} ->
          reason
      end

    send(parent, {:touched, outcome})
    touch_loop(shard, parent)
  end

  # Waits for `n` client attempts that STARTED after this call, discarding older ones.
  defp await_fresh_touches(n) do
    flush_touches()
    for _ <- 1..n, do: assert_receive({:touched, _}, 5_000)
  end

  defp flush_touches do
    receive do
      {:touched, _} -> flush_touches()
    after
      0 -> :ok
    end
  end

  describe "drain_for_migration re-open gate (expert review 2026-10-12 #23)" do
    # A hot tenant's requests keep arriving at the holder (the LB routes there) while a migrator on
    # another node waits for its drain_for_migration to complete and then acquires the lease. The
    # request right after the drain re-opened the shard and re-took the lease first, every time, so
    # the migration snoozed forever. The invariant pinned: after the holder completes the drain, a
    # continuously-touching client cannot re-take the lease ahead of the migrator.
    setup %{shard: shard} do
      on_exit(fn -> Fathom.Shards.MigrationGate.open(shard) end)
      :ok
    end

    test "the migrator's lease acquire wins against a continuously-touched shard",
         %{shard: shard, node: node} do
      {:ok, pid, ref, _path} = Shards.checkout(shard)
      Fathom.Shard.checkin(pid, ref)

      parent = self()
      toucher = spawn_link(fn -> touch_loop(shard, parent) end)
      await_fresh_touches(3)

      {:ok, cmd} = Commands.issue(shard, node, "drain_for_migration")
      start_supervised!(CommandPoller)
      assert CommandPoller.poll_now() == 1
      assert Commands.get(cmd.id).status == "done"

      # The client gets its post-drain requests in BEFORE the migrator acquires — the race the
      # migrator lost. Two, so at least one started after the drain completed.
      await_fresh_touches(2)

      assert {:ok, lease} =
               Fathom.Shard.Storage.acquire_lease(shard, "migrator@test@1", 30_000),
             "a client re-opened the drained shard and re-took the lease before the migrator"

      Process.unlink(toucher)
      Process.exit(toucher, :kill)
      :ok = Fathom.Shard.Storage.release_lease(shard, lease)
    end

    test "open/1 (a failed drain) lifts the gate so the shard serves here again", %{shard: shard} do
      Fathom.Shards.MigrationGate.close(shard, 60_000)
      assert {:error, {:shard_migrating, :drained_for_migration}} = Shards.checkout(shard)

      Fathom.Shards.MigrationGate.open(shard)
      assert {:ok, pid, ref, _path} = Shards.checkout(shard)
      Fathom.Shard.checkin(pid, ref)
    end

    test "the gate expires on its own", %{shard: shard} do
      Fathom.Shards.MigrationGate.close(shard, 0)
      refute Fathom.Shards.MigrationGate.closed?(shard)
      assert {:ok, pid, ref, _path} = Shards.checkout(shard)
      Fathom.Shard.checkin(pid, ref)
    end

    test "a running coordinator is never gated (only re-opens are)", %{shard: shard} do
      {:ok, pid, ref, _path} = Shards.checkout(shard)
      Fathom.Shard.checkin(pid, ref)

      Fathom.Shards.MigrationGate.close(shard, 60_000)
      assert {:ok, ^pid, ref, _path} = Shards.checkout(shard)
      Fathom.Shard.checkin(pid, ref)
    end
  end

  # Expert review 2026-10-12 #34. A command whose task is killed by the per-command timeout never
  # reached its own completion, so its row stayed `pending` and the next tick re-selected it: a live
  # drain re-ran every tick and re-closed the MigrationGate each time. The poller must mark it failed.
  test "a command killed by the task timeout is marked failed, not re-selected (#34)",
       %{shard: shard, node: node} do
    {:ok, _} = Overrides.pin(shard, node, reason: "test")
    prev_t = Application.get_env(:fathom, :command_task_timeout_ms)
    prev_d = Application.get_env(:fathom, :command_drain_ms)
    Application.put_env(:fathom, :command_task_timeout_ms, 300)
    Application.put_env(:fathom, :command_drain_ms, 5_000)

    on_exit(fn ->
      restore(:command_task_timeout_ms, prev_t)
      restore(:command_drain_ms, prev_d)
    end)

    # Hold a checkout open so the drain blocks on the busy connection for its whole 5 s budget;
    # the 300 ms task timeout kills the execute long before that.
    {:ok, pid, ref, _path} = Shards.checkout(shard)

    {:ok, cmd} = Commands.issue(shard, node, "drain")
    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 0

    assert %{status: "failed", detail: "did not finish: " <> _} = Commands.get(cmd.id)
    assert Commands.pending_for(node) == [], "a killed command must not be re-selected"

    Fathom.Shard.checkin(pid, ref)
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

    {:ok, _} = Commands.issue(shard, Rebalancer.node_key(), "drain")
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

  # rig 2026-10-14 B2 once made the poller claim commands under node() as well as :node_key,
  # because the lease owner carried node(). The owner now carries the key (expert review
  # 2026-10-15 #17, see owner_node_key_test.exs), so the poller claims the key ONLY: a command
  # addressed to the BEAM node name when the two differ is someone else's.
  test "with :node_key != node(), only key-addressed commands are claimed", %{shard: shard} do
    Application.put_env(:fathom, :node_key, "fathom-keyed-#{System.unique_integer([:positive])}")
    on_exit(fn -> Application.delete_env(:fathom, :node_key) end)
    refute Rebalancer.node_key() == to_string(node())

    {:ok, by_name} = Commands.issue(shard, to_string(node()), "flush")
    {:ok, by_key} = Commands.issue(shard, Rebalancer.node_key(), "flush")
    start_supervised!(CommandPoller)

    assert CommandPoller.poll_now() == 1
    assert Commands.get(by_key.id).status == "done"
    assert Commands.get(by_name.id).status == "pending"
  end

  test "a crashing telemetry handler never wedges a command's completion (#8)", %{node: node} do
    # Pins the safety property: an observability handler that raises must not prevent the
    # durable Commands.complete (telemetry isolates handler crashes, and the emit is after the
    # write). A wedged-pending command would otherwise re-execute every poll.
    id = "crash-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      [:fathom, :rebalancer, :command, :stop],
      fn _e, _m, _meta, _cfg -> raise "handler boom" end,
      %{}
    )

    on_exit(fn -> :telemetry.detach(id) end)

    shard = "crashwedge_#{System.unique_integer([:positive])}"
    {:ok, _} = Overrides.pin(shard, node, reason: "test")
    {:ok, cmd} = Commands.issue(shard, node, "drain")
    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 1

    assert Commands.get(cmd.id).status == "done", "command completed despite the crashing handler"
  end

  test "a poll batch executes all commands concurrently off the poller (#8)", %{node: node} do
    # Multiple commands in one batch all complete via a single poll (the Task.Supervisor
    # async_stream), so a slow drain can't head-of-line-block the drains behind it.
    ids =
      for i <- 1..3 do
        shard = "drainbatch_#{i}_#{System.unique_integer([:positive])}"
        {:ok, _} = Overrides.pin(shard, node, reason: "test")
        {:ok, c} = Commands.issue(shard, node, "drain")
        c.id
      end

    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 3

    for id <- ids, do: assert(Commands.get(id).status == "done")
  end

  test "the per-command task timeout is ordered above the drain worst-case (#11)" do
    # on_timeout: :kill_task is only meaningful with a FINITE timeout, and it must exceed the
    # poller's worst-case drain (command_drain_ms + Shards.drain's +30s net) so a slow-but-
    # succeeding drain isn't killed mid-flight — only a genuinely wedged command hits it.
    prev = Application.get_env(:fathom, :command_drain_ms)
    prev_t = Application.get_env(:fathom, :command_task_timeout_ms)
    Application.put_env(:fathom, :command_drain_ms, 10_000)
    Application.delete_env(:fathom, :command_task_timeout_ms)

    on_exit(fn ->
      restore(:command_drain_ms, prev)
      restore(:command_task_timeout_ms, prev_t)
    end)

    assert CommandPoller.task_timeout() >= 10_000 + 30_000

    # An explicit override wins (operator owns the ordering).
    Application.put_env(:fathom, :command_task_timeout_ms, 99_999)
    assert CommandPoller.task_timeout() == 99_999
  end

  test "the poll timer decouples from a slow in-flight drain (#11)", %{node: node} do
    # A slow drain must not gate the next tick's pickup of a DIFFERENT command. Drive dispatch
    # explicitly (high auto-poll interval) so the assertions are deterministic, and hold a
    # connection on the slow shard so its drain blocks ~command_drain_ms instead of completing.
    # (Before warm standby was retired 2026-09-14 the second command was a `warm`; it is now a
    # second, fast drain — the decoupling property is identical.)
    slow_shard = "poll11d_#{System.unique_integer([:positive])}"
    fast_shard = "poll11f_#{System.unique_integer([:positive])}"

    prev_poll = Application.get_env(:fathom, :command_poll_ms)
    prev_drain = Application.get_env(:fathom, :command_drain_ms)
    Application.put_env(:fathom, :command_poll_ms, 60_000)
    Application.put_env(:fathom, :command_drain_ms, 5_000)

    on_exit(fn ->
      restore(:command_poll_ms, prev_poll)
      restore(:command_drain_ms, prev_drain)

      for id <- [slow_shard, fast_shard],
          dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          s <- ["", "-wal", "-shm"] do
        File.rm(Path.join([dir, "#{id}.db"]) <> s)
      end
    end)

    {:ok, _} = Overrides.pin(slow_shard, node, reason: "test")
    {:ok, cpid, ref, _} = Shards.checkout(slow_shard)
    {:ok, slow_cmd} = Commands.issue(slow_shard, node, "drain")

    poller = start_supervised!(CommandPoller)

    # Tick 1: dispatch the slow drain; its task blocks on the held connection (in-flight).
    send(poller, :poll)
    assert MapSet.member?(:sys.get_state(poller).in_flight, slow_cmd.id)

    # A fast drain (no held connection) arrives while the slow one is still blocked.
    {:ok, _} = Overrides.pin(fast_shard, node, reason: "test")
    {:ok, fast_cmd} = Commands.issue(fast_shard, node, "drain")

    # Tick 2: the fast drain is picked up and completes within a short window while the slow drain
    # (5s) is still pending — the decoupling. (Pre-fix the poller blocked on the first drain batch,
    # so the second command couldn't be dispatched until the slow drain returned.)
    send(poller, :poll)
    assert {:ok, %{status: "done"}} = Commands.await(fast_cmd.id, timeout_ms: 2_000, poll_ms: 25)

    assert Commands.get(slow_cmd.id).status == "pending",
           "slow drain still draining, didn't block the fast one"

    # Release so the slow drain finishes; the poller survives.
    down = Process.monitor(cpid)
    Fathom.Shard.checkin(cpid, ref)
    assert_receive {:DOWN, ^down, :process, ^cpid, _}, 8_000
    assert Process.alive?(poller)
  end

  # (The "warm command is best-effort" test was removed 2026-09-14 with the WarmFollower retirement.)

  test "a batch task killed by an exit signal frees its in-flight ids — no permanent leak (#19)",
       %{node: node} do
    # The detached batch task's try/after sends {:batch_done} on a normal exit or a raise, but NOT
    # on an exit SIGNAL: a TaskSupervisor restart (or a kill) leaves the ids in in_flight forever,
    # so fresh_pending rejects the command every future tick and it's wedged `pending`. The poller's
    # MONITOR is the guaranteed cleanup — DOWN frees the ids regardless.
    drain_shard = "poll19_#{System.unique_integer([:positive])}"

    prev_poll = Application.get_env(:fathom, :command_poll_ms)
    prev_drain = Application.get_env(:fathom, :command_drain_ms)
    # High auto-poll interval (deterministic manual dispatch) + long drain so the batch blocks,
    # giving us a live task to kill.
    Application.put_env(:fathom, :command_poll_ms, 60_000)
    Application.put_env(:fathom, :command_drain_ms, 30_000)

    on_exit(fn ->
      restore(:command_poll_ms, prev_poll)
      restore(:command_drain_ms, prev_drain)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          s <- ["", "-wal", "-shm"] do
        File.rm(Path.join([dir, "#{drain_shard}.db"]) <> s)
      end
    end)

    {:ok, _} = Overrides.pin(drain_shard, node, reason: "test")
    # Hold a connection so the drain BLOCKS (busy) — keeping the batch task alive to kill.
    {:ok, cpid, cref, _} = Shards.checkout(drain_shard)
    {:ok, drain_cmd} = Commands.issue(drain_shard, node, "drain")

    poller = start_supervised!(CommandPoller)

    # Timer path: dispatch the drain as a detached batch; it blocks on the held connection.
    send(poller, :poll)
    assert MapSet.member?(:sys.get_state(poller).in_flight, drain_cmd.id)

    # Kill every TaskSupervisor child with an exit SIGNAL (the batch task included) — a
    # TaskSupervisor restart. try/after does NOT run, so no {:batch_done} is ever sent.
    children = Task.Supervisor.children(Fathom.Rebalancer.TaskSupervisor)
    assert children != [], "the blocked batch task must be alive to kill"
    for pid <- children, do: Process.exit(pid, :kill)

    # Pre-fix: the id stayed in in_flight forever. Now the monitor's DOWN frees it.
    wait_until(fn -> not MapSet.member?(:sys.get_state(poller).in_flight, drain_cmd.id) end)
    assert Process.alive?(poller), "the poller must survive the batch task's death"

    # The command never completed (complete/3 never ran) — still pending, now re-selectable.
    assert Commands.get(drain_cmd.id).status == "pending"

    # Prove re-selection: release the connection so the re-drain succeeds, then poll again.
    down = Process.monitor(cpid)
    Fathom.Shard.checkin(cpid, cref)
    assert CommandPoller.poll_now() == 1, "the freed command is re-selected on the next poll"
    assert Commands.get(drain_cmd.id).status == "done"
    assert_receive {:DOWN, ^down, :process, ^cpid, _}, 8_000
  end
end
