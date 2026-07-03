defmodule Fathom.ApplicationTest do
  # The supervision tree is partitioned into plane sub-supervisors so a control-plane
  # restart-storm can't take the data plane down with it. async: false — it kills a
  # shared supervised process.
  use ExUnit.Case, async: false

  @planes [
    Fathom.Infra.Supervisor,
    Fathom.ControlPlane.Supervisor,
    Fathom.DataPlane.Supervisor,
    Fathom.Edge.Supervisor
  ]

  test "the tree boots with the four plane sub-supervisors running" do
    for name <- @planes do
      assert is_pid(Process.whereis(name)), "#{inspect(name)} should be running"
    end
  end

  test "key children live under the expected plane" do
    assert_child_of(Fathom.DataPlane.Supervisor, Fathom.ShardLoad)
    assert_child_of(Fathom.ControlPlane.Supervisor, Fathom.Directory.Recorder)
    assert_child_of(Fathom.Infra.Supervisor, Fathom.Repo)
  end

  test "a control-plane child crash does not restart a data-plane child (plane isolation)" do
    shard_load = Process.whereis(Fathom.ShardLoad)
    assert is_pid(shard_load)
    ref = Process.monitor(shard_load)

    # Kill a control-plane child. It restarts within the control-plane subtree; the
    # data plane must be entirely untouched.
    recorder = Process.whereis(Fathom.Directory.Recorder)
    assert is_pid(recorder)
    rref = Process.monitor(recorder)
    Process.exit(recorder, :kill)
    assert_receive {:DOWN, ^rref, :process, ^recorder, :killed}

    # No DOWN for the data-plane child, and its pid is unchanged: the crash did not
    # cross the plane boundary.
    refute_receive {:DOWN, ^ref, :process, ^shard_load, _}, 200
    assert Process.whereis(Fathom.ShardLoad) == shard_load

    # And the control plane self-heals under its own supervisor (wait for the restart so
    # we don't hand the next test a dead child; a start has no DOWN to await).
    assert wait_restarted(Fathom.Directory.Recorder, recorder)
  end

  # Expert review #4: supervisors stop children in reverse start order. If the heartbeat
  # stops (deleting its stored liveness object) before the shard supervisor, every
  # coordinator still running its terminate-flush on a stopping node is judged dead and
  # stealable — a survivor steals the lock, the dying coordinator's fenced flush
  # self-fences, and up to a flush interval of acknowledged writes is dropped on a routine
  # deploy. The violated invariant: the heartbeat must outlive the coordinators' final
  # flushes, i.e. start before the shard Registry/DynamicSupervisor.
  test "the heartbeat starts before the shard supervisor so it stops after the coordinators" do
    prev = Application.get_env(:fathom, :heartbeat_server)
    Application.put_env(:fathom, :heartbeat_server, true)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :heartbeat_server),
        else: Application.put_env(:fathom, :heartbeat_server, prev)
    end)

    ids =
      for spec <- Fathom.Application.data_plane_children(),
          do: Supervisor.child_spec(spec, []).id

    heartbeat = Enum.find_index(ids, &(&1 == Fathom.Shard.Heartbeat))
    shard_sup = Enum.find_index(ids, &(&1 == Fathom.ShardSupervisor))

    assert is_integer(heartbeat), "heartbeat child missing with :heartbeat_server on"
    assert is_integer(shard_sup), "shard DynamicSupervisor child missing"

    assert heartbeat < shard_sup,
           "the heartbeat must start before (stop after) the shard supervisor, " <>
             "or a clean shutdown makes every flushing shard stealable"
  end

  defp assert_child_of(supervisor, name) do
    pid = Process.whereis(name)
    assert is_pid(pid), "#{inspect(name)} should be running"

    pids =
      for {_id, child, _type, _mods} <- Supervisor.which_children(supervisor),
          is_pid(child),
          do: child

    assert pid in pids, "#{inspect(name)} should be a child of #{inspect(supervisor)}"
  end

  defp wait_restarted(name, old, tries \\ 200) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != old -> true
      _ when tries > 0 -> Process.sleep(5) && wait_restarted(name, old, tries - 1)
      _ -> false
    end
  end
end
