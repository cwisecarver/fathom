defmodule Fathom.ShardSupervisorIntensityTest do
  # Finding #16: ShardSupervisor ran the OTP-default 3-restarts/5s budget. A burst of co-resident
  # child crashes in one window terminated the supervisor and killed every co-resident shard,
  # risking a DataPlane -> top cascade. The budget is now sized to the fan-out. Pin the invariant:
  # a supervisor built from shard_supervisor_opts/0 survives well past 3 rapid child restarts — a
  # default-intensity supervisor would be dead after the 4th. (The shard coordinators themselves
  # are now restart: :temporary — finding #8 — so their crashes never restart or consume this
  # budget at all; the raised budget stays as defense-in-depth for any restartable child.)
  use ExUnit.Case, async: true

  test "shard supervisor opts raise the restart budget above the OTP default" do
    opts = Fathom.Application.shard_supervisor_opts()
    assert Keyword.fetch!(opts, :strategy) == :one_for_one
    assert Keyword.fetch!(opts, :max_restarts) > 3
    assert Keyword.fetch!(opts, :max_seconds) >= 5
  end

  test "a supervisor built from those opts survives more than 3 rapid child restarts" do
    opts =
      Fathom.Application.shard_supervisor_opts()
      |> Keyword.put(:name, :"shard_sup_intensity_#{System.unique_integer([:positive])}")

    sup =
      start_supervised!(%{id: :intensity_sup, start: {DynamicSupervisor, :start_link, [opts]}})

    {:ok, _} =
      DynamicSupervisor.start_child(sup, %{
        id: :crasher,
        start: {Agent, :start_link, [fn -> :ok end]},
        restart: :permanent
      })

    # Crash the child 6 times (> the default max_restarts of 3), each synchronized on the
    # child's DOWN so there is no sleep and every crash lands in one intensity window. A
    # default-intensity supervisor would terminate on the 4th crash; ours must still be alive.
    for _ <- 1..6 do
      [{_, pid, _, _}] = DynamicSupervisor.which_children(sup)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
      await_restart(sup, pid)
    end

    assert Process.alive?(sup)
  end

  # Wait (event-driven, no sleep) for the supervisor to bring up a fresh child pid after a
  # kill. which_children settles a poll or two after the restart; if the supervisor itself
  # died (the pre-fix failure), which_children raises and the test fails.
  defp await_restart(sup, old_pid) do
    case DynamicSupervisor.which_children(sup) do
      [{_, pid, _, _}] when is_pid(pid) and pid != old_pid -> :ok
      _ -> await_restart(sup, old_pid)
    end
  end
end
