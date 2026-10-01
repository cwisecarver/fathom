defmodule Fathom.Admin.FleetCollectorTest do
  @moduledoc """
  Expert review 2026-07-24 #13: the fleet roll-up is polled once per NODE and broadcast, so the
  control-plane cost is independent of how many dashboard tabs are open. Previously every
  `AdminOverviewLive` ran its own `start_async` on its own 5 s timer, so the directory-scale reads
  behind `Fleet.overview/0` multiplied by viewers — worst during an incident, when several
  operators have the dashboard up and the control plane can least absorb it.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Admin.FleetCollector
  alias Fathom.Directory

  setup do
    # The collector is gated off in test (:metrics_collector false), so start it explicitly and
    # let it see this test's sandboxed connection — its poll runs in a Task.
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, {:shared, self()})
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual) end)
    :ok
  end

  test "polls once and broadcasts the roll-up to every subscriber" do
    {:ok, _} = Directory.resolve("acme")

    Phoenix.PubSub.subscribe(Fathom.PubSub, FleetCollector.topic())
    start_supervised!(FleetCollector)
    FleetCollector.watch()

    assert_receive {:fleet, overview}, 5_000
    assert overview.total_shards >= 1
    assert is_map(overview.by_status)

    # Every subscriber gets the SAME broadcast — that is the O(1)-per-node property. A second
    # subscriber must not cause a second poll; it just receives the next tick's broadcast.
    assert FleetCollector.snapshot() == overview
  end

  # Expert review 2026-10-01 perf #20. `Fleet.overview/0` is a GROUP BY over the whole `shards`
  # table, and the collector used to run it every interval on every node whether or not anyone
  # was looking. It now polls only while a viewer is registered.
  describe "polls only while watched (expert review 2026-10-01 perf #20)" do
    setup do
      prev = Application.get_env(:fathom, :admin_fleet_refresh_ms)
      Application.put_env(:fathom, :admin_fleet_refresh_ms, 30)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, :admin_fleet_refresh_ms),
          else: Application.put_env(:fathom, :admin_fleet_refresh_ms, prev)
      end)

      Phoenix.PubSub.subscribe(Fathom.PubSub, FleetCollector.topic())
      :ok
    end

    test "with no viewer it never queries the directory" do
      start_supervised!(FleetCollector)

      # Ten refresh intervals with nobody watching: the old collector polled and broadcast on
      # every one of them.
      refute_receive {:fleet, _}, 300
      assert FleetCollector.snapshot() == nil
    end

    test "a viewer starts the polling, and it stops when the last viewer goes" do
      start_supervised!(FleetCollector)

      viewer = spawn(fn -> receive do: (:stop -> :ok) end)
      FleetCollector.watch(viewer)

      # Polled immediately on the first watcher, then on the interval.
      assert_receive {:fleet, _}, 2_000
      assert_receive {:fleet, _}, 2_000

      ref = Process.monitor(viewer)
      send(viewer, :stop)
      assert_receive {:DOWN, ^ref, :process, ^viewer, _}

      # Wait (synchronously, no sleep) until the collector has seen the viewer go and has no poll
      # in flight; from then on no new poll can start. Drop whatever landed before that.
      assert await_idle(1_000), "the collector never went idle after its last viewer left"
      flush_fleet()
      refute_receive {:fleet, _}, 300
    end
  end

  defp await_idle(0), do: false

  defp await_idle(n) do
    s = :sys.get_state(FleetCollector)
    if s.task == nil and map_size(s.watchers) == 0, do: true, else: await_idle(n - 1)
  end

  defp flush_fleet do
    receive do
      {:fleet, _} -> flush_fleet()
    after
      0 -> :ok
    end
  end

  test "snapshot/0 returns nil when the collector isn't running" do
    # The dashboard's mount falls back to a single direct load in this case rather than rendering
    # permanently-empty panels — see AdminOverviewLive.initial_fleet/1.
    refute Process.whereis(FleetCollector)
    assert FleetCollector.snapshot() == nil
  end
end
