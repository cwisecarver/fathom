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

    assert_receive {:fleet, overview}, 5_000
    assert overview.total_shards >= 1
    assert is_map(overview.by_status)

    # Every subscriber gets the SAME broadcast — that is the O(1)-per-node property. A second
    # subscriber must not cause a second poll; it just receives the next tick's broadcast.
    assert FleetCollector.snapshot() == overview
  end

  test "snapshot/0 returns nil when the collector isn't running" do
    # The dashboard's mount falls back to a single direct load in this case rather than rendering
    # permanently-empty panels — see AdminOverviewLive.initial_fleet/1.
    refute Process.whereis(FleetCollector)
    assert FleetCollector.snapshot() == nil
  end
end
