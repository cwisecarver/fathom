defmodule FathomWeb.AdminClusterLiveTest do
  @moduledoc """
  The Cluster page (expert review 2026-10-15 #9, #21): BasicAuth gate, the Lease-health panel in
  both liveness modes, and the recent-lease-events table (empty and populated states).
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.LeaseEvents
  alias Fathom.Shard.Heartbeat

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  setup do
    LeaseEvents.clear()
    :ok
  end

  test "GET /admin/cluster without credentials is challenged (401)", %{conn: conn} do
    assert get(conn, "/admin/cluster").status == 401
  end

  test "legacy mode: renders lease health and the empty events state", %{conn: conn} do
    refute Heartbeat.running?()
    {:ok, view, _html} = conn |> auth() |> live("/admin/cluster")

    assert has_element?(view, "#lease-health")
    assert has_element?(view, "#lease-mode", "legacy")
    assert has_element?(view, "#lease-state", "n/a")
    assert has_element?(view, "#lease-events-empty")
    refute has_element?(view, "#lease-events")
  end

  test "heartbeat mode: renders valid state and generation", %{conn: conn} do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    {:ok, view, _html} = conn |> auth() |> live("/admin/cluster")

    assert has_element?(view, "#lease-mode", "heartbeat")
    assert has_element?(view, "#lease-generation")
    assert has_element?(view, "#lease-deadline")
  end

  test "heartbeat mode: a past deadline renders as lapsed", %{conn: conn} do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    state = :sys.get_state(hb)
    Heartbeat.publish_status(%{state | mono_deadline_ms: System.monotonic_time(:millisecond) - 1})

    {:ok, view, _html} = conn |> auth() |> live("/admin/cluster")
    assert has_element?(view, "#lease-state", "lapsed")
  end

  test "recorded lease events appear in the table, refreshed on the tick", %{conn: conn} do
    {:ok, view, _html} = conn |> auth() |> live("/admin/cluster")
    assert has_element?(view, "#lease-events-empty")

    LeaseEvents.record(:superseded, "ui_cluster_shard", "")
    send(view.pid, :refresh)

    assert has_element?(view, "#lease-events")
    assert has_element?(view, "#lease-events td", "ui_cluster_shard")
  end
end
