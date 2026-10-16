defmodule FathomWeb.AdminRebalancerLiveTest do
  @moduledoc "The /admin/rebalancer page: pins, pending handoffs, decision ring, last LB apply."
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Rebalancer.DecisionLog

  @auth "Basic " <> Base.encode64("admin:secret")

  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  defp broadcast_fleet(extra) do
    fleet = Map.merge(Fathom.Admin.Fleet.overview(), extra)

    Phoenix.PubSub.broadcast(
      Fathom.PubSub,
      Fathom.Admin.FleetCollector.topic(),
      {:fleet, fleet}
    )
  end

  test "requires admin auth", %{conn: conn} do
    assert get(conn, "/admin/rebalancer").status == 401
  end

  test "empty state: no pins, no handoffs, no decisions, no LB apply", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/rebalancer")
    broadcast_fleet(%{pins: [], pending_handoffs: []})

    assert has_element?(view, "#pins-empty")
    assert has_element?(view, "#handoffs-empty")
    assert has_element?(view, "#decisions-scope")
  end

  test "renders pins (live / stale / failed) and pending handoffs with a stale badge", %{
    conn: conn
  } do
    {:ok, view, _} = conn |> auth() |> live("/admin/rebalancer")
    now = DateTime.utc_now()
    old = DateTime.add(now, -3600, :second)

    broadcast_fleet(%{
      nodes: [%{node_key: "n1", last_seen_at: now, q_p99: 1.0, sample_count: 1, alive: true}],
      pins: [
        %{
          shard_id: "live-1",
          pinned_node: "n1",
          failed_at: nil,
          updated_at: now,
          inserted_at: now
        },
        %{
          shard_id: "gone-1",
          pinned_node: "dead",
          failed_at: nil,
          updated_at: old,
          inserted_at: old
        },
        %{
          shard_id: "fail-1",
          pinned_node: "n1",
          failed_at: now,
          updated_at: now,
          inserted_at: now
        }
      ],
      pending_handoffs: [
        %{id: 1, shard_id: "live-1", command: "warm", node: "n1", inserted_at: now},
        %{id: 2, shard_id: "gone-1", command: "drain", node: "dead", inserted_at: old}
      ]
    })

    assert has_element?(view, "#pin-live-1", "live")
    assert has_element?(view, "#pin-gone-1", "stale")
    assert has_element?(view, "#pin-fail-1", "failed")
    assert has_element?(view, "#handoff-1", "pending")
    assert has_element?(view, "#handoff-2", "stale")
  end

  test "shows recorded decisions with their reasons and declining guards, and the LB apply", %{
    conn: conn
  } do
    DecisionLog.record_tick(
      [
        %{
          shard_id: "moved-1",
          from_node: "n1",
          to_node: "n2",
          q_per_s: 900.0,
          threshold: 500.0,
          reason: "hot 900.0 q/s ≥ 500.0 q/s"
        }
      ],
      [%{shard_id: "held-1", node_key: "n1", q_per_s: 800.0, guard: :cooldown}]
    )

    DecisionLog.record_lb_apply(:reload_failed,
      error: "{:reload_failed, 1}",
      hash: "deadbeefcafe0000",
      entries: 4
    )

    {:ok, view, _} = conn |> auth() |> live("/admin/rebalancer")
    [tick | _] = DecisionLog.recent()

    assert has_element?(view, "#tick-#{tick.seq}", "moved-1")
    assert has_element?(view, "#tick-#{tick.seq}", "hot 900.0 q/s")
    assert has_element?(view, "#tick-#{tick.seq}", "held-1")
    assert has_element?(view, "#tick-#{tick.seq}", "cooling down")
    assert has_element?(view, "#lb-apply", "reload_failed")
    assert has_element?(view, "#lb-apply", "deadbeefcafe")
    assert has_element?(view, "#lb-apply-error", "reload_failed")
  end
end
