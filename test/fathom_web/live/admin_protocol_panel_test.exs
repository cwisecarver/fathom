defmodule FathomWeb.AdminProtocolPanelTest do
  @moduledoc "Overview Protocol panel + S3 errors/p99 rows (Fathom.Telemetry.Protocol stats)."
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.MetricsCollector

  @auth "Basic " <> Base.encode64("admin:secret")

  defp metrics(extra) do
    Map.merge(
      %{
        node_key: Fathom.Rebalancer.node_key(),
        at_ms: 1_700_000_000_000,
        open_shards: 1,
        memory_bytes: 1,
        node_qps: 0.0,
        query_p50_ms: 0.0,
        query_p95_ms: 0.0,
        query_p99_ms: 0.0,
        dirty_shards: 0,
        oldest_rpo_ms: 0.0,
        s3_ops_per_s: %{},
        s3_bytes_per_s: %{},
        checkout_per_s: %{},
        storage_objects: 0,
        storage_bytes: 0,
        hot_shards: []
      },
      extra
    )
  end

  defp open(conn) do
    {:ok, view, _} = conn |> Plug.Conn.put_req_header("authorization", @auth) |> live("/admin")
    view
  end

  defp send_metrics(view, m) do
    Phoenix.PubSub.broadcast(Fathom.PubSub, MetricsCollector.topic(), {:metrics, m})
    render(view)
  end

  test "renders the protocol breakdown and S3 error rows", %{conn: conn} do
    view = open(conn)

    send_metrics(
      view,
      metrics(%{
        protocol: %{
          http: %{rate: %{"2xx" => 12.5, "4xx" => 0.0, "5xx" => 1.0}, p99_ms: 25.0},
          ws: %{opens_per_s: 1.0, closes_per_s: 1.0, errors_per_s: 0.0, frames_per_s: 40.0},
          s3: %{errors_per_s: 0.5, p99_ms: 100.0}
        }
      })
    )

    assert has_element?(view, "#protocol-panel", "HTTP p99")
    assert has_element?(view, "#protocol-panel", "WS frames in")
    assert has_element?(view, "dl", "S3 errors / s")
  end

  test "empty state: metrics without :protocol render zeros, not a crash", %{conn: conn} do
    view = open(conn)
    send_metrics(view, metrics(%{}))
    assert has_element?(view, "#protocol-panel")
  end
end
