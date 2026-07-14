defmodule FathomWeb.AdminLiveTest do
  @moduledoc "The admin dashboard: BasicAuth gate, mount, the realtime PubSub update path, /metrics."
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.MetricsCollector
  alias Fathom.Directory

  # config/test.exs sets admin_auth to admin/secret.
  @auth "Basic " <> Base.encode64("admin:secret")

  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  # A synthetic collector broadcast matching MetricsCollector's payload shape.
  defp metrics(overrides) do
    Map.merge(
      %{
        node_key: Fathom.Rebalancer.node_key(),
        at_ms: 1_700_000_000_000,
        open_shards: 3,
        memory_bytes: 128_000_000,
        node_qps: 4242.0,
        query_p50_ms: 1.0,
        query_p95_ms: 5.0,
        query_p99_ms: 9.0,
        cold_open_p50_ms: 24.0,
        dirty_shards: 2,
        oldest_rpo_ms: 1500.0,
        s3_ops_per_s: %{"get" => 3.0, "put" => 1.0},
        s3_bytes_per_s: %{"get" => 1024.0},
        checkout_per_s: %{"ok" => 10.0},
        storage_objects: 7,
        storage_bytes: 4096,
        hot_shards: [
          %{
            shard_id: "acme",
            q_per_s: 99.0,
            rows_read_per_s: 5.0,
            rows_written_per_s: 1.0,
            queries: 500,
            p50_ms: 1.5,
            p95_ms: 7.0,
            p99_ms: 42.0
          }
        ]
      },
      overrides
    )
  end

  describe "auth" do
    test "GET /admin without credentials is challenged (401)", %{conn: conn} do
      conn = get(conn, "/admin")
      assert conn.status == 401
    end

    test "GET /admin/metrics without credentials is challenged (401)", %{conn: conn} do
      conn = get(conn, "/admin/metrics")
      assert conn.status == 401
    end

    test "GET /admin/metrics with credentials returns text/plain 200", %{conn: conn} do
      conn = conn |> auth() |> get("/admin/metrics")
      assert conn.status == 200
      assert get_resp_header(conn, "content-type") |> hd() =~ "text/plain"
    end

    test "fails closed with 503 when no credentials are configured", %{conn: conn} do
      prev = Application.get_env(:fathom, :admin_auth)
      Application.delete_env(:fathom, :admin_auth)
      on_exit(fn -> Application.put_env(:fathom, :admin_auth, prev) end)

      conn = get(conn, "/admin")
      assert conn.status == 503
    end
  end

  describe "overview" do
    test "mounts behind auth and renders the shell + node KPIs", %{conn: conn} do
      {:ok, _} = Directory.resolve("acme")

      {:ok, view, _html} = conn |> auth() |> live("/admin")

      assert has_element?(view, "a", "Overview")
      assert has_element?(view, "#qps-chart")
      assert has_element?(view, "#latency-chart")

      # The fleet panel loads async (Postgres) — wait for it, then assert directory data.
      html = render_async(view)
      assert html =~ "Total shards"
      assert html =~ "Storage (S3)"
    end

    test "a collector broadcast updates the live KPIs", %{conn: conn} do
      {:ok, view, _html} = conn |> auth() |> live("/admin")

      Phoenix.PubSub.broadcast(
        Fathom.PubSub,
        MetricsCollector.topic(),
        {:metrics, metrics(%{node_qps: 4242.0})}
      )

      html = render(view)
      # node QPS rendered via fmt_rate → "4,242.0", and the hot shard row appears.
      assert html =~ "4,242"
      assert html =~ "acme"
    end

    # Regression (expert review 2026-07-14 #13): mount seeded @history once from the collector
    # snapshot, but handle_info({:metrics, …}) never advanced it — so the server-rendered KPI
    # sparklines froze at mount. The SVG sparkline only renders a <polyline> once @history has ≥2
    # points, so its appearance after successive broadcasts proves @history is being folded forward.
    test "KPI sparklines advance with each collector broadcast (not frozen at mount)", %{
      conn: conn
    } do
      {:ok, view, _html} = conn |> auth() |> live("/admin")

      # Mount snapshot is empty in test (collector not running) ⇒ no history ⇒ no sparkline yet.
      refute has_element?(view, "svg polyline")

      for i <- 1..3 do
        Phoenix.PubSub.broadcast(
          Fathom.PubSub,
          MetricsCollector.topic(),
          {:metrics, metrics(%{at_ms: 1_700_000_000_000 + i * 1000, node_qps: 100.0 * i})}
        )
      end

      # @history advanced to ≥2 points ⇒ the qps + p99 sparklines now render polylines.
      assert has_element?(view, "svg polyline")
    end
  end

  describe "other pages" do
    test "shards page mounts", %{conn: conn} do
      {:ok, view, _html} = conn |> auth() |> live("/admin/shards")
      assert has_element?(view, "a", "Shards")
    end

    test "shards page shows per-shard p50/p99 latency on a collector broadcast", %{conn: conn} do
      {:ok, view, _html} = conn |> auth() |> live("/admin/shards")

      Phoenix.PubSub.broadcast(
        Fathom.PubSub,
        MetricsCollector.topic(),
        {:metrics, metrics(%{})}
      )

      html = render(view)
      # The hot-shard row carries per-shard tail latency: p99_ms 42.0 → "42 ms".
      assert html =~ "acme"
      assert html =~ "p50"
      assert html =~ "p99"
      assert html =~ "42 ms"
    end

    test "migrations page mounts and loads fleet state", %{conn: conn} do
      {:ok, view, _html} = conn |> auth() |> live("/admin/migrations")
      html = render_async(view)
      assert html =~ "Fleet HEAD"
      assert html =~ "Release history"
    end
  end
end
