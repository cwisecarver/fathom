defmodule FathomWeb.AdminLiveTest do
  @moduledoc "The admin dashboard: BasicAuth gate, mount, the realtime PubSub update path, /metrics."
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.MetricsCollector
  alias Fathom.Directory
  alias Fathom.Migrator

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
      # An explicit timeout: `render_async/1` defaults to 100ms, which is not a bound this test
      # chose to assert, it is an accidental latency assertion smuggled into a functional test.
      # A Postgres round-trip on a busy machine exceeds it easily. What is under test is that the
      # async assign COMPLETES and renders, not how fast. Matches admin_query_live_test.exs.
      html = render_async(view, 5_000)
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
      # Explicit timeout for the same reason as the overview panel above — this was the most
      # frequent load-induced failure in the suite (6 of 7 full runs at load average 30-60 on
      # 2026-07-26), purely because render_async/1's default is 100ms.
      html = render_async(view, 5_000)
      assert html =~ "Fleet HEAD"
      assert html =~ "Release history"
    end
  end

  # Expert review 2026-08-01 #26, dashboard half. The API half (`/api/migrations/status`) already
  # reports `review_blocks`; this is the surface the human watching a frozen burndown actually
  # looks at. Before this the page rendered "Fleet HEAD v1" with no hint that v2 was held, so the
  # operator saw laggards that never converged and nothing explaining why.
  describe "migrations page — review blocks" do
    defp migrations_html(conn) do
      {:ok, view, _html} = conn |> auth() |> live("/admin/migrations")

      # Same 5s render_async timeout as the mount test above (load-induced flake, not a real wait).
      {view, render_async(view, 5_000)}
    end

    test "no held versions ⇒ no review panel, and the tile reads zero", %{conn: conn} do
      {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app (id integer)"])

      {_view, html} = migrations_html(conn)

      assert html =~ "Held for review"
      refute html =~ "Rollout held"
      refute html =~ "attach_transform"
    end

    test "a held data migration shows why it is held and BOTH options", %{conn: conn} do
      {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app (id integer)"])

      {:ok, _} =
        Migrator.release(2, "0002_backfill", ["UPDATE app SET tier = 'gold'"], nil, true)

      Migrator.set_review_reason(2, "data_migration", %{
        "statements" => ["UPDATE app SET tier = 'gold'"]
      })

      {_view, html} = migrations_html(conn)

      # HEAD is capped below the held version — the state the panel exists to explain.
      assert html =~ "v1"
      assert html =~ "Rollout held"
      assert html =~ "0002_backfill"
      assert html =~ "data_migration"

      # The statement that tripped the flag, verbatim. "which statements" is the first thing an
      # operator needs and the one thing the old `pending_review: [2]` could never tell them.
      assert html =~ "UPDATE app SET tier"

      # Both options, each with its runnable command. The panel is the decision, spelled out.
      assert html =~ "attach_transform"
      assert html =~ "Fathom.Migrator.attach_transform"
      assert html =~ "approve_review"
      assert html =~ "Fathom.Migrator.approve_review"
    end

    # A gap means the fleet is MISSING DDL, so `approve_review` is never the right move — advancing
    # HEAD past it hides a real template/fleet divergence. `Migrator.options_for/1` already refuses
    # to offer it; this pins that the panel does not reintroduce it in the rendering.
    test "a migration gap offers reconcile ONLY — never approve", %{conn: conn} do
      {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app (id integer)"])

      {:ok, _} =
        Migrator.release(2, "0002_nonatomic", ["ALTER TABLE app ADD COLUMN t text"], nil, true)

      Migrator.set_review_reason(2, "migration_gap", %{"gap" => "%{before: 9, last: 7}"})

      {_view, html} = migrations_html(conn)

      assert html =~ "Rollout held"
      assert html =~ "migration_gap"
      assert html =~ "reconcile_template"
      assert html =~ "atomic = False"
      # The gap detail itself, so the operator can see how far the template ran ahead.
      assert html =~ "before: 9"

      refute html =~ "approve_review"
      refute html =~ "attach_transform"
    end

    # The upgrade path: releases captured BEFORE #26 added the reason columns carry
    # `review_reason: nil`. `Migrator.review_block/1` re-derives the reason from the statements, so
    # an already-frozen fleet gets the explanation the moment it upgrades — not only versions
    # captured from here on. Without that, the operator whose fleet is ALREADY stuck (the one who
    # most needs this panel) would still see a bare version number.
    test "a release flagged before #26 (no stored reason) still renders a legible block", %{
      conn: conn
    } do
      {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app (id integer)"])
      # No set_review_reason/3 call — review_reason stays nil, exactly as a pre-#26 row.
      {:ok, _} = Migrator.release(2, "0002_legacy", ["INSERT INTO app VALUES (1)"], nil, true)

      {_view, html} = migrations_html(conn)

      assert html =~ "Rollout held"
      assert html =~ "data_migration"
      assert html =~ "INSERT INTO app VALUES (1)"
      assert html =~ "Fathom.Migrator.approve_review"
    end

    # The panel is deliberately READ-ONLY (see the LiveView's moduledoc). `attach_transform` cannot
    # be a button at all, and `approve_review` replays the template's literal rows onto every
    # tenant — offering only the dangerous half of the decision would bias an operator toward it.
    # This fails the moment someone adds a click handler here, which is the point: the reasoning
    # lives in the moduledoc and this is the tripwire that sends them to read it.
    test "the review panel exposes no clickable action", %{conn: conn} do
      {:ok, _} = Migrator.release(1, "v1", ["CREATE TABLE app (id integer)"])
      {:ok, _} = Migrator.release(2, "0002_backfill", ["UPDATE app SET tier = 'gold'"], nil, true)

      {view, html} = migrations_html(conn)

      assert html =~ "Rollout held"
      refute has_element?(view, "#review-blocks [phx-click]")
    end
  end
end
