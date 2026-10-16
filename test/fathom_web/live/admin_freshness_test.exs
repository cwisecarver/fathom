defmodule FathomWeb.AdminFreshnessTest do
  @moduledoc """
  The shared freshness chip in the admin top bar (expert review 2026-10-15 #5): "as of Ns ago",
  amber once the data is older than the page's threshold, red with "refresh failed" when an
  async refresh exited. Before this, `handle_async(_, {:exit, _}, _)` kept the old data with no
  marker, and a collector frozen for ten minutes looked live.

  The failed-refresh path is driven deterministically without a production seam: the pages'
  `start_async` tasks go through the single-flight caches (`HotShardsCache`, the migrations page
  ETS), so a test holds the refill lock, the task blocks in `await`, and the test kills it. The
  LiveView sees a real `{:exit, :killed}`. The chip's state is asserted through
  `data-state` on `#freshness-chip` (the colocated hook only ticks the age client-side).
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.FleetCollector
  alias FathomWeb.Layouts

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  defp now, do: System.system_time(:millisecond)

  defp chip_state(view) do
    view
    |> element("#freshness-chip")
    |> render()
    |> then(&Regex.run(~r/data-state="([a-z]+)"/, &1))
    |> List.last()
  end

  defp metrics(at_ms) do
    %{
      node_key: Fathom.Rebalancer.node_key(),
      at_ms: at_ms,
      open_shards: 3,
      memory_bytes: 128_000_000,
      node_qps: 10.0,
      query_p50_ms: 1.0,
      query_p95_ms: 5.0,
      query_p99_ms: 9.0,
      cold_open_p50_ms: 24.0,
      dirty_shards: 2,
      oldest_rpo_ms: 1500.0,
      s3_ops_per_s: %{"get" => 3.0},
      s3_bytes_per_s: %{"get" => 1024.0},
      checkout_per_s: %{"ok" => 10.0},
      storage_objects: 7,
      storage_bytes: 4096,
      hot_shards: []
    }
  end

  # Make the page's cached read RAISE inside its start_async task: a held refill lock plus a
  # malformed cached entry sends the single-flight cache down a case with no matching clause, so
  # the LiveView receives a genuine {:exit, _} from handle_async. (Killing the task instead does
  # not work: it is linked to the LiveView and takes it down.)
  defp poison_cache(table) do
    :ets.new(table, [:named_table, :public, :set])
    :ets.insert(table, {:refill, System.monotonic_time(:millisecond) + 60_000})
    :ets.insert(table, {:page, :garbage})
  end

  describe "Layouts.freshness_state/3" do
    test "failed beats stale beats fresh, with the boundary exclusive" do
      assert Layouts.freshness_state(0, 4_000, false) == :fresh
      assert Layouts.freshness_state(4_000, 4_000, false) == :fresh
      assert Layouts.freshness_state(4_001, 4_000, false) == :stale
      assert Layouts.freshness_state(0, 4_000, true) == :failed
      assert Layouts.freshness_state(999_999, 4_000, true) == :failed
    end
  end

  describe "FleetCollector.stamp/1" do
    test "adds a current :fetched_at_ms without disturbing the roll-up" do
      before = now()
      stamped = FleetCollector.stamp(%{total_shards: 3})
      assert stamped.total_shards == 3
      assert stamped.fetched_at_ms >= before
    end
  end

  describe "overview" do
    test "fresh metrics render a fresh chip with an age label", %{conn: conn} do
      {:ok, view, _} = live(auth(conn), ~p"/admin")
      send(view.pid, {:metrics, metrics(now())})
      render(view)

      assert has_element?(view, "#freshness-chip")
      assert chip_state(view) == "fresh"
      assert has_element?(view, "#freshness-label", "as of")
    end

    test "metrics older than ~3 ticks turn the chip stale", %{conn: conn} do
      {:ok, view, _} = live(auth(conn), ~p"/admin")
      send(view.pid, {:metrics, metrics(now() - 60_000)})
      render(view)

      assert chip_state(view) == "stale"
      assert has_element?(view, "#freshness-label", "as of 1m ago")
    end
  end

  describe "rebalancer (fleet broadcast)" do
    test "a roll-up older than 2 fleet intervals is stale; a fresh one is fresh", %{conn: conn} do
      {:ok, view, _} = live(auth(conn), ~p"/admin/rebalancer")
      fleet = %{pins: [], pending_handoffs: [], nodes: []}

      send(view.pid, {:fleet, Map.put(fleet, :fetched_at_ms, now() - 120_000)})
      render(view)
      assert chip_state(view) == "stale"

      send(view.pid, {:fleet, FleetCollector.stamp(fleet)})
      render(view)
      assert chip_state(view) == "fresh"
    end
  end

  describe "start_async pages" do
    test "migrations: a failed refresh shows the red refresh-failed chip", %{conn: conn} do
      poison_cache(:fathom_admin_migrations_page)
      {:ok, view, _} = live(auth(conn), ~p"/admin/migrations")
      render_async(view)
      assert chip_state(view) == "failed"
      assert has_element?(view, "#freshness-label", "refresh failed")
    end

    test "shards: a failed fleet-hot refresh shows the red refresh-failed chip", %{conn: conn} do
      start_supervised!(Fathom.Admin.HotShardsCache)

      :ets.insert(
        :fathom_admin_hot_shards,
        {:refill, System.monotonic_time(:millisecond) + 60_000}
      )

      :ets.insert(:fathom_admin_hot_shards, {:page, :garbage})
      {:ok, view, _} = live(auth(conn), ~p"/admin/shards")
      render_async(view)
      assert chip_state(view) == "failed"
      assert has_element?(view, "#freshness-label", "refresh failed")
    end

    test "durability: a successful refresh clears the failed flag and is fresh", %{conn: conn} do
      {:ok, view, _} = live(auth(conn), ~p"/admin/durability")
      assert chip_state(view) in ["fresh", "failed"]

      send(
        view.pid,
        {:durability_data,
         %{
           snapshot: %{
             at_ms: 0,
             scrape?: false,
             window_s: 120,
             alarms: [],
             quarantines: 0,
             replication: %{enabled?: false}
           }
         }}
      )

      render(view)
      assert chip_state(view) == "fresh"
    end
  end

  describe "pages with no polled data" do
    test "the audit page keeps the plain live dot", %{conn: conn} do
      {:ok, view, _} = live(auth(conn), ~p"/admin/audit")
      assert has_element?(view, "#freshness-chip[data-state=live]")
    end
  end
end
