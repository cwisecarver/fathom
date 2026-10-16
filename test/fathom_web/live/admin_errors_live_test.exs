defmodule FathomWeb.AdminErrorsLiveTest do
  @moduledoc """
  Overview Errors / Data-plane auth panels, the RPO under-reporting badge and the Cluster page's
  Events / 5 min strip (admin-UI round 2), driven by `{:metrics, map}` broadcasts and asserted by
  DOM id and `data-state`.
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.MetricsCollector

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  defp base(overrides) do
    Map.merge(
      %{
        node_key: Fathom.Rebalancer.node_key(),
        at_ms: 1_700_000_000_000,
        open_shards: 3,
        memory_bytes: 128_000_000,
        node_qps: 10.0,
        query_p50_ms: 1.0,
        query_p95_ms: 5.0,
        query_p99_ms: 9.0,
        cold_open_p50_ms: 0.0,
        cold_open_p95_ms: 0.0,
        cold_open_p99_ms: 0.0,
        cold_open_n: 0,
        cold_open_by_warm: %{},
        dirty_shards: 0,
        oldest_rpo_ms: 0.0,
        s3_ops_per_s: %{},
        s3_bytes_per_s: %{},
        checkout_per_s: %{},
        storage_objects: 0,
        storage_bytes: 0,
        hot_shards: []
      },
      overrides
    )
  end

  defp group(rates, window), do: %{rates: rates, window: window}

  defp errors(rates, window) do
    %{
      rates: rates,
      window: window,
      total_rate: rates |> Map.values() |> Enum.sum(),
      query_rate: 10.0,
      ratio: 0.05
    }
  end

  defp send_metrics(view, m) do
    Phoenix.PubSub.broadcast(Fathom.PubSub, MetricsCollector.topic(), {:metrics, m})
    render(view)
  end

  describe "Overview Errors panel" do
    test "clean: renders ok with zero rates", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{errors: errors(%{"busy" => 0.0}, %{}), no_quorum: group(%{}, %{})})
      )

      assert has_element?(view, "#errors-panel[data-state=ok]")
      assert has_element?(view, "#errors-ratio", "5")
      assert has_element?(view, "#errors-class-timeout[data-state=ok]")
      assert has_element?(view, "#errors-no-quorum", "none seen")
    end

    test "timeout in the window turns the panel red", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{
          errors: errors(%{"timeout" => 0.0}, %{"timeout" => 2}),
          no_quorum: group(%{}, %{})
        })
      )

      assert has_element?(view, "#errors-panel[data-state=error]")
      assert has_element?(view, "#errors-class-timeout[data-state=error]")
    end

    test "no-quorum above zero turns the panel red and lists the reason", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{
          errors: errors(%{}, %{}),
          no_quorum: group(%{"followers_down" => 0.5}, %{"followers_down" => 3})
        })
      )

      assert has_element?(view, "#errors-panel[data-state=error]")
      assert has_element?(view, "#errors-nq-followers_down[data-state=error]")
    end

    test "a non-timeout error is a warning, not red", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{
          errors: errors(%{"sql_error" => 1.0}, %{"sql_error" => 5}),
          no_quorum: group(%{}, %{})
        })
      )

      assert has_element?(view, "#errors-panel[data-state=warn]")
      assert has_element?(view, "#errors-class-sql_error[data-state=warn]")
    end

    test "before the first tick the panels render idle, not crash", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      assert has_element?(view, "#errors-panel[data-state=idle]")
      assert has_element?(view, "#auth-panel[data-state=ok]")
    end
  end

  describe "Overview Data-plane auth panel" do
    test "wrong_shard and revoked are highlighted; expired is not", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{
          auth_rejected:
            group(
              %{"wrong_shard" => 0.2, "expired" => 1.0},
              %{"wrong_shard" => 1, "expired" => 9}
            )
        })
      )

      assert has_element?(view, "#auth-panel[data-state=error]")
      assert has_element?(view, "#auth-reason-wrong_shard[data-state=error]")
      assert has_element?(view, "#auth-reason-expired[data-state=warn]")
      assert has_element?(view, "#auth-reason-revoked[data-state=ok]")
      assert has_element?(view, "#auth-security-note")
    end

    test "only benign rejections leave the panel ok", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{auth_rejected: group(%{"missing" => 1.0}, %{"missing" => 4})}))

      assert has_element?(view, "#auth-panel[data-state=ok]")
      refute has_element?(view, "#auth-security-note")
    end
  end

  describe "Overview RPO tile" do
    test "under-reporting badge shows when watermark rows < open shards", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{rpo_under_reporting: true, rpo_rows: 1, rpo_open: 3}))
      assert has_element?(view, "#rpo-under-reporting", "1 / 3")
    end

    test "no badge when coverage is complete", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{rpo_under_reporting: false, rpo_rows: 3, rpo_open: 3}))
      assert has_element?(view, "#rpo-tile")
      refute has_element?(view, "#rpo-under-reporting")
    end
  end

  describe "Cluster Events / 5 min strip" do
    test "zero chips are neutral, non-zero chips are red", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin/cluster")
      assert has_element?(view, "#events-5m")
      assert has_element?(view, "#event-flush_failed[data-state=ok]")

      send_metrics(
        view,
        base(%{events_5m: %{flush_failed: 2, heartbeat_lapsed: 1, corrupt_flush: 0}})
      )

      assert has_element?(view, "#event-flush_failed[data-state=error]", "2")
      assert has_element?(view, "#event-heartbeat_lapsed[data-state=error]")
      assert has_element?(view, "#event-corrupt_flush[data-state=ok]")

      for k <-
            ~w(flush_failed lease_superseded fenced_quarantine corrupt_flush replication_reject
               at_capacity heartbeat_lapsed) do
        assert has_element?(view, "#event-#{k}")
      end
    end
  end
end
