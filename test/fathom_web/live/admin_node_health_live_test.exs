defmodule FathomWeb.AdminNodeHealthLiveTest do
  @moduledoc """
  Overview panels from `FathomWeb.AdminComponents.NodeHealth` (expert review 2026-10-15
  #2/#6/#10/#15/#17): driven by `{:metrics, map}` broadcasts, asserted by DOM id and `data-state`.
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.MetricsCollector
  alias FathomWeb.AdminComponents.NodeHealth

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
        cold_open_p50_ms: 24.0,
        cold_open_p95_ms: 80.0,
        cold_open_p99_ms: 120.0,
        cold_open_n: 12,
        cold_open_by_warm: %{
          "true" => %{p50: 5.0, p95: 8.0, p99: 9.0, n: 9},
          "false" => %{p50: 90.0, p95: 110.0, p99: 120.0, n: 3}
        },
        dirty_shards: 0,
        oldest_rpo_ms: 0.0,
        s3_ops_per_s: %{},
        s3_bytes_per_s: %{},
        checkout_per_s: %{"ok" => 10.0, "held" => 1.0},
        storage_objects: 0,
        storage_bytes: 0,
        hot_shards: []
      },
      overrides
    )
  end

  defp health(overrides) do
    Map.merge(
      %{
        disks: %{
          "data" => %{used_ratio: 0.42, free_bytes: 50_000_000_000},
          "replica" => %{used_ratio: 0.55, free_bytes: 40_000_000_000}
        },
        process_used_ratio: 0.01,
        port_used_ratio: 0.02,
        connections_live: 120,
        flush_gate: %{in_flight: 1, cap: 4, refusals_per_s: 0.0},
        recorder: %{buffered: 5, flush_buffered: 0, last_flush_age_ms: 900}
      },
      overrides
    )
  end

  defp send_metrics(view, m) do
    Phoenix.PubSub.broadcast(Fathom.PubSub, MetricsCollector.topic(), {:metrics, m})
    render(view)
  end

  describe "Node health strip" do
    test "renders each gauge, all ok", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{node_health: health(%{})}))

      for id <- ~w(nh-disk-data nh-disk-replica nh-procs nh-ports nh-conns nh-flush-gate
                   nh-recorder nh-memory) do
        assert has_element?(view, "##{id}[data-state=ok]"), "#{id} should be ok"
      end
    end

    test "warns above 0.8 and errors above 0.95; warns when the flush gate is pinned", %{
      conn: conn
    } do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{
          node_health:
            health(%{
              disks: %{"data" => %{used_ratio: 0.85, free_bytes: 1}},
              process_used_ratio: 0.97,
              flush_gate: %{in_flight: 4, cap: 4, refusals_per_s: 2.0},
              recorder: %{buffered: 900, flush_buffered: 0, last_flush_age_ms: 120_000}
            })
        })
      )

      assert has_element?(view, "#nh-disk-data[data-state=warn]")
      assert has_element?(view, "#nh-procs[data-state=error]")
      assert has_element?(view, "#nh-flush-gate[data-state=warn]")
      assert has_element?(view, "#nh-recorder[data-state=warn]")
      assert has_element?(view, "#nh-ports[data-state=ok]")
    end

    test "empty state: no readings yet renders idle cells, not a crash", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{}))

      assert has_element?(view, "#node-health")
      assert has_element?(view, "#nh-disk-none[data-state=idle]")
      assert has_element?(view, "#nh-flush-gate[data-state=idle]")
    end
  end

  describe "cold-open tile (#6)" do
    test "shows windowed percentiles and the warm/pulled split", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{}))

      assert has_element?(view, "#cold-open-tile", "p95")
      assert has_element?(view, "#cold-open-tile", "p99")
      assert has_element?(view, "#cold-open-warm-true", "warm 9")
      assert has_element?(view, "#cold-open-warm-false", "pulled 3")
    end

    test "no opens in the window reads as empty, not 0 ms", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{cold_open_n: 0, cold_open_by_warm: %{}}))

      assert has_element?(view, "#cold-open-tile", "no opens in window")
    end
  end

  describe "Admission / pressure (#10)" do
    test "utilisation bar warns near the cap; refusal rates and recent ids render", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{
          admission: %{
            open: 90,
            cap: 100,
            rates: %{evicted: 2.0, at_capacity: 1.5, novel_rate_limited: 0.0, pool_hit: 9.0},
            pool_hit_ratio: 0.9,
            recent: [
              %{kind: :at_capacity, shard_id: "refused-shard", t: 1},
              %{kind: :evicted, shard_id: "evicted-shard", t: 0}
            ]
          }
        })
      )

      assert has_element?(view, "#admission-open", "90 / 100")
      assert has_element?(view, "#admission-bar[data-state=warn]")
      assert has_element?(view, "#admission-at-capacity[data-state=warn]")
      assert has_element?(view, "#admission-novel[data-state=ok]")
      assert has_element?(view, "#admission-pool", "90.0%")
      assert has_element?(view, "#admission-checkouts")
      assert has_element?(view, "#admission-recent", "refused-shard")
      assert has_element?(view, "#admission-recent", "evicted-shard")
    end

    test "no cap and nothing refused: says so", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{admission: %{open: 3, cap: nil, rates: %{}, recent: []}}))

      assert has_element?(view, "#admission-open", "no cap")
      refute has_element?(view, "#admission-bar")
      assert has_element?(view, "#admission-recent-empty")
    end
  end

  describe "VM panel (#15)" do
    test "dirty-IO backlog warns; memory breakdown lists components", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")

      send_metrics(
        view,
        base(%{
          vm: %{
            run_queue: %{normal: 0.0, dirty_cpu: 0.0, dirty_io: 5.0},
            util: %{normal: 0.2, dirty_cpu: 0.0, dirty_io: 0.95},
            memory: %{processes: 1_000_000, binary: 2_000_000, ets: 500_000},
            dirty_io_schedulers: 10
          }
        })
      )

      assert has_element?(view, "#vm-rq-dirty_io[data-state=warn]")
      assert has_element?(view, "#vm-util-dirty_io[data-state=warn]")
      assert has_element?(view, "#vm-rq-normal[data-state=ok]")
      assert has_element?(view, "#vm-mem-binary")
      assert has_element?(view, "#vm-mem-ets")
    end

    test "before the first poll the panel shows idle cells and 'No reading yet'", %{conn: conn} do
      {:ok, view, _} = conn |> auth() |> live("/admin")
      send_metrics(view, base(%{}))

      assert has_element?(view, "#vm-rq-dirty_io[data-state=idle]")
      assert has_element?(view, "#vm-memory", "No reading yet")
    end
  end

  test "threshold helpers" do
    assert NodeHealth.ratio_state(0.8) == :ok
    assert NodeHealth.ratio_state(0.81) == :warn
    assert NodeHealth.ratio_state(0.951) == :error
    assert NodeHealth.ratio_state(nil) == :idle
    assert NodeHealth.gate_state(4, 4) == :warn
    assert NodeHealth.gate_state(3, 4) == :ok
    assert NodeHealth.gate_state(0, nil) == :idle
  end
end
