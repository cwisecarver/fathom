defmodule FathomWeb.AdminDurabilityLiveTest do
  @moduledoc """
  The Durability page (expert review 2026-10-15 #1, #8, #16): auth gate, the empty /
  no-replication state, and the rendering of firing alarms, A2 slack, Backup & DR and the
  under-reporting badge. Data is injected through the LiveView's `{:durability_data, map}` message
  (the same shape its async task produces) so each state is deterministic.
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Admin.Durability

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  defp snapshot(overrides \\ %{}) do
    alarms =
      for {key, label} <- Durability.alarm_keys() do
        %{key: key, label: label, window: 0, total: 0, firing?: false}
      end

    Map.merge(
      %{
        at_ms: 0,
        scrape?: true,
        window_s: 120,
        alarms: alarms,
        quarantines: 0,
        replication: %{enabled?: false}
      },
      overrides
    )
  end

  defp repl(overrides) do
    Map.merge(
      %{
        enabled?: true,
        connected: 3,
        configured: 3,
        quorum: 2,
        slack: 1,
        budget_ratio: 0.25,
        catching_up_shards: 2,
        catchup_armed: 3,
        commit: %{
          total: 10,
          p50: 4.0,
          p95: 40.0,
          p99: 90.0,
          outcomes: %{"ok" => 8, "no_quorum" => 1, "timeout" => 1, "other" => 0}
        },
        followers: [
          %{node_key: "n2", host: "10.0.0.2", port: 7000, connected?: true, alive?: true},
          %{node_key: "n3", host: "10.0.0.3", port: 7000, connected?: false, alive?: :unknown}
        ],
        rates: %{
          reject: %{"overloaded" => 3},
          seed: %{},
          disk_pressure: 0,
          promotion_raced: %{},
          recovered_from_peer: 0
        }
      },
      overrides
    )
  end

  test "GET /admin/durability without credentials is challenged (401)", %{conn: conn} do
    assert get(conn, "/admin/durability").status == 401
  end

  test "the disconnected render is the shell only", %{conn: conn} do
    html = conn |> auth() |> get("/admin/durability") |> html_response(200)
    assert html =~ "durability-page"
    refute html =~ "alarm-strip"
  end

  test "empty state: no reporter, no replication, nothing dirty, no directory", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    # Drain the connected mount's own refresh first, or its result races the injected data.
    render_async(view)

    send(
      view.pid,
      {:durability_data, %{snapshot: snapshot(%{scrape?: false}), backup: nil, dirty: nil}}
    )

    render(view)

    assert has_element?(view, "#alarm-strip")
    assert has_element?(view, "#alarm-no-scrape")
    assert has_element?(view, "#replication-off")
    refute has_element?(view, "#replication-panel")
    assert has_element?(view, "#backup-empty")
    refute has_element?(view, "[data-firing='true']")
  end

  test "a firing counter turns its chip red; quiet ones stay quiet", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    # Drain the connected mount's own refresh first, or its result races the injected data.
    render_async(view)

    alarms =
      for {key, label} <- Durability.alarm_keys() do
        %{
          key: key,
          label: label,
          window: if(key == :corrupt_flush, do: 2, else: 0),
          total: 2,
          firing?: key == :corrupt_flush
        }
      end

    send(view.pid, {:durability_data, %{snapshot: snapshot(%{alarms: alarms, quarantines: 4})}})
    render(view)

    assert has_element?(view, "#alarm-corrupt_flush[data-firing='true']")
    assert has_element?(view, "#alarm-flush_failed[data-firing='false']")
    assert has_element?(view, "#alarm-quarantines[data-firing='true']")
  end

  test "replication latency tiles, outcomes and a populated lag table linking to shards", %{
    conn: conn
  } do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    render_async(view)

    lag = %{
      sessions: 5,
      queried: 5,
      answered: 4,
      timed_out: 1,
      catching_up: 1,
      worst: [
        %{
          shard_id: "lagshard",
          catchup_fails: 3,
          catching_up?: true,
          inflight_pushes: 1,
          seeds_in_flight: 0,
          behind_followers: 1,
          unacked_bytes: 4096
        }
      ]
    }

    send(
      view.pid,
      {:durability_data, %{snapshot: snapshot(%{replication: repl(%{})}), lag: lag}}
    )

    render(view)

    assert has_element?(view, "#repl-commit-p99", "90 ms")
    assert has_element?(view, "#repl-commit-no_quorum", "no_quorum 1")
    assert has_element?(view, "#repl-catching-up", "2")
    assert has_element?(view, "#repl-lag-table")
    assert has_element?(view, "#lag-lagshard a[href='/admin/shards/lagshard']")
    assert has_element?(view, "#repl-lag-partial", "4 answered, 1 timed out")
    refute has_element?(view, "#repl-lag-empty")
  end

  # The report asks only flagged Sessions (expert review 2026-10-21 #24), so with nothing lagging it
  # asked none; the line counts the registered Sessions instead of the (zero) answers.
  test "enabled but nothing lagging says no session is catching up", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    render_async(view)

    lag = %{sessions: 3, queried: 0, answered: 0, timed_out: 0, catching_up: 0, worst: []}

    send(
      view.pid,
      {:durability_data, %{snapshot: snapshot(%{replication: repl(%{})}), lag: lag}}
    )

    render(view)

    assert has_element?(view, "#repl-lag-empty", "No session catching up (3 sessions)")
    refute has_element?(view, "#repl-lag-table")
    refute has_element?(view, "#repl-lag-partial")
  end

  test "replication panel: slack, budget, per-follower rows, 5-minute rates", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    # Drain the connected mount's own refresh first, or its result races the injected data.
    render_async(view)

    send(view.pid, {:durability_data, %{snapshot: snapshot(%{replication: repl(%{})})}})
    render(view)

    assert has_element?(view, "#replication-panel")
    assert has_element?(view, "#repl-slack")
    assert has_element?(view, "#repl-budget")
    assert has_element?(view, "#follower-n2")
    assert has_element?(view, "#follower-n3")
    assert has_element?(view, "#repl-rate-reject", "overloaded 3")
    refute has_element?(view, "#replication-off")
  end

  test "zero slack is flagged as one loss from failing", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    # Drain the connected mount's own refresh first, or its result races the injected data.
    render_async(view)

    send(
      view.pid,
      {:durability_data, %{snapshot: snapshot(%{replication: repl(%{slack: 0, connected: 2})})}}
    )

    render(view)

    assert has_element?(view, "#repl-slack", "one loss from failing")
  end

  test "Backup & DR and dirtiest shards render, with the under-reporting badge", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    # Drain the connected mount's own refresh first, or its result races the injected data.
    render_async(view)

    backup = %{
      total: 10,
      last_snapshot_at: DateTime.add(DateTime.utc_now(), -3_600, :second),
      never_snapshotted: 2,
      stale_snapshot: 3,
      verify: %{"ok" => 6, nil => 3, "corrupt" => 1}
    }

    dirty = %{
      rows: [%{shard_id: "acme", age_ms: 61_000, unflushed_writes: 12}],
      dirty_count: 1,
      watermark_rows: 1,
      open_shards: 5,
      under_reporting?: true
    }

    send(view.pid, {:durability_data, %{snapshot: snapshot(), backup: backup, dirty: dirty}})
    render(view)

    assert has_element?(view, "#backup-panel")
    assert has_element?(view, "#verify-corrupt")
    assert has_element?(view, "#verify-never")
    assert has_element?(view, "#dirty-acme")
    assert has_element?(view, "#under-reporting")
  end

  test "no dirty shards shows the empty message and no badge", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    # Drain the connected mount's own refresh first, or its result races the injected data.
    render_async(view)

    dirty = %{
      rows: [],
      dirty_count: 0,
      watermark_rows: 5,
      open_shards: 5,
      under_reporting?: false
    }

    send(view.pid, {:durability_data, %{snapshot: snapshot(), dirty: dirty}})
    render(view)

    assert has_element?(view, "#dirty-empty")
    refute has_element?(view, "#under-reporting")
  end

  test "the connected mount polls the cache and renders without injection", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/durability")
    # Drain the connected mount's own refresh first, or its result races the injected data.
    render_async(view)
    # The async task runs against the real (uncached, reporter-less) module.
    assert render_async(view) =~ "Durability alarms"
    assert has_element?(view, "#alarm-strip")
  end
end
