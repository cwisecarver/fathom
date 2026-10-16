defmodule FathomWeb.AdminOverviewLive do
  @moduledoc """
  The dashboard centerpiece: this node's realtime metrics (via `Fathom.Admin.MetricsCollector`)
  alongside the fleet roll-up (via `Fathom.Admin.FleetCollector`), both delivered over PubSub.

  Neither is polled per-viewer: one collector per node does the work and broadcasts, so the
  control-plane cost is independent of how many dashboard tabs are open (expert review
  2026-07-24 #13). The only query this LiveView can issue is a single fallback load at connected
  mount, when the collector has no snapshot yet or isn't running.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents
  import FathomWeb.AdminComponents.NodeHealth
  import FathomWeb.AdminComponents.Errors

  alias Fathom.Admin.{Fleet, FleetCollector, MetricsCollector}

  @impl true
  def mount(_params, _session, socket) do
    connected = connected?(socket)

    if connected do
      Phoenix.PubSub.subscribe(Fathom.PubSub, MetricsCollector.topic())
      # The fleet roll-up is polled ONCE PER NODE by Fathom.Admin.FleetCollector and broadcast
      # (expert review 2026-07-24 #13). Each tab used to run its own start_async on its own 5 s
      # timer, so the directory-scale reads behind Fleet.overview/0 multiplied by open tabs —
      # worst exactly during an incident, when several operators have the dashboard up.
      Phoenix.PubSub.subscribe(Fathom.PubSub, FleetCollector.topic())
      # The collector polls only while a viewer is registered (expert review 2026-10-01 perf #20).
      FleetCollector.watch()
    end

    snap = MetricsCollector.snapshot()

    socket =
      socket
      |> assign(:page_title, "Overview")
      |> assign(:node_key, Fathom.Rebalancer.node_key())
      |> assign(:reporter_on, Fathom.Rebalancer.Reporter.enabled?())
      |> assign(:metrics, snap.current)
      |> assign(:history, snap.history)
      |> assign(:fleet, initial_fleet(connected))
      |> assign(:draining, Fathom.HealthPlug.draining?())

    {:ok, socket}
  end

  @impl true
  def handle_info({:metrics, m}, socket) do
    # The server-rendered KPI sparklines read @history, which mount seeds once from the collector's
    # ring but this handler never advanced — so they froze at the mount snapshot for the whole
    # session. Fold each fresh metric into @history (the collector's ring-point shape), APPENDED so
    # the series stays oldest-first (the sparkline draws left→right and the uPlot initial data needs
    # ascending x), bounded to @ring (300 ≈ 5 min at 1 s) so memory stays flat. Expert review
    # 2026-07-14 #13.
    point = MetricsCollector.ring_point(m)

    history = Enum.take(socket.assigns.history ++ [point], -300)

    socket =
      socket
      |> assign(:metrics, m)
      |> assign(:history, history)
      # A persistent_term read — cheap on the 1 s metrics tick, and keeps the draining badge live.
      |> assign(:draining, Fathom.HealthPlug.draining?())
      |> push_event("chart:qps-chart", %{x: m.at_ms / 1000, ys: [m.node_qps]})
      |> push_event("chart:latency-chart", %{
        x: m.at_ms / 1000,
        ys: [m.query_p50_ms, m.query_p95_ms, m.query_p99_ms]
      })

    {:noreply, socket}
  end

  # Broadcast by this node's FleetCollector. A failed poll simply doesn't broadcast, so the last
  # good roll-up stays on screen — a Postgres blip must not blank the panels.
  def handle_info({:fleet, fleet}, socket), do: {:noreply, assign(socket, :fleet, fleet)}

  # No DB in the disconnected mount. Connected: take the collector's snapshot if it has one,
  # otherwise fall back to a SINGLE load so the first paint isn't blank.
  #
  # The fallback is what keeps this correct when the collector isn't running at all (it is gated on
  # Fathom.Admin.enabled?, which is false in test) or hasn't finished its first poll — without it
  # those cases render permanently-empty panels. It is bounded: one query per tab at mount, versus
  # the old one per tab every 5 s forever. The O(viewers) amplification #13 is about was the
  # recurring poll, and that is gone either way.
  # "12s ago" for a roster `last_seen_at` — the alive/stale badge's 30 s window made visible.
  defp last_seen(nil), do: "—"

  defp last_seen(%DateTime{} = at) do
    case max(DateTime.diff(DateTime.utc_now(), at), 0) do
      s when s < 120 -> "#{s}s ago"
      s when s < 7200 -> "#{div(s, 60)}m ago"
      s -> "#{div(s, 3600)}h ago"
    end
  end

  # node_key is `name@host` — `@` and `.` break a CSS id selector, which tests (and hooks) use.
  defp dom_safe(key), do: String.replace(key, ~r/[^A-Za-z0-9_-]/, "-")

  # Freshness chip (expert review 2026-10-15 #5): the metrics tick (1 s) is the page's pulse; a fleet
  # roll-up older than 2 intervals (30 s) overrides it so a frozen FleetCollector also goes amber.
  @fleet_stale_ms 30_000
  defp as_of_ms(metrics, fleet) do
    fleet_at = Map.get(fleet || %{}, :fetched_at_ms)

    if fleet_at && System.system_time(:millisecond) - fleet_at > @fleet_stale_ms,
      do: fleet_at,
      else: Map.get(metrics || %{}, :at_ms)
  end

  defp initial_fleet(false), do: nil

  defp initial_fleet(true) do
    case FleetCollector.snapshot() do
      nil -> safe_overview()
      fleet -> fleet
    end
  end

  defp safe_overview do
    Fleet.overview()
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin
      flash={@flash}
      active={:overview}
      node_key={@node_key}
      as_of_ms={as_of_ms(@metrics, @fleet)}
      stale_after_ms={4_000}
    >
      <:actions>
        <%!-- Every tile except the Nodes/Migrations panels is THIS node's view (expert review
             2026-10-15 #7): shown at all widths, not `hidden sm:inline`. --%>
        <.badge kind={:info}>
          <span id="scope-this-node">this node · 5m window</span>
        </.badge>
        <.badge :if={@draining} kind={:warn}>
          <span id="draining-this-node">draining</span>
        </.badge>
      </:actions>

      <div class="space-y-6">
        <%!-- Fleet KPI row (Postgres, async) --%>
        <div :if={@fleet == nil} class="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <div :for={_ <- 1..4} class="skeleton h-24 rounded-lg"></div>
        </div>
        <div :if={@fleet} class="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <.stat_tile label="Total shards" value={fmt_int(@fleet.total_shards)} />
          <.stat_tile label="Active" value={fmt_int(@fleet.by_status["active"] || 0)} />
          <%!-- The roster is written ONLY by the load reporter (LOAD_REPORTER, off by default), so an
               empty one is "nobody reports", not "nobody is alive": this very node is serving the
               page. "0 / 0" read like an outage (admin walkthrough 2026-09-30). --%>
          <div id="nodes-live-tile">
            <.stat_tile
              :if={@fleet.nodes != []}
              label="Nodes live"
              value={fmt_int(Enum.count(@fleet.nodes, & &1.alive))}
              unit={"/ #{length(@fleet.nodes)}"}
            />
            <.stat_tile
              :if={@fleet.nodes == []}
              label="Nodes live"
              value="—"
              unit={if(@reporter_on, do: "none reporting", else: "reporter off")}
            />
          </div>
          <.stat_tile
            label="Fleet HEAD"
            value={if(@fleet.head_version, do: "v#{@fleet.head_version}", else: "—")}
            unit={"#{fmt_int(@fleet.laggards)} behind"}
          />
        </div>

        <%!-- This-node KPI row (realtime) --%>
        <div class="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-6">
          <.stat_tile
            label="Queries / sec"
            value={fmt_rate(m(@metrics, :node_qps))}
            accent="text-info"
          >
            <.sparkline values={spark(@history, :qps)} stroke="currentColor" />
          </.stat_tile>
          <.stat_tile label="Query p99" value={fmt_ms(m(@metrics, :query_p99_ms))}>
            <.sparkline values={spark(@history, :p99)} />
          </.stat_tile>
          <.stat_tile label="Open shards" value={fmt_int(m(@metrics, :open_shards))} />
          <.cold_open_tile metrics={@metrics} />
          <.stat_tile
            label="Dirty shards"
            value={fmt_int(m(@metrics, :dirty_shards))}
            accent="text-warning"
          />
          <div id="rpo-tile">
            <.stat_tile
              label="Oldest unflushed (S3)"
              value={fmt_ms(m(@metrics, :oldest_rpo_ms))}
              accent="text-warning"
            >
              <.badge :if={m(@metrics, :rpo_under_reporting)} kind={:warn}>
                <span
                  id="rpo-under-reporting"
                  title="Fewer flush-watermark rows than open shards: dirty count and RPO age exclude shards with no published watermark"
                >
                  under-reporting {fmt_int(m(@metrics, :rpo_rows))} / {fmt_int(m(@metrics, :rpo_open))}
                </span>
              </.badge>
            </.stat_tile>
          </div>
        </div>

        <%!-- Node health strip + admission pressure + VM (components/admin_components/node_health.ex) --%>
        <.node_health metrics={@metrics} />
        <div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <.admission_panel metrics={@metrics} />
          <.vm_panel metrics={@metrics} />
        </div>

        <%!-- Errors + data-plane auth (components/admin_components/errors.ex) --%>
        <div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <.errors_panel metrics={@metrics} history={@history} />
          <.auth_panel metrics={@metrics} />
        </div>

        <%!-- Hero charts --%>
        <div class="grid grid-cols-1 gap-6 xl:grid-cols-2">
          <.panel title="Queries / sec">
            <div
              id="qps-chart"
              phx-hook="Chart"
              phx-update="ignore"
              data-opts={
                Jason.encode!(%{
                  height: 200,
                  yLabel: "q/s",
                  series: [%{label: "q/s", stroke: "#58A6FF", fill: "rgba(88,166,255,0.15)"}]
                })
              }
              data-initial={Jason.encode!(qps_series(@history))}
            >
            </div>
          </.panel>

          <.panel title="Query latency (ms)">
            <div
              id="latency-chart"
              phx-hook="Chart"
              phx-update="ignore"
              data-opts={
                Jason.encode!(%{
                  height: 200,
                  yLabel: "ms",
                  series: [
                    %{label: "p50", stroke: "#58A6FF"},
                    %{label: "p95", stroke: "#BC8CFF"},
                    %{label: "p99", stroke: "#F778BA"}
                  ]
                })
              }
              data-initial={Jason.encode!(latency_series(@history))}
            >
            </div>
          </.panel>
        </div>

        <%!-- Hot shards + storage --%>
        <div class="grid grid-cols-1 gap-6 lg:grid-cols-3">
          <.panel title="Hottest shards (this node)" class="lg:col-span-2">
            <div :if={hot(@metrics) == []} class="py-6 text-center text-sm text-base-content/40">
              No shard traffic recorded. Enable <span class="num">SHARD_LOAD=true</span>
              for per-shard rates.
            </div>
            <table :if={hot(@metrics) != []} class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Shard</th>
                  <th class="py-2 text-right font-medium">q/s</th>
                  <th class="py-2 text-right font-medium">rows read/s</th>
                  <th class="py-2 text-right font-medium">rows written/s</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={s <- hot(@metrics)}
                  class="border-b border-base-300/50 hover:bg-base-content/5"
                >
                  <td class="num py-1.5 text-left text-base-content/80">
                    <.link navigate={~p"/admin/shards/#{s.shard_id}"} class="link">{s.shard_id}</.link>
                  </td>
                  <td class="num py-1.5 text-right">{fmt_rate(s.q_per_s)}</td>
                  <td class="num py-1.5 text-right text-base-content/60">
                    {fmt_rate(s.rows_read_per_s)}
                  </td>
                  <td class="num py-1.5 text-right text-base-content/60">
                    {fmt_rate(s.rows_written_per_s)}
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>

          <.panel title="Storage (S3)">
            <dl class="space-y-3">
              <.stat_row label="Objects stored" value={fmt_int(m(@metrics, :storage_objects))} />
              <.stat_row label="Bytes stored" value={fmt_bytes(m(@metrics, :storage_bytes))} />
              <.stat_row label="S3 GET / s" value={fmt_rate(s3(@metrics, "get"))} />
              <.stat_row label="S3 PUT / s" value={fmt_rate(s3(@metrics, "put"))} />
              <.stat_row label="S3 bytes / s" value={fmt_bytes(s3_bytes(@metrics))} />
            </dl>
          </.panel>
        </div>

        <%!-- Nodes + migrations (fleet) --%>
        <div :if={@fleet == nil} class="skeleton h-40 rounded-lg"></div>
        <div :if={@fleet} class="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <.panel title="Nodes">
            <table class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Node</th>
                  <th class="py-2 text-right font-medium">q/s</th>
                  <th
                    class="hidden py-2 text-right font-medium sm:table-cell"
                    title="99th percentile of this node's per-shard q/s (its hottest shards), not a latency"
                  >
                    hot-shard p99 q/s
                  </th>
                  <th class="py-2 text-right font-medium">Last seen</th>
                  <th class="py-2 text-right font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={n <- @fleet.nodes} class="border-b border-base-300/50">
                  <td
                    class="num max-w-[10rem] truncate py-1.5 pr-2 text-left sm:max-w-none"
                    title={n.node_key}
                  >
                    {n.node_key}
                  </td>
                  <td class="num py-1.5 text-right">
                    {fmt_rate(Map.get(@fleet.node_load, n.node_key, 0))}
                  </td>
                  <td class="num hidden py-1.5 text-right text-base-content/60 sm:table-cell">
                    {fmt_rate(n.q_p99)}
                  </td>
                  <td
                    id={"node-last-seen-#{dom_safe(n.node_key)}"}
                    class="num py-1.5 text-right text-base-content/60"
                  >
                    {last_seen(n.last_seen_at)}
                  </td>
                  <td class="py-1.5 text-right">
                    <.badge kind={if(n.alive, do: :ok, else: :error)}>
                      {if(n.alive, do: "alive", else: "stale")}
                    </.badge>
                    <.badge
                      :if={@draining and n.node_key == @node_key}
                      kind={:warn}
                    >
                      draining
                    </.badge>
                  </td>
                </tr>
                <tr :if={@fleet.nodes == []}>
                  <td colspan="5" class="py-6 text-center text-sm text-base-content/40">
                    No nodes reporting (enable the load reporter).
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>

          <.panel title="Migrations">
            <div class="grid grid-cols-3 gap-4">
              <.stat_row
                label="HEAD"
                value={if(@fleet.head_version, do: "v#{@fleet.head_version}", else: "—")}
              />
              <.stat_row label="Laggards" value={fmt_int(@fleet.laggards)} />
              <.stat_row label="Quarantined" value={fmt_int(@fleet.failed)} />
            </div>
            <div class="mt-4 border-t border-base-300 pt-3">
              <div class="mb-2 text-[11px] uppercase tracking-wide text-base-content/50">
                Background jobs
              </div>
              <div :if={@fleet.oban == []} class="text-sm text-base-content/40">No jobs.</div>
              <div class="flex flex-wrap gap-2">
                <.badge :for={j <- @fleet.oban} kind={oban_kind(j.state)}>
                  {j.queue}/{j.state} {j.count}
                </.badge>
              </div>
            </div>
          </.panel>
        </div>
      </div>
    </Layouts.admin>
    """
  end

  # A small label/value row for the storage + migration panels.
  attr :label, :string, required: true
  attr :value, :string, required: true

  defp stat_row(assigns) do
    ~H"""
    <div class="flex items-baseline justify-between gap-2">
      <dt class="text-xs text-base-content/50">{@label}</dt>
      <dd class="num text-sm">{@value}</dd>
    </div>
    """
  end

  # ── view helpers ──

  # Safe read of a metrics field (metrics may be nil before the first tick / collector off).
  defp m(nil, _key), do: nil
  defp m(metrics, key), do: Map.get(metrics, key)

  defp hot(nil), do: []
  defp hot(metrics), do: Map.get(metrics, :hot_shards, [])

  defp s3(nil, _method), do: 0.0
  defp s3(metrics, method), do: metrics |> Map.get(:s3_ops_per_s, %{}) |> Map.get(method, 0.0)

  defp s3_bytes(nil), do: 0.0

  defp s3_bytes(metrics),
    do: metrics |> Map.get(:s3_bytes_per_s, %{}) |> Map.values() |> Enum.sum()

  defp oban_kind("completed"), do: :ok
  defp oban_kind("executing"), do: :info
  defp oban_kind("available"), do: :info
  defp oban_kind("retryable"), do: :warn
  defp oban_kind("discarded"), do: :error
  defp oban_kind(_), do: :neutral

  # Extract a series (oldest-first) from the history ring for a sparkline.
  defp spark(history, key), do: Enum.map(history || [], &Map.get(&1, key, 0.0))

  # uPlot initial data: [[xs], [ys...]] with x in seconds.
  defp qps_series(history) do
    h = history || []
    [Enum.map(h, fn p -> p.t / 1000 end), Enum.map(h, fn p -> p.qps end)]
  end

  defp latency_series(history) do
    h = history || []

    [
      Enum.map(h, fn p -> p.t / 1000 end),
      Enum.map(h, fn p -> p.p50 end),
      Enum.map(h, fn p -> p.p95 end),
      Enum.map(h, fn p -> p.p99 end)
    ]
  end
end
