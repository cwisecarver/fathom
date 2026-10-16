defmodule FathomWeb.AdminShardsLive do
  @moduledoc """
  Shard drill-down: this node's live hottest shards (realtime, from `Fathom.Admin.MetricsCollector`)
  and the fleet-wide hot set with serving node (`Fathom.Admin.Fleet.hot_shards/0` through
  `Fathom.Admin.HotShardsCache`, so N open tabs cost one compute per TTL, not N; flicker-free —
  the last-good set stays visible while a refresh runs).
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.{HotShardsCache, MetricsCollector}

  @fleet_refresh_ms 5_000

  @impl true
  def mount(_params, _session, socket) do
    connected = connected?(socket)

    if connected do
      Phoenix.PubSub.subscribe(Fathom.PubSub, MetricsCollector.topic())
      Process.send_after(self(), :refresh_fleet, @fleet_refresh_ms)
    end

    snap = MetricsCollector.snapshot()

    socket =
      socket
      |> assign(:page_title, "Shards")
      |> assign(:node_key, Fathom.Rebalancer.node_key())
      |> assign(:metrics, snap.current)
      |> assign(:fleet_hot, nil)
      |> assign(:load_enabled?, Fathom.ShardLoad.enabled?())
      |> load_fleet(connected)

    {:ok, socket}
  end

  # Fleet hot set off-process (no DB in the disconnected mount); start_async keeps the previous
  # value visible during a refresh (no skeleton flicker).
  defp load_fleet(socket, false), do: socket

  defp load_fleet(socket, true),
    do: start_async(socket, :fleet_hot, fn -> HotShardsCache.get() end)

  @impl true
  def handle_info({:metrics, m}, socket), do: {:noreply, assign(socket, :metrics, m)}

  def handle_info(:refresh_fleet, socket) do
    Process.send_after(self(), :refresh_fleet, @fleet_refresh_ms)
    {:noreply, load_fleet(socket, true)}
  end

  @impl true
  def handle_async(:fleet_hot, {:ok, hot}, socket),
    do: {:noreply, assign(socket, :fleet_hot, hot)}

  def handle_async(:fleet_hot, {:exit, _reason}, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:shards} node_key={@node_key}>
      <div class="space-y-6">
        <.panel title="Hottest shards on this node (live)">
          <div :if={hot(@metrics) == []} class="py-6 text-center text-sm text-base-content/40">
            <span :if={!@load_enabled?} id="shard-load-off">
              Per-shard load recording is OFF on this node — set
              <span class="num">SHARD_LOAD=true</span>
              to see per-shard traffic here.
            </span>
            <span :if={@load_enabled?}>No per-shard traffic recorded yet.</span>
          </div>
          <table :if={hot(@metrics) != []} class="w-full text-sm">
            <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
              <tr class="border-b border-base-300">
                <th class="py-2 text-left font-medium">Shard</th>
                <th class="py-2 text-right font-medium">q/s</th>
                <th class="py-2 text-right font-medium">p50</th>
                <th class="py-2 text-right font-medium">p99</th>
                <th class="py-2 text-right font-medium">rows read/s</th>
                <th class="py-2 text-right font-medium">rows written/s</th>
                <th class="py-2 text-right font-medium">queries (total)</th>
              </tr>
            </thead>
            <tbody>
              <tr
                :for={s <- hot(@metrics)}
                class="border-b border-base-300/50 hover:bg-base-content/5"
              >
                <td class="num py-1.5 text-base-content/80">
                  <.link navigate={~p"/admin/shards/#{s.shard_id}"} class="link">{s.shard_id}</.link>
                </td>
                <td class="num py-1.5 text-right">{fmt_rate(s.q_per_s)}</td>
                <td class="num py-1.5 text-right text-base-content/60">{fmt_ms(s.p50_ms)}</td>
                <td class="num py-1.5 text-right text-base-content/60">{fmt_ms(s.p99_ms)}</td>
                <td class="num py-1.5 text-right text-base-content/60">
                  {fmt_rate(s.rows_read_per_s)}
                </td>
                <td class="num py-1.5 text-right text-base-content/60">
                  {fmt_rate(s.rows_written_per_s)}
                </td>
                <td class="num py-1.5 text-right text-base-content/60">{fmt_int(s.queries)}</td>
              </tr>
            </tbody>
          </table>
        </.panel>

        <.panel title="Fleet hot set (merged, with serving node)">
          <div :if={@fleet_hot == nil} class="skeleton h-24 rounded"></div>
          <div :if={@fleet_hot == []} class="py-6 text-center text-sm text-base-content/40">
            No fleet load samples. <.load_empty_reason scope={:fleet} />
          </div>
          <table :if={is_list(@fleet_hot) and @fleet_hot != []} class="w-full text-sm">
            <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
              <tr class="border-b border-base-300">
                <th class="py-2 text-left font-medium">Shard</th>
                <th class="py-2 text-left font-medium">Node</th>
                <th class="py-2 text-right font-medium">q/s</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={s <- @fleet_hot} class="border-b border-base-300/50">
                <td class="num py-1.5">
                  <.link navigate={~p"/admin/shards/#{s.shard_id}"} class="link">{s.shard_id}</.link>
                </td>
                <td class="num py-1.5 text-base-content/70">{s.node_key}</td>
                <td class="num py-1.5 text-right">{fmt_rate(s.q_per_s)}</td>
              </tr>
            </tbody>
          </table>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end

  defp hot(nil), do: []
  defp hot(metrics), do: Map.get(metrics, :hot_shards, [])
end
