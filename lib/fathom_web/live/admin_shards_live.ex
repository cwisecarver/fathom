defmodule FathomWeb.AdminShardsLive do
  @moduledoc """
  Shard drill-down: this node's live hottest shards (realtime, from `Fathom.Admin.MetricsCollector`)
  and the fleet-wide hot set with serving node (from `Fathom.Admin.Fleet`, async).
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.{Fleet, MetricsCollector}

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(Fathom.PubSub, MetricsCollector.topic())
    end

    snap = MetricsCollector.snapshot()

    socket =
      socket
      |> assign(:page_title, "Shards")
      |> assign(:node_key, Fathom.Rebalancer.node_key())
      |> assign(:metrics, snap.current)
      |> assign_async(:hot, fn -> {:ok, %{hot: Fleet.hot_shards()}} end)

    {:ok, socket}
  end

  @impl true
  def handle_info({:metrics, m}, socket), do: {:noreply, assign(socket, :metrics, m)}

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:shards} node_key={@node_key}>
      <div class="space-y-6">
        <.panel title="Hottest shards on this node (live)">
          <div :if={hot(@metrics) == []} class="py-6 text-center text-sm text-base-content/40">
            No per-shard traffic recorded — enable <span class="num">SHARD_LOAD=true</span>.
          </div>
          <table :if={hot(@metrics) != []} class="w-full text-sm">
            <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
              <tr class="border-b border-base-300">
                <th class="py-2 text-left font-medium">Shard</th>
                <th class="py-2 text-right font-medium">q/s</th>
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
                <td class="num py-1.5 text-base-content/80">{s.shard_id}</td>
                <td class="num py-1.5 text-right">{fmt_rate(s.q_per_s)}</td>
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
          <.async_result :let={hot} assign={@hot}>
            <:loading>
              <div class="skeleton h-24 rounded"></div>
            </:loading>
            <:failed :let={_}>
              <span class="text-sm text-error">Load samples unavailable.</span>
            </:failed>
            <div :if={hot == []} class="py-6 text-center text-sm text-base-content/40">
              No fleet load samples (enable the load reporter).
            </div>
            <table :if={hot != []} class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Shard</th>
                  <th class="py-2 text-left font-medium">Node</th>
                  <th class="py-2 text-right font-medium">q/s</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={s <- hot} class="border-b border-base-300/50">
                  <td class="num py-1.5">{s.shard_id}</td>
                  <td class="num py-1.5 text-base-content/70">{s.node_key}</td>
                  <td class="num py-1.5 text-right">{fmt_rate(s.q_per_s)}</td>
                </tr>
              </tbody>
            </table>
          </.async_result>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end

  defp hot(nil), do: []
  defp hot(metrics), do: Map.get(metrics, :hot_shards, [])
end
