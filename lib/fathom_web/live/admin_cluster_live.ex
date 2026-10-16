defmodule FathomWeb.AdminClusterLive do
  @moduledoc """
  This node's lease health and recent lease/quarantine events (expert review 2026-10-15 #9, #21).

  Node-local and lock-free by construction: the Lease-health panel reads the heartbeat status row
  and the write-fence table (`Fathom.Admin.LeaseHealth`), the events table reads the bounded
  `Fathom.Admin.LeaseEvents` ring. Nothing here calls a coordinator or touches Postgres, so the
  one-second refresh is O(1) in shards and costs the same with a thousand viewers' worth of ticks.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.{LeaseEvents, LeaseHealth}

  @refresh_ms 1_000
  @events_shown 100

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: Process.send_after(self(), :refresh, @refresh_ms)

    {:ok,
     socket
     |> assign(:page_title, "Cluster")
     |> assign(:node_key, Fathom.Rebalancer.node_key())
     |> load()}
  end

  @impl true
  def handle_info(:refresh, socket) do
    Process.send_after(self(), :refresh, @refresh_ms)
    {:noreply, load(socket)}
  end

  defp load(socket) do
    socket
    |> assign(:lease, LeaseHealth.snapshot())
    |> assign(:events, LeaseEvents.recent(@events_shown))
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:cluster} node_key={@node_key}>
      <div class="space-y-6">
        <.panel title="Lease health" class="">
          <div id="lease-health" class="grid grid-cols-2 gap-4 md:grid-cols-4">
            <div id="lease-mode">
              <div class="text-[11px] uppercase tracking-wide text-base-content/50">Mode</div>
              <.badge kind={if @lease.mode == :heartbeat, do: :ok, else: :warn}>
                {@lease.mode}
              </.badge>
            </div>
            <div id="lease-state">
              <div class="text-[11px] uppercase tracking-wide text-base-content/50">State</div>
              <.badge :if={@lease.lapsed == nil} kind={:neutral}>n/a (legacy)</.badge>
              <.badge :if={@lease.lapsed == true} kind={:error}>lapsed</.badge>
              <.badge :if={@lease.lapsed == false} kind={:ok}>valid</.badge>
            </div>
            <div id="lease-generation">
              <div class="text-[11px] uppercase tracking-wide text-base-content/50">Generation</div>
              <span class="num">{if @lease.generation, do: @lease.generation, else: "—"}</span>
            </div>
            <div id="lease-deadline">
              <div class="text-[11px] uppercase tracking-wide text-base-content/50">
                Until deadline
              </div>
              <span class="num">{fmt_ms(@lease.ms_until_deadline)}</span>
            </div>
            <div id="lease-margin">
              <div class="text-[11px] uppercase tracking-wide text-base-content/50">Margin</div>
              <span class="num">{fmt_ms(@lease.margin_ms)}</span>
            </div>
            <div id="lease-skew">
              <div class="text-[11px] uppercase tracking-wide text-base-content/50">
                Clock skew vs store
              </div>
              <span class="num">{fmt_ms(@lease.skew_ms)}</span>
            </div>
            <div id="lease-fenced">
              <div class="text-[11px] uppercase tracking-wide text-base-content/50">
                Shards write-fenced
              </div>
              <.badge kind={if @lease.write_fenced_shards > 0, do: :error, else: :ok}>
                {fmt_int(@lease.write_fenced_shards)}
              </.badge>
            </div>
          </div>
        </.panel>

        <.panel title="Recent lease events">
          <div
            :if={@events == []}
            id="lease-events-empty"
            class="py-6 text-center text-sm text-base-content/40"
          >
            No lease events recorded on this node since boot.
          </div>
          <table :if={@events != []} id="lease-events" class="w-full text-sm">
            <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
              <tr class="border-b border-base-300">
                <th class="py-2 text-left font-medium">When</th>
                <th class="py-2 text-left font-medium">Event</th>
                <th class="py-2 text-left font-medium">Shard</th>
                <th class="py-2 text-left font-medium">Detail</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={e <- @events} id={"lease-event-#{e.seq}"} class="border-b border-base-300/50">
                <td class="num py-1.5 text-base-content/60">{time(e.at_ms)}</td>
                <td class="py-1.5">
                  <.badge kind={kind(e.kind)}>{e.kind}</.badge>
                </td>
                <td class="num py-1.5">{e.shard_id || "node"}</td>
                <td class="py-1.5 text-base-content/70">{e.detail}</td>
              </tr>
            </tbody>
          </table>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end

  defp kind(k) when k in [:superseded, :fenced_quarantine, :lapsed, :skew_lapse], do: :error
  defp kind(k) when k in [:held, :renew_failed], do: :warn
  defp kind(_), do: :info

  defp time(ms), do: ms |> DateTime.from_unix!(:millisecond) |> Calendar.strftime("%H:%M:%S")
end
