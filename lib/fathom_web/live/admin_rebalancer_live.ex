defmodule FathomWeb.AdminRebalancerLive do
  @moduledoc """
  The rebalancer's state, which used to be fetched every 15 s and thrown away (expert review
  2026-10-15 #13): LB pins and pending handoff commands with age and a stale badge, a bounded ring
  of recent policy decisions (the `reason` and the guard that declined each hot candidate), and the
  last LB-apply outcome.

  Pins and pending handoffs ride the existing `Fathom.Admin.FleetCollector` broadcast (no new
  query). The decision ring and LB-apply status are **in-memory on the node that ran them**
  (`Fathom.Rebalancer.DecisionLog`) — the rebalance tick runs on the Oban leader only, so a viewer
  on another node sees those two panels empty and the page says so.

  A pin is **stale** when its node is not beating (the reconciler will unpin it); a pending
  handoff command is **stale** past the same window `RebalanceJob` expires it at.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.{Fleet, FleetCollector}
  alias Fathom.Rebalancer.DecisionLog

  @local_refresh_ms 5_000
  # Mirrors RebalanceJob's @command_stale_ms default (config :rebalance_command_stale_ms).
  @command_stale_ms 900_000

  @impl true
  def mount(_params, _session, socket) do
    connected = connected?(socket)

    if connected do
      Phoenix.PubSub.subscribe(Fathom.PubSub, FleetCollector.topic())
      FleetCollector.watch()
      Process.send_after(self(), :refresh_local, @local_refresh_ms)
    end

    socket =
      socket
      |> assign(:page_title, "Rebalancer")
      |> assign(:node_key, Fathom.Rebalancer.node_key())
      |> assign(:rebalancer_on, Application.get_env(:fathom, :rebalancer_enabled, false) == true)
      |> assign(:fleet, initial_fleet(connected))
      |> load_local()

    {:ok, socket}
  end

  @impl true
  def handle_info({:fleet, fleet}, socket), do: {:noreply, assign(socket, :fleet, fleet)}

  def handle_info(:refresh_local, socket) do
    Process.send_after(self(), :refresh_local, @local_refresh_ms)
    {:noreply, load_local(socket)}
  end

  # In-memory reads (ETS) — cheap, so a plain timer, not an async task.
  defp load_local(socket) do
    socket
    |> assign(:ticks, DecisionLog.recent())
    |> assign(:last_tick, DecisionLog.last_tick())
    |> assign(:lb_apply, DecisionLog.lb_apply_status())
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

  # ── view helpers ──

  defp age(nil), do: "—"

  defp age(%DateTime{} = at) do
    case max(DateTime.diff(DateTime.utc_now(), at), 0) do
      s when s < 120 -> "#{s}s"
      s when s < 7200 -> "#{div(s, 60)}m"
      s when s < 172_800 -> "#{div(s, 3600)}h"
      s -> "#{div(s, 86_400)}d"
    end
  end

  # The pin's own clock: when it was last written (a handoff re-pin updates it).
  defp pin_at(pin), do: Map.get(pin, :updated_at) || Map.get(pin, :inserted_at)

  defp alive_nodes(%{nodes: nodes}),
    do: for(n <- nodes, n.alive, into: MapSet.new(), do: n.node_key)

  defp alive_nodes(_), do: MapSet.new()

  # Stale only when we have a roster to judge by: with nobody reporting, every pin would read stale.
  defp stale_pin?(pin, fleet) do
    alive = alive_nodes(fleet)

    is_nil(pin.failed_at) and MapSet.size(alive) > 0 and
      not MapSet.member?(alive, pin.pinned_node)
  end

  defp stale_command?(cmd) do
    case Map.get(cmd, :inserted_at) do
      %DateTime{} = at -> DateTime.diff(DateTime.utc_now(), at, :millisecond) > command_stale_ms()
      _ -> false
    end
  end

  defp command_stale_ms,
    do: Application.get_env(:fathom, :rebalance_command_stale_ms, @command_stale_ms)

  defp guard_text(:not_backend), do: "source not an LB backend"
  defp guard_text(:cooldown), do: "cooling down (recently pinned)"
  defp guard_text(:ineligible), do: "ineligible (not active / migrating)"
  defp guard_text(:unconfirmed), do: "not hot in enough windows"
  defp guard_text(:no_improving_target), do: "no target improves balance"
  defp guard_text(:max_moves), do: "max moves per tick reached"
  defp guard_text(other), do: to_string(other)

  defp apply_kind(outcome) when outcome in [:applied, :noop], do: :ok
  defp apply_kind(:not_written), do: :neutral
  defp apply_kind(_), do: :error

  defp short_hash(nil), do: "—"
  defp short_hash(h), do: String.slice(h, 0, 12)

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin
      flash={@flash}
      active={:rebalancer}
      node_key={@node_key}
      as_of_ms={Map.get(@fleet || %{}, :fetched_at_ms)}
      stale_after_ms={30_000}
    >
      <:actions>
        <.badge kind={if(@rebalancer_on, do: :ok, else: :neutral)}>
          <span id="rebalancer-gate">{if(@rebalancer_on, do: "rebalancer on", else: "rebalancer off")}</span>
        </.badge>
      </:actions>

      <div class="space-y-6">
        <div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <.panel title="Pins (LB overrides)">
            <div :if={@fleet == nil} class="skeleton h-24 rounded-lg"></div>
            <table :if={@fleet} id="pins-table" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Shard</th>
                  <th class="py-2 text-left font-medium">Pinned to</th>
                  <th class="py-2 text-right font-medium">Age</th>
                  <th class="py-2 text-right font-medium">State</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={p <- @fleet.pins}
                  id={"pin-#{p.shard_id}"}
                  class="border-b border-base-300/50"
                >
                  <td class="num py-1.5 text-left">{p.shard_id}</td>
                  <td class="num py-1.5 text-left">{p.pinned_node}</td>
                  <td class="num py-1.5 text-right text-base-content/60">{age(pin_at(p))}</td>
                  <td class="py-1.5 text-right">
                    <.badge :if={p.failed_at} kind={:warn}>failed (cooldown)</.badge>
                    <.badge :if={stale_pin?(p, @fleet)} kind={:error}>stale</.badge>
                    <.badge :if={!p.failed_at and !stale_pin?(p, @fleet)} kind={:ok}>live</.badge>
                  </td>
                </tr>
                <tr :if={@fleet.pins == []}>
                  <td
                    id="pins-empty"
                    colspan="4"
                    class="py-6 text-center text-sm text-base-content/40"
                  >
                    No pins — every shard is on its hash home.
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>

          <.panel title="Pending handoff commands">
            <div :if={@fleet == nil} class="skeleton h-24 rounded-lg"></div>
            <table :if={@fleet} id="handoffs-table" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Shard</th>
                  <th class="py-2 text-left font-medium">Command</th>
                  <th class="py-2 text-left font-medium">Node</th>
                  <th class="py-2 text-right font-medium">Age</th>
                  <th class="py-2 text-right font-medium">State</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={c <- @fleet.pending_handoffs}
                  id={"handoff-#{c.id}"}
                  class="border-b border-base-300/50"
                >
                  <td class="num py-1.5 text-left">{c.shard_id}</td>
                  <td class="num py-1.5 text-left">{c.command}</td>
                  <td class="num py-1.5 text-left">{c.node}</td>
                  <td class="num py-1.5 text-right text-base-content/60">{age(c.inserted_at)}</td>
                  <td class="py-1.5 text-right">
                    <.badge kind={if(stale_command?(c), do: :error, else: :info)}>
                      {if(stale_command?(c), do: "stale", else: "pending")}
                    </.badge>
                  </td>
                </tr>
                <tr :if={@fleet.pending_handoffs == []}>
                  <td
                    id="handoffs-empty"
                    colspan="5"
                    class="py-6 text-center text-sm text-base-content/40"
                  >
                    No pending handoff commands.
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>
        </div>

        <.panel title="Last LB apply (this node)">
          <div :if={@lb_apply == nil} id="lb-apply-empty" class="text-sm text-base-content/40">
            No LB apply has run on this node since boot (it runs on the rebalance leader each tick).
          </div>
          <dl :if={@lb_apply} id="lb-apply" class="grid grid-cols-2 gap-4 text-sm lg:grid-cols-5">
            <div>
              <dt class="text-[11px] uppercase tracking-wide text-base-content/50">Outcome</dt>
              <dd>
                <.badge kind={apply_kind(@lb_apply.outcome)}>{@lb_apply.outcome}</.badge>
              </dd>
            </div>
            <div>
              <dt class="text-[11px] uppercase tracking-wide text-base-content/50">When</dt>
              <dd class="num">{age(@lb_apply.at)} ago</dd>
            </div>
            <div>
              <dt class="text-[11px] uppercase tracking-wide text-base-content/50">Map hash</dt>
              <dd class="num">{short_hash(@lb_apply.hash)}</dd>
            </div>
            <div>
              <dt class="text-[11px] uppercase tracking-wide text-base-content/50">Pin entries</dt>
              <dd class="num">{if(@lb_apply.entries, do: fmt_int(@lb_apply.entries), else: "—")}</dd>
            </div>
            <div :if={@lb_apply.error}>
              <dt class="text-[11px] uppercase tracking-wide text-base-content/50">Error</dt>
              <dd id="lb-apply-error" class="num break-all text-error">{@lb_apply.error}</dd>
            </div>
          </dl>
        </.panel>

        <.panel title="Recent decisions">
          <p id="decisions-scope" class="mb-3 text-xs text-base-content/50">
            In-memory on this node only: populated here when this node is the rebalance leader.
            <span :if={@last_tick}>Last tick {age(@last_tick)} ago.</span>
            <span :if={!@last_tick}>No tick has run on this node since boot.</span>
          </p>
          <div :if={@ticks == []} id="decisions-empty" class="py-4 text-sm text-base-content/40">
            No moves proposed and no hot candidates declined.
          </div>
          <div
            :for={t <- @ticks}
            id={"tick-#{t.seq}"}
            class="mb-3 rounded border border-base-300/60 p-3 text-sm"
          >
            <div class="mb-2 text-xs text-base-content/50">
              {age(t.at)} ago <span :if={t.threshold}> · hot bar {fmt_rate(t.threshold)} q/s</span>
            </div>
            <div :for={m <- t.moves} class="num">
              <.badge kind={:ok}>move</.badge>
              {m.shard_id} {m.from_node} → {m.to_node} — {m.reason}
            </div>
            <div :for={d <- t.declined} class="num text-base-content/70">
              <.badge kind={:warn}>declined</.badge>
              {d.shard_id} on {d.node_key} ({fmt_rate(d.q_per_s)} q/s) — {guard_text(d.guard)}
            </div>
            <div :if={t.declined_total > length(t.declined)} class="text-xs text-base-content/50">
              +{t.declined_total - length(t.declined)} more declined
            </div>
          </div>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end
end
