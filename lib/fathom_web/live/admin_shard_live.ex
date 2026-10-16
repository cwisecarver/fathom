defmodule FathomWeb.AdminShardLive do
  @moduledoc """
  Per-shard detail page at `/admin/shards/:id` (expert review 2026-10-15 #4 / #20): the full
  directory row, the LB pin, pending rebalancer commands, recent load samples, this node's
  coordinator state, the lease holder, and the Hrana token ledger for ONE shard.

  Cost model: the Postgres reads run once per page load in a `start_async` (never polled); this
  node's coordinator state comes from lock-free sources only (`Fathom.Admin.ShardDetail.local_state/1`);
  the lease probe (a storage read) and the coordinator liveness-mode probe (a short-timeout
  `GenServer.call`) run only on mount-connect / a button click, never from a timer.

  The id is validated with `Fathom.ShardId.cast/1`; an invalid id flashes and redirects. Every query
  is keyed on the canonical id, so this page can only ever show the one shard's rows.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.ShardDetail

  @impl true
  def mount(%{"id" => raw}, _session, socket) do
    case Fathom.ShardId.cast(raw) do
      {:ok, id} ->
        socket =
          socket
          |> assign(:page_title, "Shard #{id}")
          |> assign(:node_key, Fathom.Rebalancer.node_key())
          |> assign(:shard_id, id)
          |> assign(:cp, nil)
          |> assign(:lease, nil)
          |> assign(:mode, nil)
          |> assign(:local, ShardDetail.local_state(id))

        {:ok, if(connected?(socket), do: load(socket), else: socket)}

      :error ->
        {:ok,
         socket
         |> put_flash(:error, "Not a valid shard id")
         |> push_navigate(to: ~p"/admin/shards")}
    end
  end

  defp load(socket) do
    id = socket.assigns.shard_id

    socket
    |> start_async(:cp, fn -> ShardDetail.load_control_plane(id) end)
    |> start_async(:lease, fn -> ShardDetail.lease(id) end)
  end

  @impl true
  def handle_async(:cp, {:ok, cp}, socket), do: {:noreply, assign(socket, :cp, cp)}
  def handle_async(:cp, {:exit, _}, socket), do: {:noreply, assign(socket, :cp, :error)}
  def handle_async(:lease, {:ok, lease}, socket), do: {:noreply, assign(socket, :lease, lease)}

  def handle_async(:lease, {:exit, _}, socket),
    do: {:noreply, assign(socket, :lease, {:error, :exit})}

  def handle_async(:mode, {:ok, mode}, socket), do: {:noreply, assign(socket, :mode, mode)}
  def handle_async(:mode, {:exit, _}, socket), do: {:noreply, assign(socket, :mode, :unavailable)}

  @impl true
  def handle_event("refresh", _params, socket) do
    id = socket.assigns.shard_id

    {:noreply,
     socket
     |> assign(:local, ShardDetail.local_state(id))
     |> assign(:lease, nil)
     |> load()}
  end

  # On demand only: a GenServer.call into the coordinator (short timeout), never polled.
  def handle_event("probe_mode", _params, socket) do
    case socket.assigns.local.pid do
      pid when is_pid(pid) ->
        {:noreply,
         socket
         |> assign(:mode, :loading)
         |> start_async(:mode, fn -> ShardDetail.liveness_mode(pid) end)}

      nil ->
        {:noreply, assign(socket, :mode, :not_open)}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:shards} node_key={@node_key}>
      <div class="space-y-6" id="shard-detail">
        <div class="flex items-center justify-between">
          <h1 class="num text-lg font-semibold" id="shard-title">{@shard_id}</h1>
          <button id="shard-refresh" phx-click="refresh" class="btn btn-sm btn-ghost">
            <.icon name="hero-arrow-path" class="size-4" /> Refresh
          </button>
        </div>

        <.panel title="This node">
          <dl class="grid grid-cols-2 gap-3 text-sm md:grid-cols-4" id="shard-local">
            <.kv label="Coordinator open">{if @local.open?, do: "yes", else: "no"}</.kv>
            <.kv label="Dirty">{dirty_text(@local.dirty?)}</.kv>
            <.kv label="Since last flush">{age_text(@local.flush_age_ms)}</.kv>
            <.kv label="Liveness mode">
              <span :if={@mode == nil}>
                <button
                  id="shard-probe-mode"
                  phx-click="probe_mode"
                  class="btn btn-xs btn-outline"
                  disabled={!@local.open?}
                >
                  Inspect
                </button>
              </span>
              <span :if={@mode != nil} id="shard-mode">{mode_text(@mode)}</span>
            </.kv>
          </dl>
        </.panel>

        <.panel title="Lease">
          <div id="shard-lease" class="text-sm">
            <div :if={@lease == nil} class="skeleton h-5 w-48 rounded"></div>
            <span :if={@lease == :free} class="text-base-content/60">No live holder (free)</span>
            <span :if={match?({:held, _, _}, @lease)}>
              held by <span class="num">{elem(@lease, 1)}</span>
              , stealable {stealable_text(elem(@lease, 2))}
            </span>
            <span :if={match?({:error, _}, @lease)} class="text-error">
              lease probe failed
            </span>
          </div>
        </.panel>

        <div :if={@cp == nil} class="skeleton h-40 rounded"></div>
        <div :if={@cp == :error} class="text-sm text-error" id="shard-cp-error">
          Could not load control-plane data.
        </div>

        <div :if={is_map(@cp)} class="space-y-6" id="shard-cp">
          <.panel title="Directory row">
            <div
              :if={@cp.directory == nil}
              id="shard-no-directory"
              class="text-sm text-base-content/50"
            >
              No directory entry for this shard.
            </div>
            <dl
              :if={@cp.directory}
              class="grid grid-cols-2 gap-3 text-sm md:grid-cols-4"
              id="shard-directory"
            >
              <.kv :for={{label, value} <- directory_fields(@cp.directory)} label={label}>
                {value}
              </.kv>
            </dl>
          </.panel>

          <.panel title="LB pin">
            <div id="shard-pin" class="text-sm">
              <span :if={@cp.override == nil} class="text-base-content/50">Not pinned</span>
              <span :if={@cp.override}>
                pinned to <span class="num">{@cp.override.pinned_node}</span>
                <span :if={@cp.override.failed_at} class="text-warning">
                  (failed handoff {ts(@cp.override.failed_at)})
                </span>
                <span :if={@cp.override.reason} class="text-base-content/60">
                  — {@cp.override.reason}
                </span>
              </span>
            </div>
          </.panel>

          <.panel title="Pending rebalancer commands">
            <div :if={@cp.commands == []} id="shard-no-commands" class="text-sm text-base-content/50">
              None.
            </div>
            <table :if={@cp.commands != []} id="shard-commands" class="w-full text-sm">
              <tbody>
                <tr :for={c <- @cp.commands} class="border-b border-base-300/50">
                  <td class="num py-1.5">{c.command}</td>
                  <td class="num py-1.5 text-base-content/70">{c.node}</td>
                  <td class="num py-1.5 text-right text-base-content/60">{ts(c.inserted_at)}</td>
                </tr>
              </tbody>
            </table>
          </.panel>

          <.panel title="Recent load samples">
            <div :if={@cp.samples == []} id="shard-no-samples" class="text-sm text-base-content/50">
              No samples. Per-shard load reporting is off (set <span class="num">SHARD_LOAD=true</span>).
            </div>
            <table :if={@cp.samples != []} id="shard-samples" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Sampled</th>
                  <th class="py-2 text-left font-medium">Node</th>
                  <th class="py-2 text-right font-medium">q/s</th>
                  <th class="py-2 text-right font-medium">rows read/s</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={s <- @cp.samples} class="border-b border-base-300/50">
                  <td class="num py-1.5">{ts(s.sampled_at)}</td>
                  <td class="num py-1.5 text-base-content/70">{s.node_key}</td>
                  <td class="num py-1.5 text-right">{fmt_rate(s.q_per_s)}</td>
                  <td class="num py-1.5 text-right text-base-content/60">
                    {fmt_rate(s.rows_read_per_s)}
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>

          <.panel title="Hrana token ledger">
            <div class="mb-3 text-sm" id="shard-ledger-summary">
              <span class="num">{length(@cp.outstanding)}</span>
              outstanding issuance(s) (at or above the revocation floor)
            </div>
            <div
              :if={@cp.issuances == []}
              id="shard-no-issuances"
              class="text-sm text-base-content/50"
            >
              No tokens minted.
            </div>
            <table :if={@cp.issuances != []} id="shard-issuances" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Minted</th>
                  <th class="py-2 text-left font-medium">Version</th>
                  <th class="py-2 text-left font-medium">Scope</th>
                  <th class="py-2 text-left font-medium">Actor</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={i <- @cp.issuances} class="border-b border-base-300/50">
                  <td class="num py-1.5">{ts(i.minted_at)}</td>
                  <td class="num py-1.5">{i.token_version}</td>
                  <td class="num py-1.5">{i.scope}</td>
                  <td class="num py-1.5 text-base-content/70">{i.actor}</td>
                </tr>
              </tbody>
            </table>
            <div class="mt-3 text-xs">
              <.link navigate={~p"/admin/audit?#{[shard: @shard_id]}"} class="link">
                Audit events for this shard
              </.link>
            </div>
          </.panel>
        </div>
      </div>
    </Layouts.admin>
    """
  end

  attr :label, :string, required: true
  slot :inner_block, required: true

  defp kv(assigns) do
    ~H"""
    <div>
      <dt class="text-[11px] uppercase tracking-wide text-base-content/50">{@label}</dt>
      <dd class="num mt-0.5">{render_slot(@inner_block)}</dd>
    </div>
    """
  end

  defp directory_fields(d) do
    [
      {"status", d.status},
      {"schema_version", d.schema_version},
      {"retained_version", d.retained_version},
      {"retaining_version", d.retaining_version},
      {"token_version", d.token_version},
      {"last_active_at", ts(d.last_active_at)},
      {"last_flushed_at", ts(d.last_flushed_at)},
      {"last_verified_at", ts(d.last_verified_at)},
      {"last_verify_status", d.last_verify_status},
      {"last_snapshot_at", ts(d.last_snapshot_at)},
      {"last_retention_at", ts(d.last_retention_at)},
      {"quarantined_at", ts(d.quarantined_at)},
      {"requeue_count", d.requeue_count},
      {"migrating_since", ts(d.migrating_since)},
      {"cutover_at", ts(d.cutover_at)},
      {"retain_until", ts(d.retain_until)}
    ]
    |> Enum.map(fn {k, v} -> {k, if(is_nil(v), do: "—", else: v)} end)
  end

  defp ts(nil), do: "—"
  defp ts(%DateTime{} = dt), do: Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")

  defp dirty_text(nil), do: "—"
  defp dirty_text(true), do: "yes"
  defp dirty_text(false), do: "no"

  defp age_text(nil), do: "—"
  defp age_text(ms), do: fmt_ms(ms)

  defp mode_text(:loading), do: "…"
  defp mode_text(:not_open), do: "not open on this node"
  defp mode_text(:unavailable), do: "unavailable"
  defp mode_text(mode), do: Atom.to_string(mode)

  defp stealable_text(at_ms) do
    delta = at_ms - System.os_time(:millisecond)
    if delta <= 0, do: "now", else: "in #{fmt_ms(delta)}"
  end
end
