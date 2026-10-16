defmodule FathomWeb.AdminDurabilityLive do
  @moduledoc """
  The Durability page (expert review 2026-10-15 #1, #8, #16): the durability alarms strip, the A2
  replication panel, the Backup & DR roll-up and this node's dirtiest shards.

  Everything is read from `Fathom.Admin.Durability`, whose reads are cached and single-flight per
  node, so N open tabs cost one computation per TTL, not N. This LiveView only polls (every 5 s,
  from the connected mount on, in an async task so a slow Postgres read never blocks the socket)
  and renders. The disconnected render queries nothing.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.Durability

  @refresh_ms 5_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: send(self(), :refresh)

    {:ok,
     socket
     |> assign(:page_title, "Durability")
     |> assign(:node_key, Fathom.Rebalancer.node_key())
     |> assign(:as_of_ms, System.system_time(:millisecond))
     |> assign(:refresh_failed, false)
     |> assign(:snap, nil)
     |> assign(:repl, nil)
     |> assign(:backup, nil)
     |> assign(:dirty, nil)
     |> assign(:lag, nil)}
  end

  @impl true
  def handle_info(:refresh, socket) do
    Process.send_after(self(), :refresh, @refresh_ms)

    {:noreply,
     start_async(socket, :durability, fn ->
       # Each read is independently cached and fails soft, so one slow/failed section keeps its
       # last value rather than blanking the page.
       snapshot = Durability.snapshot()

       %{
         snapshot: snapshot,
         backup: Durability.backup(),
         dirty: Durability.dirtiest(),
         # The lag fan-out only runs when replication is on (and, being cached, once per TTL).
         lag: if(match?(%{replication: %{enabled?: true}}, snapshot), do: Durability.lag())
       }
     end)}
  end

  # Test seam and a manual-refresh hook: accepts the same map the async task produces.
  def handle_info({:durability_data, data}, socket), do: {:noreply, apply_data(socket, data)}

  @impl true
  def handle_async(:durability, {:ok, data}, socket), do: {:noreply, apply_data(socket, data)}

  def handle_async(:durability, {:exit, _reason}, socket),
    do: {:noreply, assign(socket, :refresh_failed, true)}

  defp apply_data(socket, data) do
    socket
    |> assign(:as_of_ms, System.system_time(:millisecond))
    |> assign(:refresh_failed, false)
    |> assign(:snap, Map.get(data, :snapshot, socket.assigns.snap))
    |> assign(:repl, repl_of(Map.get(data, :snapshot, socket.assigns.snap)))
    |> assign(:backup, Map.get(data, :backup, socket.assigns.backup))
    |> assign(:dirty, Map.get(data, :dirty, socket.assigns.dirty))
    |> assign(:lag, Map.get(data, :lag, socket.assigns.lag))
  end

  defp repl_of(%{replication: %{enabled?: true} = r}), do: r
  defp repl_of(_), do: nil

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin
      flash={@flash}
      active={:durability}
      node_key={@node_key}
      as_of_ms={@as_of_ms}
      stale_after_ms={15_000}
      refresh_failed={@refresh_failed}
    >
      <div id="durability-page" class="space-y-6">
        <div :if={@snap == nil} id="durability-loading" class="skeleton h-40 rounded-lg"></div>

        <.panel :if={@snap} title="Durability alarms">
          <:actions>
            <span id="alarm-window" class="text-xs text-base-content/50">
              {window_label(@snap)}
            </span>
          </:actions>
          <div id="alarm-strip" class="flex flex-wrap gap-2">
            <span
              :for={a <- @snap.alarms}
              id={"alarm-#{a.key}"}
              data-firing={to_string(a.firing?)}
              class={[
                "inline-flex items-center gap-2 rounded border px-2 py-1 text-xs",
                if(a.firing?,
                  do: "border-error/50 bg-error/15 text-error",
                  else: "border-base-300 text-base-content/60"
                )
              ]}
            >
              {a.label}
              <span class="num font-medium">{a.window}</span>
              <span :if={not a.firing? and a.total > 0} class="num text-base-content/40">
                ({a.total} since boot)
              </span>
            </span>
            <span
              id="alarm-quarantines"
              data-firing={to_string(@snap.quarantines > 0)}
              class={[
                "inline-flex items-center gap-2 rounded border px-2 py-1 text-xs",
                if(@snap.quarantines > 0,
                  do: "border-warning/50 bg-warning/15 text-warning",
                  else: "border-base-300 text-base-content/60"
                )
              ]}
            >
              Quarantine files <span class="num font-medium">{@snap.quarantines}</span>
            </span>
          </div>
          <p :if={not @snap.scrape?} id="alarm-no-scrape" class="mt-3 text-xs text-base-content/50">
            The metrics reporter is not available on this node, so alarms cannot be computed.
          </p>
        </.panel>

        <.panel :if={@snap} title="Replication (A2)">
          <div
            :if={not @snap.replication.enabled?}
            id="replication-off"
            class="text-sm text-base-content/50"
          >
            Replication is not enabled on this node.
          </div>
          <div :if={@repl} id="replication-panel" class="space-y-4">
            <div class="grid grid-cols-2 gap-3 md:grid-cols-4">
              <div id="repl-slack" class="rounded-lg border border-base-300 p-3">
                <span class="text-[11px] uppercase tracking-wide text-base-content/50">
                  Quorum slack
                </span>
                <div class="mt-1 flex items-baseline gap-2">
                  <span class={["num text-2xl font-medium", slack_class(@repl.slack)]}>{@repl.slack}</span>
                  <.badge :if={@repl.slack <= 0} kind={if @repl.slack < 0, do: :error, else: :warn}>
                    {if @repl.slack < 0, do: "writes failing", else: "one loss from failing"}
                  </.badge>
                </div>
                <div class="mt-1 text-xs text-base-content/50">
                  {@repl.connected} connected / {@repl.configured} configured / quorum {@repl.quorum}
                </div>
              </div>
              <div id="repl-budget" class="rounded-lg border border-base-300 p-3 md:col-span-3">
                <span class="text-[11px] uppercase tracking-wide text-base-content/50">
                  Replication byte budget
                </span>
                <div class="mt-2 h-2 w-full overflow-hidden rounded bg-base-300">
                  <div
                    class={["h-full", budget_class(@repl.budget_ratio)]}
                    style={"width: #{budget_pct(@repl.budget_ratio)}%"}
                  >
                  </div>
                </div>
                <div class="num mt-1 text-xs text-base-content/60">
                  {budget_pct(@repl.budget_ratio)}% used
                </div>
              </div>
            </div>

            <div
              :if={@repl.followers == []}
              id="repl-no-followers"
              class="text-sm text-base-content/50"
            >
              No followers supervised on this node.
            </div>
            <table :if={@repl.followers != []} id="repl-followers" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 pr-4 text-left font-medium">Follower</th>
                  <th class="py-2 pr-4 text-left font-medium">Address</th>
                  <th class="py-2 pr-4 text-left font-medium">Socket</th>
                  <th class="py-2 text-left font-medium">Roster</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={f <- @repl.followers}
                  id={"follower-#{f.node_key}"}
                  class="border-b border-base-300/50"
                >
                  <td class="num py-1.5 pr-4">{f.node_key}</td>
                  <td class="num py-1.5 pr-4 text-base-content/60">{f.host}:{f.port}</td>
                  <td class="py-1.5 pr-4">
                    <.badge kind={if f.connected?, do: :ok, else: :error}>
                      {if f.connected?, do: "connected", else: "down"}
                    </.badge>
                  </td>
                  <td class="py-1.5">
                    <.badge kind={alive_kind(f.alive?)}>{alive_label(f.alive?)}</.badge>
                  </td>
                </tr>
              </tbody>
            </table>

            <div class="grid grid-cols-2 gap-3 md:grid-cols-5">
              <div id="repl-catching-up">
                <.stat_tile label="Shards catching up" value={fmt_int(@repl.catching_up_shards)} />
              </div>
              <div id="repl-catchup-armed">
                <.stat_tile label="Catch-ups armed" value={fmt_int(@repl.catchup_armed)} />
              </div>
              <div id="repl-commit-p50">
                <.stat_tile label="Commit ack p50" value={ms_label(@repl.commit.p50)} />
              </div>
              <div id="repl-commit-p95">
                <.stat_tile label="Commit ack p95" value={ms_label(@repl.commit.p95)} />
              </div>
              <div id="repl-commit-p99">
                <.stat_tile label="Commit ack p99" value={ms_label(@repl.commit.p99)} />
              </div>
            </div>
            <div
              id="repl-commit-outcomes"
              class="flex flex-wrap gap-x-6 gap-y-1 text-xs text-base-content/60"
            >
              <span :for={{o, n} <- Enum.sort(@repl.commit.outcomes)} id={"repl-commit-#{o}"}>
                {o} {n}
              </span>
              <span class="text-base-content/40">commits, {window_label(@snap)}</span>
            </div>

            <div id="repl-lag" class="space-y-2">
              <div class="flex items-center gap-2 text-sm font-medium">
                Replication lag (worst sessions)
                <.badge :if={@lag && @lag.timed_out > 0} kind={:warn}>
                  <span id="repl-lag-partial">
                    partial: {@lag.answered} answered, {@lag.timed_out} timed out
                  </span>
                </.badge>
              </div>
              <div :if={@lag == nil} id="repl-lag-loading" class="text-sm text-base-content/50">
                Loading.
              </div>
              <div
                :if={@lag && @lag.worst == []}
                id="repl-lag-empty"
                class="text-sm text-base-content/50"
              >
                All sessions caught up ({@lag.answered} answered)
              </div>
              <table :if={@lag && @lag.worst != []} id="repl-lag-table" class="w-full text-sm">
                <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                  <tr class="border-b border-base-300">
                    <th class="py-2 pr-4 text-left font-medium">Shard</th>
                    <th class="py-2 pr-4 text-right font-medium">Unacked bytes</th>
                    <th class="py-2 pr-4 text-right font-medium">Behind followers</th>
                    <th class="py-2 pr-4 text-right font-medium">Catch-up fails</th>
                    <th class="py-2 pr-4 text-left font-medium">Catching up</th>
                    <th class="py-2 pr-4 text-right font-medium">Pushes in flight</th>
                    <th class="py-2 text-right font-medium">Seeds in flight</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    :for={row <- @lag.worst}
                    id={"lag-#{row.shard_id}"}
                    class="border-b border-base-300/50"
                  >
                    <td class="num py-1.5 pr-4">
                      <.link navigate={~p"/admin/shards/#{row.shard_id}"} class="link">
                        {row.shard_id}
                      </.link>
                    </td>
                    <td class="num py-1.5 pr-4 text-right">{fmt_int(row.unacked_bytes)}</td>
                    <td class="num py-1.5 pr-4 text-right">{row.behind_followers}</td>
                    <td class="num py-1.5 pr-4 text-right">{row.catchup_fails}</td>
                    <td class="py-1.5 pr-4">{if row.catching_up?, do: "yes", else: "no"}</td>
                    <td class="num py-1.5 pr-4 text-right">{row.inflight_pushes}</td>
                    <td class="num py-1.5 text-right">{row.seeds_in_flight}</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div id="repl-rates" class="flex flex-wrap gap-x-6 gap-y-1 text-xs text-base-content/60">
              <span id="repl-rate-reject">reject {rate_label(@repl.rates.reject)}</span>
              <span id="repl-rate-seed">seed {rate_label(@repl.rates.seed)}</span>
              <span id="repl-rate-disk_pressure">
                disk pressure {rate_label(@repl.rates.disk_pressure)}
              </span>
              <span id="repl-rate-promotion_raced">
                promotion raced {rate_label(@repl.rates.promotion_raced)}
              </span>
              <span id="repl-rate-recovered_from_peer">
                recovered from peer {rate_label(@repl.rates.recovered_from_peer)}
              </span>
              <span class="text-base-content/40">({window_label(@snap)})</span>
            </div>
          </div>
        </.panel>

        <.panel title="Backup & DR">
          <div :if={@backup == nil} id="backup-empty" class="text-sm text-base-content/50">
            Directory summary unavailable.
          </div>
          <div :if={@backup} id="backup-panel" class="space-y-3">
            <div class="grid grid-cols-2 gap-3 md:grid-cols-4">
              <.stat_tile label="Newest snapshot" value={ago_label(@backup.last_snapshot_at)} />
              <.stat_tile label="Never snapshotted" value={fmt_int(@backup.never_snapshotted)} />
              <.stat_tile label="No snapshot in 24 h" value={fmt_int(@backup.stale_snapshot)} />
              <.stat_tile label="Shards (not deleted)" value={fmt_int(@backup.total)} />
            </div>
            <table id="backup-verify" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 pr-4 text-left font-medium">Last verify status</th>
                  <th class="py-2 text-right font-medium">Shards</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={{status, n} <- verify_rows(@backup.verify)}
                  id={"verify-#{status || "never"}"}
                  class="border-b border-base-300/50"
                >
                  <td class="py-1.5 pr-4">
                    <.badge kind={verify_kind(status)}>{status || "never verified"}</.badge>
                  </td>
                  <td class="num py-1.5 text-right">{n}</td>
                </tr>
              </tbody>
            </table>
            <.link navigate={~p"/admin/directory"} class="text-xs text-primary">
              Directory trouble filters
            </.link>
          </div>
        </.panel>

        <.panel title="Dirtiest shards (this node)">
          <:actions>
            <.badge :if={@dirty && @dirty.under_reporting?} kind={:warn}>
              <span id="under-reporting">
                under-reporting: {@dirty.watermark_rows} of {@dirty.open_shards} open shards report
              </span>
            </.badge>
          </:actions>
          <div :if={@dirty == nil} class="text-sm text-base-content/50">Loading.</div>
          <div
            :if={@dirty && @dirty.rows == []}
            id="dirty-empty"
            class="text-sm text-base-content/50"
          >
            No shard holds unflushed writes. Age is time since the last successful S3 flush, so this
            is the second durability tier; with A2 on, quorum acks come first.
          </div>
          <table :if={@dirty && @dirty.rows != []} id="dirty-table" class="w-full text-sm">
            <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
              <tr class="border-b border-base-300">
                <th class="py-2 pr-4 text-left font-medium">Shard</th>
                <th class="py-2 pr-4 text-right font-medium">Since last S3 flush</th>
                <th class="py-2 text-right font-medium">Unflushed writes</th>
              </tr>
            </thead>
            <tbody>
              <tr
                :for={row <- @dirty.rows}
                id={"dirty-#{row.shard_id}"}
                class="border-b border-base-300/50"
              >
                <td class="num py-1.5 pr-4">
                  <.link navigate={~p"/admin/shards/#{row.shard_id}"} class="link">
                    {row.shard_id}
                  </.link>
                </td>
                <td class="num py-1.5 pr-4 text-right">{fmt_ms(row.age_ms)}</td>
                <td class="num py-1.5 text-right">{fmt_int(row.unflushed_writes)}</td>
              </tr>
            </tbody>
          </table>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end

  defp window_label(%{window_s: s}) when s < 10, do: "collecting (window starts when viewed)"
  defp window_label(%{window_s: s}), do: "last #{div(s, 60)}m #{rem(s, 60)}s"

  defp ms_label(nil), do: "-"
  defp ms_label(ms), do: fmt_ms(ms)

  defp slack_class(s) when s < 0, do: "text-error"
  defp slack_class(0), do: "text-warning"
  defp slack_class(_), do: "text-success"

  defp budget_pct(ratio) when is_number(ratio), do: ratio |> Kernel.*(100) |> round() |> min(100)
  defp budget_pct(_), do: 0

  defp budget_class(r) when is_number(r) and r > 0.8, do: "bg-error"
  defp budget_class(r) when is_number(r) and r > 0.5, do: "bg-warning"
  defp budget_class(_), do: "bg-success"

  defp alive_kind(true), do: :ok
  defp alive_kind(false), do: :error
  defp alive_kind(:unknown), do: :neutral

  defp alive_label(true), do: "alive"
  defp alive_label(false), do: "not beating"
  defp alive_label(:unknown), do: "unknown"

  defp rate_label(0), do: "0"
  defp rate_label(n) when is_integer(n), do: Integer.to_string(n)
  defp rate_label(m) when m == %{}, do: "0"
  defp rate_label(m) when is_map(m), do: Enum.map_join(m, ", ", fn {k, v} -> "#{k} #{v}" end)

  defp verify_rows(verify), do: Enum.sort_by(verify, fn {status, _} -> status || "" end)

  defp verify_kind("ok"), do: :ok
  defp verify_kind(nil), do: :neutral
  defp verify_kind(_), do: :error

  defp ago_label(nil), do: "never"

  defp ago_label(%DateTime{} = dt) do
    s = DateTime.diff(DateTime.utc_now(), dt, :second)

    cond do
      s < 120 -> "#{max(s, 0)} s ago"
      s < 7_200 -> "#{div(s, 60)} min ago"
      s < 172_800 -> "#{div(s, 3600)} h ago"
      true -> "#{div(s, 86_400)} d ago"
    end
  end
end
