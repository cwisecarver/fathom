defmodule FathomWeb.AdminComponents.NodeHealth do
  @moduledoc """
  Overview panels fed by `Fathom.Admin.MetricsCollector` (expert review 2026-10-15 #2, #6, #10,
  #15, #17): the Node-health strip, the cold-open tile, the Admission/Pressure panel and the VM
  panel. Kept out of `AdminOverviewLive` so that file's diff stays small.

  Every component reads defensively (`Map.get` with a default): the `{:metrics, map}` payload may
  predate a field, or be `nil` before the first tick.

  Threshold colouring is carried in a `data-state` attribute (`ok | warn | error | idle`) as well as
  the colour class, so tests assert state without matching markup.
  """
  use FathomWeb, :html

  import FathomWeb.AdminComponents

  # ── thresholds (one place; tests pin them) ──
  @ratio_warn 0.8
  @ratio_error 0.95
  @recorder_stale_ms 30_000

  @doc "Health state for a 0..1 used-ratio: `:ok` below 0.8, `:warn` above, `:error` above 0.95."
  @spec ratio_state(number() | nil) :: :ok | :warn | :error | :idle
  def ratio_state(nil), do: :idle
  def ratio_state(r) when r > @ratio_error, do: :error
  def ratio_state(r) when r > @ratio_warn, do: :warn
  def ratio_state(_), do: :ok

  @doc "Flush gate: `:warn` when in-flight is pinned at the cap (nothing else can flush)."
  @spec gate_state(number() | nil, number() | nil) :: :ok | :warn | :idle
  def gate_state(nil, _cap), do: :idle
  def gate_state(_in_flight, nil), do: :idle
  def gate_state(in_flight, cap) when in_flight >= cap, do: :warn
  def gate_state(_, _), do: :ok

  @doc "Directory recorder: `:warn` when the last successful flush is stale and rows are buffered."
  @spec recorder_state(map()) :: :ok | :warn | :idle
  def recorder_state(%{last_flush_age_ms: nil}), do: :idle

  def recorder_state(%{last_flush_age_ms: age, buffered: b}) when age > @recorder_stale_ms,
    do: if((b || 0) > 0, do: :warn, else: :ok)

  def recorder_state(_), do: :ok

  defp nh(m), do: (m && Map.get(m, :node_health)) || %{}
  defp mm(m, key), do: m && Map.get(m, key)

  # ───────────────────────── Node health strip ─────────────────────────

  attr :metrics, :map, default: nil

  def node_health(assigns) do
    h = nh(assigns.metrics)
    disks = Map.get(h, :disks, %{})
    gate = Map.get(h, :flush_gate, %{})
    rec = Map.get(h, :recorder, %{})

    assigns =
      assign(assigns,
        disks: Enum.sort(disks),
        gate: gate,
        rec: rec,
        proc: Map.get(h, :process_used_ratio),
        port: Map.get(h, :port_used_ratio),
        conns: Map.get(h, :connections_live),
        checkout: checkout_total(assigns.metrics)
      )

    ~H"""
    <section id="node-health" class="rounded-lg border border-base-300 bg-base-200/40 p-3">
      <div class="mb-2 text-[11px] uppercase tracking-wide text-base-content/50">Node health</div>
      <div class="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-8">
        <.cell
          :for={{dir, d} <- @disks}
          id={"nh-disk-#{dir}"}
          label={"Disk #{dir}"}
          state={ratio_state(d.used_ratio)}
          value={fmt_pct(d.used_ratio)}
          sub={"#{fmt_bytes(d.free_bytes)} free"}
        />
        <.cell
          :if={@disks == []}
          id="nh-disk-none"
          label="Disk"
          state={:idle}
          value="—"
          sub="no reading yet"
        />
        <.cell
          id="nh-procs"
          label="Processes"
          state={ratio_state(@proc)}
          value={fmt_pct(@proc)}
          sub="of +P"
        />
        <.cell
          id="nh-ports"
          label="Ports"
          state={ratio_state(@port)}
          value={fmt_pct(@port)}
          sub="of +Q"
        />
        <.cell
          id="nh-conns"
          label="SQLite conns"
          state={if(@conns, do: :ok, else: :idle)}
          value={fmt_int(@conns)}
          sub="live"
        />
        <.cell
          id="nh-flush-gate"
          label="Flush gate"
          state={gate_state(@gate[:in_flight], @gate[:cap])}
          value={gate_value(@gate)}
          sub={"#{fmt_rate(@gate[:refusals_per_s] || 0.0)} refused/s"}
        />
        <.cell
          id="nh-recorder"
          label="Dir recorder"
          state={recorder_state(@rec)}
          value={fmt_int(@rec[:buffered])}
          sub={"buffered · last flush #{age(@rec[:last_flush_age_ms])}"}
        />
        <.cell
          id="nh-memory"
          label="BEAM memory"
          state={if(mm(@metrics, :memory_bytes), do: :ok, else: :idle)}
          value={fmt_bytes(mm(@metrics, :memory_bytes))}
          sub={"#{fmt_rate(@checkout)} checkouts/s"}
        />
      </div>
    </section>
    """
  end

  attr :id, :string, required: true
  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :sub, :string, default: nil
  attr :state, :atom, default: :ok

  defp cell(assigns) do
    ~H"""
    <div id={@id} data-state={@state} class={["rounded border px-2 py-1.5", cell_class(@state)]}>
      <div class="text-[10px] uppercase tracking-wide text-base-content/50">{@label}</div>
      <div class="num text-lg font-medium leading-tight">{@value}</div>
      <div :if={@sub} class="num truncate text-[11px] text-base-content/50">{@sub}</div>
    </div>
    """
  end

  defp cell_class(:error), do: "border-error/60 bg-error/10 text-error"
  defp cell_class(:warn), do: "border-warning/60 bg-warning/10 text-warning"
  defp cell_class(_), do: "border-base-300 bg-transparent"

  defp gate_value(%{in_flight: i, cap: c}) when is_number(i) and is_number(c),
    do: "#{round(i)} / #{round(c)}"

  defp gate_value(_), do: "off"

  defp age(nil), do: "—"
  defp age(ms), do: fmt_ms(ms) <> " ago"

  defp checkout_total(nil), do: 0.0
  defp checkout_total(m), do: m |> Map.get(:checkout_per_s, %{}) |> Map.values() |> Enum.sum()

  defp fmt_pct(nil), do: "—"
  defp fmt_pct(r) when is_number(r), do: "#{Float.round(r * 100, 1)}%"

  # ───────────────────────── Cold-open tile ─────────────────────────

  attr :metrics, :map, default: nil

  @doc "Windowed (60 s) cold-open p50 with p95/p99 and the warm/cold split."
  def cold_open_tile(assigns) do
    m = assigns.metrics
    n = (m && Map.get(m, :cold_open_n)) || 0
    by = (m && Map.get(m, :cold_open_by_warm)) || %{}

    assigns =
      assign(assigns,
        n: n,
        p50: if(n > 0, do: m.cold_open_p50_ms),
        p95: if(n > 0, do: m.cold_open_p95_ms),
        p99: if(n > 0, do: m.cold_open_p99_ms),
        by: by
      )

    ~H"""
    <div id="cold-open-tile">
      <.stat_tile
        label="Cold-open p50 · 60 s"
        value={fmt_ms(@p50)}
        unit={if(@n > 0, do: "p95 #{fmt_ms(@p95)} · p99 #{fmt_ms(@p99)}", else: "no opens in window")}
      >
        <div class="num text-[11px] text-base-content/50">
          <span :for={{warm, s} <- Enum.sort(@by)} id={"cold-open-warm-#{warm}"} class="mr-2">
            {if(warm == "true", do: "warm", else: "pulled")} {s.n}
          </span>
        </div>
      </.stat_tile>
    </div>
    """
  end

  # ───────────────────────── Admission / pressure ─────────────────────────

  attr :metrics, :map, default: nil

  def admission_panel(assigns) do
    a = (assigns.metrics && Map.get(assigns.metrics, :admission)) || %{}
    open = Map.get(a, :open)
    cap = Map.get(a, :cap)
    util = if(is_number(open) and is_number(cap) and cap > 0, do: open / cap)
    rates = Map.get(a, :rates, %{})

    assigns =
      assign(assigns,
        open: open,
        cap: cap,
        util: util,
        state: ratio_state(util),
        rates: rates,
        pool: Map.get(a, :pool_hit_ratio),
        recent: Map.get(a, :recent, []),
        checkout: (assigns.metrics && Map.get(assigns.metrics, :checkout_per_s)) || %{}
      )

    ~H"""
    <.panel title="Admission / pressure">
      <div id="admission-panel" class="space-y-3">
        <div>
          <div class="mb-1 flex items-baseline justify-between text-xs">
            <span class="text-base-content/50">Open shards / cap</span>
            <span id="admission-open" class="num">
              {fmt_int(@open)} / {if(@cap, do: fmt_int(@cap), else: "no cap")}
            </span>
          </div>
          <div class="h-2 overflow-hidden rounded bg-base-300">
            <div
              :if={@util}
              id="admission-bar"
              data-state={@state}
              class={["h-full", bar_class(@state)]}
              style={"width: #{min(round(@util * 100), 100)}%"}
            >
            </div>
          </div>
        </div>

        <dl class="grid grid-cols-2 gap-x-4 gap-y-2">
          <.kv id="admission-evicted" label="Evicted / s" value={fmt_rate(@rates[:evicted])} />
          <.kv
            id="admission-at-capacity"
            label="At capacity / s"
            value={fmt_rate(@rates[:at_capacity])}
            bad={(@rates[:at_capacity] || 0) > 0}
          />
          <.kv
            id="admission-novel"
            label="Novel limited / s"
            value={fmt_rate(@rates[:novel_rate_limited])}
            bad={(@rates[:novel_rate_limited] || 0) > 0}
          />
          <.kv id="admission-pool" label="Pool hit rate" value={fmt_pct(@pool)} />
          <.kv id="admission-held" label="Held retry / s" value={fmt_rate(@rates[:held_retry])} />
          <.kv id="admission-crash" label="Crash wait / s" value={fmt_rate(@rates[:crash_wait])} />
        </dl>

        <div
          :if={@checkout != %{}}
          id="admission-checkouts"
          class="flex flex-wrap items-center gap-1.5"
        >
          <span class="text-[11px] uppercase tracking-wide text-base-content/50">checkouts/s</span>
          <.badge :for={{outcome, r} <- Enum.sort(@checkout)} kind={outcome_kind(outcome)}>
            {outcome} {fmt_rate(r)}
          </.badge>
        </div>

        <div>
          <div class="mb-1 text-[11px] uppercase tracking-wide text-base-content/50">
            Recent refused / evicted
          </div>
          <div :if={@recent == []} id="admission-recent-empty" class="text-sm text-base-content/40">
            None since this node started.
          </div>
          <ul :if={@recent != []} id="admission-recent" class="space-y-0.5">
            <li :for={e <- @recent} class="num flex justify-between gap-2 text-xs">
              <span class="truncate text-base-content/80">{e.shard_id}</span>
              <span class="text-base-content/50">{recent_label(e.kind)}</span>
            </li>
          </ul>
        </div>
      </div>
    </.panel>
    """
  end

  attr :id, :string, required: true
  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :bad, :boolean, default: false

  defp kv(assigns) do
    ~H"""
    <div id={@id} data-state={if(@bad, do: "warn", else: "ok")} class="flex justify-between gap-2">
      <dt class="text-xs text-base-content/50">{@label}</dt>
      <dd class={["num text-sm", @bad && "text-warning"]}>{@value}</dd>
    </div>
    """
  end

  defp bar_class(:error), do: "bg-error"
  defp bar_class(:warn), do: "bg-warning"
  defp bar_class(_), do: "bg-info"

  defp outcome_kind("ok"), do: :ok
  defp outcome_kind("held"), do: :warn
  defp outcome_kind("unavailable"), do: :error
  defp outcome_kind("error"), do: :error
  defp outcome_kind(_), do: :neutral

  defp recent_label(:at_capacity), do: "refused: at capacity"
  defp recent_label(:novel_rate_limited), do: "refused: novel rate"
  defp recent_label(:evicted), do: "evicted"
  defp recent_label(other), do: to_string(other)

  # ───────────────────────── VM panel ─────────────────────────

  attr :metrics, :map, default: nil

  def vm_panel(assigns) do
    vm = (assigns.metrics && Map.get(assigns.metrics, :vm)) || %{}
    rq = Map.get(vm, :run_queue, %{})
    util = Map.get(vm, :util, %{})
    mem = Map.get(vm, :memory, %{})

    assigns =
      assign(assigns,
        vm: vm,
        rq: rq,
        util: util,
        mem: Enum.filter(mem, fn {_k, v} -> is_number(v) end) |> Enum.sort_by(&elem(&1, 1), :desc)
      )

    ~H"""
    <.panel title="VM (schedulers + memory)">
      <div id="vm-panel" class="space-y-3">
        <table class="w-full text-sm">
          <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
            <tr class="border-b border-base-300">
              <th class="py-1 text-left font-medium">Scheduler</th>
              <th class="py-1 text-right font-medium">Run queue</th>
              <th class="py-1 text-right font-medium">Utilisation</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={{k, label} <- [normal: "Normal", dirty_cpu: "Dirty CPU", dirty_io: "Dirty IO"]}>
              <td class="py-1 text-left text-base-content/70">
                {label}
                <span :if={k == :dirty_io and @vm[:dirty_io_schedulers]} class="text-base-content/40">
                  ({@vm[:dirty_io_schedulers]})
                </span>
              </td>
              <td
                id={"vm-rq-#{k}"}
                data-state={rq_state(k, @rq[k])}
                class={["num py-1 text-right", rq_state(k, @rq[k]) == :warn && "text-warning"]}
              >
                {fmt_int(@rq[k])}
              </td>
              <td
                id={"vm-util-#{k}"}
                data-state={util_state(@util[k])}
                class={["num py-1 text-right", util_state(@util[k]) == :warn && "text-warning"]}
              >
                {fmt_pct(@util[k])}
              </td>
            </tr>
          </tbody>
        </table>
        <div id="vm-memory">
          <div class="mb-1 text-[11px] uppercase tracking-wide text-base-content/50">Memory</div>
          <div :if={@mem == []} class="text-sm text-base-content/40">No reading yet.</div>
          <div class="flex flex-wrap gap-x-4 gap-y-1">
            <span :for={{k, v} <- @mem} id={"vm-mem-#{k}"} class="num text-xs">
              <span class="text-base-content/50">{k}</span> {fmt_bytes(v)}
            </span>
          </div>
        </div>
      </div>
    </.panel>
    """
  end

  # A dirty-IO queue that is non-empty means NIF work is waiting for a thread (every query is one);
  # any queue on the normal schedulers above the scheduler count is a backlog too.
  defp rq_state(:dirty_io, n) when is_number(n) and n > 0, do: :warn
  defp rq_state(:dirty_cpu, n) when is_number(n) and n > 0, do: :warn
  defp rq_state(:normal, n) when is_number(n) and n > 8, do: :warn
  defp rq_state(_, nil), do: :idle
  defp rq_state(_, _), do: :ok

  defp util_state(u) when is_number(u) and u > 0.9, do: :warn
  defp util_state(nil), do: :idle
  defp util_state(_), do: :ok
end
