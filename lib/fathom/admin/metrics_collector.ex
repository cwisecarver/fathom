defmodule Fathom.Admin.MetricsCollector do
  @moduledoc """
  The realtime backend for the admin dashboard: one supervised node-local GenServer that, every
  tick, reads this node's live metrics and fans them out over `Phoenix.PubSub` to every dashboard
  LiveView — so N viewers cost one set of reads, not N (the reporter's "one reader, publish" model).

  ## Where each series comes from

  Two planes, matching the architecture (there is no BEAM cluster):

    * **Per-shard + node-direct** (cheap, cardinality-bound): `Fathom.ShardLoad` snapshots diffed
      into per-shard **query rates** for the hot-shard table (top N), on a 10 s cadence rather
      than every tick (expert review 2026-10-17 #1); node QPS comes from the scrape-derived
      query rate, not a per-shard sum,
      `Fathom.Admin.FlushWatermark` + `Fathom.Shard.WriteCounter` for **dirty shards / RPO age**,
      `Registry.count/1` for **open shards**, `:erlang.memory/0` for **memory**.
    * **Aggregate metrics** (low cardinality): read from the in-process Prometheus reporter's
      aggregation via `TelemetryMetricsPrometheus.Core.scrape/1` + `Fathom.Admin.PrometheusScrape`
      — windowed **query-latency percentiles** (a diff of the cumulative histogram between ticks),
      **cold-open p50**, **S3 op/byte rates** and **checkout-outcome rates** (counter diffs). One
      instrumentation, read here for realtime; the same reporter also serves `/metrics` for Grafana.

  Fleet roll-ups (Postgres directory / migrations / node roster) are **not** here — they're slower
  and read by the LiveView via `assign_async`, so Postgres latency never delays the realtime push.

  Storage footprint (`Fathom.Shard.Storage.stored_usage/0`) can be a full S3 LIST, so it's polled
  on a separate slow cadence and cached; the tick reads the cache.

  Gated by `Fathom.Admin.enabled?/0` (off in test). Broadcasts `{:metrics, map}` on
  `"admin:node:" <> node_key`; a LiveView calls `snapshot/0` for the initial paint then subscribes.

  Each tick also publishes `current` to a small public ETS table (`current/0`), so readers that only
  need the latest map (NodeHealth on the Reporter's beat, LiveView mounts) never queue behind the
  collector's mailbox (expert review 2026-10-17 #17).
  """
  use GenServer

  alias Fathom.Admin.{FlushWatermark, PrometheusScrape}
  alias Fathom.ShardLatency
  alias Fathom.ShardLoad

  @default_tick_ms 1_000

  # How often the O(open-shards) RPO walk may actually run, regardless of tick rate
  # (expert review 2026-08-26 #27). Matches `Fathom.Admin.Measurements.durability/0`'s poller
  # cadence, so the dashboard's number is no staler than the Prometheus gauge derived from the
  # identical reduce.
  @rpo_interval_ms 10_000
  # The per-shard hot-set pass (a full ShardLoad walk) runs at the Reporter's cadence, not every
  # tick (expert review 2026-10-17 #1). `:admin_hot_interval_ms` overrides (tests).
  @default_hot_interval_ms 10_000
  @current_table Module.concat(__MODULE__, Current)
  # Expert review 2026-10-17 #26: every node LISTs the WHOLE bucket for this one footprint gauge,
  # so fleet LIST traffic is nodes x keys / cadence (xmerl SAX per 1000-key page; at 1M keys the
  # old 30 s budget killed the poll before it ever published). 10 min is plenty for a storage
  # footprint number. Every node still polls, deliberately: the dashboard's "Bytes stored" is
  # per-node, and leader-only polling would leave it blank on every follower (a fleet-wide share
  # would need a Postgres row; parked, see the report). The first poll still runs at boot.
  @default_usage_ms 600_000
  # Budget for one storage-usage poll (an S3 LIST); a slower one is killed so it can't pin the
  # overlap-guard slot (#22). Comfortably under the 10 min poll cadence.
  @default_usage_timeout_ms 120_000
  @hot_n 20
  # Bounded ring of recent points for the hero charts, so a freshly-connected LiveView can paint a
  # populated chart from snapshot/0 instead of waiting to accumulate ticks. ~5 min at 1 s.
  @ring 300
  @s3_methods ~w(get put head delete post)

  # Cold opens and flushes are sparse (a handful a minute on a warm node), so the 1 s diff used for
  # query latency reads as 0 almost always. Their percentiles are windowed over a longer sliding
  # span: the current cumulative histogram minus the oldest retained snapshot (expert review
  # 2026-10-15 #6). Reading the raw cumulative histogram, as this once did for cold opens, is a
  # LIFETIME percentile: it flattens after boot and can never show a current S3-pull regression.
  @cold_window_ms 60_000
  @flush_window_ms 30_000
  @admission_recent 20

  # Window behind the Errors / Data-plane auth "in window" counts and the Cluster page's
  # "Events / 5 min" strip (the Overview scope badge says "5m window"). Cumulative counter totals
  # are snapshotted each tick and the window is `now - oldest retained`; no second scrape.
  @events_window_ms 300_000
  @window_tags [:errors, :no_quorum, :auth, :events]
  @error_classes ~w(timeout too_large busy sql_error no_quorum other)
  @auth_reasons ~w(expired invalid wrong_shard revoked missing other)
  # Cluster-page event counters (summed across tags): key => Prometheus family prefix.
  @event_counters [
    flush_failed: "fathom_shard_flush_failed",
    lease_superseded: "fathom_shard_lease_superseded",
    fenced_quarantine: "fathom_shard_fenced_quarantine",
    corrupt_flush: "fathom_shard_corrupt_flush",
    replication_reject: "fathom_replication_reject",
    at_capacity: "fathom_shards_at_capacity",
    heartbeat_lapsed: "fathom_shard_heartbeat_lapsed"
  ]
  # Cumulative counters (summed across tags) whose per-second rate the Admission panel shows.
  @admission_counters [
    evicted: "fathom_shards_evicted",
    at_capacity: "fathom_shards_at_capacity",
    novel_rate_limited: "fathom_shards_novel_rate_limited",
    held_retry: "fathom_shards_held_retry",
    crash_wait: "fathom_shards_crash_wait"
  ]

  @doc false
  def start_link(opts) do
    # fullsweep_after: 0 — the hot-shard baseline map is N-sized and replaced wholesale every pass;
    # default GC lets the superseded one linger (expert review 2026-10-17 #18).
    GenServer.start_link(__MODULE__, opts, name: __MODULE__, spawn_opt: [fullsweep_after: 0])
  end

  @doc "PubSub topic this node broadcasts `{:metrics, map}` on."
  @spec topic() :: String.t()
  def topic, do: "admin:node:" <> Fathom.Rebalancer.node_key()

  @doc "The latest computed metrics + the hero-chart history ring (for a LiveView's initial paint)."
  @spec snapshot() :: %{current: map() | nil, history: [map()]}
  def snapshot do
    GenServer.call(__MODULE__, :snapshot)
  catch
    :exit, _ -> %{current: nil, history: []}
  end

  @doc """
  The latest computed metrics map, read straight from ETS — no `GenServer.call`, so it answers
  even while the collector is busy mid-tick or suspended (expert review 2026-10-17 #17). `nil`
  before the first tick or when the collector is not running. Callers that also need the history
  ring still use `snapshot/0`.
  """
  @spec current() :: map() | nil
  def current do
    case :ets.lookup(@current_table, :current) do
      [{:current, c}] -> c
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @impl true
  def init(_opts) do
    :ets.new(@current_table, [:set, :public, :named_table, read_concurrency: true])

    state = %{
      node_key: Fathom.Rebalancer.node_key(),
      prev_load: %{},
      prev_mono: mono_ms(),
      # Hot-set pass bookkeeping: when it last ran (nil forces a pass on the first tick), the
      # monotonic ms its `prev_load` baseline was taken, its cached result, and a count of REAL
      # walks (a seam for asserting the 1 s tick does not walk ShardLoad).
      hot_at: nil,
      hot_prev_mono: mono_ms(),
      hot: [],
      hot_walks: 0,
      prev_query_buckets: [],
      prev_counters: %{},
      # Protocol/S3 counter snapshot behind Fathom.Telemetry.Protocol.window/2.
      prev_proto: nil,
      # Snapshot history ([{mono_ms, %{key => cumulative_buckets}}], newest first) behind the
      # sparse-histogram windows (see @cold_window_ms).
      hists: %{cold: [], flush: []},
      # Per-shard latency histograms from last tick (shard_id → raw count vector), for the reported
      # hot shards only — the windowing baseline (#12). usage_task holds the in-flight storage-usage
      # poll (overlap guard, #22). Both expert review 2026-07-14.
      prev_latency: %{},
      usage_task: nil,
      usage: nil,
      # Cached RPO reduce + when it was taken (#27). nil forces a walk on the first tick.
      rpo: {0, 0},
      rpo_at: nil,
      # Count of REAL walks. Exists so the throttle can be asserted without depending on
      # millisecond clock resolution — several ticks inside one millisecond leave `rpo_at`
      # numerically unchanged whether or not the walk re-ran, which made a stamp-based test
      # report the wrong failure.
      rpo_walks: 0,
      # {FlushWatermark rows, open shards} at the last RPO walk: rows < open means the dirty/RPO
      # readout under-reports (some open shard has published no watermark). Same 10 s cadence.
      rpo_cov: {0, 0},
      # [{mono_ms, %{{tag, key} => cumulative}}] newest first, behind the 5 min event windows.
      win_hist: [],
      rings: [],
      current: nil
    }

    # Which shard was refused/evicted goes to the bounded EventRing (review 2026-10-15 #10);
    # the handler fires on those failure branches only. Left attached if this process dies:
    # `EventRing.push/2` is a no-op without its table, and `attach/0` re-binds idempotently.
    Fathom.Admin.AdmissionEvents.attach()
    Fathom.Telemetry.Protocol.attach()

    schedule(:tick, tick_ms())
    send(self(), :usage_poll)
    {:ok, state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, %{current: state.current, history: Enum.reverse(state.rings)}, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = tick(state)
    :ets.insert(@current_table, {:current, state.current})
    Phoenix.PubSub.broadcast(Fathom.PubSub, topic(), {:metrics, state.current})
    schedule(:tick, tick_ms())
    {:noreply, state}
  end

  # Storage footprint on a slow cadence (an S3 LIST is expensive) — cache it + publish it as a
  # gauge so Prometheus/Grafana also see it. Runs as a SUPERVISED, TIMED, OVERLAP-GUARDED task
  # (never an unlinked/unbounded `Task.start`), off the realtime tick — expert review 2026-07-14
  # #22. A poll already in flight is skipped rather than piling up orphan S3 LISTs.
  def handle_info(:usage_poll, %{usage_task: task} = state) when not is_nil(task) do
    schedule(:usage_poll, usage_ms())
    {:noreply, state}
  end

  def handle_info(:usage_poll, state) do
    task =
      Task.Supervisor.async_nolink(Fathom.Admin.TaskSupervisor, fn ->
        case Fathom.Shard.Storage.stored_usage() do
          {objects, bytes} when is_integer(objects) and is_integer(bytes) -> {objects, bytes}
          _ -> nil
        end
      end)

    # async_nolink monitors but doesn't time out on its own; arm our own budget so a wedged LIST
    # can't pin the slot forever (we kill it in the :usage_timeout clause below).
    timer = Process.send_after(self(), {:usage_timeout, task.ref}, usage_timeout_ms())
    schedule(:usage_poll, usage_ms())
    {:noreply, %{state | usage_task: %{ref: task.ref, pid: task.pid, timer: timer}}}
  end

  # The poll task's result (async_nolink delivers `{ref, result}`): cache + republish it as a gauge,
  # then drop the pending DOWN + our timeout and free the slot.
  def handle_info({ref, result}, %{usage_task: %{ref: ref, timer: timer}} = state) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(timer)

    state =
      case result do
        {objects, bytes} ->
          :telemetry.execute([:fathom, :storage, :usage], %{objects: objects, bytes: bytes}, %{})
          %{state | usage: {objects, bytes}}

        _ ->
          state
      end

    {:noreply, %{state | usage_task: nil}}
  end

  # The poll task crashed (async_nolink monitors, doesn't link) — free the slot so the next tick polls.
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %{usage_task: %{ref: ref, timer: timer}} = state
      ) do
    Process.cancel_timer(timer)
    {:noreply, %{state | usage_task: nil}}
  end

  # The poll overran its budget — kill the task and free the slot (a wedged LIST never wedges polling).
  def handle_info({:usage_timeout, ref}, %{usage_task: %{ref: ref, pid: pid}} = state) do
    Task.Supervisor.terminate_child(Fathom.Admin.TaskSupervisor, pid)
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | usage_task: nil}}
  end

  # Late/stale reply, DOWN, or timeout for an already-cleared poll — ignore (never crash the tick).
  def handle_info({ref, _result}, state) when is_reference(ref), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) when is_reference(ref),
    do: {:noreply, state}

  def handle_info({:usage_timeout, _ref}, state), do: {:noreply, state}

  # --- the tick computation (kept as small pure-ish steps for readability) ---

  defp tick(state) do
    t0_us = System.monotonic_time(:microsecond)
    now = mono_ms()
    window_s = max((now - state.prev_mono) / 1000, 0.001)

    # The per-shard hot set is a full ShardLoad walk (O(resident shards)), and `:shard_load` is ON
    # in prod, so it runs on a 10 s cadence, never every tick (expert review 2026-10-17 #1).
    # Between passes the last hot list is served as-is; the Overview hot-shards table therefore
    # updates every 10 s. node_qps no longer needs the walk at all: it is the scrape-derived
    # query rate below.
    {state, hot} = hot_throttled(state, now)

    # Scrape-derived windowed series (query-latency percentiles + counter rates + cold-open and
    # flush percentiles). On a blank/failed scrape the diff baselines are HELD, not advanced, so
    # recovery never spikes (#11); see scrape_step/6. The scrape is INDEXED once (review
    # 2026-10-15 #23): every lookup below is a map hit, not a scan of the sample list.
    samples = PrometheusScrape.parse(safe_scrape())
    idx = PrometheusScrape.index(samples)

    s =
      scrape_step(
        # a blank scrape stays `[]` (the held-baseline clause); otherwise hand over the index
        if(samples == [], do: [], else: idx),
        state.prev_query_buckets,
        state.prev_counters,
        window_s,
        state.hists,
        now
      )

    {protocol, prev_proto} = Fathom.Telemetry.Protocol.window(state.prev_proto, window_s)

    node_qps = s.counter_rates |> Map.get(:queries, %{}) |> Map.get(:total, 0.0)

    {state, {dirty, oldest_rpo}} = rpo_throttled(state)

    {rpo_rows, rpo_open} = state.rpo_cov

    {win, win_hist} =
      window_counts(state.win_hist, now, window_snapshot(s.counters), @events_window_ms)

    cold = cold_open_stats(s.cold_win)
    flush_p99 = PrometheusScrape.percentile_cumulative(Map.get(s.flush_win, "all", []), 99)

    current = %{
      node_key: state.node_key,
      at_ms: system_ms(),
      open_shards: Fathom.Shards.open_count(),
      memory_bytes: :erlang.memory(:total),
      node_qps: node_qps,
      query_p50_ms: PrometheusScrape.percentile_cumulative(s.win, 50),
      query_p95_ms: PrometheusScrape.percentile_cumulative(s.win, 95),
      query_p99_ms: PrometheusScrape.percentile_cumulative(s.win, 99),
      cold_open_p50_ms: cold.all.p50,
      cold_open_p95_ms: cold.all.p95,
      cold_open_p99_ms: cold.all.p99,
      cold_open_n: cold.all.n,
      cold_open_by_warm: Map.delete(cold, :all),
      flush_p99_ms: flush_p99,
      dirty_shards: dirty,
      oldest_rpo_ms: oldest_rpo,
      rpo_rows: rpo_rows,
      rpo_open: rpo_open,
      rpo_under_reporting: rpo_rows < rpo_open,
      errors: error_summary(s.counter_rates, win),
      no_quorum: group_summary(s.counter_rates, win, :no_quorum),
      auth_rejected: group_summary(s.counter_rates, win, :auth),
      events_5m: group_window(win, :events),
      s3_ops_per_s: s.counter_rates.s3_ops,
      s3_bytes_per_s: s.counter_rates.s3_bytes,
      checkout_per_s: s.counter_rates.checkout,
      storage_objects: usage_elem(state.usage, 0),
      storage_bytes: usage_elem(state.usage, 1),
      hot_shards: hot,
      node_health: node_health(idx, Map.get(s.counter_rates, :health, %{})),
      admission: admission(Map.get(s.counter_rates, :admission, %{})),
      vm: vm(idx),
      protocol: protocol
    }

    tick_ms = (System.monotonic_time(:microsecond) - t0_us) / 1000
    current = Map.put(current, :collector_tick_ms, tick_ms)
    :telemetry.execute([:fathom, :admin, :collector], %{tick_ms: tick_ms}, %{})

    ring = ring_point(current)

    %{
      state
      | prev_mono: now,
        prev_query_buckets: s.query_buckets,
        prev_counters: s.counters,
        prev_proto: prev_proto,
        hists: s.hists,
        win_hist: win_hist,
        current: current,
        rings: [ring | state.rings] |> Enum.take(@ring)
    }
  end

  @doc false
  # Attaches WINDOWED p50/p95/p99 (ms) to each hot shard and returns the next `prev_latency`
  # baseline (only the reported shards, so it stays bounded). ShardLatency histograms are
  # lifetime-cumulative (reset only by forget/1), so a shard slow an hour ago would otherwise
  # show a permanently elevated p99 next to the node-wide 1s-windowed p99 (#12). We diff each hot
  # shard's current histogram against last tick's — clamped ≥ 0 per bucket to survive a coordinator
  # reset/forget (mirrors rate/3's reset clamp) — and take percentiles over the window. A shard on
  # its first appearance (no prior sample) falls back to its cumulative histogram for that tick.
  def window_latency(hot, prev_latency) do
    cur = Map.new(hot, fn s -> {s.shard_id, ShardLatency.histogram(s.shard_id)} end)

    hot =
      Enum.map(hot, fn s ->
        c = Map.fetch!(cur, s.shard_id)

        windowed =
          case Map.get(prev_latency, s.shard_id) do
            nil -> c
            p -> diff_hist(c, p)
          end

        Map.merge(s, ShardLatency.percentiles_from_histogram(windowed))
      end)

    {hot, cur}
  end

  defp diff_hist(cur, prev), do: Enum.zip_with(cur, prev, fn c, p -> max(c - p, 0) end)

  @doc false
  # The scrape-derived windowed series. On a blank/failed scrape (`scrape == []`, i.e. the reporter
  # errored/timed out) the diff baselines are HELD, not advanced, and this tick emits zeros — so
  # the NEXT (recovered) tick diffs against the last GOOD scrape rather than against zeros. Storing
  # the empty scrape as the baseline (the old behaviour) made recovery read the entire cumulative
  # count as one window's worth — `rate(N, 0.0, window) = N` — a huge false spike (same for the
  # query-latency percentiles, which would reflect the lifetime distribution). Expert review
  # 2026-07-14 #11. Returns the emitted values + the next diff baselines.
  #
  # `hists` carries the snapshot history for the sparse cold-open / flush windows and `now` is a
  # monotonic ms; both default so a caller that predates them still works.
  def scrape_step(
        scrape,
        prev_query_buckets,
        prev_counters,
        window_s,
        hists \\ %{cold: [], flush: []},
        now \\ 0
      )

  def scrape_step([], prev_query_buckets, prev_counters, _window_s, hists, _now) do
    %{
      win: [],
      cold_buckets: [],
      cold_win: %{},
      flush_win: %{},
      counter_rates: %{s3_ops: %{}, s3_bytes: %{}, checkout: %{}},
      query_buckets: prev_query_buckets,
      counters: prev_counters,
      hists: hists
    }
  end

  def scrape_step(scrape, prev_query_buckets, prev_counters, window_s, hists, now) do
    idx = PrometheusScrape.index(scrape)
    query_buckets = PrometheusScrape.buckets(idx, "fathom_shard_query_duration")
    win = PrometheusScrape.diff_buckets(query_buckets, prev_query_buckets)
    {counters, counter_rates} = counter_rates(idx, prev_counters, window_s)

    cold_cur = hist_snapshot(idx, "fathom_shard_cold_open_duration", "warm")
    flush_cur = hist_snapshot(idx, "fathom_shard_flush_duration", nil)
    {cold_win, cold_hist} = window_hist(hists.cold, now, cold_cur, @cold_window_ms)
    {flush_win, flush_hist} = window_hist(hists.flush, now, flush_cur, @flush_window_ms)

    %{
      win: win,
      cold_buckets: Map.get(cold_cur, "all", []),
      cold_win: cold_win,
      flush_win: flush_win,
      counter_rates: counter_rates,
      query_buckets: query_buckets,
      counters: counters,
      hists: %{cold: cold_hist, flush: flush_hist}
    }
  end

  # `%{"all" => buckets}` plus one entry per value of `split_label` (e.g. warm=true|false).
  defp hist_snapshot(idx, prefix, split_label) do
    split =
      if split_label do
        Map.new(PrometheusScrape.label_values(idx, prefix, split_label), fn v ->
          {v, PrometheusScrape.buckets(idx, prefix, %{split_label => v})}
        end)
      else
        %{}
      end

    Map.put(split, "all", PrometheusScrape.buckets(idx, prefix))
  end

  @doc """
  Windows a set of cumulative histograms over the last `window_ms` (expert review 2026-10-15 #6).

  `hist` is `[{mono_ms, %{key => cumulative_buckets}}]`, newest first; `cur` is this tick's
  `%{key => cumulative_buckets}`. Returns `{%{key => windowed_buckets}, next_hist}`, where the
  window is `cur` minus the OLDEST retained snapshot. With no history there is no baseline, so the
  window is empty (zero), never the lifetime distribution. A key first seen mid-window (a new tag
  value) is diffed against nothing, i.e. counted whole. Per-bucket clamped at 0 so a counter
  reset cannot go negative.
  """
  @spec window_hist([{integer(), map()}], integer(), map(), pos_integer()) ::
          {map(), [{integer(), map()}]}
  def window_hist(hist, now, cur, window_ms) do
    kept = Enum.filter(hist, fn {t, _} -> now - t <= window_ms end)

    base =
      case List.last(kept) do
        nil -> cur
        {_t, snap} -> snap
      end

    win =
      Map.new(cur, fn {k, buckets} ->
        {k, PrometheusScrape.diff_buckets(buckets, Map.get(base, k, []))}
      end)

    {win, [{now, cur} | kept]}
  end

  # p50/p95/p99 + sample count per window: `%{all: stats, "true" => stats, "false" => stats}`.
  defp cold_open_stats(cold_win) do
    empty = %{p50: 0.0, p95: 0.0, p99: 0.0, n: 0}

    cold_win
    |> Map.new(fn {k, b} ->
      n =
        case List.last(b) do
          {_, c} -> round(c)
          nil -> 0
        end

      {if(k == "all", do: :all, else: k),
       %{
         p50: PrometheusScrape.percentile_cumulative(b, 50),
         p95: PrometheusScrape.percentile_cumulative(b, 95),
         p99: PrometheusScrape.percentile_cumulative(b, 99),
         n: n
       }}
    end)
    |> Map.put_new(:all, empty)
  end

  @doc """
  The history-ring point for a `current` metrics map (the shape the hero charts and sparklines
  read). Public so the Overview LiveView folds each broadcast into its local history with the
  SAME function the collector uses to build the ring, instead of a second copy of the field list.
  """
  @spec ring_point(map()) :: map()
  def ring_point(m) do
    %{
      t: m.at_ms,
      qps: m.node_qps,
      p50: m.query_p50_ms,
      p95: m.query_p95_ms,
      p99: m.query_p99_ms,
      mem: Map.get(m, :memory_bytes),
      checkout: m |> Map.get(:checkout_per_s, %{}) |> Map.values() |> Enum.sum(),
      flush_p99: Map.get(m, :flush_p99_ms),
      cold_p99: Map.get(m, :cold_open_p99_ms),
      dirty: Map.get(m, :dirty_shards),
      err: get_in(m, [:errors, :total_rate]) || 0.0,
      err_ratio: get_in(m, [:errors, :ratio]) || 0.0
    }
  end

  @doc """
  Windows the cumulative counter totals in `cur` (`%{{tag, key} => total}`) over the last
  `window_ms`: `{%{{tag, key} => count_in_window}, next_hist}`. Same shape as `window_hist/4`: the
  window is `cur` minus the OLDEST retained snapshot, empty with no history, clamped at 0 so a
  counter reset cannot go negative.
  """
  @spec window_counts([{integer(), map()}], integer(), map(), pos_integer()) ::
          {map(), [{integer(), map()}]}
  def window_counts(hist, now, cur, window_ms) do
    kept = Enum.filter(hist, fn {t, _} -> now - t <= window_ms end)

    base =
      case List.last(kept) do
        nil -> cur
        {_t, snap} -> snap
      end

    win = Map.new(cur, fn {k, v} -> {k, max(v - Map.get(base, k, v), 0.0)} end)
    {win, [{now, cur} | kept]}
  end

  defp window_snapshot(counters) do
    Map.filter(counters, fn
      {{tag, _k}, _v} -> tag in @window_tags
      _ -> false
    end)
  end

  defp group_window(win, tag), do: for({{^tag, k}, v} <- win, into: %{}, do: {k, round(v)})

  defp group_summary(rates, win, tag) do
    %{rates: Map.get(rates, tag, %{}), window: group_window(win, tag)}
  end

  # Query errors: per-class rates and window counts, plus the windowed error ratio
  # errors / (errors + queries) from the same 1 s diff (queries = the latency histogram's count).
  defp error_summary(rates, win) do
    er = Map.get(rates, :errors, %{})
    total = er |> Map.values() |> Enum.sum()
    q = rates |> Map.get(:queries, %{}) |> Map.get(:total, 0.0)

    %{
      rates: er,
      window: group_window(win, :errors),
      total_rate: total,
      query_rate: q,
      ratio: if(total + q > 0, do: total / (total + q), else: 0.0)
    }
  end

  # Node-health strip: the 10 s poller gauges, read from the scrape (nil = gauge absent).
  defp node_health(idx, rates) do
    gauge = fn name, labels -> PrometheusScrape.value(idx, name, labels, nil) end

    disks =
      for dir <- PrometheusScrape.label_values(idx, "fathom_node_disk_used_ratio", "dir"),
          into: %{} do
        {dir,
         %{
           used_ratio: gauge.("fathom_node_disk_used_ratio", %{"dir" => dir}),
           free_bytes: gauge.("fathom_node_disk_free_bytes", %{"dir" => dir})
         }}
      end

    %{
      disks: disks,
      process_used_ratio: gauge.("fathom_node_process_used_ratio", %{}),
      port_used_ratio: gauge.("fathom_node_port_used_ratio", %{}),
      connections_live: gauge.("fathom_node_connections_live", %{}),
      flush_gate: %{
        in_flight: gauge.("fathom_shard_flush_gate_in_flight", %{}),
        cap: gauge.("fathom_shard_flush_gate_cap", %{}),
        refusals_per_s: Map.get(rates, :flush_gate_refusals, 0.0)
      },
      recorder: %{
        buffered: gauge.("fathom_directory_recorder_buffered", %{}),
        flush_buffered: gauge.("fathom_directory_recorder_flush_buffered", %{}),
        last_flush_age_ms: gauge.("fathom_directory_recorder_last_flush_age_ms", %{})
      }
    }
  end

  # Admission/pressure panel: open vs the soft cap, refusal/eviction rates, pool hit rate, and the
  # bounded ring of recent refused/evicted shard ids (`Fathom.Admin.EventRing`).
  defp admission(rates) do
    # WINDOWED hit ratio from the 1 s take rates, not the lifetime counters (a lifetime ratio
    # flattens after boot, same defect as the cold-open tile).
    hit = Map.get(rates, :pool_hit, 0.0)
    miss = Map.get(rates, :pool_miss, 0.0)

    %{
      open: Fathom.Shards.open_count(),
      cap: max_open_shards(),
      rates: rates,
      pool_hit_ratio: if(hit + miss > 0, do: hit / (hit + miss), else: nil),
      recent: Fathom.Admin.EventRing.recent(:admission, @admission_recent)
    }
  end

  defp max_open_shards do
    case Application.get_env(:fathom, :max_open_shards, :infinity) do
      n when is_integer(n) and n > 0 -> n
      _ -> nil
    end
  end

  # VM panel: run queues + scheduler utilisation + memory breakdown (10 s poller gauges).
  defp vm(idx) do
    group = fn prefix, keys ->
      Map.new(keys, fn k -> {k, PrometheusScrape.value(idx, "#{prefix}_#{k}", %{}, nil)} end)
    end

    %{
      run_queue: group.("fathom_node_run_queue", [:normal, :dirty_cpu, :dirty_io]),
      util: group.("fathom_node_scheduler_util", [:normal, :dirty_cpu, :dirty_io]),
      memory:
        group.("fathom_node_memory_breakdown", [:processes, :binary, :ets, :code, :atom, :system]),
      schedulers: :erlang.system_info(:schedulers_online),
      dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers),
      dirty_cpu_schedulers: :erlang.system_info(:dirty_cpu_schedulers_online)
    }
  end

  # The throttled hot-set pass: a real ShardLoad walk at most every `hot_interval_ms`, otherwise the
  # cached result. The rate window is the time since the PREVIOUS pass (not the 1 s tick), and the
  # per-hot-shard latency baseline advances only with it, so both stay consistent.
  defp hot_throttled(%{hot_at: at} = state, now) do
    if is_nil(at) or now - at >= hot_interval_ms() do
      window_s = max((now - state.hot_prev_mono) / 1000, 0.001)
      {hot, load} = hot_shards(state.prev_load, window_s)

      # Per-shard tail latency for the top-N hot shards only (cardinality-bounded: read cost never
      # scales with resident-shard count), windowed against the previous pass (#12).
      {hot, latency} = window_latency(hot, state.prev_latency)

      {%{
         state
         | hot: hot,
           hot_at: now,
           hot_prev_mono: now,
           hot_walks: state.hot_walks + 1,
           prev_load: load,
           prev_latency: latency
       }, hot}
    else
      {state, state.hot}
    end
  end

  # One pass over the raw ShardLoad tuples: keep the top-@hot_n by rate, and carry forward the
  # baseline for every resident shard.
  #
  # Returns `{hot_maps, next_prev}`. Maps are allocated for the survivors only —
  # `prev` is `%{shard_id => {queries, rows_read, rows_written}}`, a 3-tuple rather than the
  # 5-key map the old `load_map/0` retained for every resident shard.
  defp hot_shards(prev, window_s) do
    {heap, next_prev} =
      Enum.reduce(ShardLoad.snapshot_tuples(), {[], %{}}, fn
        {id, _checkouts, queries, rows_read, rows_written}, {heap, next} ->
          {pq, prr, prw} = Map.get(prev, id, {0, 0, 0})
          q_per_s = rate(queries, pq, window_s)

          # EVERY resident shard keeps a baseline, including one that did not move. Dropping
          # unmoved shards (which is the memory reduction the review suggested) is incorrect:
          # next pass `Map.get(prev, id, {0, 0, 0})` would return zeros and the rate would
          # compute as `queries / window` — a large spurious rate for an idle shard, which is
          # exactly the hot-shard signal this feeds. The allocation win is the 3-tuple in place
          # of the old 5-key map, which is per-row and does not depend on the filter.
          next = Map.put(next, id, {queries, rows_read, rows_written})

          heap =
            if q_per_s > 0.0 do
              keep_top(
                heap,
                {q_per_s, id, queries, rate(rows_read, prr, window_s),
                 rate(rows_written, prw, window_s)}
              )
            else
              heap
            end

          {heap, next}
      end)

    hot =
      heap
      |> Enum.sort_by(fn {q, _, _, _, _} -> q end, :desc)
      |> Enum.map(fn {q, id, queries, rr, rw} ->
        %{
          shard_id: id,
          q_per_s: q,
          rows_read_per_s: rr,
          rows_written_per_s: rw,
          queries: queries
        }
      end)

    {hot, next_prev}
  end

  # A bounded "keep the N largest" list. At most @hot_n elements are ever retained, so the
  # sort at the end is over 20 items rather than the whole resident set.
  defp keep_top(heap, entry) when length(heap) < @hot_n, do: [entry | heap]

  defp keep_top(heap, {q, _, _, _, _} = entry) do
    {min_q, _, _, _, _} = min = Enum.min_by(heap, fn {hq, _, _, _, _} -> hq end)
    if q > min_q, do: [entry | List.delete(heap, min)], else: heap
  end

  # Diff the tracked cumulative counters (S3 ops/bytes by method, checkout counts by outcome,
  # admission refusals/evictions, flush-gate refusals) into per-second rates. Returns the fresh
  # cumulative map (next tick's baseline) + the grouped rates.
  defp counter_rates(scrape, prev, window_s) do
    s3_ops = counter_group(scrape, "fathom_s3_op_count", "op", @s3_methods)
    s3_bytes = counter_group(scrape, "fathom_s3_op_bytes", "op", @s3_methods)
    outcomes = PrometheusScrape.label_values(scrape, "fathom_shards_checkout_stop", "outcome")
    checkout = counter_group(scrape, "fathom_shards_checkout_stop", "outcome", outcomes)

    # Untagged totals (summed across tags): bounded by construction, never per shard.
    admission =
      Map.new(@admission_counters, fn {key, prefix} ->
        {key, PrometheusScrape.sum(scrape, prefix)}
      end)

    pool =
      Map.new([hit: "hit", miss: "miss"], fn {k, r} ->
        {k, PrometheusScrape.value(scrape, "fathom_shard_pool_take", %{"result" => r}, 0.0)}
      end)

    queries = %{total: PrometheusScrape.sum(scrape, "fathom_shard_query_duration")}
    errors = tagged(scrape, "fathom_shard_query_error", "class", @error_classes)
    auth = tagged(scrape, "fathom_hrana_auth_rejected", "reason", @auth_reasons)
    no_quorum = tagged(scrape, "fathom_replication_no_quorum", "reason", [])

    events =
      Map.new(@event_counters, fn {k, prefix} -> {k, PrometheusScrape.sum(scrape, prefix)} end)

    health = %{
      flush_gate_refusals:
        PrometheusScrape.value(scrape, "fathom_shard_flush_gate_refusals", %{}, 0.0)
    }

    cur =
      Enum.reduce(
        [
          prefixed(s3_ops, :s3_ops),
          prefixed(s3_bytes, :s3_bytes),
          prefixed(checkout, :checkout),
          prefixed(admission, :admission),
          prefixed(pool, :pool),
          prefixed(health, :health),
          prefixed(queries, :queries),
          prefixed(errors, :errors),
          prefixed(auth, :auth),
          prefixed(no_quorum, :no_quorum),
          prefixed(events, :events)
        ],
        %{},
        &Map.merge/2
      )

    pool_rates =
      Map.new(rate_group(pool, prev, :pool, window_s), fn {k, v} -> {:"pool_#{k}", v} end)

    rates = %{
      s3_ops: rate_group(s3_ops, prev, :s3_ops, window_s),
      s3_bytes: rate_group(s3_bytes, prev, :s3_bytes, window_s),
      checkout: rate_group(checkout, prev, :checkout, window_s),
      admission: Map.merge(rate_group(admission, prev, :admission, window_s), pool_rates),
      health: rate_group(health, prev, :health, window_s),
      queries: rate_group(queries, prev, :queries, window_s),
      errors: rate_group(errors, prev, :errors, window_s),
      auth: rate_group(auth, prev, :auth, window_s),
      no_quorum: rate_group(no_quorum, prev, :no_quorum, window_s)
    }

    {cur, rates}
  end

  defp counter_group(scrape, prefix, label_key, keys) do
    Map.new(keys, fn k -> {k, PrometheusScrape.value(scrape, prefix, %{label_key => k})} end)
  end

  # A counter by label: the fixed (bounded) label values plus any the scrape shows beyond them.
  defp tagged(scrape, prefix, label, fixed) do
    keys = Enum.uniq(fixed ++ PrometheusScrape.label_values(scrape, prefix, label))
    counter_group(scrape, prefix, label, keys)
  end

  defp prefixed(group, tag), do: Map.new(group, fn {k, v} -> {{tag, k}, v} end)

  defp rate_group(group, prev, tag, window_s) do
    Map.new(group, fn {k, cur} -> {k, rate(cur, Map.get(prev, {tag, k}, cur), window_s)} end)
  end

  # Run the RPO walk at most every `@rpo_interval_ms`, reusing the last result in between
  # (expert review 2026-08-26 #27).
  #
  # `rpo/0` copies the ENTIRE `FlushWatermark` table — one 4-tuple per open shard — into this
  # process's heap and then does one `WriteCounter.count/1` lookup per row. It sat inside a tick
  # that runs EVERY SECOND and is on by default (`Fathom.Admin.enabled?` defaults true, and
  # `FlushWatermark` is written whenever it is). At 30k open shards that is ~30k tuples copied plus
  # ~30k ETS lookups per second, in a process running the ERTS-default `fullsweep_after: 65535`, so
  # its heap grows to the largest snapshot and stays.
  #
  # That is the same O(resident-shards) walk review 2026-07-23 #30 moved behind a 10 s poller, and
  # the same shape review 2026-08-01 #31 rewrote for the HOT-SHARD half of this very tick — whose
  # comment ("This runs EVERY SECOND, on by default, and is O(resident shards)… a fixed node-level
  # tax growing on exactly the axis fathom scales on") describes this half exactly. The RPO half was
  # left doing it 10x more often than #30's fix. (The hot-shard half was once "free" because
  # `:shard_load` defaulted off; it is ON in prod now, so it is throttled to 10 s as well — expert
  # review 2026-10-17 #1.)
  #
  # Throttling rather than reading the Prometheus gauge, which was the review's first suggestion:
  # `[:fathom, :durability, :rpo]` is already emitted every 10 s and already scraped on this tick,
  # so reading it would cost nothing — but it would make the dashboard depend on the reporter being
  # up AND would inherit #17, where a `FlushWatermark` owner restart makes that gauge silently
  # report a fully-flushed fleet. Recomputing on the same cadence keeps this readout independent of
  # both, for one walk per 10 s instead of ten.
  defp rpo_throttled(%{rpo_at: at, rpo: cached} = state) do
    now = mono_ms()

    if is_nil(at) or now - at >= @rpo_interval_ms do
      fresh = rpo()
      cov = {FlushWatermark.size(), Fathom.Shards.open_count()}

      {%{state | rpo: fresh, rpo_at: now, rpo_walks: state.rpo_walks + 1, rpo_cov: cov}, fresh}
    else
      {state, cached}
    end
  end

  # Dirty shards + oldest RPO age: the shared ~9 s cached snapshot the 10 s poller also feeds
  # (`Measurements.rpo_snapshot/0`, expert review 2026-10-17 #16), so the walk runs once per 10 s.
  defp rpo, do: Fathom.Admin.Measurements.rpo_snapshot()

  # A shard's ShardLoad counters are cumulative + monotonic; curr < prev only when the coordinator
  # stopped + reopened (row dropped, recreated at 0) — the routine LRU churn. Clamp that reset to
  # `curr/window` (not 0), same as Fathom.Rebalancer.Reporter, so a churny hot shard isn't dropped.
  defp rate(curr, prev, window_s) when curr < prev, do: curr / window_s
  defp rate(curr, prev, window_s), do: (curr - prev) / window_s

  defp safe_scrape do
    Fathom.Telemetry.scrape()
  rescue
    _ -> ""
  catch
    :exit, _ -> ""
  end

  defp usage_elem(nil, _), do: nil
  defp usage_elem(usage, i), do: elem(usage, i)

  defp mono_ms, do: System.monotonic_time(:millisecond)
  defp system_ms, do: System.system_time(:millisecond)
  defp schedule(msg, ms), do: Process.send_after(self(), msg, ms)

  defp tick_ms, do: Application.get_env(:fathom, :admin_tick_ms, @default_tick_ms)

  defp hot_interval_ms,
    do: Application.get_env(:fathom, :admin_hot_interval_ms, @default_hot_interval_ms)

  defp usage_ms, do: Application.get_env(:fathom, :admin_usage_ms, @default_usage_ms)

  defp usage_timeout_ms,
    do: Application.get_env(:fathom, :admin_usage_timeout_ms, @default_usage_timeout_ms)
end
