defmodule Fathom.Admin.MetricsCollector do
  @moduledoc """
  The realtime backend for the admin dashboard: one supervised node-local GenServer that, every
  tick, reads this node's live metrics and fans them out over `Phoenix.PubSub` to every dashboard
  LiveView — so N viewers cost one set of reads, not N (the reporter's "one reader, publish" model).

  ## Where each series comes from

  Two planes, matching the architecture (there is no BEAM cluster):

    * **Per-shard + node-direct** (cheap, cardinality-bound): `Fathom.ShardLoad` snapshots diffed
      into per-shard **query rates** (node QPS = their sum; the hot-shard table = the top N),
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
  """
  use GenServer

  alias Fathom.Admin.{FlushWatermark, PrometheusScrape}
  alias Fathom.Shard.WriteCounter
  alias Fathom.ShardLatency
  alias Fathom.ShardLoad

  @default_tick_ms 1_000
  @default_usage_ms 60_000
  @hot_n 20
  # Bounded ring of recent points for the hero charts, so a freshly-connected LiveView can paint a
  # populated chart from snapshot/0 instead of waiting to accumulate ticks. ~5 min at 1 s.
  @ring 300
  @reporter :fathom_metrics
  @s3_methods ~w(get put head delete post)

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

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

  @impl true
  def init(_opts) do
    state = %{
      node_key: Fathom.Rebalancer.node_key(),
      prev_load: load_map(),
      prev_mono: mono_ms(),
      prev_query_buckets: [],
      prev_counters: %{},
      usage: nil,
      rings: [],
      current: nil
    }

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
    Phoenix.PubSub.broadcast(Fathom.PubSub, topic(), {:metrics, state.current})
    schedule(:tick, tick_ms())
    {:noreply, state}
  end

  # Storage footprint on a slow cadence (an S3 LIST is expensive) — cache it + publish it as a
  # gauge so Prometheus/Grafana also see it. Run the (possibly slow) call in a Task so it never
  # blocks the realtime tick.
  def handle_info(:usage_poll, state) do
    parent = self()

    Task.start(fn ->
      usage =
        case Fathom.Shard.Storage.stored_usage() do
          {objects, bytes} when is_integer(objects) and is_integer(bytes) -> {objects, bytes}
          _ -> nil
        end

      send(parent, {:usage, usage})
    end)

    schedule(:usage_poll, usage_ms())
    {:noreply, state}
  end

  def handle_info({:usage, nil}, state), do: {:noreply, state}

  def handle_info({:usage, {objects, bytes} = usage}, state) do
    :telemetry.execute([:fathom, :storage, :usage], %{objects: objects, bytes: bytes}, %{})
    {:noreply, %{state | usage: usage}}
  end

  # --- the tick computation (kept as small pure-ish steps for readability) ---

  defp tick(state) do
    now = mono_ms()
    window_s = max((now - state.prev_mono) / 1000, 0.001)

    load = load_map()
    rates = shard_rates(load, state.prev_load, window_s)
    node_qps = rates |> Enum.map(& &1.q_per_s) |> Enum.sum()

    hot =
      rates
      |> Enum.reject(&(&1.q_per_s <= 0.0))
      |> Enum.sort_by(& &1.q_per_s, :desc)
      |> Enum.take(@hot_n)
      # Per-shard tail latency — computed ONLY for the top-N hot shards (cardinality-bounded:
      # the read cost never scales with resident-shard count). Each is one ETS lookup + a
      # histogram-percentile walk over Fathom.ShardLatency's µs buckets.
      |> Enum.map(fn s -> Map.merge(s, ShardLatency.percentiles(s.shard_id)) end)

    scrape = PrometheusScrape.parse(safe_scrape())

    query_buckets = PrometheusScrape.buckets(scrape, "fathom_shard_query_duration")
    win = PrometheusScrape.diff_buckets(query_buckets, state.prev_query_buckets)

    cold_buckets = PrometheusScrape.buckets(scrape, "fathom_shard_cold_open_duration")

    {counters, counter_rates} = counter_rates(scrape, state.prev_counters, window_s)
    {dirty, oldest_rpo} = rpo()

    current = %{
      node_key: state.node_key,
      at_ms: system_ms(),
      open_shards: Registry.count(Fathom.ShardRegistry),
      memory_bytes: :erlang.memory(:total),
      node_qps: node_qps,
      query_p50_ms: PrometheusScrape.percentile_cumulative(win, 50),
      query_p95_ms: PrometheusScrape.percentile_cumulative(win, 95),
      query_p99_ms: PrometheusScrape.percentile_cumulative(win, 99),
      cold_open_p50_ms: PrometheusScrape.percentile_cumulative(cold_buckets, 50),
      dirty_shards: dirty,
      oldest_rpo_ms: oldest_rpo,
      s3_ops_per_s: counter_rates.s3_ops,
      s3_bytes_per_s: counter_rates.s3_bytes,
      checkout_per_s: counter_rates.checkout,
      storage_objects: usage_elem(state.usage, 0),
      storage_bytes: usage_elem(state.usage, 1),
      hot_shards: hot
    }

    ring = %{
      t: current.at_ms,
      qps: node_qps,
      p50: current.query_p50_ms,
      p95: current.query_p95_ms,
      p99: current.query_p99_ms
    }

    %{
      state
      | prev_load: load,
        prev_mono: now,
        prev_query_buckets: query_buckets,
        prev_counters: counters,
        current: current,
        rings: [ring | state.rings] |> Enum.take(@ring)
    }
  end

  defp shard_rates(load, prev, window_s) do
    Enum.map(load, fn {id, c} ->
      p = Map.get(prev, id, %{queries: 0, rows_read: 0, rows_written: 0, checkouts: 0})

      %{
        shard_id: id,
        q_per_s: rate(c.queries, p.queries, window_s),
        rows_read_per_s: rate(c.rows_read, p.rows_read, window_s),
        rows_written_per_s: rate(c.rows_written, p.rows_written, window_s),
        queries: c.queries
      }
    end)
  end

  # Diff the tracked cumulative counters (S3 ops/bytes by method, checkout counts by outcome) into
  # per-second rates. Returns the fresh cumulative map (next tick's baseline) + the grouped rates.
  defp counter_rates(scrape, prev, window_s) do
    s3_ops = counter_group(scrape, "fathom_s3_op_count", "op", @s3_methods)
    s3_bytes = counter_group(scrape, "fathom_s3_op_bytes", "op", @s3_methods)
    outcomes = PrometheusScrape.label_values(scrape, "fathom_shards_checkout_stop", "outcome")
    checkout = counter_group(scrape, "fathom_shards_checkout_stop", "outcome", outcomes)

    cur =
      Map.merge(
        Map.merge(prefixed(s3_ops, :s3_ops), prefixed(s3_bytes, :s3_bytes)),
        prefixed(checkout, :checkout)
      )

    rates = %{
      s3_ops: rate_group(s3_ops, prev, :s3_ops, window_s),
      s3_bytes: rate_group(s3_bytes, prev, :s3_bytes, window_s),
      checkout: rate_group(checkout, prev, :checkout, window_s)
    }

    {cur, rates}
  end

  defp counter_group(scrape, prefix, label_key, keys) do
    Map.new(keys, fn k -> {k, PrometheusScrape.value(scrape, prefix, %{label_key => k})} end)
  end

  defp prefixed(group, tag), do: Map.new(group, fn {k, v} -> {{tag, k}, v} end)

  defp rate_group(group, prev, tag, window_s) do
    Map.new(group, fn {k, cur} -> {k, rate(cur, Map.get(prev, {tag, k}, cur), window_s)} end)
  end

  # Dirty shards + oldest RPO age, derived from the published watermark exactly as
  # Fathom.Shard.unflushed?/1 does (matches Fathom.Admin.Measurements.durability/0).
  defp rpo do
    now = mono_ms()
    gen = WriteCounter.generation()

    Enum.reduce(FlushWatermark.snapshot(), {0, 0}, fn
      {id, flushed_through, counter_gen, flushed_at}, {dirty, oldest} ->
        if counter_gen != gen or WriteCounter.count(id) > flushed_through do
          {dirty + 1, max(oldest, now - flushed_at)}
        else
          {dirty, oldest}
        end
    end)
  end

  # A shard's ShardLoad counters are cumulative + monotonic; curr < prev only when the coordinator
  # stopped + reopened (row dropped, recreated at 0) — the routine LRU churn. Clamp that reset to
  # `curr/window` (not 0), same as Fathom.Rebalancer.Reporter, so a churny hot shard isn't dropped.
  defp rate(curr, prev, window_s) when curr < prev, do: curr / window_s
  defp rate(curr, prev, window_s), do: (curr - prev) / window_s

  defp load_map, do: ShardLoad.snapshot() |> Map.new(&{&1.shard_id, &1})

  defp safe_scrape do
    @reporter |> TelemetryMetricsPrometheus.Core.scrape() |> IO.iodata_to_binary()
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
  defp usage_ms, do: Application.get_env(:fathom, :admin_usage_ms, @default_usage_ms)
end
