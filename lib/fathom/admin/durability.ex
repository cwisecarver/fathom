defmodule Fathom.Admin.Durability do
  @moduledoc """
  Data for the admin **Durability** page (expert review 2026-10-15 #1, #8, #16): the durability
  alarms, the A2 replication state, the Backup & DR roll-up and the dirtiest shards on this node.

  Every number comes from something that already exists — the Prometheus counters/gauges the
  metrics layer aggregates (parsed with `Fathom.Admin.PrometheusScrape`), `Replication.Fleet.health/0`,
  one aggregate Postgres query, and `Fathom.Admin.FlushWatermark` — so there is no new
  instrumentation here.

  ## Cost model: bounded per NODE, never per viewer

  The page polls `snapshot/0` every 5 s per open tab. Each of the three sections is cached in an
  ETS table this process owns, with its own TTL, and refilled SINGLE-FLIGHT (the same shape as
  `Fathom.Admin.FleetCollector.migrations_page/1`): one caller computes, the rest serve the last
  result. Computation runs in the CALLER (the LiveView's async task), never in this process.

    * counters/gauges/replication — TTL `:admin_durability_cache_ms` (default 4 s);
    * Backup & DR (Postgres) — TTL `:admin_durability_backup_cache_ms` (default 60 s);
    * dirtiest shards (a walk of `FlushWatermark`) — TTL 10 s, and it only ever runs because a
      viewer asked, so a node nobody is watching pays nothing. The walk keeps a bounded top-20
      heap, not the whole table.

  ## The 5-minute window

  Alarm chips are counter DIFFS over a rolling window held in the cache: each fresh scrape is
  appended to a short sample list (at most one per 10 s, pruned past the window), and the diff is
  newest minus oldest. The window therefore starts when the first viewer arrives; `window_s` in the
  snapshot is the span actually covered, so the page can say "collecting" instead of claiming a
  quiet 5 minutes it did not observe. A counter that went backwards (node restart) clamps to 0.
  """
  use GenServer

  alias Fathom.Admin.{FlushWatermark, PrometheusScrape}
  alias Fathom.Shard.Replication.{Fleet, Session}
  alias Fathom.Shard.WriteCounter

  @table :fathom_admin_durability
  @window_ms 300_000
  @sample_every_ms 10_000
  @lock_ms 30_000
  @default_ttl_ms 4_000
  @default_backup_ttl_ms 60_000
  @dirty_ttl_ms 10_000
  @dirty_top 20
  @lag_ttl_ms 10_000
  @commit_family "fathom_replication_commit_duration"
  @commit_outcomes ["ok", "no_quorum", "timeout", "other"]

  # {key, label, scrape family}. Each is "page-worthy on first occurrence" per its metric's doc.
  @alarms [
    {:flush_failed, "Flush failed", "fathom_shard_flush_failed_count"},
    {:too_large, "Object too large", "fathom_shard_flush_too_large_count"},
    {:object_absent, "Stored object absent", "fathom_shard_flush_object_absent_count"},
    {:corrupt_flush, "Corrupt flush", "fathom_shard_corrupt_flush_count"},
    {:no_space, "No space to flush", "fathom_shard_flush_no_space_count"},
    {:fenced_quarantine, "Fenced quarantine", "fathom_shard_fenced_quarantine_count"}
  ]

  # Replication counters shown as 5-minute rates, optionally split by one bounded label.
  @repl_counters [
    {:reject, "fathom_replication_reject_count", "reason"},
    {:seed, "fathom_replication_seed_count", "result"},
    {:disk_pressure, "fathom_replication_disk_pressure_count", nil},
    {:promotion_raced, "fathom_replication_promotion_raced_count", "reason"},
    {:recovered_from_peer, "fathom_replication_recovered_from_peer_count", nil}
  ]

  @typedoc "One alarm chip: `window` is the diff over the page's window, `total` the lifetime count."
  @type alarm :: %{
          key: atom(),
          label: String.t(),
          window: non_neg_integer(),
          total: non_neg_integer(),
          firing?: boolean()
        }

  @type follower :: %{
          node_key: String.t(),
          host: String.t(),
          port: :inet.port_number(),
          connected?: boolean(),
          alive?: boolean() | :unknown
        }

  @type counters :: %{optional(term()) => number()}

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set])
    {:ok, %{}}
  end

  @doc "The alarm definitions as `{key, label}`, in display order."
  @spec alarm_keys() :: [{atom(), String.t()}]
  def alarm_keys, do: Enum.map(@alarms, fn {k, label, _} -> {k, label} end)

  # ---------------------------------------------------------------------------------------------
  # Public, cached reads
  # ---------------------------------------------------------------------------------------------

  @doc """
  The page's main snapshot: alarms, quarantines, replication panel. Cached
  (`:admin_durability_cache_ms`) and single-flight; never raises.
  """
  @spec snapshot() :: map()
  def snapshot do
    ttl = Application.get_env(:fathom, :admin_durability_cache_ms, @default_ttl_ms)
    cached(:snapshot, ttl, &compute_snapshot/0)
  end

  @doc """
  The Backup & DR roll-up (`Fathom.Directory.backup_summary/1`), cached ~60 s single-flight.
  `nil` when Postgres cannot be read and nothing is cached.
  """
  @spec backup() :: map() | nil
  def backup do
    ttl = Application.get_env(:fathom, :admin_durability_backup_cache_ms, @default_backup_ttl_ms)
    cached(:backup, ttl, &compute_backup/0)
  end

  @doc """
  The dirtiest shards on THIS node, top #{@dirty_top} by age since last successful S3 flush, plus
  the `under_reporting?` signal (`watermark_rows < open_shards`: some open shards have published
  no watermark, so the oldest-unflushed age is a lower bound). Cached 10 s, single-flight.
  """
  @spec dirtiest() :: %{
          rows: [%{shard_id: String.t(), age_ms: integer(), unflushed_writes: integer()}],
          dirty_count: non_neg_integer(),
          watermark_rows: non_neg_integer(),
          open_shards: non_neg_integer(),
          under_reporting?: boolean()
        }
  def dirtiest, do: cached(:dirtiest, @dirty_ttl_ms, &compute_dirtiest/0)

  @doc false
  # Test seam / reset: drops every cached entry and the window samples.
  @spec reset() :: :ok
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # ---------------------------------------------------------------------------------------------
  # Compute (runs in the caller, under the single-flight lock)
  # ---------------------------------------------------------------------------------------------

  defp compute_snapshot do
    now = System.monotonic_time(:millisecond)
    scrape = PrometheusScrape.parse(safe_scrape())
    counters = extract_counters(scrape)
    samples = record_sample(now, counters, scrape != [])
    {window_counters, span_ms} = window_diff(samples)

    enabled? = safe(fn -> Session.enabled?() end, false)

    %{
      at_ms: System.system_time(:millisecond),
      scrape?: scrape != [],
      window_s: div(span_ms, 1000),
      alarms: alarms(counters, window_counters),
      quarantines: round(PrometheusScrape.value(scrape, "fathom_shard_quarantines")),
      replication: replication(scrape, window_counters, enabled?)
    }
  end

  defp compute_backup, do: Fathom.Directory.backup_summary()

  defp compute_dirtiest do
    now = System.monotonic_time(:millisecond)
    gen = WriteCounter.generation()

    {dirty, heap} =
      FlushWatermark.fold({0, []}, fn {id, flushed_through, counter_gen, flushed_at},
                                      {dirty, heap} = acc ->
        written = WriteCounter.count(id)

        if counter_gen != gen or written > flushed_through do
          {dirty + 1, keep_top(heap, {now - flushed_at, id, max(written - flushed_through, 0)})}
        else
          acc
        end
      end)

    rows =
      heap
      |> Enum.sort_by(&elem(&1, 0), :desc)
      |> Enum.map(fn {age, id, n} -> %{shard_id: id, age_ms: age, unflushed_writes: n} end)

    watermark_rows = FlushWatermark.size()
    open = safe(fn -> Fathom.Shards.open_count() end, 0)

    %{
      rows: rows,
      dirty_count: dirty,
      watermark_rows: watermark_rows,
      open_shards: open,
      under_reporting?: watermark_rows < open
    }
  end

  # Bounded "keep the N oldest": never holds more than @dirty_top entries.
  defp keep_top(heap, entry) when length(heap) < @dirty_top, do: [entry | heap]

  defp keep_top(heap, {age, _, _} = entry) do
    {min_age, _, _} = min = Enum.min_by(heap, &elem(&1, 0))
    if age > min_age, do: [entry | List.delete(heap, min)], else: heap
  end

  defp safe_scrape do
    Fathom.Telemetry.scrape()
  rescue
    _ -> ""
  catch
    :exit, _ -> ""
  end

  defp safe(fun, default) do
    fun.()
  rescue
    _ -> default
  catch
    :exit, _ -> default
  end

  # ---------------------------------------------------------------------------------------------
  # Pure shaping (public for tests)
  # ---------------------------------------------------------------------------------------------

  @doc false
  # Flattens the scrape into `%{key => cumulative}` for every counter the page diffs.
  @spec extract_counters([PrometheusScrape.sample()]) :: counters()
  def extract_counters(scrape) do
    alarm_counts =
      Map.new(@alarms, fn {key, _label, family} -> {{:alarm, key}, family_sum(scrape, family)} end)

    counters =
      Enum.reduce(@repl_counters, alarm_counts, fn {key, family, label}, acc ->
        if label do
          scrape
          |> family_by_label(family, label)
          |> Enum.reduce(acc, fn {lv, n}, a -> Map.put(a, {key, lv}, n) end)
        else
          Map.put(acc, {key, nil}, family_sum(scrape, family))
        end
      end)

    # Commit-ack latency: cumulative buckets SUMMED across outcomes (`buckets/3` sums series that
    # share an `le`, so the tagged histogram does not yield duplicate `le` entries), plus the
    # per-outcome observation counts. Windowed by the same newest-minus-oldest diff as the rest.
    counters =
      scrape
      |> PrometheusScrape.buckets(@commit_family)
      |> Enum.reduce(counters, fn {le, n}, a -> Map.put(a, {:commit_bucket, le}, n) end)

    counters =
      scrape
      |> family_by_label(@commit_family <> "_count", "outcome")
      |> Enum.reduce(counters, fn {o, n}, a -> Map.put(a, {:commit_outcome, o}, n) end)

    Map.put(
      counters,
      :catchup_fails_armed,
      family_sum(scrape, "fathom_replication_catchup_fails_count")
    )
  end

  @doc false
  # Newest minus oldest sample, clamped at 0 per key (a counter that went backwards is a restart),
  # plus the span in ms. One sample (or none) means the window has not started: no diff.
  @spec window_diff([{integer(), counters()}]) :: {counters(), non_neg_integer()}
  def window_diff([{t_new, newest} | _] = samples) do
    {t_old, oldest} = List.last(samples)

    diff =
      Map.new(newest, fn {k, v} -> {k, max(v - Map.get(oldest, k, 0), 0)} end)

    {diff, max(t_new - t_old, 0)}
  end

  def window_diff([]), do: {%{}, 0}

  @doc false
  @spec alarms(counters(), counters()) :: [alarm()]
  def alarms(totals, window) do
    for {key, label, _} <- @alarms do
      w = round(Map.get(window, {:alarm, key}, 0))

      %{
        key: key,
        label: label,
        window: w,
        total: round(Map.get(totals, {:alarm, key}, 0)),
        firing?: w > 0
      }
    end
  end

  defp replication(_scrape, _window, false), do: %{enabled?: false}

  defp replication(scrape, window, true) do
    connected = round(PrometheusScrape.value(scrape, "fathom_replication_followers_connected"))
    configured = round(PrometheusScrape.value(scrape, "fathom_replication_followers_configured"))
    slack = round(PrometheusScrape.value(scrape, "fathom_replication_followers_slack"))

    %{
      enabled?: true,
      connected: connected,
      configured: configured,
      quorum: connected - slack,
      slack: slack,
      budget_ratio: PrometheusScrape.value(scrape, "fathom_replication_budget_used_ratio"),
      followers: safe(fn -> Fleet.health() end, []),
      rates: replication_rates(window),
      commit: commit_latency(window),
      catchup_armed: round(Map.get(window, :catchup_fails_armed, 0)),
      catching_up_shards:
        round(PrometheusScrape.value(scrape, "fathom_replication_lag_catching_up_shards"))
    }
  end

  @doc false
  # Windowed commit-ack latency from the diffed counters: p50/p95/p99 in ms (nil with no commits
  # in the window) and the ok/no_quorum/timeout/other counts.
  @spec commit_latency(counters()) :: %{
          total: non_neg_integer(),
          p50: float() | nil,
          p95: float() | nil,
          p99: float() | nil,
          outcomes: %{String.t() => non_neg_integer()}
        }
  def commit_latency(window) do
    cum =
      Enum.sort_by(
        for({{:commit_bucket, le}, n} <- window, do: {le, n}),
        fn {le, _} -> le end,
        fn
          :infinity, :infinity -> true
          :infinity, _ -> false
          _, :infinity -> true
          a, b -> a <= b
        end
      )

    base =
      Map.new(@commit_outcomes, fn o -> {o, round(Map.get(window, {:commit_outcome, o}, 0))} end)

    # Any outcome outside the bounded four still counts toward `other`.
    extra =
      for {{:commit_outcome, o}, n} <- window, o not in @commit_outcomes, reduce: 0 do
        acc -> acc + round(n)
      end

    outcomes = Map.update!(base, "other", &(&1 + extra))
    total = outcomes |> Map.values() |> Enum.sum()

    pct = fn p -> if total > 0, do: PrometheusScrape.percentile_cumulative(cum, p), else: nil end
    %{total: total, p50: pct.(50), p95: pct.(95), p99: pct.(99), outcomes: outcomes}
  end

  @doc """
  The replication lag report (`Fleet.lag_report/1`), cached 10 s and single-flight so N viewers
  cause one fan-out per node. `nil` when replication is not running. `opts[:compute]` replaces the
  report function (test seam). Only ever called while a viewer is mounted.
  """
  @spec lag(keyword()) :: map() | nil
  def lag(opts \\ []) do
    compute =
      Keyword.get_lazy(opts, :compute, fn ->
        fn ->
          if safe(fn -> Fleet.replicating?() end, false), do: Fleet.lag_report(), else: nil
        end
      end)

    cached(:lag, @lag_ttl_ms, compute)
  end

  # `%{reject: %{"overloaded" => 3}, seed: %{"ok" => 1}, disk_pressure: 0, ...}` — 5-min counts.
  defp replication_rates(window) do
    Enum.reduce(@repl_counters, %{}, fn {key, _family, label}, acc ->
      entries = for {{^key, lv}, n} <- window, n > 0, do: {lv, round(n)}

      value =
        if label,
          do: Map.new(entries),
          else: entries |> Enum.map(&elem(&1, 1)) |> Enum.sum()

      Map.put(acc, key, value)
    end)
  end

  # Exact-family sum across every label set (a tagged counter has one sample per tag value).
  defp family_sum(scrape, family) do
    Enum.reduce(scrape, 0, fn
      {^family, _ls, v}, acc when is_number(v) -> acc + v
      _, acc -> acc
    end)
  end

  defp family_by_label(scrape, family, label) do
    Enum.reduce(scrape, %{}, fn
      {^family, ls, v}, acc when is_number(v) ->
        Map.update(acc, Map.get(ls, label, ""), v, &(&1 + v))

      _, acc ->
        acc
    end)
  end

  # Appends `counters` to the window (at most one per @sample_every_ms) and returns the samples,
  # newest first. A blank scrape records nothing, so a recovered scrape diffs against the last good
  # one (same rule as `MetricsCollector.scrape_step/4`).
  defp record_sample(_now, _counters, false), do: samples()

  defp record_sample(now, counters, true) do
    kept = Enum.filter(samples(), fn {t, _} -> now - t <= @window_ms end)

    next =
      case kept do
        [{t, _} | _] when now - t < @sample_every_ms -> replace_head(kept, {now, counters})
        _ -> [{now, counters} | kept]
      end

    # Keep the OLDEST sample in the window stable: replacing only the head means the baseline
    # never slides forward faster than the window.
    put(:samples, next)
    next
  end

  defp replace_head([_ | rest], sample), do: [sample | rest]

  defp samples, do: get(:samples) || []

  # ---------------------------------------------------------------------------------------------
  # Single-flight TTL cache (ETS). Falls back to computing uncached when the table is absent
  # (admin layer off, tests that don't start it).
  # ---------------------------------------------------------------------------------------------

  defp cached(key, ttl, compute) do
    now = System.monotonic_time(:millisecond)

    case get({:entry, key}) do
      {at, value} when now - at < ttl -> value
      stale -> refill(key, stale, compute)
    end
  end

  defp refill(key, stale, compute) do
    now = System.monotonic_time(:millisecond)

    case acquire(key, now) do
      :acquired ->
        try do
          value = compute.()
          put({:entry, key}, {System.monotonic_time(:millisecond), value})
          value
        after
          :ets.delete(@table, {:lock, key})
        end

      :busy ->
        case stale do
          {_at, value} -> value
          # Cold and someone else is computing: compute for ourselves rather than wait.
          nil -> compute.()
        end

      :no_table ->
        compute.()
    end
  rescue
    ArgumentError -> compute.()
  end

  defp acquire(key, now) do
    lock = {:lock, key}

    if :ets.insert_new(@table, {lock, now + @lock_ms}) do
      :acquired
    else
      # A lock past its expiry belongs to a dead or wedged winner: take it over.
      case :ets.lookup(@table, lock) do
        [{^lock, expires}] when expires <= now ->
          :ets.delete_object(@table, {lock, expires})
          if :ets.insert_new(@table, {lock, now + @lock_ms}), do: :acquired, else: :busy

        _ ->
          :busy
      end
    end
  rescue
    ArgumentError -> :no_table
  end

  defp get(key) do
    case :ets.lookup(@table, key) do
      [{^key, value}] -> value
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp put(key, value) do
    :ets.insert(@table, {key, value})
  rescue
    ArgumentError -> :ok
  end
end
