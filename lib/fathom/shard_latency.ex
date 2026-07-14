defmodule Fathom.ShardLatency do
  @moduledoc """
  Per-shard query-latency histograms — the tail-latency companion to `Fathom.ShardLoad`.

  The node-wide query latency (`[:fathom, :shard, :query]` → the Prometheus
  `fathom_shard_query_duration` distribution) is deliberately **un-tagged**: a per-shard
  Prometheus label at millions of shards is cardinality death (one time-series per shard).
  But an operator staring at the dashboard still wants to know *which* shard is slow, not
  just that the node's p99 climbed. That question is answered the same way `Fathom.ShardLoad`
  answers "which shard is hot": a **per-shard read-API backed by ETS**, not a metric tag.

  ## Why this is not the cardinality trap

  "Cardinality death" is about Prometheus *series*, not ETS *rows*. This table holds one row
  per **resident** shard — the same bound `Fathom.ShardLoad` already lives within (a few
  thousand per node, `forget/1` from `Fathom.Shard.terminate` drops a stopped shard's row).
  Percentiles are only ever *computed* for the handful of hot shards the dashboard surfaces
  (`Fathom.Admin.MetricsCollector`'s top-N), so the read cost is bounded too.

  ## Representation

  Each row is `{shard_id, b0, b1, …}` — a fixed **µs log-scale histogram** whose edges mirror
  the node-wide `fathom_shard_query_duration` buckets (×1000 for µs, plus a `0` floor), so a
  per-shard p99 lines up with the hero chart's node-wide p99. `record/2` bumps one bucket with
  a single lock-free `:ets.update_counter` (per-shard key, `decentralized_counters` — no
  cross-shard contention, no process hop), the same hot-path class as `ShardLoad.record_query`.
  Percentiles are computed **on read** by `Fathom.Rebalancer.Stats.percentile_from_histogram/3`
  (shared with the fleet q/s histogram, given these µs `edges/0`).

  ## Gating

  Rides `Fathom.ShardLoad`'s gate (`config :fathom, :shard_load`, default off): per-shard
  latency is per-shard load data, and the Shards page already tells the operator to enable
  `SHARD_LOAD=true` for it. When off, `record/2` no-ops and the read API returns empty
  (all-zero) histograms. The table owner is always supervised, so the read API never depends
  on the feature being on.
  """
  use GenServer

  alias Fathom.Rebalancer.Stats

  @table __MODULE__

  # Bucket lower-edges in **microseconds**. These mirror the node-wide
  # `fathom_shard_query_duration` ms buckets ([0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500,
  # 1000, 5000]) scaled to µs, with an added `0` floor so a sub-100µs query has its own bucket
  # rather than being lumped into [100, 500). Bucket i is [edges[i], edges[i+1]); the last is
  # [5_000_000, +inf). All query durations are ≥ 0, so nothing underflows bucket 0.
  @edges_us [
    0,
    100,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    5_000_000
  ]

  @bucket_count length(@edges_us)
  # ETS position of bucket i is 2 + i (position 1 is the shard_id key). An all-zero seed row.
  @empty_row List.to_tuple([nil | List.duplicate(0, @bucket_count)])
  @zero_hist List.duplicate(0, @bucket_count)

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether per-shard latency recording is enabled — rides `Fathom.ShardLoad`'s `:shard_load` gate."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :shard_load, false) == true

  @doc """
  Records one query's latency (a `System.monotonic_time/0` diff, native units) against
  `shard_id`. Converts to µs, picks the log-bucket, and bumps it with a single lock-free
  `:ets.update_counter`. A no-op when disabled or before the table is up.
  """
  @spec record(String.t(), integer()) :: :ok
  def record(shard_id, dt_native) do
    if enabled?() do
      us = System.convert_time_unit(dt_native, :native, :microsecond)
      pos = 2 + bucket_index(us)
      :ets.update_counter(@table, shard_id, {pos, 1}, default_row(shard_id))
    end

    :ok
  rescue
    # Table not up yet (boot/teardown) — a dropped sample is harmless.
    ArgumentError -> :ok
  end

  defp default_row(shard_id), do: put_elem(@empty_row, 0, shard_id)

  # The largest bucket index whose lower edge is ≤ us (µs ≥ last edge land in the overflow
  # bucket). Mirrors Fathom.Rebalancer.Stats.bucket_index/1 exactly.
  defp bucket_index(us), do: max(Enum.count(@edges_us, &(&1 <= us)) - 1, 0)

  @doc "Drops `shard_id`'s row — called from `Fathom.Shard.terminate` so stopped shards don't leak."
  @spec forget(String.t()) :: :ok
  def forget(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  The latency histogram (bucket-count vector, one per `edges/0`) for `shard_id`, or an
  all-zero vector if it has no queries recorded. Feed to
  `Fathom.Rebalancer.Stats.percentile_from_histogram/3` with `edges/0`.
  """
  @spec histogram(String.t()) :: [non_neg_integer()]
  def histogram(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [row] -> row |> Tuple.to_list() |> tl()
      [] -> @zero_hist
    end
  rescue
    ArgumentError -> @zero_hist
  end

  @doc """
  The p50/p95/p99 latency (in **milliseconds**, float) for `shard_id`, read from its histogram
  in one lookup. `%{p50_ms: _, p95_ms: _, p99_ms: _}`; all-zero (no queries) ⇒ zeros.
  """
  @spec percentiles(String.t()) :: %{p50_ms: float(), p95_ms: float(), p99_ms: float()}
  def percentiles(shard_id), do: shard_id |> histogram() |> percentiles_from_histogram()

  @doc """
  p50/p95/p99 (ms) from an already-fetched histogram vector (`histogram/1`'s shape). This is the
  windowing seam for `Fathom.Admin.MetricsCollector`, which diffs two ticks' histograms *before*
  taking percentiles so per-shard tail latency is a recent-window figure, not the lifetime
  cumulative one (expert review 2026-07-14 #12). `percentiles/1` is this over the shard's current
  cumulative histogram.
  """
  @spec percentiles_from_histogram([non_neg_integer()]) :: %{
          p50_ms: float(),
          p95_ms: float(),
          p99_ms: float()
        }
  def percentiles_from_histogram(counts) do
    %{
      p50_ms: percentile_ms(counts, 50),
      p95_ms: percentile_ms(counts, 95),
      p99_ms: percentile_ms(counts, 99)
    }
  end

  # µs percentile from the histogram, converted to ms for the dashboard (matches the node-wide
  # p50/p95/p99 hero chart's unit).
  defp percentile_ms(counts, pct),
    do: Stats.percentile_from_histogram(counts, pct, @edges_us) / 1000

  @doc "The µs bucket lower-edges (for `Fathom.Rebalancer.Stats.percentile_from_histogram/3`)."
  @spec edges() :: [non_neg_integer()]
  def edges, do: @edges_us

  @doc "Clears all latency histograms (test/ops helper)."
  @spec reset() :: :ok
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    # Same shape as Fathom.ShardLoad: many executing processes bump concurrently, each on its
    # own shard key; readers (the collector) are off the hot path. decentralized_counters keeps
    # the concurrent update_counter writes off a shared counter cache line.
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      decentralized_counters: true,
      read_concurrency: true
    ])

    {:ok, %{}}
  end
end
