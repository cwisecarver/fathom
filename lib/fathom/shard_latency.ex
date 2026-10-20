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

  Each shard has a fixed **µs log-scale histogram** whose edges mirror the node-wide
  `fathom_shard_query_duration` buckets (×1000 for µs, plus a `0` floor), so a per-shard p99 lines
  up with the hero chart's node-wide p99. The row is `{shard_id, counters_ref}`, one `:counters`
  slot per bucket, and `record/2` bumps one slot after an ETS READ of the ref — no ETS write per
  sample, so the streams of one hot shard do not serialise on its key lock (expert review
  2026-10-19 #5; the measurement is in `Fathom.ShardLoad`'s moduledoc, the same hot-path class).
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
  # `:counters` index of bucket i is 1 + i.
  @zero_hist List.duplicate(0, @bucket_count)

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # Whether per-shard latency recording is enabled — rides `Fathom.ShardLoad`'s `:shard_load` gate.
  @spec enabled?() :: boolean()
  defp enabled?, do: Application.get_env(:fathom, :shard_load, false) == true

  @doc """
  Records one query's latency (a `System.monotonic_time/0` diff, native units) against
  `shard_id`. Converts to µs, picks the log-bucket, and bumps its slot in the shard's `:counters`
  block. A no-op when disabled or before the table is up.
  """
  @spec record(String.t(), integer()) :: :ok
  def record(shard_id, dt_native) do
    if enabled?() do
      case block(shard_id) do
        nil ->
          :ok

        ref ->
          us = System.convert_time_unit(dt_native, :native, :microsecond)
          :counters.add(ref, 1 + bucket_index(us), 1)
      end
    end

    :ok
  end

  # The shard's histogram block — an ETS READ once it exists; created on first touch with
  # `insert_new` so racing first samples agree on one block (the `Fathom.ShardLoad` pattern).
  # `nil` when the table is not up (boot/teardown) — a dropped sample is harmless.
  defp block(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{_, ref}] -> ref
      [] -> create_block(shard_id)
    end
  rescue
    ArgumentError -> nil
  end

  defp create_block(shard_id) do
    ref = :counters.new(@bucket_count, [])

    if :ets.insert_new(@table, {shard_id, ref}) do
      ref
    else
      case :ets.lookup(@table, shard_id) do
        [{_, winner}] -> winner
        [] -> ref
      end
    end
  end

  # The largest bucket index whose lower edge is ≤ us (µs ≥ last edge land in the overflow
  # bucket). Mirrors Fathom.Rebalancer.Stats.bucket_index/1 exactly.
  #
  # Comparison ladder, not a scan of `@edges_us` (expert review 2026-10-20 #35: `Enum.count` over the
  # 14 edges per query). Ascending, because nearly every query is under 500 us, so it exits within
  # two comparisons. A clause per edge; `shard_latency_test` pins it against the scan at every edge.
  @doc false
  @spec bucket_index(integer()) :: non_neg_integer()
  def bucket_index(us) when us < 100, do: 0
  def bucket_index(us) when us < 500, do: 1
  def bucket_index(us) when us < 1_000, do: 2
  def bucket_index(us) when us < 2_000, do: 3
  def bucket_index(us) when us < 5_000, do: 4
  def bucket_index(us) when us < 10_000, do: 5
  def bucket_index(us) when us < 25_000, do: 6
  def bucket_index(us) when us < 50_000, do: 7
  def bucket_index(us) when us < 100_000, do: 8
  def bucket_index(us) when us < 250_000, do: 9
  def bucket_index(us) when us < 500_000, do: 10
  def bucket_index(us) when us < 1_000_000, do: 11
  def bucket_index(us) when us < 5_000_000, do: 12
  def bucket_index(_us), do: 13

  @doc false
  # Test seam: the scan this ladder replaced, so the test can compare them edge by edge.
  @spec bucket_index_scan(integer()) :: non_neg_integer()
  def bucket_index_scan(us), do: max(Enum.count(@edges_us, &(&1 <= us)) - 1, 0)

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
      [{_, ref}] -> for i <- 1..@bucket_count, do: :counters.get(ref, i)
      [] -> @zero_hist
    end
  rescue
    ArgumentError -> @zero_hist
  end

  @doc """
  p50/p95/p99 (ms) from an already-fetched histogram vector (`histogram/1`'s shape). This is the
  windowing seam for `Fathom.Admin.MetricsCollector`, which diffs two ticks' histograms *before*
  taking percentiles so per-shard tail latency is a recent-window figure, not the lifetime
  cumulative one (expert review 2026-07-14 #12).
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
    # Same shape as Fathom.ShardLoad: the hot path only READS a shard's row (its counter block);
    # the one write per shard is its first touch. Readers (the collector) are off the hot path.
    #
    # NO `decentralized_counters` — see the measurement recorded in `Fathom.Shard.WriteCounter`
    # (expert review 2026-08-26 #41). It decentralises the TABLE'S internal counters, not user
    # counter values, so on a per-shard-keyed table it bought nothing and made
    # `:ets.info(tab, :size)` ~2.4x slower.
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    {:ok, %{}}
  end
end
