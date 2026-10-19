defmodule Fathom.ShardLoad do
  @moduledoc """
  Per-shard load counters — the input a control plane needs to spot hot shards
  (the Phase-2 dynamic-rebalancing prerequisite; see `docs/phase2-scoping.md` §B).

  Placement is a pure `Host`-subdomain hash today, so a persistently hot shard (or a
  node that hashed several hot ones) can't be moved. Moving one needs load evidence:
  *which* shards are hot, by checkout rate and query cost. This module measures that.

  ## Why ETS counters, not coordinator state

  A hot shard serves thousands of queries a second. Recording load by sending the
  `Fathom.Shard` coordinator a message per query would turn it into a mailbox
  bottleneck — the same trap `Fathom.Directory.Recorder` avoids for directory
  writes. So load lives in **public ETS** and is bumped **lock-free from the executing
  process** (a per-shard `:counters` block, below): no contention across shards, none between
  the streams of one shard, and no process hop.

  Each shard's counters are `{checkouts, queries, rows_read, rows_written}`, cumulative
  since the shard's coordinator opened (`Fathom.Shard.terminate` calls `forget/1` so a
  stopped shard's row doesn't leak). `rows_read`/`rows_written` are the query **cost**
  dimension — a shard doing a few big scans is hotter than one doing many trivial
  point reads.

  ## One `:counters` block per shard, not an `update_counter` per sample

  The row is `{shard_id, counters_ref}` (expert review 2026-10-19 #5). The executing process looks
  the ref up — an ETS READ on a `read_concurrency` table — and bumps a plain `:counters` array.
  The old row held the integers themselves and every sample was an `:ets.update_counter`, a WRITE
  under the key's lock: fine across shards, but every stream of ONE hot shard took the same lock.
  Probe, record_query + ShardLatency.record on one key with ~50 reductions of work between:
  0.63 us/op alone, 7.5 at 4 workers, 32 at 8, 159 at 18. The `:counters` form: 0.31 / 0.66 /
  0.8 / 1.4. `:write_concurrency` counters were flatter still but cost 3.7 KB per shard (one
  cache-line-padded array per scheduler) against 184 B — a density cost at 30k resident shards.

  The first sample for a shard creates its block with `:ets.insert_new`, so racing first-touches
  converge on one block; a sample racing `forget/1` can land in a block that is already gone, which
  drops it (the same "a dropped sample is harmless" contract as before).

  ## Reading it

  A control plane reads `top/2` (the N hottest shards on this node by a dimension) or
  `snapshot/0`, and computes **rates** by diffing two snapshots over a window — which
  is churn-safe (a shard that stopped between snapshots just drops out). Per-shard
  values are deliberately **not** exported as `Telemetry.Metrics` (a per-shard tag at
  millions of shards is cardinality death); the read API is the interface.

  ## Gating

  Off by default in the module (`config :fathom, :shard_load`), and ON in prod via
  `config/runtime.exs` (`SHARD_LOAD=false` opts a node out). Dev/test stay off so the
  hot path there doesn't pay for an unread counter. When off, the `record_*` calls no-op; the read API still works
  (empty). The table owner is always supervised (an idle owner + empty table is free),
  so the API never depends on the feature being on.
  """
  use GenServer

  @table __MODULE__

  # Row layout: {shard_id, counters_ref}. Counter indices within the block.
  @ix_checkouts 1
  @ix_queries 2
  @ix_rows_read 3
  @ix_rows_written 4
  @slots 4

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether per-shard load recording is enabled (`config :fathom, :shard_load`, module default off; prod sets it on in `config/runtime.exs`)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :shard_load, false) == true

  @doc """
  Records a checkout of `shard_id` (the traffic/activity signal). Lock-free ETS
  increment, no process hop; a no-op when disabled or before the table is up.
  """
  @spec record_checkout(String.t()) :: :ok
  def record_checkout(shard_id) do
    if enabled?() do
      case block(shard_id) do
        nil -> :ok
        ref -> :counters.add(ref, @ix_checkouts, 1)
      end
    end

    :ok
  end

  @doc """
  Records a query on `shard_id` and its cost (`rows_read`, `rows_written`). Lock-free
  ETS increment, no process hop; a no-op when disabled or before the table is up.
  """
  @spec record_query(String.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def record_query(shard_id, rows_read, rows_written) do
    if enabled?() do
      case block(shard_id) do
        nil ->
          :ok

        ref ->
          :counters.add(ref, @ix_queries, 1)
          :counters.add(ref, @ix_rows_read, rows_read)
          :counters.add(ref, @ix_rows_written, rows_written)
      end
    end

    :ok
  end

  # The shard's counter block: an ETS READ on the hot path, never a write once the block exists.
  # `nil` when the table is not up (boot/teardown) — a dropped sample is harmless.
  defp block(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{_, ref}] -> ref
      [] -> create_block(shard_id)
    end
  rescue
    ArgumentError -> nil
  end

  # First touch. `insert_new` makes racing first-touches agree on ONE block: the loser re-reads the
  # winner's. A `forget/1` between the two leaves the loser's re-read empty; it then counts into its
  # own block, which is not in the table — a dropped sample, the documented contract.
  defp create_block(shard_id) do
    ref = :counters.new(@slots, [])

    if :ets.insert_new(@table, {shard_id, ref}) do
      ref
    else
      case :ets.lookup(@table, shard_id) do
        [{_, winner}] -> winner
        [] -> ref
      end
    end
  end

  # A stored row in the read API's tuple shape, `{shard_id, checkouts, queries, rows_read,
  # rows_written}` — what `snapshot_tuples/0`'s callers (the Rebalancer.Reporter, the
  # MetricsCollector) destructure. Each counter is read separately, so a row read concurrently with
  # a sample is not one atomic snapshot; for rates diffed over a window that is immaterial.
  defp to_tuple({shard_id, ref}) do
    {shard_id, :counters.get(ref, @ix_checkouts), :counters.get(ref, @ix_queries),
     :counters.get(ref, @ix_rows_read), :counters.get(ref, @ix_rows_written)}
  end

  @doc "Drops `shard_id`'s row — called from `Fathom.Shard.terminate` so stopped shards don't leak."
  @spec forget(String.t()) :: :ok
  def forget(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "The load counters for `shard_id`, or `nil` if it has none recorded."
  @spec get(String.t()) :: map() | nil
  def get(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [row] -> row |> to_tuple() |> to_map()
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "Every shard's load counters on this node (unordered)."
  @spec snapshot() :: [map()]
  def snapshot do
    @table |> :ets.tab2list() |> Enum.map(&(&1 |> to_tuple() |> to_map()))
  rescue
    ArgumentError -> []
  end

  # Raw table rows `{shard_id, checkouts, queries, rows_read, rows_written}` for a bulk
  # reader that materializes its own shape (the Rebalancer.Reporter diffs two snapshots
  # per window; the map-per-row of snapshot/0 was pure intermediate allocation at high
  # shard counts — review 2026-07-23 #29). The read API remains the interface — this is
  # that API in its cheapest form, not an invitation to touch the table directly.
  @doc false
  @spec snapshot_tuples() :: [tuple()]
  def snapshot_tuples do
    @table |> :ets.tab2list() |> Enum.map(&to_tuple/1)
  rescue
    ArgumentError -> []
  end

  @doc """
  The `n` hottest shards on this node by `dimension`
  (`:checkouts | :queries | :rows_read | :rows_written`, default `:queries`), highest
  first. The control plane's "which of my shards are hot" query.
  """
  @spec top(pos_integer(), :checkouts | :queries | :rows_read | :rows_written) :: [map()]
  def top(n, dimension \\ :queries) when is_integer(n) and n > 0 do
    # BOUNDED selection, not tab2list -> map-per-row -> full sort -> take(n) (review dsv41f.perf
    # 2026-09-16 #4). This is the million-shard "which of my shards are hot" query, so it must be
    # O(shards) with an O(n) working set, not O(shards) allocation + an O(shards log shards) sort.
    # Same shape `Fathom.Admin.MetricsCollector.keep_top/2` already uses; `to_map/1` is paid for
    # the <= n survivors only. Tie-breaking among equal values is unspecified (ETS iteration order
    # is), exactly as the old sort-then-take was over arbitrary tab2list order.
    idx = dim_index(dimension)

    @table
    |> :ets.tab2list()
    |> Enum.reduce([], fn stored, heap ->
      row = to_tuple(stored)
      keep_top(heap, n, elem(row, idx), row)
    end)
    |> Enum.sort_by(fn {v, _row} -> v end, :desc)
    |> Enum.map(fn {_v, row} -> to_map(row) end)
  rescue
    ArgumentError -> []
  end

  # Tuple position of a dimension in `{shard_id, checkouts, queries, rows_read, rows_written}`.
  defp dim_index(:checkouts), do: 1
  defp dim_index(:queries), do: 2
  defp dim_index(:rows_read), do: 3
  defp dim_index(:rows_written), do: 4

  # Keep at most `n` `{value, row}` entries — the largest by `value`. `length(heap) < n` fills the
  # heap; once full, a new row displaces the current minimum only when strictly greater (so equal
  # values never churn the set), mirroring `MetricsCollector.keep_top/2`.
  defp keep_top(heap, n, value, row) when length(heap) < n, do: [{value, row} | heap]

  defp keep_top(heap, _n, value, row) do
    {min_v, _} = min_entry = Enum.min_by(heap, fn {v, _} -> v end)
    if value > min_v, do: [{value, row} | List.delete(heap, min_entry)], else: heap
  end

  @doc "Clears all load counters (test/ops helper)."
  @spec reset() :: :ok
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp to_map({shard_id, checkouts, queries, rows_read, rows_written}) do
    %{
      shard_id: shard_id,
      checkouts: checkouts,
      queries: queries,
      rows_read: rows_read,
      rows_written: rows_written
    }
  end

  @impl true
  def init(_opts) do
    # public + read_concurrency: the hot path only READS a shard's row (its counter block, expert
    # review 2026-10-19 #5); the one write per shard is the first touch. write_concurrency stays
    # for that insert and for forget/1. Readers (the control plane) are off the hot path.
    # NO `decentralized_counters` — it does not do what a reader (and this comment, until expert
    # review 2026-08-26 #41) assumed. The option decentralises the TABLE'S OWN internal counters
    # (what `:ets.info(tab, :size)` and `:memory` read), NOT user counter values, and its
    # documented trade-off is that `:ets.info(tab, :size)` gets slower. Both this table and
    # `Fathom.ShardLoad` are keyed per shard, so concurrent `update_counter` calls already land on
    # different key locks; there is no shared counter cache line for it to relieve.
    #
    # Measured before removing it — N schedulers, 5,000 distinct keys, 40,000 bumps each, 3 trials:
    #
    #     update_counter    with 87 / 89 / 91 ms      without 82 / 91 / 92 ms    (indistinguishable)
    #     info(:size)x2000  with 1297 / 1384 / 1297us without 620 / 492 / 519us  (~2.4x SLOWER with)
    #
    # So it bought nothing and cost on the one operation it does affect. `Directory.Recorder` calls
    # `:ets.info(table, :size)` on a sibling table's shutdown path, which is the shape that would
    # have paid for it here.
    #
    # Do NOT add this to `Fathom.Shard.FlushGate` either, where every writer on the node genuinely
    # does hit ONE key: it would not help there for the same reason. That contention needs a
    # wake-on-release queue (review #16), not a table flag.
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
