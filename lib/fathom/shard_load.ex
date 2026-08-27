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
  writes. So load is a **public ETS table** bumped **lock-free from the executing
  process** (`:ets.update_counter` with `write_concurrency`): different shards hit
  different key locks, so there's no contention across shards, and no process hop.

  Each row is `{shard_id, checkouts, queries, rows_read, rows_written}`, cumulative
  since the shard's coordinator opened (`Fathom.Shard.terminate` calls `forget/1` so a
  stopped shard's row doesn't leak). `rows_read`/`rows_written` are the query **cost**
  dimension — a shard doing a few big scans is hotter than one doing many trivial
  point reads.

  ## Reading it

  A control plane reads `top/2` (the N hottest shards on this node by a dimension) or
  `snapshot/0`, and computes **rates** by diffing two snapshots over a window — which
  is churn-safe (a shard that stopped between snapshots just drops out). Per-shard
  values are deliberately **not** exported as `Telemetry.Metrics` (a per-shard tag at
  millions of shards is cardinality death); the read API is the interface.

  ## Gating

  Off by default (`config :fathom, :shard_load` — a node/deployment opts in), because
  nothing consumes it until the rebalancer lands, and the hot path shouldn't pay for
  an unread counter. When off, the `record_*` calls no-op; the read API still works
  (empty). The table owner is always supervised (an idle owner + empty table is free),
  so the API never depends on the feature being on.
  """
  use GenServer

  @table __MODULE__

  # Row layout: {shard_id, checkouts, queries, rows_read, rows_written}. Positions
  # for :ets.update_counter (1 is the key).
  @pos_checkouts 2
  @pos_queries 3
  @pos_rows_read 4
  @pos_rows_written 5
  @empty {nil, 0, 0, 0, 0}

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether per-shard load recording is enabled (`config :fathom, :shard_load`, default off)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :shard_load, false) == true

  @doc """
  Records a checkout of `shard_id` (the traffic/activity signal). Lock-free ETS
  increment, no process hop; a no-op when disabled or before the table is up.
  """
  @spec record_checkout(String.t()) :: :ok
  def record_checkout(shard_id) do
    if enabled?(), do: bump(shard_id, {@pos_checkouts, 1})
    :ok
  end

  @doc """
  Records a query on `shard_id` and its cost (`rows_read`, `rows_written`). Lock-free
  ETS increment, no process hop; a no-op when disabled or before the table is up.
  """
  @spec record_query(String.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def record_query(shard_id, rows_read, rows_written) do
    if enabled?() do
      bump(shard_id, [
        {@pos_queries, 1},
        {@pos_rows_read, rows_read},
        {@pos_rows_written, rows_written}
      ])
    end

    :ok
  end

  # The single ETS write. `default_row/1` is inserted first if the shard has no row
  # yet, then the op is applied — so first-touch and increment are one atomic call.
  defp bump(shard_id, op) do
    :ets.update_counter(@table, shard_id, op, default_row(shard_id))
    :ok
  rescue
    # Table not up yet (boot/teardown) — a dropped sample is harmless.
    ArgumentError -> :ok
  end

  defp default_row(shard_id), do: put_elem(@empty, 0, shard_id)

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
      [row] -> to_map(row)
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "Every shard's load counters on this node (unordered)."
  @spec snapshot() :: [map()]
  def snapshot do
    @table |> :ets.tab2list() |> Enum.map(&to_map/1)
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
    :ets.tab2list(@table)
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
    snapshot()
    |> Enum.sort_by(&Map.fetch!(&1, dimension), :desc)
    |> Enum.take(n)
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
    # public + write_concurrency: many executing/checkout processes bump concurrently,
    # each on its own shard key; readers (the control plane) are off the hot path.
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
