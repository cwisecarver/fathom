defmodule Fathom.Shards.Lru do
  @moduledoc """
  Node-local recency index for **idle-eviction at capacity**.

  When a node is at `:max_open_shards` and a new shard wants in, `Fathom.Shards`
  would rather evict the least-recently-used *idle* shard (flush + drop + release
  its lease) than refuse the open with a 503 — a shard only fails over away from
  its home, and an idle shard's file is bottomless-backed, so dropping it costs
  only a cold re-open if it's touched again. This table is how the router picks
  *which* idle shard is coldest.

  It's a public ETS `set` of `{shard_id, monotonic_time}`, written **lock-free
  from the checkout process** (`:ets.insert`, `write_concurrency` — the
  `Fathom.ShardLoad` / `Fathom.Shard.WriteCounter` pattern, no GenServer hop), and
  a stopped coordinator drops its row in `terminate` (`forget/1`). Reads
  (`lru_order/1`) are off the hot path — only the cold at-capacity admission path
  sorts the table.

  `touch/1` is a **no-op unless eviction is actually reachable** (a finite
  `:max_open_shards` and `:evict_idle_at_capacity` on), so with the cap disabled
  the hot path pays nothing.
  """
  use GenServer

  @table __MODULE__
  # A sibling table of `{shard_id, conn_count}` — a per-shard busy hint so `lru_order/1` returns only
  # plausibly-IDLE candidates (expert review #14). Without it, a long-lived stream (one checkout held
  # for hours — the primary django-libsql WebSocket transport) keeps its checkout-time recency stamp
  # and ages to the LRU front while continuously busy, so the bounded eviction probe wastes all its
  # slots on busy shards and admission 503s even though idle, evictable shards sit just past the probe
  # window (the soft cap silently degrading into a hard cap). The count is a best-effort HINT — the
  # `evict/2` primitive remains the atomic evict-if-idle arbiter — so a slightly-stale value is safe.
  @busy_table __MODULE__.Conns

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether idle-eviction (and therefore recency tracking) is active on this node."
  @spec enabled?() :: boolean()
  def enabled? do
    is_integer(Application.get_env(:fathom, :max_open_shards, :infinity)) and
      Application.get_env(:fathom, :evict_idle_at_capacity, true) == true
  end

  @doc """
  Record that `shard_id` was just used. Lock-free, last-writer-wins (two concurrent
  checkouts of one shard both stamp a fresh time — either is "recent"). Skipped when
  eviction can't fire, so the default-cap-disabled hot path pays nothing.
  """
  @spec touch(String.t()) :: :ok
  def touch(shard_id) do
    if enabled?(), do: :ets.insert(@table, {shard_id, System.monotonic_time()})
    :ok
  rescue
    # The table only exists once this process has started; a checkout during boot
    # (or in a test that didn't start it) must not crash the open path.
    ArgumentError -> :ok
  end

  @doc """
  Publish `shard_id`'s current checked-out-connection count (the coordinator's `map_size(conns)`,
  set on each grant/release) so `lru_order/1` can skip busy shards (#14). A best-effort hint;
  skipped when eviction can't fire.
  """
  @spec record_conns(String.t(), non_neg_integer()) :: :ok
  def record_conns(shard_id, count) do
    if enabled?(), do: :ets.insert(@busy_table, {shard_id, count})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Whether `shard_id` is currently serving connections (per its last-published count)."
  @spec busy?(String.t()) :: boolean()
  def busy?(shard_id) do
    case :ets.lookup(@busy_table, shard_id) do
      [{^shard_id, count}] -> count > 0
      _ -> false
    end
  rescue
    ArgumentError -> false
  end

  @doc "Drop a shard's rows (called from the coordinator's terminate)."
  @spec forget(String.t()) :: :ok
  def forget(shard_id) do
    :ets.delete(@table, shard_id)
    :ets.delete(@busy_table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Up to `limit` **idle** shard ids, least-recently-used first — the eviction candidate order. Busy
  shards (a checked-out connection per `record_conns/2`) are filtered out BEFORE the `limit` cut (#14),
  so the caller's bounded probe never wastes its slots on shards that can't be evicted anyway. Off the
  hot path (cold at-capacity admission only), so the full sort + filter is fine.
  """
  @spec lru_order(pos_integer()) :: [String.t()]
  def lru_order(limit) when is_integer(limit) and limit > 0 do
    @table
    |> :ets.tab2list()
    |> Enum.sort_by(fn {_id, t} -> t end)
    |> Enum.flat_map(fn {id, _t} -> if busy?(id), do: [], else: [id] end)
    |> Enum.take(limit)
  rescue
    ArgumentError -> []
  end

  @doc false
  def reset do
    :ets.delete_all_objects(@table)
    :ets.delete_all_objects(@busy_table)
  end

  @impl true
  def init(_opts) do
    # public + write_concurrency: many checkout processes stamp concurrently, each on
    # its own shard key; the only reader (the at-capacity admission path) is cold.
    opts = [:set, :public, :named_table, write_concurrency: true, read_concurrency: true]
    :ets.new(@table, opts)
    :ets.new(@busy_table, opts)

    {:ok, %{}}
  end
end
