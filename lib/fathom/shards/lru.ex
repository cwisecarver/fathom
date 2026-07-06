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

  @doc "Drop a shard's row (called from the coordinator's terminate)."
  @spec forget(String.t()) :: :ok
  def forget(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Up to `limit` shard ids, **least-recently-used first** — the eviction candidate
  order. Off the hot path (cold at-capacity admission only), so a full sort is fine;
  `limit` bounds how many the caller will probe.
  """
  @spec lru_order(pos_integer()) :: [String.t()]
  def lru_order(limit) when is_integer(limit) and limit > 0 do
    @table
    |> :ets.tab2list()
    |> Enum.sort_by(fn {_id, t} -> t end)
    |> Enum.take(limit)
    |> Enum.map(fn {id, _t} -> id end)
  rescue
    ArgumentError -> []
  end

  @doc false
  def reset, do: :ets.delete_all_objects(@table)

  @impl true
  def init(_opts) do
    # public + write_concurrency: many checkout processes stamp concurrently, each on
    # its own shard key; the only reader (the at-capacity admission path) is cold.
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
