defmodule Fathom.Shards.Lru do
  @moduledoc """
  Node-local recency index for **idle-eviction at capacity**.

  When a node is at `:max_open_shards` and a new shard wants in, `Fathom.Shards`
  would rather evict the least-recently-used *idle* shard (flush + drop + release
  its lease) than refuse the open with a 503 — a shard only fails over away from
  its home, and an idle shard's file is bottomless-backed, so dropping it costs
  only a cold re-open if it's touched again. This table is how the router picks
  *which* idle shard is coldest.

  It's a pair of public ETS tables written **lock-free from the checkout process**
  (`:ets.insert`, `write_concurrency` — the `Fathom.ShardLoad` /
  `Fathom.Shard.WriteCounter` pattern, no GenServer hop): a `set` of
  `{shard_id, monotonic_time}` (each shard's current stamp — the authority) plus an
  `:ordered_set` of `{{monotonic_time, shard_id}}` keys (the recency order). A
  stopped coordinator drops its rows in `terminate` (`forget/1`).

  Two tables because the probe must be cheap AT capacity (review 2026-07-23 #14):
  the old single-table read did `tab2list |> sort` — an O(N log N) copy of the
  whole recency table into the admitting stream's heap (milliseconds + a
  multi-hundred-KB allocation at 30k open shards) on **every** at-capacity
  admission, which is a dense node's designed steady state, exactly when the node
  is saturated. `lru_order/1` now walks the ordered table from the cold end and
  stops after `limit` validated candidates — O(probes), not O(N log N). A `touch`
  replaces the shard's previous order key (so the order table stays ~one key per
  shard); a concurrent-touch race can leave an occasional stale key, which the
  walk detects (stamp ≠ the set's current stamp) and deletes lazily.

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
  # Recency order: `{{monotonic_time, shard_id}}` composite keys in an :ordered_set, walked
  # cold-end-first by lru_order/1. The @table stamp is the authority; an order key whose stamp
  # no longer matches is stale and lazily deleted during the walk.
  @order_table __MODULE__.Order

  # How stale a recency stamp may be before touch/1 rewrites it (native units). 1s is far finer
  # than any eviction decision needs — the cold end of the LRU is separated by minutes — while
  # collapsing a shard doing 1000 checkouts/s from ~4000 ETS ops/s to ~1000 cheap lookups.
  @stamp_granularity System.convert_time_unit(1, :second, :native)

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
    if enabled?() do
      stamp = System.monotonic_time()

      # Replace the previous order key so the order table stays ~one key per shard rather than
      # growing per checkout. Two concurrent touches of one shard can each delete the same old
      # key and insert their own — the loser's key goes stale and the probe walk cleans it.
      #
      # COARSENED (expert review 2026-07-24 #24): re-stamp only when the existing stamp is older
      # than @stamp_granularity. The write side used to run 4 ETS ops on EVERY checkout — and three
      # of them hit @order_table, an :ordered_set with write_concurrency, which on OTP 25+ selects
      # the contention-adapting CA tree, so a delete+insert pair rewrote a CA-tree key per checkout
      # at exactly the rate this architecture is optimized for. (The 2026-07-23 audit's #14 fixed
      # the READ side, the at-capacity probe; this is the write side.)
      #
      # Safe because recency is explicitly a HINT — see the moduledoc: `record_conns/2` is
      # best-effort and `evict/2` remains the atomic evict-if-idle arbiter, re-checking idleness
      # under the coordinator. So a stamp up to one granularity window stale can at worst pick a
      # slightly-less-cold victim, which costs one cold re-open of a bottomless-backed idle shard.
      # It can never evict a busy shard.
      case :ets.lookup(@table, shard_id) do
        [{^shard_id, prev}] when stamp - prev < @stamp_granularity ->
          :ok

        [{^shard_id, prev}] ->
          :ets.delete(@order_table, {prev, shard_id})
          write_stamp(shard_id, stamp)

        _ ->
          write_stamp(shard_id, stamp)
      end
    end

    :ok
  rescue
    # The tables only exist once this process has started; a checkout during boot
    # (or in a test that didn't start it) must not crash the open path.
    ArgumentError -> :ok
  end

  defp write_stamp(shard_id, stamp) do
    :ets.insert(@table, {shard_id, stamp})
    :ets.insert(@order_table, {{stamp, shard_id}})
    :ok
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
    # Drop the current order key too (a touch racing this forget can leave a stray —
    # the probe walk cleans it lazily).
    case :ets.lookup(@table, shard_id) do
      [{^shard_id, stamp}] -> :ets.delete(@order_table, {stamp, shard_id})
      _ -> :ok
    end

    :ets.delete(@table, shard_id)
    :ets.delete(@busy_table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Up to `limit` **idle** shard ids, least-recently-used first — the eviction candidate order. Busy
  shards (a checked-out connection per `record_conns/2`) are filtered out BEFORE the `limit` cut (#14),
  so the caller's bounded probe never wastes its slots on shards that can't be evicted anyway.

  Walks the ordered table from the cold end, stopping after `limit` validated candidates —
  O(probes + stale/busy skipped), never the old full `tab2list |> sort` (review 2026-07-23 #14).
  Stale keys met along the way are deleted (self-cleaning), so each is visited at most once.
  """
  @spec lru_order(pos_integer()) :: [String.t()]
  def lru_order(limit) when is_integer(limit) and limit > 0 do
    collect_idle(:ets.first(@order_table), limit, [])
  rescue
    ArgumentError -> []
  end

  defp collect_idle(_key, 0, acc), do: Enum.reverse(acc)
  defp collect_idle(:"$end_of_table", _limit, acc), do: Enum.reverse(acc)

  defp collect_idle({stamp, id} = key, limit, acc) do
    next = :ets.next(@order_table, key)

    case :ets.lookup(@table, id) do
      [{^id, ^stamp}] ->
        # Current — a real candidate unless busy (busy keeps its key: still valid recency).
        if busy?(id),
          do: collect_idle(next, limit, acc),
          else: collect_idle(next, limit - 1, [id | acc])

      _ ->
        # Superseded stamp or forgotten shard — self-clean and keep walking.
        :ets.delete(@order_table, key)
        collect_idle(next, limit, acc)
    end
  end

  @doc false
  def reset do
    :ets.delete_all_objects(@table)
    :ets.delete_all_objects(@busy_table)
    :ets.delete_all_objects(@order_table)
  end

  @impl true
  def init(_opts) do
    # public + write_concurrency: many checkout processes stamp concurrently, each on
    # its own shard key; the only reader (the at-capacity admission path) is cold.
    opts = [:set, :public, :named_table, write_concurrency: true, read_concurrency: true]
    :ets.new(@table, opts)
    :ets.new(@busy_table, opts)

    :ets.new(@order_table, [
      :ordered_set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    {:ok, %{}}
  end
end
