defmodule Fathom.Shards.HeldCache do
  @moduledoc """
  A short, node-local negative cache for "this shard's lease is held by another node" (expert review
  2026-10-14 N4, the bytes half; the temp-leak half is 2a6805e1).

  ## Why

  A cold open overlaps a full object GET with `acquire_lease`. While a dead owner's lease is still
  held (the RTO floor is the lease TTL), every client retry starts a coordinator, the acquire answers
  `held`, the pull is cancelled, and the bytes already moved are thrown away: measured on the chaos
  rig at ~33 MiB of download per steal for a 5 MiB object against ~10 MiB for a clean steal, times
  every retry.

  ## What

  A `{:shard_held, owner, stealable_at}` produced by a real checkout-originated open is remembered
  for `min(stealable_at - now, ceiling)` ms (ceiling `:held_refusal_cache_ms`, default 2000, 0
  disables). A checkout that would START a coordinator inside that window is answered with the SAME
  error before anything starts, so it makes zero Storage calls. The error is the held error itself,
  so the client-visible status and the held-retry machinery in `Fathom.Shards` (aimed crash hold,
  handoff hold) see exactly what the real acquire would have given them.

  Only a held error that carries an integer `stealable_at` is cached; a `nil` instant (S3's
  lost-create-race) is not, because the window is bounded by it.

  ## Decisions

    * **Early release.** If the holder releases cleanly (or a handoff drain finishes) inside the
      window, a checkout stays refused for at most the remainder of the window, and never longer than
      the ceiling. The refusal is the retryable one, and refusing is the safe direction.
    * **Handoff.** A pinned handoff target must QUEUE for the source's drain, not poll a cache, so
      `Fathom.Shards` clears the entry as it enters the handoff hold and the retries are real.
    * **This node holding the lease.** A running coordinator is found in the Registry before this
      cache is consulted, so it is never answered from it. A non-checkout `Shards.ensure/1` never
      consults or populates the cache, and clears the entry when it starts a coordinator, so a shard
      this node opened by that path and later dropped is not refused on a stale "held elsewhere".
    * **Hot path.** Consulted on the cold-start branch of a checkout only: one ETS lookup. A running
      shard's checkout never reaches it.
  """
  use GenServer

  @table __MODULE__
  @default_ceiling_ms 2_000
  @sweep_threshold 1024
  # Last-sweep timestamp, stored as a 2-tuple in the same table. Its shape never matches the 4-tuple
  # rows the sweep selects, and a ShardId is a string, so no lookup can collide with it.
  @sweep_stamp :__last_sweep__

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The held error recorded for `shard_id` moments ago, or nil. READ-ONLY: a hit never extends the
  window, so the ceiling bounds how long a retry storm can be turned away.
  """
  @spec recent(Fathom.ShardId.t()) :: {:shard_held, term(), integer()} | nil
  def recent(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{_, owner, at, until}] ->
        if System.monotonic_time(:millisecond) < until, do: {:shard_held, owner, at}

      [] ->
        nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc """
  Records a real held answer. `at` is the system-clock instant (ms) the holder becomes stealable.
  A no-op when disabled, when `at` is not an integer, or when it has already passed.
  """
  @spec put(Fathom.ShardId.t(), term(), term()) :: :ok
  def put(shard_id, owner, at) when is_integer(at) do
    window = min(at - System.system_time(:millisecond), ceiling_ms())

    if window > 0 do
      now = System.monotonic_time(:millisecond)
      maybe_sweep(now)
      :ets.insert(@table, {shard_id, owner, at, now + window})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  def put(_shard_id, _owner, _at), do: :ok

  @doc "Forgets `shard_id`."
  @spec clear(Fathom.ShardId.t()) :: :ok
  def clear(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # The full-table sweep is amortized: at most once per ceiling window, and only once the table is
  # above the threshold. The old per-put sweep cost O(rows) under the table's write lock on every
  # insert of a failover herd (248 us per put at 15k unexpired rows; expert review 2026-10-21 #10).
  # That is safe because `recent/1` checks expiry itself, so a row the sweep has not reached is
  # never served.
  defp maybe_sweep(now) do
    if :ets.info(@table, :size) >= @sweep_threshold and sweep_due?(now) do
      :ets.insert(@table, {@sweep_stamp, now})
      sweep(now)
    end

    :ok
  end

  defp sweep_due?(now) do
    case :ets.lookup(@table, @sweep_stamp) do
      [{@sweep_stamp, last}] -> now - last >= max(ceiling_ms(), 1)
      [] -> true
    end
  end

  defp sweep(now),
    do: :ets.select_delete(@table, [{{:_, :_, :_, :"$1"}, [{:"=<", :"$1", now}], [true]}])

  defp ceiling_ms, do: Application.get_env(:fathom, :held_refusal_cache_ms, @default_ceiling_ms)

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, nil}
  end
end
