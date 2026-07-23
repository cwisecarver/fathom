defmodule Fathom.RateLimiter do
  @moduledoc """
  A per-node, per-key fixed-window counter — the small ETS token bucket behind the
  control-plane throttles (expert review #34): the admin BasicAuth brute-force lockout
  (`FathomWeb.Router.require_admin_auth`) and the `/api` request-rate limit.

  One public ETS table, keyed `{bucket, key}` (bucket separates the throttles; key is the
  source IP tuple), holding `{count, window_start_ms}`. Callers hit it **lock-free from their
  own process** (`:ets.update_counter` / `:ets.lookup`) — the `Fathom.ShardLoad` /
  `Fathom.Directory.Recorder` pattern — with no GenServer hop on the request path. The owning
  GenServer only creates the table and periodically sweeps stale entries so a spray of unique
  source IPs can't grow it unbounded.

  Windows are approximate under concurrency (a lookup-then-increment can race a rollover), which
  is the right trade for a throttle: it may admit or refuse a request or two at the boundary,
  never far off. Every op **fails open** if the table isn't up yet (pre-boot): a limiter that
  can't answer must never block a legitimate request. All throttles that use it are config-gated
  and off by default, so nothing pays until an operator enables them.
  """
  use GenServer

  @table __MODULE__
  @default_sweep_ms 600_000
  # Entries idle longer than this are swept — comfortably larger than any real throttle window,
  # so a live window is never dropped, and a burst of one-shot IPs can't accumulate forever.
  @stale_after_ms 3_600_000

  @doc false
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Current count for `{bucket, key}` within `window_ms` (0 if absent or the window elapsed)."
  @spec count(atom(), term(), pos_integer(), integer()) :: non_neg_integer()
  def count(bucket, key, window_ms, now \\ now_ms()) do
    case :ets.lookup(@table, {bucket, key}) do
      [{_, c, start}] when now - start < window_ms -> c
      _ -> 0
    end
  rescue
    # Table not up (pre-boot / limiter not started): fail open — never block on a missing limiter.
    ArgumentError -> 0
  end

  @doc "Increment `{bucket, key}` in the current window (resetting an elapsed one); returns the new count."
  @spec bump(atom(), term(), pos_integer(), integer()) :: non_neg_integer()
  def bump(bucket, key, window_ms, now \\ now_ms()) do
    k = {bucket, key}

    case :ets.lookup(@table, k) do
      [{^k, _c, start}] when now - start < window_ms ->
        :ets.update_counter(@table, k, {2, 1})

      # Window elapsed — reset to a fresh window.
      [{^k, _c, _old}] ->
        :ets.insert(@table, {k, 1, now})
        1

      # New key — insert_new so a concurrent inserter doesn't get clobbered; if it landed first,
      # fall through to the increment path (bounded: one retry).
      [] ->
        if :ets.insert_new(@table, {k, 1, now}), do: 1, else: bump(bucket, key, window_ms, now)
    end
  rescue
    ArgumentError -> 0
  end

  @doc """
  Allow up to `limit` hits per `window_ms` for `{bucket, key}`: `:ok` counts the hit, `:limited`
  refuses (over budget). Peek-then-bump, so exactly `limit` hits pass before the window refuses.
  """
  @spec check(atom(), term(), pos_integer(), pos_integer(), integer()) :: :ok | :limited
  def check(bucket, key, limit, window_ms, now \\ now_ms()) do
    if count(bucket, key, window_ms, now) >= limit do
      :limited
    else
      bump(bucket, key, window_ms, now)
      :ok
    end
  end

  @doc "Clear `{bucket, key}` (e.g. a successful admin auth resets its failure count)."
  @spec forget(atom(), term()) :: :ok
  def forget(bucket, key) do
    :ets.delete(@table, {bucket, key})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  # Test hook: the table is node-global, so tests clear it rather than inherit each other's counters.
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    schedule_sweep()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:sweep, state) do
    cutoff = now_ms() - @stale_after_ms
    # Delete every entry whose window started before the cutoff (idle far longer than any window).
    :ets.select_delete(@table, [{{:_, :_, :"$1"}, [{:<, :"$1", cutoff}], [true]}])
    schedule_sweep()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule_sweep do
    Process.send_after(
      self(),
      :sweep,
      Application.get_env(:fathom, :rate_limiter_sweep_ms, @default_sweep_ms)
    )
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
