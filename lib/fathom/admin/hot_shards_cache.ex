defmodule Fathom.Admin.HotShardsCache do
  @moduledoc """
  A single-flight ETS TTL cache for `Fathom.Admin.Fleet.hot_shards/0`.

  Expert review 2026-10-15 #14: `AdminShardsLive` ran `Fleet.hot_shards/0` (a `shard_load_samples`
  scan over the load window) on its own 5 s timer per open tab, the O(viewers) shape the overview
  and migrations pages already shed. Inside `:admin_hot_shards_cache_ms` (default 10 s) a tab reuses
  the last result; at expiry one caller computes and the rest serve the stale set (or, with none
  yet, wait briefly for the winner's). Computed in the CALLER (the LiveView's async task), never in
  this process, which only owns the table. Without the owner (dashboard gating off, tests) there is
  no table and `get/1` computes uncached.
  """
  use GenServer

  alias Fathom.Admin.Fleet

  @table :fathom_admin_hot_shards
  @default_ttl_ms 10_000
  @lock_ms 30_000
  @wait_ms 15_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "The fleet hot set, cached (see the moduledoc)."
  @spec get() :: [Fathom.Rebalancer.LoadSample.t()]
  def get, do: get(&Fleet.hot_shards/0)

  @doc false
  # `compute` is a seam so a test can count how many callers actually compute.
  @spec get((-> term())) :: term()
  def get(compute) when is_function(compute, 0) do
    ttl = Application.get_env(:fathom, :admin_hot_shards_cache_ms, @default_ttl_ms)
    now = System.monotonic_time(:millisecond)

    case lookup() do
      {at, value} when now - at < ttl -> value
      stale -> refill(stale, compute, now)
    end
  end

  defp refill(stale, compute, now) do
    case claim(now) do
      :acquired ->
        try do
          value = compute.()
          store({System.monotonic_time(:millisecond), value})
          value
        after
          release()
        end

      :busy ->
        case stale do
          {_at, value} -> value
          nil -> await(compute, now + @wait_ms)
        end

      :no_table ->
        compute.()
    end
  end

  # `insert_new` is the atomic claim; a lock past its expiry belongs to a dead winner, so take it
  # over (delete-then-insert_new keeps two takers resolving to exactly one winner).
  defp claim(now) do
    if :ets.insert_new(@table, {:refill, now + @lock_ms}) do
      :acquired
    else
      case :ets.lookup(@table, :refill) do
        [{:refill, expires}] when expires <= now ->
          :ets.delete_object(@table, {:refill, expires})

          if :ets.insert_new(@table, {:refill, now + @lock_ms}), do: :acquired, else: :busy

        _ ->
          :busy
      end
    end
  rescue
    ArgumentError -> :no_table
  end

  defp release do
    :ets.delete(@table, :refill)
  rescue
    ArgumentError -> :ok
  end

  defp await(compute, deadline) do
    case lookup() do
      {_at, value} ->
        value

      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          compute.()
        else
          Process.sleep(25)
          await(compute, deadline)
        end
    end
  end

  defp lookup do
    case :ets.lookup(@table, :page) do
      [{:page, entry}] -> entry
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp store(entry) do
    :ets.insert(@table, {:page, entry})
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set])
    {:ok, nil}
  end
end
