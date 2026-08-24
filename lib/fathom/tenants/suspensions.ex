defmodule Fathom.Tenants.Suspensions do
  @moduledoc """
  A node-local set of suspended (administratively-disabled) shard ids — the admission deny gate
  for tenant suspension (expert review 2026-07-14 #20).

  The reversible sibling of `Fathom.Tenants.Tombstones`: same ETS + Oban-notifier + boot-load
  shape (checked O(1) in `Fathom.Shards.ensure/1` so a suspended tenant is denied a new stream
  without a Postgres round-trip on the near-hot open path), but a suspension **comes and goes** —
  `resume/1` removes it. So the notification carries an add/remove flag and the periodic refresh
  **reconciles** the set against the directory (`:tenant_suspend_refresh_ms`, default 5 min) rather
  than only adding, so a missed resume converges too.

  Fail-open on a Postgres outage (an empty/failed load leaves the last-known set in place, never
  wrongly suspends), matching the directory's best-effort contract.
  """
  use GenServer

  require Logger

  alias Fathom.Directory
  alias Fathom.Tenants.DenyList

  @table __MODULE__
  @channel :fathom_tenant_suspension
  @default_refresh_ms 300_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "The Oban LISTEN/NOTIFY channel suspend/resume broadcasts on."
  @spec channel() :: atom()
  def channel, do: @channel

  @doc "True if `shard_id` is currently suspended — checked O(1) in admission."
  @spec suspended?(String.t()) :: boolean()
  def suspended?(shard_id) do
    :ets.member(@table, shard_id)
  rescue
    ArgumentError -> false
  end

  @doc "Records `shard_id` suspended locally (called on the suspending node for immediacy)."
  @spec put(String.t()) :: :ok
  def put(shard_id) do
    insert(@table, shard_id)
    :ok
  end

  @doc "Clears `shard_id`'s suspension locally (called on the resuming node)."
  @spec remove(String.t()) :: :ok
  def remove(shard_id) do
    delete(@table, shard_id)
    :ok
  end

  @impl true
  def init(opts) do
    # `:table` / `:name` / `:loader` are seams for an isolated test instance; the app singleton
    # uses the module defaults. `:retry_ms` overrides the first fast-retry delay per-instance.
    table = Keyword.get(opts, :table, @table)
    loader = Keyword.get(opts, :loader, &Directory.suspended_shard_ids/0)
    retry_ms = Keyword.get(opts, :retry_ms, DenyList.initial_retry_ms())

    :ets.new(table, [:set, :public, :named_table, read_concurrency: true])

    listen()

    state = %{table: table, loader: loader, retry_ms: retry_ms}

    case reconcile(state) do
      :ok ->
        schedule_refresh()
        {:ok, Map.put(state, :loaded, true)}

      # Expert review #33: a FAILED boot reconcile must not wait the full refresh interval — that
      # leaves the suspend gate empty (403 contract broken) for up to 5 min during a Postgres wobble
      # coincident with this restart. Fast-retry with backoff, and signal degraded.
      {:error, reason} ->
        DenyList.degraded(:suspensions, reason)
        schedule_retry(retry_ms)
        {:ok, Map.put(state, :loaded, false)}
    end
  end

  @impl true
  def handle_info({:notification, @channel, %{"shard_id" => id, "suspended" => true}}, state)
      when is_binary(id) do
    insert(state.table, id)
    {:noreply, state}
  end

  def handle_info({:notification, @channel, %{"shard_id" => id, "suspended" => false}}, state)
      when is_binary(id) do
    delete(state.table, id)
    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    reconcile(state)
    schedule_refresh()
    {:noreply, state}
  end

  def handle_info(:retry_load, %{loaded: false} = state) do
    case reconcile(state) do
      :ok ->
        DenyList.recovered(:suspensions)
        schedule_refresh()
        {:noreply, %{state | loaded: true}}

      {:error, reason} ->
        DenyList.degraded(:suspensions, reason)
        next = DenyList.next_retry_ms(state.retry_ms)
        schedule_retry(next)
        {:noreply, %{state | retry_ms: next}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp insert(table, id) do
    :ets.insert(table, {id})
  rescue
    ArgumentError -> :ok
  end

  defp delete(table, id) do
    :ets.delete(table, id)
  rescue
    ArgumentError -> :ok
  end

  # Full reconcile against the directory: add every currently-suspended id, then drop any the
  # directory no longer shows suspended (a resume this node missed). Insert-then-prune (not
  # clear-then-load) so a still-suspended id is never briefly absent from the gate. Returns
  # `:ok` on a successful directory read, `{:error, reason}` on a failure so init/retry can
  # fast-retry (expert review #33) instead of leaving the gate empty for the full refresh interval.
  defp reconcile(%{table: table, loader: loader}) do
    current = MapSet.new(loader.())
    Enum.each(current, &insert(table, &1))

    table
    |> all_ids()
    |> Enum.reject(&MapSet.member?(current, &1))
    |> Enum.each(&delete(table, &1))

    :ok
  rescue
    e ->
      Logger.warning("suspension reconcile failed: #{Exception.message(e)}")
      {:error, e}
  catch
    :exit, reason ->
      Logger.warning("suspension reconcile failed: #{inspect(reason)}")
      {:error, reason}
  end

  defp all_ids(table) do
    :ets.select(table, [{{:"$1"}, [], [:"$1"]}])
  rescue
    ArgumentError -> []
  end

  defp listen do
    :ok = Oban.Notifier.listen(Oban, [@channel])
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, refresh_ms())
  end

  defp schedule_retry(ms) do
    Process.send_after(self(), :retry_load, ms)
  end

  defp refresh_ms,
    do: Application.get_env(:fathom, :tenant_suspend_refresh_ms, @default_refresh_ms)
end
