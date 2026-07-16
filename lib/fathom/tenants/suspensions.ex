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

  @table __MODULE__
  @channel :fathom_tenant_suspension
  @default_refresh_ms 300_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

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
    insert(shard_id)
    :ok
  end

  @doc "Clears `shard_id`'s suspension locally (called on the resuming node)."
  @spec remove(String.t()) :: :ok
  def remove(shard_id) do
    delete(shard_id)
    :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    listen()
    reconcile()
    schedule_refresh()

    {:ok, %{}}
  end

  @impl true
  def handle_info({:notification, @channel, %{"shard_id" => id, "suspended" => true}}, state)
      when is_binary(id) do
    insert(id)
    {:noreply, state}
  end

  def handle_info({:notification, @channel, %{"shard_id" => id, "suspended" => false}}, state)
      when is_binary(id) do
    delete(id)
    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    reconcile()
    schedule_refresh()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp insert(id) do
    :ets.insert(@table, {id})
  rescue
    ArgumentError -> :ok
  end

  defp delete(id) do
    :ets.delete(@table, id)
  rescue
    ArgumentError -> :ok
  end

  # Full reconcile against the directory: add every currently-suspended id, then drop any the
  # directory no longer shows suspended (a resume this node missed). Insert-then-prune (not
  # clear-then-load) so a still-suspended id is never briefly absent from the gate.
  defp reconcile do
    current = MapSet.new(Directory.suspended_shard_ids())
    Enum.each(current, &insert/1)

    @table
    |> all_ids()
    |> Enum.reject(&MapSet.member?(current, &1))
    |> Enum.each(&delete/1)

    :ok
  rescue
    e ->
      Logger.warning("suspension reconcile failed: #{Exception.message(e)}")
      :ok
  catch
    :exit, reason ->
      Logger.warning("suspension reconcile failed: #{inspect(reason)}")
      :ok
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

  defp refresh_ms,
    do: Application.get_env(:fathom, :tenant_suspend_refresh_ms, @default_refresh_ms)
end
