defmodule Fathom.Tenants.LocalEraser do
  @moduledoc """
  One serialized worker that erases a tombstoned shard's node-local files in BATCHES (expert review
  2026-11-04 #6).

  Symptom it fixes: every tombstone used to `Task.start` its own `erase_local/1`, each doing a full
  listing of the fleet-sized data dir. Measured on unfixed code (100k stub files, 200 tombstones,
  `test/fathom/tenants/tombstone_burst_bench_test.exs`): +2.8 GB peak memory, 7.7 s to finish, and an
  unrelated shard's point-query p99 84 -> 736 us with a 7.4 s worst stall.

  Now: `erase_async/1` (the tombstone NOTIFY hook) queues the id and arms a short window
  (`:tombstone_erase_window_ms`, default 500 ms); when it fires, ONE `Fathom.Tenants.erase_local_batch/1`
  does one listing for the whole batch. `erase_now/1` (the synchronous `Tenants.erase_local/1`) flushes
  immediately, taking any queued ids with it, and replies with that id's own result. Erasure runs
  inside this process, so batches are serialized and ids arriving mid-batch coalesce into the next.
  A batch that raises replies `{:error, ...}` to its waiters and the worker survives; the boot
  `Tombstones.sweep_local/0` re-runs every tombstoned id on disk, as before.
  """
  use GenServer

  require Logger

  @default_window_ms 500
  # Longer than the batch's own stop budget (120 s per coordinator) plus a listing.
  @call_timeout 300_000

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Queue `shard_id` for the next batch. Never blocks the caller."
  @spec erase_async(String.t()) :: :ok
  def erase_async(shard_id) do
    if Process.whereis(__MODULE__) do
      GenServer.cast(__MODULE__, {:erase, shard_id})
    else
      # Not started (a bare unit test): do the work inline, off the caller.
      Task.start(fn -> Fathom.Tenants.erase_local_batch([shard_id]) end)
    end

    :ok
  end

  @doc "Erase `shard_id` now (flushing any queued ids with it) and return its result."
  @spec erase_now(String.t()) :: :ok | {:error, term()}
  def erase_now(shard_id) do
    GenServer.call(__MODULE__, {:erase_now, shard_id}, @call_timeout)
  catch
    :exit, {:noproc, _} ->
      [{_, result}] = Fathom.Tenants.erase_local_batch([shard_id])
      result
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       pending: [],
       waiters: [],
       timer: nil,
       window_ms:
         Keyword.get(
           opts,
           :window_ms,
           Application.get_env(:fathom, :tombstone_erase_window_ms, @default_window_ms)
         )
     }}
  end

  @impl true
  def handle_cast({:erase, id}, state),
    do: {:noreply, arm(%{state | pending: [id | state.pending]})}

  @impl true
  def handle_call({:erase_now, id}, from, state) do
    state = %{state | pending: [id | state.pending], waiters: [{from, id} | state.waiters]}
    {:noreply, flush(state)}
  end

  @impl true
  def handle_info(:flush, state), do: {:noreply, flush(%{state | timer: nil})}
  def handle_info(_msg, state), do: {:noreply, state}

  defp arm(%{timer: nil} = state),
    do: %{state | timer: Process.send_after(self(), :flush, state.window_ms)}

  defp arm(state), do: state

  defp flush(%{pending: []} = state), do: state

  defp flush(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    ids = state.pending |> Enum.reverse() |> Enum.uniq()

    results =
      try do
        Map.new(Fathom.Tenants.erase_local_batch(ids))
      rescue
        e ->
          Logger.warning("tombstone local erase batch failed: #{Exception.message(e)}")
          Map.new(ids, &{&1, {:error, {:local_erase, e}}})
      catch
        :exit, reason ->
          Logger.warning("tombstone local erase batch failed: #{inspect(reason)}")
          Map.new(ids, &{&1, {:error, {:local_erase, reason}}})
      end

    for {from, id} <- state.waiters, do: GenServer.reply(from, Map.get(results, id, :ok))
    %{state | pending: [], waiters: [], timer: nil}
  end
end
