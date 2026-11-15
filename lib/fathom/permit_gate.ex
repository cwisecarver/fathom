defmodule Fathom.PermitGate do
  @moduledoc """
  The node-wide counting permit that `Fathom.Shard.ScanGate`, `Fathom.Shard.BackgroundScanGate` and
  `Fathom.Shard.Replication.SeedGate` share (cleanliness audit 2026-11-13 #31). Each was a copy of
  the same ~60-line GenServer and differed only in where the cap comes from, what "unlimited"
  means, and what a timed-out caller is told.

  A GenServer with MONITORS: one call per acquire, and a permit tied to the holder's monitor, so a
  holder that is killed (a flush task, an Oban job, a seed task `Process.exit(pid, :kill)`ed by
  `Session`, none of which run an `after`) cannot leak it: the `:DOWN` frees it. Waiters queue
  oldest-first and a waiter that gives up (`withdraw`) or dies leaves the queue.

  The cap is a FUNCTION, `cap: (-> pos_integer() | nil)`, read at every acquire and every grant so a
  `Application.put_env` takes effect without a restart. `nil` is "unlimited": acquire returns
  `:ok` without calling the server, and a grant hands a freed permit to every waiter. Each gate
  maps its own configuration onto that one meaning, and keeps its own public API and its own timeout
  reply:

    * `ScanGate` / `BackgroundScanGate`: `cap/0` is already `pos_integer() | nil` (`0`/`false`
      disables).
    * `SeedGate`: any non-positive-integer env value (`0`, `nil`, junk) is unlimited.

  Fail-open stays each caller's: `acquire/3` returns `:ok` when the gate is not running (a script or
  test without the application), because a missing gate must not fail a flush or a seed.
  """
  use GenServer

  @type cap :: pos_integer() | nil

  @doc """
  Start a gate. Options: `:cap` (required, `(-> cap())`) and `:name` (required: each gate passes its
  own module as the default).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    cap_fun = Keyword.fetch!(opts, :cap)
    GenServer.start_link(__MODULE__, cap_fun, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Wait for a permit, up to `timeout` ms (`:infinity` allowed). `cap` is the caller's reading of the
  cap (`nil` = unlimited, so `:ok` at once). `:ok`: held until `release/1` or the caller exits.
  `:timeout`: none came free in time and nothing is held (the waiter is withdrawn). `:ok` when the
  gate process is not running.
  """
  @spec acquire(GenServer.server(), cap(), timeout()) :: :ok | :timeout
  def acquire(_gate, nil, _timeout), do: :ok

  def acquire(gate, cap, timeout) when is_integer(cap) and cap > 0 do
    GenServer.call(gate, {:acquire, cap}, timeout)
  catch
    :exit, {:noproc, _} ->
      :ok

    :exit, {:timeout, _} ->
      # Withdraw, so a permit granted after we stopped waiting is not held by a caller that has gone
      # on to fail. Our own exit would free it too; this frees it now.
      GenServer.cast(gate, {:withdraw, self()})
      :timeout
  end

  @doc "Give the caller's permit back. A no-op if it holds none."
  @spec release(GenServer.server()) :: :ok
  def release(gate), do: GenServer.cast(gate, {:release, self()})

  @doc "Permits held and callers waiting. For tests and the dashboard."
  @spec stats(GenServer.server()) :: %{held: non_neg_integer(), waiting: non_neg_integer()}
  def stats(gate), do: GenServer.call(gate, :stats)

  @impl true
  def init(cap_fun), do: {:ok, %{cap: cap_fun, held: %{}, waiting: :queue.new()}}

  @impl true
  def handle_call({:acquire, cap}, {pid, _} = from, state) do
    if map_size(state.held) < cap do
      {:reply, :ok, hold(state, pid)}
    else
      {:noreply, %{state | waiting: :queue.in({from, Process.monitor(pid)}, state.waiting)}}
    end
  end

  def handle_call(:stats, _from, state),
    do: {:reply, %{held: map_size(state.held), waiting: :queue.len(state.waiting)}, state}

  @impl true
  def handle_cast({:release, pid}, state), do: {:noreply, state |> free(pid) |> grant()}

  def handle_cast({:withdraw, pid}, state),
    do: {:noreply, state |> unwait(pid) |> free(pid) |> grant()}

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _}, state),
    do: {:noreply, state |> unwait(pid) |> free(pid) |> grant()}

  defp hold(state, pid), do: %{state | held: Map.put(state.held, pid, Process.monitor(pid))}

  defp free(state, pid) do
    case Map.pop(state.held, pid) do
      {nil, _} ->
        state

      {ref, held} ->
        Process.demonitor(ref, [:flush])
        %{state | held: held}
    end
  end

  defp unwait(state, pid) do
    {gone, keep} =
      state.waiting |> :queue.to_list() |> Enum.split_with(fn {{p, _}, _} -> p == pid end)

    Enum.each(gone, fn {_, ref} -> Process.demonitor(ref, [:flush]) end)
    %{state | waiting: :queue.from_list(keep)}
  end

  # Hand freed permits to waiters, oldest first. Reads the cap now: it may have changed.
  defp grant(state) do
    cap = state.cap.()

    if (cap == nil or map_size(state.held) < cap) and not :queue.is_empty(state.waiting) do
      {{:value, {{pid, _} = from, ref}}, rest} = :queue.out(state.waiting)
      Process.demonitor(ref, [:flush])
      GenServer.reply(from, :ok)
      grant(hold(%{state | waiting: rest}, pid))
    else
      state
    end
  end
end
