defmodule Fathom.Shard.ScanGate do
  @moduledoc """
  Node-wide permit for the flush SCAN (`quick_check` + `VACUUM INTO`), separate from the upload cap
  (expert review 2026-10-20 #19).

  `FlushGate`'s default cap is `min(pool/4, +SDio)`, i.e. up to the dirty-IO thread count, and each
  in-flight flush runs its scan as one dirty-IO NIF job that holds a thread for the whole page scan
  (multi-ms to seconds on a large shard). A phase-aligned wave of that many flushes can therefore
  occupy EVERY dirty-IO thread, and every tenant statement on the node queues behind the scans.
  This gate bounds the number of scans in flight to `max(1, SDio - 2)`, leaving at least two dirty
  threads for tenant work. The upload cap is unchanged (a slot is still held across the PUT); this
  permit is held across the scan only, never the PUT, so S3 latency does not hold it.

  Not a revert of the 2026-10-06 experiment recorded in `FlushGate`'s moduledoc: that one raised
  the upload cap to 50 AT THE SAME TIME as adding a scan permit, and the cost was the extra
  checkpoints from more frequent flushes. This leaves the upload cap (and so the number of
  flushes that proceed) exactly as it was.

  A GenServer with monitors (like `Fathom.Shard.Replication.SeedGate`): one call per flush, and a
  permit tied to the holder's monitor so a killed flush task cannot leak it. `acquire/1` waits at
  most `timeout_ms`; on timeout (or if the gate is not running) the caller scans WITHOUT a permit
  rather than failing the durability flush.

  `:shard_flush_scan_permits` pins the count (`0`/`false` disables the gate).
  """
  use GenServer

  @doc false
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc """
  The permit count: `:shard_flush_scan_permits` if set to a positive integer, `nil` if set to
  `0`/`false` (gate off), else `max(1, dirty_io_schedulers - 2)`.
  """
  @spec cap() :: pos_integer() | nil
  def cap do
    case Application.get_env(:fathom, :shard_flush_scan_permits, :default) do
      :default -> default_cap(:erlang.system_info(:dirty_io_schedulers))
      n when is_integer(n) and n > 0 -> n
      _ -> nil
    end
  end

  @doc "Pure derivation of the default: leave 2 dirty-IO threads free, never below 1."
  @spec default_cap(pos_integer()) :: pos_integer()
  def default_cap(dirty_io_schedulers)
      when is_integer(dirty_io_schedulers) and dirty_io_schedulers > 3,
      do: dirty_io_schedulers - 2

  def default_cap(_dirty_io_schedulers), do: 1

  @doc """
  Wait for a scan permit, up to `timeout_ms`. `:ok` — held until `release/1` or the caller exits.
  `:timeout` — none came free in time (the caller proceeds without one). `:ok` immediately when
  the gate is disabled or not running (nothing held, `release/1` is then a harmless no-op).
  """
  @spec acquire(non_neg_integer(), GenServer.server()) :: :ok | :timeout
  def acquire(timeout_ms, gate \\ __MODULE__) do
    case cap() do
      cap when is_integer(cap) ->
        try do
          GenServer.call(gate, {:acquire, cap}, timeout_ms)
        catch
          :exit, {:noproc, _} ->
            :ok

          :exit, {:timeout, _} ->
            GenServer.cast(gate, {:withdraw, self()})
            :timeout
        end

      nil ->
        :ok
    end
  end

  @doc "Give the caller's permit back. A no-op if it holds none."
  @spec release(GenServer.server()) :: :ok
  def release(gate \\ __MODULE__), do: GenServer.cast(gate, {:release, self()})

  @doc """
  Run `fun` holding a permit. Waits at most `timeout_ms`; on timeout `fun` runs without one.
  """
  @spec with_permit(non_neg_integer(), (-> result), GenServer.server()) :: result
        when result: var
  def with_permit(timeout_ms, fun, gate \\ __MODULE__) do
    got = acquire(timeout_ms, gate)

    try do
      fun.()
    after
      if got == :ok, do: release(gate)
    end
  end

  @doc "Permits held and callers waiting. For tests and the dashboard."
  @spec stats(GenServer.server()) :: %{held: non_neg_integer(), waiting: non_neg_integer()}
  def stats(gate \\ __MODULE__), do: GenServer.call(gate, :stats)

  @impl true
  def init(_opts), do: {:ok, %{held: %{}, waiting: :queue.new()}}

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
    cap = cap()

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
