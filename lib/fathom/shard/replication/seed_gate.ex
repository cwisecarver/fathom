defmodule Fathom.Shard.Replication.SeedGate do
  @moduledoc """
  Node-wide cap on concurrent replication SEEDS (expert review 2026-10-01 #6).

  ## Why

  A seed streams a tenant's whole `.db` + `-wal` to one follower. Since reseed-on-ownership-change
  (73b4d08) every idle drop followed by a write seeds every follower, and a node failover reopens
  ~1/N of the fleet at once — so seeds arrive in bursts, one task per shard × follower, with
  nothing bounding how many run. Each seed shares its follower's single link with every other
  shard's pushes (a 4 MiB chunk holds a 1 Gbit link ~34 ms), so an unbounded burst is head-of-line
  blocking for every tenant on the node and `cap × chunk` of resident memory.

  `acquire/1` blocks the seed task until a slot is free; the commit path never waits on it (seeds
  run in their own task, and the shard's pushes to that follower are already refused until its seed
  lands). A waiting seed is simply a later seed.

  ## Why a GenServer with monitors, unlike `FlushGate`

  `FlushGate` is lock-free ETS because it is asked on every flush; a seed is rare and long, so one
  call per seed costs nothing. In exchange the slot is tied to a MONITOR: `Session` kills a wedged
  seed task with `Process.exit(pid, :kill)` (`expire_seeds/1`), which runs no `after` block, and a
  release that depended on the task's own cleanup would leak the slot. The `:DOWN` frees it.

  `:replication_seed_max_concurrency` sets the cap (default 8); `0` or `nil` disables the gate.
  """
  use GenServer

  @default_cap 8

  @doc false
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc """
  Wait for a seed slot, up to `timeout_ms`. `:ok` — the slot is held until `release/1` or until the
  calling process exits. `{:error, :seed_gate_timeout}` — no slot came free in time.
  `:ok` immediately when the gate is disabled or not running.
  """
  @spec acquire(non_neg_integer(), GenServer.server()) :: :ok | {:error, :seed_gate_timeout}
  def acquire(timeout_ms, gate \\ __MODULE__) do
    case cap() do
      cap when is_integer(cap) and cap > 0 ->
        try do
          GenServer.call(gate, {:acquire, cap}, timeout_ms)
        catch
          :exit, {:noproc, _} ->
            :ok

          :exit, {:timeout, _} ->
            # Withdraw, so a slot granted after we stopped waiting is not held by a caller that
            # has gone on to fail. Our own exit would free it too; this frees it now.
            GenServer.cast(gate, {:withdraw, self()})
            {:error, :seed_gate_timeout}
        end

      _ ->
        :ok
    end
  end

  @doc "Give the caller's slot back. A no-op if it holds none."
  @spec release(GenServer.server()) :: :ok
  def release(gate \\ __MODULE__) do
    GenServer.cast(gate, {:release, self()})
  end

  @doc "Slots held and seeds waiting. For tests and the dashboard."
  @spec stats(GenServer.server()) :: %{held: non_neg_integer(), waiting: non_neg_integer()}
  def stats(gate \\ __MODULE__), do: GenServer.call(gate, :stats)

  @doc false
  def cap,
    do: Application.get_env(:fathom, :replication_seed_max_concurrency, @default_cap)

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

  def handle_cast({:withdraw, pid}, state) do
    {:noreply, state |> unwait(pid) |> free(pid) |> grant()}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _}, state) do
    {:noreply, state |> unwait(pid) |> free(pid) |> grant()}
  end

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

  # Hand freed slots to waiters, oldest first. Read the cap now: it may have been raised.
  defp grant(state) do
    cap = cap()

    if (not is_integer(cap) or cap <= 0 or map_size(state.held) < cap) and
         not :queue.is_empty(state.waiting) do
      {{:value, {{pid, _} = from, ref}}, rest} = :queue.out(state.waiting)
      Process.demonitor(ref, [:flush])
      GenServer.reply(from, :ok)
      grant(hold(%{state | waiting: rest}, pid))
    else
      state
    end
  end
end
