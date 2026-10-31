defmodule Fathom.Shard.BackgroundScanGate do
  @moduledoc """
  Node-wide permit for the NON-flush whole-shard SQLite scans: the migration replay
  (`Fathom.Migrator.Copy.migrate_chain/4`), `verify_migrated`, the restore drill's `integrity_check`
  and row counts, and the tenant export's `integrity_check` (expert review 2026-10-29 #3).

  Every exqlite NIF is a dirty-IO job, and a `PRAGMA integrity_check` / `INSERT ... SELECT` /
  `CREATE INDEX` holds one dirty-IO thread for the whole scan. The node has `+SDio` (10) of them for
  ALL SQLite work and file I/O, and `Fathom.Shard.ScanGate` bounds only the FLUSH scans. Ten
  concurrent migrations (the `:migrations` queue is 10 wide) or eight drill workers could each park a
  thread and starve every tenant statement on the node.

  A SEPARATE gate from `ScanGate`, deliberately. Sharing the flush permit count would let a rollout
  starve flushes (RPO) and a flush wave starve a rollout; neither the flush permit count nor any
  flush code is touched. The two gates are independent GenServers: a saturated background gate never
  blocks a flush `acquire`, and vice versa.

  ## Default cap

  `max(1, div(SDio, 4))`: 2 on the default `+SDio 10`. Reasoning: flush scans may hold up to
  `SDio - 2` (8) when a wave is phase-aligned, but the steady-state flush occupancy on the 1024
  small-shard rig is a fraction of ONE thread (7 flushes/s/node at ~50 ms, about 0.35 thread;
  expert review 2026-10-26 #13), so the background scans are the only thing that can sit on the pool
  for minutes. Two permits take a fifth of the threads and leave 8 for tenant statements in the
  steady state. The two gates are independent, so a maximal flush wave (8) plus both background
  permits (2) is the whole pool on the default `+SDio 10`: that needs a phase-aligned wave landing
  on a rollout, which the jittered flush timers make rare, and coupling the gates to rule it out
  would let a rollout starve flushes. A quarter of the pool is also the audit's sizing. Pin it with
  `:background_scan_permits` (`BACKGROUND_SCAN_PERMITS`; `0`/`false` disables the gate).

  ## Waiting, and what a timeout means

  Unlike `ScanGate` a timed-out caller does NOT scan anyway: the whole point is that these scans
  yield to tenants, and a durability flush (which must never fail) is not what is waiting.

    * `with_permit/3` waits at most `:background_scan_wait_ms` (default 120 s) and returns
      `{:error, :background_scan_busy}`. The migration job treats that as TRANSIENT
      (`ShardMigrationJob.transient?/1`): it snoozes without burning an attempt, the same
      back-pressure shape as a held lease or a busy shard. The wait is long on purpose: the job has
      already pulled the object, so giving up early throws away an S3 GET of the whole shard.
    * `queued/1` waits with no deadline of its own, for callers that already run under one (the
      restore drill's `Task.async_stream` per-shard `:kill_task` timeout). A killed waiter is removed
      from the queue by its monitor.

  A GenServer with monitors, like `ScanGate`: the permit is tied to the holder's monitor so a killed
  Oban job or Task cannot leak it. NOT re-entrant: a process that holds a permit must not acquire
  another (it would wait on itself once the cap is reached).

  Fails open when the gate is not running (a script or test without the application) or is disabled:
  the scan simply runs.
  """
  use GenServer

  @default_wait_ms 120_000
  @wait_key {__MODULE__, :wait_ms}

  @doc false
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc """
  The permit count: `:background_scan_permits` if set to a positive integer, `nil` if set to
  `0`/`false` (gate off), else `max(1, div(dirty_io_schedulers, 4))`.
  """
  @spec cap() :: pos_integer() | nil
  def cap do
    case Application.get_env(:fathom, :background_scan_permits, :default) do
      :default -> default_cap(:erlang.system_info(:dirty_io_schedulers))
      n when is_integer(n) and n > 0 -> n
      _ -> nil
    end
  end

  @doc "Pure derivation of the default: a quarter of the dirty-IO threads, never below 1."
  @spec default_cap(pos_integer()) :: pos_integer()
  def default_cap(dirty_io_schedulers)
      when is_integer(dirty_io_schedulers) and dirty_io_schedulers > 3,
      do: div(dirty_io_schedulers, 4)

  def default_cap(_dirty_io_schedulers), do: 1

  @doc """
  How long `with_permit/3` waits by default: the calling process's `with_wait_ms/2` override if it is
  inside one, else `:background_scan_wait_ms`.
  """
  @spec wait_ms() :: non_neg_integer()
  def wait_ms,
    do:
      Process.get(@wait_key) ||
        Application.get_env(:fathom, :background_scan_wait_ms, @default_wait_ms)

  @doc """
  Run `fun` with every `with_permit/3` the CALLING process makes inside it waiting at most `ms`.

  For a scan that runs on a TENANT's request path (the `:inline` migrate-on-touch checkout), where
  the Oban jobs' two-minute wait would be an outage: it needs a short wait and a fallback, not a
  queue. A process dictionary key rather than an argument because the permit is taken four calls
  below the entry point (`ShardMigration.run` -> `forward` -> `Copy.migrate_chain`), all in the
  caller's process, and widening each of those signatures for one caller is worse.
  """
  @spec with_wait_ms(non_neg_integer(), (-> result)) :: result when result: var
  def with_wait_ms(ms, fun) when is_integer(ms) and ms >= 0 do
    prev = Process.put(@wait_key, ms)

    try do
      fun.()
    after
      if prev == nil, do: Process.delete(@wait_key), else: Process.put(@wait_key, prev)
    end
  end

  @doc """
  Run `fun` holding a permit, waiting at most `wait_ms` for one. Returns `fun`'s result, or
  `{:error, :background_scan_busy}` (fun NOT run) if none came free in time. `fun` runs unpermitted
  if the gate is disabled or not running.
  """
  @spec with_permit((-> result), GenServer.server(), non_neg_integer()) ::
          result | {:error, :background_scan_busy}
        when result: var
  def with_permit(fun, gate \\ __MODULE__, wait_ms \\ wait_ms()) do
    case acquire(wait_ms, gate) do
      :ok -> run_holding(fun, gate)
      :timeout -> {:error, :background_scan_busy}
    end
  end

  @doc """
  `with_permit/3` with no deadline of its own: waits until a permit is free. Only for a caller whose
  process is killed by an enclosing deadline (the waiter's monitor then drops it from the queue).
  """
  @spec queued((-> result), GenServer.server()) :: result when result: var
  def queued(fun, gate \\ __MODULE__) do
    :ok = acquire(:infinity, gate)
    run_holding(fun, gate)
  end

  defp run_holding(fun, gate) do
    fun.()
  after
    release(gate)
  end

  @doc """
  Wait for a permit, up to `timeout_ms`. `:ok` — held until `release/1` or the caller exits.
  `:timeout` — none came free in time and nothing is held. `:ok` immediately when the gate is
  disabled or not running (`release/1` is then a harmless no-op).
  """
  @spec acquire(timeout(), GenServer.server()) :: :ok | :timeout
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
