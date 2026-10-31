defmodule Fathom.Host.SystemMonitor do
  @moduledoc """
  Turns the BEAM's `:erlang.system_monitor/2` stall reports into telemetry (expert review
  2026-10-29 #17d), so an operator can tell a scheduler stall from a disk stall.

    * `[:fathom, :node, :long_schedule]` (`duration_ms`, metadata `kind: :process | :port`): a
      process or port held a normal scheduler for longer than `long_schedule_ms` (20 ms) without
      yielding. That is a scheduler-wide stall, the kind that parks every tenant statement queued on
      that run queue.
    * `[:fathom, :node, :long_gc]` (`duration_ms`): one garbage collection took longer than
      `long_gc_ms` (50 ms).

  **`large_heap` is deliberately not monitored.** It reports a process crossing a heap size, and
  there is no defensible threshold: a shard coordinator or a replication follower legitimately holds
  a large state, so any cut that catches a runaway also fires continuously on healthy processes.

  **Does not cover dirty NIFs.** `long_schedule` watches the normal schedulers. A tenant statement
  blocked in an exqlite dirty-IO NIF (an fsync waiting behind writeback) does not appear here; that
  is what `fathom.node.run_queue.dirty_io`, `fathom.node.io_pressure.*` and the optional fsync probe
  are for.

  ## Ownership

  `:erlang.system_monitor/2` is process-global: one monitor per VM, and setting a second silently
  replaces the first. So this never takes over. If something already owns the monitor (an operator's
  remote shell running `recon`, an APM agent), `init/1` logs a warning and returns `:ignore`, and the
  counters stay at zero. `SYSTEM_MONITOR=false` leaves the monitor free for that tool. A grep of
  `lib/` and `deps/` found no other owner at the time of writing.
  """
  use GenServer

  require Logger

  @long_schedule_ms 20
  @long_gc_ms 50

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @impl true
  def init(opts) do
    long_schedule = Keyword.get(opts, :long_schedule_ms, @long_schedule_ms)
    long_gc = Keyword.get(opts, :long_gc_ms, @long_gc_ms)

    case :erlang.system_monitor() do
      :undefined ->
        _ = :erlang.system_monitor(self(), long_schedule: long_schedule, long_gc: long_gc)
        {:ok, nil}

      {owner, owner_opts} ->
        Logger.warning(
          "system monitor: :erlang.system_monitor is already owned by #{inspect(owner)} " <>
            "(#{inspect(owner_opts)}); not replacing it, so fathom.node.long_schedule and " <>
            "fathom.node.long_gc will stay at zero (SYSTEM_MONITOR=false silences this)"
        )

        :ignore
    end
  end

  @impl true
  def handle_info({:monitor, subject, :long_schedule, info}, state) do
    :telemetry.execute(
      [:fathom, :node, :long_schedule],
      %{duration_ms: Keyword.get(info, :timeout, 0)},
      %{kind: if(is_port(subject), do: :port, else: :process)}
    )

    {:noreply, state}
  end

  def handle_info({:monitor, _pid, :long_gc, info}, state) do
    :telemetry.execute(
      [:fathom, :node, :long_gc],
      %{duration_ms: Keyword.get(info, :timeout, 0)},
      %{}
    )

    {:noreply, state}
  end

  # Any other monitor report (none is requested) or stray message is dropped, never crashed on.
  def handle_info(_other, state), do: {:noreply, state}
end
