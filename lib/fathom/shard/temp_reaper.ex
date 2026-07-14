defmodule Fathom.Shard.TempReaper do
  @moduledoc """
  Amortized janitor for orphaned shard temp files.

  Externally-killed pulls/snapshots (`Task.shutdown` brutal_kill, pull timeouts, a hard
  coordinator crash) strand uniquely-suffixed `.dl.*` / `.snap.*` / `.tmp.*` temps next
  to the shard's `.db` in the shard data dir — an unbounded, shard-sized disk leak
  (expert review round-2 #27) that eats the density budget, and one no fixed-suffix
  sweeper can enumerate without listing the directory.

  This cleanup used to run on the cold-open hot path (`Fathom.Shard.handle_continue/2`
  called `Storage.reap_stale_temps/2`), but `Path.wildcard` can't prefix-optimize a
  pattern whose filename component holds a `*`, so it full-`readdir`d the FLEET-sized
  shard data dir on EVERY open — tens-to-hundreds of ms at 30k–105k shards, re-paid on
  every idle re-open (expert review 2026-07-14 #2). So the coordinator now clears only
  its own DETERMINISTIC `.pull` family O(1) by direct name, and this process does the
  one directory scan for the uniquely-suffixed orphans ONCE every
  `:temp_reaper_interval_ms` (default #{div(300_000, 60_000)} minutes), amortized across
  all opens. Coverage is strictly better than the old per-open reap: a shard that
  strands a temp and never re-opens is still swept within the interval.

  Age-gated to `2 * Fathom.Shard`'s pull timeout so a concurrent pull/snapshot's live
  temp is never touched. Gated `:temp_reaper` (default on; off in test, where tests
  drive `sweep/0` synchronously).
  """
  use GenServer

  require Logger

  alias Fathom.Shard
  alias Fathom.Shard.Storage

  @default_interval_ms 300_000
  # Match the coordinator's per-open age gate (2 * Fathom.Shard @pull_timeout of 60_000).
  @stale_after_ms 120_000

  @doc "Start the reaper (registered under the module name)."
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Run one sweep synchronously and return the number of orphaned temps reaped. The test
  seam (the periodic timer is gated off in test); safe to call at any time.
  """
  @spec sweep() :: non_neg_integer()
  def sweep, do: GenServer.call(__MODULE__, :sweep)

  @impl true
  def init(_opts) do
    # Sweep once at boot (a previous incarnation's crash may have stranded temps),
    # then on the timer. The scan runs off any request path, so a fleet-sized dir is fine.
    {:ok, %{}, {:continue, :sweep}}
  end

  @impl true
  def handle_continue(:sweep, state) do
    do_sweep()
    {:noreply, schedule(state)}
  end

  @impl true
  def handle_info(:sweep, state) do
    do_sweep()
    {:noreply, schedule(state)}
  end

  @impl true
  def handle_call(:sweep, _from, state), do: {:reply, do_sweep(), state}

  # One directory scan of the shard data dir for stale, uniquely-suffixed orphan temps.
  defp do_sweep do
    reaped = Storage.reap_stale_temps(Path.join(Shard.data_dir(), "*"), @stale_after_ms)
    if reaped > 0, do: Logger.info("shard temp reaper: reaped #{reaped} orphaned temp(s)")
    reaped
  end

  defp schedule(state) do
    Process.send_after(self(), :sweep, interval_ms())
    state
  end

  defp interval_ms,
    do: Application.get_env(:fathom, :temp_reaper_interval_ms, @default_interval_ms)
end
