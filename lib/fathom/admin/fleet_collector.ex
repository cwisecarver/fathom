defmodule Fathom.Admin.FleetCollector do
  @moduledoc """
  One process per node that polls the fleet roll-up (`Fathom.Admin.Fleet.overview/0`) on a timer
  and fans the result out over `Phoenix.PubSub`, so the cost is O(1) queries per interval per node
  instead of O(connected viewers).

  Expert review 2026-07-24 #13: every `AdminOverviewLive` ran its own `start_async(:fleet, …)` on
  its own 5 s timer, so the directory-scale reads behind `overview/0` multiplied by the number of
  open dashboard tabs. At 1M shards that is a full group-by over `shards` plus a bounded `oban_jobs`
  group-by every 5 s **per tab** — and it lands hardest during an incident, exactly when several
  operators have the dashboard open and the control plane is least able to absorb it. The per-node
  realtime metrics already solved this shape (`Fathom.Admin.MetricsCollector`); the fleet roll-up
  did not get the same treatment.

  Gated on `Fathom.Admin.enabled?/0` in the supervision tree, so a node without the operator
  dashboard configured runs nothing at all.

  A LiveView calls `snapshot/0` for its initial paint, subscribes to `topic/0` for
  `{:fleet, map}` updates, and calls `watch/0`.

  ## It polls only while someone is watching (expert review 2026-10-01 perf #20)

  `Fleet.overview/0` is a GROUP BY over the whole `shards` table, so a collector polling every
  15 s on every node with nobody looking was N full scans of the hottest control-plane table per
  interval, forever — ~200 MB/s of sequential reads at 1M shards across 20 nodes, competing for
  shared buffers with the Recorder. A viewer registers with `watch/0`; the collector monitors it,
  polls while at least one is alive, and stops when the last one goes. The first watcher after a
  quiet period gets a poll immediately rather than waiting out an interval.
  """
  use GenServer

  require Logger

  alias Fathom.Admin.Fleet

  # The underlying data — laggard convergence, the node roster, pin state — moves on minute
  # timescales, so a 5 s cadence was paying directory-scale reads for numbers that had not changed.
  @default_refresh_ms 15_000
  @topic "admin:fleet"

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "PubSub topic this node broadcasts `{:fleet, map}` on."
  @spec topic() :: String.t()
  def topic, do: @topic

  @doc """
  Register `pid` (default: the caller) as a viewer. The collector polls only while at least one
  viewer is alive. A no-op when the collector is not running.
  """
  @spec watch(pid()) :: :ok
  def watch(pid \\ self()), do: GenServer.cast(__MODULE__, {:watch, pid})

  @doc """
  The latest fleet roll-up, for a LiveView's initial paint.

  Returns `nil` if the collector isn't running (dashboard disabled) or hasn't completed its first
  poll — callers fall back to rendering an empty state, never to running the queries themselves.
  """
  @spec snapshot() :: map() | nil
  def snapshot do
    GenServer.call(__MODULE__, :snapshot, 5_000)
  catch
    :exit, _ -> nil
  end

  @impl true
  def init(_opts) do
    # No poll at boot: nobody is watching yet. The first `watch/0` starts the loop.
    {:ok, %{current: nil, task: nil, timer: nil, watchers: %{}}}
  end

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, state.current, state}

  @impl true
  def handle_cast({:watch, pid}, state) do
    ref = Process.monitor(pid)
    state = %{state | watchers: Map.put(state.watchers, ref, pid)}

    # Idle (no poll running, none scheduled): this viewer ended a quiet period, so poll NOW.
    if state.task == nil and state.timer == nil, do: send(self(), :tick)
    {:noreply, state}
  end

  @impl true
  # Overlap guard: a poll slower than the interval must not stack a second one. The next tick
  # retries — a skipped refresh just means the dashboard shows the previous value a little longer.
  def handle_info(:tick, %{task: task} = state) when not is_nil(task) do
    {:noreply, schedule(%{state | timer: nil})}
  end

  # Nobody watching: let the loop lapse. The next `watch/0` restarts it.
  def handle_info(:tick, %{watchers: watchers} = state) when map_size(watchers) == 0 do
    {:noreply, %{state | timer: nil}}
  end

  def handle_info(:tick, state) do
    task =
      Task.Supervisor.async_nolink(Fathom.Admin.TaskSupervisor, fn ->
        Fleet.overview()
      end)

    {:noreply, %{state | task: task, timer: nil}}
  end

  def handle_info({ref, overview}, %{task: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Phoenix.PubSub.broadcast(Fathom.PubSub, @topic, {:fleet, overview})
    {:noreply, schedule(%{state | current: overview, task: nil})}
  end

  # The poll failed (a Postgres blip). Keep the last-good value — a stale roll-up is strictly
  # better for an operator than a blank dashboard — and try again next tick.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %{ref: ref}} = state) do
    Logger.warning("admin fleet roll-up failed: #{inspect(reason)}")
    {:noreply, schedule(%{state | task: nil})}
  end

  # A viewer went away. Placed after the task's own DOWN clause above, which matches first.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{watchers: watchers} = state)
      when is_map_key(watchers, ref) do
    {:noreply, %{state | watchers: Map.delete(watchers, ref)}}
  end

  def handle_info({ref, _result}, state) when is_reference(ref), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) when is_reference(ref),
    do: {:noreply, state}

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule(state) do
    %{state | timer: Process.send_after(self(), :tick, refresh_ms())}
  end

  defp refresh_ms do
    case Application.get_env(:fathom, :admin_fleet_refresh_ms, @default_refresh_ms) do
      ms when is_integer(ms) and ms > 0 -> ms
      _ -> @default_refresh_ms
    end
  end
end
