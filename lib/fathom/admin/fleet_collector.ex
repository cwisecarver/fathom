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
  @page_table :fathom_admin_migrations_page
  @default_page_ttl_ms 10_000
  @refill_lock_ms 30_000
  @refill_wait_ms 15_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "PubSub topic this node broadcasts `{:fleet, map}` on."
  @spec topic() :: String.t()
  def topic, do: @topic

  @doc """
  Stamp a roll-up with `:fetched_at_ms` (system time) so the admin UI can show how old it is
  (expert review 2026-10-15 #5). `MetricsCollector` already carries `at_ms`.
  """
  @spec stamp(map()) :: map()
  def stamp(overview), do: Map.put(overview, :fetched_at_ms, System.system_time(:millisecond))

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

  @doc """
  The migrations page's data (`Fleet.migrations/0` + `Fleet.oban_counts/0`), cached for
  `:admin_migrations_cache_ms` (default 10 s) in an ETS table this collector owns.

  Expert review 2026-10-12 #33: `AdminMigrationsLive` ran those directory-scale reads on its own 5 s
  timer per open tab, the O(viewers) shape expert review 2026-07-24 #13 removed from the overview.
  Inside the window a tab reuses the last result, so a node's cost is bounded by the TTL, not the
  tab count. Computed in the CALLER (the LiveView's async task), never in the collector, so a slow
  query can't block it. A failed compute raises and is not cached (the LiveView keeps its last-good
  data). Without the collector (dashboard gating off, tests) there is no table and it computes
  uncached.
  """
  @spec migrations_page() :: %{mig: map(), oban: list(), jobs: map(), lifecycle: map()}
  def migrations_page, do: migrations_page(&compute_page/0)

  @doc false
  # `compute` is a seam so a test can count how many callers actually compute.
  @spec migrations_page((-> term())) :: term()
  def migrations_page(compute) when is_function(compute, 0) do
    ttl = Application.get_env(:fathom, :admin_migrations_cache_ms, @default_page_ttl_ms)
    now = System.monotonic_time(:millisecond)

    case lookup_page() do
      {at, page} when now - at < ttl -> page
      stale -> refill_page(stale, compute)
    end
  end

  defp compute_page do
    oban = Fleet.oban_counts()

    %{
      mig: Fleet.migrations(),
      oban: oban,
      # Expert review 2026-10-15 #12/#18: the jobs + lifecycle panels ride this same single-flight
      # cache, so their Postgres reads are once per node per TTL, never per viewer.
      jobs: %{health: Fleet.oban_health(), failed: Fleet.failed_jobs()},
      lifecycle: Fleet.lifecycle(oban)
    }
  end

  # SINGLE-FLIGHT refill (expert review 2026-10-13 #28). At TTL expiry every open tab's async task
  # (and every caller) used to recompute at once, so the directory-scale reads behind this page ran
  # once per caller instead of once per node per TTL. One caller takes the ETS lock and computes;
  # the others serve the stale page, or, with none yet, wait briefly for the winner's. The lock has
  # its own expiry so a winner killed mid-compute cannot wedge the page, and a waiter that times out
  # computes for itself rather than failing.
  defp refill_page(stale, compute) do
    now = System.monotonic_time(:millisecond)

    case acquire_refill(now) do
      :acquired ->
        try do
          page = compute.()
          store_page({System.monotonic_time(:millisecond), page})
          page
        after
          release_refill()
        end

      :busy ->
        case stale do
          {_at, page} -> page
          nil -> await_page(compute, now + @refill_wait_ms)
        end

      :no_table ->
        # Collector not running (dashboard gating off, tests): compute uncached, as before.
        compute.()
    end
  end

  defp acquire_refill(now) do
    # `insert_new` is the atomic claim. A lock past its expiry belongs to a dead or wedged winner:
    # take it over (delete-then-insert_new, so two takers still resolve to exactly one winner).
    case :ets.insert_new(@page_table, {:refill, now + @refill_lock_ms}) do
      true ->
        :acquired

      false ->
        case :ets.lookup(@page_table, :refill) do
          [{:refill, expires}] when expires <= now ->
            :ets.delete_object(@page_table, {:refill, expires})

            if :ets.insert_new(@page_table, {:refill, now + @refill_lock_ms}),
              do: :acquired,
              else: :busy

          _ ->
            :busy
        end
    end
  rescue
    ArgumentError -> :no_table
  end

  defp release_refill do
    :ets.delete(@page_table, :refill)
  rescue
    ArgumentError -> :ok
  end

  # A cold start with no stale page: wait for the winner's, bounded, then compute for ourselves.
  defp await_page(compute, deadline) do
    case lookup_page() do
      {_at, page} ->
        page

      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          compute.()
        else
          Process.sleep(25)
          await_page(compute, deadline)
        end
    end
  end

  defp lookup_page do
    case :ets.lookup(@page_table, :page) do
      [{:page, entry}] -> entry
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp store_page(entry) do
    :ets.insert(@page_table, {:page, entry})
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@page_table, [:named_table, :public, :set])
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
    overview = stamp(overview)
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
