defmodule Fathom.Rebalancer.CommandPoller do
  @moduledoc """
  Executes handoff commands addressed to this node — the node-local half of the cross-node
  command channel. Every `:command_poll_ms` it reads `Commands.pending_for/1` for this
  node's `Fathom.Rebalancer.node_key/0` and runs each:

    * **warm** — `Fathom.Shard.WarmFollower.warm_now/1` pulls the shard into this node's
      warm cache ahead of a handoff. Best-effort: a shard with no stored object (never
      flushed) or a transient pull failure still marks `done`, because the target's
      cold-open revalidates/pulls correctly anyway — warm only trims the blip.
    * **drain** — `Fathom.Shards.drain/2` flushes + releases the lease so another node can
      acquire it. `:ok` ⇒ `done`; a `{:error, _}` (e.g. `:busy` — connections didn't drain
      in time) ⇒ `failed`, and the orchestrator decides whether to retry. Skipped
      (`cancelled`) if the shard has no active pin when the poller reaches it — the handoff
      reverted and the source is serving again (finding #7).

  Gated by `:command_poller` (default off). Only this node's poller matches a command
  addressed to this node's key, so there's no cross-node contention.

  ## Concurrent execution + poll-tick decoupling (findings #8, #11)

  Commands in a poll batch run **concurrently** off the poller via a `Task.Supervisor`
  (`async_stream_nolink`, bounded by `:command_poll_concurrency`), so a slow `drain` (up to
  `Shards.drain`'s safety net) never head-of-line-blocks the `warm` commands the node needs
  when it's simultaneously a handoff target. The timer path **dispatches each batch as a
  detached child task and reschedules immediately** (finding #11), so a slow batch doesn't gate
  the *next* tick either — a `warm` arriving just after a drain batch begins is picked up on the
  next tick rather than waiting the whole drain out. In-flight command ids are tracked in state
  so an overlapping tick can't re-select a command still running (it stays `pending` in Postgres
  until its task calls `complete/3`). `nolink` keeps a crashing command from taking the poller
  down, and each task has a **finite** timeout (`:command_task_timeout_ms`, default ordered above
  the drain worst-case) so `on_timeout: :kill_task` actually bounds a wedged command rather than
  the batch blocking forever. `poll_now/0` still consumes one batch synchronously (so tests
  observe completion) and respects the same in-flight set. The handoff-side timeout budget is
  ordered so a legitimately-slow drain isn't mislabeled a timeout (`HandoffJob`'s drain await ≥
  `command_drain_ms` + shutdown grace).
  """
  use GenServer

  require Logger

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{Commands, Overrides}
  alias Fathom.Shard.WarmFollower
  alias Fathom.Shards

  @default_poll_ms 1_000
  @default_drain_ms 10_000
  @default_concurrency 8
  @task_supervisor Fathom.Rebalancer.TaskSupervisor
  # A per-command task timeout must exceed the poller's worst-case drain (`command_drain_ms`
  # + `Shards.drain`'s coordinator-shutdown safety net) so a legitimately-slow drain isn't
  # killed mid-flight; only a genuinely wedged command hits it (finding #11).
  @task_shutdown_grace_ms 35_000

  @doc "Whether this node acts on handoff commands (`:command_poller`, default off)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :command_poller, false) == true

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Runs one poll synchronously and returns how many commands were executed (tests)."
  @spec poll_now() :: non_neg_integer()
  def poll_now, do: GenServer.call(__MODULE__, :poll_now, 120_000)

  @impl true
  def init(_opts) do
    schedule()
    {:ok, %{in_flight: MapSet.new()}}
  end

  # Timer path: dispatch this tick's fresh commands as a detached batch and reschedule
  # IMMEDIATELY (finding #11), so a slow batch never gates the next tick's warm pickup.
  @impl true
  def handle_info(:poll, state) do
    state = dispatch(state)
    schedule()
    {:noreply, state}
  end

  # A detached batch clears its ids from the in-flight set when it finishes (or dies).
  @impl true
  def handle_info({:batch_done, ids}, state) do
    {:noreply, %{state | in_flight: MapSet.difference(state.in_flight, ids)}}
  end

  # Test-only: run one batch to completion synchronously so a test observes results
  # deterministically. Respects the same in-flight set so it can't double-run a command a
  # timer tick already dispatched.
  @impl true
  def handle_call(:poll_now, _from, state), do: {:reply, do_poll_sync(state), state}

  # Synchronous consume for poll_now (tests). Wraps fresh_pending's `Commands.pending_for` so a
  # Postgres blip is a no-op tick (like dispatch/1), not a crashed caller (finding #13).
  defp do_poll_sync(state) do
    state |> fresh_pending() |> run_batch()
  rescue
    e ->
      Logger.warning("command poller tick failed: #{Exception.message(e)}")
      0
  catch
    :exit, reason ->
      Logger.warning("command poller tick exited: #{inspect(reason)}")
      0
  end

  # Dispatch the fresh pending commands as ONE detached child task (which internally bounds
  # concurrency), returning the state with those ids marked in-flight. The child clears them via
  # {:batch_done} even if the batch raises (try/after), so in-flight never leaks.
  defp dispatch(state) do
    case fresh_pending(state) do
      [] ->
        state

      fresh ->
        poller = self()
        ids = MapSet.new(fresh, & &1.id)

        Task.Supervisor.start_child(@task_supervisor, fn ->
          try do
            run_batch(fresh)
          after
            send(poller, {:batch_done, ids})
          end
        end)

        %{state | in_flight: MapSet.union(state.in_flight, ids)}
    end
  rescue
    # A Postgres blip (e.g. pending_for) just means we retry next tick — never crash.
    e ->
      Logger.warning("command poller tick failed: #{Exception.message(e)}")
      state
  catch
    # Pool / :noproc / shutdown surface as an exit, which `rescue` misses (finding #13);
    # catch it too so a Postgres blip never takes the node down (like Directory.Recorder).
    :exit, reason ->
      Logger.warning("command poller tick exited: #{inspect(reason)}")
      state
  end

  # Pending commands for this node, minus any a prior tick already dispatched and hasn't
  # cleared yet (still `pending` in Postgres until its task completes) — the anti-re-select gate.
  defp fresh_pending(state) do
    Rebalancer.node_key()
    |> Commands.pending_for()
    |> Enum.reject(&MapSet.member?(state.in_flight, &1.id))
  end

  # Consume a batch concurrently off the caller; returns how many executed. `nolink` +
  # a finite per-task timeout keep a crashing/wedged command from taking the batch down.
  defp run_batch([]), do: 0

  defp run_batch(commands) do
    @task_supervisor
    |> Task.Supervisor.async_stream_nolink(commands, &execute/1,
      max_concurrency: concurrency(),
      timeout: task_timeout(),
      on_timeout: :kill_task
    )
    |> Enum.reduce(0, fn
      {:ok, _}, acc ->
        acc + 1

      {:exit, reason}, acc ->
        Logger.warning("command poller: a command crashed: #{inspect(reason)}")
        acc
    end)
  rescue
    e ->
      Logger.warning("command poller tick failed: #{Exception.message(e)}")
      0
  catch
    :exit, reason ->
      Logger.warning("command poller tick exited: #{inspect(reason)}")
      0
  end

  # Warm is best-effort: correctness comes from the target's cold-open, so even a skip is
  # `done` (the handoff proceeds; warm only saved a body transfer).
  defp execute(%{command: "warm", shard_id: id} = cmd) do
    detail =
      case WarmFollower.warm_now(id) do
        :ok -> "warmed"
        {:error, reason} -> "warm skipped (#{inspect(reason)})"
      end

    complete(cmd, "done", detail)
  end

  # Drain releases the lease. Failure (busy / drain_failed) is terminal-for-this-command;
  # the orchestrator re-issues if it still wants the move.
  #
  # Defense-in-depth for #7: re-check the pin immediately before draining. If the shard has
  # no active pin (never pinned, or reverted → failed_at set), the handoff was abandoned and
  # the source is serving again — draining now would strand it. Skip (cancel) instead. This
  # covers the race where the poller picks up the drain between issue and cancel_pending.
  defp execute(%{command: "drain", shard_id: id} = cmd) do
    case Overrides.for_shard(id) do
      %{failed_at: nil} ->
        case Shards.drain(id, drain_ms()) do
          :ok ->
            complete(cmd, "done", "drained")

          {:error, reason} ->
            complete(cmd, "failed", "drain failed (#{inspect(reason)})")
        end

      _gone_or_reverted ->
        complete(cmd, "cancelled", "pin reverted; drain skipped")
    end
  end

  defp execute(%{command: other} = cmd) do
    complete(cmd, "failed", "unknown command #{inspect(other)}")
  end

  # Complete a command and emit its outcome (rebalancer telemetry): drain :failed is the
  # thrash signal at the executor (a wedged/busy source), :cancelled is the abandoned-pin
  # skip (#7). Emit AFTER the durable completion (review 2026-07-09 #8) — for consistency with
  # the other emit sites and so a buggy handler can never sit between the executor and the DB
  # write. (`:telemetry.execute` already isolates a raising/exiting handler from the caller, so
  # the emit can't abort the completion in EITHER order — but emit-after is the correct shape.)
  defp complete(cmd, status, detail) do
    result = Commands.complete(cmd, status, detail)

    :telemetry.execute([:fathom, :rebalancer, :command, :stop], %{count: 1}, %{
      command: cmd.command,
      outcome: outcome_atom(status)
    })

    result
  end

  defp outcome_atom("done"), do: :done
  defp outcome_atom("failed"), do: :failed
  defp outcome_atom("cancelled"), do: :cancelled
  defp outcome_atom(_), do: :other

  defp schedule, do: Process.send_after(self(), :poll, poll_ms())
  defp poll_ms, do: Application.get_env(:fathom, :command_poll_ms, @default_poll_ms)
  defp drain_ms, do: Application.get_env(:fathom, :command_drain_ms, @default_drain_ms)

  # Ordered above the drain worst-case (drain budget + shutdown grace) so a slow-but-succeeding
  # drain isn't killed; an explicit :command_task_timeout_ms wins (operator owns the ordering).
  @doc false
  def task_timeout do
    Application.get_env(:fathom, :command_task_timeout_ms) || drain_ms() + @task_shutdown_grace_ms
  end

  defp concurrency,
    do: Application.get_env(:fathom, :command_poll_concurrency, @default_concurrency)
end
