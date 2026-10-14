defmodule Fathom.Shard.Replication.SeedPin do
  @moduledoc """
  One pinned read snapshot shared by the seed tasks of one `Session.start_seeds/3` batch (perf
  review 2026-10-14 #12(a)).

  A reopened shard re-seeds EVERY follower (ownership change), and `start_seeds` runs one task per
  follower. Each task used to open its own `:ro` `Connection` and pin its own snapshot (~361 us per
  open plus a read transaction); N followers meant N identical snapshots of the same file. The pin's
  only job is to stop a PASSIVE checkpoint backfilling pages past the reader's mark while the `.db`
  streams (see the note above `Session.do_seed/6`), and one reader does that for any number of
  streams.

  ## The invariant each task still relies on

  `attach/1` returns only after the snapshot is held, and the task reads the WAL header AFTER it
  returns — "snapshot first, THEN `Wal.read`", the ordering `do_seed/6` argues is the whole point.
  A task that attaches later than its siblings sees a mark `M` no older than the shared one and an
  extent `E >= M`, so every page in the `.db` it streams comes from a frame inside the prefix it
  ships, exactly as with a private pin. The pin is held until the LAST attached task detaches.

  ## Lifetime

  Reference-counted by attached task, not by batch: the connection is closed when the attached set
  empties (a task that attaches afterwards opens a fresh snapshot, which is also correct). The
  process itself ends once every task registered through `expect/2` has finished, or when the
  Session dies — it monitors both, so a `:kill`ed task (`expire_seeds/1`, the Session's watcher)
  releases its claim without running any `after` block. A pin opened by a batch can be held while a
  gated sibling waits for a `SeedGate` slot only if that sibling attached first; `attach/1` is called
  AFTER the gate, so a seed waiting in line never holds a snapshot.

  Emits `[:fathom, :replication, :seed_pin, :open]` per snapshot opened: the count the regression
  test pins (4 followers: 4 opens before, 1 after).
  """
  use GenServer

  alias Fathom.Shard.Connection

  @doc "Start a pin for `db_path`, owned by (monitoring) `session`. Not linked."
  @spec start(pid(), Path.t()) :: GenServer.on_start()
  def start(session, db_path), do: GenServer.start(__MODULE__, {session, db_path})

  @doc "Register the batch's task pids; the pin process ends when they have all finished."
  @spec expect(GenServer.server(), [pid()]) :: :ok
  def expect(pin, pids), do: GenServer.cast(pin, {:expect, pids})

  @doc """
  Hold the snapshot for the calling process. `:ok` once it is pinned, `{:error, reason}` if the
  `.db` is missing or the snapshot could not be taken (the seed must then fail, never stream
  unpinned).
  """
  @spec attach(GenServer.server()) :: :ok | {:error, term()}
  def attach(pin), do: GenServer.call(pin, {:attach, self()}, 30_000)

  @doc "Release the calling process's claim."
  @spec detach(GenServer.server()) :: :ok
  def detach(pin), do: GenServer.call(pin, {:detach, self()}, 30_000)

  @impl true
  def init({session, db_path}) do
    ref = Process.monitor(session)

    {:ok,
     %{
       db_path: db_path,
       session_ref: ref,
       conn: nil,
       attached: MapSet.new(),
       live: MapSet.new(),
       mons: %{},
       expected?: false
     }}
  end

  @impl true
  def handle_cast({:expect, pids}, state) do
    state =
      Enum.reduce(pids, %{state | expected?: true}, fn pid, acc ->
        ref = Process.monitor(pid)
        %{acc | live: MapSet.put(acc.live, pid), mons: Map.put(acc.mons, ref, pid)}
      end)

    maybe_stop(state)
  end

  @impl true
  def handle_call({:attach, pid}, _from, state) do
    case ensure_pinned(state) do
      {:ok, state} ->
        {:reply, :ok, %{state | attached: MapSet.put(state.attached, pid)}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:detach, pid}, _from, state) do
    state = state |> release(pid) |> finish(pid)
    if done?(state), do: {:stop, :normal, :ok, close(state)}, else: {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, %{session_ref: ref} = state) do
    {:stop, :normal, close(state)}
  end

  def handle_info({:DOWN, ref, :process, pid, _}, state) do
    case Map.pop(state.mons, ref) do
      {nil, _} ->
        {:noreply, state}

      {_pid, mons} ->
        maybe_stop(state |> Map.put(:mons, mons) |> release(pid) |> finish(pid))
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: close(state)

  defp maybe_stop(state) do
    if done?(state), do: {:stop, :normal, close(state)}, else: {:noreply, state}
  end

  defp done?(state), do: state.expected? and MapSet.size(state.live) == 0

  defp release(state, pid) do
    attached = MapSet.delete(state.attached, pid)
    state = %{state | attached: attached}
    if MapSet.size(attached) == 0, do: close(state), else: state
  end

  defp finish(state, pid), do: %{state | live: MapSet.delete(state.live, pid)}

  defp ensure_pinned(%{conn: nil} = state) do
    # `file_size/1` first: a `:ro` `Connection.open/2` materializes an ABSENT file, and a seed of a
    # shard with no `.db` must fail as it always did, not ship an empty database. A trusted handle
    # (no `tenant?`): no authorizer, no statement deadline.
    with {:ok, _} <- File.stat(state.db_path),
         {:ok, conn} <- Connection.open(state.db_path, scope: :ro) do
      case pin_snapshot(conn) do
        :ok ->
          :telemetry.execute([:fathom, :replication, :seed_pin, :open], %{count: 1}, %{})
          {:ok, %{state | conn: conn}}

        {:error, reason} ->
          Connection.close(conn)
          {:error, {:seed_snapshot_failed, reason}}
      end
    end
  end

  defp ensure_pinned(state), do: {:ok, state}

  # BEGIN alone takes no snapshot (it is DEFERRED); the read is what acquires the WAL read mark.
  defp pin_snapshot(conn) do
    with :ok <- Connection.exec(conn, "BEGIN") do
      Connection.exec(conn, "SELECT 1 FROM sqlite_schema LIMIT 1")
    end
  end

  # `conn` stays referenced from the state for the whole hold: a NIF resource nothing references is
  # finalized at the next GC, which releases the snapshot mid-stream (probed in
  # seed_pinned_snapshot_test.exs).
  defp close(%{conn: nil} = state), do: state

  defp close(%{conn: conn} = state) do
    _ = Connection.rollback_if_open(conn)
    Connection.close(conn)
    %{state | conn: nil}
  end
end
