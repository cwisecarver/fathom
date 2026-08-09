defmodule Fathom.Shard.Replication.Session do
  @moduledoc """
  One replication session per shard — the commit-path integration for A2.
  See `docs/a2-quorum-replication.md`.

  Owns the shard's `Primary` state (`{wal_gen, salt1, offset}`) and is the **serialization point**
  for shipping. One process per shard, so two streams that commit back to back cannot both compute
  a delta from the same stale offset and ship overlapping ranges.

  ## Why this is not in `Fathom.Shard`

  The coordinator would have been the obvious home — it is already one process per shard and
  already owns the lease this module fences with. It is also ~3000 lines of intricate
  lease/fence/flush state machine, and a synchronous multi-millisecond network wait inside its
  mailbox would sit in front of every checkout and every durability flush for that shard. Keeping
  the wait in a separate process means a slow follower delays commits for its shard and nothing
  else.

  ## The gate, and what a tenant sees when the quorum fails

  Off unless `:replication_enabled`. When on, a committing statement does not return to the client
  until `q` followers have acked.

  **The local commit has already happened by then** — SQLite committed before the WAL could be
  read, and there is no un-commit. So a quorum failure returns an error for a write that IS durable
  locally and WILL reach S3 on the next flush. That is deliberate and it is the honest direction:
  the client asked for a quorum-durable write and did not get one, so it must not be told it did.
  The cost is an **at-least-once** hazard — a client that retries may apply the write twice — which
  is the same hazard any commit-ack-lost path already has (`docs/durability.md`), not a new one.

  Reporting success instead would be worse in the way that matters: it would make the failure
  invisible exactly when the data is least protected.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Replication
  alias Fathom.Shard.Replication.Primary
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Wal

  @registry Fathom.Shard.Replication.SessionRegistry

  # ------------------------------------------------------------------------------------------
  # api
  # ------------------------------------------------------------------------------------------

  @doc "Whether commit-path replication is on. Off by default."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :replication_enabled, false)

  @doc """
  Replicate everything committed to `wal_path` since the last successful ship.

  Synchronous by design: this is the quorum wait, and the point of A2 is that the client's success
  is gated on it. Returns `:ok` when nothing needed shipping, too.
  """
  @spec commit(String.t(), Path.t(), pid()) :: :ok | {:error, term()}
  def commit(shard_id, wal_path, coordinator) do
    with {:ok, pid} <- ensure_started(shard_id, coordinator) do
      GenServer.call(pid, {:commit, wal_path}, timeout())
    end
  catch
    :exit, reason -> {:error, {:session_down, reason}}
  end

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, opts[:shard_id]},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  def start_link(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {@registry, shard_id}})
  end

  @doc """
  Forget a shard's replication state — used when it drains, and by tests.

  A no-op when replication is not running. The registry only exists under `Fleet`, so with the
  feature off (or already shut down) there is nothing to look up, and a caller tidying up should
  not have to know which. Raising here made teardown fail in tests that had stopped `Fleet` first.
  """
  @spec stop(String.t()) :: :ok
  def stop(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] -> GenServer.stop(pid, :normal)
      [] -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp ensure_started(shard_id, coordinator) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               Fathom.Shard.Replication.SessionSupervisor,
               {__MODULE__, shard_id: shard_id, coordinator: coordinator}
             ) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp timeout, do: Application.get_env(:fathom, :replication_timeout_ms, 5_000)

  # ------------------------------------------------------------------------------------------
  # server
  # ------------------------------------------------------------------------------------------

  @impl true
  def init(opts) do
    coordinator = Keyword.fetch!(opts, :coordinator)

    # The cached epoch must never outlive the ownership it describes. Monitoring the coordinator
    # means a steal — which stops it — also stops this session, so the next commit starts a fresh
    # one and re-reads the epoch. Without this, a deposed node would keep shipping under its old
    # epoch and rely entirely on the follower to notice.
    Process.monitor(coordinator)

    {:ok,
     %{
       shard_id: Keyword.fetch!(opts, :shard_id),
       coordinator: coordinator,
       epoch: nil,
       repl: nil
     }}
  end

  @impl true
  def handle_call({:commit, wal_path}, _from, state) do
    with {:ok, state} <- with_epoch(state),
         {:ok, new_repl} <- ship(state, wal_path, state.epoch) do
      {:reply, :ok, %{state | repl: new_repl}}
    else
      :nothing -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, coordinator, _reason}, %{coordinator: coordinator} = s) do
    # The shard moved or drained. Our offset describes a WAL this node no longer owns.
    {:stop, :normal, s}
  end

  def handle_info(_, state), do: {:noreply, state}

  # Read the lease epoch once. `{:error, :no_lease}` must NOT be shipped past — frames from a node
  # without a lease are exactly what the follower's epoch check refuses.
  defp with_epoch(%{epoch: e} = state) when is_integer(e), do: {:ok, state}

  defp with_epoch(state) do
    case Fathom.Shard.epoch(state.coordinator) do
      {:ok, epoch} -> {:ok, %{state | epoch: epoch}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ship(state, wal_path, epoch) do
    with {:ok, header} <- Wal.read(wal_path),
         plan when plan != :nothing <- Primary.plan(state.repl, header),
         {kind, offset, len} = plan,
         {:ok, payload} <- Wal.read_delta(wal_path, offset, len) do
      push = %Push{
        shard_id: state.shard_id,
        epoch: epoch,
        wal_gen: header.ckpt_seq,
        # A reset must arrive at offset 0 or FollowerLog refuses it — the follower has to discard
        # its old generation rather than splice. `plan/2` already returns offset 0 for a reset;
        # this only makes the coupling explicit.
        offset: if(kind == :reset, do: 0, else: offset),
        payload: payload
      }

      case Replication.ship_quorum(shippers(), push, quorum(), timeout()) do
        :ok ->
          # Advance ONLY after the quorum acked. Advancing on a failure would leave the next delta
          # starting past a gap no follower holds.
          {:ok, Primary.advance(header, plan)}

        {:error, reason} ->
          {:error, reason}
      end
    else
      # An empty WAL arrives here as `:nothing` too: `Wal.read/1` returns `{:ok, :empty}`, which the
      # `with` binds as the header, and `Primary.plan/2` maps to `:nothing`. No separate clause.
      :nothing ->
        :nothing

      {:error, reason} ->
        Logger.warning("replication read failed for #{state.shard_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp shippers, do: Fathom.Shard.Replication.Fleet.shippers()
  defp quorum, do: Application.get_env(:fathom, :replication_quorum, 2)
end
