defmodule Fathom.Migrator.Capture do
  @moduledoc """
  Captures Django's schema migrations off the reserved template shard, with no
  change to how Django migrates.

  Django runs `migrate` against the template like any database; `Fathom.ShardExecutor`
  feeds each statement here in order. We buffer the statements of a transaction and,
  on `COMMIT`, check whether the template's `django_migrations` row count rose — if
  it did, a migration just landed, so the buffered SQL (the DDL plus Django's own
  `INSERT INTO django_migrations` bookkeeping) is recorded as the next fleet
  version via `Fathom.Migrator.release/3`. The rollout later replays that SQL onto
  every shard.

  Boundary detection is the row-count rise, not SQL parsing; only transaction-
  control verbs are classified (`classify/1`). Statements outside a tracked
  transaction (autocommit) are ignored — see the engine plan's limitations.
  """
  use GenServer

  require Logger

  alias Fathom.Migrator

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Starts a transaction buffer for `conn_id`, recording the pre-transaction count."
  def begin(conn_id, migrations_count, server \\ __MODULE__),
    do: GenServer.cast(server, {:begin, conn_id, migrations_count})

  @doc "Buffers a statement for `conn_id` (no-op outside a tracked transaction)."
  def append(conn_id, sql, server \\ __MODULE__),
    do: GenServer.cast(server, {:append, conn_id, sql})

  @doc """
  Closes `conn_id`'s transaction: if `migrations_count` rose, records the buffered
  SQL as the next version. Returns `{:recorded, version}`, `:noop`, or `{:error, _}`.
  """
  def commit(conn_id, migrations_count, server \\ __MODULE__),
    do: GenServer.call(server, {:commit, conn_id, migrations_count})

  @doc "Discards `conn_id`'s buffered transaction (ROLLBACK)."
  def rollback(conn_id, server \\ __MODULE__),
    do: GenServer.cast(server, {:rollback, conn_id})

  @doc "Drops any buffer for `conn_id` (connection closed)."
  def forget(conn_id, server \\ __MODULE__),
    do: GenServer.cast(server, {:forget, conn_id})

  @doc "Classifies a statement's transaction role. Savepoints count as `:other`."
  @spec classify(String.t()) :: :begin | :commit | :rollback | :other
  def classify(sql) do
    norm = sql |> String.trim() |> String.upcase()

    cond do
      String.starts_with?(norm, "BEGIN") -> :begin
      String.starts_with?(norm, "COMMIT") -> :commit
      String.starts_with?(norm, "END") -> :commit
      norm == "ROLLBACK" or String.starts_with?(norm, "ROLLBACK;") -> :rollback
      String.starts_with?(norm, "ROLLBACK TRANSACTION") -> :rollback
      true -> :other
    end
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_cast({:begin, conn_id, count}, state) do
    {:noreply, Map.put(state, conn_id, %{buffer: [], count_at_begin: count})}
  end

  def handle_cast({:append, conn_id, sql}, state) do
    case Map.get(state, conn_id) do
      nil -> {:noreply, state}
      entry -> {:noreply, Map.put(state, conn_id, %{entry | buffer: [sql | entry.buffer]})}
    end
  end

  def handle_cast({:rollback, conn_id}, state), do: {:noreply, Map.delete(state, conn_id)}
  def handle_cast({:forget, conn_id}, state), do: {:noreply, Map.delete(state, conn_id)}

  @impl true
  def handle_call({:commit, conn_id, count}, _from, state) do
    case Map.pop(state, conn_id) do
      {nil, state} ->
        {:reply, :noop, state}

      {%{buffer: buffer, count_at_begin: before}, state} ->
        if count > before and buffer != [] do
          statements = Enum.reverse(buffer)

          case record(statements) do
            {:recorded, _} = recorded ->
              {:reply, recorded, state}

            {:error, _} = error ->
              # Expert review #19: the migration has ALREADY committed on the template
              # shard, so re-running `manage.py migrate` is a no-op — dropping this
              # buffer on a Postgres blip would permanently fork template schema from
              # fleet schema (every subsequent captured version assumes DDL the fleet
              # never received, so all future replays fail or half-apply). Keep the
              # statements and retry until the control plane recovers.
              {:reply, error, stash_pending(state, statements)}
          end
        else
          {:reply, :noop, state}
        end
    end
  end

  @impl true
  def handle_info(:retry_pending, state) do
    state = Map.delete(state, :retry_timer)

    case Map.pop(state, :pending, []) do
      {[], state} ->
        {:noreply, state}

      {pending, state} ->
        case drain_pending(pending) do
          [] -> {:noreply, state}
          still -> {:noreply, schedule_retry(Map.put(state, :pending, still))}
        end
    end
  end

  # Record pending captures IN ORDER, stopping at the first failure — each release
  # takes head()+1, so recording a later capture past a failed earlier one would
  # assign the fleet versions out of order.
  defp drain_pending([]), do: []

  defp drain_pending([statements | rest] = all) do
    case record(statements) do
      {:recorded, _} -> drain_pending(rest)
      {:error, _} -> all
    end
  end

  defp stash_pending(state, statements) do
    schedule_retry(Map.update(state, :pending, [statements], &(&1 ++ [statements])))
  end

  defp schedule_retry(state) do
    if Map.get(state, :retry_timer) do
      state
    else
      Map.put(state, :retry_timer, Process.send_after(self(), :retry_pending, retry_ms()))
    end
  end

  defp retry_ms, do: Application.get_env(:fathom, :capture_retry_ms, 5_000)

  # The version is computed per attempt (head()+1 under the unique index as the
  # arbiter), so a retry after the control plane recovers picks the then-current
  # head. Rescue/catch: a Postgres outage RAISES from Repo (it doesn't return an
  # error tuple), and a crash here would take the whole capture state — including
  # every pending buffer this path exists to preserve — down with it.
  defp record(statements) do
    version = Migrator.head() + 1

    case Migrator.release(version, "auto-captured", statements) do
      {:ok, _} ->
        Logger.info("captured shard-schema version #{version} (#{length(statements)} statements)")
        {:recorded, version}

      {:error, _} = error ->
        Logger.error("failed to record captured version #{version}: #{inspect(error)}")
        error
    end
  rescue
    e ->
      Logger.error("failed to record captured version: #{Exception.message(e)}")
      {:error, e}
  catch
    :exit, reason ->
      Logger.error("failed to record captured version: #{inspect(reason)}")
      {:error, {:exit, reason}}
  end
end
