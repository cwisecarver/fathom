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
        cond do
          count > before and buffer != [] ->
            statements = Enum.reverse(buffer)

            # `count` is the template's post-commit django_migrations count — recorded on the
            # release so the post-revert drift check (#32) can compare the template against HEAD.
            case record(statements, count) do
              {:recorded, _} = recorded ->
                {:reply, recorded, state}

              {:error, _} = error ->
                # Expert review #19: the migration has ALREADY committed on the template
                # shard, so re-running `manage.py migrate` is a no-op — dropping this
                # buffer on a Postgres blip would permanently fork template schema from
                # fleet schema (every subsequent captured version assumes DDL the fleet
                # never received, so all future replays fail or half-apply). Keep the
                # statements and retry until the control plane recovers.
                {:reply, error, stash_pending(state, statements, count)}
            end

          # django_migrations SHRANK: a backwards Django migrate (`manage.py migrate <app> <prev>`)
          # deleted its bookkeeping row (expert review 2026-07-14 #6). The fleet does NOT follow a
          # backwards migrate — fleet undo is a fathom revert — so alarm and reconcile before the
          # next capture, or the next captured version assumes DDL the fleet still has.
          count < before ->
            alarm_backwards(before, count)
            {:reply, :noop, state}

          true ->
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
  # takes next_version(), so recording a later capture past a failed earlier one
  # would assign the fleet versions out of order.
  defp drain_pending([]), do: []

  defp drain_pending([{statements, count} | rest] = all) do
    case record(statements, count) do
      {:recorded, _} -> drain_pending(rest)
      {:error, _} -> all
    end
  end

  defp stash_pending(state, statements, count) do
    schedule_retry(
      Map.update(state, :pending, [{statements, count}], &(&1 ++ [{statements, count}]))
    )
  end

  defp schedule_retry(state) do
    if Map.get(state, :retry_timer) do
      state
    else
      Map.put(state, :retry_timer, Process.send_after(self(), :retry_pending, retry_ms()))
    end
  end

  defp retry_ms, do: Application.get_env(:fathom, :capture_retry_ms, 5_000)

  # Detect template-literal DATA migrations in a captured buffer (expert review 2026-07-14 #1). A
  # Django RunPython backfill's ORM writes cross the wire as literal INSERT/UPDATE/DELETE on tenant
  # tables carrying the TEMPLATE's row values; replayed verbatim onto every shard they overwrite
  # tenants whose ids collide or (from an empty template) silently never run — either way the
  # version stamp says "applied". We still RECORD the version — refusing would fork the template
  # from the fleet, since the migration already committed on the template (the expert-review-#19
  # invariant) — but convert the previously SILENT corruption into a loud, structured, alertable
  # signal so an operator can review/yank the version before rollout replays it fleet-wide.
  defp alarm_on_data_migration(version, statements) do
    case data_migration_statements(statements) do
      [] ->
        Logger.info("captured shard-schema version #{version} (#{length(statements)} statements)")

      suspect ->
        Logger.error(
          "captured shard-schema version #{version} contains #{length(suspect)} DATA-MIGRATION " <>
            "statement(s) (INSERT/UPDATE/DELETE on non-django_migrations tables) that REPLAY AS " <>
            "TEMPLATE-LITERAL SQL onto every tenant (expert review #1) — review before rollout and " <>
            "yank the version if unintended. First: #{inspect(hd(suspect))}"
        )

        :telemetry.execute(
          [:fathom, :migrator, :data_migration_captured],
          %{count: length(suspect)},
          %{version: version}
        )
    end
  end

  # A backwards Django migrate on the template (expert review #6): shout, don't silently :noop.
  defp alarm_backwards(before, count) do
    Logger.error(
      "template django_migrations shrank #{before} → #{count} on commit — a BACKWARDS Django " <>
        "migrate ran on the template (expert review #6). The fleet does NOT follow a backwards " <>
        "migrate; fleet undo is a fathom revert. Reconcile before the next capture."
    )

    :telemetry.execute(
      [:fathom, :migrator, :backwards_migrate],
      %{before: before, count: count},
      %{}
    )
  end

  @dml_leads ~w(insert update delete replace)

  # A statement is a (template-literal) data migration if it's DML and doesn't touch
  # `django_migrations` — Django's own bookkeeping `INSERT INTO django_migrations` is the one benign
  # DML in a migration transaction. A heuristic, not a SQL parser: it flags the RunPython-backfill
  # case; a data migration that references django_migrations in a WHERE clause (rare) would be missed.
  defp data_migration_statements(statements) do
    Enum.filter(statements, fn sql ->
      lead = sql |> String.trim_leading() |> String.slice(0, 12) |> String.downcase()

      Enum.any?(@dml_leads, &String.starts_with?(lead, &1)) and
        not String.contains?(String.downcase(sql), "django_migrations")
    end)
  end

  # The version is computed per attempt (next_version() under the unique index as
  # the arbiter — NOT head()+1, which excludes yanked rows and would collide on
  # the tombstoned number forever after a yank, expert review #10), so a retry
  # after the control plane recovers picks the then-current max. Rescue/catch: a
  # Postgres outage RAISES from Repo (it doesn't return an error tuple), and a
  # crash here would take the whole capture state — including every pending
  # buffer this path exists to preserve — down with it.
  defp record(statements, template_migration_count) do
    version = Migrator.next_version()

    case Migrator.release(version, "auto-captured", statements, template_migration_count) do
      {:ok, _} ->
        alarm_on_data_migration(version, statements)
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
