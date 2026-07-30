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

  **The count does not always rise by `COMMIT`.** Django's SQLite backend has to commit before it
  can re-enable foreign-key checks, so for any migration that disables them — a `CreateModel`
  carrying a ForeignKey, for instance — it writes the `django_migrations` row *after* the
  transaction:

      BEGIN / CREATE TABLE … / CREATE INDEX … / COMMIT / PRAGMA foreign_keys = ON /
      INSERT INTO django_migrations …

  A commit with no rise therefore parks its buffer **awaiting bookkeeping** instead of discarding
  it, and `bookkeeping/4` records the version when that row lands (`bookkeeping?/1` recognizes it).
  An awaiting buffer that never gets confirmed is dropped on the next `BEGIN` or on stream close, so
  a genuinely empty transaction is still a no-op. Without this the migration read as a no-op, the
  version was never recorded, and the template silently advanced past the fleet with no alarm.
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

  A commit whose count did NOT rise keeps its buffer *awaiting bookkeeping* rather than
  discarding it — see `bookkeeping/4`.
  """
  def commit(conn_id, migrations_count, server \\ __MODULE__),
    do: GenServer.call(server, {:commit, conn_id, migrations_count})

  @doc """
  Whether `sql` is Django's own `INSERT INTO django_migrations` bookkeeping row — the statement
  that marks a migration as applied. Deliberately does not match a `DELETE` (a backwards migrate,
  which `commit/3`'s shrink branch alarms on).
  """
  @spec bookkeeping?(String.t()) :: boolean()
  def bookkeeping?(sql) when is_binary(sql) do
    norm = sql |> String.trim() |> String.upcase()
    String.starts_with?(norm, "INSERT") and String.contains?(norm, "DJANGO_MIGRATIONS")
  end

  def bookkeeping?(_), do: false

  @doc """
  Feeds Django's `INSERT INTO django_migrations` bookkeeping row for `conn_id`, with the
  template's post-statement `migrations_count`.

  Two shapes reach here, and both must work:

  - **Inside the transaction** (the common case, e.g. a plain `0001_initial`): a buffer is open, so
    this behaves exactly like `append/3` and `commit/3` records the version as before.
  - **After the transaction** (`COMMIT` then the bookkeeping row): Django's SQLite backend disables
    FK checks around some DDL — a `CreateModel` carrying a ForeignKey, for instance — which forces
    it to commit the DDL *before* recording the migration. The count has not risen at `COMMIT`, so
    the count-rose boundary test alone reads the migration as a no-op and the version is **never
    recorded**: the template advances and the fleet silently never hears (found live 2026-07-30).
    `commit/3` therefore parks such a buffer *awaiting bookkeeping*, and this call is what closes it.

  Returns `{:recorded, version}`, `:noop`, or `{:error, _}` (same contract as `commit/3`).
  """
  def bookkeeping(conn_id, sql, migrations_count, server \\ __MODULE__),
    do: GenServer.call(server, {:bookkeeping, conn_id, sql, migrations_count})

  @doc "Discards `conn_id`'s buffered transaction (ROLLBACK)."
  def rollback(conn_id, server \\ __MODULE__),
    do: GenServer.cast(server, {:rollback, conn_id})

  @doc "Drops any buffer for `conn_id` (connection closed)."
  def forget(conn_id, server \\ __MODULE__),
    do: GenServer.cast(server, {:forget, conn_id})

  @doc """
  How many captured template versions are buffered pending a Postgres write (expert review
  2026-07-18 #6). A value > 0 means a control-plane outage is in progress: these migrations HAVE
  committed on the template but have NOT been recorded fleet-wide, so a restart now (a rolling
  deploy) loses them — and every later captured version then assumes DDL the fleet never received.
  A deploy/drain gate can poll this and refuse a rolling restart until it drains to 0; `terminate/2`
  alarms loudly if a shutdown happens while it's > 0.
  """
  @spec pending_count(GenServer.server()) :: non_neg_integer()
  def pending_count(server \\ __MODULE__), do: GenServer.call(server, :pending_count)

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
  def init(state) do
    # Trap exits so a graceful supervisor shutdown (a rolling deploy / SIGTERM) runs terminate/2,
    # where the #6 guard alarms if captures are still buffered pending a Postgres write.
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  @impl true
  def handle_cast({:begin, conn_id, count}, state) do
    # A new transaction supersedes any buffer awaiting bookkeeping: that one never got its
    # `django_migrations` row, so it really was a no-op transaction (the pre-fix behavior).
    {:noreply, Map.put(state, conn_id, %{buffer: [], count_at_begin: count, awaiting?: false})}
  end

  def handle_cast({:append, conn_id, sql}, state) do
    case Map.get(state, conn_id) do
      nil ->
        {:noreply, state}

      # Awaiting bookkeeping = the transaction is CLOSED. Statements after the COMMIT (Django sends
      # `PRAGMA foreign_keys = ON` there) are not part of the migration and must not pollute the
      # version. Only the bookkeeping row itself is accepted, via bookkeeping/4.
      %{awaiting?: true} ->
        {:noreply, state}

      entry ->
        {:noreply, Map.put(state, conn_id, %{entry | buffer: [sql | entry.buffer]})}
    end
  end

  def handle_cast({:rollback, conn_id}, state), do: {:noreply, Map.delete(state, conn_id)}
  def handle_cast({:forget, conn_id}, state), do: {:noreply, Map.delete(state, conn_id)}

  @impl true
  def handle_call(:pending_count, _from, state) do
    {:reply, length(Map.get(state, :pending, [])), state}
  end

  def handle_call({:commit, conn_id, count}, _from, state) do
    case Map.pop(state, conn_id) do
      {nil, state} ->
        {:reply, :noop, state}

      {%{buffer: buffer, count_at_begin: before}, state} ->
        cond do
          count > before and buffer != [] ->
            statements = Enum.reverse(buffer)

            # `count`/`before` are the template's post-commit and pre-transaction django_migrations
            # counts — recorded so the post-revert drift check (#32) can compare the template against
            # HEAD, and so record/3 can detect a non-atomic-migration GAP (#6): `before` should equal
            # the last captured count; if it's higher, migrations ran OUTSIDE a tracked transaction.
            case record(statements, count, before) do
              {:recorded, _} = recorded ->
                {:reply, recorded, state}

              {:error, _} = error ->
                # Expert review #19: the migration has ALREADY committed on the template
                # shard, so re-running `manage.py migrate` is a no-op — dropping this
                # buffer on a Postgres blip would permanently fork template schema from
                # fleet schema (every subsequent captured version assumes DDL the fleet
                # never received, so all future replays fail or half-apply). Keep the
                # statements and retry until the control plane recovers.
                {:reply, error, stash_pending(state, statements, count, before)}
            end

          # django_migrations SHRANK: a backwards Django migrate (`manage.py migrate <app> <prev>`)
          # deleted its bookkeeping row (expert review 2026-07-14 #6). The fleet does NOT follow a
          # backwards migrate — fleet undo is a fathom revert — so alarm and reconcile before the
          # next capture, or the next captured version assumes DDL the fleet still has.
          count < before ->
            alarm_backwards(before, count)
            {:reply, :noop, state}

          # The count did NOT rise, but we buffered DDL. Django's SQLite backend commits the DDL
          # BEFORE writing the `django_migrations` row whenever it has to disable FK checks, so this
          # is not necessarily a no-op transaction — the bookkeeping row may be one statement away.
          # Park the buffer AWAITING BOOKKEEPING instead of dropping it (see bookkeeping/4); an entry
          # that never gets confirmed is discarded on the next BEGIN or on stream close, which is
          # exactly the old no-op behavior.
          buffer != [] ->
            {:reply, :noop, Map.put(state, conn_id, awaiting(buffer, before))}

          true ->
            {:reply, :noop, state}
        end
    end
  end

  def handle_call({:bookkeeping, conn_id, sql, count}, _from, state) do
    case Map.get(state, conn_id) do
      # An OPEN transaction: this is the ordinary in-transaction bookkeeping row. Buffer it and let
      # commit/3 do the recording, byte-for-byte as before.
      %{awaiting?: false} = entry ->
        {:reply, :noop, Map.put(state, conn_id, %{entry | buffer: [sql | entry.buffer]})}

      # A committed-but-unrecorded buffer, and the row Django was missing just landed. The recorded
      # statements INCLUDE this INSERT, so a replayed shard gets its own bookkeeping row — the same
      # shape the in-transaction path produces.
      %{awaiting?: true, buffer: buffer, count_at_begin: before} when count > before ->
        {entry, state} = Map.pop(state, conn_id)
        _ = entry
        statements = Enum.reverse([sql | buffer])

        case record(statements, count, before) do
          {:recorded, _} = recorded ->
            {:reply, recorded, state}

          # Same durability rule as commit/3: the migration has ALREADY committed on the template,
          # so re-running `manage.py migrate` is a no-op. Keep the statements and retry.
          {:error, _} = error ->
            {:reply, error, stash_pending(state, statements, count, before)}
        end

      _ ->
        {:reply, :noop, state}
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

  # We trap exits (see init/1) for the shutdown guard; a stray non-parent EXIT would otherwise
  # crash the process — and take the pending buffer this whole path exists to preserve down with it.
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    case Map.get(state, :pending, []) do
      [] ->
        :ok

      pending ->
        # Expert review 2026-07-18 #6: a clean shutdown (a rolling deploy) WHILE captures are
        # buffered pending a Postgres write loses them silently — the buffer is in-memory only, and
        # Postgres is the thing that's down so it can't be persisted. We can't veto the shutdown, but
        # we make it LOUD (Logger.error + telemetry) so it's alertable, and pending_count/1 lets a
        # deploy gate refuse the restart until the buffer drains.
        Logger.error(
          "Fathom.Migrator.Capture is shutting down with #{length(pending)} captured template " <>
            "version(s) NOT yet recorded to Postgres (a control-plane outage) — these committed-on-" <>
            "template migrations are being LOST, so the fleet will assume DDL it never received. Do " <>
            "NOT deploy/restart during a control-plane outage; drain Capture.pending_count/1 to 0 first."
        )

        :telemetry.execute(
          [:fathom, :migrator, :capture_pending_on_shutdown],
          %{count: length(pending)},
          %{}
        )

        :ok
    end
  end

  # Record pending captures IN ORDER, stopping at the first failure — each release
  # takes next_version(), so recording a later capture past a failed earlier one
  # would assign the fleet versions out of order.
  defp drain_pending([]), do: []

  defp drain_pending([{statements, count, before} | rest] = all) do
    case record(statements, count, before) do
      {:recorded, _} -> drain_pending(rest)
      {:error, _} -> all
    end
  end

  defp stash_pending(state, statements, count, before) do
    entry = {statements, count, before}
    schedule_retry(Map.update(state, :pending, [entry], &(&1 ++ [entry])))
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

  # Expert review #6: a gap exists when this transaction's pre-count `before` exceeds the last
  # captured template count — uncaptured migrations (a non-atomic `atomic = False` one runs
  # autocommit, invisible to capture) landed in between. `nil` when nothing captured yet or the last
  # release predates the recorded count (the documented Django-baseline offset / pre-feature rows).
  defp migration_gap(before) do
    case Migrator.last_template_count() do
      last when is_integer(last) and before > last -> %{before: before, last: last}
      _ -> nil
    end
  end

  defp alarm_gap(version, %{before: before, last: last}) do
    Logger.error(
      "captured version #{version} began with django_migrations count #{before}, but the last " <>
        "captured count was #{last} — #{before - last} migration(s) landed on the template OUTSIDE " <>
        "capture (a non-atomic `atomic = False` migration runs autocommit and is invisible). The " <>
        "fleet never received them, so this version and everything above it assume DDL the fleet " <>
        "lacks. Flagged requires_review (rollout frozen below it); reconcile template↔fleet before " <>
        "approving. Fleet undo is a fathom revert, never a Django backwards migrate."
    )

    :telemetry.execute(
      [:fathom, :migrator, :migration_gap],
      %{count: 1, gap: before - last},
      %{version: version}
    )
  end

  # A committed-but-unrecorded buffer: the DDL landed, the `django_migrations` row has not (yet).
  defp awaiting(buffer, count_at_begin),
    do: %{buffer: buffer, count_at_begin: count_at_begin, awaiting?: true}

  @dml_leads ~w(insert update delete replace)

  # A statement is a (template-literal) data migration if it's DML and doesn't touch
  # `django_migrations` — Django's own bookkeeping `INSERT INTO django_migrations` is the one benign
  # DML in a migration transaction. A heuristic, not a SQL parser: it flags the RunPython-backfill
  # case; a data migration that references django_migrations in a WHERE clause (rare) would be missed.
  defp data_migration_statements(statements) do
    Enum.filter(statements, fn sql ->
      lead = sql |> String.trim_leading() |> String.slice(0, 12) |> String.downcase()
      down = String.downcase(sql)

      Enum.any?(@dml_leads, &String.starts_with?(lead, &1)) and
        not String.contains?(down, "django_migrations") and
        not shard_local_row_copy?(lead, down)
    end)
  end

  # Django's SQLite backend cannot ALTER most things in place, so it REBUILDS the table
  # (`_remake_table`): CREATE TABLE new__x → INSERT INTO new__x (cols) SELECT cols FROM x →
  # DROP TABLE x → ALTER TABLE new__x RENAME TO x. That INSERT is DML, and flagging it froze the
  # fleet HEAD below almost every real Django migration on SQLite — AlterField, AddField with a
  # default, unique_together, index changes all take the rebuild path — so the unattended rollout
  # this engine exists for could essentially never proceed. Found by running an actual Django
  # `migrate` through capture (a real `0001_initial` with `Meta.indexes` rebuilds), which the
  # hand-written toy SQL in the tests never exercised.
  #
  # The exemption is narrow and safe by construction: the INSERT's rows come from a SELECT over a
  # table in the SAME file (a shard is one SQLite database and fathom never ATTACHes another), so
  # replaying it onto another tenant copies THAT tenant's rows. There are no template values in it
  # to leak. That is precisely the opposite of the RunPython-backfill shape this lint exists to
  # catch, where the template's own row values ride along in the statement.
  #
  # Fails closed: only an INSERT drawing from a SELECT with NO `values` row-source is exempt.
  # Anything carrying VALUES (including `INSERT ... VALUES ((SELECT …))`) stays flagged, as does
  # every UPDATE/DELETE/REPLACE — the rebuild path never emits those.
  defp shard_local_row_copy?(lead, down) do
    String.starts_with?(lead, "insert") and String.contains?(down, "select") and
      not String.contains?(down, "values")
  end

  # The version is computed per attempt (next_version() under the unique index as
  # the arbiter — NOT head()+1, which excludes yanked rows and would collide on
  # the tombstoned number forever after a yank, expert review #10), so a retry
  # after the control plane recovers picks the then-current max. Rescue/catch: a
  # Postgres outage RAISES from Repo (it doesn't return an error tuple), and a
  # crash here would take the whole capture state — including every pending
  # buffer this path exists to preserve — down with it.
  defp record(statements, count, before) do
    version = Migrator.next_version()

    # Expert review #6: a NON-ATOMIC migration (`atomic = False`) runs autocommit — no tracked
    # BEGIN/COMMIT — so capture never sees it, the template schema moves, and the fleet never hears.
    # We catch it at the NEXT capture: `before` (this transaction's pre-count) should equal the last
    # captured count; if it's higher, uncaptured migrations landed in between (the gap). Compared
    # against DURABLE per-release state, not in-memory Capture state (the shared-singleton false-alarm
    # the earlier attempt hit).
    gap = migration_gap(before)

    # Expert review #1: flag a captured version that carries template-literal DATA migrations (or a
    # detected gap) so HEAD stays below it until an operator reviews it — replaying its DML fleet-wide
    # corrupts/skips tenant data, and a gap means it assumes DDL the fleet doesn't have. We still
    # RECORD it (refusing would fork the template from the fleet, the #19 invariant); the flag blocks
    # the rollout, not the capture.
    requires_review = data_migration_statements(statements) != [] or gap != nil

    case Migrator.release(version, "auto-captured", statements, count, requires_review) do
      {:ok, _} ->
        if gap, do: alarm_gap(version, gap)
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
