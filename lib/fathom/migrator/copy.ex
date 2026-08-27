defmodule Fathom.Migrator.Copy do
  @moduledoc """
  Builds a shard's next version: copy the old database file, then replay the
  version's captured Django SQL onto the copy and stamp the version.

  The captured statements (DDL plus Django's own `INSERT INTO django_migrations`
  bookkeeping) are replayed verbatim in a single transaction, so the new file ends
  up exactly as if Django had migrated the shard directly — its `django_migrations`
  table stays consistent for free. `PRAGMA user_version = N` is stamped after the
  commit as an O(1) version gate (and the crash-recovery signal). On any replay
  error the transaction rolls back, leaving the copy at the old schema.
  """
  alias Fathom.Migrator.Transform
  alias Fathom.Shard.Connection

  @doc """
  Copies `source_path` to `dest_path`, replays `statements` onto the copy, and
  stamps `PRAGMA user_version = version`. Returns `:ok` or `{:error, reason}`
  (leaving `dest_path` at the old schema on a replay error).
  """
  @typedoc """
  One replayable statement: the SQL and the values to BIND to it, never interpolated.

  The pair is the whole point. Django sends parameterized SQL — its bookkeeping row is
  `INSERT INTO django_migrations … VALUES (?, ?, ?)` — and storing the text alone made every
  replay bind NULL and die on `django_migrations.app NOT NULL`, so no captured migration could be
  replayed onto a tenant at all (fixed in `beff929`). That fix changed `replay_each/2`, the
  capture, the release row and every caller — but not this spec, which still said `[String.t()]`.
  Dialyzer caught it from `Fathom.Bench`, which passes pairs and was therefore reported as a call
  that "will not succeed".
  """
  @type statement :: {String.t(), [term()]}

  @spec migrate(Path.t(), Path.t(), non_neg_integer(), [statement()]) :: :ok | {:error, term()}
  def migrate(source_path, dest_path, version, statements),
    do: migrate_chain(source_path, dest_path, [{version, statements}])

  @doc """
  Copies `source_path` to `dest_path` and replays a CHAIN of versions in order
  (round-2 #9: a multi-step laggard must apply every intermediate version's
  statements, not just the target's). Each step runs in its own transaction and
  stamps its `user_version` after commit, so a failure mid-chain leaves the copy
  at the last fully-applied version — never half a step. Returns `:ok` or
  `{:error, reason}`.

  A step is `{version, statements}` or, since expert review 2026-08-01 #26,
  `{version, statements, transform}` — the name of a module implementing
  `Fathom.Migrator.Transform`, run **inside the same transaction, after the DDL**.

  `opts[:shard_id]` is passed to the transform so it knows whose data it is operating on. It is
  REQUIRED when any step carries a transform: a backfill that does not know its tenant is either
  wrong or does not need to be a transform at all, and defaulting it to `nil` would hide the
  mistake behind a `nil` argument.
  """
  @spec migrate_chain(Path.t(), Path.t(), [tuple()], keyword()) :: :ok | {:error, term()}
  def migrate_chain(source_path, dest_path, chain, opts \\ []) do
    shard_id = Keyword.get(opts, :shard_id)

    with :ok <- validate_transforms(chain, shard_id),
         :ok <- copy_file(source_path, dest_path),
         # SERVING CONFIGURATION ON PURPOSE — a `purpose: :scratch` handle was built, MEASURED, and
         # REVERTED (expert review 2026-08-26 #29). Do not rebuild it without new evidence.
         #
         # The finding is right about the mechanism: this destination is a temp file that gets
         # uploaded and then `drop_temp`'d, so it pays `synchronous=FULL`'s WAL fsync per commit
         # plus a `wal_checkpoint(TRUNCATE)` main-DB fsync per chain step for bytes deleted seconds
         # later. The fix (`journal_mode=MEMORY`, `synchronous=OFF`, no checkpoint) was implemented
         # and run through the audit's own falsifying experiment.
         #
         # MEASURED: `copy_keystone_rows_per_s` 2 994 909 without vs 2 977 254 with — **-0.6%**,
         # inside noise. The audit's own instruction for that outcome is "the copy is dominated by
         # `File.cp` plus the S3 round trips, this is not the bottleneck — bank that and stop".
         #
         # Two reasons it stayed reverted rather than shipping as a free win. The metric is
         # single-threaded and one chain step, so it cannot see the finding's ACTUAL claim (fsync
         # pressure at 10 concurrent migration workers) — the number proves no throughput win, not
         # no fsync reduction. And dropping the checkpoint introduces a coupling that did not exist
         # before: it is only safe BECAUSE `journal_mode=MEMORY` leaves no `-wal` to fold, so a
         # future caller opening this path with the serving configuration would silently upload an
         # incomplete file. Paying a new correctness coupling for an unmeasurable gain is the wrong
         # trade; if the concurrency claim is ever measured on the rig, this becomes worth it again.
         {:ok, conn} <- Connection.open(dest_path) do
      try do
        Enum.reduce_while(chain, :ok, fn step, :ok ->
          {version, statements, transform} = normalize_step(step)

          case replay(conn, version, statements, transform, shard_id) do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      after
        Connection.close(conn)
      end
    end
  end

  # Resolve every transform BEFORE copying a file or opening a connection. A rollout that is going
  # to fail on an unregistered module should fail before it has done any work, not after the copy.
  defp validate_transforms(chain, shard_id) do
    chain
    |> Enum.map(&normalize_step/1)
    |> Enum.filter(fn {_v, _s, transform} -> transform not in [nil, ""] end)
    |> case do
      [] ->
        :ok

      steps ->
        cond do
          is_nil(shard_id) ->
            {:error, {:transform_requires_shard_id, Enum.map(steps, &elem(&1, 0))}}

          true ->
            Enum.reduce_while(steps, :ok, fn {version, _s, transform}, :ok ->
              case Transform.resolve(transform) do
                {:ok, module} ->
                  if Transform.valid?(module),
                    do: {:cont, :ok},
                    else: {:halt, {:error, {:invalid_transform, version, transform}}}

                {:error, reason} ->
                  {:halt, {:error, {reason, version, transform}}}
              end
            end)
        end
    end
  end

  defp normalize_step({version, statements}), do: {version, statements, nil}
  defp normalize_step({version, statements, transform}), do: {version, statements, transform}

  # Runs the version's per-shard transform inside the open transaction (#26). `nil` is the
  # overwhelmingly common case — a DDL-only version — and costs one pattern match.
  #
  # The module was already resolved and validated by `validate_transforms/2` before any file was
  # copied; re-resolving here rather than threading the module through keeps the step tuple a plain
  # data shape (it comes from a Postgres row) at the cost of one list lookup per version.
  defp run_transform(_conn, transform, _shard_id, _version) when transform in [nil, ""], do: :ok

  defp run_transform(conn, transform, shard_id, version) do
    with {:ok, module} <- Transform.resolve(transform) do
      case module.run(conn, shard_id) do
        :ok ->
          :ok

        {:error, reason} ->
          {:error, {:transform_failed, version, transform, reason}}

        other ->
          # A transform that returns something else has a bug, and treating an unrecognized value
          # as success would commit a half-done backfill.
          {:error, {:transform_bad_return, version, transform, other}}
      end
    else
      {:error, reason} -> {:error, {reason, version, transform}}
    end
  rescue
    e -> {:error, {:transform_raised, version, transform, Exception.message(e)}}
  end

  defp copy_file(source_path, dest_path) do
    File.mkdir_p!(Path.dirname(dest_path))
    File.cp(source_path, dest_path)
  end

  defp replay(conn, version, statements, transform, shard_id) do
    result =
      with :ok <- Connection.exec(conn, "BEGIN"),
           :ok <- replay_each(conn, statements),
           # INSIDE the transaction, AFTER the DDL: the backfill needs the columns the DDL just
           # added, and a transform that failed must roll back the DDL with it rather than leaving
           # the shard with a new column and no data in it.
           :ok <- run_transform(conn, transform, shard_id, version),
           :ok <- Connection.exec(conn, "COMMIT") do
        :ok
      else
        {:error, _reason} = error ->
          Connection.exec(conn, "ROLLBACK")
          error
      end

    with :ok <- result,
         # Not transactional, so after the commit: stamp the version, then fold the
         # WAL into the main file so the copy is a complete single file to flush.
         :ok <- Connection.exec(conn, "PRAGMA user_version = #{version}"),
         :ok <- Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)") do
      :ok
    end
  end

  # Statements arrive as `{sql, args}`. Django sends PARAMETERIZED SQL — its bookkeeping row is
  # `INSERT INTO django_migrations … VALUES (?, ?, ?)` with the values carried separately — so
  # running the text alone through `Connection.exec/2` bound NULL and every replay died on
  # `NOT NULL constraint failed: django_migrations.app`, rolling back the whole copy. Since every
  # Django migration ends with that row, NO captured migration could be replayed onto a tenant.
  #
  # The values are BOUND, never substituted into the SQL: interpolating them would be the
  # injection/quoting hazard AGENTS.md forbids, and a migration name is attacker-influenceable (it
  # is a filename). `Connection.query/4` binds; a release captured before args were stored yields
  # `[]`, which behaves exactly as before.
  defp replay_each(conn, statements) do
    Enum.reduce_while(statements, :ok, fn {sql, args}, :ok ->
      # No bare `:ok` clause: `Connection.query/4` is `{:ok, map()} | {:error, term()}` and never
      # returns one. (`Connection.exec/2` does, which is where the habit came from.)
      case Connection.query(conn, sql, args) do
        {:ok, _result} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
