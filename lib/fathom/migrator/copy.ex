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
  alias Fathom.Shard.Connection

  @doc """
  Copies `source_path` to `dest_path`, replays `statements` onto the copy, and
  stamps `PRAGMA user_version = version`. Returns `:ok` or `{:error, reason}`
  (leaving `dest_path` at the old schema on a replay error).
  """
  @spec migrate(Path.t(), Path.t(), non_neg_integer(), [String.t()]) :: :ok | {:error, term()}
  def migrate(source_path, dest_path, version, statements),
    do: migrate_chain(source_path, dest_path, [{version, statements}])

  @doc """
  Copies `source_path` to `dest_path` and replays a CHAIN of versions in order
  (round-2 #9: a multi-step laggard must apply every intermediate version's
  statements, not just the target's). Each `{version, statements}` step runs in its
  own transaction and stamps its `user_version` after commit, so a failure
  mid-chain leaves the copy at the last fully-applied version — never half a step.
  Returns `:ok` or `{:error, reason}`.
  """
  @spec migrate_chain(Path.t(), Path.t(), [{non_neg_integer(), [String.t()]}]) ::
          :ok | {:error, term()}
  def migrate_chain(source_path, dest_path, chain) do
    with :ok <- copy_file(source_path, dest_path),
         {:ok, conn} <- Connection.open(dest_path) do
      try do
        Enum.reduce_while(chain, :ok, fn {version, statements}, :ok ->
          case replay(conn, version, statements) do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      after
        Connection.close(conn)
      end
    end
  end

  defp copy_file(source_path, dest_path) do
    File.mkdir_p!(Path.dirname(dest_path))
    File.cp(source_path, dest_path)
  end

  defp replay(conn, version, statements) do
    result =
      with :ok <- Connection.exec(conn, "BEGIN"),
           :ok <- replay_each(conn, statements),
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
      case Connection.query(conn, sql, args) do
        {:ok, _result} -> {:cont, :ok}
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
