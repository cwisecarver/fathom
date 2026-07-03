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
  def migrate(source_path, dest_path, version, statements) do
    with :ok <- copy_file(source_path, dest_path),
         {:ok, conn} <- Connection.open(dest_path) do
      try do
        replay(conn, version, statements)
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

  defp replay_each(conn, statements) do
    Enum.reduce_while(statements, :ok, fn sql, :ok ->
      case Connection.exec(conn, sql) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
