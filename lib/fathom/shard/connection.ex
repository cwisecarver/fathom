defmodule Fathom.Shard.Connection do
  @moduledoc """
  A single SQLite connection to a shard's database file.

  Each Hrana stream opens its own connection (via `Fathom.ShardExecutor`), so
  per-stream transactions are isolated: SQLite/WAL gives every connection its own
  write transaction and read snapshot, and concurrent writers serialize on
  `busy_timeout`. These are plain functions — the connection is owned and used by
  the calling process throughout its life.
  """
  alias Exqlite.Sqlite3

  @doc "Opens a connection to the shard file at `path` (WAL, 5s busy timeout)."
  @spec open(Path.t()) :: {:ok, reference()} | {:error, term()}
  def open(path) do
    File.mkdir_p!(Path.dirname(path))

    with {:ok, conn} <- Sqlite3.open(path),
         :ok <- Sqlite3.execute(conn, "PRAGMA journal_mode=WAL"),
         :ok <- Sqlite3.execute(conn, "PRAGMA busy_timeout=5000") do
      {:ok, conn}
    end
  end

  @doc """
  Runs `sql` (with native-value `args`) on `conn`, returning native Elixir values
  in `{:ok, %{columns, rows, num_changes, last_insert_rowid}}` or `{:error, _}`.
  """
  @spec query(reference(), String.t(), list()) :: {:ok, map()} | {:error, term()}
  def query(conn, sql, args) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(stmt, args),
         {:ok, columns} <- Sqlite3.columns(conn, stmt),
         {:ok, rows} <- collect(conn, stmt) do
      Sqlite3.release(conn, stmt)

      {:ok,
       %{
         columns: columns,
         rows: rows,
         num_changes: changes(conn),
         last_insert_rowid: last_rowid(conn)
       }}
    end
  rescue
    e in ArgumentError -> {:error, Exception.message(e)}
  end

  @doc """
  Executes raw `sql` with no result rows — for DDL and migration replay (a single
  statement, or several separated by `;`). Use `query/3` for anything returning
  rows or taking bound args.
  """
  @spec exec(reference(), String.t()) :: :ok | {:error, term()}
  def exec(conn, sql), do: Sqlite3.execute(conn, sql)

  @doc "Closes the connection."
  @spec close(reference()) :: :ok
  def close(conn) do
    Sqlite3.close(conn)
    :ok
  end

  defp collect(conn, stmt, acc \\ []) do
    case Sqlite3.multi_step(conn, stmt) do
      {:rows, rows} -> collect(conn, stmt, acc ++ rows)
      {:done, rows} -> {:ok, acc ++ rows}
      :busy -> {:error, :busy}
      {:error, reason} -> {:error, reason}
    end
  end

  defp changes(conn) do
    case Sqlite3.changes(conn) do
      {:ok, n} -> n
      n when is_integer(n) -> n
    end
  end

  defp last_rowid(conn) do
    case Sqlite3.last_insert_rowid(conn) do
      {:ok, id} -> id
      id when is_integer(id) -> id
    end
  end
end
