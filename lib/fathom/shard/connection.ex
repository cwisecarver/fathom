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

  @doc """
  Opens a connection to the shard file at `path` (WAL, synchronous=FULL, 5s busy
  timeout, and — unless `config :fathom, :foreign_keys` is false — foreign keys ON).
  """
  @spec open(Path.t()) :: {:ok, reference()} | {:error, term()}
  def open(path) do
    File.mkdir_p!(Path.dirname(path))

    with {:ok, conn} <- Sqlite3.open(path),
         :ok <- Sqlite3.execute(conn, "PRAGMA journal_mode=WAL"),
         # synchronous=FULL fsyncs the WAL on every commit, so a committed write survives a node
         # crash (per-commit local durability, tightening the local RPO below the S3 flush
         # interval). It is ~free for fathom's sharded model: throughput is wire/executor-bound,
         # not commit-bound, and the per-shard fsyncs parallelize across the fan-out — measured
         # FULL vs NORMAL is within noise (node_tps 4167→4140, TPC-C tpmC unchanged), whereas a
         # single-DB engine pays ~2–3× for the same guarantee. See
         # docs/reviews/competitive-oltp-2026-07-10.md.
         :ok <- Sqlite3.execute(conn, "PRAGMA synchronous=FULL"),
         :ok <- Sqlite3.execute(conn, "PRAGMA busy_timeout=5000"),
         :ok <- maybe_foreign_keys(conn) do
      {:ok, conn}
    end
  end

  # SQLite defaults foreign_keys=OFF, but Django (≥2.2) assumes ON and enforces it via a
  # per-connection `PRAGMA foreign_keys = ON` in get_new_connection (expert review 2026-07-14
  # #2). A remote client can't be relied on to replay that on every stream — and even when it
  # does, the pragma is scoped to the current Hrana stream's connection, so a transparently
  # re-created stream (HTTP idle-expiry + reconnect) would silently come up with enforcement OFF.
  # Default it ON here, server-side, so on_delete=CASCADE/PROTECT and FK integrity hold regardless
  # of client init. A migration needing it off can still send `PRAGMA foreign_keys=OFF` for its own
  # stream (that overrides this connection's setting); set `:foreign_keys` false to flip the default.
  defp maybe_foreign_keys(conn) do
    if Application.get_env(:fathom, :foreign_keys, true),
      do: Sqlite3.execute(conn, "PRAGMA foreign_keys=ON"),
      else: :ok
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

  @doc """
  Whether `conn` is in **autocommit** mode — i.e. no explicit transaction is open
  (`sqlite3_get_autocommit` via exqlite's `transaction_status`). Fails safe to `true`
  (the historical default) if the status can't be read.
  """
  @spec autocommit?(reference()) :: boolean()
  def autocommit?(conn) do
    case Sqlite3.transaction_status(conn) do
      {:ok, :idle} -> true
      {:ok, :transaction} -> false
      _ -> true
    end
  end

  @doc "Closes the connection."
  @spec close(reference()) :: :ok
  def close(conn) do
    Sqlite3.close(conn)
    :ok
  end

  # Assemble the result in O(R), not O(R²). `multi_step` returns rows in batches; the old
  # `acc ++ rows` copied the whole accumulator on every batch, so a large result cost
  # O(R²/batch) — measured at 1385 ms for 200k rows (see connection_test.exs), pathological
  # for a wide SELECT (TPC-C / the isolation checks hit it). Keep the batches in reverse
  # arrival order (O(1) prepend), then reverse + one-level concat once at the end (O(R), ~tens
  # of ms at 200k). `Enum.concat/1` joins the list of row-batches without recursing into a row
  # (a row is itself a list of column values), so row shape + order are preserved.
  defp collect(conn, stmt, batches \\ []) do
    case Sqlite3.multi_step(conn, stmt) do
      {:rows, rows} -> collect(conn, stmt, [rows | batches])
      {:done, rows} -> {:ok, [rows | batches] |> Enum.reverse() |> Enum.concat()}
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
