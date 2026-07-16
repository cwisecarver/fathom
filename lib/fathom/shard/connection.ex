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
         :ok <- maybe_foreign_keys(conn),
         :ok <- maybe_max_page_count(conn) do
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

  # Enforce a per-shard size cap (expert review 2026-07-14 #19). "Limited dataset per shard" is
  # fathom's premise but was never enforced, so one runaway tenant (an app bug, an unbounded log
  # table) could grow to GBs — inflating every whole-shard cost (each dirty flush is a full-file
  # PUT, cold-open pulls the whole body, eviction/drain/warm-standby all copy it). `max_page_count`
  # is per-connection (not persisted), so set it on every open from `:shard_max_page_count` (pages;
  # size = pages × page_size, default 4096B). A write past the cap fails `SQLITE_FULL` (mapped to the
  # `SQLITE_FULL` Hrana code) — exactly the brake a platform wants. Unset ⇒ unlimited (no cap).
  defp maybe_max_page_count(conn) do
    case Application.get_env(:fathom, :shard_max_page_count) do
      n when is_integer(n) and n > 0 -> Sqlite3.execute(conn, "PRAGMA max_page_count=#{n}")
      _ -> :ok
    end
  end

  @doc """
  Runs `sql` (with native-value `args`) on `conn`, returning native Elixir values
  in `{:ok, %{columns, rows, num_changes, last_insert_rowid}}` or `{:error, _}`.
  """
  @spec query(reference(), String.t(), list()) :: {:ok, map()} | {:error, term()}
  def query(conn, sql, args) do
    # Statement deadline (expert review 2026-07-14 #26): when `:query_timeout_ms` is set, a runaway
    # query (missing-index full scan) is interrupted so it can't pin memory and keep the shard busy
    # (blocking eviction/drain/handoff). Only armed when configured — unset ⇒ no watchdog, no
    # overhead (the default, matching fathom's other protective knobs).
    case timeout_ms() do
      nil ->
        do_query(conn, sql, args)

      ms when is_integer(ms) and ms > 0 ->
        with_deadline(conn, ms, fn -> do_query(conn, sql, args) end)

      _ ->
        do_query(conn, sql, args)
    end
  rescue
    e in ArgumentError -> {:error, Exception.message(e)}
  end

  defp do_query(conn, sql, args) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql) do
      # Always release the prepared statement — including on a bind/collect error or a row-cap
      # abort — so a failed query never leaks a statement handle on the connection.
      try do
        with :ok <- Sqlite3.bind(stmt, args),
             {:ok, columns} <- Sqlite3.columns(conn, stmt),
             {:ok, rows} <- collect(conn, stmt) do
          {:ok,
           %{
             columns: columns,
             rows: rows,
             num_changes: changes(conn),
             last_insert_rowid: last_rowid(conn)
           }}
        end
      after
        Sqlite3.release(conn, stmt)
      end
    end
  end

  # Run `fun` (the query) in THIS process while a cheap watchdog process interrupts `conn` if the
  # deadline passes — the running `multi_step` is a blocking dirty NIF, so only another process can
  # interrupt it. A completed query cancels the watchdog; a spurious late interrupt on an
  # already-finished statement is a no-op, so a successful result is returned even then (we only
  # surface `:query_timeout` when the interrupt actually errored the query).
  defp with_deadline(conn, ms, fun) do
    parent = self()
    ref = make_ref()

    watchdog =
      spawn(fn ->
        receive do
          {:done, ^ref} -> :ok
        after
          ms ->
            Sqlite3.interrupt(conn)
            send(parent, {:timed_out, ref})
        end
      end)

    result = fun.()
    send(watchdog, {:done, ref})

    timed_out? =
      receive do
        {:timed_out, ^ref} -> true
      after
        0 -> false
      end

    case {timed_out?, result} do
      {true, {:error, _}} -> {:error, :query_timeout}
      _ -> result
    end
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
  defp collect(conn, stmt, count \\ 0, batches \\ []) do
    case Sqlite3.multi_step(conn, stmt) do
      {:rows, rows} ->
        # Max-result-rows cap (expert review 2026-07-14 #26): bound how much a single query can
        # materialize in BEAM memory (a `SELECT *` on a large table), erroring instead of OOMing
        # the node. Only counts when `:query_max_rows` is set — no cap ⇒ no per-batch length walk.
        case max_rows() do
          cap when is_integer(cap) and cap > 0 ->
            count = count + length(rows)

            if count > cap,
              do: {:error, {:too_many_rows, cap}},
              else: collect(conn, stmt, count, [rows | batches])

          _ ->
            collect(conn, stmt, count, [rows | batches])
        end

      {:done, rows} ->
        {:ok, [rows | batches] |> Enum.reverse() |> Enum.concat()}

      :busy ->
        {:error, :busy}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp timeout_ms, do: Application.get_env(:fathom, :query_timeout_ms)
  defp max_rows, do: Application.get_env(:fathom, :query_max_rows)

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
