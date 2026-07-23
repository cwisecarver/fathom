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
    with {:ok, stmt} <- cached_stmt(conn, sql) do
      # The statement is owned by this connection's cache — reset it (not release) when done,
      # including on a bind/collect error or a row-cap abort. sqlite3_reset ends the statement's
      # execution (releasing its locks / read-transaction hold, which release/finalize used to
      # do) while keeping the compiled plan for the next execution of the same SQL.
      try do
        with :ok <- Sqlite3.bind(stmt, args),
             {:ok, columns} <- Sqlite3.columns(conn, stmt),
             {:ok, rows, row_count} <- collect(conn, stmt, row_cap()) do
          {:ok,
           %{
             columns: columns,
             rows: rows,
             row_count: row_count,
             num_changes: changes(conn),
             last_insert_rowid: last_rowid(conn)
           }}
        end
      after
        Sqlite3.reset(stmt)
      end
    end
  end

  # --- prepared-statement cache (review 2026-07-23 #1) -------------------------------------
  #
  # Every execution used to pay a full sqlite3_prepare_v2 parse+plan through the NIF —
  # 5–50 µs, comparable to or larger than the step cost of the point reads/writes that
  # dominate — even though ORM clients (Django) replay identical parameterized SQL on a
  # stream for its whole life. Cache prepared statements per connection, keyed by the exact
  # SQL string, LRU-capped. prepare_v2 statements auto-recompile on schema change, so no
  # invalidation logic is needed.
  #
  # The cache lives in the OWNING process's dictionary keyed by the conn ref: a connection
  # is single-owner by design (one per Hrana stream, used only by the stream process), so
  # this is race-free, dies with the process (statement NIF resources are GC-finalized, and
  # close is sqlite3_close_v2, which defers until they are), and keeps the executor-handle
  # shape unchanged. `close/1` releases the cached statements explicitly for the orderly path.
  @stmt_cache_cap 64

  defp cached_stmt(conn, sql) do
    key = {__MODULE__, :stmt_cache, conn}
    {seq, cache} = Process.get(key, {0, %{}})

    case cache do
      %{^sql => {stmt, _stamp}} ->
        Process.put(key, {seq + 1, Map.put(cache, sql, {stmt, seq + 1})})
        {:ok, stmt}

      _ ->
        with {:ok, stmt} <- Sqlite3.prepare(conn, sql) do
          cache = maybe_evict(conn, cache)
          Process.put(key, {seq + 1, Map.put(cache, sql, {stmt, seq + 1})})
          {:ok, stmt}
        end
    end
  end

  # At the cap, release the least-recently-used statement to make room. O(cap) scan, paid
  # only on an over-cap miss — a workload cycling >64 distinct statements per stream is
  # re-preparing anyway, exactly as before the cache.
  defp maybe_evict(conn, cache) when map_size(cache) >= @stmt_cache_cap do
    {lru_sql, {stmt, _stamp}} = Enum.min_by(cache, fn {_sql, {_stmt, stamp}} -> stamp end)
    Sqlite3.release(conn, stmt)
    Map.delete(cache, lru_sql)
  end

  defp maybe_evict(_conn, cache), do: cache

  defp purge_stmt_cache(conn) do
    case Process.delete({__MODULE__, :stmt_cache, conn}) do
      {_seq, cache} -> Enum.each(cache, fn {_sql, {stmt, _}} -> Sqlite3.release(conn, stmt) end)
      nil -> :ok
    end
  end

  # Resolve `:query_max_rows` ONCE per query — the old shape re-read the app env inside every
  # collect batch (a 200k-row result did ~4k redundant get_envs in one query's hot loop).
  # Normalized here so collect's per-batch check is a bare integer comparison.
  defp row_cap do
    case Application.get_env(:fathom, :query_max_rows) do
      cap when is_integer(cap) and cap > 0 -> cap
      _ -> nil
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

    # Always stop the watchdog, even if fun raises (Sqlite3.bind raises ArgumentError on a bad bind
    # value — the reason query/3 has a rescue). Without try/after the raise unwinds before the
    # {:done, ref} send, leaving the watchdog alive to interrupt a LATER statement reusing this
    # connection `ms` later — a spurious FILO_QUERY_TIMEOUT on a healthy query (expert review
    # 2026-07-18 #13). The raise then propagates to query/3's rescue as before.
    result =
      try do
        fun.()
      after
        send(watchdog, {:done, ref})
      end

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
  Introspects a statement WITHOUT running it (the Hrana `describe` request, #34): returns its result
  column names and its bound-parameter count as `%{params: [nil, …], cols: [name, …]}`. Parameters
  are reported positionally (`nil` each — exqlite exposes the count, not the names), which is what a
  libSQL client needs to know how many values to bind. The prepared statement is always released.
  """
  @spec describe(reference(), String.t()) ::
          {:ok, %{params: [nil], cols: [String.t()]}} | {:error, term()}
  def describe(conn, sql) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql) do
      try do
        cols = with {:ok, c} <- Sqlite3.columns(conn, stmt), do: c, else: (_ -> [])

        count =
          case Sqlite3.bind_parameter_count(stmt) do
            n when is_integer(n) and n >= 0 -> n
            _ -> 0
          end

        {:ok, %{params: List.duplicate(nil, count), cols: cols}}
      after
        Sqlite3.release(conn, stmt)
      end
    end
  rescue
    e in ArgumentError -> {:error, Exception.message(e)}
  end

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

  @doc """
  Closes the connection, releasing this process's cached prepared statements first.
  (Statements cached by a process that died without closing are NIF resources —
  they GC-finalize, and `sqlite3_close_v2` defers the close until they do.)
  """
  @spec close(reference()) :: :ok
  def close(conn) do
    purge_stmt_cache(conn)
    Sqlite3.close(conn)
    :ok
  end

  # exqlite's default multi_step chunk is 50, so a 200k-row result cost 4,000 dirty-scheduler
  # NIF round-trips; 500 amortizes the per-batch dispatch while a chunk stays cheap to build
  # (review 2026-07-23 #12). The row cap below is enforced exactly (including the final batch),
  # so the chunk size never changes what a capped query can materialize.
  @multi_step_chunk 500

  # Assemble the result in O(R), not O(R²). `multi_step` returns rows in batches; the old
  # `acc ++ rows` copied the whole accumulator on every batch, so a large result cost
  # O(R²/batch) — measured at 1385 ms for 200k rows (see connection_test.exs), pathological
  # for a wide SELECT (TPC-C / the isolation checks hit it). Keep the batches in reverse
  # arrival order (O(1) prepend), then reverse + one-level concat once at the end (O(R), ~tens
  # of ms at 200k). `Enum.concat/1` joins the list of row-batches without recursing into a row
  # (a row is itself a list of column values), so row shape + order are preserved.
  #
  # Counts unconditionally and returns `{:ok, rows, count}` — the per-batch length sums to the
  # same O(R) the executor's `length(rows)` walk paid AGAIN on the materialized list, so the
  # count is now computed once here and threaded through (review 2026-07-23 #16). `cap` is the
  # pre-resolved `:query_max_rows` (nil = uncapped): bound how much a single query can
  # materialize in BEAM memory (a `SELECT *` on a large table, expert review 2026-07-14 #26),
  # erroring instead of OOMing the node. Checked on EVERY batch including the final one, so
  # enforcement is exact regardless of @multi_step_chunk.
  defp collect(conn, stmt, cap, count \\ 0, batches \\ []) do
    case Sqlite3.multi_step(conn, stmt, @multi_step_chunk) do
      {:rows, rows} ->
        count = count + length(rows)

        if is_integer(cap) and count > cap,
          do: {:error, {:too_many_rows, cap}},
          else: collect(conn, stmt, cap, count, [rows | batches])

      {:done, rows} ->
        count = count + length(rows)

        if is_integer(cap) and count > cap,
          do: {:error, {:too_many_rows, cap}},
          else: {:ok, [rows | batches] |> Enum.reverse() |> Enum.concat(), count}

      :busy ->
        {:error, :busy}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp timeout_ms, do: Application.get_env(:fathom, :query_timeout_ms)

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
