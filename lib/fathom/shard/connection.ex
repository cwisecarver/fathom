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

  ## Options

    * `:tenant?` — this handle will execute **client-supplied** SQL, so deny
      `ATTACH`/`DETACH` at the SQLite authorizer (expert review 2026-08-01 #1).
      Defaults to `false`: fathom's own callers (the coordinator's durability
      snapshot, the migration engine's replay, the bench/scale harnesses) are
      trusted and MUST NOT get the authorizer — see `maybe_authorizer/2`.
    * `:scope` — `:rw` (default) or `:ro`. A `:ro` handle is opened
      `mode: :readonly` so SQLite itself refuses every write (#7).

  Only `Fathom.ShardExecutor` passes `tenant?: true`; it is the one path that
  runs SQL fathom did not author.
  """
  @spec open(Path.t(), keyword()) :: {:ok, reference()} | {:error, term()}
  def open(path, opts \\ []) do
    File.mkdir_p!(Path.dirname(path))

    tenant? = Keyword.get(opts, :tenant?, false)
    scope = Keyword.get(opts, :scope, :rw)

    case open_handle(path, scope) do
      {:ok, conn, :readonly} -> configure_readonly(conn, tenant?)
      {:ok, conn, :readwrite} -> configure_readwrite(conn, tenant?, scope)
      {:error, reason} -> {:error, reason}
    end
  end

  # A `:ro` stream gets a genuinely read-only SQLite handle: the scope check in
  # ShardExecutor is a leading-keyword heuristic and was bypassable by a `/* */` prefix, a
  # CTE-prefixed DML, or a durable `PRAGMA` assignment (expert review 2026-08-01 #7).
  # SQLite's own enforcement has no such holes.
  #
  # `PRAGMA query_only=ON` is NOT used as the primary mechanism: it is connection-local and
  # the tenant can simply send `PRAGMA query_only=OFF` (verified). It is only the fallback
  # for the narrow case where a read-only open cannot proceed — a database whose `-wal`
  # needs recovery, which requires write access. That fallback is still safe because
  # `ShardExecutor` refuses `query_only` as a pragma assignment.
  defp open_handle(path, :ro) do
    case Sqlite3.open(path, mode: :readonly) do
      {:ok, conn} -> {:ok, conn, :readonly}
      {:error, _} -> with {:ok, conn} <- Sqlite3.open(path), do: {:ok, conn, :readwrite}
    end
  end

  defp open_handle(path, _scope) do
    with {:ok, conn} <- Sqlite3.open(path), do: {:ok, conn, :readwrite}
  end

  # A read-only handle takes only the connection-local pragmas. The file-level ones
  # (`journal_mode`, `wal_autocheckpoint`) describe writes this handle cannot perform, and
  # `max_page_count` is a growth cap on a connection that cannot grow the file.
  defp configure_readonly(conn, tenant?) do
    with :ok <- Sqlite3.set_busy_timeout(conn, 5000),
         :ok <- maybe_foreign_keys(conn),
         :ok <- maybe_cache_size(conn),
         :ok <- maybe_authorizer(conn, tenant?) do
      {:ok, conn}
    end
  end

  defp configure_readwrite(conn, tenant?, scope) do
    with :ok <- configure(conn),
         :ok <- maybe_authorizer(conn, tenant?),
         :ok <- maybe_query_only(conn, scope) do
      {:ok, conn}
    end
  end

  # Deny ATTACH/DETACH on any handle that runs client SQL. Without this, one statement —
  # `ATTACH DATABASE '<data_dir>/<victim>.db' AS v` — gave a tenant full read AND write
  # access to every co-resident tenant's shard file, bypassing the `:ro` scope, the write
  # fence, `:block_tenant_ddl`, and the single-writer lease (expert review 2026-08-01 #1,
  # found independently by two panels and verified by execution). The victim's path is not
  # even guessed: `PRAGMA database_list` returns the attacker's own absolute path and every
  # shard is a sibling in one flat directory.
  #
  # SCOPED TO TENANT HANDLES ON PURPOSE. SQLite implements `VACUUM INTO` as an internal
  # ATTACH, so denying `:attach` also denies `VACUUM INTO` ("authorization denied", verified)
  # — and `Fathom.Shard.snapshot/2` is a `VACUUM INTO` on a `Connection`. Setting the
  # authorizer unconditionally in `open/1` would therefore have broken EVERY durability
  # flush. That is a happy accident on the tenant side: it also closes the `VACUUM INTO
  # '<any path>'` arbitrary-file-write primitive (#8) at the engine, not just at the
  # statement gate.
  defp maybe_authorizer(_conn, false), do: :ok
  defp maybe_authorizer(conn, true), do: Sqlite3.set_authorizer(conn, [:attach, :detach])

  # Fallback belt for a `:ro` scope that could not get a read-only handle (see open_handle/2).
  defp maybe_query_only(conn, :ro), do: Sqlite3.execute(conn, "PRAGMA query_only=ON")
  defp maybe_query_only(_conn, _scope), do: :ok

  defp configure(conn) do
    with :ok <- Sqlite3.execute(conn, "PRAGMA journal_mode=WAL"),
         # synchronous=FULL fsyncs the WAL on every commit, so a committed write survives a node
         # crash (per-commit local durability, tightening the local RPO below the S3 flush
         # interval). It is ~free for fathom's sharded model: throughput is wire/executor-bound,
         # not commit-bound, and the per-shard fsyncs parallelize across the fan-out — measured
         # FULL vs NORMAL is within noise (node_tps 4167→4140, TPC-C tpmC unchanged), whereas a
         # single-DB engine pays ~2–3× for the same guarantee. See
         # docs/reviews/competitive-oltp-2026-07-10.md.
         :ok <- Sqlite3.execute(conn, "PRAGMA synchronous=FULL"),
         # NOT `PRAGMA busy_timeout` (expert review 2026-07-24 #1): the pragma routes to
         # sqlite3_busy_timeout(), which REPLACES exqlite's custom busy handler with SQLite's
         # default — a bare sqlite3OsSleep loop observing neither the interrupt flag nor exqlite's
         # cancel flag. That made a lock wait uninterruptible: `:query_timeout_ms` bounded VDBE
         # execution only, so a query blocked on the write lock slept the full 5s pinning a
         # dirty-IO scheduler thread (all SQLite NIFs are DIRTY_JOB_IO_BOUND, and the pool is
         # +SDio-sized), starving every co-located tenant. set_busy_timeout/2 updates the value the
         # CUSTOM handler reads, keeping the wait cancellable (see with_deadline's Sqlite3.cancel).
         # exqlite's handler walks the SAME delay ladder as SQLite's default, so the wait timing
         # is unchanged; it just also polls the cancel flag and the caller's liveness.
         :ok <- Sqlite3.set_busy_timeout(conn, 5000),
         # Raise the autocheckpoint threshold well above SQLite's 1000-frame (~4 MB) default
         # (expert review 2026-07-24 #4). An autocheckpoint runs INLINE inside the committing
         # tenant statement that crosses it — a p99/p999 spike billed to an arbitrary client query,
         # with a main-database fsync under synchronous=FULL. The coordinator now checkpoints
         # PASSIVE after each durability snapshot (see Fathom.Shard.snapshot/2), where no client is
         # waiting, so in steady state it wins the race and no tenant query ever checkpoints.
         #
         # Raised, NOT disabled: at 0 a wedged coordinator would let the WAL grow unbounded. This
         # keeps a backstop at ~4× the default.
         :ok <- Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=4000"),
         :ok <- maybe_foreign_keys(conn),
         :ok <- maybe_cache_size(conn),
         :ok <- maybe_max_page_count(conn) do
      :ok
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
  # Make the per-connection page cache an explicit, tunable ceiling (expert review 2026-07-24 #29).
  # SQLite's default is -2000, i.e. "up to ~2 MiB of page cache PER CONNECTION", and fathom holds one
  # connection per Hrana stream for the stream's life. The measured density regimes (~16 KiB idle,
  # ~220 KiB served, ~640 KiB served-with-data) are all workloads whose touched page set is small,
  # so none of them ever approached that ceiling — which means those figures are a sample of a
  # distribution whose tail is 2 MiB per held stream, not a bound. At 30k held streams that tail is
  # ~60 GB.
  #
  # Every other resource here has an explicit cap for exactly this reason — :max_open_shards,
  # :max_checkouts_per_shard, :query_max_rows, :shard_max_page_count, :query_timeout_ms. Page cache
  # was the one that was missed: one tenant scanning on 50 held streams silently claims 100 MB and
  # nothing observes or bounds it.
  #
  # The DEFAULT IS 2000 — exactly SQLite's effective default — so this ships as a no-op and changes
  # no measured number. It is a knob and a declared ceiling, not a reduction: lowering it trades RAM
  # for page re-reads on scan-heavy tenants, which is a deployment-shaped decision, not one to make
  # blind. Negative value = KiB (SQLite's own convention); a positive value would mean *pages*, so
  # the sign is forced here rather than left to the operator.
  defp maybe_cache_size(conn) do
    case Application.get_env(:fathom, :shard_cache_size_kb, 2000) do
      kb when is_integer(kb) and kb > 0 -> Sqlite3.execute(conn, "PRAGMA cache_size=-#{kb}")
      _ -> :ok
    end
  end

  # 1,048,576 pages × the 4096B default page size = 4 GiB, ~1 GiB under the 5 GiB S3 single-PUT
  # ceiling (`Fathom.Shard.Storage.S3.@max_single_put`).
  @default_max_page_count 1_048_576

  # Expert review 2026-07-24 #37. This used to be UNSET by default, which made the first defence
  # against a runaway tenant a FLUSH failure rather than a write failure — and those fail in
  # opposite directions:
  #
  #   * Unset: the shard keeps accepting and ACKNOWLEDGING writes past 5 GiB, then can never upload
  #     again. It stays permanently dirty, retries every interval forever, and its RPO becomes
  #     unbounded — the exact failure docs/durability.md names ("this bound holds only while flushes
  #     succeed"). The same ceiling silently disables snapshot, fork_shard and retain. There is no
  #     operator remedy: the data is acked and cannot be made durable.
  #   * Capped: the write is rejected with SQLITE_FULL and never acknowledged. Nothing acknowledged
  #     goes unflushed. Strictly safer.
  #
  # Defaulting it is safe for a shard that is ALREADY over the cap: SQLite will not set
  # max_page_count below a database's current page count, so an oversized shard keeps serving reads
  # and writes within its existing size and merely stops growing — it is not bricked at open.
  #
  # A cap also matches the stated premise. "Limited dataset per shard" is what makes a
  # million-shard fleet work: every whole-shard cost (a dirty flush is a full-file PUT, cold-open
  # pulls the whole body, eviction/drain/warm-standby all copy it) is linear in shard size.
  # Set `SHARD_MAX_PAGE_COUNT=0` to opt out and restore the unlimited behaviour.
  defp maybe_max_page_count(conn) do
    case Application.get_env(:fathom, :shard_max_page_count, @default_max_page_count) do
      n when is_integer(n) and n > 0 -> Sqlite3.execute(conn, "PRAGMA max_page_count=#{n}")
      _ -> :ok
    end
  end

  @doc """
  Runs `sql` (with native-value `args`) on `conn`, returning native Elixir values
  in `{:ok, %{columns, rows, num_changes, last_insert_rowid}}` or `{:error, _}`.
  """
  @spec query(reference(), String.t(), list(), keyword()) :: {:ok, map()} | {:error, term()}
  def query(conn, sql, args, opts \\ []) do
    dml? = Keyword.get(opts, :dml?, true)

    # Statement deadline (expert review 2026-07-14 #26): when `:query_timeout_ms` is set, a runaway
    # query (missing-index full scan) is interrupted so it can't pin memory and keep the shard busy
    # (blocking eviction/drain/handoff). Only armed when configured — unset ⇒ no watchdog, no
    # overhead (the default, matching fathom's other protective knobs).
    case timeout_ms() do
      nil ->
        do_query(conn, sql, args, dml?)

      ms when is_integer(ms) and ms > 0 ->
        with_deadline(conn, ms, fn -> do_query(conn, sql, args, dml?) end)

      _ ->
        do_query(conn, sql, args, dml?)
    end
  rescue
    e in ArgumentError -> {:error, Exception.message(e)}
  end

  defp do_query(conn, sql, args, dml?) do
    with {:ok, stmt, columns} <- cached_stmt(conn, sql) do
      # The statement is owned by this connection's cache — reset it (not release) when done,
      # including on a bind/collect error or a row-cap abort. sqlite3_reset ends the statement's
      # execution (releasing its locks / read-transaction hold, which release/finalize used to
      # do) while keeping the compiled plan for the next execution of the same SQL.
      try do
        with :ok <- Sqlite3.bind(stmt, args),
             {:ok, rows, row_count} <- collect(conn, stmt, row_cap()) do
          {:ok,
           %{
             columns: columns,
             rows: rows,
             row_count: row_count,
             num_changes: changes(conn),
             # Only fetch the rowid when the caller can actually use it (expert review
             # 2026-07-24 #18). Sqlite3.last_insert_rowid/1 is ERL_NIF_DIRTY_JOB_IO_BOUND — a full
             # dirty-scheduler dispatch — and ShardExecutor.to_stmt_result/2 discards it for every
             # read-only statement, which is the dominant path. Removing it cuts ~25% of the
             # dirty-IO dispatches on a plain SELECT, against the +SDio pool #2 resized.
             #
             # The predicate is exactly to_stmt_result's own `not read_only?`
             # (`read_only? = cols != [] and not dml?`), so what the caller sees is byte-identical.
             #
             # num_changes stays UNCONDITIONAL: wrote?/2 checks `changes > 0` first, and the
             # durability classifier's deliberate over-dirtying depends on it.
             last_insert_rowid: if(dml? or columns == [], do: last_rowid(conn))
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

  # Cache entries are `{canonical_sql, stmt, stamp}` (expert review 2026-07-24 #8). The key is
  # stored alongside the statement so the HIT path can re-put under the CANONICAL binary rather
  # than the caller's, which matters because of where the caller's binary comes from:
  #
  #   filo reads the whole HTTP body / WebSocket frame into one binary and Jason.decode's it.
  #   Jason extracts strings with binary_part/3, so on a body >64 bytes (every Hrana pipeline
  #   body) each extracted string is a SUB-BINARY holding a reference to the entire body. The
  #   stmt map's "sql" is taken verbatim, and ERTS copies a >=64-byte sub-binary across a process
  #   boundary as a reference, not a flatten — so stashing it as a cache key pins the whole
  #   originating request body for as long as the entry lives.
  #
  # At the 64-entry cap that is up to 64 whole request bodies pinned per stream, freed only on the
  # holder's GC. With modest 4 KiB pipeline bodies that is ~256 KiB per served shard — the same
  # order as the entire measured ~220 KiB/shard served cost. It never showed up in the density
  # numbers because it lands in :erlang.memory(:binary), not the process heap.
  #
  # :binary.copy/1 on the MISS path only, so the hot hit path pays nothing.
  defp cached_stmt(conn, sql) do
    key = {__MODULE__, :stmt_cache, conn}
    {seq, cache} = Process.get(key, {0, %{}})

    case cache do
      %{^sql => {canonical, stmt, cols, _stamp}} ->
        # Re-put under `canonical`, never the incoming sub-binary: a map that has grown past a
        # flatmap adopts the new key term, which would silently reinstate the pin.
        Process.put(key, {seq + 1, Map.put(cache, canonical, {canonical, stmt, cols, seq + 1})})
        {:ok, stmt, cols}

      _ ->
        with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
             {:ok, cols} <- Sqlite3.columns(conn, stmt) do
          canonical = :binary.copy(sql)
          cache = maybe_evict(conn, cache)
          Process.put(key, {seq + 1, Map.put(cache, canonical, {canonical, stmt, cols, seq + 1})})
          {:ok, stmt, cols}
        end
    end
  end

  # At the cap, release the least-recently-used statement to make room. O(cap) scan, paid
  # only on an over-cap miss — a workload cycling >64 distinct statements per stream is
  # re-preparing anyway, exactly as before the cache.
  defp maybe_evict(conn, cache) when map_size(cache) >= @stmt_cache_cap do
    {lru_sql, {_canonical, stmt, _cols, _stamp}} =
      Enum.min_by(cache, fn {_sql, {_canonical, _stmt, _cols, stamp}} -> stamp end)

    Sqlite3.release(conn, stmt)
    Map.delete(cache, lru_sql)
  end

  defp maybe_evict(_conn, cache), do: cache

  @doc """
  Drops this connection's cached prepared statements (and their cached column lists).

  Must be called after DDL on the connection (expert review 2026-07-24 #17): `prepare_v2`
  transparently recompiles a statement on a schema change, so the STATEMENT stays valid — but the
  column list cached alongside it does not. After `ALTER TABLE t ADD COLUMN c`, a cached
  `SELECT * FROM t` would keep reporting the pre-ALTER columns while returning post-ALTER rows.
  """
  @spec purge_statements(reference()) :: :ok
  def purge_statements(conn), do: purge_stmt_cache(conn)

  defp purge_stmt_cache(conn) do
    case Process.delete({__MODULE__, :stmt_cache, conn}) do
      {_seq, cache} ->
        Enum.each(cache, fn {_sql, {_canonical, stmt, _cols, _}} ->
          Sqlite3.release(conn, stmt)
        end)

      nil ->
        :ok
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

  # Run `fun` (the query) in THIS process while a watchdog process interrupts `conn` if the
  # deadline passes — the running `multi_step` is a blocking dirty NIF, so only another process can
  # interrupt it. A completed query disarms the watchdog; a spurious late interrupt on an
  # already-finished statement is a no-op, so a successful result is returned even then (we only
  # surface `:query_timeout` when the interrupt actually errored the query).
  #
  # ONE long-lived watchdog per connection, armed per query (review 2026-07-23 #26): the old
  # shape spawned a fresh process per query — ~2–5 µs + a ~2.7 KB process heap + scheduler
  # churn each, i.e. ~50k spawns/s of pure overhead at measured node throughput on exactly the
  # co-tenancy deployments the timeout knob targets. Spawn cost now amortizes to once per
  # connection; it lives in the owner's process dictionary beside the statement cache and dies
  # with the owner (it monitors it) or at close/1.
  defp with_deadline(conn, ms, fun) do
    ref = make_ref()
    watchdog = ensure_watchdog(conn)
    send(watchdog, {:arm, ref, ms, self()})

    # Always disarm, even if fun raises (Sqlite3.bind raises ArgumentError on a bad bind
    # value — the reason query/3 has a rescue). Without try/after the raise unwinds before the
    # {:done, ref} send, leaving the watchdog armed to interrupt a LATER statement reusing this
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

  defp ensure_watchdog(conn) do
    key = {__MODULE__, :watchdog, conn}

    case Process.get(key) do
      pid when is_pid(pid) ->
        # A dead watchdog (it can only die abnormally — e.g. interrupt on a torn-down conn)
        # must not silently disable the deadline for the rest of the stream: replace it.
        if Process.alive?(pid) do
          pid
        else
          Process.delete(key)
          ensure_watchdog(conn)
        end

      nil ->
        owner = self()
        pid = spawn(fn -> watchdog_init(conn, owner) end)
        Process.put(key, pid)
        pid
    end
  end

  defp watchdog_init(conn, owner) do
    mon = Process.monitor(owner)
    watchdog_loop(conn, mon)
  end

  defp watchdog_loop(conn, mon) do
    receive do
      {:arm, ref, ms, parent} ->
        receive do
          {:done, ^ref} ->
            :ok

          {:DOWN, ^mon, :process, _, _} ->
            exit(:normal)
        after
          ms ->
            # cancel/1, not interrupt/1 (expert review 2026-07-24 #1): interrupt only aborts VDBE
            # execution, so a statement parked in the busy handler waiting for the write lock
            # ignored it and slept out the full busy timeout. cancel/1 is a superset — it sets the
            # flag the busy handler polls AND calls sqlite3_interrupt — so the deadline now bounds
            # lock waits too. It is a non-dirty NIF that deliberately avoids the connection mutex,
            # so it cannot block behind the running query. A spurious late cancel is still a no-op:
            # exqlite resets `cancelled` at the top of every subsequent db op.
            Sqlite3.cancel(conn)
            send(parent, {:timed_out, ref})

            # Consume the guaranteed late {:done, ref} before the next arm (the interrupt
            # errors the running step, so the executing after-clause always sends it) —
            # blocking here is what makes a stale interrupt racing a NEWER statement
            # structurally impossible, the same guarantee the per-query watchdog had.
            receive do
              {:done, ^ref} -> :ok
              {:DOWN, ^mon, :process, _, _} -> exit(:normal)
            end
        end

        watchdog_loop(conn, mon)

      :stop ->
        exit(:normal)

      {:DOWN, ^mon, :process, _, _} ->
        exit(:normal)
    end
  end

  defp stop_watchdog(conn) do
    case Process.delete({__MODULE__, :watchdog, conn}) do
      nil -> :ok
      pid -> send(pid, :stop)
    end

    :ok
  end

  @doc """
  Executes raw `sql` with no result rows — for DDL and migration replay (a single
  statement, or several separated by `;`). Use `query/3` for anything returning
  rows or taking bound args.
  """
  @spec exec(reference(), String.t()) :: :ok | {:error, term()}
  def exec(conn, sql), do: Sqlite3.execute(conn, sql)

  @doc """
  Sets how long a statement waits on a contended lock before failing `SQLITE_BUSY`.

  Always use this over `PRAGMA busy_timeout` (expert review 2026-07-24 #1): the pragma calls
  `sqlite3_busy_timeout()`, which swaps exqlite's cancellable busy handler out for SQLite's
  default `sqlite3OsSleep` loop — making the wait uninterruptible, so `:query_timeout_ms` can no
  longer bound it and a blocked statement pins a dirty-IO scheduler thread for the full timeout.
  """
  @spec set_busy_timeout(reference(), non_neg_integer()) :: :ok | {:error, term()}
  def set_busy_timeout(conn, ms), do: Sqlite3.set_busy_timeout(conn, ms)

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
    stop_watchdog(conn)
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
