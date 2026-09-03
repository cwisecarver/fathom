defmodule Fathom.Shard.Connection do
  @moduledoc """
  A single SQLite connection to a shard's database file.

  Each Hrana stream opens its own connection (via `Fathom.ShardExecutor`), so
  per-stream transactions are isolated: SQLite/WAL gives every connection its own
  write transaction and read snapshot, and concurrent writers serialize on
  `busy_timeout`. These are plain functions — the connection is owned and used by
  the calling process throughout its life.
  """
  require Logger

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
    # `File.dir?` first, `mkdir_p!` only on a miss (expert review 2026-08-26 #10).
    #
    # This is on the PER-STREAM path: every Hrana stream opens a handle, which is every request
    # for an HTTP SDK and for django-libsql under Django's default `CONN_MAX_AGE=0`. The
    # unconditional `mkdir_p!` was ~23% of `Connection.open/2` and ~24% of `hrana_open_rt_us`.
    #
    # Why it is that expensive: on Elixir 1.20 `File.mkdir_p!/1` routes to
    # `:filelib.ensure_path/1`, which walks EVERY path component from the root and issues a
    # syscall per component. It is not one `stat`. Measured here, 3 runs x 2000 reps on the same
    # already-existing directory: `mkdir_p!` 98.9 / 100.0 / 105.1 µs vs `File.dir?` 14.5 / 14.4 /
    # 15.3 µs. Full `open(path, tenant?: true, scope: :rw)` went 428.8 µs -> 335.0 µs.
    #
    # Semantics are unchanged for all callers: `mkdir_p` on an existing directory is already a
    # no-op, so the only difference is the syscall count. The slow path still runs whenever the
    # directory is genuinely absent, which is what the migration engine, snapshots, the restore
    # drill and the bench/scale harnesses rely on when they open into a fresh temp directory.
    #
    # On the tenant path the directory provably already exists — `Fathom.Shard`'s
    # `handle_continue(:open, …)` creates it, and OTP runs the continue before any queued
    # `:checkout`, so `Shards.checkout/1` cannot hand back a path whose directory is missing.
    # `checkpoint_and_verify/1` (shard.ex) named this per-open cost and never removed it; nothing
    # measured it because `hrana_open_rt_us` was ratio-gated only. Review #39 added the absolute
    # guard that pins this win.
    dir = Path.dirname(path)
    unless File.dir?(dir), do: File.mkdir_p!(dir)

    tenant? = Keyword.get(opts, :tenant?, false)
    scope = Keyword.get(opts, :scope, :rw)

    case open_handle(path, scope) do
      {:ok, conn, :readonly} -> close_on_error(conn, configure_readonly(conn, tenant?))
      {:ok, conn, :readwrite} -> close_on_error(conn, configure_readwrite(conn, tenant?, scope))
      {:error, reason} -> {:error, reason}
    end
  end

  # Close the just-opened handle when configuration fails (expert review 2026-08-31 #25). A `with`
  # short-circuit in configure_readonly/2 or configure_readwrite/3 — a busy_timeout/cache/authorizer
  # error, or the security-relevant `{:could_not_disable_extension_loading, _}` — otherwise returns
  # the error WITHOUT closing `conn`, so the fd/handle is released only when the NIF resource is
  # GC-finalized. This is the per-stream open path, and relying on GC to close fds there is fragile
  # and defeats the fail-the-open intent of the extension-disable guard. On success the handle rides
  # out as before.
  @doc false
  def close_on_error(_conn, {:ok, _} = ok), do: ok

  def close_on_error(conn, {:error, _reason} = err) do
    _ = Sqlite3.close(conn)
    err
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
  #
  # THE ABSENT FILE IS NOT THAT NARROW CASE (expert review 2026-08-24 #3, verified by
  # execution). `Sqlite3.open(missing, mode: :readonly)` returns `{:error, :database_open_failed}`,
  # and exqlite only creates a shard file on first connection open (see the note at
  # `Fathom.Shard`'s cold-open). So the fallback — written for WAL recovery — was in fact taken
  # for EVERY brand-new tenant and every shard dropped clean while storage held nothing, which
  # is the common path, not the narrow one. Measured before this change: a `:ro` handle on a
  # never-opened shard came back with `query_only = 1`, `PRAGMA main . query_only = OFF` (the
  # qualifier bypass, then live) set it to 0, and `CREATE TABLE written_by_ro(x)` succeeded.
  #
  # Materialize the empty database first and re-try the read-only open, so the scope rests on
  # the engine rather than on a defeatable pragma. There is nothing to lose by creating it: a
  # shard with no stored object has no data for the `:ro` caller to read either way, and the
  # `:rw` path would have created the same file on its first open.
  defp open_handle(path, :ro) do
    case readonly_open(path) do
      {:ok, conn} ->
        {:ok, conn, :readonly}

      {:error, reason} ->
        # The genuine WAL-recovery case the fallback was written for. Loud, because this is the
        # ONE place a `:ro` scope rests on `PRAGMA query_only` instead of on SQLite itself.
        Logger.warning(
          "#{Path.basename(path)}: a :ro handle could not be opened read-only " <>
            "(#{inspect(reason)}); falling back to a read-write handle guarded only by " <>
            "PRAGMA query_only"
        )

        with {:ok, conn} <- Sqlite3.open(path), do: {:ok, conn, :readwrite}
    end
  end

  defp open_handle(path, _scope) do
    with {:ok, conn} <- Sqlite3.open(path), do: {:ok, conn, :readwrite}
  end

  defp readonly_open(path) do
    case Sqlite3.open(path, mode: :readonly) do
      {:ok, conn} -> {:ok, conn}
      {:error, _} = err -> if File.exists?(path), do: err, else: create_then_readonly(path)
    end
  end

  # Open read-write purely to bring the file into existence, close it, and re-open read-only.
  # A zero-length file is a valid empty SQLite database, so the second open succeeds.
  defp create_then_readonly(path) do
    with {:ok, conn} <- Sqlite3.open(path),
         :ok <- Sqlite3.close(conn) do
      Sqlite3.open(path, mode: :readonly)
    end
  end

  # A read-only handle takes only the connection-local pragmas. The file-level ones
  # (`journal_mode`, `wal_autocheckpoint`) describe writes this handle cannot perform, and
  # `max_page_count` is a growth cap on a connection that cannot grow the file.
  defp configure_readonly(conn, tenant?) do
    with :ok <- Sqlite3.set_busy_timeout(conn, 5000),
         :ok <- maybe_foreign_keys(conn),
         :ok <- maybe_cache_size(conn),
         :ok <- load_extension(conn),
         :ok <- maybe_authorizer(conn, tenant?) do
      {:ok, conn}
    end
  end

  defp configure_readwrite(conn, tenant?, scope) do
    with :ok <- configure(conn),
         :ok <- load_extension(conn),
         :ok <- maybe_authorizer(conn, tenant?),
         :ok <- maybe_query_only(conn, scope) do
      {:ok, conn}
    end
  end

  # Register Django's user-defined functions on this connection (expert review 2026-08-01 #19).
  #
  # ORDERING MATTERS in two directions, and both are load-bearing:
  #
  #   * BEFORE `maybe_authorizer/2`. The authorizer denies `:attach`, and SQLite implements
  #     `sqlite3_load_extension` as a privileged operation the authorizer can also refuse. Loading
  #     first keeps the extension available on tenant handles — which is the entire point, since
  #     tenant handles are the ones running Django's SQL — while the authorizer still governs
  #     everything the tenant subsequently sends.
  #   * On EVERY handle, read-only ones included. A `:ro` stream runs the same Django SELECTs
  #     (`__year`, `Trunc*`), and a read replica that silently lacked the functions would fail
  #     exactly the queries a read scope exists to serve.
  #
  # `Fathom.Shard.Extension` re-disables extension loading before returning, so a tenant cannot
  # load one of its own; see its moduledoc for that contract. A failure to re-disable fails the
  # OPEN rather than degrading, because the alternative is handing a tenant a connection with
  # arbitrary code loading enabled.
  defp load_extension(conn) do
    case Fathom.Shard.Extension.load(conn) do
      :ok -> :ok
      :skipped -> :ok
      {:error, reason} -> {:error, reason}
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

  # This file's actual page size. Falls back to SQLite's default rather than raising: an
  # unreadable pragma must not stop a shard opening, and 4096 is the value that reproduces the
  # pre-#21 behaviour — the safe direction is to keep capping, not to skip the cap.
  #
  # `prepare` + `fetch_all`, NOT `Sqlite3.execute/2`, which returns a bare `:ok` and discards
  # result rows. Written with `execute` first: it compiled, ran, and silently took the 4096
  # fallback on EVERY open — the fix would have shipped as a no-op at exactly the non-default
  # page sizes it exists for. Probed rather than assumed.
  #
  # Not `query/3` either: this runs inside `configure/1` during `open/2`, before the statement
  # cache and deadline machinery that path expects are meaningful.
  # RELEASES the statement. Leaving it open kept a schema snapshot alive on the connection for the
  # rest of its life, and the failure showed up nowhere near here: `Copy.migrate/4` came back
  # `{:error, "no such table: app_thing"}` because the destination connection could not see a
  # table created after this pragma was read. Every other prepare in this module releases; this one
  # did not, and a full local suite passed anyway — CI on other OTP versions is what caught it.
  defp page_size(conn) do
    case Sqlite3.prepare(conn, "PRAGMA page_size") do
      {:ok, stmt} ->
        result = Sqlite3.fetch_all(conn, stmt)
        Sqlite3.release(conn, stmt)

        case result do
          {:ok, [[n]]} when is_integer(n) and n > 0 -> n
          _ -> 4096
        end

      _ ->
        4096
    end
  end

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

  # 4 GiB — ~1 GiB under the 5 GiB S3 single-PUT ceiling
  # (`Fathom.Shard.Storage.S3.@max_single_put`), which is the limit this cap exists to stay under.
  # Identical to the previous 1,048,576-page default at SQLite's 4096-byte page, so this is a
  # no-op at default config and correct at every other page size.
  @default_max_bytes 4 * 1024 * 1024 * 1024

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
  # Set `SHARD_MAX_BYTES=0` (or `SHARD_MAX_PAGE_COUNT=0`) to opt out and restore the unlimited
  # behaviour.
  #
  # DENOMINATED IN BYTES, derived per-file from the actual `PRAGMA page_size` (expert review
  # 2026-08-01 #21). The cap was a fixed page COUNT while the ceiling it protects — S3's 5 GiB
  # single-PUT limit — is in BYTES, and the two coincide only at SQLite's default 4096-byte page.
  # At 16 KiB the same 1,048,576 pages is 16 GiB and at 64 KiB it is 64 GiB, which re-enters
  # exactly the state review 2026-07-24 #37 closed: a shard that keeps ACKing writes past the
  # ceiling and can then never upload again, with no operator remedy because the data is already
  # acknowledged.
  #
  # Page size is a per-FILE property that arrives with the database rather than being chosen here:
  # `Tenants.fork`, `Migrator.fork_from_template`, or an operator-seeded template can all bring a
  # non-default one, and it is not something the cap can assume.
  #
  # `:shard_max_page_count` still wins when set explicitly — an operator who configured pages meant
  # pages, and silently reinterpreting their number as bytes would be its own bug.
  defp maybe_max_page_count(conn) do
    case {Application.get_env(:fathom, :shard_max_page_count),
          Application.get_env(:fathom, :shard_max_bytes, @default_max_bytes)} do
      {n, _} when is_integer(n) and n > 0 ->
        Sqlite3.execute(conn, "PRAGMA max_page_count=#{n}")

      {n, _} when is_integer(n) ->
        # Explicit 0/negative: the documented opt-out.
        :ok

      {_, bytes} when is_integer(bytes) and bytes > 0 ->
        pages = max(div(bytes, page_size(conn)), 1)
        Sqlite3.execute(conn, "PRAGMA max_page_count=#{pages}")

      _ ->
        :ok
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
    # A 4-tuple with a plain pattern, not a 3-tuple plus a guard. A `with` whose pattern carries a
    # guard lets the unmatched value fall through as the function's return, which dialyzer caught
    # as `{:ok, _, _} | {:refresh, reference(), [any()]}` escaping `query/4`'s spec.
    with {:ok, stmt, columns, refresh_columns?} <- cached_stmt(conn, sql) do
      # The statement is owned by this connection's cache — reset it (not release) when done,
      # including on a bind/collect error or a row-cap abort. sqlite3_reset ends the statement's
      # execution (releasing its locks / read-transaction hold, which release/finalize used to
      # do) while keeping the compiled plan for the next execution of the same SQL.
      try do
        with :ok <- Sqlite3.bind(stmt, args),
             {:ok, rows, row_count} <- collect(conn, stmt, row_cap()) do
          # Post-step column read when the schema moved under this connection (expert review
          # 2026-08-26 #7). Only on the `:refresh` path — a DDL happened somewhere since this
          # statement was cached — so the ordinary hit pays nothing. See `cached_stmt/2` for why
          # this cannot be done any earlier.
          columns =
            if refresh_columns?, do: refreshed_columns(conn, stmt, sql, columns), else: columns

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

  # Re-read the column list now that the statement has stepped, and correct the cache entry so the
  # next execution takes the ordinary hit path. Falls back to the pre-step list if the read fails —
  # degrading to the old (possibly stale) answer rather than failing a query that already succeeded.
  defp refreshed_columns(conn, stmt, sql, fallback) do
    case Sqlite3.columns(conn, stmt) do
      {:ok, cols} ->
        key = {__MODULE__, :stmt_cache, conn}

        case Process.get(key) do
          {seq, %{^sql => {canonical, ^stmt, _cols, stamp, gen}} = cache} ->
            Process.put(
              key,
              {seq, Map.put(cache, canonical, {canonical, stmt, cols, stamp, gen})}
            )

          _ ->
            :ok
        end

        cols

      _ ->
        fallback
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
  # THE CACHED COLUMN LIST GOES STALE ACROSS ANOTHER STREAM'S DDL (expert review 2026-08-26 #7).
  #
  # `prepare_v2` recompiles a statement transparently on a schema change, so the STATEMENT survives
  # — but `cols`, captured beside it at prepare time, does not, and nothing re-read it. Verified by
  # execution: connection B caches `SELECT * FROM t`, connection A runs
  # `ALTER TABLE t ADD COLUMN y`, and B then returns two column NAMES with three VALUES per row.
  # That `StmtResult` goes on the wire; a client zipping cols to values silently mis-maps them.
  #
  # Review 2026-07-24 #17 purges this cache for DDL the SAME connection ran. What it cannot see is
  # DDL from a sibling stream on the same shard, which is the case `:block_tenant_ddl` exists to
  # refuse and which is off by default.
  #
  # Each entry now carries the `Fathom.Shard.SchemaGen` value it was prepared under. On a hit with
  # a moved generation, re-read the columns — NOT re-prepare: `prepare_v2` has already recompiled
  # the statement itself, so only the Elixir-side list is stale, and `Sqlite3.columns/2` is the
  # cheap half.
  #
  # NOT the tempting `length(hd(rows)) != length(cols)` check, which the review names explicitly:
  # it cannot see the fault on a zero-row `SELECT *`, where the reported cols are still wrong.
  defp cached_stmt(conn, sql) do
    key = {__MODULE__, :stmt_cache, conn}
    {seq, cache} = Process.get(key, {0, %{}})
    gen = Fathom.Shard.SchemaGen.current()

    case cache do
      %{^sql => {canonical, stmt, cols, _stamp, ^gen}} ->
        # Re-put under `canonical`, never the incoming sub-binary: a map that has grown past a
        # flatmap adopts the new key term, which would silently reinstate the pin.
        Process.put(
          key,
          {seq + 1, Map.put(cache, canonical, {canonical, stmt, cols, seq + 1, gen})}
        )

        {:ok, stmt, cols, false}

      # Hit, but the schema moved since this entry was prepared. Re-prepare AND tell the caller to
      # re-read the columns AFTER the statement has stepped.
      #
      # Both cheaper ideas were tried and measured wrong:
      #
      #   * `Sqlite3.columns/2` on the cached statement — `prepare_v2` recompiles LAZILY at the next
      #     `sqlite3_step`, so the column names before stepping are still the pre-DDL ones.
      #   * A fresh `Sqlite3.prepare/2` on this connection — ALSO returns the stale names, because
      #     this connection's in-memory schema is itself stale until something makes it re-read.
      #     Measured: a brand-new connection to the same file returned `["id", "x", "y"]` while a
      #     fresh prepare on the DDL-unaware connection returned `["id", "x"]`.
      #
      # SQLite settles it at step (SQLITE_SCHEMA → automatic re-prepare), so the correct columns
      # only exist after execution. The re-prepare is still worth doing — it drops a statement
      # compiled against a schema this connection has since left behind — but the authoritative
      # read happens in `do_query/4`.
      %{^sql => {canonical, stmt, _stale_cols, _stamp, _older_gen}} ->
        Sqlite3.release(conn, stmt)

        with {:ok, fresh} <- Sqlite3.prepare(conn, canonical),
             {:ok, cols} <- Sqlite3.columns(conn, fresh) do
          Process.put(
            key,
            {seq + 1, Map.put(cache, canonical, {canonical, fresh, cols, seq + 1, gen})}
          )

          {:ok, fresh, cols, true}
        end

      _ ->
        with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
             {:ok, cols} <- Sqlite3.columns(conn, stmt) do
          canonical = :binary.copy(sql)
          cache = maybe_evict(conn, cache)

          Process.put(
            key,
            {seq + 1, Map.put(cache, canonical, {canonical, stmt, cols, seq + 1, gen})}
          )

          {:ok, stmt, cols, false}
        end
    end
  end

  # At the cap, release the least-recently-used statement to make room. O(cap) scan, paid
  # only on an over-cap miss — a workload cycling >64 distinct statements per stream is
  # re-preparing anyway, exactly as before the cache.
  defp maybe_evict(conn, cache) when map_size(cache) >= @stmt_cache_cap do
    {lru_sql, {_canonical, stmt, _cols, _stamp, _gen}} =
      Enum.min_by(cache, fn {_sql, {_canonical, _stmt, _cols, stamp, _gen}} -> stamp end)

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
        Enum.each(cache, fn {_sql, {_canonical, stmt, _cols, _stamp, _gen}} ->
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

  # How often the watchdog re-issues a cancel that has not yet taken effect. See await_disarm/3.
  @cancel_retry_ms 5

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
    # Sweep residue from an EARLIER deadline before arming this one (expert review 2026-08-01
    # #46's residual race, which its own tests said they did not close).
    #
    # The peek below is `after 0` — non-blocking — so it misses a `{:timed_out, ref}` the watchdog
    # sent in the instant between the query finishing and that peek running. The ref is unique per
    # call, so no later peek can ever match it either: the message sits in the stream process's
    # mailbox forever, one per near-miss, growing without bound.
    #
    # CI found it, not reading: `connection_watchdog_test` drives 400 queries at a 1 ms deadline
    # specifically to sit on that boundary, and a loaded 2–4 core runner widens the window enough
    # that it fires. It passed on an 18-core dev box for the same reason the other two CI-only
    # failures did.
    #
    # Swept at ARM rather than acknowledged at disarm: a synchronous disarm handshake would close
    # the window completely but costs a round trip on every guarded query. Anything still in the
    # mailbox at this point is necessarily stale — `ref` does not exist yet — so this is exact,
    # and it is O(residue): normally the mailbox is empty and the receive returns immediately.
    drain_stale_timeouts()

    ref = make_ref()
    watchdog = arm_watchdog(conn, ref, ms)

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

  defp drain_stale_timeouts do
    receive do
      {:timed_out, _stale} -> drain_stale_timeouts()
    after
      0 -> :ok
    end
  end

  # Arm THIS query's deadline, re-checking liveness AFTER the send (expert review 2026-08-31 #22).
  # `ensure_watchdog/1` replaces a watchdog that is already dead, but there is a TOCTOU between it
  # returning a LIVE pid and the fire-and-forget `{:arm}` send: the watchdog can die in that window
  # (it dies abnormally — e.g. a stale `await_disarm` cancel on a torn-down conn), and a send to a
  # just-dead pid is dropped silently, leaving the query with NO deadline until the next query
  # replaces the pid. Re-check after the send and re-arm a FRESH watchdog if it is gone.
  #
  # An async re-check, not a synchronous ack: the module deliberately rejected a per-query handshake
  # round trip (see with_deadline). A vanishing residual window remains — a death after this
  # `Process.alive?` check but before the `{:arm}` is processed — and it self-heals on the next
  # query. That residual is why this race is NOT unit-testable: `ensure_watchdog/1` gatekeeps every
  # non-race dead-pid case, so no test can inject a pid that reaches the send dead without also
  # dying in a scheduler-dependent window a test cannot force. Bounded retries so a conn being torn
  # down (its fresh watchdogs die immediately) cannot spin forever — after that the query is failing
  # anyway.
  @arm_attempts 3

  defp arm_watchdog(conn, ref, ms, attempts \\ @arm_attempts) do
    watchdog = ensure_watchdog(conn)
    send(watchdog, {:arm, ref, ms, self()})

    cond do
      Process.alive?(watchdog) -> watchdog
      attempts > 0 -> arm_watchdog(conn, ref, ms, attempts - 1)
      true -> watchdog
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
    # HIGH PRIORITY so a saturated scheduler cannot starve the deadline (CI, OTP 28/29, 2026-08-29).
    #
    # `await_disarm/3` re-issues its cancel every @cancel_retry_ms until the owner disarms, and its
    # correctness rests on one unstated assumption: that the next cancel actually RUNS. The watchdog
    # is a plain process, and while the owner is blocked in the `multi_step` dirty NIF the watchdog
    # must get an ORDINARY scheduler slot to call `Sqlite3.cancel`. On a loaded 2-core runner it did
    # not — the run queue was full of the rest of the suite, the watchdog was not scheduled for the
    # whole life of the query, and a 200 000 000-row recursive CTE ran to COMPLETION under a 60 ms
    # deadline (`rows: [[200000000]]`). The retry loop was right; it never executed.
    #
    # `:high` makes the watchdog preempt normal-priority work, so its cancels land within a retry
    # interval of the statement reaching a scheduler. It is cheap to run at this priority: the
    # process is blocked in `receive` (not runnable) except in the brief window when a deadline is
    # actually firing, and `Sqlite3.cancel` is a quick non-dirty NIF, so it never competes with the
    # query itself (which runs on a DIRTY scheduler). Not `:max`, which is reserved for core runtime
    # processes and can wedge the scheduler.
    Process.flag(:priority, :high)
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
            # Tell the owner BEFORE cancelling (expert review 2026-08-01 #46). Cancelling first
            # opens a window: the query can finish naturally between `cancel/1` and this send,
            # in which case the owner's `after 0` peek for `{:timed_out, ref}` already ran and
            # found nothing, and the message lands in a long-lived stream process's mailbox
            # where nothing will ever match it again (the next call uses a fresh `make_ref/0`).
            #
            # Sending first makes the ordering structural: the message is in the mailbox before
            # the cancel can let `fun.()` return, so the peek either sees it or the query was
            # never cancelled at all. A spurious late cancel is already a no-op — exqlite resets
            # `cancelled` at the top of every subsequent db op.
            send(parent, {:timed_out, ref})
            await_disarm(conn, ref, mon)
        end

        watchdog_loop(conn, mon)

      :stop ->
        exit(:normal)

      {:DOWN, ^mon, :process, _, _} ->
        exit(:normal)
    end
  end

  # Cancel, then KEEP cancelling until the owner disarms (CI, OTP 29, 2026-08-28).
  #
  # A single cancel is LOST whenever the deadline expires before the statement has actually
  # started running, and the deadline then does not apply at all. Both of its mechanisms fail
  # for the same reason: `sqlite3_interrupt` issued while no statement is running is a
  # documented no-op that explicitly does NOT affect statements started after it returns, and
  # exqlite's own `cancelled` flag — the one the busy handler polls — is reset at the top of
  # the next db op. So the query then runs unbounded, which is precisely what
  # `:query_timeout_ms` exists to prevent.
  #
  # That gap is not exotic. `Sqlite3.multi_step/3` is a DIRTY NIF, so the owner blocks waiting
  # for a dirty-CPU scheduler slot when they are all busy — and the count equals the core
  # count. On a 2-core CI runner mid-suite that wait passed a 60 ms deadline and
  # `connection_watchdog_test`'s SECOND slow query ran a 200 000 000-row recursive CTE to
  # completion (`rows: [[200000000]]`, job 98748804302). The protection failed under exactly
  # the load it is for, and would in production for the same reason.
  #
  # Retrying latches it: whenever the statement finally reaches a scheduler, the next cancel
  # lands on a RUNNING statement and errors it. Overshoot is bounded by @cancel_retry_ms, and
  # the healthy path pays nothing — an interrupted query's `{:done, ref}` arrives long before
  # the first retry.
  #
  # Still blocking until `{:done, ref}`, so the 2026-07-23 #26 guarantee is intact: the
  # watchdog cannot process a NEWER `{:arm, …}` while these cancels are in flight. A retry
  # issued in the sliver between `Sqlite3.reset/1` and `{:done, ref}` is the same no-op class
  # the single cancel already accepted, not a new hazard.
  defp await_disarm(conn, ref, mon) do
    Sqlite3.cancel(conn)

    receive do
      {:done, ^ref} -> :ok
      {:DOWN, ^mon, :process, _, _} -> exit(:normal)
    after
      @cancel_retry_ms -> await_disarm(conn, ref, mon)
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
  Reads a scalar `PRAGMA` off this connection — `pragma(conn, "synchronous")` ⇒ `{:ok, 2}`.

  A READ helper, deliberately separate from `exec/2`: pragma state is per-connection and
  invisible from the outside, so invariants about it (the durability snapshot's `synchronous`
  relaxation being scoped and restored — expert review 2026-08-01 #5 / #30 item 7) had no way to
  be asserted at all. `name` is interpolated, so it must be a literal from this codebase, never
  client input; the pragma grammar takes no bound parameters.
  """
  @spec pragma(reference(), String.t()) :: {:ok, term()} | {:error, term()}
  def pragma(conn, name) do
    case query(conn, "PRAGMA #{name}", []) do
      {:ok, %{rows: [[value] | _]}} -> {:ok, value}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, _} = error -> error
    end
  end

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
