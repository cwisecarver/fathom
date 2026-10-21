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
  alias Filo.StmtResult

  @doc """
  Opens a connection to the shard file at `path` (WAL, synchronous=FULL, 5s busy
  timeout, and — unless `config :fathom, :foreign_keys` is false — foreign keys ON).

  ## Options

    * `:tenant?` — this handle will execute **client-supplied** SQL, so deny
      `ATTACH`/`DETACH` at the SQLite authorizer (expert review 2026-08-01 #1) and,
      when the `fathom_udf` extension is loaded, PRAGMA assignments outside the
      tenant allow-list (expert review 2026-10-08 #1, tier b).
      Defaults to `false`: fathom's own callers (the coordinator's durability
      snapshot, the migration engine's replay, the bench/scale harnesses) are
      trusted and MUST NOT get the authorizer — see `maybe_authorizer/3`.
    * `:scope` — `:rw` (default) or `:ro`. A `:ro` handle is opened
      `mode: :readonly` so SQLite itself refuses every write (#7).
    * `:wal?` — require the handle to enter WAL mode (default `true`; the open fails with
      `{:wal_unavailable, mode}` otherwise, expert review 2026-10-12 #11). Only a scratch
      `:memory:` handle, which can never be WAL, passes `false`.

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
    #
    # The tenant path now skips even the `File.dir?` (expert review 2026-10-17 #7: ~14.5 us of a
    # ~285 us open): the directory provably exists (above), and if that ever stops being true the
    # open fails with an error, on which we `mkdir_p!` and retry exactly once.
    tenant? = Keyword.get(opts, :tenant?, false)
    dir = Path.dirname(path)
    unless tenant?, do: ensure_dir(dir)

    scope = Keyword.get(opts, :scope, :rw)
    guard_opts = [block_user_version?: Keyword.get(opts, :block_user_version?, false)]
    wal? = Keyword.get(opts, :wal?, true)

    case open_handle_ensuring_dir(path, scope, tenant?, dir) do
      {:ok, conn, :readonly} ->
        track_open(
          close_on_error(conn, configure_readonly(conn, tenant?, guard_opts)),
          tenant?,
          path
        )

      {:ok, conn, :readwrite} ->
        track_open(
          close_on_error(conn, configure_readwrite(conn, tenant?, scope, guard_opts, wal?)),
          tenant?,
          path
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_dir(dir), do: unless(File.dir?(dir), do: File.mkdir_p!(dir))

  defp open_handle_ensuring_dir(path, scope, true = _tenant?, dir) do
    case open_handle(path, scope) do
      {:error, _} = err ->
        if File.dir?(dir) do
          err
        else
          File.mkdir_p!(dir)
          open_handle(path, scope)
        end

      ok ->
        ok
    end
  end

  defp open_handle_ensuring_dir(path, scope, false, _dir), do: open_handle(path, scope)

  # --- live-connection gauge (perf review 2026-10-14 #16, gauge half) ---------------------------
  #
  # Every open SQLite connection costs ~166 KB of fixed memory that `soft_heap_limit` cannot shed,
  # and nothing counted them node-wide. The gauge is the size of a public ETS set of handles that
  # `open/2` returned and `close/1` has not yet released — NOT a bare `:counters` ref, because
  # `close/1` is documented idempotent and is called from the stream AND the coordinator on the same
  # handle (shard_executor.ex's checkin paths): a counter would decrement twice, while
  # `:ets.take/2` makes only the first close count. Cost: two O(1) ETS ops per connection lifetime
  # against a ~335 us open. Caveat: a handle whose owner died without calling `close/1`
  # is GC-closed by the NIF resource finalizer and its row stays counted here (an over-estimate,
  # never an under-estimate).
  #
  # The row is keyed by `key/1` (the ref's printed identity), NOT by the resource term: an ETS row
  # holds a reference, so a row keyed by the exqlite resource itself kept the NIF resource alive and
  # the finalizer (which closes the sqlite handle: fd, ~166 KB, WAL reader slot) could never run for
  # an orphaned handle (expert review 2026-10-17 #2). Not `phash2` — collisions would drop rows. Without the table (the app not started, e.g. a bare script) tracking is a no-op.
  @live_table __MODULE__.Live

  @doc false
  @spec create_live_table() :: :ok
  def create_live_table do
    if :ets.whereis(@live_table) == :undefined do
      :ets.new(@live_table, [:named_table, :public, :set, write_concurrency: true])
    end

    create_busy_queue()
  end

  @doc "Number of connections opened through `open/2` and not yet closed through `close/1`."
  @spec live_count() :: non_neg_integer()
  def live_count do
    case :ets.info(@live_table, :size) do
      :undefined -> 0
      n -> n
    end
  end

  # A TENANT handle's row carries its file path: the busy queue's shard key (expert review
  # 2026-10-20-2027 #13), read only on the lock-contended path (`busy_shard/1`).
  defp track_open({:ok, conn} = ok, tenant?, path) do
    _ =
      if :ets.whereis(@live_table) != :undefined,
        do: :ets.insert(@live_table, {live_key(conn), if(tenant?, do: path)})

    ok
  end

  defp track_open(other, _tenant?, _path), do: other

  defp live_key(conn), do: :erlang.ref_to_list(conn)

  defp track_close(conn) do
    if :ets.whereis(@live_table) != :undefined, do: :ets.delete(@live_table, live_key(conn))
    :ok
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
  defp configure_readonly(conn, tenant?, guard_opts) do
    with :ok <- Sqlite3.set_busy_timeout(conn, 5000),
         :ok <- maybe_foreign_keys(conn),
         :ok <- maybe_cache_size(conn),
         {:ok, ext} <- load_extension(conn, tenant?),
         :ok <- maybe_harden(conn, tenant?, ext),
         :ok <- maybe_authorizer(conn, tenant?, ext, guard_opts) do
      {:ok, conn}
    end
  end

  defp configure_readwrite(conn, tenant?, scope, guard_opts, wal?) do
    with :ok <- configure(conn, wal?),
         {:ok, ext} <- load_extension(conn, tenant?),
         :ok <- maybe_harden(conn, tenant?, ext),
         :ok <- maybe_query_only(conn, scope),
         :ok <- maybe_authorizer(conn, tenant?, ext, guard_opts) do
      {:ok, conn}
    end
  end

  # Register Django's user-defined functions on this connection (expert review 2026-08-01 #19).
  #
  # ORDERING MATTERS in two directions, and both are load-bearing:
  #
  #   * BEFORE `maybe_authorizer/3`. The authorizer denies `:attach`, and SQLite implements
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
  #
  # Returns `{:ok, :loaded | :skipped}`: `maybe_authorizer/3` needs to know which authorizer the
  # handle can carry.
  defp load_extension(conn, tenant?) do
    case Fathom.Shard.Extension.load(conn) do
      :ok ->
        if tenant?, do: :ok = arm_backstop(conn)
        {:ok, :loaded}

      :skipped ->
        {:ok, :skipped}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # THE ON-THREAD DEADLINE BACKSTOP (2026-10-04; native/fathom_udf/src/backstop.rs). The watchdog
  # enforces `:query_timeout_ms` with a BEAM timer, and after the VM had idled no normal-scheduler
  # timer fired while a long query ran in its dirty NIF: a 5 ms deadline fired at ~5,550 ms, when
  # the query had already finished. The extension's progress handler runs ON the query's thread,
  # so it cannot be starved that way. Set here, BEFORE the authorizer and any tenant SQL, because
  # the extension accepts only the first value — a tenant's own `fathom_backstop(0)` is ignored.
  #
  # The cost of set-once: a runtime change to `:query_timeout_ms` reaches the backstop only on
  # connections opened after it. The watchdog reads the live value on every query.
  #
  # A failure is logged, not fatal: the backstop is a second line, and an extension artifact that
  # predates it should not make every open fail.
  #
  # TENANT HANDLES ONLY (expert review 2026-10-08 #11), for the reason `query/4` gives: the deadline
  # bounds client SQL. Armed on every handle, it also cut off fathom's own `VACUUM INTO` snapshot and
  # `quick_check` — through `exec/2` too, which the watchdog never covered.
  defp arm_backstop(conn) do
    case timeout_ms() do
      ms when is_integer(ms) and ms > 0 ->
        case Sqlite3.execute(conn, "SELECT fathom_backstop(#{ms})") do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning("statement-deadline backstop not armed: #{inspect(reason)}")
            :ok
        end

      _ ->
        :ok
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
  #
  # THE ENGINE PRAGMA GATE (expert review 2026-10-08 #1, tier b). `Fathom.ShardExecutor`'s PRAGMA
  # allow-list is a hand parser over statement text, and five of its defects shipped as live
  # bypasses — each one let a tenant (a `:ro` one too) turn off `max_page_count`, `synchronous`,
  # `writable_schema` or the process-global `hard_heap_limit`. When the extension is loaded, the
  # authorizer is the extension's instead of exqlite's (native/fathom_udf/src/pragma_guard.rs): it
  # sees each pragma as SQLite PARSED it and denies an ASSIGNMENT whose name the tenant may not set,
  # so the next parser defect is a 403-vs-"not authorized" difference rather than a bypass. A READ
  # (no value) is always allowed. A connection has ONE authorizer slot, so the extension's also
  # denies ATTACH/DETACH, keeping everything above.
  #
  #   * The allowed names come from `ShardExecutor.tenant_pragma_assignable/0` — the text gate's own
  #     lists — so the two cannot drift. `pinned_pragmas/0` adds the exact assignments FATHOM makes
  #     on a tenant handle after this point (pooled reuse, the lazy TEMP cap).
  #   * Installed LAST in the open, after `maybe_harden/2` and `maybe_query_only/2` (fathom's own
  #     pragmas), and before any tenant SQL. The extension accepts only the first call, so a
  #     tenant's own `fathom_pragma_guard(...)` cannot widen it.
  #   * Fails the OPEN if it cannot be installed while the extension IS loaded: the alternative is a
  #     tenant handle with neither the pragma backstop nor exqlite's ATTACH deny.
  #   * Extension absent (`:skipped`): exqlite's ATTACH/DETACH authorizer, exactly as before.
  defp maybe_authorizer(_conn, false, _ext, _guard_opts), do: :ok

  defp maybe_authorizer(conn, true, :skipped, _guard_opts),
    do: Sqlite3.set_authorizer(conn, [:attach, :detach])

  defp maybe_authorizer(conn, true, :loaded, guard_opts),
    do: install_pragma_guard(conn, guard_opts)

  # Bound, not interpolated: the operator's `:tenant_pragma_allow` reaches this SQL.
  #
  # `user_version` is dropped from the allow-list when the caller asks (expert review 2026-10-10
  # #15): under `:block_tenant_ddl` a non-template tenant must not stamp the schema version, and the
  # text gate catches only statements that START with "pragma". The engine sees the pragma however
  # it is spelled.
  defp install_pragma_guard(conn, guard_opts) do
    assignable = Fathom.ShardExecutor.tenant_pragma_assignable()

    names =
      if guard_opts[:block_user_version?],
        do: Enum.join(assignable -- ["user_version"], ","),
        else: Enum.join(assignable, ",")

    with {:ok, stmt} <- Sqlite3.prepare(conn, "SELECT fathom_pragma_guard(?1, ?2)") do
      try do
        with :ok <- Sqlite3.bind(stmt, [names, pinned_pragmas()]),
             {:row, [1]} <- Sqlite3.step(conn, stmt) do
          :ok
        else
          {:error, reason} -> {:error, {:pragma_guard_not_installed, reason}}
          other -> {:error, {:pragma_guard_not_installed, other}}
        end
      after
        Sqlite3.release(conn, stmt)
      end
    else
      {:error, reason} -> {:error, {:pragma_guard_not_installed, reason}}
    end
  end

  # The `[schema.]name=value` assignments fathom itself sends on a TENANT handle after the guard is
  # installed, each allowed with exactly this value (a tenant sending the same one changes nothing —
  # every value is fathom's own protective setting):
  #
  #   * `synchronous=FULL`, `cache_size=-<kb>` — `reconfigure_for_reuse/1` on a pooled `:rw` handle
  #     (`foreign_keys` is already an allowed name, and the main `max_page_count` is re-applied there
  #     only when the operator allowed that name).
  #   * `query_only=ON` — `@ro_reuse_sql` on a pooled `:ro` handle.
  #   * `temp.max_page_count=<n>` — `cap_temp/1`, before TEMP DDL. Pinned WITH its schema: the value
  #     is derived at a 4096-byte page and is larger than the main cap on a bigger-page file, so a
  #     bare `max_page_count=<n>` must not match it.
  #
  # Read from config at open, the same reads those paths make. If an operator changes
  # `:shard_cache_size_kb` or the size cap at runtime, a pooled handle's reuse batch is refused and
  # `reset_for_reuse/2`'s caller discards the handle and opens a fresh one — the safe direction.
  defp pinned_pragmas do
    [
      "synchronous=FULL",
      "query_only=ON",
      cache_size_kb() && "cache_size=-#{cache_size_kb()}",
      temp_cap_pages() && "temp.max_page_count=#{temp_cap_pages()}"
    ]
    |> Enum.filter(& &1)
    |> Enum.join(",")
  end

  # Fallback belt for a `:ro` scope that could not get a read-only handle (see open_handle/2).
  defp maybe_query_only(conn, :ro), do: Sqlite3.execute(conn, "PRAGMA query_only=ON")
  defp maybe_query_only(_conn, _scope), do: :ok

  # Engine-level hardening for handles that run untrusted client SQL (expert review 2026-09-05 #3).
  # The PRAGMA allow-list in `Fathom.ShardExecutor` is the half of the tenant gate with NO engine
  # backstop: unlike ATTACH (stopped by the authorizer) and `:ro` writes (stopped by a
  # `mode: :readonly` handle), a PRAGMA miss in the head parser is a complete bypass — which is
  # how 2026-08-20 #19, 2026-08-24 #1 and 2026-09-05 #1/#2 each landed. (Since 2026-10-08 #1 tier b
  # the extension's authorizer backstops assignments — see `maybe_authorizer/3` — but only when the
  # extension is loaded, and these still hold underneath it.) These two put a floor
  # UNDER the parser, at the engine, where SQLite enforces them however the SQL is spelled:
  #
  #   * `trusted_schema=OFF` — refuse schema objects (views/triggers/CHECK/generated columns)
  #     that call non-innocuous functions or virtual tables: the `writable_schema=ON`
  #     hostile-schema class. Safe for Django — `native/fathom_udf` marks every `django_*`
  #     function `SQLITE_INNOCUOUS` (lib.rs), so expression indexes and CHECK constraints over
  #     them still compile; only fathom's own process-state WAL functions (correctly NOT
  #     innocuous) are refused inside a schema object, which a tenant has no reason to embed.
  #   * `cell_size_check=ON` — validate b-tree cell sizes as pages are read, so a hand-corrupted
  #     page (reachable once `writable_schema` is set) fails loudly rather than parsing as a
  #     hostile structure.
  #
  # SQLite's third recommended leg, `SQLITE_DBCONFIG_DEFENSIVE`, is NOT set: exqlite exposes no
  # `sqlite3_db_config`, so it needs an upstream change (tracked). And they apply to tenant
  # handles only — the coordinator's VACUUM-INTO snapshot and the migration replay run fathom's
  # own SQL and legitimately need the unhardened engine. A failure to apply them fails the OPEN
  # (like the extension re-disable), because the alternative is a tenant handle without the floor.
  defp maybe_harden(_conn, false, _ext), do: :ok

  # ONE batch (expert review 2026-10-19 #23): both are plain PRAGMAs, and `sqlite3_exec` stops at
  # the first error, which is what the two-step `with` did. Probe: two executes 11-17 us mean per
  # open, one batch 5-9 us. The engine-level tests (`connection_test.exs`, "engine hardening") read
  # both values back after open, so a batch that silently dropped one would fail there.
  #
  # With the extension loaded the same batch turns on `fathom_busy_yield(1)` (expert review
  # 2026-10-20 #2): a lock wait on a tenant handle gives up at once and `query/4`
  # retries it from the owner process instead of sleeping on a dirty-IO thread (see
  # `retry_busy/4`). Riding this batch keeps the open's dispatch count unchanged.
  defp maybe_harden(conn, true, :loaded),
    do:
      Sqlite3.execute(
        conn,
        "PRAGMA trusted_schema=OFF;PRAGMA cell_size_check=ON;SELECT fathom_busy_yield(1)"
      )

  defp maybe_harden(conn, true, :skipped),
    do: Sqlite3.execute(conn, "PRAGMA trusted_schema=OFF;PRAGMA cell_size_check=ON")

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
  #
  # NOT cached per coordinator (expert review 2026-10-19 #23): a `{path => page_size}` cache is
  # only safe while the file at that path is the one it was read from, and the pull, fork and
  # promote paths replace files in place. A stale value would size the cap wrongly (up to 16x
  # off), and the cost it saves is one prepare/fetch/release, about 18 us per open.
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

  # `PRAGMA journal_mode=WAL` does not fail when SQLite cannot honour it (a filesystem without
  # shared memory — NFS/EFS/9p — or an in-memory database): it returns the mode it ended up in and
  # silently stays there. A shard in `delete` mode has no WAL, so A2 ships nothing from it for the
  # life of the file (expert review 2026-10-12 #11). Read the result row back and fail the open
  # unless it is "wal". `prepare` + `fetch_all`, because `Sqlite3.execute/2` discards rows.
  defp enable_wal(conn) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, "PRAGMA journal_mode=WAL") do
      result = Sqlite3.fetch_all(conn, stmt)
      Sqlite3.release(conn, stmt)

      case result do
        {:ok, [[mode]]} when is_binary(mode) ->
          if String.downcase(mode) == "wal", do: :ok, else: {:error, {:wal_unavailable, mode}}

        other ->
          {:error, {:wal_unavailable, other}}
      end
    end
  end

  defp configure(conn, wal?) do
    with :ok <- if(wal?, do: enable_wal(conn), else: :ok),
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
         # ONE BATCH for the fixed per-connection pragmas (perf review 2026-10-14 #21): `synchronous`,
         # `wal_autocheckpoint`, `journal_size_limit`, `foreign_keys` and `cache_size` were five
         # separate `execute` calls, i.e. five dirty-IO dispatches per fresh open (`execute` is a
         # dirty NIF; the reuse path was batched in c969f40, the fresh path was not). Measured on this
         # tree with `Fathom.Test.NifDispatchCounter`: a tenant `:rw` open 23 -> 19 dispatches.
         # `sqlite3_exec` runs the statements in order and stops at the first error, which is what
         # the `with` chain did. `journal_mode=WAL` stays a separate prepare+fetch (it returns the
         # row that gates the open) and `max_page_count` stays separate (it may need a page_size
         # read). Everything here still runs BEFORE `load_extension/2`, which is what the
         # wal_autocheckpoint / wal-hook slot ordering needs (AGENTS.md loud warning).
         :ok <- Sqlite3.execute(conn, rw_open_sql()),
         :ok <- maybe_max_page_count(conn) do
      :ok
    end
  end

  # Built from config (integers and booleans fathom owns, never tenant input). The comments that
  # used to sit on each separate call:
  #
  #   * `synchronous=FULL` fsyncs the WAL on every commit, so a committed write survives a node
  #     crash (per-commit local durability, tightening the local RPO below the S3 flush interval).
  #     Measured FULL vs NORMAL within noise (node_tps 4167->4140, TPC-C tpmC unchanged) because
  #     throughput is wire/executor-bound and per-shard fsyncs parallelize across the fan-out. See
  #     docs/reviews/competitive-oltp-2026-07-10.md.
  #   * `wal_autocheckpoint=4000` (expert review 2026-07-24 #4) raises the threshold above SQLite's
  #     1000-frame default so an autocheckpoint (INLINE in the committing tenant statement, with a
  #     main-database fsync under synchronous=FULL) does not bill a p99 spike to an arbitrary client
  #     query. The coordinator checkpoints PASSIVE after each durability snapshot, where no client
  #     waits. Raised, NOT disabled: at 0 a wedged coordinator would grow the WAL without bound.
  #   * `journal_size_limit`, `foreign_keys`, `cache_size`: see `journal_size_limit_sql/0`,
  #     `maybe_foreign_keys/1` and `maybe_cache_size/1`.
  defp rw_open_sql do
    IO.iodata_to_binary([
      "PRAGMA synchronous=FULL;PRAGMA wal_autocheckpoint=4000",
      journal_size_limit_sql(),
      foreign_keys_sql(),
      cache_size_sql()
    ])
  end

  defp foreign_keys_sql do
    if Application.get_env(:fathom, :foreign_keys, true),
      do: [";PRAGMA foreign_keys=ON"],
      else: []
  end

  defp cache_size_sql do
    case cache_size_kb() do
      nil -> []
      kb -> [";PRAGMA cache_size=-#{kb}"]
    end
  end

  # THE CONNECTION-LOCAL HALF OF `configure/1`, for a pooled `:rw` handle's reuse (expert review
  # 2026-09-29 #28). Re-running all of `configure/1` re-issued `PRAGMA wal_autocheckpoint`, and
  # AGENTS.md's loud warning applies: that pragma and `sqlite3_wal_hook` are THE SAME SLOT, so it
  # re-installed SQLite's built-in checkpoint hook over the extension's `on_commit` hook — and
  # extension loading is disabled after open, so the extension's hook never came back. Every reused
  # handle silently lost the A2 frame seam and its commit counters (checkpointing itself was fine:
  # the built-in hook runs the same PASSIVE 4000-page checkpoint the extension's does).
  #
  # `journal_mode` is file-level and persistent, and `wal_autocheckpoint` is not settable by a tenant,
  # so neither needs re-applying; the rest are cheap and are what a stream could have changed or
  # what `configure/1` guarantees per connection.
  #
  # ONE BATCH, AND ONLY WHAT CAN HAVE CHANGED (expert review 2026-10-01 perf #24). The schema reload
  # (see "RELOAD THE SCHEMA" below) leads, then the pragmas a stream can set. Dropped from the reuse path:
  #
  #   * `set_busy_timeout` — `busy_timeout` is on `ShardExecutor`'s hard DENY list, which no
  #     operator allow-list can re-open, so the value set at open is still there.
  #   * `max_page_count` — not tenant-settable, and recomputing it cost a `PRAGMA page_size` prepare,
  #     fetch and release on every reuse. Re-applied only when an operator has put it on
  #     `:tenant_pragma_allow`, the one way a stream could have changed it.
  #
  # `synchronous` stays in the batch for the same operator-allow reason; inside one `execute` it is
  # nearly free.
  defp reconfigure_for_reuse(conn) do
    with :ok <- Sqlite3.execute(conn, rw_reuse_sql()) do
      if "max_page_count" in Application.get_env(:fathom, :tenant_pragma_allow, []),
        do: maybe_max_page_count(conn),
        else: :ok
    end
  end

  # Built from config (booleans and integers fathom owns, never tenant input), mirroring
  # `maybe_foreign_keys/1` and `maybe_cache_size/1` exactly.
  defp rw_reuse_sql do
    fk =
      if Application.get_env(:fathom, :foreign_keys, true),
        do: [";PRAGMA foreign_keys=ON"],
        else: []

    cache =
      case cache_size_kb() do
        nil -> []
        kb -> [";PRAGMA cache_size=-#{kb}"]
      end

    IO.iodata_to_binary([
      "SELECT 1 FROM sqlite_schema LIMIT 0;PRAGMA synchronous=FULL",
      fk,
      cache
    ])
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
    case cache_size_kb() do
      nil -> :ok
      kb -> Sqlite3.execute(conn, "PRAGMA cache_size=-#{kb}")
    end
  end

  # Bound the WAL's on-disk high-water mark (expert review 2026-10-10 #30). Without it a WAL that
  # once grew (a long reader holding off the checkpoint, one large transaction) keeps that size on
  # disk forever, since SQLite reuses the file in place after a checkpoint rather than shrinking it.
  # With it, the writer that resets the log truncates the file back to this many bytes.
  #
  # A2 IS SAFE WITH THIS, and what makes it safe is `Session.build_pushes/5`'s header re-check
  # (expert review 2026-10-12 #33), not the truncation being detectable on its own. A truncation
  # happens only at a log reset, which changes the WAL header's (ckpt_seq, salt1). A2 reads the
  # header, then the planned range through a held fd, then RE-READS the header and fails the round
  # with `:checkpoint_during_push` unless it is unchanged. That covers BOTH shapes: a reset that
  # truncates the file (the range then reads `:short_read`, which also fails the round), and a reset
  # whose new generation has already regrown PAST the planned range, where the pread succeeds and
  # returns new-generation bytes under the old header; only the header re-check catches that one,
  # and `replication_commit_test.exs` (#33) pins it. The next
  # round re-plans. Proved by running the whole suite at a limit of 0 (every reset truncates): no
  # A2 correctness test failed; three fixtures could no longer BUILD a reused-in-place WAL, which a
  # 64 MiB limit leaves intact for any WAL under it. `wal_truncate_test.exs` pins only the narrow
  # truncated-short-read direction. The rig check is recorded in the commit.
  #
  # Not a tenant pragma (the executor's allow-list does not admit it), so pooled reuse never sees a
  # changed value and it is not in `pinned_pragmas/0`. A negative value means "no limit".
  defp journal_size_limit_sql do
    case Application.get_env(:fathom, :journal_size_limit, 64 * 1024 * 1024) do
      bytes when is_integer(bytes) and bytes >= 0 -> [";PRAGMA journal_size_limit=#{bytes}"]
      _ -> []
    end
  end

  # The configured per-connection page cache in KiB, or nil when unset/invalid. Shared by the open,
  # the pooled-reuse batch and `pinned_pragmas/0`, which must agree on the exact value.
  defp cache_size_kb do
    case Application.get_env(:fathom, :shard_cache_size_kb, 2000) do
      kb when is_integer(kb) and kb > 0 -> kb
      _ -> nil
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
  @doc """
  Caps this connection's TEMP schema at the shard size cap (expert review 2026-10-08 #9).

  `max_page_count` caps `main` only, and a TEMP table lives in the temp schema, outside it — a
  long-lived stream grew one statement by statement past `:shard_max_bytes` (four 100 MB inserts
  under a 50 MB cap) onto node disk. CALLED LAZILY, before a statement that can create a TEMP
  object, never at open: setting `temp.max_page_count` makes SQLite create the connection's temp
  database, which cost ~85 KiB per connection when this ran at open — the served-density bench gate
  refused it (195 -> 287 KiB per served shard). Idempotent and cheap, so callers need not track
  whether it already ran. The temp database's page size is SQLite's default, 4096: it is fixed when
  the temp database is created and `page_size` is not tenant-settable, and `temp_store` (which would
  discard the temp database and this cap) is not on the tenant allow-list.
  """
  @spec cap_temp(reference()) :: :ok | {:error, term()}
  def cap_temp(conn) do
    case temp_cap_pages() do
      nil -> :ok
      n -> Sqlite3.execute(conn, "PRAGMA temp.max_page_count=#{n}")
    end
  end

  # The TEMP cap in pages, or nil when the size cap is off. Shared with `pinned_pragmas/0`, which
  # must allow exactly the value `cap_temp/1` sends.
  defp temp_cap_pages do
    case {Application.get_env(:fathom, :shard_max_page_count),
          Application.get_env(:fathom, :shard_max_bytes, @default_max_bytes)} do
      {n, _} when is_integer(n) and n > 0 -> n
      {n, _} when is_integer(n) -> nil
      {_, bytes} when is_integer(bytes) and bytes > 0 -> max(div(bytes, 4096), 1)
      _ -> nil
    end
  end

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

  `deadline: true` bounds the statement by `:query_timeout_ms`. Only CLIENT SQL passes it — see
  the comment in the body for why fathom's own statements do not.

  `read?: true` is the caller's promise that the statement's leading verb is provably incapable of
  mutating the database (`ShardExecutor`'s `read_verb?/1` whitelist: select / values / explain).
  When the statement also returns columns, `sqlite3_changes()` is not fetched (a dirty-NIF
  dispatch) and `num_changes` is reported as 0; the caller must not need the inherited count.

  `control?: true` is the caller's promise that the statement is transaction control (BEGIN /
  COMMIT / END / ROLLBACK / SAVEPOINT / RELEASE): `num_changes` is reported as 0 and no rowid
  without fetching either (expert review 2026-10-20-2027 #1).

  `conn_state?: true` fetches changes, last rowid and the autocommit flag in one statement step
  via the extension's `fathom_conn_state()` and reports the flag as `autocommit?` (expert review
  2026-10-17 #6); `autocommit?` is nil when that path was not taken. The CALLER decides when it is
  worth it: it replaces the single `changes` fetch one-for-one, so it only pays off where the
  caller would otherwise ask `autocommit?/1` right after (a DML statement, or a COMMIT boundary
  that may ship, expert review 2026-10-20 #27).
  """
  @spec query(reference(), String.t(), list() | {list(), [{String.t(), term()}]}, keyword()) ::
          {:ok, map()} | {:error, term()}
  def query(conn, sql, args, opts \\ []) do
    dml? = Keyword.get(opts, :dml?, true)
    read? = Keyword.get(opts, :read?, false)
    control? = Keyword.get(opts, :control?, false)
    state? = Keyword.get(opts, :conn_state?, false)

    # Statement deadline (expert review 2026-07-14 #26): when `:query_timeout_ms` is set, a runaway
    # query (missing-index full scan) is interrupted so it can't pin memory and keep the shard busy
    # (blocking eviction/drain/handoff). Only armed when configured — unset ⇒ no watchdog, no
    # overhead (the default, matching fathom's other protective knobs).
    #
    # CLIENT SQL ONLY (expert review 2026-10-08 #11, verified by execution). This applied to every
    # caller, so fathom's own durability work ran under the tenant's 30 s budget: with the deadline
    # at 20 ms a 58 MB shard's snapshot returned `{:error, :query_timeout}` and `Integrity.verify`
    # `{:quick_check_failed, :query_timeout}`. In prod that is a shard large or I/O-starved enough
    # that its `quick_check` passes 30 s — it can then never complete a periodic flush (unbounded
    # RPO), and its drop keeps the local copy instead of uploading it. The deadline bounds what a
    # CLIENT can make the node do; `Fathom.ShardExecutor` is the one path that runs client SQL, and
    # it opts in.
    # Busy-queue admission (expert review 2026-10-20-2027 #13, see `busy_gate/1`): while this
    # shard has queued lock waiters, a DML statement outside any transaction joins the queue
    # instead of barging past it.
    if dml? and not read? and busy_waiters?(), do: busy_gate(conn)

    result =
      case Keyword.get(opts, :deadline, false) && timeout_ms() do
        falsy when falsy in [nil, false] ->
          do_query(conn, sql, args, dml?, read?, control?, state?, nil)

        ms when is_integer(ms) and ms > 0 ->
          with_deadline(conn, ms, fn deadline_at ->
            do_query(conn, sql, args, dml?, read?, control?, state?, deadline_at)
          end)

        _ ->
          do_query(conn, sql, args, dml?, read?, control?, state?, nil)
      end

    # Busy-queue handoff (expert review 2026-10-20-2027 #13, see `busy_wait/6`). A read-verb
    # statement never frees a write lock, and a BUSY result never held one, so neither wakes.
    busy_ungate()
    if not read? and busy_waiters?(), do: wake_after(conn, result)
    result
  rescue
    e in ArgumentError -> {:error, Exception.message(e)}
  after
    # Idempotent; here so a raise cannot strand the gate ticket at the head of the queue.
    busy_ungate()
  end

  defp do_query(conn, sql, args, dml?, read?, control?, state?, deadline_at) do
    # A 4-tuple with a plain pattern, not a 3-tuple plus a guard. A `with` whose pattern carries a
    # guard lets the unmatched value fall through as the function's return, which dialyzer caught
    # as `{:ok, _, _} | {:refresh, reference(), [any()]}` escaping `query/4`'s spec.
    with {:ok, stmt, columns, cols_encoded, refresh_columns?} <-
           cached_stmt(conn, sql, deadline_at) do
      # The statement is owned by this connection's cache and must leave here RESET (sqlite3_reset
      # ends the execution, releasing its locks / read-transaction hold, while keeping the compiled
      # plan for the next execution of the same SQL).
      #
      # Perf review 2026-10-14 #7: that used to be an unconditional `Sqlite3.reset` in an `after`,
      # one extra dirty-NIF dispatch on EVERY statement. exqlite 0.42's `multi_step` (the only
      # thing that steps the statement here) already resets on SQLITE_DONE, SQLITE_BUSY and every
      # step error (sqlite3_nif.c exqlite_multi_step), so on those paths the statement is already
      # reset. What is left to reset by hand:
      #   * a `{:rows, _}` continuation abandoned by the row cap (`:too_many_rows`) — the statement
      #     is mid-execution and holds its read transaction;
      #   * an exception or throw/exit unwinding through here (post-step column refresh, a raise from
      #     the deadline wrapper).
      # A `bind` failure never steps the statement, so there is nothing to reset. Pinned by
      # `NifDispatchTest`: a second handle's `wal_checkpoint(TRUNCATE)` after every outcome.
      try do
        with :ok <- bind_args(stmt, args),
             {:ok, rows, row_count} <-
               reset_if_abandoned(stmt, step_all(conn, stmt, row_cap(), deadline_at)) do
          # Post-step column read when the schema moved under this connection (expert review
          # 2026-08-26 #7). Only on the `:refresh` path — a DDL happened somewhere since this
          # statement was cached — so the ordinary hit pays nothing. See `cached_stmt/2` for why
          # this cannot be done any earlier.
          {columns, cols_encoded} =
            if refresh_columns?,
              do: refreshed_columns(conn, stmt, sql, columns, cols_encoded),
              else: {columns, cols_encoded}

          # `sqlite3_changes()` is a dirty-NIF dispatch. `ShardExecutor.wrote?/2` ignores it for a
          # read-verb statement that returns columns (it answers `false` before looking), and
          # `to_stmt_result/2` reports 0 affected rows for `cols != [] and not dml?`, which a
          # read verb always is. So the caller says so with `read?` and the fetch is skipped. Never
          # set for a `WITH` statement, whose `INSERT … RETURNING` body is only detectable by
          # `changes > 0`.
          #
          # Expert review 2026-10-17 #6: for a DML statement whose caller asked (`conn_state?`, set
          # when replication is ON, which also needs the autocommit flag right after), the
          # extension's `fathom_conn_state()` returns `changes`, `last_insert_rowid` and autocommit
          # in ONE statement step: 1 dirty dispatch instead of 3 (changes + rowid + the executor's
          # separate `transaction_status`). Not used otherwise: without the autocommit read, the
          # step costs about what `changes` + `last_insert_rowid` cost (measured 4.0 vs 3.9 us), so
          # it would be no win. Expert review 2026-10-20 #27: the executor ALSO asks for it on a
          # COMMIT/END/RELEASE boundary under replication, where it would otherwise follow the
          # `changes` fetch with an `autocommit?/1` dispatch (2 extra -> 1). A BEGIN keeps the
          # single `changes` fetch: nothing reads its autocommit flag.
          {changes, fast_rowid, autocommit} =
            case if(state?, do: fetch_conn_state(conn)) do
              {:ok, ch, rid, ac} ->
                {ch, rid, ac}

              _ ->
                {if(control? or (read? and columns != []), do: 0, else: changes(conn)), :lazy,
                 nil}
            end

          # Expert review 2026-10-20-2027 #1: a transaction-control statement (BEGIN / SAVEPOINT /
          # RELEASE / ROLLBACK / COMMIT) modifies nothing, so it reports 0 changes and no rowid
          # even when the fast path returned the PREVIOUS write's inherited values.
          changes = if control?, do: 0, else: changes

          {:ok,
           %{
             columns: columns,
             # Wire form of `columns`, cached beside them (expert review 2026-10-19 #10); every
             # path that replaces a cached column list replaces this with it.
             cols_encoded: cols_encoded,
             rows: rows,
             row_count: row_count,
             num_changes: changes,
             # nil when the fast path was not taken (the caller then asks `autocommit?/1`).
             autocommit?: autocommit,
             # Only fetch the rowid when the caller can actually use it (expert review
             # 2026-07-24 #18; tightened by perf review 2026-10-14 #7). Sqlite3.last_insert_rowid/1
             # is ERL_NIF_DIRTY_JOB_IO_BOUND — a full dirty-scheduler dispatch — and
             # ShardExecutor.to_stmt_result/2 reports it only when `not read_only? and changes > 0`
             # (`read_only? = cols != [] and not dml?`), i.e. `(dml? or cols == []) and changes > 0`.
             # That is exactly this predicate, so what the caller sees is byte-identical; BEGIN,
             # COMMIT, SAVEPOINT and no-op statements (changes == 0) no longer pay the dispatch.
             last_insert_rowid:
               if(changes > 0 and (dml? or columns == []),
                 do: if(fast_rowid == :lazy, do: last_rowid(conn), else: fast_rowid)
               )
           }}
        end
      rescue
        e ->
          _ = Sqlite3.reset(stmt)
          reraise e, __STACKTRACE__
      catch
        kind, reason ->
          _ = Sqlite3.reset(stmt)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  # RETRY A YIELDED LOCK WAIT FROM THIS PROCESS (expert review 2026-10-20 #2).
  #
  # The busy handler used to sleep out a lock wait (SQLite's 1..100 ms ladder, up to the 5 s busy
  # timeout) INSIDE the `multi_step` NIF, which runs on a dirty-IO thread. The node has +SDio (10)
  # of them for ALL SQLite work and file I/O, so more same-lock waiters than that stalled every
  # tenant on the node: measured 0 ms -> ~2.1 s for an unrelated shard's SELECT between 9 and 11
  # waiters, with the lock holder's own COMMIT queued behind its waiters. Tenant handles now carry
  # `fathom_busy_yield(1)` (`maybe_harden/3`): the handler gives up at once, and the wait happens
  # here, in `receive after`, holding no thread.
  #
  # Retried only when re-running the statement is valid, which is exactly when the BUSY came from
  # the busy handler: SQLite calls it only while the connection holds no transaction on the database
  # (no lock, no snapshot to go stale), and `multi_step` has already reset the statement. That is
  # autocommit, `BEGIN IMMEDIATE`/`EXCLUSIVE` themselves, AND an explicit transaction's first
  # statement — Django's `transaction.atomic` (BEGIN, then a write), the likeliest many-writer shape.
  # A transaction that already READ and then cannot upgrade to a writer, or `SQLITE_BUSY_SNAPSHOT`,
  # returns BUSY without calling the handler and can never succeed on a re-step; exqlite reports it
  # as the same `:busy` (no extended result codes), so the extension remembers which statement it
  # yielded for and `fathom_busy_retry_ms()` (one dispatch, run as the very next statement) answers 0
  # for anything else — that BUSY fails at once, as before. Checked on the first BUSY only: a
  # re-step reaches the same point in the same state. A BUSY after rows were already returned is not
  # retried either.
  #
  # BOUNDED by the handle's busy timeout (read from the extension, so `set_busy_timeout/2` and a
  # tenant's own lowered `fathom_busy_timeout` still apply) and by the statement deadline, so a
  # timeout still maps to FILO_QUERY_TIMEOUT in `with_deadline/3`. No extension (cargo absent) -> no
  # yield, the handler waited out the full timeout already, and the budget read fails -> no retry.
  #
  # FAIRNESS (expert review 2026-10-20-2027 #13). The first version backed off on SQLite's own
  # 1..100 ms ladder, per waiter and per statement. That is ~LIFO: a waiter already on 50-100 ms
  # steps loses the lock to every newcomer still on 1-2 ms steps, so the hot shard's write tail
  # was bad (in-process probe `busy_fairness_bench_test.exs`, K closed-loop writers on one shard:
  # K=16 p99 22 ms / max 946 ms, K=32 p99 38 ms / max 2.2 s). A plain shorter ladder (capped at
  # 20 ms) was tried first and made it WORSE (K=32 ~38 -> 67-85 ms): more blind retriers collide
  # on the lock. So the wait is now a per-shard FIFO queue (`busy_wait/6`):
  #
  #   * a statement that gets a yielded BUSY joins its shard's queue (an ordered ETS set keyed by
  #     `{path, arrival}`) and waits for a wake message, not a timer;
  #   * a statement on that shard that ends with the connection in AUTOCOMMIT (an autocommit write,
  #     a COMMIT/ROLLBACK — the moments the write lock goes free) wakes the queue's HEAD only
  #     (`wake_busy_head/2`, called from `query/4`); the head retries, and when its own statement
  #     completes it wakes the next;
  #   * while a queue exists, a new autocommit DML statement joins it instead of barging
  #     (`busy_gate/1`, which says why BEGIN and in-transaction statements are not gated);
  #   * only the head also polls, on a ladder capped at 20 ms (`@busy_head_poll_ms`): the lock can
  #     be held by something that never wakes anyone — fathom's own handles (checkpoint, snapshot),
  #     a tenant script, a stream that rolled back by closing. Non-head waiters poll only every
  #     `@busy_tail_poll_ms` as a liveness backstop (a head that died without dequeuing).
  #
  # The hot path pays one `:persistent_term` read + one `:atomics` read per statement (the
  # node-wide waiter count, `busy_waiters?/0`); everything else runs only while someone waits.
  @busy_ladder_ms {1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100}
  @busy_head_poll_ms {1, 2, 5, 10, 15, 20}
  @busy_tail_poll_ms 100
  @busy_queue __MODULE__.BusyQueue
  @busy_waiters {__MODULE__, :busy_waiters}
  @busy_gate_key {__MODULE__, :busy_gate}
  @busy_gate_max_ms 1_000

  # exqlite's message for a `prepare` the busy handler gave up on (SQLITE_BUSY).
  @locked "database is locked"

  defp step_all(conn, stmt, cap, deadline_at) do
    case collect(conn, stmt, cap) do
      :busy_before_rows -> retry_busy(conn, stmt, cap, deadline_at)
      other -> other
    end
  end

  defp retry_busy(conn, stmt, cap, deadline_at) do
    with {:ok, budget_ms} when budget_ms > 0 <- busy_retry_budget_ms(conn) do
      # `busy_wait/6` serves both the step and the prepare retry, so dialyzer's success typing for
      # it is the union of both; the match narrows it back to what `collect/3` returns.
      case busy_wait(
             conn,
             :step,
             give_up_at(budget_ms, deadline_at),
             fn -> collect(conn, stmt, cap) end,
             &(&1 == :busy_before_rows),
             {:error, :busy}
           ) do
        {:ok, _rows, _count} = done -> done
        {:error, _} = err -> err
      end
    else
      _ -> {:error, :busy}
    end
  end

  # A BUSY AT PREPARE TIME (round-4 durability review 2026-10-20 F4). Preparing a statement on a
  # handle whose schema is not loaded reads the schema, which needs a SHARED lock, so the busy handler
  # can yield DURING `prepare` (e.g. while another handle holds EXCLUSIVE in rollback-journal mode, or
  # a last-close checkpoint+unlink). `prepare` reports that as the message "database is locked", and
  # only a step-time BUSY used to be retried, so this failed at once. Same budget and queue as
  # `retry_busy/4`; re-preparing is valid for the same reason (the handler runs only while the
  # connection holds no transaction on the database).
  defp prepare_retrying(conn, sql, deadline_at) do
    case Sqlite3.prepare(conn, sql) do
      {:error, @locked} = locked ->
        with {:ok, budget_ms} when budget_ms > 0 <- busy_retry_budget_ms(conn) do
          # Narrowed for dialyzer, as in `retry_busy/4`.
          case busy_wait(
                 conn,
                 :prepare,
                 give_up_at(budget_ms, deadline_at),
                 fn -> Sqlite3.prepare(conn, sql) end,
                 &(&1 == locked),
                 locked
               ) do
            {:ok, stmt} when is_reference(stmt) -> {:ok, stmt}
            {:error, _} = err -> err
          end
        else
          _ -> locked
        end

      other ->
        other
    end
  end

  defp give_up_at(budget_ms, deadline_at) do
    give_up_at = System.monotonic_time(:millisecond) + budget_ms
    if deadline_at, do: min(give_up_at, deadline_at), else: give_up_at
  end

  @doc false
  # Called from `create_live_table/0` at boot. Without it (a bare script) the queue is off and a
  # yielded wait falls back to the plain ladder.
  @spec create_busy_queue() :: :ok
  def create_busy_queue do
    if :ets.whereis(@busy_queue) == :undefined do
      :ets.new(@busy_queue, [:named_table, :public, :ordered_set])
    end

    if :persistent_term.get(@busy_waiters, nil) == nil,
      do: :persistent_term.put(@busy_waiters, :atomics.new(1, signed: true))

    :ok
  end

  # One yielded-lock wait episode: enqueue, wait/retry until `attempt` stops returning BUSY or the
  # budget runs out, dequeue (in `after`, so a raise cannot strand the entry), one telemetry event.
  #
  # A statement admitted through `busy_gate/1` keeps its gate ticket (and so its place in line)
  # for its retry; the gate's owner dequeues it.
  defp busy_wait(conn, kind, give_up_at, attempt, busy?, gave_up) do
    started = System.monotonic_time(:microsecond)
    gate = Process.get(@busy_gate_key)
    ticket = gate || busy_enqueue(conn)

    {result, attempts, oversleep_us} =
      try do
        busy_loop(ticket, give_up_at, attempt, busy?, gave_up, 0, 0)
      after
        if gate == nil, do: busy_dequeue(ticket)
      end

    :telemetry.execute(
      [:fathom, :shard, :busy_retry],
      %{
        attempts: attempts,
        wait_us: System.monotonic_time(:microsecond) - started,
        oversleep_us: oversleep_us
      },
      %{kind: kind, result: if(busy?.(result) or result == gave_up, do: :busy, else: :ok)}
    )

    result
  end

  defp busy_loop(ticket, give_up_at, attempt, busy?, gave_up, n, oversleep_us) do
    left = give_up_at - System.monotonic_time(:millisecond)

    if left <= 0 do
      {gave_up, n, oversleep_us}
    else
      wait_ms = min(busy_poll_ms(ticket, n), left)
      t0 = System.monotonic_time(:microsecond)

      oversleep_us =
        if busy_await(ticket, wait_ms) == :woken,
          do: oversleep_us,
          else: oversleep_us + max(0, System.monotonic_time(:microsecond) - t0 - wait_ms * 1000)

      result = attempt.()

      if busy?.(result),
        do: busy_loop(ticket, give_up_at, attempt, busy?, gave_up, n + 1, oversleep_us),
        else: {result, n + 1, oversleep_us}
    end
  end

  # No ticket (no queue table, or not a tenant handle): a plain sleep.
  defp busy_await(nil, wait_ms) do
    receive do
    after
      wait_ms -> :timeout
    end
  end

  defp busy_await({_key, alias}, wait_ms) do
    receive do
      {:fathom_busy_wake, ^alias} -> :woken
    after
      wait_ms -> :timeout
    end
  end

  # No ticket: the plain SQLite ladder, as before the queue.
  defp busy_poll_ms(nil, n), do: ladder(@busy_ladder_ms, n)

  defp busy_poll_ms(ticket, n) do
    if busy_head?(ticket), do: ladder(@busy_head_poll_ms, n), else: @busy_tail_poll_ms
  end

  defp ladder(steps, n), do: elem(steps, min(n, tuple_size(steps) - 1))

  # ADMISSION. Waking only the head is not enough on its own: the stream that just released the
  # lock is usually back with its next write before the woken head has been scheduled, so the head
  # lost nearly every handoff and the queue barely moved (measured, K=32 autocommit writers in a
  # closed loop: p99 38 -> 122 ms with wake-only, throughput halved). So while a shard has a queue,
  # a DML statement (`dml?`, the executor's verb check) on a connection that holds no transaction
  # (autocommit: the one dirty dispatch this costs, paid only then) queues at the tail and runs
  # when woken. Not gated:
  #   * a connection INSIDE a transaction: it may hold the write lock the head is waiting for, and
  #     queuing it would deadlock until the poll fallback;
  #   * BEGIN and other non-DML statements: a deferred BEGIN takes no lock, and gating it would
  #     hold the queue's turn across the client's whole transaction, reads and round trips
  #     included (Django's ATOMIC_REQUESTS wraps every view, read-only ones too).
  # Bounded: the head proceeds after its first poll, a non-head after `@busy_gate_max_ms`, so a
  # lost wake costs latency, never liveness.
  defp busy_gate(conn) do
    with path when is_binary(path) <- busy_shard(conn),
         {_, _} = head <- busy_head(path),
         :ok <- reap_if_dead(head),
         true <- autocommit?(conn),
         {_, _} = ticket <- busy_enqueue_path(path) do
      Process.put(@busy_gate_key, ticket)
      gate_loop(ticket, System.monotonic_time(:millisecond) + @busy_gate_max_ms, 0)
    end
  end

  defp gate_loop(ticket, until, n) do
    head? = busy_head?(ticket)
    wait_ms = if head?, do: ladder(@busy_head_poll_ms, n), else: @busy_tail_poll_ms

    cond do
      busy_await(ticket, wait_ms) == :woken -> :ok
      head? or System.monotonic_time(:millisecond) >= until -> :ok
      true -> gate_loop(ticket, until, n + 1)
    end
  end

  defp busy_ungate do
    case Process.delete(@busy_gate_key) do
      nil -> :ok
      ticket -> busy_dequeue(ticket)
    end
  end

  # A ticket is `{key, alias}`: `key = {path, arrival}` orders the shard's queue in ETS, and the
  # wake message goes to the waiter's process ALIAS. Dequeuing unaliases it, so a wake that loses
  # the race with the dequeue is dropped by the runtime instead of landing in the stream process's
  # mailbox, where it would sit forever (or reach a `handle_info` that does not expect it).
  defp busy_enqueue(conn) do
    case busy_shard(conn) do
      path when is_binary(path) -> busy_enqueue_path(path)
      _ -> nil
    end
  end

  defp busy_enqueue_path(path) do
    case :persistent_term.get(@busy_waiters, nil) do
      nil ->
        nil

      counter ->
        key = {path, :erlang.unique_integer([:monotonic, :positive])}
        alias = :erlang.alias()
        true = :ets.insert(@busy_queue, {key, self(), alias})
        :atomics.add(counter, 1, 1)
        {key, alias}
    end
  end

  defp busy_dequeue(nil), do: :ok

  defp busy_dequeue({key, alias}) do
    _ = :erlang.unalias(alias)
    busy_remove(key)
    flush_busy_wakes(alias)
  end

  defp busy_remove(key) do
    if :ets.take(@busy_queue, key) != [],
      do: :atomics.sub(:persistent_term.get(@busy_waiters), 1, 1)

    :ok
  end

  defp flush_busy_wakes(alias) do
    receive do
      {:fathom_busy_wake, ^alias} -> flush_busy_wakes(alias)
    after
      0 -> :ok
    end
  end

  # Only TENANT handles carry their path in the live table (`open/2`): fathom's own handles never
  # yield a lock wait, so they neither queue, nor are gated, nor wake anyone.
  defp busy_shard(conn) do
    case :ets.lookup(@live_table, live_key(conn)) do
      [{_, path}] -> path
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  # The first queue key for `path`, or nil. Arrival numbers are positive, so `{path, 0}` sorts
  # immediately before the shard's first key.
  defp busy_head(path) do
    case :ets.next(@busy_queue, {path, 0}) do
      {^path, _} = key -> key
      _ -> nil
    end
  end

  defp busy_head?({{path, _} = key, _alias}), do: busy_head(path) == key

  defp busy_waiters? do
    case :persistent_term.get(@busy_waiters, nil) do
      nil -> false
      counter -> :atomics.get(counter, 1) > 0
    end
  end

  # A waiter killed mid-wait never ran its `after`, so its row would stay at the head of the queue
  # and every gated writer behind it would wait out `@busy_gate_max_ms` (measured: 994 ms without
  # this). Checked where a head is about to be trusted: on admission, and on each wake. A ghost on
  # a shard that never sees another write stays until that shard's next contended statement; its
  # only cost meanwhile is that `busy_waiters?/0` reads true, so each non-read statement node-wide
  # pays the two ETS reads that find its own shard has no queue.
  defp reap_if_dead(key) do
    case :ets.lookup(@busy_queue, key) do
      [{_, pid, _alias}] -> if Process.alive?(pid), do: :ok, else: busy_remove(key)
      [] -> :ok
    end
  end

  # After a statement: if anyone waits on this shard and the connection is now in autocommit (so
  # it holds no write lock), hand the lock to the queue's head. `autocommit` is the flag the
  # statement already fetched (`conn_state?`), or nil to ask SQLite — a dirty dispatch, paid only
  # while someone is queued on this shard.
  defp wake_busy_head(conn, autocommit) do
    with path when is_binary(path) <- busy_shard(conn),
         {_, _} <- busy_head(path),
         true <- if(is_boolean(autocommit), do: autocommit, else: autocommit?(conn)) do
      wake_head(path)
    end

    :ok
  end

  defp wake_head(path) do
    with {_, _} = key <- busy_head(path) do
      case :ets.lookup(@busy_queue, key) do
        [{_, pid, alias}] ->
          if Process.alive?(pid) do
            send(alias, {:fathom_busy_wake, alias})
          else
            busy_remove(key)
            wake_head(path)
          end

        [] ->
          wake_head(path)
      end
    end
  end

  defp wake_after(_conn, {:error, :busy}), do: :ok
  defp wake_after(conn, {:ok, %{autocommit?: ac}}), do: wake_busy_head(conn, ac)
  defp wake_after(conn, _), do: wake_busy_head(conn, nil)

  # How long to keep retrying: the handle's busy timeout when the handler yielded for the statement
  # that just returned BUSY, else 0
  # (native/fathom_udf/src/backstop.rs `fathom_busy_retry_ms`). One dispatch on the contended path.
  defp busy_retry_budget_ms(conn) do
    with {:ok, stmt, _cols, _enc, _refresh} <- cached_stmt(conn, "SELECT fathom_busy_retry_ms()") do
      case Sqlite3.multi_step(conn, stmt, 1) do
        {:done, [[ms]]} when is_integer(ms) ->
          {:ok, ms}

        {:rows, [[ms]]} when is_integer(ms) ->
          _ = Sqlite3.reset(stmt)
          {:ok, ms}

        _ ->
          _ = Sqlite3.reset(stmt)
          :error
      end
    else
      _ -> :error
    end
  end

  # Plain positional args take exqlite's bind untouched (the hot path). `{positional, named}` is a
  # Hrana statement carrying `named_args` (expert review 2026-10-20 #10): each name is resolved to
  # its parameter index with `sqlite3_bind_parameter_index` (exqlite's `bind_parameter_index`), the
  # values are merged with the positional ones into ONE index-ordered list, and exqlite's own bind
  # does the (parameterized) binding and the count check. No SQL parsing, no interpolation.
  # exqlite's own map bind cannot be used: it needs every parameter named and every name prefixed.
  # Per the Hrana spec a client may send the name with or without its `:`/`@`/`$` prefix.
  defp bind_args(stmt, args) when is_list(args) or is_nil(args), do: Sqlite3.bind(stmt, args)

  defp bind_args(stmt, {positional, named}) do
    case Sqlite3.bind_parameter_count(stmt) do
      {:error, reason} ->
        {:error, reason}

      count ->
        slots =
          positional
          |> Enum.with_index(1)
          |> Map.new(fn {value, idx} -> {idx, value} end)

        slots =
          Enum.reduce(named, slots, fn {name, value}, acc ->
            idx = named_index(stmt, name)

            cond do
              idx == 0 ->
                raise ArgumentError, "unknown named parameter: #{inspect(name)}"

              Map.has_key?(acc, idx) ->
                raise ArgumentError,
                      "parameter #{inspect(name)} (index #{idx}) is bound twice " <>
                        "(positional and named arguments overlap)"

              true ->
                Map.put(acc, idx, value)
            end
          end)

        if map_size(slots) != count or Enum.any?(Map.keys(slots), &(&1 > count)) do
          raise ArgumentError,
                "expected #{count} arguments, got #{map_size(slots)} " <>
                  "(#{length(positional)} positional, #{length(named)} named)"
        end

        Sqlite3.bind(stmt, for(i <- 1..count//1, do: Map.fetch!(slots, i)))
    end
  end

  defp named_index(stmt, <<c, _::binary>> = name) when c in [?:, ?@, ?$],
    do: param_index(stmt, name)

  defp named_index(stmt, name) do
    Enum.find_value([":", "@", "$"], 0, fn prefix ->
      case param_index(stmt, prefix <> name) do
        0 -> nil
        idx -> idx
      end
    end)
  end

  defp param_index(stmt, name) do
    case Exqlite.Sqlite3NIF.bind_parameter_index(stmt, name) do
      idx when is_integer(idx) -> idx
      _ -> 0
    end
  end

  # The row cap stops `collect/5` while the statement may still be mid-execution (after a
  # `{:rows, _}` batch), and nothing else will reset it now that `multi_step`'s own resets are the
  # only ones. After a `{:done, _}` the extra reset is a harmless no-op.
  defp reset_if_abandoned(stmt, {:error, {:too_many_rows, _}} = err) do
    _ = Sqlite3.reset(stmt)
    err
  end

  defp reset_if_abandoned(_stmt, result), do: result

  # Re-read the column list now that the statement has stepped, and correct the cache entry so the
  # next execution takes the ordinary hit path. Falls back to the pre-step list if the read fails —
  # degrading to the old (possibly stale) answer rather than failing a query that already succeeded.
  defp refreshed_columns(conn, stmt, sql, fallback, fallback_encoded) do
    case Sqlite3.columns(conn, stmt) do
      {:ok, cols} ->
        key = {__MODULE__, :stmt_cache, conn}
        encoded = encode_cols(cols)

        case Process.get(key) do
          {seq, %{^sql => {^stmt, stamp, gen, bytes, packed}} = cache} ->
            {canonical, _cols, _enc} = decoded(conn, stmt, packed, sql)
            entry = {stmt, stamp, gen, bytes, pack(cols, encoded)}
            Process.put(key, {seq, Map.put(cache, canonical, entry)})
            put_decoded(conn, stmt, {canonical, cols, encoded})

          _ ->
            :ok
        end

        {cols, encoded}

      _ ->
        {fallback, fallback_encoded}
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

  # THE CACHE IS ALSO BOUNDED BY BYTES (expert review 2026-10-20-2027 #7). The count cap alone was
  # blind to statement width: measured with `fathom_stmt_memused` (the extension's
  # `sqlite3_stmt_status(SQLITE_STMTSTATUS_MEMUSED)`), a 29-column Django SELECT on `ks_scalars`
  # holds 10-19 KiB of SQLite heap where a point read holds ~2 KiB, so 64 wide statements is
  # ~1 MiB per connection and a node's stream count multiplies it. `soft_heap_limit` cannot reclaim
  # it (a prepared statement's memory is not cache). Each entry records its MEMUSED at prepare
  # (it does not change on execution — measured identical after the first and second step), and a
  # miss evicts LRU entries until the sum fits `:stmt_cache_bytes` (default 256 KiB; nil disables
  # the byte bound, leaving the count cap). The just-prepared statement is never evicted, so one
  # statement wider than the budget still caches. Only SQLite's side is counted: the BEAM side is a
  # few hundred bytes per entry beside it.
  @stmt_cache_bytes_default 256 * 1024
  @memused_sql "SELECT fathom_stmt_memused(?1)"

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
  defp cached_stmt(conn, sql, deadline_at \\ nil) do
    key = {__MODULE__, :stmt_cache, conn}
    {seq, cache} = Process.get(key, {0, %{}})
    gen = Fathom.Shard.SchemaGen.current()

    case cache do
      %{^sql => {stmt, _stamp, ^gen, bytes, packed}} ->
        {canonical, cols, enc} = decoded(conn, stmt, packed, sql)

        # Re-put under `canonical`, never the incoming sub-binary: a map that has grown past a
        # flatmap adopts the new key term, which would silently reinstate the pin.
        Process.put(
          key,
          {seq + 1, Map.put(cache, canonical, {stmt, seq + 1, gen, bytes, packed})}
        )

        {:ok, stmt, cols, enc, false}

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
      %{^sql => {stmt, _stamp, _older_gen, _bytes, packed}} ->
        {canonical, _cols, _enc} = decoded(conn, stmt, packed, sql)
        Sqlite3.release(conn, stmt)
        drop_decoded(conn, stmt)

        with {:ok, fresh} <- prepare_retrying(conn, canonical, deadline_at),
             {:ok, cols} <- Sqlite3.columns(conn, fresh) do
          bytes = stmt_bytes(conn, canonical)
          # Re-read: measuring ran a statement through this same cache.
          {seq, cache} = Process.get(key, {seq, cache})
          enc = encode_cols(cols)
          entry = {fresh, seq + 1, gen, bytes, pack(cols, enc)}
          Process.put(key, {seq + 1, Map.put(cache, canonical, entry)})
          put_decoded(conn, fresh, {canonical, cols, enc})

          {:ok, fresh, cols, enc, true}
        end

      _ ->
        with {:ok, stmt} <- prepare_retrying(conn, sql, deadline_at),
             {:ok, cols} <- Sqlite3.columns(conn, stmt) do
          canonical = :binary.copy(sql)
          bytes = stmt_bytes(conn, canonical)
          # Re-read: measuring ran a statement through this same cache (and may have cached it).
          {seq, cache} = Process.get(key, {seq, cache})
          cache = maybe_evict(conn, cache, bytes)

          enc = encode_cols(cols)
          entry = {stmt, seq + 1, gen, bytes, pack(cols, enc)}
          Process.put(key, {seq + 1, Map.put(cache, canonical, entry)})
          put_decoded(conn, stmt, {canonical, cols, enc})

          {:ok, stmt, cols, enc, false}
        end
    end
  end

  # Make room for a new entry of `incoming` bytes: release least-recently-used statements while the
  # cache is at the count cap or the bytes would exceed the budget. O(cache) per eviction, paid only
  # on a miss — a workload cycling more distinct statements than fit is re-preparing anyway, exactly
  # as before the cache.
  defp maybe_evict(conn, cache, incoming) do
    budget = Application.get_env(:fathom, :stmt_cache_bytes, @stmt_cache_bytes_default)
    evict_until_fits(conn, cache, incoming, budget, cache_bytes(cache))
  end

  defp evict_until_fits(_conn, cache, _incoming, _budget, _used) when map_size(cache) == 0,
    do: cache

  defp evict_until_fits(conn, cache, incoming, budget, used) do
    if map_size(cache) >= @stmt_cache_cap or (is_integer(budget) and used + incoming > budget) do
      {lru_sql, {stmt, _stamp, _gen, bytes, _packed}} =
        Enum.min_by(cache, fn {_sql, {_stmt, stamp, _gen, _bytes, _packed}} -> stamp end)

      Sqlite3.release(conn, stmt)
      drop_decoded(conn, stmt)
      evict_until_fits(conn, Map.delete(cache, lru_sql), incoming, budget, used - bytes)
    else
      cache
    end
  end

  defp cache_bytes(cache),
    do:
      Enum.reduce(cache, 0, fn {_sql, {_stmt, _stamp, _gen, bytes, _packed}}, acc ->
        acc + bytes
      end)

  # SQLite heap held by the just-prepared statement `sql`, via the extension's
  # `fathom_stmt_memused`. The probe statement lives in this same cache and is never measured itself
  # (that would recurse; it is ~1.6 KiB). If the extension is not loaded (config-disabled, or a dev
  # box without cargo) the probe cannot prepare and the size is ESTIMATED from the SQL length:
  # measured MEMUSED runs 13-27x the SQL text across the keystone mix, so 16x keeps the bound
  # meaningful rather than silently turning it off.
  #
  # fathom's own one-row probes are not measured either (they are ~1.6 KiB each), and for
  # `fathom_busy_retry_ms()` it is a correctness rule, not a saving: that function grants a retry
  # budget only when the PREVIOUS statement on the connection is the one that yielded BUSY
  # (src/backstop.rs), so running the memused probe in front of its first execution made every lock
  # wait give up at once (caught by busy_yield_test and query_bounds_test).
  defp stmt_bytes(_conn, sql)
       when sql in [
              @memused_sql,
              "SELECT fathom_busy_retry_ms()",
              "SELECT fathom_conn_state()"
            ],
       do: 0

  defp stmt_bytes(conn, sql) do
    with {:ok, probe, _cols, _enc, _refresh} <- cached_stmt(conn, @memused_sql),
         :ok <- Sqlite3.bind(probe, [sql]),
         {:done, [[n]]} when is_integer(n) <- Sqlite3.multi_step(conn, probe, 2) do
      n
    else
      _ -> 16 * byte_size(sql)
    end
  end

  # ENTRIES CARRY THEIR COLUMNS PACKED; THE DECODED FORM IS PROCESS-LOCAL (expert review
  # 2026-10-20-2027 #20). A pooled handle's cache crosses two process boundaries per stream cycle
  # (stream -> coordinator -> next stream), each a full heap copy, and an entry's column names and
  # their wire form were most of it (~60 small terms for a 29-column statement). So an entry holds
  # them as ONE refc binary (`packed`, which copies as a pointer), and the decoded
  # `{canonical, cols, enc}` the hit path uses lives in a separate per-process overlay keyed by the
  # statement ref, which `detach_statements/1` simply drops. The next stream decodes each entry on
  # its first hit (`binary_to_term`, ~1 us for 29 columns) and re-copies its canonical key from the
  # caller's SQL (`:binary.copy/1`, as on a miss). Entries are `{stmt, stamp, gen, bytes, packed}`.
  # Measured, 64 29-column entries, pooled round trip: 110 KiB / 62-81 us carried whole -> ~8 us.
  #
  # Rejected, both measured: (a) stripping the decoded fields at detach — 20 us at 64 entries,
  # because rebuilding a >32-key map re-hashes every key and the keys are the full SQL text (~800 B
  # for a wide SELECT); (b) keeping `canonical` in the entry — the copy is not sharing-preserving,
  # so the key and the element become two refc headers, and the round trip was 14 us, not 8.
  defp pack(cols, enc), do: :erlang.term_to_binary({cols, enc})

  defp decoded(conn, stmt, packed, sql) do
    case Process.get({__MODULE__, :stmt_cols, conn}) do
      %{^stmt => decoded} ->
        decoded

      _ ->
        {cols, enc} = :erlang.binary_to_term(packed)
        decoded = {:binary.copy(sql), cols, enc}
        put_decoded(conn, stmt, decoded)
        decoded
    end
  end

  defp put_decoded(conn, stmt, decoded) do
    key = {__MODULE__, :stmt_cols, conn}
    Process.put(key, Map.put(Process.get(key, %{}), stmt, decoded))
  end

  defp drop_decoded(conn, stmt) do
    key = {__MODULE__, :stmt_cols, conn}

    case Process.get(key) do
      %{^stmt => _} = overlay -> Process.put(key, Map.delete(overlay, stmt))
      _ -> :ok
    end
  end

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
    Process.delete({__MODULE__, :stmt_cols, conn})
    release_cache(conn, Process.delete({__MODULE__, :stmt_cache, conn}))
  end

  # The cached wire form, flattened: `Filo.StmtResult.encode_cols/1` returns the protobuf fields as
  # a nested iolist (~590 heap words for 30 columns); one binary is 35 words plus the bytes.
  defp encode_cols(cols) do
    %{json: json, protobuf: pb} = StmtResult.encode_cols(cols)
    %{json: json, protobuf: IO.iodata_to_binary(pb)}
  end

  defp release_cache(conn, {_seq, cache}) do
    Enum.each(cache, fn {_sql, {stmt, _stamp, _gen, _bytes, _packed}} ->
      Sqlite3.release(conn, stmt)
    end)
  end

  defp release_cache(_conn, nil), do: :ok

  # --- the statement cache travels with a POOLED handle (perf review 2026-10-14 #10) -------------
  #
  # A pooled checkin used to purge the stream's cache, so every reused stream re-prepared every
  # statement: measured 41 vs 24 us for a three-statement Django-shaped stream (purge + re-prepare
  # vs cache hits, 2026-10-04). The stream now DETACHES its cache at checkin and hands it to the
  # coordinator with the handle, and the next stream ADOPTS it.
  #
  # Safe to carry because: a statement holds no snapshot once it has returned DONE, and every
  # abandoned one is reset (`do_query/5`); each entry carries the SchemaGen generation it was
  # prepared under, so a schema change while parked re-prepares on first use (and reset_for_reuse/2
  # reloads the schema before the adopt); and the pool is per shard and scope, so a cache never
  # crosses tenants. What must NOT change is the `sqlite3_close_v2` invariant
  # (`release_owner_state/1`): every path that closes a pooled handle goes through `close_pooled/1`,
  # which finalizes the carried statements first. If the coordinator dies without closing, the cache
  # dies with its state and GC finalizes the statements, as it did for the stream before.
  #
  # Parked 2026-10-04 because the hand-off cost a serial one-shot client +8% (the release and the
  # cache message moved onto the coordinator, and the pool was drained on every close anyway). With
  # the pool drain grace (`Fathom.Shard`, perf review 2026-10-14 #4) that client's next request is a
  # pool HIT that adopts the cache, so the hand-off buys the prepares back instead of only costing.

  @typedoc "A pooled handle: a bare connection, or one carrying its last stream's statement cache."
  @type pooled :: reference() | {reference(), term()}

  @doc false
  # Remove this stream's statement cache WITHOUT releasing it, for handing to the pool.
  # Only the packed entries travel; the decoded overlay stays behind as garbage (see `pack/2`).
  @spec detach_statements(reference()) :: term()
  def detach_statements(conn) do
    Process.delete({__MODULE__, :stmt_cols, conn})
    Process.delete({__MODULE__, :stmt_cache, conn})
  end

  @doc false
  # Install a cache a previous stream detached, in THIS stream's dictionary.
  @spec adopt_statements(reference(), term()) :: :ok
  def adopt_statements(_conn, nil), do: :ok

  def adopt_statements(conn, cache) do
    Process.delete({__MODULE__, :stmt_cols, conn})
    Process.put({__MODULE__, :stmt_cache, conn}, cache)
    :ok
  end

  @doc """
  Release this connection's SQLite page cache (`PRAGMA shrink_memory`), for an IDLE pooled handle
  (expert review 2026-10-21 #15). Best effort: a handle closed underneath it just errors.
  """
  @spec shrink_memory(reference()) :: :ok | {:error, term()}
  def shrink_memory(conn) do
    Sqlite3.execute(conn, "PRAGMA shrink_memory")
  rescue
    e -> {:error, e}
  end

  @doc false
  # Stop this stream's watchdog only (the half of `release_owner_state/1` a pooled checkin still
  # needs once the statements travel with the handle).
  @spec release_watchdog(reference()) :: :ok
  def release_watchdog(conn), do: stop_watchdog(conn)

  @doc """
  Close a pooled handle: finalize the statement cache it carries, then close. The only way a
  pooled handle may be closed — see "the statement cache travels with a POOLED handle" above.
  """
  @spec close_pooled(pooled()) :: :ok
  def close_pooled({conn, cache}) do
    release_cache(conn, cache)
    close(conn)
  end

  def close_pooled(conn), do: close(conn)

  # Resolve `:query_max_rows` ONCE per query — the old shape re-read the app env inside every
  # collect batch (a 200k-row result did ~4k redundant get_envs in one query's hot loop).
  # Normalized here so collect's per-batch check is a bare integer comparison.
  defp row_cap do
    case Application.get_env(:fathom, :query_max_rows) do
      cap when is_integer(cap) and cap > 0 -> cap
      _ -> nil
    end
  end

  # Atomics layout shared by the owner and its watchdog: slot 1 is the running generation (bumped at
  # arm and again at disarm, so "armed" generations are odd and a finished one is never current);
  # slot 2 is the last generation whose deadline actually fired.
  @gen_slot 1
  @fired_slot 2

  # How often the watchdog re-issues a cancel that has not yet taken effect. See await_disarm/4.
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
    # NO MESSAGES PER STATEMENT (expert review 2026-10-20 #24). The deadline used to cost two sends
    # to a second process per statement (`{:arm}` + `{:done}`), a `make_ref`, an alias, and a
    # selective receive pair, and after idle each send woke the watchdog from a cold scheduler
    # (measured +2-3.5 us hot, +29-34 us median after 100 ms idle). Now:
    #
    #   * arm: bump the connection's generation counter (an `:atomics`, shared with the watchdog)
    #     and start a BEAM timer addressed to the watchdog. No wake-up: the timer wheel holds it.
    #   * disarm: bump the generation again and cancel the timer asynchronously. A timer that was
    #     already delivered is harmless: the watchdog compares the generation it carries with the
    #     live one before doing anything, so a fire for a finished statement is ignored.
    #   * the watchdog, on a genuine fire, sets the "timed out" mark for that generation BEFORE it
    #     cancels (same ordering as the old send-before-cancel, expert review 2026-08-01 #46), and
    #     the owner reads the mark after the statement returns. A mark in a shared atomic replaces
    #     the `{:timed_out, ref}` message, so there is no mailbox residue to sweep, nothing a
    #     `Filo.Stream` GenServer could crash on (2026-10-08 #27), and no alias.
    {watchdog, flags} = ensure_watchdog(conn)
    gen = :atomics.add_get(flags, @gen_slot, 1)
    tref = :erlang.start_timer(ms, watchdog, {:fire, gen})

    # Always disarm, even if fun raises (Sqlite3.bind raises ArgumentError on a bad bind value —
    # the reason query/3 has a rescue). Without try/after the raise unwinds before the disarm,
    # leaving the deadline armed to interrupt a LATER statement reusing this connection `ms` later
    # — a spurious FILO_QUERY_TIMEOUT on a healthy query (expert review 2026-07-18 #13).
    started = System.monotonic_time(:millisecond)

    result =
      try do
        fun.(started + ms)
      after
        :atomics.add(flags, @gen_slot, 1)
        _ = :erlang.cancel_timer(tref, async: true, info: false)
      end

    timed_out? = :atomics.get(flags, @fired_slot) == gen

    # A fire is in flight (or retrying its cancel): wake it so it stops now instead of at its next
    # retry tick, which keeps the synchronous `stop_watchdog/1` at pooled checkin immediate.
    if timed_out?, do: send(watchdog, :disarmed)

    case {timed_out?, result} do
      {true, {:error, _}} ->
        {:error, :query_timeout}

      # The extension's backstop stopped it (see `arm_backstop/1`) while the watchdog's timer had
      # not fired: an interrupt, or a lock wait given up as SQLITE_BUSY, with the deadline already
      # past is the same timeout.
      {false, {:error, reason}} ->
        if deadline_error?(reason) and System.monotonic_time(:millisecond) - started >= ms,
          do: {:error, :query_timeout},
          else: result

      _ ->
        result
    end
  end

  # SQLITE_INTERRUPT arrives as SQLite's message ("interrupted"); a lock wait given up arrives as
  # exqlite's `:busy` from step. Only consulted when the deadline has already passed.
  defp deadline_error?(:busy), do: true
  defp deadline_error?(@locked), do: true
  defp deadline_error?(reason) when is_binary(reason), do: reason =~ "interrupt"
  defp deadline_error?(_reason), do: false

  # `{pid, flags}` lives in the owner's dictionary beside the statement cache. A dead watchdog (it
  # can only die abnormally — e.g. a cancel on a torn-down conn) must not silently disable the
  # deadline for the rest of the stream: replace it. This is the ONE liveness check per statement;
  # the post-arm re-check the message protocol needed (2026-08-31 #22) is gone with it. The
  # residual window — the watchdog dying between this check and `start_timer` — self-heals on the
  # next statement, the same class the old re-check also left open.
  defp ensure_watchdog(conn) do
    key = {__MODULE__, :watchdog, conn}

    case Process.get(key) do
      {pid, _flags} = entry when is_pid(pid) ->
        if Process.alive?(pid) do
          entry
        else
          Process.delete(key)
          ensure_watchdog(conn)
        end

      nil ->
        owner = self()
        flags = :atomics.new(2, signed: false)
        pid = spawn(fn -> watchdog_init(conn, owner, flags) end)
        entry = {pid, flags}
        Process.put(key, entry)
        entry
    end
  end

  defp watchdog_init(conn, owner, flags) do
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
    watchdog_loop(conn, flags, mon)
  end

  # The fire handler, split out so a test can interleave the owner's disarm between the generation
  # check (R1) and the fired mark (W) through `before_mark` (expert review 2026-10-20-2027 #19). The
  # hook is a no-op in production.
  @doc false
  @spec handle_fire(reference(), :atomics.atomics_ref(), integer(), reference(), (-> any())) ::
          :ok
  def handle_fire(conn, flags, gen, mon, before_mark \\ fn -> :ok end) do
    if :atomics.get(flags, @gen_slot) == gen do
      before_mark.()

      # Mark BEFORE cancelling (expert review 2026-08-01 #46): the cancel is what lets the
      # statement return, so the owner's read of the mark afterwards cannot miss it.
      :atomics.put(flags, @fired_slot, gen)

      # Expert review 2026-10-20-2027 #19: re-read the generation AFTER the mark (R2). The owner
      # disarms (bumps the generation) and THEN reads the mark; the watchdog marks and THEN
      # re-reads the generation. Atomics are sequentially consistent, so either the owner sees
      # the mark (and `stop_watchdog/1` waits for this process) or this re-read sees the
      # disarm and no cancel is issued. That is what lets `stop_watchdog/1` skip the
      # synchronous wait for a watchdog whose mark it has not seen.
      if :atomics.get(flags, @gen_slot) == gen, do: await_disarm(conn, flags, gen, mon)
    end

    :ok
  end

  # Runs only when a deadline timer fires (or on `:stop` / owner death): between fires it sits in
  # `receive` and is never woken by a healthy statement.
  defp watchdog_loop(conn, flags, mon) do
    receive do
      {:timeout, _tref, {:fire, gen}} ->
        # A fire for a statement that already finished (timer delivered as the owner disarmed) is
        # ignored: the generation moved on. cancel/1, not interrupt/1 (expert review 2026-07-24 #1):
        # interrupt only aborts VDBE execution, so a statement parked in the busy handler waiting
        # for the write lock ignored it and slept out the full busy timeout. cancel/1 is a superset
        # — it sets the flag the busy handler polls AND calls sqlite3_interrupt — and is a
        # non-dirty NIF that deliberately avoids the connection mutex, so it cannot block behind
        # the running query. A spurious late cancel is a no-op: exqlite resets `cancelled` at the
        # top of every subsequent db op.
        handle_fire(conn, flags, gen, mon)

        watchdog_loop(conn, flags, mon)

      :stop ->
        exit(:normal)

      {:DOWN, ^mon, :process, _, _} ->
        owner_down(conn)

      # A `:disarmed` that raced past the fire it was meant for.
      :disarmed ->
        watchdog_loop(conn, flags, mon)
    end
  end

  # Expert review 2026-10-10 #23. The owner died, possibly mid-statement. Exiting without a cancel
  # left that statement running while holding the connection mutex, and the coordinator's
  # `close_lent` -> `Sqlite3.close` then blocked on it for the statement's remaining runtime,
  # stalling the whole shard. The owner is gone, so nothing can want the statement's result: cancel
  # it (a late cancel on an idle handle is a no-op — exqlite resets its flag at the next db op).
  defp owner_down(conn) do
    _ = Sqlite3.cancel(conn)
    exit(:normal)
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
  # the healthy path pays nothing — an interrupted query's `:disarmed` arrives long before
  # the first retry.
  #
  # Still blocking until the owner disarms (`:disarmed`, or the generation moving on), so the
  # watchdog cannot act on a NEWER statement's fire while these cancels are in flight. A retry
  # issued in the sliver between the statement finishing (`multi_step` returning) and the disarm is
  # the same no-op class the single cancel already accepted, not a new hazard.
  defp await_disarm(conn, flags, gen, mon) do
    Sqlite3.cancel(conn)

    receive do
      :disarmed -> :ok
      {:DOWN, ^mon, :process, _, _} -> owner_down(conn)
    after
      @cancel_retry_ms ->
        # The owner normally sends `:disarmed`; the generation check also ends the loop when it
        # could not (the fire raced the owner's mark read), so the watchdog never keeps cancelling
        # a LATER statement.
        if :atomics.get(flags, @gen_slot) == gen,
          do: await_disarm(conn, flags, gen, mon),
          else: :ok
    end
  end

  # SYNCHRONOUS: wait for the watchdog to be gone (expert review 2026-09-29 #33). With pooling the
  # handle is handed to ANOTHER stream right after this, and a watchdog that fired on this stream's
  # last query can still be between its retry timer and `Sqlite3.cancel/1` when the disarm is
  # signalled — a fire-and-forget `:stop` let that cancel land on the NEXT stream's running statement: a
  # SQLITE_INTERRUPT on an unrelated request, not even mapped to FILO_QUERY_TIMEOUT (that stream
  # never armed the ref). The watchdog handles its mailbox in order, so once it has exited every
  # cancel it issued has returned, and a returned cancel cannot touch a later statement (exqlite
  # resets its flag at the next op; `sqlite3_interrupt` with nothing running is a no-op).
  #
  # Cheap: an idle watchdog is blocked in `receive`, so this is one message round trip, paid only
  # when `:query_timeout_ms` is set. Bounded: a watchdog mid-`await_disarm` sees `:disarmed`
  # within @cancel_retry_ms; past @watchdog_stop_ms it is killed rather than waited on.
  @watchdog_stop_ms 1_000

  #
  # Expert review 2026-10-20-2027 #19: the wait is only needed when this watchdog EVER FIRED
  # (`@fired_slot` != 0): an unfired watchdog has never called `Sqlite3.cancel` on this handle, and
  # the re-read in `watchdog_loop/3` guarantees a fire the owner has not seen cannot cancel after
  # its disarm. So the common pooled checkin sends `:stop` and moves on instead of a monitor +
  # message + DOWN round trip (measured 15 us after a 20 ms gap vs 8 us spawn + async kill). The
  # `Process.delete` below still drops the handle's entry, so the next stream spawns a fresh one.
  #
  # The unfired async path ALSO relies on ERTS per-pair signal ordering: `:stop` is sent by the owner
  # before the owner can die, so it reaches the watchdog BEFORE the owner's DOWN, and `owner_down/1`
  # (which cancels) therefore cannot run against the next stream's handle. A selective `receive` in
  # `watchdog_loop/3` that took the DOWN ahead of `:stop` would break that.
  defp stop_watchdog(conn) do
    case Process.delete({__MODULE__, :watchdog, conn}) do
      nil ->
        :ok

      {pid, flags} ->
        if :atomics.get(flags, @fired_slot) == 0 do
          send(pid, :stop)
        else
          mon = Process.monitor(pid)
          send(pid, :stop)

          receive do
            {:DOWN, ^mon, :process, _, _} -> :ok
          after
            @watchdog_stop_ms ->
              Process.exit(pid, :kill)

              receive do
                {:DOWN, ^mon, :process, _, _} -> :ok
              end
          end
        end
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
  `exec/2` for a TENANT script (Hrana `sequence`): lock waits stay in the busy handler for the
  script's duration.

  A script runs several statements in one `sqlite3_exec`, and in autocommit each one commits on its
  own, so a BUSY halfway through cannot be retried from the BEAM the way `query/4` retries a single
  statement (expert review 2026-10-20 #2): the statements before it already committed. Yielding is
  switched off around the script and back on after it, whatever the outcome. Scripts are
  migration-rare, so the two extra dispatches do not matter. Without the extension both toggles fail
  harmlessly and the script runs exactly as `exec/2` would.
  """
  @spec exec_script(reference(), String.t()) :: :ok | {:error, term()}
  def exec_script(conn, sql) do
    _ = Sqlite3.execute(conn, "SELECT fathom_busy_yield(0)")

    try do
      Sqlite3.execute(conn, sql)
    after
      _ = Sqlite3.execute(conn, "SELECT fathom_busy_yield(1)")
    end
  end

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
  #
  # TWO places hold the limit since 2026-10-04: exqlite's own field (read by exqlite's busy handler,
  # which is what runs when the extension is not loaded) and the extension's (its busy handler
  # replaced exqlite's to enforce the statement deadline during lock waits — see
  # native/fathom_udf/src/backstop.rs). Setting only exqlite's would silently stop working wherever
  # the extension is loaded. The extension's limit can only go DOWN, so a raise reaches exqlite only.
  @spec set_busy_timeout(reference(), non_neg_integer()) :: :ok | {:error, term()}
  def set_busy_timeout(conn, ms) do
    with :ok <- Sqlite3.set_busy_timeout(conn, ms) do
      # "no such function" means the extension is not loaded here; exqlite's handler is the live one.
      _ = Sqlite3.execute(conn, "SELECT fathom_busy_timeout(#{ms})")
      :ok
    end
  end

  @doc """
  Applies `:shard_soft_heap_limit_bytes` as SQLite's node-wide `soft_heap_limit` (expert review
  2026-10-08 #8). Called once at boot by `Fathom.Application`; idempotent, so calling it again
  re-asserts the configured value. Unset (nil) or `0` leaves SQLite's own setting untouched.

  Returns `:ok`, `:skipped` (not configured), or raises if SQLite did not take the value — a
  memory bound an operator configured and the node silently lacks is worse than a boot failure.
  """
  #
  # WHY A SOFT HEAP LIMIT, AND WHY HERE. `:shard_cache_size_kb` bounds page cache PER CONNECTION, and
  # fathom holds one connection per Hrana stream, so total page cache is streams × that ceiling
  # (2 MiB × 30k held streams ≈ 60 GB of tail) with nothing node-wide over it. `soft_heap_limit` is
  # the node-wide bound: past it SQLite's pcache recycles unpinned pages instead of allocating new
  # ones (`sqlite3HeapNearlyFull` → `pcache1UnderMemoryPressure`), so caches SHED rather than any
  # statement failing. That is why it is the soft limit and never `hard_heap_limit`, which makes
  # allocations fail with SQLITE_NOMEM.
  #
  # It is PROCESS-GLOBAL in SQLite, not per connection — which is both why it works (one setting
  # covers every exqlite connection in this BEAM) and why it is applied from a trusted, throwaway
  # `:memory:` connection rather than a tenant one. Tenants cannot set it: `soft_heap_limit` /
  # `hard_heap_limit` assignments are refused by `ShardExecutor`'s pragma gate.
  #
  # Verified before building (2026-10-09 probe, exqlite 0.42): set on connection A, read back the
  # same value on an ALREADY-OPEN connection B, on a freshly opened C, and on B after A closed.
  # Effect: 4 connections at `cache_size=-200000` each scanning a 120 MB table grew the BEAM's
  # system memory by ~505 MiB with no limit and ~61 MiB at a 64 MiB limit, with every scan
  # returning the full, correct result.
  @spec apply_soft_heap_limit() :: :ok | :skipped
  def apply_soft_heap_limit do
    case Application.get_env(:fathom, :shard_soft_heap_limit_bytes) do
      bytes when is_integer(bytes) and bytes > 0 -> set_soft_heap_limit!(bytes)
      _ -> :skipped
    end
  end

  defp set_soft_heap_limit!(bytes) do
    {:ok, conn} = Sqlite3.open(":memory:")

    try do
      {:ok, stmt} = Sqlite3.prepare(conn, "PRAGMA soft_heap_limit=#{bytes}")

      result =
        try do
          Sqlite3.fetch_all(conn, stmt)
        after
          Sqlite3.release(conn, stmt)
        end

      case result do
        {:ok, [[^bytes]]} ->
          :ok

        other ->
          raise "PRAGMA soft_heap_limit=#{bytes} did not take (SQLite answered #{inspect(other)})"
      end
    after
      Sqlite3.close(conn)
    end
  end

  @doc """
  Introspects a statement WITHOUT running it (the Hrana `describe` request, #34): returns its result
  column names and its bound-parameter count as `%{params: [nil, …], cols: [name, …]}`. Parameters
  are reported positionally (`nil` each — exqlite exposes the count, not the names), which is what a
  libSQL client needs to know how many values to bind. The prepared statement is always released.
  """
  @spec describe(reference(), String.t()) ::
          {:ok, %{params: [nil], cols: [String.t()]}} | {:error, term()}
  def describe(conn, sql) do
    with {:ok, stmt} <- prepare_retrying(conn, sql, nil) do
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
    release_owner_state(conn)
    Sqlite3.close(conn)
    track_close(conn)
  end

  @doc """
  Release THIS process's per-connection state — the cached prepared statements and the query
  watchdog — WITHOUT closing the connection. Both live in the owning process's dictionary keyed by
  `conn`, so they can only be reached from that process.

  Connection pooling calls this in the STREAM process on checkin, before handing the handle to the
  coordinator. It is load-bearing, not just tidiness: `Sqlite3.close/1` is `sqlite3_close_v2`, which
  DEFERS the close (and so skips the last-connection WAL checkpoint) while any prepared statement is
  unfinalized. A stream's statements live in the stream's dict, unreachable from the coordinator — so
  without finalizing them here, the coordinator's later close would leave the WAL un-checkpointed,
  which desynchronises the durability position stamp (A2 promote-on-open ranks on it). `close/1`
  calls this then closes; the next stream to reuse the pooled handle starts from a clean owner state.
  """
  @spec release_owner_state(reference()) :: :ok
  def release_owner_state(conn) do
    purge_stmt_cache(conn)
    stop_watchdog(conn)
    :ok
  end

  @doc """
  Return a POOLED handle to its just-opened connection-local state before another stream of the SAME
  shard reuses it (connection pooling — the pool is per-`{shard_id, scope}`, so this is cross-REQUEST
  hygiene for one tenant, NOT a cross-tenant boundary; a handle never crosses shards).

  What must be scrubbed is the SQLITE HANDLE, not this process: the statement cache and watchdog live
  in the stream process's dictionary (see `close/1`), so a new stream already starts clean — but the
  connection itself carries an open transaction and any pragmas the previous stream toggled. So:

    * **Roll back any open transaction** — otherwise the next stream's statements silently join the
      previous one's transaction. This is the load-bearing guard.
    * **`:rw` only — re-apply the connection-local pragmas `configure/1` sets**, so a
      `PRAGMA foreign_keys=OFF` (or another allow-listed toggle) from the previous stream does not
      carry over. A `:ro` handle cannot write, so rollback of its read transaction is enough and the
      writable-path reconfigure is skipped.

  Returns `:ok`, or `{:error, reason}` — on which the caller MUST discard (close) the handle and open
  a fresh one rather than serve on a half-reset connection.

  NOT force-reset: the `@tenant_pragma_allow` connection-local toggles `configure/1` does not itself
  set (`defer_foreign_keys`, `recursive_triggers`, `case_sensitive_like`, …). They are client-session
  behavioural settings and same-tenant here, so a leak between one tenant's own requests is a benign
  quirk, not an isolation breach; widen this reset if a concrete need appears.
  """
  # The `:ro` reuse batch: schema reload, then re-assert read-only. See `reset_for_reuse/2`.
  @ro_reuse_sql "SELECT 1 FROM sqlite_schema LIMIT 0;PRAGMA query_only=ON"

  @spec reset_for_reuse(reference(), :ro | :rw) :: :ok | {:error, term()}
  def reset_for_reuse(conn, scope) when scope in [:ro, :rw] do
    with :ok <- rollback_if_open(conn) do
      # `:rw` re-applies the writable-path pragmas (`configure/1`). `:ro` RE-ASSERTS `query_only=ON`
      # (expert review 2026-09-18 #13): a `:ro` handle from the WAL-recovery / new-file fallback is
      # physically READ-WRITE, and its read-only-ness rests SOLELY on `PRAGMA query_only=ON` (see the
      # open path's `maybe_query_only/2` and the note there). Rollback alone left that unenforced on
      # reuse — safe only because `ShardExecutor` denies the `query_only` pragma to tenants, but a
      # trust boundary must not rest on a single upstream guard.
      #
      # Both are ONE `Sqlite3.execute` batch that starts with the schema reload (expert review
      # 2026-10-01 perf #24): every exqlite call is a dirty-IO NIF dispatch on a pool sized to the
      # core count, and the separate calls were ~7 per reuse. Measured on a warm handle (2026-10-01,
      # 3,000 samples): 19 us separate vs 7 us batched.
      if scope == :rw, do: reconfigure_for_reuse(conn), else: Sqlite3.execute(conn, @ro_reuse_sql)
    end
  end

  # RELOAD THE SCHEMA BEFORE A POOLED HANDLE SERVES AGAIN (expert review 2026-09-29 #11).
  #
  # A handle idle in the pool does not see a sibling stream's DDL until it next reads the schema
  # cookie, and `prepare` alone does not read it: on a stale schema it returns the OLD column list,
  # which the statement-cache miss path stores — so after a sibling's `ALTER TABLE t ADD COLUMN y`,
  # `SELECT *` reported `["id","x"]` over rows `[1, 1, nil]` for the rest of the stream (the
  # 2026-08-26 #7 mis-mapping, back through pooling). Measured on exqlite: neither a rollback,
  # `PRAGMA query_only=ON`, `PRAGMA schema_version` nor `BEGIN; ROLLBACK` refreshes it; STEPPING a
  # statement that reads `sqlite_schema` does. `:rw` was protected only incidentally, by
  # `PRAGMA max_page_count` in `configure/1`, and not at all when the size cap is disabled.
  #
  # That statement now LEADS both reuse batches (`@ro_reuse_sql`, `rw_reuse_sql/0`) rather than
  # running as its own call (expert review 2026-10-01 perf #24).

  @doc """
  True when this connection's `temp` schema holds no objects — a precondition for POOLING it
  (expert review 2026-09-29 #6).

  A TEMP table, view or trigger is connection state that neither a rollback nor `configure/1`
  touches, so a pooled handle carried it into the NEXT stream: a `CREATE TEMP TRIGGER … ON main.t`
  fired on a later request's writes, and a TEMP table named like a main table SHADOWED it for
  unqualified names (which is how Django writes SQL) — reads returned the temp rows, and writes
  landed in a table that is never flushed. No watchdog: it runs at checkin, in the stream process.

  THE PROBE STATEMENT IS KEPT (perf review 2026-10-14 #10). It used to be a raw prepare + step +
  release, three dirty-NIF dispatches on every pooled checkin. It now lives in the stream's
  statement cache, which travels with the pooled handle, so a reused handle's checkin is ONE step.
  A `:done` step holds no snapshot. A `:row` (the temp schema is NOT empty) answers `false` and the
  caller closes the handle, which finalizes the statement with the rest of the cache. Must run
  BEFORE `detach_statements/1` / `release_owner_state/1`, while the cache is still in this process.

  Any failure answers `false`: a handle whose temp schema cannot be read is not pooled.
  """
  @temp_probe_sql "SELECT 1 FROM temp.sqlite_schema LIMIT 1"

  @spec temp_schema_empty?(reference()) :: boolean()
  def temp_schema_empty?(conn) do
    case cached_stmt(conn, @temp_probe_sql) do
      {:ok, stmt, _cols, _enc, _refetch?} -> Sqlite3.step(conn, stmt) == :done
      {:error, _} -> false
    end
  rescue
    _ -> false
  end

  # Expert review 2026-10-20-2027 #18: the pooled-stream CLOSE used to pay two dirty dispatches
  # for two facts about the handle — the temp-schema probe (a `step`) and `rollback_if_open/1`'s
  # `transaction_status`. Both are readable in ONE step of a cached statement: the extension's
  # `fathom_conn_state()` carries the autocommit flag, and the EXISTS column is the temp probe.
  # (The reuse-side `transaction_status` in `reset_for_reuse/2` is NOT touched: expert review
  # 2026-10-17 #31 tried dropping it and one-shot got 12% slower, unexplained.)
  @checkin_probe_sql "SELECT fathom_conn_state(), EXISTS(SELECT 1 FROM temp.sqlite_schema)"

  @doc """
  `{temp_schema_empty?, autocommit?}` for a handle about to be pooled, in ONE dirty dispatch when
  the extension is loaded (expert review 2026-10-20-2027 #18). Same answers as
  `temp_schema_empty?/1` + `autocommit?/1`, and the same failure direction: a handle whose temp
  schema cannot be read is not poolable (`false`), an unreadable autocommit flag counts as `true`.
  Falls back to the two separate calls when the fused statement is unavailable. Like
  `temp_schema_empty?/1`, it must run BEFORE `detach_statements/1`.
  """
  @spec checkin_probe(reference()) :: {boolean(), boolean()}
  def checkin_probe(conn) do
    with {:ok, stmt, _cols, _enc, _refresh} <- cached_stmt(conn, @checkin_probe_sql),
         # One row, so the chunk size is immaterial (`@multi_step_chunk` is defined further down).
         {:done, [[<<_changes::unsigned-little-64, _rowid::signed-little-64, ac>>, exists]]} <-
           Sqlite3.multi_step(conn, stmt, 2) do
      {exists == 0, ac == 1}
    else
      _ -> {temp_schema_empty?(conn), autocommit?(conn)}
    end
  rescue
    _ -> {false, true}
  end

  @doc """
  Roll back any transaction left open on `conn`; `:ok` when it is already in autocommit.

  Called from `reset_for_reuse/2` AND directly at pool CHECKIN (`ShardExecutor.close/1`). The
  checkin call is what fixes expert review 2026-09-18 #3: `reset_for_reuse/2` rolls back only LAZILY
  on the next reuse, so a handle checked in mid-`BEGIN` would sit in the idle pool holding the SQLite
  write lock and pinning the WAL — blocking every other `:rw` writer with `SQLITE_BUSY` and growing
  the WAL unbounded — until some later stream happened to reuse it. Fails safe: `autocommit?/1`
  returns `true` on a read error, so a handle whose state cannot be read is treated as clean and left
  for `sqlite3_close_v2` to roll back.
  """
  @spec rollback_if_open(reference()) :: :ok | {:error, term()}
  def rollback_if_open(conn) do
    if autocommit?(conn) do
      :ok
    else
      case Sqlite3.execute(conn, "ROLLBACK") do
        :ok -> :ok
        other -> other
      end
    end
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
          else: {:ok, finish_batches(rows, batches), count}

      # Retryable (see `step_all/4`) only when nothing has been returned yet.
      :busy ->
        if batches == [], do: :busy_before_rows, else: {:error, :busy}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The common result is ONE batch (a point read): return its rows as they are instead of
  # reverse + concat copying them (expert review 2026-10-20 #35).
  defp finish_batches(rows, []), do: rows
  defp finish_batches(rows, batches), do: [rows | batches] |> Enum.reverse() |> Enum.concat()

  defp timeout_ms, do: Application.get_env(:fathom, :query_timeout_ms)

  # One step of the extension's `fathom_conn_state()` (native/fathom_udf `scalars::conn_state_blob`):
  # <<changes::u64-le, last_insert_rowid::i64-le, autocommit::u8>>. `:error` (extension not loaded,
  # or any unexpected shape) sends the caller to the three separate NIF calls, so behaviour is
  # identical wherever the extension is absent. Rides the ordinary statement cache.
  defp fetch_conn_state(conn) do
    with {:ok, stmt, _cols, _enc, _refresh} <- cached_stmt(conn, "SELECT fathom_conn_state()") do
      case Sqlite3.multi_step(conn, stmt, @multi_step_chunk) do
        {:done, [[<<changes::unsigned-little-64, rowid::signed-little-64, ac>>]]} ->
          {:ok, changes, rowid, ac == 1}

        _ ->
          _ = Sqlite3.reset(stmt)
          :error
      end
    else
      _ -> :error
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
