defmodule Fathom.ShardExecutor do
  @moduledoc """
  A `Filo.Executor` over Fathom's shards. Filo owns the Hrana protocol (the wire,
  batons, streams, both transports); this module gives each Hrana stream its own
  SQLite connection to the right shard's database file.

  The handle is `{coordinator_pid, ref, connection}` — one SQLite connection per
  stream, so per-stream transactions are isolated, and the `ref` tracks the
  checkout so the shard coordinator knows the connection is in use (it won't flush
  to storage / idle-stop until every connection is checked back in). `close/1`
  closes the connection and checks it in.
  """
  @behaviour Filo.Executor

  require Logger

  alias Fathom.HranaAuth
  alias Fathom.Migrator.Capture
  alias Fathom.Shard
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.WriteFence
  alias Fathom.Shards
  alias Filo.{Describe, Error, Stmt, StmtResult}

  # The behaviour's required 1-arity open; the real open is open/2. A direct open/1 (or an
  # executor caller with no auth context) gets the full-access default scope.
  @impl true
  def open(shard_id), do: open(shard_id, :rw)

  @impl true
  # No shard could be resolved for the request (no subdomain, override off, and no configured
  # default — the prod fail-closed posture, finding #26). Refuse rather than commingle the caller
  # into a shared default shard. A 400 status so Filo's HTTP pipeline surfaces it as a client error.
  def open(nil, _context),
    do: {:error, %Error{message: "no shard specified", code: "FILO_NO_SHARD", status: 400}}

  # `context` is the token's scope (:rw/:ro, #24), which Filo threads from HranaAuth.authorize/2
  # as the connection's authenticated context (Filo.Executor.open/2). This replaced a process-dict
  # side-channel that silently failed across the HTTP request→stream process hop (a `ro` token then
  # got full write access) and leaked on the 2nd WS stream of one connection — audit #3.
  def open(shard_id, context) do
    # Normalize (validate + downcase) at this trust boundary so the handle's id — which drives the
    # write counter, template-capture check, and load counters — matches the registry key / file /
    # S3 key that Shards.checkout uses (finding #19). Build the handle from the canonical id.
    case Fathom.ShardId.cast(shard_id) do
      {:ok, id} ->
        {scope, token_version} = context_of(context)
        do_open(id, scope, token_version)

      :error ->
        {:error, open_error(:invalid_shard_id)}
    end
  end

  # The authorize context is HranaAuth's scope atom. Anything else — nil (a bare `:ok` / no
  # authorize callback) or an unexpected term — is the safe default: full access is only granted
  # by the absence of a restriction, and `:ro` is the only restriction, only ever set explicitly.
  # The context is `{scope, token_version}` from HranaAuth.authorize/2 (expert review 2026-08-20
  # #22 added the version). Older shapes — a bare scope atom, `nil` from a plain `:ok`, an internal
  # caller with no auth context — degrade to full access with no version, i.e. no floor re-check.
  defp context_of({scope, version}), do: {scope_of(scope), version}
  defp context_of(other), do: {scope_of(other), nil}

  defp scope_of(:ro), do: :ro
  defp scope_of(_), do: :rw

  defp do_open(shard_id, scope, token_version) do
    case Shards.checkout(shard_id) do
      {:ok, pid, ref, path} ->
        # THE one connection in fathom that runs SQL fathom did not author, so it is the one
        # that gets the restricted handle: ATTACH/DETACH denied at the SQLite authorizer, and a
        # genuinely read-only handle for a `:ro` token (expert review 2026-08-01 #1, #7).
        # Everything else — the coordinator's VACUUM INTO snapshot, the migration replay, the
        # harnesses — opens unrestricted, which is required: the authorizer also blocks
        # VACUUM INTO. See Fathom.Shard.Connection.maybe_authorizer/2.
        case Connection.open(path, tenant?: true, scope: scope) do
          {:ok, conn} ->
            # The scope rides the handle so execute/2 can enforce read-only across baton-resumes
            # and every stream on the connection.
            {:ok, {pid, ref, conn, shard_id, scope, token_version, stream_opts(shard_id)}}

          {:error, reason} ->
            Shard.checkin(pid, ref)
            {:error, open_error(reason)}
        end

      {:error, reason} ->
        {:error, open_error(reason)}
    end
  end

  @impl true
  def execute({_pid, _ref, _conn, shard_id, _scope, _ver, _opts} = handle, %Stmt{} = stmt) do
    do_execute(handle, stmt)
  rescue
    # A statement must never crash the Hrana stream: exqlite/bind/result-mapping can raise
    # (connection.ex only rescues ArgumentError), so convert any raise at the executor
    # boundary to a protocol error the client can see and the stream survives (finding #25).
    e ->
      Logger.warning("shard #{shard_id} execute raised: #{Exception.message(e)}")
      {:error, %Error{message: Exception.message(e), code: "SQLITE_ERROR"}}
  catch
    :exit, reason ->
      Logger.warning("shard #{shard_id} execute exited: #{inspect(reason)}")
      {:error, %Error{message: "shard connection unavailable", code: "SQLITE_ERROR"}}
  end

  defp do_execute(
         {_pid, _ref, _conn, shard_id, scope, token_version, opts} = handle,
         %Stmt{sql: sql} = stmt
       ) do
    # Classify the statement ONCE — dml?/ddl? each slice+downcase the statement head, and the old
    # shape recomputed them up to 3× per statement (twice in this cond, again in to_stmt_result).
    dml? = dml?(sql)
    ddl? = ddl?(sql)
    write? = dml? or ddl?

    cond do
      # Statements no tenant may run, regardless of scope or config (expert review 2026-08-01
      # #1/#8). Checked first so it cannot be reached around. The SQLite authorizer on the
      # tenant handle is the real enforcement — this branch exists to return a *diagnosable*
      # error code instead of a bare "not authorized"/"authorization denied" from the engine,
      # and to cover the pragma names the authorizer cannot express per-name.
      blocked = blocked_statement(sql) ->
        {:error, blocked}

      # A read-only token (#24) may only read: refuse any write (DML or DDL) with a distinct 403 so
      # export/analytics/BI credentials can't mutate a tenant. Checked before DDL-block/run so a `ro`
      # token is refused even where a full token would be allowed.
      #
      # This is now DEFENCE IN DEPTH, not the enforcement: a `:ro` stream's handle is opened
      # `mode: :readonly`, so SQLite refuses the write even when this leading-keyword test
      # misses it (a `/* */` prefix, a CTE-prefixed DML, a durable PRAGMA — all verified
      # bypasses before #7).
      scope == :ro and write? ->
        {:error,
         %Error{message: "read-only token cannot write", code: "FILO_READONLY", status: 403}}

      denied = lifecycle_denied(shard_id) ->
        {:error, denied}

      # The write circuit-breaker (expert review 2026-07-19 #3): this node's heartbeat has been
      # not-valid long enough (> ttl + steal_margin) that a peer may already have stolen the shard,
      # so a write ACKed here would be quarantined on partition-heal. Refuse writes with a retryable
      # 503 while READS still serve from the local copy (the flag is set only for provably-stealable
      # shards, and only when :fence_writes_when_stealable is on — default prod). Lock-free ETS read,
      # only on writes.
      # THE TOKEN'S REVOCATION FLOOR, RE-CHECKED PER STATEMENT (expert review 2026-08-20 #22).
      #
      # `HranaAuth.authorize/2` runs exactly ONCE per Hrana WebSocket, at `hello`. Every later
      # `open_stream` reuses the stashed context, `Filo.Socket` has no idle timeout and no maximum
      # connection lifetime, and a django-libsql stream lives for hours between requests. So a
      # revoke — the compromise-response path, documented as IMMEDIATE — stopped only NEW
      # connections: an attacker holding a stolen token's socket kept reading and writing
      # indefinitely while every dashboard and ledger row said the credential was revoked. It also
      # defeated `:hrana_token_max_age`, which prod refuses to boot without: that bounds how long a
      # stolen token can START a session, never how long one runs.
      #
      # Reads AND writes, because a revoked credential must not keep reading a tenant's data
      # either — unlike the write fence below, which is about durability and deliberately lets
      # reads through. Cache-only, so this never reaches Postgres; an unknown floor allows, since
      # `authorize/2` already did the authoritative check when the connection opened.
      not HranaAuth.version_current?(shard_id, token_version) ->
        {:error,
         %Error{
           message: "the token for shard \"#{shard_id}\" has been revoked",
           code: "FILO_UNAUTHORIZED",
           status: 401
         }}

      # `write_candidate?/1`, NOT `write?` (expert review 2026-08-26 #1). See its definition for
      # why the fence needs a wider, fail-closed predicate than the one driving `read_only?`.
      write_candidate?(sql) and WriteFence.fenced?(shard_id) ->
        {:error,
         %Error{
           message:
             "shard \"#{shard_id}\" lease is stale on this node (storage unreachable); retry",
           code: "FILO_STALE_LEASE",
           status: 503
         }}

      # Block client-issued DDL on a tenant shard when strict mode is on (expert review 2026-07-14
      # #7): a direct `manage.py migrate` against a tenant advances its schema + `django_migrations`
      # while fathom's three-place version stamp stays at 0, so the shard then reads as a laggard, the
      # engine replays the captured chain onto the already-migrated file, hits "already exists", and
      # quarantines the tenant. Refuse loudly at the client instead — schema evolution goes through the
      # template + migration engine, never a direct tenant migrate. The template (the capture source)
      # is exempt, and the engine's own replay uses `Connection` directly (not this Hrana path), so
      # neither is affected. Off by default (`:block_tenant_ddl`); enable in prod to enforce the model.
      opts.block_ddl? and not opts.template? and ddl? ->
        {:error,
         %Error{
           message:
             "schema changes must go through the migration engine on the template shard, not a " <>
               "direct migrate on tenant \"#{shard_id}\"",
           code: "FILO_DDL_BLOCKED",
           status: 400
         }}

      true ->
        run_statement(handle, stmt, dml?, ddl?)
    end
  end

  defp run_statement(
         {pid, _ref, conn, shard_id, _scope, _ver, opts},
         %Stmt{sql: sql, args: args},
         dml?,
         ddl?
       ) do
    started = System.monotonic_time()

    case Connection.query(conn, sql, args, dml?: dml?) do
      {:ok, result} ->
        # A write bumps the shard's write counter so the periodic durability flush knows local
        # holds un-flushed changes; a read-only shard stays clean and skips the upload (the
        # durability-flush storm fix). Lock-free ETS from this stream process — no per-write cast
        # to the coordinator (finding #27). Also bump on the COMMIT/END boundary: a periodic flush
        # landing mid-transaction captures WriteCounter (which already counts the in-flight
        # INSERTs) but its VACUUM INTO snapshot — on a separate connection — excludes the
        # uncommitted rows and then advances the watermark past them. In the common case the COMMIT
        # inherits the last data statement's changes()>0 so wrote?/2 already re-bumps; but if the
        # transaction's last data statement modified 0 rows (a no-op UPDATE/DELETE) changes() is 0
        # and wrote?/2 is false, so an idle drop_clean could delete a committed row that never
        # reached storage. Bumping on the commit boundary re-dirties the shard so the next flush
        # re-snapshots the now-committed data. Over-counting is the safe direction (an extra flush,
        # never lost data).
        # Expert review 2026-07-24 #3: the commit-boundary bump used to be UNCONDITIONAL, so a
        # transaction in which nothing wrote still dirtied the shard. Django's ATOMIC_REQUESTS (and
        # any transaction.atomic() around a read view) wraps reads in BEGIN…COMMIT, so a purely
        # read-only tenant was dirty at every tick and paid a full VACUUM INTO + full-object PUT
        # every :shard_flush_interval_ms — defeating the write-gated flush ("PUTs track writes, not
        # open-shard count"). Gate it on whether this connection actually wrote since BEGIN. The
        # race the unconditional bump existed for is untouched: it needs an in-flight write, which
        # sets the flag. A transaction with no write has nothing uncommitted to lose.
        wrote? = wrote?(result, sql)

        if wrote? or (commit_boundary?(sql) and txn_wrote?(conn)) do
          Fathom.Shard.WriteCounter.bump(shard_id)
          # Tell the coordinator once per checkout that this stream has written (expert review
          # 2026-08-01 #42). A served-but-clean shard polls at a reduced flush cadence; this is
          # the clean→dirty edge that restores the full rate. Guarded by a process-dictionary
          # flag — the same single-owner-connection idiom as the statement cache — so a
          # write-heavy stream sends exactly one message, not one per statement.
          signal_dirty_once(pid, conn)
        end

        # A2 quorum replication (off unless :replication_enabled). Computed BEFORE
        # track_txn_write/3, which clears the in-transaction flag this decision reads.
        repl = maybe_replicate(shard_id, pid, conn, sql, wrote?)

        track_txn_write(conn, sql, wrote?)

        # On the reserved template shard, feed each successful statement to the
        # migration capture so a Django migrate becomes a fleet version.
        # prepare_v2 recompiles a cached statement on a schema change, so the STATEMENT stays
        # valid — but the column list now cached beside it does not (review 2026-07-24 #17). Drop
        # the cache on DDL so a later `SELECT *` cannot report pre-ALTER columns for post-ALTER
        # rows. Off the hot path: only DDL pays it, and fathom blocks client DDL on tenant shards
        # entirely when :block_tenant_ddl is on.
        if ddl? do
          # Purge THIS connection's cache, and tell every OTHER connection on this node that its
          # cached column lists may be stale (expert review 2026-08-26 #7). The local purge alone
          # was the 2026-07-24 #17 fix; it cannot see a sibling stream on the same shard, whose
          # cached `SELECT *` then reports the pre-DDL column names against post-DDL rows.
          Connection.purge_statements(conn)
          Fathom.Shard.SchemaGen.bump()
        end

        capture(opts.template?, conn, sql, args)
        stmt_result = to_stmt_result(result, dml?)
        latency = System.monotonic_time() - started

        # Per-shard load + tail latency: the "which shard is hot / slow" signals for the
        # rebalancer. Lock-free ETS bumps, riding one :shard_load gate check here (the two
        # modules each re-reading the same key was a doubled env lookup on every query —
        # review 2026-07-23 #7). ShardLatency's per-shard ETS histogram is deliberately NOT a
        # metric tag (a per-shard Prometheus label is cardinality death); the read API is the
        # interface.
        if Fathom.ShardLoad.enabled?() do
          Fathom.ShardLoad.record_query(
            shard_id,
            stmt_result.rows_read,
            stmt_result.rows_written
          )

          Fathom.ShardLatency.record(shard_id, latency)
        end

        # Node-wide query latency for the metrics layer (Prometheus distribution / the dashboard's
        # latency chart). Deliberately UN-tagged, for the same cardinality reason. On success only,
        # matching the cold_open convention. A no-op when no reporter is attached.
        :telemetry.execute([:fathom, :shard, :query], %{duration: latency}, %{})

        # The client's success is gated on the follower quorum. The local commit ALREADY happened
        # — SQLite committed before the WAL could be read and there is no un-commit — so this
        # reports an error for a write that is durable locally and will still reach S3. That is
        # deliberate: the client asked for a quorum-durable write and did not get one. See
        # Fathom.Shard.Replication.Session on the at-least-once hazard that follows.
        case repl do
          :ok -> {:ok, stmt_result}
          {:error, reason} -> {:error, no_quorum_error(reason)}
        end

      # Per-query resource bounds (expert review 2026-07-14 #26) — distinct, non-SQLITE codes so a
      # client can tell "my query was too expensive" from an ordinary SQL error.
      {:error, :query_timeout} ->
        {:error,
         %Error{
           message: "query exceeded the statement timeout",
           code: "FILO_QUERY_TIMEOUT",
           status: 503
         }}

      {:error, {:too_many_rows, cap}} ->
        {:error,
         %Error{
           message: "result exceeded the #{cap}-row cap",
           code: "FILO_RESULT_TOO_LARGE",
           status: 400
         }}

      {:error, reason} ->
        {:error, %Error{message: reason_to_string(reason), code: sqlite_code(reason)}}
    end
  end

  # Did this statement change the shard file? Check `num_changes` FIRST: an
  # `INSERT/UPDATE/DELETE ... RETURNING` returns columns *and* mutates rows, so the old
  # columns-first test classified every RETURNING write as a read — the shard stayed
  # clean, dropped its only local copy on idle, and silently lost the rows (Django emits
  # `... RETURNING "id"` on every insert). A row-returning read (SELECT) reports 0 changes
  # so it still classifies clean; a no-column, no-change statement is DDL (`CREATE TABLE` =
  # 0 changes) or transaction/PRAGMA control — treat control as read and everything else
  # (DDL) as a write so DDL is never dropped from durability.
  #
  # sqlite3_changes() is not reset by a SELECT, a BEGIN, a COMMIT or a ROLLBACK, so a statement
  # following a write on the same connection inherits that write's change count. For a *data*
  # statement that over-marks dirty, which is the safe direction and costs nothing (the shard is
  # already dirty from the write).
  #
  # For a CONTROL statement it was actively harmful (expert review 2026-07-24 #3), which is why
  # `control_statement?` is now tested BEFORE `changes`: a COMMIT on a long-lived connection that
  # once wrote inherited changes()>0 and classified as a write forever after, so every read-only
  # transaction re-dirtied the shard and paid a full VACUUM INTO + full-object PUT. That is the
  # primary production topology — Django's recommended CONN_MAX_AGE keeps connections alive across
  # requests — so the finding's win exists only with this ordering. No control statement can itself
  # set sqlite3_changes, so the reorder removes inherited noise and nothing else. The commit
  # boundary is instead governed by the per-transaction write flag below, which tracks actual
  # writes precisely (including the `exec` script path).
  #
  # The RETURNING rationale is untouched: `INSERT ... RETURNING` returns columns *and* mutates
  # rows, and is not control-prefixed, so it still reaches `changes > 0` and classifies as a write.
  # Durable PRAGMA assignments (`user_version`) are likewise not control — `control_statement?`
  # returns false for them — so they still classify as writes via the DDL fallthrough.
  defp wrote?(%{columns: cols, num_changes: changes}, sql) do
    cond do
      control_statement?(sql) -> false
      cols != [] and read_verb?(sql) -> false
      changes > 0 -> true
      cols != [] -> false
      true -> true
    end
  end

  # Statements whose leading verb is PROVABLY incapable of mutating the database file, so their
  # result may ignore the inherited `sqlite3_changes()` (review 2026-07-24 #3). Without this, the
  # first plain SELECT after any write on a long-lived connection re-classified as a write —
  # re-dirtying the shard on every read of a Django CONN_MAX_AGE connection, which is the topology
  # the finding is about.
  #
  # This is a WHITELIST, and it must stay one. The tempting inverse — "returns columns and is not
  # `dml?` ⇒ read" — LOSES DATA: `dml?/1` is leading-keyword only and its own comment documents
  # that a CTE-prefixed `WITH x AS (...) INSERT INTO t ... RETURNING id` is not detected, so the
  # only thing classifying that statement as a write today is the `changes > 0` branch below.
  # Blacklisting would make it a read, leave the shard clean, and let drop_clean delete the
  # committed rows. **Never add `with` here.**
  #
  # Why the set is closed: no top-level SELECT can mutate the file (triggers fire only on
  # INSERT/UPDATE/DELETE, SQLite has no writing table-valued functions, and `Fathom.Shard.
  # Connection` registers no UDFs and loads no extensions — a future writing UDF would break this
  # invariant); VALUES is a pure row constructor; EXPLAIN [QUERY PLAN] compiles and reports, it
  # does not execute the inner statement's effects.
  #
  # Anything unrecognized — a leading comment, an unusual verb — simply misses the whitelist and
  # falls through to the conservative path. The failure direction is always an extra flush, never
  # a lost write.
  @read_verbs ~w(select values explain)

  # The six classifiers below each used to carry a `defp foo?(_sql), do: false` catch-all. Dialyzer
  # proves every one unreachable: they are only reached from `wrote?/2` and the transaction tracker,
  # both of which run AFTER a statement has executed, so SQLite already compiled the string and it
  # is necessarily a binary. None of the catch-alls was commented, and the documented safety
  # property they look like they serve — "anything unrecognized falls through to the conservative
  # path, so the failure direction is an extra flush, never a lost write" — is carried by the
  # `is_binary` clause itself, which returns false for an unknown verb. The catch-alls only ever
  # answered the non-binary case, which cannot occur. A future caller that broke that now fails at
  # the dialyzer gate rather than silently classifying a write as a read.
  defp read_verb?(sql) when is_binary(sql) do
    head = sql |> String.trim_leading() |> String.slice(0, 7) |> String.downcase()
    Enum.any?(@read_verbs, &String.starts_with?(head, &1))
  end

  @control_prefixes ~w(begin commit end rollback savepoint release pragma)

  # Header-writing pragma ASSIGNMENTS are durable file mutations that report
  # num_changes == 0 and no columns, so the blanket "pragma is control" rule dropped
  # them on idle (expert review #15): the shard stayed clean, drop_clean deleted the
  # local copy without a flush, and the acknowledged stamp vanished. Pointed because
  # `user_version` is fathom's own O(1) schema-version gate — external migration
  # tooling stamping it through the data path is exactly the write that was lost.
  # Whitelist (not "any pragma with ="): connection-local assignments like
  # busy_timeout must stay clean, or per-connection setup pragmas would re-dirty
  # read-only shards into needless durability uploads.
  # `page_size` and `journal_mode` added 2026-08-03 (expert review #21): both rewrite the database
  # HEADER, so a client setting either changes bytes that must reach storage. Classifying them as
  # control ⇒ read meant such a statement did not even mark the shard dirty. `page_size` matters
  # beyond dirtiness — it is the unit the size cap is denominated in (see
  # `Connection.maybe_max_page_count/1`), so a shard whose page size changes underneath a
  # page-denominated cap silently changes its own byte ceiling.
  @durable_pragmas ~w(user_version application_id schema_version page_size journal_mode)

  # Slice + downcase a few head chars rather than the whole statement — this runs on every
  # no-column/no-change result, and the full-statement downcase was an O(len) binary copy per
  # query (a multi-KB ORM statement paid it on every execution). 9 covers the longest prefix
  # ("savepoint"); only a pragma needs the full text (durable_pragma_write? scans the body for
  # `=` and the header-writing names), and pragma statements are short, so the full downcase
  # is confined to that branch.
  defp control_statement?(sql) when is_binary(sql) do
    trimmed = String.trim_leading(sql)
    head = trimmed |> String.slice(0, 9) |> String.downcase()

    cond do
      String.starts_with?(head, "pragma") -> not durable_pragma_write?(String.downcase(trimmed))
      Enum.any?(@control_prefixes, &String.starts_with?(head, &1)) -> true
      true -> false
    end
  end

  # A transaction-committing statement (COMMIT / END — SQLite treats END as a COMMIT synonym).
  # This is the durability boundary where in-flight INSERTs become committed; see the bump site
  # in do_execute/2 for why the commit must re-dirty the shard. ROLLBACK is deliberately excluded
  # (it discards the writes, so there is nothing new to flush). Matches the leading token of the
  # whole statement, so a trigger body's inner `... BEGIN ... END` (the statement starts with
  # CREATE) is never mistaken for a transaction commit. Slice-first for the same per-query
  # reason as control_statement?/1 — this runs on every clean (SELECT) result.
  defp ends_transaction?(sql) when is_binary(sql) do
    head = sql |> String.trim_leading() |> String.slice(0, 6) |> String.downcase()
    String.starts_with?(head, "commit") or String.starts_with?(head, "end")
  end

  # --- per-transaction write tracking (expert review 2026-07-24 #3) -------------------------
  #
  # Did this connection write since its transaction opened? Lives in the OWNING process's
  # dictionary keyed by the conn ref — the same idiom (and the same race-freedom argument) as the
  # prepared-statement cache and the deadline watchdog: a connection is single-owner by design,
  # one per Hrana stream, used only by that stream process.
  #
  # Deliberately NOT derived from the shared WriteCounter ETS value across the BEGIN/COMMIT pair:
  # a concurrent stream writing to the same shard would bump that counter and spuriously re-dirty
  # us on commit — safe, but common on a hot shard, which is exactly the case this finding is
  # trying to make cheap.
  # One `:became_dirty` cast per checkout (expert review 2026-08-01 #42). Keyed by the
  # connection ref in the owning process's dictionary — the same single-owner idiom as the
  # statement cache and the txn-write flag — so it dies with the stream and a write-heavy
  # stream costs exactly one message rather than one per statement.
  @dirty_signalled_key :dirty_signalled

  defp signal_dirty_once(pid, conn) do
    key = {__MODULE__, @dirty_signalled_key, conn}

    unless Process.get(key, false) do
      Process.put(key, true)
      Fathom.Shard.signal_dirty(pid)
    end

    :ok
  end

  @txn_wrote_key :txn_wrote

  defp txn_wrote?(conn), do: Process.get({__MODULE__, @txn_wrote_key, conn}, false)

  # BEGIN opens a fresh transaction ⇒ nothing written yet. COMMIT/END/ROLLBACK close it ⇒ reset,
  # so a subsequent read-only transaction can't inherit this one's dirtiness. Any write sets it.
  # Order matters: a write is recorded even on a statement that also ends a transaction (there is
  # no such statement in SQLite today, but the ordering makes the flag's meaning unambiguous).
  defp track_txn_write(conn, sql, wrote?) do
    key = {__MODULE__, @txn_wrote_key, conn}

    cond do
      # BEGIN first, deliberately: a BEGIN cannot itself write, so it always opens a clean
      # transaction regardless of how the classifier scored it.
      begins_transaction?(sql) -> Process.put(key, false)
      wrote? -> Process.put(key, true)
      # Only a statement that actually ENDS the transaction may clear the flag. Notably absent:
      # RELEASE (an inner savepoint release commits nothing — the outer COMMIT must still re-bump
      # if a flush straddles it) and `ROLLBACK TO <savepoint>` (see rolls_back?/1).
      ends_transaction?(sql) or rolls_back?(sql) -> Process.delete(key)
      true -> :ok
    end

    :ok
  end

  # Mark the connection as having written, without a statement to classify — the `exec` script
  # path, which presumes a write and bumps unconditionally. Without this, a BEGIN (query path) →
  # write (exec path) → COMMIT (query path) sequence would find the flag false at COMMIT and skip
  # the boundary re-bump, reopening the mid-transaction watermark race for scripts.
  defp mark_txn_write(conn), do: Process.put({__MODULE__, @txn_wrote_key, conn}, true)

  defp forget_txn_write(conn), do: Process.delete({__MODULE__, @txn_wrote_key, conn})

  # ---------------------------------------------------------------------------------------------
  # A2 quorum replication (expert-reviewed design in docs/a2-quorum-replication.md)
  # ---------------------------------------------------------------------------------------------

  # Ship this shard's newly committed WAL frames and wait for a follower quorum. Entirely inert
  # unless `:replication_enabled` — the gate is checked before anything else, so a node with the
  # feature off pays one `Application.get_env` per write and nothing more.
  defp maybe_replicate(shard_id, pid, conn, sql, wrote?) do
    if Session.enabled?() and ships_now?(conn, sql, wrote?) do
      Session.commit(shard_id, Shard.db_path(shard_id) <> "-wal", pid)
    else
      :ok
    end
  end

  # Only where the frames are actually COMMITTED: an autocommit write, or a commit boundary closing
  # a transaction that wrote. Shipping on every write statement inside a transaction would pay a
  # network round trip per statement, for frames SQLite has not committed and a follower would
  # ignore until the commit frame arrives anyway.
  #
  # `Process.get/1` WITHOUT a default is what separates the two states `txn_wrote?/1` collapses
  # into `false`: `begins_transaction?` puts an explicit `false`, so a MISSING key means no explicit
  # transaction is open. Hence `wrote? and is_nil(...)` is precisely "an autocommit write".
  defp ships_now?(conn, sql, wrote?) do
    in_txn? = Process.get({__MODULE__, @txn_wrote_key, conn}) != nil

    (wrote? and not in_txn?) or (commit_boundary?(sql) and txn_wrote?(conn))
  end

  # Distinct from a SQLite error, and a 503 rather than a 400: nothing is wrong with the statement,
  # the node could not reach enough followers. A client should retry — accepting the at-least-once
  # hazard the Session moduledoc describes — not rewrite its SQL.
  defp no_quorum_error(reason) do
    %Error{
      message: "write not replicated to a follower quorum (#{inspect(reason)})",
      code: "FILO_NO_QUORUM",
      status: 503
    }
  end

  defp begins_transaction?(sql) when is_binary(sql) do
    head = sql |> String.trim_leading() |> String.slice(0, 5) |> String.downcase()
    String.starts_with?(head, "begin")
  end

  # Statements at which in-flight writes become durable-able and the shard must be re-dirtied if
  # the transaction wrote: COMMIT/END, plus RELEASE. RELEASE matters because `ends_transaction?`
  # matches only commit/end, while a client using savepoints alone (`SAVEPOINT a; …; RELEASE a`)
  # never issues a COMMIT — so without this a flush straddling the savepoint would advance the
  # watermark past the uncommitted rows and the idle drop would lose them. Django opens nested
  # `atomic()` blocks as savepoints, so this is a live shape. An inner RELEASE that commits nothing
  # costs at most one spurious bump (over-dirty, the safe direction).
  defp commit_boundary?(sql), do: ends_transaction?(sql) or releases_savepoint?(sql)

  defp releases_savepoint?(sql) when is_binary(sql) do
    head = sql |> String.trim_leading() |> String.slice(0, 7) |> String.downcase()
    String.starts_with?(head, "release")
  end

  # A plain `ROLLBACK` [TRANSACTION] discards the whole transaction — nothing is left to flush, so
  # the flag resets. `ROLLBACK [TRANSACTION] TO [SAVEPOINT] name` does NOT end the transaction: it
  # rewinds to a savepoint, leaving every pre-savepoint write live and uncommitted. Clearing the
  # flag there would drop that write's re-bump at the eventual COMMIT and lose it on idle drop —
  # exactly Django's nested-atomic-raises-then-outer-commit shape. When in doubt, KEEP the flag:
  # keeping over-bumps (safe), clearing loses data.
  defp rolls_back?(sql) when is_binary(sql) do
    lead = sql |> String.trim_leading() |> String.slice(0, 40) |> String.downcase()

    String.starts_with?(lead, "rollback") and not String.contains?(lead, " to ")
  end

  # The assignment form (`pragma [db.]name = value`) of a header-writing pragma; the
  # bare read form (`pragma user_version`) has no `=` and stays a read.
  defp durable_pragma_write?(lead) do
    String.starts_with?(lead, "pragma") and String.contains?(lead, "=") and
      Enum.any?(@durable_pragmas, &String.contains?(lead, &1))
  end

  # Report the connection's REAL autocommit state (expert review 2026-07-14 #35). Hrana 3's
  # `get_autocommit` / `is_autocommit` batch conditions are consumed by libSQL SDK batch builders
  # (`{:not, :is_autocommit}`-guarded COMMIT/ROLLBACK steps), so a hardcoded `true` skipped those
  # steps mid-transaction — leaving an open transaction dangling on the connection, holding the WAL
  # write lock until stream teardown. exqlite exposes `transaction_status`, so answer truthfully.
  @impl true
  def autocommit?({_pid, _ref, conn, _shard_id, _scope, _ver, _opts}),
    do: Connection.autocommit?(conn)

  # The coordinator is the connection's owner (Filo's owner seam): the stream holding this
  # handle monitors it and tears down — closing the exqlite connection — if it dies. Without
  # this, a stream outliving a crashed coordinator kept writing as an orphan: the NEXT
  # checkout re-creates a fresh coordinator (restart: :temporary) that doesn't know the old
  # connection exists, so its idle flush-and-drop checkpoints past the orphan's un-folded WAL
  # frames and unlinks the db/wal/shm under it — the orphan's writes land in unlinked inodes
  # and vanish (finding #8, the residual orphan-writer race).
  @impl true
  def owner({pid, _ref, _conn, _shard_id, _scope, _ver, _opts}), do: pid

  # Runs a SQL script (the Hrana `sequence` request — libSQL's `executescript()`, #34): one or more
  # statements for side effects, no rows. A read-only token can't run one (a script writes), and the
  # tenant-DDL block applies to its leading statement.
  @impl true
  def execute_sequence({_pid, _ref, conn, shard_id, scope, token_version, opts}, sql)
      when is_binary(sql) do
    cond do
      # Same per-statement revocation re-check as execute/2 — a script is not a loophole.
      not HranaAuth.version_current?(shard_id, token_version) ->
        {:error,
         %Error{
           message: "the token for shard \"#{shard_id}\" has been revoked",
           code: "FILO_UNAUTHORIZED",
           status: 401
         }}

      scope == :ro ->
        {:error,
         %Error{
           message: "read-only token cannot run a script",
           code: "FILO_READONLY",
           status: 403
         }}

      denied = lifecycle_denied(shard_id) ->
        {:error, denied}

      # A SCRIPT IS A WRITE (expert review 2026-08-26 #1). `execute_sequence/2` never consulted the
      # write fence at all, even though this clause already presumes a script writes — it bumps
      # `WriteCounter` unconditionally below. So `executescript()` was an unconditional way past
      # the one circuit breaker that stops a stale-lease node ACKing writes a peer will quarantine.
      # No `write_candidate?/1` check here: a multi-statement script's leading keyword says nothing
      # about the rest of it, and the conservative answer for a script is always "it writes".
      WriteFence.fenced?(shard_id) ->
        {:error,
         %Error{
           message:
             "shard \"#{shard_id}\" lease is stale on this node (storage unreachable); retry",
           code: "FILO_STALE_LEASE",
           status: 503
         }}

      true ->
        # EVERY STATEMENT IN THE SCRIPT GOES THROUGH THE SAME GATE AS `execute/2` (expert review
        # 2026-08-20 #18, verified by execution against this project's own exqlite 0.37.0).
        #
        # This clause used to call Connection.exec/2 directly, checking only `ddl?(sql)` — and
        # `ddl?/1` is a LEADING-keyword test, so on a multi-statement script it inspected the first
        # statement and nothing else. `blocked_statement/1` was not called at all. Measured:
        #
        #     "SELECT 1; PRAGMA synchronous=OFF; PRAGMA max_page_count=777; CREATE TABLE evil(a);"
        #     #=> :ok — synchronous 0, max_page_count 777, table `evil` created
        #
        # ATTACH/DETACH/VACUUM survived because the SQLite AUTHORIZER enforces them at the engine,
        # which is the correct second layer — but `blocked_statement/1`'s own moduledoc says it
        # exists in part "to cover the pragma names the authorizer cannot express per-name", and
        # exqlite's authorizer is an action deny-list with no per-name granularity. So the half of
        # the gate with no engine backstop was exactly the half `sequence` skipped.
        #
        # Checked BEFORE anything runs, and one bad statement refuses the WHOLE script: a script
        # is the tenant's unit of intent, and half-applying it is worse than refusing it.
        case refuse_script(sql, opts, shard_id) do
          %Error{} = err ->
            {:error, err}

          nil ->
            run_script(conn, sql, shard_id, opts)
        end
    end
  end

  defp run_script(conn, sql, shard_id, opts) do
    case Connection.exec(conn, sql) do
      :ok ->
        # THE DURABILITY TRAP (#34): `exec` bypasses the `wrote?`-based WriteCounter bump in the
        # single-statement path, so a script-only session would leave the shard CLEAN and its
        # write-gated idle flush would drop the local copy WITHOUT uploading — silently losing the
        # script's writes (same class as the RETURNING bug). A script is presumed to write, so
        # bump unconditionally.
        Fathom.Shard.WriteCounter.bump(shard_id)

        # …and record it against the open transaction, so a BEGIN (query) → write (exec) →
        # COMMIT (query) sequence still re-bumps at the commit boundary (review 2026-07-24 #3).
        mark_txn_write(conn)
        # A script is opaque and may contain DDL, so drop the statement cache unconditionally
        # (review 2026-07-24 #17). Scripts are migration-rare; the cost is irrelevant here.
        Connection.purge_statements(conn)
        # On the template shard, a script is (part of) a migration — feed it to capture.
        # A Hrana `sequence` is opaque SQL text with no bind parameters, so there are no args
        # to record for it (unlike the `execute` path, where Django's parameterized statements
        # carry their values separately and must be stored to replay).
        capture(opts.template?, conn, sql, [])
        :ok

      {:error, reason} ->
        {:error, %Error{message: reason_to_string(reason), code: sqlite_code(reason)}}
    end
  end

  # The per-statement gate for a script. Returns the first `%Error{}` any statement trips, or nil.
  defp refuse_script(sql, opts, shard_id) do
    sql
    |> split_statements()
    |> Enum.find_value(fn stmt ->
      cond do
        opts.block_ddl? and not opts.template? and ddl?(stmt) ->
          %Error{
            message:
              "schema changes must go through the migration engine on the template shard, not a " <>
                "direct script on tenant \"#{shard_id}\"",
            code: "FILO_DDL_BLOCKED",
            status: 400
          }

        true ->
          blocked_statement(stmt)
      end
    end)
  end

  # A statement splitter for SQLite, hand-written because there is nothing to delegate to.
  # exqlite's `prepare/2` discards the tail SQLite's own `sqlite3_prepare_v2` returns, and the
  # authorizer is an action deny-list with no per-name granularity — so neither the binding nor
  # the engine can answer "where does this statement end". AGENTS.md's bar for hand-rolling a
  # grammar is exactly that sentence, and it is met here; the code is small and lives in one place.
  #
  # THE SAFETY DIRECTION IS DELIBERATE. Over-splitting is harmless: an extra fragment is checked
  # against the same allowlist and a legitimate one passes. UNDER-splitting is the bypass — it
  # happens only if the scanner believes it is inside a quoted region when it is not, so every
  # quoting form SQLite has is tracked, including the doubling escapes (`'it''s'`), the bracket and
  # backtick identifier forms, and both comment styles. `split_statements_test` pins each one.
  @doc false
  def split_statements(sql) when is_binary(sql), do: do_split(sql, "", [])

  defp do_split(<<>>, acc, out), do: Enum.reverse(emit(acc, out))

  defp do_split(<<?;, rest::binary>>, acc, out), do: do_split(rest, "", emit(acc, out))

  # Quoted regions: consume to the matching close, keeping the bytes so the fragment stays runnable.
  defp do_split(<<?\', rest::binary>>, acc, out) do
    {chunk, rest} = quoted(rest, ?\', "")
    do_split(rest, acc <> "\'" <> chunk, out)
  end

  defp do_split(<<?\", rest::binary>>, acc, out) do
    {chunk, rest} = quoted(rest, ?\", "")
    do_split(rest, acc <> "\"" <> chunk, out)
  end

  defp do_split(<<?`, rest::binary>>, acc, out) do
    {chunk, rest} = quoted(rest, ?`, "")
    do_split(rest, acc <> "`" <> chunk, out)
  end

  # `[...]` has no doubling escape in SQLite: the first `]` closes it.
  defp do_split(<<?[, rest::binary>>, acc, out) do
    {chunk, rest} = until(rest, "]", "")
    do_split(rest, acc <> "[" <> chunk, out)
  end

  defp do_split(<<"--", rest::binary>>, acc, out) do
    {chunk, rest} = until(rest, "\n", "")
    do_split(rest, acc <> "--" <> chunk, out)
  end

  defp do_split(<<"/*", rest::binary>>, acc, out) do
    {chunk, rest} = until(rest, "*/", "")
    do_split(rest, acc <> "/*" <> chunk, out)
  end

  defp do_split(<<c::utf8, rest::binary>>, acc, out),
    do: do_split(rest, acc <> <<c::utf8>>, out)

  # A doubled quote character is an escaped literal, not a close — the one case that, gotten wrong,
  # leaves the scanner inside-out for the rest of the script and hides every later statement.
  defp quoted(<<q, q, rest::binary>>, q, acc), do: quoted(rest, q, acc <> <<q, q>>)
  defp quoted(<<q, rest::binary>>, q, acc), do: {acc <> <<q>>, rest}
  defp quoted(<<c::utf8, rest::binary>>, q, acc), do: quoted(rest, q, acc <> <<c::utf8>>)
  # Unterminated: return what we have. The fragment is still gated, and SQLite will reject it.
  defp quoted(<<>>, _q, acc), do: {acc, <<>>}

  defp until(bin, delim, acc) do
    case :binary.match(bin, delim) do
      {at, len} ->
        {binary_part(bin, 0, at + len), binary_part(bin, at + len, byte_size(bin) - at - len)}

      :nomatch ->
        {bin <> acc, <<>>}
    end
  end

  defp emit(acc, out) do
    case String.trim(acc) do
      "" -> out
      stmt -> [stmt | out]
    end
  end

  # Introspects a statement without running it (the Hrana `describe` request, #34): its parameters,
  # result columns, and whether it's an EXPLAIN / read-only. `is_readonly` is the leading-keyword
  # classification (a SELECT/PRAGMA read vs a DML/DDL write) — the hint a libSQL client wants.
  #
  # DESCRIBE IS A STATEMENT PATH AND CARRIES THE SAME TWO GATES AS EXECUTE (expert review
  # 2026-08-24 #2, verified by execution). It used to run NONE of them — not the blocked-statement
  # check, not the revocation re-check — on the assumption that preparing a statement without
  # stepping it cannot have an effect. SQLite implements several pragmas OUTSIDE the VDBE, so they
  # take effect during `sqlite3_prepare` with no step at all. Measured on a real shard with the
  # tenant authorizer set:
  #
  #     before:                               writable_schema = 0, synchronous = 2
  #     describe "PRAGMA writable_schema=ON"  -> {:ok, %Describe{is_readonly: true}}
  #     describe "PRAGMA synchronous=OFF"     -> {:ok, %Describe{is_readonly: true}}
  #     after:                                writable_schema = 1, synchronous = 0
  #
  # So `describe` voided the pragma gate outright, and voided 2026-08-20 #22's per-statement
  # revocation floor — a revoked credential holding a live socket kept enumerating the tenant's
  # schema indefinitely. `describe` is reachable on both transports (`Filo.Request.handle/4`,
  # `%{"type" => "describe"}`).
  #
  # There is deliberately NO `:ro` write refusal here: a read-only client legitimately describes
  # a SELECT, and `blocked_statement/1` is what stops the pragma for every scope.
  @impl true
  def describe({_pid, _ref, conn, shard_id, _scope, token_version, _opts}, sql)
      when is_binary(sql) do
    with :ok <- describe_allowed(shard_id, token_version, sql) do
      do_describe(conn, sql)
    end
  end

  defp describe_allowed(shard_id, token_version, sql) do
    cond do
      not HranaAuth.version_current?(shard_id, token_version) ->
        {:error,
         %Error{
           message: "the token for shard \"#{shard_id}\" has been revoked",
           code: "FILO_UNAUTHORIZED",
           status: 401
         }}

      blocked = blocked_statement(sql) ->
        {:error, blocked}

      denied = lifecycle_denied(shard_id) ->
        {:error, denied}

      true ->
        :ok
    end
  end

  # THE TENANT-LIFECYCLE DENIES, RE-CHECKED PER STATEMENT (expert review 2026-08-24 #5).
  #
  # These live in `Fathom.Shards.ensure/1`, which runs once per CHECKOUT — and a checkout is
  # taken in `open/2` and released in `close/1`, i.e. once per Hrana stream. `HranaAuth`'s own
  # docstring claimed that was enough ("ensure/1 already re-checks the tombstone and suspension
  # gates on every checkout, so those two lifecycle denies did reach a live connection"). It is
  # the same reasoning error 2026-08-20 #22 corrected for token revocation: a django-libsql
  # stream lives for HOURS between requests, so per-checkout is per-session, not per-request.
  #
  # Two sequences it left open. An attacker holds a WS stream on `acme`; an operator suspends
  # `acme`; `Tenants.suspend/1` drains with a 5 s budget and the stream outlives it, so the
  # coordinator keeps serving and the attacker reads and writes indefinitely. And on a multi-node
  # fleet the control-plane call lands on whichever node the LB picked, while `Shards.drain/2`
  # and `Shards.stop/1` are local-`Registry` lookups that no-op elsewhere — so a suspend or a
  # delete issued on node A never touches the coordinator on node B, and a DELETED tenant's data
  # stays readable through the stream already open there.
  #
  # Both levers exist precisely for abusive or compromised clients, which is exactly the posture
  # of a client that already has a stream open. Two O(1) `:ets.member` reads, never Postgres —
  # the same cost argument that justified the revocation re-check beside it. The cross-node
  # coordinator-stop problem is tracked separately (`rebalance_commands` in
  # docs/component-notes.md); this makes it moot for correctness.
  defp lifecycle_denied(shard_id) do
    cond do
      Fathom.Tenants.tombstoned?(shard_id) -> open_error(:shard_tombstoned)
      Fathom.Tenants.suspended?(shard_id) -> open_error(:shard_suspended)
      true -> nil
    end
  end

  defp do_describe(conn, sql) do
    case Connection.describe(conn, sql) do
      {:ok, %{params: params, cols: cols}} ->
        {:ok,
         %Describe{
           params: params,
           cols: cols,
           is_explain: explain?(sql),
           is_readonly: not (dml?(sql) or ddl?(sql))
         }}

      {:error, reason} ->
        {:error, %Error{message: reason_to_string(reason), code: sqlite_code(reason)}}
    end
  end

  @impl true
  def close({pid, ref, conn, _shard_id, _scope, _ver, opts}) do
    Connection.close(conn)
    Shard.checkin(pid, ref)
    forget_txn_write(conn)
    if opts.template?, do: Capture.forget(conn)
    :ok
  end

  # Leading-keyword EXPLAIN detection for describe/2 (only ever called with binary sql).
  defp explain?(sql) do
    sql
    |> String.trim_leading()
    |> String.slice(0, 7)
    |> String.downcase()
    |> String.starts_with?("explain")
  end

  # --- migration capture (template shard only) ---

  # First arg is the handle's precomputed `template?` boolean (see stream_opts/1).
  defp capture(true, conn, sql, args) when is_binary(sql), do: observe(conn, sql, args)
  defp capture(_template?, _conn, _sql, _args), do: :ok

  defp observe(conn, sql, args) do
    case Capture.classify(sql) do
      :begin ->
        Capture.begin(conn, migrations_count(conn))

      :commit ->
        Capture.commit(conn, migrations_count(conn))

      :rollback ->
        Capture.rollback(conn)

      :other ->
        # Django's `INSERT INTO django_migrations` bookkeeping row is what marks a migration applied,
        # and its SQLite backend sometimes emits it AFTER the COMMIT (it has to commit before it can
        # re-enable FK checks). Route it to Capture.bookkeeping/4 so a buffer parked awaiting that row
        # is recorded — otherwise the count-rose boundary test reads the whole migration as a no-op
        # and the fleet silently never receives the version. Costs one count query on that single
        # statement; every other statement still takes the plain append. Note capture runs AFTER the
        # statement succeeded, so the count here already includes this INSERT.
        if Capture.bookkeeping?(sql) do
          Capture.bookkeeping(conn, sql, args, migrations_count(conn))
        else
          Capture.append(conn, sql, args)
        end
    end

    :ok
  rescue
    e ->
      Logger.warning("template capture failed: #{inspect(e)}")
      :ok
  catch
    :exit, reason ->
      Logger.warning("template capture unavailable: #{inspect(reason)}")
      :ok
  end

  # Per-stream config resolved ONCE at open (review 2026-07-23 #7 + #21): template?/1 re-cast
  # the CONSTANT configured template id through the ShardId regex on every successful statement
  # (and again at close), and block_tenant_ddl? re-read the app env per statement. Both are
  # operator config — a stream seeing a consistent value for its lifetime is the correct
  # semantic (the DDL guard / template identity never flip mid-stream), and every existing
  # test configures them before opening.
  defp stream_opts(shard_id) do
    %{template?: template?(shard_id), block_ddl?: block_tenant_ddl?()}
  end

  # `shard_id` here is already canonical (normalized in open/1), so normalize the configured
  # template id the same way before comparing — otherwise a mixed-case `:template_shard_id` would
  # never match and capture would silently never fire (finding #19).
  defp template?(shard_id) do
    case Fathom.ShardId.cast(Application.get_env(:fathom, :template_shard_id)) do
      {:ok, template} -> template == shard_id
      :error -> false
    end
  end

  defp migrations_count(conn) do
    case Connection.query(conn, "SELECT count(*) FROM django_migrations", []) do
      {:ok, %{rows: [[n]]}} -> n
      _ -> 0
    end
  end

  # HTTP status hints (Filo surfaces them on the pipeline path — else the transport default): a bad
  # id is a client error (400); a full node is a retryable overload (503); anything else is a 500.
  defp open_error(:invalid_shard_id),
    do: %Error{message: "invalid shard id", code: "FILO_SHARD_OPEN", status: 400}

  defp open_error(:node_at_capacity),
    do: %Error{message: "node at capacity", code: "FILO_AT_CAPACITY", status: 503}

  # An over-rate NOVEL shard creation (finding #14): the caller is asking to mint a
  # brand-new shard faster than the node's budget. 429 (back off), not 503 — the node
  # is healthy and existing shards keep serving.
  defp open_error(:novel_shard_rate_limited),
    do: %Error{message: "shard creation rate limited", code: "FILO_RATE_LIMITED", status: 429}

  # A drain is a routine, short-lived migration state ("the caller should retry
  # later"), not a server fault: 503, so client SDKs back off and retry instead of
  # treating every planned blue/green window as an error (expert review #33 — the
  # fallthrough's status-less error surfaced as the transport-default 500).
  defp open_error(:draining),
    do: %Error{message: "shard draining", code: "FILO_DRAINING", status: 503}

  # The shard already holds its max concurrent streams (#26): one tenant can't monopolize a node's
  # streams. 503 (retry later) — a checked-in stream frees a slot, and the shard is otherwise healthy.
  defp open_error(:shard_at_stream_capacity),
    do: %Error{message: "shard at stream capacity", code: "FILO_SHARD_BUSY", status: 503}

  # A suspended tenant (#20): administratively disabled, not a transient fault — 403 so the
  # client sees a distinct "forbidden" (a retry won't help; the operator must resume it).
  defp open_error(:shard_suspended),
    do: %Error{message: "tenant suspended", code: "FILO_TENANT_SUSPENDED", status: 403}

  # A deleted tenant (#15): the shard is gone for good — 410 Gone, distinct from a 400/500, so
  # the client can tell "this tenant no longer exists" from "bad request" / "server error".
  defp open_error(:shard_tombstoned),
    do: %Error{message: "tenant deleted", code: "FILO_TENANT_DELETED", status: 410}

  # Another node currently owns the lease (expert review 2026-08-01 #15). This is the SAME
  # class of routine, short-lived state as `:draining` above, and it fell through the catch-all
  # into a status-less error, which Filo's plug renders as a 500.
  #
  # The single largest producer is the migration engine: `ShardMigration.run/3` holds a
  # `migrator@<node>` lease for the whole drain → retain → pull → copy → replay → PUT →
  # cutover sequence. The holder is a LIVE node, so no steal-soon retry path applies. That made
  # every fleet schema rollout — the project's headline capability — a user-facing 500 in each
  # tenant's migration window, rather than a retry the client SDK backs off on.
  #
  # The owner is deliberately NOT in the message: it named the holding node, leaking internal
  # topology to a tenant.
  defp open_error({:shard_held, _owner, _stealable_at}),
    do: %Error{
      message: "shard temporarily owned by another node; retry",
      code: "FILO_SHARD_HELD",
      status: 503
    }

  defp open_error(reason),
    do: %Error{message: "cannot open shard: #{inspect(reason)}", code: "FILO_SHARD_OPEN"}

  @doc """
  Resolves the shard id for a request. Primary: the **Host subdomain** (e.g.
  `acme.fathom.example` → `acme`), which is how libSQL clients address a database
  (`libsql-client` rejects unknown URL query params, so a subdomain is the only
  thing the WebSocket client can carry). The `?db=` / `x-fathom-shard` fallbacks are
  a curl/dev convenience gated by `:allow_shard_override` (off in prod — see
  `maybe_override/1`); otherwise a request resolves to the configured `:default_shard`, or **`nil`
  when none is set** — the prod fail-closed posture (finding #26), which `open/1` refuses with a 400
  rather than commingling the caller into a shared shard. The result is case-normalized (#19).
  Used as `Filo.Plug`'s `:open_arg`.
  """
  @spec shard_from_conn(Plug.Conn.t()) :: String.t() | nil
  def shard_from_conn(conn) do
    (shard_from_host(conn.host) || maybe_override(conn) || default_shard())
    |> normalize_resolved()
  end

  # The fallback shard when nothing else resolves. Unset (nil) in prod ⇒ a hostless/anonymous
  # request fails closed (open(nil) → 400) instead of commingling into a shared shard (finding #26);
  # dev/test set it to "demo" for curl/localhost convenience.
  defp default_shard, do: Application.get_env(:fathom, :default_shard)

  # Downcase the resolved id (finding #19) so routing names the same canonical shard the data path
  # opens, regardless of Host case. Nil-safe: a fail-closed nil default (finding #26) passes through
  # as nil. Validation stays at open/1 / ensure/1 — this only canonicalizes case.
  defp normalize_resolved(nil), do: nil
  defp normalize_resolved(id) when is_binary(id), do: String.downcase(id, :ascii)
  # Fail closed on any non-binary reaching the trust boundary (expert review #36):
  # Plug parses `?db[]=a` into a list and `?db[k]=v` into a map, and a
  # FunctionClauseError here crashed the whole resolve path (a remote 500 oracle
  # wherever the override is enabled). nil falls through to open(nil)'s 400.
  defp normalize_resolved(_other), do: nil

  # The `?db=` / `x-fathom-shard` fallbacks are an unauthenticated shard-selection primitive
  # (finding #4): a caller who reaches a node directly controls the Host, so a bare/IP host
  # forces this fallback and `?db=<victim>` would open any shard. Gate it behind config, off
  # by default (prod-safe); dev/test/curl opt in via `:allow_shard_override`. With it off, a
  # hostless request falls through to `@default_shard`, never an attacker-named one. The
  # LB-routed subdomain path above is unaffected — it is tried first and can't be overridden.
  defp maybe_override(conn) do
    if Application.get_env(:fathom, :allow_shard_override, false),
      do: shard_from_params(conn),
      else: nil
  end

  defp shard_from_host(host) do
    # A multi-label host like "acme.fathom.example" yields "acme"; bare hosts
    # ("localhost") and IPs are not shards. A single trailing dot (a legal FQDN form
    # some clients/resolvers emit) is stripped FIRST — pre-fix "localhost." split to
    # ["localhost", ""] and promoted an otherwise-bare hostname to a shard, so the
    # same logical host routed to different shards with vs without the dot (expert
    # review #35). All remaining labels must be non-empty, so ".."/leading-dot hosts
    # never resolve.
    host = String.trim_trailing(host, ".")

    with false <- ip?(host),
         [sub | rest] when rest != [] <- String.split(host, "."),
         true <- Enum.all?([sub | rest], &(&1 != "")),
         true <- zone_matches?(rest),
         true <- Fathom.ShardId.valid?(sub) do
      sub
    else
      _ -> nil
    end
  end

  # Expert review #13: the first label of ANY multi-label host used to select a
  # shard, so the primary tenant-selection path trusted a fully attacker-controlled
  # header with no domain anchoring — the same unauthenticated primitive finding #4
  # gated the ?db= override for, left open on the main path. With
  # `config :fathom, :shard_base_domain` set (prod: SHARD_BASE_DOMAIN, e.g.
  # "fathom.example"), the first label is promoted ONLY when the remaining labels
  # equal the serving zone; anything else fails closed to the default-shard chain.
  # nil (the dev/test default) keeps the unanchored behavior for localhost rigs.
  defp zone_matches?(rest) do
    case Application.get_env(:fathom, :shard_base_domain) do
      nil ->
        true

      zone ->
        # An empty/whitespace zone matches NOTHING — denying all subdomain routing,
        # and commingling every tenant into :default_shard where that is set.
        # runtime.exs treats a blank env var as unset (round-2 #35); this is the
        # belt for a blank value arriving through any other config path.
        case zone |> String.trim() |> String.trim_leading(".") |> String.trim_trailing(".") do
          "" ->
            Logger.warning("shard_base_domain is blank; treating as unset (misconfig)")
            true

          normalized ->
            String.downcase(Enum.join(rest, ".")) == String.downcase(normalized)
        end
    end
  end

  defp shard_from_params(conn) do
    conn = Plug.Conn.fetch_query_params(conn)

    case conn.query_params["db"] do
      # Only a plain string selects a shard — `?db[]=`/`?db[k]=` arrive as list/map
      # and must not crash resolution (expert review #36).
      db when is_binary(db) -> db
      _ -> conn |> Plug.Conn.get_req_header("x-fathom-shard") |> List.first()
    end
  end

  defp ip?(host), do: host =~ ~r/^\d+\.\d+\.\d+\.\d+$/

  # A read-only statement (SELECT / read PRAGMA) returns columns but mutates nothing → report no
  # affected rows / rowid (SQLite otherwise leaks the previous write's counters). But an
  # `INSERT/UPDATE/DELETE ... RETURNING` returns columns AND mutates, so classifying by
  # "returns columns" alone reported affected_row_count 0 / last_insert_rowid nil / rows_written 0
  # for every RETURNING write (expert review 2026-07-14 #42). exqlite exposes no
  # `sqlite3_stmt_readonly`, so use the leading DML keyword: a column-returning statement is
  # read-only unless it is a mutation.
  defp to_stmt_result(
         %{
           columns: cols,
           rows: rows,
           row_count: row_count,
           num_changes: changes,
           last_insert_rowid: rowid
         },
         dml?
       ) do
    read_only? = cols != [] and not dml?

    %StmtResult{
      cols: cols,
      rows: rows,
      affected_row_count: if(read_only?, do: 0, else: changes),
      last_insert_rowid: if(not read_only? and changes > 0, do: rowid, else: nil),
      # Counted once during collect — the old `length(rows)` here was a second full O(R)
      # walk of the just-materialized list (review 2026-07-23 #16).
      rows_read: row_count,
      rows_written: if(read_only?, do: 0, else: changes)
    }
  end

  @dml_prefixes ~w(insert update delete replace)

  # Leading-keyword mutation detection so a `... RETURNING` write is classified as a mutation, not
  # a query. A CTE-prefixed `WITH ... INSERT` is not detected here — Django emits plain
  # `INSERT ... RETURNING "id"`, the case that matters; the CTE-DML edge falls back to the old
  # column-based read classification.
  defp dml?(sql) when is_binary(sql) do
    # Only the leading keyword matters — slice + downcase a few chars rather than the whole
    # statement, since this runs on every query result.
    Enum.any?(@dml_prefixes, &String.starts_with?(lead(sql, 7), &1))
  end

  defp dml?(_), do: false

  # THE FENCE'S OWN PREDICATE. Deliberately separate from `dml?/1`, and deliberately FAIL-CLOSED.
  #
  # `Fathom.Shard.WriteFence` is the circuit breaker that stops a provably-stealable node from
  # ACKing writes a peer will quarantine on partition-heal. Unlike the `:ro` scope it has NO engine
  # backstop — SQLite refuses a write on a `mode: :readonly` handle no matter how the SQL is
  # spelled, but a fenced shard's handle is perfectly writable, so this predicate IS the entire
  # enforcement. Two holes were verified by execution (expert review 2026-08-26 #1):
  #
  #   * `WITH src(v) AS (VALUES (?1)) INSERT INTO t(x) SELECT v FROM src` mutates
  #     (num_changes: 1) while `lead(sql, 7)` is "with sr" — matching no `@dml_prefixes` and no
  #     `@ddl_leads`, so `write? == false` and the fence was never consulted.
  #   * `execute_sequence/2` never consulted the fence at all.
  #
  # WHY NOT JUST ADD "with" TO @dml_prefixes: `dml?/1` also drives `to_stmt_result/2`'s
  # `read_only?`, so a read-only `WITH … SELECT` would start reporting the inherited
  # `sqlite3_changes()` as `affected_row_count` — re-opening the defect fixed by 2026-07-14 #42
  # from the other side. The two questions are genuinely different: "did this statement mutate"
  # wants to be precise, "might this statement mutate" wants to be paranoid.
  #
  # So this returns true for anything NOT known to be a pure read. The trade is explicit: while a
  # shard is fenced, a read-only CTE (and any statement whose head this file does not recognise) is
  # also refused — on a node that has already lost its lease and is returning 503s for writes
  # anyway. Refusing a read there is cheap; ACKing a write that will be quarantined is not.
  defp write_candidate?(sql) when is_binary(sql) do
    not read_shaped?(lead(sql, 7))
  end

  defp write_candidate?(_), do: true

  # The only statement heads that cannot mutate the tenant's data.
  #
  #   * `with` is absent ON PURPOSE — a CTE can prefix INSERT/UPDATE/DELETE, which is the bypass
  #     this predicate exists to close.
  #   * `pragma` and `analyze` are absent on purpose too, and were in the first draft of this list.
  #     A pragma ASSIGNMENT can change the file (`journal_mode`, `max_page_count`), and `ANALYZE`
  #     writes `sqlite_stat1`. Both are writes; on a fenced shard both should be refused.
  #   * Transaction control IS listed. Refusing COMMIT on a fenced shard would not un-ACK anything
  #     — the statements inside the transaction were already gated individually — and refusing
  #     ROLLBACK would leave the stream's transaction open with no way to close it. Blocking them
  #     wedges clients without preventing a single write.
  @read_shaped ~w(select explain begin commit rollback savepoint release)

  defp read_shaped?(head), do: Enum.any?(@read_shaped, &String.starts_with?(head, &1))

  # --- statement gate (expert review 2026-08-01 #1, #7, #8) ---------------------------------
  #
  # `String.trim_leading/1` removes whitespace, NOT SQL comments, so every leading-keyword
  # classifier here was defeated by a `/* c */` or `-- c\n` prefix. That was only a
  # *durability* miss for the whitelists (an unrecognized head falls through to the
  # conservative "assume it wrote" path), but for the two AUTHORIZATION gates riding
  # `dml?/ddl?` — the `:ro` refusal and `:block_tenant_ddl` — it was a bypass:
  # `/* c */ INSERT …` classified as a read.
  #
  # Hot path: this runs on every statement, so the common case (a statement starting with a
  # letter) costs one extra 2-byte match over the previous `trim_leading`. Only a statement
  # that actually opens with a comment pays the scan.
  defp lead(sql, n), do: sql |> strip_lead_comments() |> String.slice(0, n) |> String.downcase()

  defp strip_lead_comments(sql) do
    case String.trim_leading(sql) do
      "/*" <> rest -> rest |> after_delim("*/") |> strip_lead_comments()
      "--" <> rest -> rest |> after_delim("\n") |> strip_lead_comments()
      other -> other
    end
  end

  # An unterminated comment leaves no statement behind — "" classifies as nothing and falls to
  # the conservative path, which is the safe direction for every caller here.
  defp after_delim(bin, delim) do
    case :binary.match(bin, delim) do
      {i, len} -> binary_part(bin, i + len, byte_size(bin) - i - len)
      :nomatch -> ""
    end
  end

  # Verbs a tenant may never run. `attach`/`detach` are the cross-tenant read+write breach
  # (#1); `vacuum` covers `VACUUM INTO '<any path>'`, an arbitrary-file-write primitive that a
  # read-only handle does NOT stop (#8). All three are already denied by the authorizer on the
  # tenant handle — this list exists so the client gets a code it can act on.
  @blocked_verbs ~w(attach detach vacuum)

  # Pragma ASSIGNMENTS a tenant may set. An allow-list, not a deny-list: the pragmas that
  # matter are fathom's own safety mechanisms, and every one of them was re-settable by
  # client SQL (verified — `max_page_count=999999999` defeated the size cap,
  # `synchronous=OFF` defeated per-commit durability, and `journal_mode=DELETE` is a DURABLE,
  # database-level change that `control_statement?/1` classified as clean, so an idle
  # `drop_clean` discarded the local copy without flushing).
  #
  # These are the connection-local pragmas a real client legitimately sets: Django's SQLite
  # backend issues `foreign_keys` and, during migrations, `legacy_alter_table` and
  # `defer_foreign_keys`. READ forms (`PRAGMA synchronous`, `PRAGMA table_info(t)`) are
  # always allowed — only assignment is gated. Widen with `:tenant_pragma_allow` rather than
  # editing this list, so a client that needs one more knob does not need a fathom release.
  # `user_version`/`application_id` are deliberately ALLOWED for a `:rw` stream. `user_version`
  # is fathom's own O(1) schema-version gate, and stamping it through the data path is a
  # documented, tested capability — expert review #15 fixed the opposite bug (the stamp being
  # classified as clean and then LOST on an idle drop), and shard_durability_test.exs pins the
  # round trip. The hole finding #7 actually named is a READ-ONLY credential rewriting the gate,
  # and that is closed at the engine: a `:ro` stream gets a `mode: :readonly` handle, so SQLite
  # refuses the write outright. `:block_tenant_ddl` remains the lever for "schema evolution goes
  # through the migration engine, not the tenant".
  #
  # `schema_version` WAS in this list and is NOT fathom's stamp — do not re-add it (expert review
  # 2026-08-24 #4, verified by execution). It was admitted here on the reasoning above, but
  # fathom's three-place stamp is `django_migrations`, `PRAGMA user_version`, and
  # `shards.schema_version` — the last being a POSTGRES COLUMN, not a SQLite pragma. `PRAGMA
  # schema_version` is SQLite's internal schema cookie: the engine compares it on every statement
  # to decide whether a cached prepared statement must be recompiled, and SQLite's own
  # documentation says "Misuse of this pragma can result in database corruption." Verified with
  # the tenant authorizer set: `PRAGMA schema_version = 999` succeeded, persisted to the file
  # header, and a second connection read back 999.
  #
  # Freezing it defeats the engine half of review 2026-07-24 #17. That fix purges the statement
  # cache on DDL THIS executor sees; a schema change made on ANOTHER stream is caught only by
  # `prepare_v2` recompilation, which keys on this cookie. Frozen, a cached statement keeps its
  # pre-ALTER column list, so `SELECT *` returns pre-ALTER columns for post-ALTER rows — the same
  # class as the recorded `Copy.migrate/4` "no such table: app_thing" incident, and reachable
  # from the migration replay and the coordinator's snapshot connections too.
  #
  # Django does not need it: `test/support/fixtures/django_migrate_capture.json`, captured from a
  # real `manage.py migrate`, contains no `PRAGMA schema_version` at all (its only pragma is
  # `foreign_key_check`). It stays in `@durable_pragmas` on purpose — that list governs whether a
  # pragma write DIRTIES the shard, and if this one is ever reached again it must still flush.
  #
  # `wal_checkpoint` is a maintenance operation, not a safety defeat — it is the same
  # checkpoint the coordinator runs after each snapshot, and fathom's own durability tests
  # drive it through this path.
  @tenant_pragma_allow ~w(foreign_keys defer_foreign_keys legacy_alter_table busy_timeout
                          cache_size temp_store recursive_triggers ignore_check_constraints
                          case_sensitive_like automatic_index reverse_unordered_selects
                          analysis_limit threads user_version application_id
                          wal_checkpoint incremental_vacuum shrink_memory)

  # Pragmas no configuration may re-open, checked BEFORE `extra_pragma_allow()` (expert review
  # 2026-08-24 #3). `query_only` is not merely another protective knob: on the one path where a
  # `:ro` handle cannot be opened `mode: :readonly` — a database whose `-wal` needs recovery —
  # it IS the read-only scope, and `Fathom.Shard.Connection` says so where it sets it. A
  # `:tenant_pragma_allow` entry naming it would silently convert every such stream's scope into
  # an advisory one, so the widening lever deliberately cannot reach it.
  @tenant_pragma_deny ~w(query_only)

  # SQLite spells an argument-taking READ the same way it spells a setter — `PRAGMA
  # table_info(t)` and `PRAGMA journal_mode(delete)` are syntactically identical — so the
  # gate cannot classify assignment-vs-read from the shape alone. These are the
  # introspection pragmas, which take an argument but mutate nothing; a client (Django's
  # schema editor and introspection layer especially) needs them.
  #
  # A BARE pragma (`PRAGMA journal_mode`, no argument) is a read and is never gated — it
  # discloses only this connection's own configuration.
  @tenant_pragma_introspect ~w(table_info table_xinfo table_list foreign_key_list index_list
                               index_info index_xinfo database_list collation_list
                               function_list module_list pragma_list compile_options
                               freelist_count page_count quick_check integrity_check
                               foreign_key_check stats optimize)

  defp blocked_statement(sql) when is_binary(sql) do
    head = lead(sql, 7)

    cond do
      Enum.any?(@blocked_verbs, &String.starts_with?(head, &1)) ->
        %Error{
          message: "statement not permitted on a tenant shard",
          code: "FILO_STATEMENT_BLOCKED",
          status: 403
        }

      String.starts_with?(head, "pragma") ->
        blocked_pragma(sql)

      true ->
        nil
    end
  end

  defp blocked_statement(_sql), do: nil

  # `PRAGMA [schema.]name = value` / `PRAGMA [schema.]name(value)` is an assignment;
  # `PRAGMA [schema.]name` alone is a read. SQLite accepts both setter forms, so both are
  # gated. The schema qualifier is stripped — `PRAGMA main.journal_mode=delete` sets the same
  # thing as `PRAGMA journal_mode=delete`.
  # THE NAME IS EXTRACTED STRUCTURALLY, NOT FROM A FIXED WINDOW (expert review 2026-08-20 #19).
  #
  # This used to be `String.slice(6, 200)` followed by `String.split(body, ["=", "("], parts: 2)`,
  # with a single-element result read as "no `=` and no `(` — a bare read". Insignificant
  # whitespace between PRAGMA and the name pushes the `=` past 200 characters, the split returns
  # one element, and the gate concludes "bare read" and executes the assignment.
  #
  # Verified against this project's own exqlite 0.37.0 with the tenant authorizer set:
  #
  #     "PRAGMA" <> String.duplicate(" ", 210) <> "max_page_count=12345"
  #     #=> gate: NOT BLOCKED; engine: PRAGMA max_page_count -> 12345
  #
  # Every protective pragma was settable that way: `max_page_count` defeats :shard_max_bytes (a
  # shard that keeps ACKNOWLEDGING writes past the cap and can never upload again — unbounded RPO
  # for that tenant AND a node-wide disk-fill outage for every co-tenant), `synchronous=OFF`
  # defeats per-commit durability, `wal_autocheckpoint=0` and `locking_mode=EXCLUSIVE` are
  # noisy-neighbour levers against the coordinator's flush path.
  #
  # Scanning to the first delimiter is also CHEAPER than the old slice: O(name) rather than
  # O(200), on a path that runs for every statement.
  defp blocked_pragma(sql) do
    rest =
      sql
      |> strip_lead_comments()
      |> String.slice(6..-1//1)
      |> String.trim_leading()

    {name_raw, tail} = split_pragma_name(rest)
    name = String.downcase(name_raw)

    allowed? =
      name not in @tenant_pragma_deny and
        (name in @tenant_pragma_allow or name in @tenant_pragma_introspect or
           name in extra_pragma_allow())

    cond do
      allowed? ->
        nil

      # Nothing after the name but whitespace or a terminator: a bare read, always allowed.
      not pragma_assignment?(tail) and not argumentish_tail?(tail) ->
        nil

      true ->
        %Error{
          message: "PRAGMA #{name} cannot be set on a tenant shard",
          code: "FILO_PRAGMA_BLOCKED",
          status: 403
        }
    end
  end

  # `PRAGMA [schema.]name` — read STRUCTURALLY, tolerating insignificant whitespace and SQLite
  # quoting at every position an identifier may carry them. Returns {unquoted_name, tail}.
  #
  # THE WHITESPACE IS THE WHOLE POINT (expert review 2026-08-24 #1). The previous parser scanned
  # the name to the first `=`, `(`, `;` or whitespace and never treated `.` specially, so
  #
  #     PRAGMA main . writable_schema = ON
  #
  # yielded name_raw = "main" and tail = " . writable_schema = ON". `pragma_assignment?/1` trims
  # and sees "." — not "=" or "(" — concludes "bare read, always allowed", and the assignment
  # executed. Verified against exqlite with the tenant authorizer set: `writable_schema`,
  # `max_page_count` and `synchronous` were all settable that way, and `PRAGMA main. foo = 1`
  # (space only AFTER the dot) plus tab/newline variants worked identically. This is the same
  # defect class as 2026-08-20 #19 (the `String.slice(6, 200)` window), reintroduced by the
  # parser that replaced it — hence structural parsing rather than a third delimiter tweak.
  defp split_pragma_name(rest) do
    {first, after_first} = read_identifier(rest)

    case String.trim_leading(after_first) do
      "." <> after_dot -> after_dot |> String.trim_leading() |> read_identifier()
      _ -> {first, after_first}
    end
  end

  # A SQLite identifier: bare, "double-quoted", [bracketed], or `backticked`. Doubling escapes
  # the delimiter in the `"` and backtick forms; SQLite's `[...]` form has NO escape, so `]`
  # always closes it. The name is returned UNQUOTED, so `PRAGMA "writable_schema" = ON` compares
  # against the allow-list as `writable_schema`. Those quoted forms happened to fail closed
  # before this change (the name kept its quotes and matched nothing) — but only by accident,
  # and they would have failed OPEN the moment anyone added unquoting without fixing the parse.
  defp read_identifier(<<?", rest::binary>>), do: read_delimited(rest, ?", true, "")
  defp read_identifier(<<?`, rest::binary>>), do: read_delimited(rest, ?`, true, "")
  defp read_identifier(<<?[, rest::binary>>), do: read_delimited(rest, ?], false, "")
  defp read_identifier(bare), do: split_at_pragma_delim(bare, "")

  defp read_delimited(<<c::utf8, c2::utf8, rest::binary>>, close, true, acc)
       when c == close and c2 == close,
       do: read_delimited(rest, close, true, acc <> <<c::utf8>>)

  defp read_delimited(<<c::utf8, rest::binary>>, close, _doubling?, acc) when c == close,
    do: {acc, rest}

  defp read_delimited(<<c::utf8, rest::binary>>, close, doubling?, acc),
    do: read_delimited(rest, close, doubling?, acc <> <<c::utf8>>)

  # Unterminated quote. Return what we have: the name will not match the allow-list and the
  # statement is refused, which is the safe direction for a malformed identifier.
  defp read_delimited("", _close, _doubling?, acc), do: {acc, ""}

  # The bare pragma name runs to the first `=`, `(`, `;`, `.` or whitespace. `.` is a delimiter
  # so the unspaced `PRAGMA main.foreign_keys=ON` splits the same way the spaced form does —
  # without it, the name would read as "main.foreign_keys" and Django's own pragmas would 403.
  defp split_at_pragma_delim(<<c::utf8, rest::binary>>, acc)
       when c not in [?=, ?(, ?;, ?., ?\s, ?\t, ?\n, ?\r],
       do: split_at_pragma_delim(rest, acc <> <<c::utf8>>)

  defp split_at_pragma_delim(tail, acc), do: {acc, tail}

  # Belt, worn because the parser above is the SECOND one to be wrong here. If anything that
  # looks like an argument survives in this statement after a name the allow-list does not
  # cover, refuse rather than guess — over-refusal costs a 403, under-refusal costs the bypass
  # above. Scoped to THIS statement (cut at the first `;`) so a bare read followed by unrelated
  # batched statements is not caught: `PRAGMA journal_mode; UPDATE t SET a=1` stays a read.
  defp argumentish_tail?(tail) do
    [this_statement | _] = String.split(tail, ";", parts: 2)
    String.contains?(this_statement, ["=", "("])
  end

  # An assignment is `= value` or `(value)`. Anything else — end of statement, a bare `;`, trailing
  # whitespace — is a read. Leading whitespace is skipped so `PRAGMA foo   =   1` is still an
  # assignment; that gap is exactly what the old windowed parse lost.
  defp pragma_assignment?(tail) do
    case String.trim_leading(tail) do
      "=" <> _ -> true
      "(" <> _ -> true
      _ -> false
    end
  end

  defp extra_pragma_allow, do: Application.get_env(:fathom, :tenant_pragma_allow, [])

  defp block_tenant_ddl?, do: Application.get_env(:fathom, :block_tenant_ddl, false)

  @ddl_leads ~w(create alter drop)

  # Leading-keyword schema-DDL detection (CREATE/ALTER/DROP — table/index/view/trigger). Cheap
  # slice, since this runs on every statement when strict mode is on. Comment-stripped: this
  # gates `:block_tenant_ddl`, which a `/* c */ CREATE TABLE` prefix defeated (#7).
  defp ddl?(sql) when is_binary(sql) do
    Enum.any?(@ddl_leads, &String.starts_with?(lead(sql, 6), &1))
  end

  defp ddl?(_), do: false

  # `:busy` (the busy-timeout expiry from Connection) is the SQLite "database is locked"
  # condition; give it the human message a client expects instead of the inspected atom ":busy"
  # (expert review 2026-07-14 #3).
  defp reason_to_string(:busy), do: "database is locked"
  defp reason_to_string(reason) when is_binary(reason), do: reason
  defp reason_to_string(reason), do: inspect(reason)

  # Map an exqlite error reason to a real Hrana/libSQL error `code` string so a client's driver
  # can classify it (expert review 2026-07-14 #3): Django keys `IntegrityError` off
  # `SQLITE_CONSTRAINT*` and "database is locked"/`OperationalError` off `SQLITE_BUSY`, so
  # flattening everything to `SQLITE_ERROR` broke `get_or_create` race handling, unique/FK
  # violation handling, and busy-retry loops. exqlite surfaces the extended condition in the
  # message text (there's no result-code accessor), so classify on that; `:busy` is our own atom.
  defp sqlite_code(:busy), do: "SQLITE_BUSY"

  defp sqlite_code(reason) when is_binary(reason) do
    r = String.downcase(reason)

    cond do
      contains?(r, "unique constraint failed") -> "SQLITE_CONSTRAINT_UNIQUE"
      contains?(r, "foreign key constraint failed") -> "SQLITE_CONSTRAINT_FOREIGNKEY"
      contains?(r, "not null constraint failed") -> "SQLITE_CONSTRAINT_NOTNULL"
      contains?(r, "primary key constraint failed") -> "SQLITE_CONSTRAINT_PRIMARYKEY"
      contains?(r, "check constraint failed") -> "SQLITE_CONSTRAINT_CHECK"
      contains?(r, "constraint failed") -> "SQLITE_CONSTRAINT"
      contains?(r, "readonly") -> "SQLITE_READONLY"
      contains?(r, "disk is full") -> "SQLITE_FULL"
      contains?(r, "database is locked") or contains?(r, "database is busy") -> "SQLITE_BUSY"
      true -> "SQLITE_ERROR"
    end
  end

  defp sqlite_code(_), do: "SQLITE_ERROR"

  defp contains?(haystack, needle), do: String.contains?(haystack, needle)
end
