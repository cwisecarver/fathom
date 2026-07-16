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

  alias Fathom.Migrator.Capture
  alias Fathom.Shard
  alias Fathom.Shard.Connection
  alias Fathom.Shards
  alias Filo.{Error, Stmt, StmtResult}

  @impl true
  # No shard could be resolved for the request (no subdomain, override off, and no configured
  # default — the prod fail-closed posture, finding #26). Refuse rather than commingle the caller
  # into a shared default shard. A 400 status so Filo's HTTP pipeline surfaces it as a client error.
  def open(nil),
    do: {:error, %Error{message: "no shard specified", code: "FILO_NO_SHARD", status: 400}}

  def open(shard_id) do
    # Normalize (validate + downcase) at this trust boundary so the handle's id — which drives the
    # write counter, template-capture check, and load counters — matches the registry key / file /
    # S3 key that Shards.checkout uses (finding #19). Build the handle from the canonical id.
    case Fathom.ShardId.cast(shard_id) do
      {:ok, id} -> do_open(id)
      :error -> {:error, open_error(:invalid_shard_id)}
    end
  end

  defp do_open(shard_id) do
    case Shards.checkout(shard_id) do
      {:ok, pid, ref, path} ->
        case Connection.open(path) do
          {:ok, conn} ->
            # The token's scope (rw/ro, #24) was stashed by HranaAuth.authorize/2 during this same
            # stream open (same process); it rides the handle so execute/2 can enforce read-only
            # across baton-resumes. Defaults to :rw when auth is disabled (no token) or absent.
            {:ok, {pid, ref, conn, shard_id, Fathom.HranaAuth.take_scope()}}

          {:error, reason} ->
            Shard.checkin(pid, ref)
            {:error, open_error(reason)}
        end

      {:error, reason} ->
        {:error, open_error(reason)}
    end
  end

  @impl true
  def execute({_pid, _ref, _conn, shard_id, _scope} = handle, %Stmt{} = stmt) do
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

  defp do_execute({_pid, _ref, _conn, shard_id, scope} = handle, %Stmt{sql: sql} = stmt) do
    cond do
      # A read-only token (#24) may only read: refuse any write (DML or DDL) with a distinct 403 so
      # export/analytics/BI credentials can't mutate a tenant. Checked before DDL-block/run so a `ro`
      # token is refused even where a full token would be allowed.
      scope == :ro and write?(sql) ->
        {:error,
         %Error{message: "read-only token cannot write", code: "FILO_READONLY", status: 403}}

      # Block client-issued DDL on a tenant shard when strict mode is on (expert review 2026-07-14
      # #7): a direct `manage.py migrate` against a tenant advances its schema + `django_migrations`
      # while fathom's three-place version stamp stays at 0, so the shard then reads as a laggard, the
      # engine replays the captured chain onto the already-migrated file, hits "already exists", and
      # quarantines the tenant. Refuse loudly at the client instead — schema evolution goes through the
      # template + migration engine, never a direct tenant migrate. The template (the capture source)
      # is exempt, and the engine's own replay uses `Connection` directly (not this Hrana path), so
      # neither is affected. Off by default (`:block_tenant_ddl`); enable in prod to enforce the model.
      block_tenant_ddl?() and not template?(shard_id) and ddl?(sql) ->
        {:error,
         %Error{
           message:
             "schema changes must go through the migration engine on the template shard, not a " <>
               "direct migrate on tenant \"#{shard_id}\"",
           code: "FILO_DDL_BLOCKED",
           status: 400
         }}

      true ->
        run_statement(handle, stmt)
    end
  end

  # A write for read-only-scope purposes: data mutation (DML) or schema change (DDL).
  defp write?(sql), do: dml?(sql) or ddl?(sql)

  defp run_statement({_pid, _ref, conn, shard_id, _scope}, %Stmt{sql: sql, args: args}) do
    started = System.monotonic_time()

    case Connection.query(conn, sql, args) do
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
        if wrote?(result, sql) or ends_transaction?(sql),
          do: Fathom.Shard.WriteCounter.bump(shard_id)

        # On the reserved template shard, feed each successful statement to the
        # migration capture so a Django migrate becomes a fleet version.
        capture(shard_id, conn, sql)
        stmt_result = to_stmt_result(result, sql)
        # Per-shard load: the query-cost signal for the rebalancer. Lock-free ETS bump,
        # gated + off by default (see Fathom.ShardLoad).
        Fathom.ShardLoad.record_query(
          shard_id,
          stmt_result.rows_read,
          stmt_result.rows_written
        )

        latency = System.monotonic_time() - started

        # Per-shard tail latency: the "which shard is slow" companion to ShardLoad's "which shard
        # is hot". Recorded in a per-shard ETS histogram (Fathom.ShardLatency), NOT a metric tag —
        # a per-shard Prometheus label is the cardinality death the read-API avoids. Rides
        # ShardLoad's :shard_load gate; one lock-free ETS bump on the same per-shard key class.
        Fathom.ShardLatency.record(shard_id, latency)

        # Node-wide query latency for the metrics layer (Prometheus distribution / the dashboard's
        # latency chart). Deliberately UN-tagged, for the same cardinality reason. On success only,
        # matching the cold_open convention. A no-op when no reporter is attached.
        :telemetry.execute([:fathom, :shard, :query], %{duration: latency}, %{})

        {:ok, stmt_result}

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
  # sqlite3_changes() is not reset by a SELECT, so a read following a write on the same
  # connection can inherit that write's change count and be over-marked dirty. That's the
  # safe direction (an extra flush, never lost data) and the shard is already dirty from
  # the write, so it costs nothing in practice.
  defp wrote?(%{columns: cols, num_changes: changes}, sql) do
    cond do
      changes > 0 -> true
      cols != [] -> false
      control_statement?(sql) -> false
      true -> true
    end
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
  @durable_pragmas ~w(user_version application_id schema_version)

  defp control_statement?(sql) when is_binary(sql) do
    lead = sql |> String.trim_leading() |> String.downcase()

    Enum.any?(@control_prefixes, &String.starts_with?(lead, &1)) and
      not durable_pragma_write?(lead)
  end

  defp control_statement?(_sql), do: false

  # A transaction-committing statement (COMMIT / END — SQLite treats END as a COMMIT synonym).
  # This is the durability boundary where in-flight INSERTs become committed; see the bump site
  # in do_execute/2 for why the commit must re-dirty the shard. ROLLBACK is deliberately excluded
  # (it discards the writes, so there is nothing new to flush). Matches the leading token of the
  # whole statement, so a trigger body's inner `... BEGIN ... END` (the statement starts with
  # CREATE) is never mistaken for a transaction commit.
  defp ends_transaction?(sql) when is_binary(sql) do
    lead = sql |> String.trim_leading() |> String.downcase()
    String.starts_with?(lead, "commit") or String.starts_with?(lead, "end")
  end

  defp ends_transaction?(_sql), do: false

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
  def autocommit?({_pid, _ref, conn, _shard_id, _scope}), do: Connection.autocommit?(conn)

  # The coordinator is the connection's owner (Filo's owner seam): the stream holding this
  # handle monitors it and tears down — closing the exqlite connection — if it dies. Without
  # this, a stream outliving a crashed coordinator kept writing as an orphan: the NEXT
  # checkout re-creates a fresh coordinator (restart: :temporary) that doesn't know the old
  # connection exists, so its idle flush-and-drop checkpoints past the orphan's un-folded WAL
  # frames and unlinks the db/wal/shm under it — the orphan's writes land in unlinked inodes
  # and vanish (finding #8, the residual orphan-writer race).
  @impl true
  def owner({pid, _ref, _conn, _shard_id, _scope}), do: pid

  @impl true
  def close({pid, ref, conn, shard_id, _scope}) do
    Connection.close(conn)
    Shard.checkin(pid, ref)
    if template?(shard_id), do: Capture.forget(conn)
    :ok
  end

  # --- migration capture (template shard only) ---

  defp capture(shard_id, conn, sql) when is_binary(sql) do
    if template?(shard_id), do: observe(conn, sql)
    :ok
  end

  defp capture(_shard_id, _conn, _sql), do: :ok

  defp observe(conn, sql) do
    case Capture.classify(sql) do
      :begin -> Capture.begin(conn, migrations_count(conn))
      :commit -> Capture.commit(conn, migrations_count(conn))
      :rollback -> Capture.rollback(conn)
      :other -> Capture.append(conn, sql)
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

  # A suspended tenant (#20): administratively disabled, not a transient fault — 403 so the
  # client sees a distinct "forbidden" (a retry won't help; the operator must resume it).
  defp open_error(:shard_suspended),
    do: %Error{message: "tenant suspended", code: "FILO_TENANT_SUSPENDED", status: 403}

  # A deleted tenant (#15): the shard is gone for good — 410 Gone, distinct from a 400/500, so
  # the client can tell "this tenant no longer exists" from "bad request" / "server error".
  defp open_error(:shard_tombstoned),
    do: %Error{message: "tenant deleted", code: "FILO_TENANT_DELETED", status: 410}

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
         %{columns: cols, rows: rows, num_changes: changes, last_insert_rowid: rowid},
         sql
       ) do
    read_only? = cols != [] and not dml?(sql)

    %StmtResult{
      cols: cols,
      rows: rows,
      affected_row_count: if(read_only?, do: 0, else: changes),
      last_insert_rowid: if(not read_only? and changes > 0, do: rowid, else: nil),
      rows_read: length(rows),
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
    head = sql |> String.trim_leading() |> String.slice(0, 7) |> String.downcase()
    Enum.any?(@dml_prefixes, &String.starts_with?(head, &1))
  end

  defp dml?(_), do: false

  defp block_tenant_ddl?, do: Application.get_env(:fathom, :block_tenant_ddl, false)

  @ddl_leads ~w(create alter drop)

  # Leading-keyword schema-DDL detection (CREATE/ALTER/DROP — table/index/view/trigger). Cheap
  # slice, since this runs on every statement when strict mode is on.
  defp ddl?(sql) when is_binary(sql) do
    lead = sql |> String.trim_leading() |> String.slice(0, 6) |> String.downcase()
    Enum.any?(@ddl_leads, &String.starts_with?(lead, &1))
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
