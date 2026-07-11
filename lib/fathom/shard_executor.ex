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
            {:ok, {pid, ref, conn, shard_id}}

          {:error, reason} ->
            Shard.checkin(pid, ref)
            {:error, open_error(reason)}
        end

      {:error, reason} ->
        {:error, open_error(reason)}
    end
  end

  @impl true
  def execute({_pid, _ref, _conn, shard_id} = handle, %Stmt{} = stmt) do
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

  defp do_execute({_pid, _ref, conn, shard_id}, %Stmt{sql: sql, args: args}) do
    started = System.monotonic_time()

    case Connection.query(conn, sql, args) do
      {:ok, result} ->
        # A write bumps the shard's write counter so the periodic durability flush knows local
        # holds un-flushed changes; a read-only shard stays clean and skips the upload (the
        # durability-flush storm fix). Lock-free ETS from this stream process — no per-write cast
        # to the coordinator (finding #27).
        if wrote?(result, sql), do: Fathom.Shard.WriteCounter.bump(shard_id)
        # On the reserved template shard, feed each successful statement to the
        # migration capture so a Django migrate becomes a fleet version.
        capture(shard_id, conn, sql)
        stmt_result = to_stmt_result(result)
        # Per-shard load: the query-cost signal for the rebalancer. Lock-free ETS bump,
        # gated + off by default (see Fathom.ShardLoad).
        Fathom.ShardLoad.record_query(
          shard_id,
          stmt_result.rows_read,
          stmt_result.rows_written
        )

        # Node-wide query latency for the metrics layer (Prometheus distribution / the dashboard's
        # latency chart). Deliberately UN-tagged — a per-shard latency histogram at millions of
        # shards is the cardinality death Fathom.ShardLoad's read-API exists to avoid. On success
        # only, matching the cold_open convention. A no-op when no reporter is attached.
        :telemetry.execute(
          [:fathom, :shard, :query],
          %{duration: System.monotonic_time() - started},
          %{}
        )

        {:ok, stmt_result}

      {:error, reason} ->
        {:error, %Error{message: reason_to_string(reason), code: "SQLITE_ERROR"}}
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

  # The assignment form (`pragma [db.]name = value`) of a header-writing pragma; the
  # bare read form (`pragma user_version`) has no `=` and stays a read.
  defp durable_pragma_write?(lead) do
    String.starts_with?(lead, "pragma") and String.contains?(lead, "=") and
      Enum.any?(@durable_pragmas, &String.contains?(lead, &1))
  end

  # exqlite exposes no autocommit query; libSQL's hrana2 clients never ask, and
  # hrana3 `is_autocommit` is consulted only by batch conditions our paths don't
  # use. Report true.
  @impl true
  def autocommit?(_handle), do: true

  # The coordinator is the connection's owner (Filo's owner seam): the stream holding this
  # handle monitors it and tears down — closing the exqlite connection — if it dies. Without
  # this, a stream outliving a crashed coordinator kept writing as an orphan: the NEXT
  # checkout re-creates a fresh coordinator (restart: :temporary) that doesn't know the old
  # connection exists, so its idle flush-and-drop checkpoints past the orphan's un-folded WAL
  # frames and unlinks the db/wal/shm under it — the orphan's writes land in unlinked inodes
  # and vanish (finding #8, the residual orphan-writer race).
  @impl true
  def owner({pid, _ref, _conn, _shard_id}), do: pid

  @impl true
  def close({pid, ref, conn, shard_id}) do
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

  # A statement that returns columns is a query: report no affected rows / rowid
  # (SQLite otherwise leaks the previous write's counters).
  defp to_stmt_result(%{
         columns: cols,
         rows: rows,
         num_changes: changes,
         last_insert_rowid: rowid
       }) do
    query? = cols != []

    %StmtResult{
      cols: cols,
      rows: rows,
      affected_row_count: if(query?, do: 0, else: changes),
      last_insert_rowid: if(not query? and changes > 0, do: rowid, else: nil),
      rows_read: length(rows),
      rows_written: if(query?, do: 0, else: changes)
    }
  end

  defp reason_to_string(reason) when is_binary(reason), do: reason
  defp reason_to_string(reason), do: inspect(reason)
end
