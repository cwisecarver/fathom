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

  defp control_statement?(sql) when is_binary(sql) do
    lead = sql |> String.trim_leading() |> String.downcase()
    Enum.any?(@control_prefixes, &String.starts_with?(lead, &1))
  end

  defp control_statement?(_sql), do: false

  # exqlite exposes no autocommit query; libSQL's hrana2 clients never ask, and
  # hrana3 `is_autocommit` is consulted only by batch conditions our paths don't
  # use. Report true.
  @impl true
  def autocommit?(_handle), do: true

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
    # ("localhost") and IPs are not shards.
    with false <- ip?(host),
         [sub, _ | _] <- String.split(host, "."),
         true <- Fathom.ShardId.valid?(sub) do
      sub
    else
      _ -> nil
    end
  end

  defp shard_from_params(conn) do
    conn = Plug.Conn.fetch_query_params(conn)
    conn.query_params["db"] || conn |> Plug.Conn.get_req_header("x-fathom-shard") |> List.first()
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
