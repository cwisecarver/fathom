defmodule Fathom.QueryConsole do
  @moduledoc """
  A thin **front-door** SQL client used by the admin query console
  (`FathomWeb.AdminQueryLive`, expert review 2026-07-14 #23).

  The point of this module is that it queries **Fathom itself, through the real
  client path**, not the shard files via an internal side-door. `run/3` issues a
  Hrana-over-HTTP pipeline request to Fathom's own Hrana listener over loopback,
  addressing the target tenant exactly as a libSQL client does — by the `Host`
  subdomain (`<shard>.<base-domain>`), routed through
  `Fathom.ShardExecutor.shard_from_conn/1`. So a query from the console traverses
  every layer a Django client hits (Host routing, admission, `Fathom.HranaAuth`,
  the `ShardExecutor`, the per-stream connection lifecycle), and what it returns
  is exactly what a client would see — including the real Hrana error `code`,
  message, and round-trip latency. That makes the console double as a live
  smoke-test of the platform path per node.

  ## Security posture

  This is an operator tool behind the admin BasicAuth surface. It runs **arbitrary
  SQL, including writes and DDL, against any tenant** — the blast radius is every
  tenant reachable from the node, gated only by the admin credential. Keep
  `ADMIN_USER`/`ADMIN_PASS` strong and the dashboard off the public internet (the
  `admin_auth` plug already fails closed when unconfigured).

  ## Addressing

  The tenant is selected by the `Host` header the same way the LB routes a real
  client: `<shard>.<base-domain>`. With `:shard_base_domain` set (prod), the base
  must match the serving zone; unset (dev/test), any suffix routes by the first
  label (`shard_from_conn/1`'s unanchored dev behavior), so we use `<shard>.local`.
  When `:hrana_auth` is not `:disabled`, a per-shard bearer token is minted via
  `Fathom.HranaAuth.token_for/1` and sent as `Authorization: Bearer`, exactly as a
  client presents libSQL's `authToken`.
  """

  alias Fathom.HranaAuth

  @max_rows 1_000

  # How long a console-minted tenant token may live. Seconds, because the console mints one per
  # execution and uses it immediately over loopback (expert review 2026-08-20 #32).
  @token_ttl_s 60
  @receive_timeout 30_000

  @type result :: %{
          cols: [String.t()],
          rows: [[term()]],
          row_count: non_neg_integer(),
          truncated: boolean(),
          affected_row_count: non_neg_integer(),
          last_insert_rowid: String.t() | nil,
          latency_ms: float()
        }

  @type error :: %{code: String.t() | nil, message: String.t(), latency_ms: float()}

  @doc """
  Runs `sql` against tenant `shard_id` through the front door and returns
  `{:ok, result}` or `{:error, error}`.

  Options:

    * `:endpoint` — base URL of the Hrana listener. Defaults to
      `:query_console_endpoint` config, else `http://127.0.0.1:<hrana_port>`.
    * `:host` — override the `Host` header (defaults to `<shard>.<base-domain>`).
    * `:actor` — who is running this, for the issuance ledger (e.g. `"console:alice"`);
    * `:max_rows` — cap the rows returned to the caller (default #{@max_rows});
      the full result still transfers, but only this many are kept (with
      `:truncated` set) so a huge scan can't balloon the LiveView.
  """
  @spec run(String.t(), String.t(), keyword()) :: {:ok, result()} | {:error, error()}
  def run(shard_id, sql, opts \\ []) when is_binary(shard_id) and is_binary(sql) do
    # Validate HERE, not only in the LiveView (expert review 2026-08-01 #47). Expert review
    # 2026-07-18 #16 fixed "a dotted id routes to the wrong tenant" by adding `ShardId.valid?`
    # at the call site — but `run/3` is a public, documented API that splices `shard_id` into a
    # `Host` header and into `HranaAuth.token_for/1`, so the guard sat one layer above the
    # function that needs it and any second caller reopens the bug. AGENTS.md: route every
    # shard resolution through one place.
    case Fathom.ShardId.cast(shard_id) do
      {:ok, id} -> do_run(id, sql, opts)
      :error -> {:error, %{code: "INPUT", message: "invalid shard id", latency_ms: 0.0}}
    end
  end

  defp do_run(shard_id, sql, opts) do
    endpoint = Keyword.get(opts, :endpoint) || default_endpoint()
    host = Keyword.get(opts, :host) || host_for(shard_id)
    max_rows = Keyword.get(opts, :max_rows, @max_rows)

    body = %{
      "baton" => nil,
      "requests" => [
        %{"type" => "execute", "stmt" => %{"sql" => sql, "want_rows" => true}},
        %{"type" => "close"}
      ]
    }

    headers = [{"host", host}] ++ auth_header(shard_id, sql, opts)
    started = System.monotonic_time()

    result =
      Req.post(endpoint <> "/v2/pipeline",
        json: body,
        headers: headers,
        receive_timeout: @receive_timeout,
        retry: false,
        decode_json: [keys: :strings]
      )

    latency = elapsed_ms(started)

    case result do
      {:ok, %Req.Response{status: 200, body: %{"results" => results}}} ->
        parse_results(results, max_rows, latency)

      {:ok, %Req.Response{status: status, body: body}} ->
        {:error, error_from_body(body, status, latency)}

      {:error, reason} ->
        {:error, %{code: "TRANSPORT", message: inspect_reason(reason), latency_ms: latency}}
    end
  end

  # --- response parsing ----------------------------------------------------

  defp parse_results(results, max_rows, latency) do
    case List.first(results) do
      %{"type" => "ok", "response" => %{"result" => r}} ->
        {:ok, ok_result(r, max_rows, latency)}

      %{"type" => "error", "error" => e} ->
        {:error, err_from_hrana(e, latency)}

      _ ->
        {:error,
         %{code: "FILO_UNEXPECTED", message: "unexpected pipeline result", latency_ms: latency}}
    end
  end

  defp ok_result(r, max_rows, latency) do
    all_rows = Map.get(r, "rows", [])
    kept = Enum.take(all_rows, max_rows)

    %{
      cols: r |> Map.get("cols", []) |> Enum.map(&col_name/1),
      rows: Enum.map(kept, &decode_row/1),
      row_count: length(all_rows),
      truncated: length(all_rows) > max_rows,
      affected_row_count: Map.get(r, "affected_row_count", 0),
      last_insert_rowid: Map.get(r, "last_insert_rowid"),
      latency_ms: latency
    }
  end

  defp col_name(%{"name" => name}), do: name || ""
  defp col_name(name) when is_binary(name), do: name
  defp col_name(_), do: ""

  defp decode_row(row) when is_list(row), do: Enum.map(row, &decode_value/1)

  # Hrana value maps → native Elixir (integers travel as strings, blobs as base64).
  defp decode_value(%{"type" => "null"}), do: nil

  defp decode_value(%{"type" => "integer", "value" => v}) when is_binary(v),
    do: String.to_integer(v)

  defp decode_value(%{"type" => "integer", "value" => v}), do: v
  defp decode_value(%{"type" => "float", "value" => v}), do: v
  defp decode_value(%{"type" => "text", "value" => v}), do: v
  defp decode_value(%{"type" => "blob", "base64" => v}), do: {:blob, v}
  defp decode_value(other), do: other

  defp err_from_hrana(e, latency) when is_map(e) do
    %{
      code: Map.get(e, "code"),
      message: Map.get(e, "message", "query error"),
      latency_ms: latency
    }
  end

  # A non-200 pipeline (stream open refused: bad/missing shard 400, at-capacity 503,
  # auth 401) carries a top-level Hrana error map.
  defp error_from_body(%{"message" => msg} = body, status, latency) do
    %{code: Map.get(body, "code") || "HTTP_#{status}", message: msg, latency_ms: latency}
  end

  defp error_from_body(_body, status, latency) do
    %{code: "HTTP_#{status}", message: "request failed (HTTP #{status})", latency_ms: latency}
  end

  defp inspect_reason(%{__exception__: true} = e), do: Exception.message(e)
  defp inspect_reason(reason), do: inspect(reason)

  # --- addressing ----------------------------------------------------------

  defp default_endpoint do
    case Application.get_env(:fathom, :query_console_endpoint) do
      url when is_binary(url) -> url
      _ -> "http://127.0.0.1:#{Application.get_env(:fathom, :hrana_port, 8080)}"
    end
  end

  defp host_for(shard_id) do
    base =
      case Application.get_env(:fathom, :shard_base_domain) do
        zone when is_binary(zone) and zone != "" -> zone
        _ -> "local"
      end

    "#{shard_id}.#{base}"
  end

  # SCOPED, ATTRIBUTED AND SHORT-LIVED (expert review 2026-08-20 #32).
  #
  # This used to be a bare `HranaAuth.token_for(shard_id)`: `scope` defaulted to `:rw` even for a
  # `SELECT`, `actor` defaulted to `nil`, and the token was valid for the whole
  # `:hrana_token_max_age`. Two consequences, and the second is the one that bites.
  #
  # CREDENTIAL SPRAWL. An operator triaging ten tenants left ten live full-access tenant
  # credentials, each valid for the configured max-age, with nothing tying them to a person.
  # `AdminTenantController.export` was given per-operator attribution precisely because it touches
  # tenant data; the console mints credentials TO tenant data and had none.
  #
  # IT CORRUPTED THE INPUT TO THE FLEET-WIDE REVOKE. `revoke_issued_before/2` bumps the floor on
  # every shard the ledger shows with an outstanding token issued before the cutoff. After a week
  # of console use that set is every tenant an operator ever looked at — so an incident-response
  # sweep scoped to one leaked laptop would disconnect a large, arbitrary slice of the fleet. The
  # ledger's careful "under-reports, which is the safe direction" reasoning is defeated by a writer
  # that OVER-reports. The `console:` actor prefix is deliberately distinct from the controller's
  # `admin:` so a future `shards_issued_before/1` can exclude console mints outright.
  defp auth_header(shard_id, sql, opts) do
    case Application.get_env(:fathom, :hrana_auth, :disabled) do
      :disabled ->
        []

      _ ->
        mint =
          HranaAuth.token_for(shard_id,
            scope: scope_for(sql),
            actor: Keyword.get(opts, :actor) || "console",
            ttl: @token_ttl_s
          )

        case mint do
          {:ok, token} -> [{"authorization", "Bearer " <> token}]
          _ -> []
        end
    end
  end

  # `:ro` ONLY WHEN WE ARE SURE, and the asymmetry is the whole design. Misreading a write as a
  # read costs a 403 `FILO_READONLY` — visible, immediate, harmless. Misreading a read as a write
  # mints a full-access credential, which is the status quo being fixed. So this is a keyword
  # check, not a parser, and everything it does not recognise stays `:rw`.
  #
  # `WITH` is deliberately NOT matched, for the reason `Migrator.Capture` records: `WITH … INSERT`
  # is valid SQLite. Same reasoning, same answer.
  defp scope_for(sql) do
    first =
      sql
      |> String.trim_leading()
      |> String.split(~r/\s/, parts: 2)
      |> List.first()
      |> to_string()
      |> String.upcase()

    if first in ["SELECT", "EXPLAIN"], do: :ro, else: :rw
  end

  defp elapsed_ms(started) do
    System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond) / 1000
  end
end
