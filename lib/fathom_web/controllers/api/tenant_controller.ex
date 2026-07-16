defmodule FathomWeb.Api.TenantController do
  @moduledoc """
  Authenticated JSON control-plane for tenant provisioning (expert review 2026-07-14 #21).

  Today tenants exist only as a side effect of traffic (novel-shard admission mints on first
  request). That's a fine data-path fallback but not a product surface — a platform customer needs
  to create/list/delete tenants and get a connection URL + token. This is that surface, on the
  `:4000` endpoint (separate from the Hrana data port), behind the same BasicAuth as `/admin`.

  Thin wrappers over the built machinery: `Fathom.Tenants.provision/1` (directory insert +
  fork-from-template birth #10 + token mint), `Fathom.Directory.list_page/1`/`get/1` (#22), and
  `Fathom.Tenants.delete/1` (#15). Ids are validated by `ShardId.cast` in those functions, so a
  malformed id is a 400, never an unsafe key.
  """
  use FathomWeb, :controller

  alias Fathom.{Directory, HranaAuth, Tenants}

  # POST /api/tenants  {"shard_id": "acme"}
  def create(conn, params) do
    case Tenants.provision(params["shard_id"] || params["id"] || "") do
      {:ok, tenant} ->
        conn |> put_status(:created) |> json(tenant)

      {:error, :invalid_shard_id} ->
        error(conn, :bad_request, "invalid shard id")

      {:error, :already_exists} ->
        error(conn, :conflict, "tenant already exists")

      {:error, :tombstoned} ->
        error(conn, :conflict, "tenant id was deleted and cannot be reused")

      {:error, reason} ->
        error(conn, :internal_server_error, "provision failed: #{inspect(reason)}")
    end
  end

  # GET /api/tenants?status=&q=&limit=&offset=
  def index(conn, params) do
    page =
      Directory.list_page(
        status: params["status"],
        q: params["q"],
        limit: parse_int(params["limit"]),
        offset: parse_int(params["offset"]) || 0
      )

    json(conn, %{
      tenants: Enum.map(page.rows, &tenant_json/1),
      total: page.total,
      limit: page.limit,
      offset: page.offset
    })
  end

  # GET /api/tenants/:id
  def show(conn, %{"id" => id}) do
    case Directory.get(id) do
      {:ok, row} -> json(conn, tenant_json(row))
      :error -> error(conn, :not_found, "no such tenant")
    end
  end

  # DELETE /api/tenants/:id
  def delete(conn, %{"id" => id}) do
    case Tenants.delete(id) do
      {:ok, :scheduled} ->
        conn |> put_status(:accepted) |> json(%{shard_id: id, status: "deleting"})

      {:error, :invalid_shard_id} ->
        error(conn, :bad_request, "invalid shard id")

      {:error, reason} ->
        error(conn, :unprocessable_entity, "delete failed: #{inspect(reason)}")
    end
  end

  # POST /api/tenants/:id/suspend
  def suspend(conn, %{"id" => id}) do
    lifecycle(conn, Tenants.suspend(id), id, "suspended")
  end

  # POST /api/tenants/:id/resume
  def resume(conn, %{"id" => id}) do
    lifecycle(conn, Tenants.resume(id), id, "active")
  end

  # POST /api/tenants/:id/token   {"scope": "rw"|"ro"}   — mint a fresh token (#24)
  def mint_token(conn, %{"id" => id} = params) do
    scope = scope_param(params)

    case HranaAuth.token_for(id, scope: scope) do
      {:ok, token} -> json(conn, %{shard_id: id, auth_token: token, scope: to_string(scope)})
      {:error, :invalid_shard_id} -> error(conn, :bad_request, "invalid shard id")
    end
  end

  # POST /api/tenants/:id/token/rotate   {"scope": ...}   — zero-downtime graceful rotate (#24)
  def rotate_token(conn, %{"id" => id} = params) do
    scope = scope_param(params)

    case HranaAuth.rotate(id, scope: scope) do
      {:ok, token} -> json(conn, %{shard_id: id, auth_token: token, scope: to_string(scope)})
      {:error, :invalid_shard_id} -> error(conn, :bad_request, "invalid shard id")
    end
  end

  # DELETE /api/tenants/:id/token   — revoke every outstanding token immediately (#24)
  def revoke_token(conn, %{"id" => id}) do
    case HranaAuth.revoke(id) do
      {:ok, version} -> json(conn, %{shard_id: id, revoked_below_version: version})
      {:error, :invalid_shard_id} -> error(conn, :bad_request, "invalid shard id")
    end
  end

  # Explicit map — never String.to_atom on the request scope (atom-exhaustion hygiene).
  defp scope_param(%{"scope" => "ro"}), do: :ro
  defp scope_param(_), do: :rw

  defp lifecycle(conn, result, id, new_status) do
    case result do
      :ok -> json(conn, %{shard_id: id, status: new_status})
      {:error, :invalid_shard_id} -> error(conn, :bad_request, "invalid shard id")
      {:error, :not_found} -> error(conn, :not_found, "no such tenant")
      {:error, :deleted} -> error(conn, :conflict, "tenant is deleted")
      {:error, reason} -> error(conn, :unprocessable_entity, "failed: #{inspect(reason)}")
    end
  end

  defp tenant_json(row) do
    %{
      shard_id: row.shard_id,
      status: row.status,
      schema_version: row.schema_version,
      last_active_at: iso(row.last_active_at),
      retain_until: iso(row.retain_until)
    }
  end

  defp iso(nil), do: nil
  defp iso(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp error(conn, status, message), do: conn |> put_status(status) |> json(%{error: message})

  defp parse_int(nil), do: nil

  defp parse_int(s) when is_binary(s) do
    case Integer.parse(s) do
      {n, _} -> n
      :error -> nil
    end
  end

  defp parse_int(n) when is_integer(n), do: n
end
