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

  alias Fathom.{ApiKeys, Directory, HranaAuth, Shards, Snapshots, Tenants}

  # Per-action scope enforcement (expert review #8): the `api_actor` set by the router's :api_auth
  # plug must hold at least the action's required scope. read < manage < destroy. `destroy` gates the
  # irreversible data/backup operations (erase a tenant, overwrite live via restore, delete a
  # snapshot); `manage` the create/modify/credential operations; `read` the inspective ones.
  plug :require_scope

  @action_scopes %{
    index: :read,
    show: :read,
    list_snapshots: :read,
    status: :read,
    create: :manage,
    suspend: :manage,
    resume: :manage,
    fork: :manage,
    flush: :manage,
    create_snapshot: :manage,
    mint_token: :manage,
    rotate_token: :manage,
    revoke_token: :manage,
    delete: :destroy,
    restore: :destroy,
    drop_snapshot: :destroy
  }

  defp require_scope(conn, _opts) do
    # Default to the most restrictive scope for any unmapped action (fail closed).
    required = Map.get(@action_scopes, action_name(conn), :destroy)
    actor = conn.assigns[:api_actor]

    if actor && ApiKeys.scope_at_least?(actor.scope, required) do
      conn
    else
      conn
      |> put_status(:forbidden)
      |> json(%{error: "insufficient scope: this action requires #{required}"})
      |> halt()
    end
  end

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

  # POST /api/tenants/:id/fork   {"dst": "acme-preview", "flush_source": true}
  # Clone a live tenant to a new id (#14). `flush_source: true` first force-flushes the source so
  # the fork carries its latest writes (keystone-fork of a just-migrated template, #10).
  def fork(conn, %{"id" => src} = params) do
    opts = if truthy?(params["flush_source"]), do: [flush_source: true], else: []

    case Tenants.fork(src, params["dst"] || params["to"] || "", opts) do
      {:ok, tenant} -> conn |> put_status(:created) |> json(tenant)
      {:error, :invalid_shard_id} -> error(conn, :bad_request, "invalid shard or destination id")
      {:error, :already_exists} -> error(conn, :conflict, "destination already exists")
      {:error, :tombstoned} -> error(conn, :conflict, "destination id was deleted")
      {:error, :no_source} -> error(conn, :not_found, "no such source tenant")
      {:error, reason} -> error(conn, :unprocessable_entity, "fork failed: #{inspect(reason)}")
    end
  end

  # POST /api/tenants/:id/flush   — force-flush the live coordinator so its state is durable (#10).
  # Local to the node holding the coordinator (fan out on a multi-node fleet); a no-op :ok if none.
  def flush(conn, %{"id" => id}) do
    case Tenants.flush(id) do
      :ok -> json(conn, %{shard_id: id, flushed: true})
      {:error, :invalid_shard_id} -> error(conn, :bad_request, "invalid shard id")
      {:error, reason} -> error(conn, :unprocessable_entity, "flush failed: #{inspect(reason)}")
    end
  end

  # POST /api/tenants/:id/snapshots   {"label": "before-import", "flush": true}
  # A point-in-time snapshot of the tenant's stored object (#12). It captures the last durably-flushed
  # state; flush: true first force-flushes the live coordinator so the snapshot is the CURRENT state.
  def create_snapshot(conn, %{"id" => id} = params) do
    # A failed force-flush must NOT be swallowed (expert review 2026-07-18 #15): flush: true is the
    # caller's request for a guaranteed-current snapshot, so a flush error (a transient store error
    # or a lease steal) has to surface — otherwise the snapshot is silently stale relative to the
    # contract. Short-circuit before creating the (stale) snapshot.
    with :ok <- maybe_flush(id, params),
         {:ok, snap} <- Snapshots.create(id, label: params["label"]) do
      conn |> put_status(:created) |> json(%{shard_id: id, snapshot_id: snap})
    else
      {:error, {:flush_failed, reason}} ->
        error(conn, :unprocessable_entity, "flush before snapshot failed: #{inspect(reason)}")

      {:error, :invalid_shard_id} ->
        error(conn, :bad_request, "invalid shard id")

      {:error, reason} ->
        error(conn, :unprocessable_entity, "snapshot failed: #{inspect(reason)}")
    end
  end

  defp maybe_flush(id, params) do
    if truthy?(params["flush"]) do
      case Shards.flush(id) do
        :ok -> :ok
        {:error, reason} -> {:error, {:flush_failed, reason}}
      end
    else
      :ok
    end
  end

  # GET /api/tenants/:id/snapshots   — list a tenant's snapshots (newest first), id + byte size.
  def list_snapshots(conn, %{"id" => id}) do
    case Snapshots.list(id) do
      {:ok, snaps} ->
        json(conn, %{shard_id: id, snapshots: snaps})

      {:error, :invalid_shard_id} ->
        error(conn, :bad_request, "invalid shard id")

      {:error, reason} ->
        error(conn, :unprocessable_entity, "list snapshots failed: #{inspect(reason)}")
    end
  end

  # POST /api/tenants/:id/restore   {"snapshot": "<snapshot_id>", "force": true}
  # Point-in-time restore (#12): drains the shard, then copies the snapshot back over the live object;
  # the next connect cold-opens the restored state. Destructive — snapshot first if you might undo.
  # A restore across a schema-migration boundary is refused unless `force` is true (#7).
  def restore(conn, %{"id" => id} = params) do
    case Snapshots.restore(id, params["snapshot"] || "", force: params["force"] == true) do
      :ok ->
        json(conn, %{shard_id: id, restored: params["snapshot"]})

      {:error, :invalid_shard_id} ->
        error(conn, :bad_request, "invalid shard id")

      {:error, :snapshot_not_found} ->
        error(conn, :not_found, "snapshot not found")

      {:error, {:schema_version_mismatch, %{snapshot: snap, directory: dir}}} ->
        error(
          conn,
          :conflict,
          "snapshot is at schema_version #{snap} but the tenant is at #{dir}; restoring crosses a " <>
            "migration boundary — pass \"force\": true to restore and re-migrate forward"
        )

      {:error, {:shard_busy, _}} ->
        error(conn, :conflict, "shard is busy (active connections) — quiesce it and retry")

      {:error, {:held, owner}} ->
        error(
          conn,
          :conflict,
          "shard is served on another node (#{owner}) — quiesce it there first"
        )

      {:error, reason} ->
        error(conn, :unprocessable_entity, "restore failed: #{inspect(reason)}")
    end
  end

  # DELETE /api/tenants/:id/snapshots/:snapshot_id   — drop a snapshot (idempotent).
  def drop_snapshot(conn, %{"id" => id, "snapshot_id" => snap}) do
    case Snapshots.drop(id, snap) do
      :ok ->
        json(conn, %{shard_id: id, dropped: snap})

      {:error, :invalid_shard_id} ->
        error(conn, :bad_request, "invalid shard id")

      {:error, reason} ->
        error(conn, :unprocessable_entity, "drop snapshot failed: #{inspect(reason)}")
    end
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

  # A JSON boolean, or a "true"/"1" string / 1 (form-ish clients), counts as true.
  defp truthy?(v), do: v in [true, "true", "1", 1]

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
