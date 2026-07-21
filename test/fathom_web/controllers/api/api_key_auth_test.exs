defmodule FathomWeb.Api.ApiKeyAuthTest do
  @moduledoc """
  Scoped API-key auth on the /api control plane (expert review #8): a Bearer API key authenticates
  and its scope (`read < manage < destroy`) is enforced per action; an invalid/revoked token is 401;
  and the legacy shared admin BasicAuth still works (mapped to a full-`destroy` actor) for backward
  compatibility.
  """
  use FathomWeb.ConnCase, async: false

  alias Fathom.ApiKeys
  alias Fathom.Directory.Shard
  alias Fathom.Repo
  alias Fathom.Tenants.Tombstones

  defp bearer(conn, token) do
    conn
    |> Plug.Conn.put_req_header("authorization", "Bearer #{token}")
    |> Plug.Conn.put_req_header("accept", "application/json")
  end

  defp mint(scope),
    do: elem(ApiKeys.mint("t-#{scope}-#{System.unique_integer([:positive])}", scope), 1)

  defp put_shard(id) do
    %Shard{}
    |> Shard.changeset(%{
      shard_id: id,
      schema_version: 0,
      status: "active",
      last_active_at: DateTime.utc_now()
    })
    |> Repo.insert!()
  end

  test "an invalid bearer token is refused (401)", %{conn: conn} do
    assert conn |> bearer("fathom_bogus") |> get("/api/tenants") |> Map.get(:status) == 401
  end

  test "a revoked key stops authenticating (401)", %{conn: conn} do
    {:ok, token, key} = ApiKeys.mint("rev-#{System.unique_integer([:positive])}", "read")
    assert conn |> bearer(token) |> get("/api/tenants") |> Map.get(:status) == 200
    {:ok, _} = ApiKeys.revoke(key.id)
    assert conn |> bearer(token) |> get("/api/tenants") |> Map.get(:status) == 401
  end

  test "a read-scoped key can list but a mutating action is 403", %{conn: conn} do
    token = mint("read")
    assert conn |> bearer(token) |> get("/api/tenants") |> Map.get(:status) == 200
    assert conn |> bearer(token) |> delete("/api/tenants/whatever") |> Map.get(:status) == 403

    assert conn |> bearer(token) |> post("/api/tenants", %{"shard_id" => "x"}) |> Map.get(:status) ==
             403
  end

  test "a manage-scoped key can create but not delete (destroy) — 403", %{conn: conn} do
    token = mint("manage")
    id = "apikey_mng_#{System.unique_integer([:positive])}"
    on_exit(fn -> :ets.delete(Tombstones, id) end)

    create_status =
      conn |> bearer(token) |> post("/api/tenants", %{"shard_id" => id}) |> Map.get(:status)

    assert create_status in [200, 201], "a manage key may create"

    assert conn |> bearer(token) |> delete("/api/tenants/#{id}") |> Map.get(:status) == 403
  end

  test "a destroy-scoped key may delete (scope allows it — not 401/403)", %{conn: conn} do
    token = mint("destroy")
    id = "apikey_del_#{System.unique_integer([:positive])}"
    put_shard(id)
    on_exit(fn -> :ets.delete(Tombstones, id) end)

    status = conn |> bearer(token) |> delete("/api/tenants/#{id}") |> Map.get(:status)
    refute status in [401, 403], "a destroy key must pass the scope gate for delete"
  end

  test "the legacy admin BasicAuth still works as a full-access fallback", %{conn: conn} do
    basic = "Basic " <> Base.encode64("admin:secret")

    status =
      conn
      |> Plug.Conn.put_req_header("authorization", basic)
      |> Plug.Conn.put_req_header("accept", "application/json")
      |> get("/api/tenants")
      |> Map.get(:status)

    assert status == 200, "the shared admin credential remains a backward-compatible fallback"
  end
end
