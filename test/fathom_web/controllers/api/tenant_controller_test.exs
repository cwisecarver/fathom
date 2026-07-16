defmodule FathomWeb.Api.TenantControllerTest do
  @moduledoc """
  The tenant provisioning control-plane API (expert review 2026-07-14 #21): authenticated JSON
  create/list/get/delete on `:4000`, behind the same BasicAuth as `/admin`.
  """
  use FathomWeb.ConnCase, async: false

  alias Fathom.Directory
  alias Fathom.Directory.Shard
  alias Fathom.Repo
  alias Fathom.Tenants.Tombstones

  @auth "Basic " <> Base.encode64("admin:secret")

  defp auth(conn) do
    conn
    |> Plug.Conn.put_req_header("authorization", @auth)
    |> Plug.Conn.put_req_header("accept", "application/json")
  end

  defp put_shard(id, status \\ "active") do
    %Shard{}
    |> Shard.changeset(%{
      shard_id: id,
      schema_version: 0,
      status: status,
      last_active_at: DateTime.utc_now()
    })
    |> Repo.insert!()
  end

  test "every route requires admin auth (401 without credentials)", %{conn: conn} do
    assert post(conn, "/api/tenants", %{}).status == 401
    assert get(conn, "/api/tenants").status == 401
    assert get(conn, "/api/tenants/x").status == 401
    assert delete(conn, "/api/tenants/x").status == 401
  end

  describe "POST /api/tenants" do
    test "provisions a new tenant with a url + token", %{conn: conn} do
      id = "api_new_#{System.unique_integer([:positive])}"
      on_exit(fn -> :ets.delete(Tombstones, id) end)

      body = conn |> auth() |> post("/api/tenants", %{shard_id: id}) |> json_response(201)

      assert body["shard_id"] == id
      assert body["url"] == "libsql://#{id}.local"
      assert is_binary(body["auth_token"])
      assert {:ok, %{status: "active"}} = Directory.get(id)
    end

    test "409 when the tenant already exists", %{conn: conn} do
      put_shard("api_dupe")
      assert (conn |> auth() |> post("/api/tenants", %{shard_id: "api_dupe"})).status == 409
    end

    test "409 for a tombstoned id (can't re-provision a deleted tenant)", %{conn: conn} do
      {:ok, _} = Directory.tombstone("api_dead")
      assert (conn |> auth() |> post("/api/tenants", %{shard_id: "api_dead"})).status == 409
    end

    test "400 for an invalid id", %{conn: conn} do
      assert (conn |> auth() |> post("/api/tenants", %{shard_id: "Not Valid!"})).status == 400
    end
  end

  describe "GET /api/tenants" do
    test "lists matching tenants with a total", %{conn: conn} do
      put_shard("api_list_a")
      put_shard("api_list_b")

      body = conn |> auth() |> get("/api/tenants?q=api_list_") |> json_response(200)

      ids = Enum.map(body["tenants"], & &1["shard_id"])
      assert "api_list_a" in ids and "api_list_b" in ids
      assert body["total"] >= 2
    end
  end

  describe "GET /api/tenants/:id" do
    test "200 for a known tenant, 404 otherwise", %{conn: conn} do
      put_shard("api_show")

      body = conn |> auth() |> get("/api/tenants/api_show") |> json_response(200)
      assert body["shard_id"] == "api_show"
      assert body["status"] == "active"

      assert (conn |> auth() |> get("/api/tenants/api_missing")).status == 404
    end
  end

  describe "DELETE /api/tenants/:id" do
    test "202 accepted and tombstones the tenant", %{conn: conn} do
      put_shard("api_del")
      on_exit(fn -> :ets.delete(Tombstones, "api_del") end)

      body = conn |> auth() |> delete("/api/tenants/api_del") |> json_response(202)
      assert body["status"] == "deleting"
      assert {:ok, %{status: "deleted"}} = Directory.get("api_del")
    end
  end
end
