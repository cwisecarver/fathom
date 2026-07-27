defmodule FathomWeb.AdminTenantControllerTest do
  @moduledoc """
  Tenant data export download (expert review 2026-07-14 #15): the BasicAuth-gated
  `/admin/tenants/:id/export` route streams the shard's SQLite file. Non-async — it drives
  real shard files through the checkout path.
  """
  use FathomWeb.ConnCase, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Filo.Stmt

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  setup do
    id = "exp_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(id, 2_000)
      Storage.purge_shard(id)

      for p <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{id}*"])),
          do: File.rm(p)

      for p <- Path.wildcard(Path.join(System.tmp_dir!(), "fathom_export_#{id}_*")),
          do: File.rm(p)
    end)

    %{id: id}
  end

  test "without credentials the export route is challenged (401)", %{conn: conn, id: id} do
    assert get(conn, "/admin/tenants/#{id}/export").status == 401
  end

  test "downloads the tenant's data as a SQLite file", %{conn: conn, id: id} do
    seed!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('dl')"])

    conn = conn |> auth() |> get("/admin/tenants/#{id}/export")

    assert conn.status == 200
    assert Enum.any?(get_resp_header(conn, "content-disposition"), &(&1 =~ "#{id}.db"))
    # The SQLite magic header proves the body is a real database, not an error page.
    assert String.starts_with?(conn.resp_body, "SQLite format 3")
  end

  test "404 when the shard has no stored data (never flushed / deleted)", %{conn: conn, id: id} do
    assert (conn |> auth() |> get("/admin/tenants/#{id}/export")).status == 404
  end

  test "400 for an invalid shard id (rejects path-traversal ids too)", %{conn: conn} do
    assert (conn |> auth() |> get("/admin/tenants/Bad%20Id/export")).status == 400
  end

  defp seed!(id, sqls) do
    {:ok, handle} = ShardExecutor.open(id)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, %Stmt{sql: s}) end)
    :ok = ShardExecutor.close(handle)
    # Flush to the durable stored object export/1 pulls from.
    :ok = Shards.drain(id, 5_000)
  end
end
