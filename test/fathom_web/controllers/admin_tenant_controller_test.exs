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

      for p <-
            Path.wildcard(
              Path.join(Fathom.Shard.TempReaper.scratch_dir(), "fathom_export_#{id}_*")
            ),
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

    # The body is sent from the temp file now (expert review 2026-10-08 #2), and the temp must
    # still be gone afterwards — no exported copy lingers on disk.
    assert Path.wildcard(
             Path.join(Fathom.Shard.TempReaper.scratch_dir(), "fathom_export_#{id}_*")
           ) == []
  end

  test "404 when the shard has no stored data (never flushed / deleted)", %{conn: conn, id: id} do
    assert (conn |> auth() |> get("/admin/tenants/#{id}/export")).status == 404
  end

  test "400 for an invalid shard id (rejects path-traversal ids too)", %{conn: conn} do
    assert (conn |> auth() |> get("/admin/tenants/Bad%20Id/export")).status == 400
  end

  # Expert review 2026-10-12 #31 (SYMPTOM): export is a GET that changes state, and a cross-site page
  # could make the operator's browser fire it with the cached Basic-auth credentials. Invariant: the
  # export runs only when the browser says the request is same-origin or user-initiated (`none`), or
  # sends no Sec-Fetch-Site at all. A cross-site or same-site request is refused before any work.
  describe "Sec-Fetch-Site guard (#31)" do
    setup %{id: id} do
      seed!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('guard')"])
      :ok
    end

    test "same-origin export is allowed", %{conn: conn, id: id} do
      conn =
        conn
        |> auth()
        |> put_req_header("sec-fetch-site", "same-origin")
        |> get("/admin/tenants/#{id}/export")

      assert conn.status == 200
      assert String.starts_with?(conn.resp_body, "SQLite format 3")
    end

    test "user-initiated (none) export is allowed", %{conn: conn, id: id} do
      conn =
        conn
        |> auth()
        |> put_req_header("sec-fetch-site", "none")
        |> get("/admin/tenants/#{id}/export")

      assert conn.status == 200
    end

    test "a missing Sec-Fetch-Site header is allowed (non-browser client)", %{conn: conn, id: id} do
      assert (conn |> auth() |> get("/admin/tenants/#{id}/export")).status == 200
    end

    test "a cross-site export is refused with 403 and streams nothing", %{conn: conn, id: id} do
      conn =
        conn
        |> auth()
        |> put_req_header("sec-fetch-site", "cross-site")
        |> get("/admin/tenants/#{id}/export")

      assert conn.status == 403
      refute String.starts_with?(conn.resp_body, "SQLite format 3")
    end

    test "a same-site (sibling subdomain) export is refused with 403", %{conn: conn, id: id} do
      conn =
        conn
        |> auth()
        |> put_req_header("sec-fetch-site", "same-site")
        |> get("/admin/tenants/#{id}/export")

      assert conn.status == 403
    end
  end

  defp seed!(id, sqls) do
    {:ok, handle} = ShardExecutor.open(id)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, %Stmt{sql: s}) end)
    :ok = ShardExecutor.close(handle)
    # Flush to the durable stored object export/1 pulls from.
    :ok = Shards.drain(id, 5_000)
  end
end
