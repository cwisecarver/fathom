defmodule FathomWeb.AdminQueryLiveTest do
  @moduledoc """
  The admin query console (#23): BasicAuth gate, mount, input validation, and a
  real front-door query rendered end to end (LiveView → `Fathom.QueryConsole` →
  in-process Hrana listener → shard).
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Bench.HranaClient
  alias Fathom.Shards

  # config/test.exs sets admin_auth to admin/secret.
  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  setup do
    {:ok, sup, port} = HranaClient.start_listener()
    prev = Application.get_env(:fathom, :query_console_endpoint)
    Application.put_env(:fathom, :query_console_endpoint, "http://127.0.0.1:#{port}")
    id = "qc_live_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :query_console_endpoint, prev),
        else: Application.delete_env(:fathom, :query_console_endpoint)

      Shards.drain(id, 5_000)
      rm_shard(id)
      HranaClient.stop_listener(sup)
    end)

    {:ok, id: id}
  end

  test "GET /admin/query without credentials is challenged (401)", %{conn: conn} do
    assert get(conn, "/admin/query").status == 401
  end

  test "mounts behind admin auth and shows the query form", %{conn: conn} do
    {:ok, view, _html} = conn |> auth() |> live("/admin/query")
    assert has_element?(view, "#query-form")
    assert has_element?(view, "#query-run")
  end

  test "an empty shard renders an input error without querying", %{conn: conn} do
    {:ok, view, _html} = conn |> auth() |> live("/admin/query")
    html = view |> form("#query-form", q: %{shard: "", sql: "SELECT 1"}) |> render_submit()
    assert html =~ "enter a shard id"
  end

  test "runs a real front-door query and renders the result", %{conn: conn, id: id} do
    {:ok, _} = Fathom.QueryConsole.run(id, "CREATE TABLE t (v TEXT)")
    {:ok, _} = Fathom.QueryConsole.run(id, "INSERT INTO t VALUES ('hi')")

    {:ok, view, _html} = conn |> auth() |> live("/admin/query")

    view
    |> form("#query-form", q: %{shard: id, sql: "SELECT v FROM t"})
    |> render_submit()

    html = render_async(view, 5_000)
    assert html =~ "hi"
    assert has_element?(view, "#query-results")
  end

  defp rm_shard(id) do
    for dir <- ["fathom_shards", "fathom_remote_test"], s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([System.tmp_dir!(), dir, "#{id}.db"]) <> s)
    end
  end
end
