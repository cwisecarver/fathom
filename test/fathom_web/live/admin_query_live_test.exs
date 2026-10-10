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

  # Expert review 2026-07-18 #16: the console only trimmed the shard field, then QueryConsole
  # interpolated it into the Host. A dotted id ("acme.other") is reinterpreted by Host-subdomain
  # routing as its FIRST label ("acme"), so an admin could run SQL against the WRONG tenant. The id
  # is now validated with ShardId.valid? before the query runs.
  test "an invalid (dotted) shard id renders an error without querying", %{conn: conn} do
    {:ok, view, _html} = conn |> auth() |> live("/admin/query")

    html =
      view |> form("#query-form", q: %{shard: "acme.other", sql: "SELECT 1"}) |> render_submit()

    assert html =~ "invalid shard id"
    # It short-circuited before the async query — no result pane appears.
    refute has_element?(view, "#query-results")
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

  # Expert review 2026-10-10 #W1: console runs (arbitrary SQL, writes/DDL on any tenant) left no
  # audit trail. Every run now records actor, shard, outcome, sql sha256 + first 200 chars.
  test "a run is audited with actor, sha256 and a truncated statement", %{conn: conn, id: id} do
    {:ok, _} = Fathom.QueryConsole.run(id, "CREATE TABLE t (v TEXT)")
    {:ok, view, _html} = conn |> auth() |> live("/admin/query")
    sql = "SELECT 1 AS x /* " <> String.duplicate("a", 300) <> " */"

    view |> form("#query-form", q: %{shard: id, sql: sql}) |> render_submit()
    render_async(view, 5_000)

    assert [ev] = Fathom.Audit.list(shard_id: id)
    assert ev.action == "console_query"
    assert ev.outcome == "ok"
    assert ev.actor == "console:admin"
    assert ev.detail["sql_sha256"] == Base.encode16(:crypto.hash(:sha256, sql), case: :lower)
    assert String.length(ev.detail["sql_head"]) == 200
  end

  test "a failing run is audited as an error", %{conn: conn, id: id} do
    {:ok, view, _html} = conn |> auth() |> live("/admin/query")
    view |> form("#query-form", q: %{shard: id, sql: "SELECT * FROM nope"}) |> render_submit()
    render_async(view, 5_000)

    assert [ev] = Fathom.Audit.list(shard_id: id)
    assert ev.outcome == "error"
  end

  test "the template shard is blocked in the console and nothing reaches it", %{conn: conn} do
    prev = Application.get_env(:fathom, :template_shard_id)
    Application.put_env(:fathom, :template_shard_id, "tmpl-w1")
    on_exit(fn -> restore_env(:template_shard_id, prev) end)

    assert {:error, %{code: "TEMPLATE_BLOCKED"}} =
             Fathom.QueryConsole.run("tmpl-w1", "CREATE TABLE x (a)")

    {:ok, view, _html} = conn |> auth() |> live("/admin/query")
    view |> form("#query-form", q: %{shard: "tmpl-w1", sql: "SELECT 1"}) |> render_submit()
    render_async(view, 5_000)
    assert has_element?(view, "#query-error")
    refute has_element?(view, "#query-results")
  end

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, v), do: Application.put_env(:fathom, key, v)

  defp rm_shard(id) do
    for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
        s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([dir, "#{id}.db"]) <> s)
    end
  end
end
