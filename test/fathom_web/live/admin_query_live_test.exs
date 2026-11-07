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

    # Two rows per run since 2026-11-07 #38: `started` (before the statement leaves) and the
    # outcome. This asserted exactly one row, which is the audit gap that finding closed.
    assert [ev, started] = Fathom.Audit.list(shard_id: id)
    assert started.outcome == "started"
    assert started.detail["sql_sha256"] == ev.detail["sql_sha256"]
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

    assert [ev, %{outcome: "started"}] = Fathom.Audit.list(shard_id: id)
    assert ev.outcome == "error"
  end

  # Expert review 2026-11-07 #38. The only audit write was in `handle_async/3`, after the statement
  # finished and only if the LiveView still held that task's ref. A second `run` (the button's
  # `disabled` is client-side only) `start_async`ed a SECOND task over the first's key: the first
  # statement kept running server-side, its result was dropped with NO audit row, and `pending`
  # named the second statement; a LiveView that died mid-flight left no row at all. The intent is
  # now recorded BEFORE the call, and the server refuses a second statement while one is in flight.
  #
  # The "tenant" here is a socket that accepts and never answers, so the first statement is
  # genuinely in flight for as long as the test wants.
  describe "intent is audited before the call; one statement in flight (#38)" do
    setup do
      {:ok, listen} =
        :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}, reuseaddr: true])

      {:ok, port} = :inet.port(listen)
      parent = self()

      acceptor =
        spawn_link(fn ->
          accept_forever(listen, parent)
        end)

      Application.put_env(:fathom, :query_console_endpoint, "http://127.0.0.1:#{port}")

      on_exit(fn ->
        Process.unlink(acceptor)
        Process.exit(acceptor, :kill)
        :gen_tcp.close(listen)
      end)

      :ok
    end

    defp accept_forever(listen, parent) do
      case :gen_tcp.accept(listen, 10_000) do
        {:ok, sock} ->
          send(parent, {:statement_left, sock})
          accept_forever(listen, parent)

        _ ->
          :ok
      end
    end

    test "a `started` row exists while the statement is still running", %{conn: conn, id: id} do
      {:ok, view, _html} = conn |> auth() |> live("/admin/query")
      sql = "DELETE FROM t WHERE 1 = 1"
      view |> form("#query-form", q: %{shard: id, sql: sql}) |> render_submit()

      assert_receive {:statement_left, _sock}, 5_000

      # No outcome yet — the request never returns — but the intent is on record.
      assert [row] = Fathom.Audit.list(shard_id: id)
      assert row.action == "console_query"
      assert row.outcome == "started"
      assert row.actor == "console:admin"
      assert row.detail["sql_sha256"] == Base.encode16(:crypto.hash(:sha256, sql), case: :lower)
    end

    test "a second run while one is in flight is refused: it never leaves, no row names it", %{
      conn: conn,
      id: id
    } do
      {:ok, view, _html} = conn |> auth() |> live("/admin/query")

      view
      |> form("#query-form", q: %{shard: id, sql: "UPDATE t SET v = 'first'"})
      |> render_submit()

      assert_receive {:statement_left, _}, 5_000

      html =
        view
        |> form("#query-form", q: %{shard: id, sql: "DROP TABLE t -- the second statement"})
        |> render_submit()

      assert html =~ "already running"
      refute_receive {:statement_left, _}, 500, "the superseding statement was sent"

      assert [row] = Fathom.Audit.list(shard_id: id)
      assert row.detail["sql_head"] == "UPDATE t SET v = 'first'"
    end

    test "a LiveView that dies mid-flight has already left its `started` row", %{
      conn: conn,
      id: id
    } do
      {:ok, view, _html} = conn |> auth() |> live("/admin/query")

      view
      |> form("#query-form", q: %{shard: id, sql: "INSERT INTO t VALUES ('x')"})
      |> render_submit()

      assert_receive {:statement_left, _}, 5_000

      # The test process is LINKED to the view (`live/2`), so killing it would take the test down
      # with it unless exits are trapped.
      Process.flag(:trap_exit, true)
      ref = Process.monitor(view.pid)
      Process.exit(view.pid, :kill)
      assert_receive {:DOWN, ^ref, :process, _, _}, 2_000

      assert [%{outcome: "started"}] = Fathom.Audit.list(shard_id: id)
    end
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
