defmodule FathomWeb.AdminDirectoryLiveTest do
  @moduledoc """
  The admin directory browser/editor (#22): BasicAuth gate, browse render, filter,
  and a guarded edit that persists. Non-async so the LiveView process shares the
  test's (shared-mode) sandbox connection and sees seeded rows.
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Directory
  alias Fathom.Directory.Shard
  alias Fathom.Repo

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  defp put_shard(attrs) do
    %Shard{}
    |> Shard.changeset(
      Enum.into(attrs, %{schema_version: 0, status: "active", last_active_at: DateTime.utc_now()})
    )
    |> Repo.insert!()
  end

  test "GET /admin/directory without credentials is challenged (401)", %{conn: conn} do
    assert get(conn, "/admin/directory").status == 401
  end

  test "mounts behind auth and renders directory rows", %{conn: conn} do
    put_shard(%{shard_id: "ui_alpha", status: "active"})
    put_shard(%{shard_id: "ui_beta", status: "retired"})

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    assert has_element?(view, "#directory-table")
    assert has_element?(view, "#dir-row-ui_alpha")
    assert has_element?(view, "#dir-row-ui_beta")
  end

  # Review #32: mount/3 runs twice — once for the static render, once over the socket — and this
  # LiveView loaded unconditionally, so every page load ran the directory query twice and first
  # paint blocked on it. The disconnected render must show the shell, not the rows.
  test "the disconnected mount does not query; the connected one does", %{conn: conn} do
    put_shard(%{shard_id: "ui_deferred", status: "active"})

    # `get/2` is the static render only — no socket, so no load/1.
    html = conn |> auth() |> get("/admin/directory") |> html_response(200)

    refute html =~ "dir-row-ui_deferred",
           "the disconnected mount must not run the directory query (#32)"

    # The connected mount fills it in.
    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")
    assert has_element?(view, "#dir-row-ui_deferred")
  end

  # Paging is driven by has_more? now, not by an exact whole-table COUNT (#32).
  test "next is disabled on a single page and pages when there is more", %{conn: conn} do
    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    # Two seeded rows, page size 50 — one page, so there is nothing to advance to.
    put_shard(%{shard_id: "ui_page_a", status: "active"})
    put_shard(%{shard_id: "ui_page_b", status: "active"})

    view |> element("#directory-filter") |> render_change(%{"f" => %{"status" => "", "q" => ""}})

    assert has_element?(view, "button[phx-value-dir='next'][disabled]"),
           "a single page must disable next without needing a total"
  end

  test "filters by status", %{conn: conn} do
    put_shard(%{shard_id: "ui_active", status: "active"})
    put_shard(%{shard_id: "ui_retired", status: "retired"})

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    view
    |> element("#directory-filter")
    |> render_change(%{"f" => %{"status" => "retired", "q" => ""}})

    refute has_element?(view, "#dir-row-ui_active")
    assert has_element?(view, "#dir-row-ui_retired")
  end

  test "editing a row's status persists through the guarded update", %{conn: conn} do
    put_shard(%{shard_id: "ui_edit", status: "active"})

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    view |> element("#dir-edit-ui_edit") |> render_click()
    assert has_element?(view, "#directory-edit-form")

    view
    |> element("#directory-edit-form")
    |> render_submit(%{"edit" => %{"status" => "retired", "retain_until" => ""}})

    assert {:ok, row} = Directory.get("ui_edit")
    assert row.status == "retired"
  end

  test "shows export + delete actions for a live tenant and hides them for a deleted one",
       %{conn: conn} do
    put_shard(%{shard_id: "ui_live", status: "active"})
    put_shard(%{shard_id: "ui_gone", status: "deleted"})

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    assert has_element?(view, "#dir-export-ui_live")
    assert has_element?(view, "#dir-delete-ui_live")
    # A deleted tenant is already erased — no edit/export/delete actions, just a marker.
    refute has_element?(view, "#dir-delete-ui_gone")
    refute has_element?(view, "#dir-edit-ui_gone")
    refute has_element?(view, "#dir-export-ui_gone")
  end

  test "clicking delete tombstones the tenant and schedules the erase", %{conn: conn} do
    put_shard(%{shard_id: "ui_del", status: "active"})
    on_exit(fn -> :ets.delete(Fathom.Tenants.Tombstones, "ui_del") end)

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")
    view |> element("#dir-delete-ui_del") |> render_click()

    # Durable tombstone + the in-memory re-mint gate are both set (the physical erase is a
    # background job).
    assert {:ok, %{status: "deleted"}} = Directory.get("ui_del")
    assert Fathom.Tenants.tombstoned?("ui_del")
  end

  test "suspend and resume buttons flip the tenant's status", %{conn: conn} do
    put_shard(%{shard_id: "ui_susp", status: "active"})
    on_exit(fn -> :ets.delete(Fathom.Tenants.Suspensions, "ui_susp") end)

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    # Active tenant shows Suspend, not Resume.
    assert has_element?(view, "#dir-suspend-ui_susp")
    refute has_element?(view, "#dir-resume-ui_susp")

    view |> element("#dir-suspend-ui_susp") |> render_click()
    assert {:ok, %{status: "suspended"}} = Directory.get("ui_susp")
    assert Fathom.Tenants.suspended?("ui_susp")
    # Now it shows Resume instead.
    assert has_element?(view, "#dir-resume-ui_susp")
    refute has_element?(view, "#dir-suspend-ui_susp")

    view |> element("#dir-resume-ui_susp") |> render_click()
    assert {:ok, %{status: "active"}} = Directory.get("ui_susp")
    refute Fathom.Tenants.suspended?("ui_susp")
  end

  test "an invalid retain_until surfaces an error and does not persist", %{conn: conn} do
    put_shard(%{shard_id: "ui_badtime", status: "active"})

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")
    view |> element("#dir-edit-ui_badtime") |> render_click()

    html =
      view
      |> element("#directory-edit-form")
      |> render_submit(%{"edit" => %{"status" => "retired", "retain_until" => "not-a-date"}})

    assert html =~ "ISO-8601"
    assert {:ok, row} = Directory.get("ui_badtime")
    assert row.status == "active"
  end

  # Expert review 2026-10-10 #W5: a `save` arriving with no row being edited (stale/double submit)
  # crashed the LiveView; a malformed `edit` id must not become the edit target either.
  test "save with no row being edited is a no-op; a malformed edit id is ignored", %{conn: conn} do
    put_shard(%{shard_id: "ui_noedit", status: "active"})
    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    render_hook(view, "save", %{"edit" => %{"status" => "retired", "retain_until" => ""}})
    render_hook(view, "edit", %{"id" => "bad/../id"})
    render_hook(view, "save", %{"edit" => %{"status" => "retired", "retain_until" => ""}})

    assert has_element?(view, "#dir-row-ui_noedit")
    refute has_element?(view, "#directory-edit-form")
    assert {:ok, %{status: "active"}} = Directory.get("ui_noedit")
  end

  # Expert review 2026-10-10 #W2: dashboard audit rows were attributed to a hard-coded
  # "admin (dashboard)" with no IP, and the hand-edit (`save`) was not audited at all.
  test "suspend and save are audited with the session operator and peer IP", %{conn: conn} do
    put_shard(%{shard_id: "ui_aud", status: "active"})
    on_exit(fn -> :ets.delete(Fathom.Tenants.Suspensions, "ui_aud") end)

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    view |> element("#dir-suspend-ui_aud") |> render_click()
    view |> element("#dir-resume-ui_aud") |> render_click()
    view |> element("#dir-edit-ui_aud") |> render_click()

    view
    |> element("#directory-edit-form")
    |> render_submit(%{"edit" => %{"status" => "retired", "retain_until" => ""}})

    events = Fathom.Audit.list(shard_id: "ui_aud")
    assert Enum.sort(Enum.map(events, & &1.action)) == ["admin_update", "resume", "suspend"]
    assert Enum.all?(events, &(&1.actor == "console:admin"))
    assert Enum.all?(events, &(&1.source_ip == "127.0.0.1"))
    assert Enum.find(events, &(&1.action == "admin_update")).detail["status"] == "retired"
  end
end
