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
end
