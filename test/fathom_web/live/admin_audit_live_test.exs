defmodule FathomWeb.AdminAuditLiveTest do
  @moduledoc "The audit page (`/admin/audit`, expert review 2026-10-15 #20)."
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Audit

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  test "requires credentials", %{conn: conn} do
    assert get(conn, "/admin/audit").status == 401
  end

  test "empty state", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/audit")
    assert has_element?(view, "#audit-empty")
    assert has_element?(view, "#audit-tiles")
  end

  test "lists events newest first, filters by shard, links to the shard page", %{conn: conn} do
    Audit.record("op", "delete", "aud-a", "10.0.0.1", "ok")
    Audit.record("op", "export", "aud-b", "10.0.0.2", "ok")

    {:ok, view, _} = conn |> auth() |> live("/admin/audit")
    assert has_element?(view, "#audit-table", "aud-a")
    assert has_element?(view, "#audit-table", "aud-b")

    view |> form("#audit-filter", f: %{shard: "aud-a"}) |> render_submit()
    assert has_element?(view, "#audit-table", "aud-a")
    refute has_element?(view, "#audit-table", "aud-b")
    assert has_element?(view, ~s(#audit-table a[href="/admin/shards/aud-a"]))
  end

  test "an invalid shard filter shows an error and no rows", %{conn: conn} do
    Audit.record("op", "delete", "aud-a", "10.0.0.1", "ok")
    {:ok, view, _} = conn |> auth() |> live("/admin/audit?shard=bad.id")
    assert has_element?(view, "#audit-filter-error")
    refute has_element?(view, "#audit-table")
  end

  test "paginates past one page", %{conn: conn} do
    for i <- 1..51, do: Audit.record("op", "flush", "page-#{i}", "10.0.0.1", "ok")

    {:ok, view, _} = conn |> auth() |> live("/admin/audit")
    assert has_element?(view, "#audit-next")
    refute has_element?(view, "#audit-prev")

    view |> element("#audit-next") |> render_click()
    assert has_element?(view, "#audit-prev")
    refute has_element?(view, "#audit-next")
  end

  test "24h tile counts only mint/rotate/revoke inside the window" do
    Audit.record("op", "mint_token", "t1", nil, "ok")
    Audit.record("op", "mint_token", "t1", nil, "ok")
    Audit.record("op", "revoke_token", "t1", nil, "ok")
    Audit.record("op", "delete", "t1", nil, "ok")

    old = DateTime.add(DateTime.utc_now(), -2 * 86_400, :second)

    Fathom.Repo.insert!(%Audit.Event{
      actor: "op",
      action: "rotate_token",
      shard_id: "t1",
      outcome: "ok",
      inserted_at: old
    })

    assert %{"mint_token" => 2, "rotate_token" => 0, "revoke_token" => 1} =
             Audit.credential_action_counts()
  end
end
