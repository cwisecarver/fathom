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

  require Ecto.Query

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

  # Symptom (browser review 2026-10-16): raw ISO timestamps wrapped onto two lines on every row.
  test "last-flushed renders relative with the full ISO timestamp in a title", %{conn: conn} do
    flushed = DateTime.add(DateTime.utc_now(), -2 * 86_400) |> DateTime.truncate(:second)
    # The column is usec-precision, so the rendered title carries ".000000".
    flushed = %{flushed | microsecond: {0, 6}}
    put_shard(%{shard_id: "ui_rel"})

    Repo.update_all(Ecto.Query.from(s in Shard, where: s.shard_id == "ui_rel"),
      set: [last_flushed_at: flushed]
    )

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")

    cell = view |> element("#dir-flushed-ui_rel") |> render()
    assert cell =~ ~s(title="#{DateTime.to_iso8601(flushed)}")
    assert cell =~ "2d ago"
    assert cell =~ "whitespace-nowrap"
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

  # Expert review 2026-10-12 #30. The operator opens the editor on an active row, a delete lands
  # while the edit is open, and the operator submits "active". The guarded write refuses with
  # :status_conflict. The LiveView must answer with a flash, not crash on an unhandled case clause,
  # and the tombstone must stay a tombstone. Pre-fix the unguarded write resurrected the row.
  test "an edit that loses a race with a delete is refused with a flash, not a crash",
       %{conn: conn} do
    put_shard(%{shard_id: "ui_race", status: "active"})

    {:ok, view, _html} = conn |> auth() |> live("/admin/directory")
    view |> element("#dir-edit-ui_race") |> render_click()
    assert has_element?(view, "#directory-edit-form")

    {:ok, _} = Directory.tombstone("ui_race")

    view
    |> element("#directory-edit-form")
    |> render_submit(%{"edit" => %{"status" => "active", "retain_until" => ""}})

    assert render(view) =~ "changed state while you were editing"
    assert {:ok, %{status: "deleted"}} = Directory.get("ui_race")
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

  # Expert review 2026-10-15 #8: trouble presets + the Last-flushed / Last-snapshot / Verify columns.
  describe "trouble filters" do
    defp ago(secs), do: DateTime.add(DateTime.utc_now(), -secs, :second)

    defp patch_shard(shard_id, attrs) do
      Repo.update_all(
        Ecto.Query.from(s in Shard, where: s.shard_id == ^shard_id),
        set: Map.to_list(attrs)
      )
    end

    defp seed_trouble do
      put_shard(%{shard_id: "tr_healthy"})

      patch_shard("tr_healthy", %{
        last_flushed_at: ago(10),
        last_snapshot_at: ago(60),
        last_verify_status: "ok"
      })

      put_shard(%{shard_id: "tr_quarantined", status: "migration_failed"})
      put_shard(%{shard_id: "tr_badverify"})
      patch_shard("tr_badverify", %{last_verify_status: "corrupt", last_snapshot_at: ago(60)})
      put_shard(%{shard_id: "tr_nosnap"})
      patch_shard("tr_nosnap", %{last_snapshot_at: ago(3 * 86_400)})
      put_shard(%{shard_id: "tr_requeued"})
      patch_shard("tr_requeued", %{requeue_count: 2, last_snapshot_at: ago(60)})
      put_shard(%{shard_id: "tr_deleted", status: "deleted"})
    end

    defp apply_filter(view, trouble, extra \\ %{}) do
      view
      |> element("#directory-filter")
      |> render_change(%{
        "f" => Map.merge(%{"status" => "", "q" => "tr_", "trouble" => trouble}, extra)
      })
    end

    test "each preset keeps exactly its trouble rows", %{conn: conn} do
      seed_trouble()
      {:ok, view, _} = conn |> auth() |> live("/admin/directory")

      apply_filter(view, "quarantined")
      assert has_element?(view, "#dir-row-tr_quarantined")
      refute has_element?(view, "#dir-row-tr_healthy")

      apply_filter(view, "verify_bad")
      assert has_element?(view, "#dir-row-tr_badverify")
      refute has_element?(view, "#dir-row-tr_healthy")
      refute has_element?(view, "#dir-row-tr_requeued")

      # Never snapshotted counts as no snapshot in 24 h; one snapshotted a minute ago does not.
      apply_filter(view, "no_snapshot")
      assert has_element?(view, "#dir-row-tr_nosnap")
      refute has_element?(view, "#dir-row-tr_healthy")
      refute has_element?(view, "#dir-row-tr_deleted")

      apply_filter(view, "requeued")
      assert has_element?(view, "#dir-row-tr_requeued")
      refute has_element?(view, "#dir-row-tr_healthy")
    end

    test "stale_flush honours the minutes input and excludes recently flushed shards",
         %{conn: conn} do
      seed_trouble()
      patch_shard("tr_nosnap", %{last_flushed_at: ago(2 * 3600)})
      {:ok, view, _} = conn |> auth() |> live("/admin/directory")

      apply_filter(view, "stale_flush", %{"flushed_min" => "60"})
      assert has_element?(view, "#dir-row-tr_nosnap")
      refute has_element?(view, "#dir-row-tr_healthy")

      # With a 5-hour threshold the 2-hour-old flush is no longer stale.
      apply_filter(view, "stale_flush", %{"flushed_min" => "300"})
      refute has_element?(view, "#dir-row-tr_nosnap")
    end

    test "an unknown trouble value is ignored, not an atom or a crash", %{conn: conn} do
      seed_trouble()
      {:ok, view, _} = conn |> auth() |> live("/admin/directory")

      apply_filter(view, "definitely_not_a_preset")
      assert has_element?(view, "#dir-row-tr_healthy")
    end

    test "the new columns render and sort by last flushed works", %{conn: conn} do
      seed_trouble()
      {:ok, view, _} = conn |> auth() |> live("/admin/directory")

      assert has_element?(view, "#directory-table th", "Last flushed")
      assert has_element?(view, "#directory-table th", "Last snapshot")
      assert has_element?(view, "#directory-table th", "Verify")
      assert has_element?(view, "#dir-row-tr_badverify", "corrupt")

      apply_filter(view, "", %{"sort" => "last_flushed_at"})
      assert has_element?(view, "#dir-row-tr_healthy")
    end
  end

  describe "Directory.backup_summary/1" do
    test "folds one grouped query into totals, never-snapshotted, stale and verify counts" do
      put_shard(%{shard_id: "bk_ok"})

      Repo.update_all(
        Ecto.Query.from(s in Shard, where: s.shard_id == "bk_ok"),
        set: [last_snapshot_at: DateTime.utc_now(), last_verify_status: "ok"]
      )

      put_shard(%{shard_id: "bk_never"})
      put_shard(%{shard_id: "bk_gone", status: "deleted"})

      s = Directory.backup_summary(86_400)
      assert s.total >= 2
      assert s.never_snapshotted >= 1
      assert s.stale_snapshot >= s.never_snapshotted
      assert s.verify["ok"] >= 1
      assert %DateTime{} = s.last_snapshot_at
    end

    test "an empty directory is all zeros and a nil newest snapshot" do
      assert %{total: 0, never_snapshotted: 0, stale_snapshot: 0, last_snapshot_at: nil} =
               Directory.backup_summary(86_400)
    end
  end
end
