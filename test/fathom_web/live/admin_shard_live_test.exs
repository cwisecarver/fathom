defmodule FathomWeb.AdminShardLiveTest do
  @moduledoc """
  The per-shard admin page (`/admin/shards/:id`, expert review 2026-10-15 #4): auth gate, id
  validation, the directory row and token ledger, empty states, and the SHARD-ISOLATION GATE — the
  page for shard A must never show shard B's data. Non-async: the LiveView shares the test's
  shared-mode sandbox connection.
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Directory.Shard
  alias Fathom.HranaAuth.Ledger
  alias Fathom.Rebalancer.{Command, Override}
  alias Fathom.Repo

  @auth "Basic " <> Base.encode64("admin:secret")
  defp auth(conn), do: Plug.Conn.put_req_header(conn, "authorization", @auth)

  defp put_shard(attrs) do
    %Shard{}
    |> Shard.changeset(
      Enum.into(attrs, %{schema_version: 3, status: "active", last_active_at: DateTime.utc_now()})
    )
    |> Repo.insert!()
  end

  test "requires credentials", %{conn: conn} do
    assert get(conn, "/admin/shards/shard-d1").status == 401
  end

  test "renders the directory row, empty pin / commands / samples / ledger", %{conn: conn} do
    put_shard(%{shard_id: "shard-d1", schema_version: 7, status: "active"})

    {:ok, view, _} = conn |> auth() |> live("/admin/shards/shard-d1")
    render_async(view)

    assert has_element?(view, "#shard-title", "shard-d1")
    assert has_element?(view, "#shard-directory")
    assert has_element?(view, "#shard-pin", "Not pinned")
    assert has_element?(view, "#shard-no-commands")
    assert has_element?(view, "#shard-no-samples")
    assert has_element?(view, "#shard-no-issuances")
    assert has_element?(view, "#shard-local", "no")
  end

  test "an unknown (but valid) shard shows the no-directory-entry state", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/shards/never-seen")
    render_async(view)
    assert has_element?(view, "#shard-no-directory")
  end

  test "an invalid id flashes and redirects to the shards list", %{conn: conn} do
    assert {:error, {:live_redirect, %{to: "/admin/shards", flash: flash}}} =
             conn |> auth() |> live("/admin/shards/bad.id")

    assert flash["error"] =~ "valid shard id"
  end

  test "the id is canonicalised (upper case resolves to the lower-case shard)", %{conn: conn} do
    put_shard(%{shard_id: "shard-case"})
    {:ok, view, _} = conn |> auth() |> live("/admin/shards/SHARD-CASE")
    render_async(view)
    assert has_element?(view, "#shard-title", "shard-case")
    assert has_element?(view, "#shard-directory")
  end

  # SHARD-ISOLATION GATE: every section is keyed on the canonical id.
  test "the page for shard A never shows shard B's data", %{conn: conn} do
    put_shard(%{shard_id: "iso-a", schema_version: 11})
    put_shard(%{shard_id: "iso-b", schema_version: 22, last_verify_status: "corrupt"})

    :ok = Ledger.record(shard_id: "iso-a", token_version: 1, actor: "actor-of-a")
    :ok = Ledger.record(shard_id: "iso-b", token_version: 1, actor: "actor-of-b")

    Repo.insert!(%Override{shard_id: "iso-b", pinned_node: "node-of-b"})

    Repo.insert!(%Command{
      shard_id: "iso-b",
      node: "cmd-node-of-b",
      command: "drain",
      status: "pending"
    })

    {:ok, view, _} = conn |> auth() |> live("/admin/shards/iso-a")
    html = render_async(view)

    assert has_element?(view, "#shard-issuances", "actor-of-a")
    assert has_element?(view, "#shard-pin", "Not pinned")
    assert has_element?(view, "#shard-no-commands")

    refute html =~ "actor-of-b"
    refute html =~ "node-of-b"
    refute html =~ "cmd-node-of-b"
    refute html =~ "corrupt"
    refute html =~ "iso-b"
  end

  test "shows pin and pending commands for the shard", %{conn: conn} do
    put_shard(%{shard_id: "pinned-1"})
    Repo.insert!(%Override{shard_id: "pinned-1", pinned_node: "node-x"})

    Repo.insert!(%Command{
      shard_id: "pinned-1",
      node: "node-y",
      command: "drain",
      status: "pending"
    })

    {:ok, view, _} = conn |> auth() |> live("/admin/shards/pinned-1")
    render_async(view)

    assert has_element?(view, "#shard-pin", "node-x")
    assert has_element?(view, "#shard-commands", "node-y")
  end

  test "probe_mode on a shard with no coordinator reports not open", %{conn: conn} do
    {:ok, view, _} = conn |> auth() |> live("/admin/shards/cold-shard")
    render_async(view)
    # The button is disabled when the coordinator is not open, so drive the event directly.
    render_click(view, "probe_mode", %{})
    assert has_element?(view, "#shard-mode", "not open")
  end
end
