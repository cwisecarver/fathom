defmodule FathomWeb.Api.MigrationControllerTest do
  @moduledoc """
  The fleet convergence API (expert review #25): `GET /api/migrations/status` — the deploy gate a
  Django CI/CD reads (was Elixir-only), behind the same admin BasicAuth as the rest of `/api`.
  """
  use FathomWeb.ConnCase, async: false

  alias Fathom.Migrator
  alias Fathom.Directory.Shard
  alias Fathom.Repo

  @auth "Basic " <> Base.encode64("admin:secret")

  defp auth(conn) do
    conn
    |> Plug.Conn.put_req_header("authorization", @auth)
    |> Plug.Conn.put_req_header("accept", "application/json")
  end

  defp put_shard(id, version, status \\ "active") do
    %Shard{}
    |> Shard.changeset(%{
      shard_id: id,
      schema_version: version,
      status: status,
      last_active_at: DateTime.utc_now()
    })
    |> Repo.insert!()
  end

  test "requires admin auth (401 without credentials)", %{conn: conn} do
    assert get(conn, "/api/migrations/status").status == 401
  end

  test "reports converged when no active shard is behind HEAD", %{conn: conn} do
    {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
    put_shard("s_at_head", 1)

    body = conn |> auth() |> get("/api/migrations/status") |> json_response(200)
    assert body["head"] == 1
    assert body["laggards"] == 0
    assert body["converged"] == true
    assert body["failed"] == 0
    assert body["pending_review"] == []
  end

  test "reports laggards, quarantines, and the review queue when not converged", %{conn: conn} do
    {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
    # A flagged (requires_review) v2 holds HEAD at 1 (#1) and shows up in pending_review.
    {:ok, _} = Migrator.release(2, "v2-data", ["UPDATE app SET x = 1"], nil, true)
    put_shard("behind", 0)
    put_shard("quarantined", 0, "migration_failed")

    body = conn |> auth() |> get("/api/migrations/status") |> json_response(200)
    assert body["head"] == 1
    assert body["laggards"] == 1
    assert body["converged"] == false
    assert body["failed"] == 1
    assert body["pending_review"] == [2]
  end
end
