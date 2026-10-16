defmodule FathomWeb.AdminMigrationsJobsTest do
  @moduledoc """
  Expert review 2026-10-15 #12/#18/#19: the Oban, lifecycle and rollout-detail panels on the
  Migrations page. DOM-id assertions only.
  """
  use FathomWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Fathom.Directory
  alias Fathom.Repo

  @auth "Basic " <> Base.encode64("admin:secret")

  defp page(conn) do
    {:ok, view, _} =
      conn |> Plug.Conn.put_req_header("authorization", @auth) |> live("/admin/migrations")

    render_async(view, 5_000)
    view
  end

  defp insert_job(attrs) do
    now = DateTime.utc_now()

    defaults = %{
      args: %{},
      worker: "Fathom.Test.Worker",
      queue: "migrations",
      state: "available",
      inserted_at: now,
      scheduled_at: now
    }

    Repo.insert!(struct(Oban.Job, Map.merge(defaults, Map.new(attrs))))
  end

  test "empty system: panels render with their empty states", %{conn: conn} do
    view = page(conn)

    assert has_element?(view, "#lifecycle")
    assert has_element?(view, "#denylist-tombstones")
    assert has_element?(view, "#denylist-suspensions")
    assert has_element?(view, "#queue-health")
    assert has_element?(view, "#queue-migrations")
    assert has_element?(view, "#failed-jobs-empty")
    assert has_element?(view, "#rollout-rate")
    assert has_element?(view, "#version-histogram")
    refute has_element?(view, "#failed-jobs")
    refute has_element?(view, "#quarantined")
  end

  test "failed jobs table links the shard and shows the first error line", %{conn: conn} do
    job =
      insert_job(%{
        worker: "Fathom.Migrator.ShardMigrationJob",
        state: "discarded",
        args: %{"shard_id" => "acme"},
        attempt: 5,
        max_attempts: 5,
        errors: [%{"error" => "storage down\nsecond line"}]
      })

    view = page(conn)

    assert has_element?(view, "#failed-jobs")
    assert has_element?(view, "#failed-job-#{job.id} a[href='/admin/shards/acme']")
    assert has_element?(view, "#failed-job-#{job.id}", "storage down")
    refute has_element?(view, "#failed-job-#{job.id}", "second line")
  end

  test "quarantined shards show their last error and attempt", %{conn: conn} do
    {:ok, _} = Directory.resolve("bad-shard")
    {:ok, _} = Directory.mark_failed("bad-shard")

    insert_job(%{
      state: "discarded",
      args: %{"shard_id" => "bad-shard"},
      attempt: 4,
      errors: [%{"error" => "copy failed"}]
    })

    view = page(conn)

    assert has_element?(view, "#quarantined-bad-shard a[href='/admin/shards/bad-shard']")
    assert has_element?(view, "#quarantined-bad-shard", "attempt 4")
    assert has_element?(view, "#quarantined-bad-shard", "copy failed")
  end
end
