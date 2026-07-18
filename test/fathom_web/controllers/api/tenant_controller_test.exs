defmodule FathomWeb.Api.TenantControllerTest do
  @moduledoc """
  The tenant provisioning control-plane API (expert review 2026-07-14 #21): authenticated JSON
  create/list/get/delete on `:4000`, behind the same BasicAuth as `/admin`.
  """
  use FathomWeb.ConnCase, async: false

  alias Fathom.Directory
  alias Fathom.Directory.Shard
  alias Fathom.Repo
  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Fathom.Tenants.{Suspensions, Tombstones}
  alias Filo.Stmt

  @auth "Basic " <> Base.encode64("admin:secret")

  defp auth(conn) do
    conn
    |> Plug.Conn.put_req_header("authorization", @auth)
    |> Plug.Conn.put_req_header("accept", "application/json")
  end

  defp put_shard(id, status \\ "active") do
    %Shard{}
    |> Shard.changeset(%{
      shard_id: id,
      schema_version: 0,
      status: status,
      last_active_at: DateTime.utc_now()
    })
    |> Repo.insert!()
  end

  test "every route requires admin auth (401 without credentials)", %{conn: conn} do
    assert post(conn, "/api/tenants", %{}).status == 401
    assert get(conn, "/api/tenants").status == 401
    assert get(conn, "/api/tenants/x").status == 401
    assert delete(conn, "/api/tenants/x").status == 401
  end

  describe "POST /api/tenants" do
    test "provisions a new tenant with a url + token", %{conn: conn} do
      id = "api_new_#{System.unique_integer([:positive])}"
      on_exit(fn -> :ets.delete(Tombstones, id) end)

      body = conn |> auth() |> post("/api/tenants", %{shard_id: id}) |> json_response(201)

      assert body["shard_id"] == id
      assert body["url"] == "libsql://#{id}.local"
      assert is_binary(body["auth_token"])
      assert {:ok, %{status: "active"}} = Directory.get(id)
    end

    test "409 when the tenant already exists", %{conn: conn} do
      put_shard("api_dupe")
      assert (conn |> auth() |> post("/api/tenants", %{shard_id: "api_dupe"})).status == 409
    end

    test "409 for a tombstoned id (can't re-provision a deleted tenant)", %{conn: conn} do
      {:ok, _} = Directory.tombstone("api_dead")
      assert (conn |> auth() |> post("/api/tenants", %{shard_id: "api_dead"})).status == 409
    end

    test "400 for an invalid id", %{conn: conn} do
      assert (conn |> auth() |> post("/api/tenants", %{shard_id: "Not Valid!"})).status == 400
    end
  end

  describe "GET /api/tenants" do
    test "lists matching tenants with a total", %{conn: conn} do
      put_shard("api_list_a")
      put_shard("api_list_b")

      body = conn |> auth() |> get("/api/tenants?q=api_list_") |> json_response(200)

      ids = Enum.map(body["tenants"], & &1["shard_id"])
      assert "api_list_a" in ids and "api_list_b" in ids
      assert body["total"] >= 2
    end
  end

  describe "GET /api/tenants/:id" do
    test "200 for a known tenant, 404 otherwise", %{conn: conn} do
      put_shard("api_show")

      body = conn |> auth() |> get("/api/tenants/api_show") |> json_response(200)
      assert body["shard_id"] == "api_show"
      assert body["status"] == "active"

      assert (conn |> auth() |> get("/api/tenants/api_missing")).status == 404
    end
  end

  describe "DELETE /api/tenants/:id" do
    test "202 accepted and tombstones the tenant", %{conn: conn} do
      put_shard("api_del")
      on_exit(fn -> :ets.delete(Tombstones, "api_del") end)

      body = conn |> auth() |> delete("/api/tenants/api_del") |> json_response(202)
      assert body["status"] == "deleting"
      assert {:ok, %{status: "deleted"}} = Directory.get("api_del")
    end
  end

  describe "POST /api/tenants/:id/{suspend,resume}" do
    test "suspend then resume flip the status", %{conn: conn} do
      put_shard("api_susp")
      on_exit(fn -> :ets.delete(Suspensions, "api_susp") end)

      body = conn |> auth() |> post("/api/tenants/api_susp/suspend") |> json_response(200)
      assert body["status"] == "suspended"
      assert {:ok, %{status: "suspended"}} = Directory.get("api_susp")

      body = conn |> auth() |> post("/api/tenants/api_susp/resume") |> json_response(200)
      assert body["status"] == "active"
      assert {:ok, %{status: "active"}} = Directory.get("api_susp")
    end

    test "404 suspending an unknown tenant", %{conn: conn} do
      assert (conn |> auth() |> post("/api/tenants/api_ghost/suspend")).status == 404
    end
  end

  describe "token lifecycle (#24)" do
    test "mint returns a token; scope ro is reflected", %{conn: conn} do
      put_shard("api_tok")

      body = conn |> auth() |> post("/api/tenants/api_tok/token") |> json_response(200)
      assert is_binary(body["auth_token"])
      assert body["scope"] == "rw"

      ro =
        conn |> auth() |> post("/api/tenants/api_tok/token", %{scope: "ro"}) |> json_response(200)

      assert ro["scope"] == "ro"
      assert is_binary(ro["auth_token"])
    end

    test "rotate returns a fresh token", %{conn: conn} do
      put_shard("api_rot")
      body = conn |> auth() |> post("/api/tenants/api_rot/token/rotate") |> json_response(200)
      assert is_binary(body["auth_token"])
    end

    test "revoke reports the new floor", %{conn: conn} do
      put_shard("api_rev")
      body = conn |> auth() |> delete("/api/tenants/api_rev/token") |> json_response(200)
      assert is_integer(body["revoked_below_version"])
    end

    test "400 minting for an invalid id", %{conn: conn} do
      assert (conn |> auth() |> post("/api/tenants/Bad%20Id/token")).status == 400
    end
  end

  describe "POST /api/tenants/:id/fork (#14)" do
    test "404 forking a source with no stored object", %{conn: conn} do
      put_shard("api_fork_src")

      assert (conn
              |> auth()
              |> post("/api/tenants/api_fork_src/fork", %{dst: "api_fork_dst"})).status ==
               404
    end

    test "409 forking onto an existing destination", %{conn: conn} do
      put_shard("api_fs2")
      put_shard("api_fd2")

      assert (conn |> auth() |> post("/api/tenants/api_fs2/fork", %{dst: "api_fd2"})).status ==
               409
    end

    test "400 for an invalid destination id", %{conn: conn} do
      put_shard("api_fs3")

      assert (conn |> auth() |> post("/api/tenants/api_fs3/fork", %{dst: "Bad Dst!"})).status ==
               400
    end

    test "accepts flush_source: true (keystone-fork, #10)", %{conn: conn} do
      put_shard("api_fs_fl")

      # No stored object -> still 404, proving the flush_source param is parsed and doesn't break the path.
      assert (conn
              |> auth()
              |> post("/api/tenants/api_fs_fl/fork", %{dst: "api_fd_fl", flush_source: true})).status ==
               404
    end
  end

  describe "snapshots + restore (PITR, #12)" do
    setup do
      sid = "api_snap_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        Shards.drain(sid, 2_000)
        Storage.purge_shard(sid)

        for path <- Path.wildcard(Path.join([System.tmp_dir!(), "fathom_shards", "#{sid}*"])),
            do: File.rm(path)
      end)

      %{sid: sid}
    end

    test "snapshot -> mutate -> restore rolls the shard back, over the API", %{
      conn: conn,
      sid: sid
    } do
      {:ok, _} = Directory.resolve(sid)
      write!(sid, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('v1')"])
      flush!(sid)

      # Snapshot the current (durable) state via the API.
      body = conn |> auth() |> post("/api/tenants/#{sid}/snapshots") |> json_response(201)
      snap = body["snapshot_id"]
      assert is_binary(snap)

      # It shows up in the list.
      list = conn |> auth() |> get("/api/tenants/#{sid}/snapshots") |> json_response(200)
      assert Enum.any?(list["snapshots"], &(&1["id"] == snap))

      # Mutate past the snapshot.
      write!(sid, ["INSERT INTO t VALUES ('v2')"])
      flush!(sid)
      assert read_one(sid, "SELECT v FROM t ORDER BY v") == [["v1"], ["v2"]]

      # Restore over the API — the shard rolls back to the snapshot.
      restored =
        conn
        |> auth()
        |> post("/api/tenants/#{sid}/restore", %{snapshot: snap})
        |> json_response(200)

      assert restored["restored"] == snap
      assert read_one(sid, "SELECT v FROM t ORDER BY v") == [["v1"]]
    end

    test "flush: true snapshots the LIVE (un-flushed) state", %{conn: conn, sid: sid} do
      {:ok, _} = Directory.resolve(sid)
      write!(sid, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
      # 'a' is NOT flushed; flush: true makes the snapshot capture it.
      body =
        conn
        |> auth()
        |> post("/api/tenants/#{sid}/snapshots", %{flush: true})
        |> json_response(201)

      # Restoring that snapshot into a fresh shard proves 'a' was captured.
      flush!(sid)
      write!(sid, ["INSERT INTO t VALUES ('b')"])
      flush!(sid)
      conn |> auth() |> post("/api/tenants/#{sid}/restore", %{snapshot: body["snapshot_id"]})
      assert read_one(sid, "SELECT v FROM t") == [["a"]]
    end

    # Expert review 2026-07-18 #15: flush: true is the caller's request for a guaranteed-current
    # snapshot, so a failed force-flush must surface (422), not be swallowed into a silently-stale
    # snapshot. Steal the lease so the coordinator's flush_now self-fences with an error.
    test "flush: true surfaces a flush failure instead of a stale snapshot", %{
      conn: conn,
      sid: sid
    } do
      {:ok, _} = Directory.resolve(sid)

      # 'a' is durably stored, so Snapshots.create WOULD succeed on its own — isolating the flush
      # error as the only reason to fail. 'b' is live-only, held by a fresh coordinator.
      write!(sid, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
      flush!(sid)
      write!(sid, ["INSERT INTO t VALUES ('b')"])

      # Another node steals the lease; the force-flush of 'b' fences and errors instead of flushing.
      File.write!(
        Path.join([System.tmp_dir!(), "fathom_remote_test", "#{sid}.lock"]),
        Jason.encode!(%{
          "owner" => "thief@node",
          "epoch" => 999,
          "expires_at_ms" => System.system_time(:millisecond) + 60_000
        })
      )

      # Pre-fix the flush error was discarded and a (stale) snapshot was still created (201).
      {result, _log} =
        ExUnit.CaptureLog.with_log(fn ->
          conn |> auth() |> post("/api/tenants/#{sid}/snapshots", %{flush: true})
        end)

      assert result.status == 422
    end

    test "400 for an invalid id, 401 without auth", %{conn: conn} do
      assert (conn |> auth() |> post("/api/tenants/Bad%20Id/snapshots")).status == 400
      assert post(conn, "/api/tenants/whatever/snapshots").status == 401
      assert get(conn, "/api/tenants/whatever/snapshots").status == 401
      assert post(conn, "/api/tenants/whatever/restore").status == 401
    end
  end

  describe "POST /api/tenants/:id/flush (#10)" do
    test "200 flushes (no-op :ok when no coordinator is running locally)", %{conn: conn} do
      put_shard("api_flush")
      body = conn |> auth() |> post("/api/tenants/api_flush/flush") |> json_response(200)
      assert body["shard_id"] == "api_flush"
      assert body["flushed"] == true
    end

    test "400 for an invalid id", %{conn: conn} do
      assert (conn |> auth() |> post("/api/tenants/Bad%20Id/flush")).status == 400
    end

    test "401 without auth", %{conn: conn} do
      assert post(conn, "/api/tenants/api_flush/flush").status == 401
    end
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp write!(shard, sqls) do
    {:ok, handle} = ShardExecutor.open(shard)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, stmt(s)) end)
    :ok = ShardExecutor.close(handle)
  end

  defp read_one(shard, sql) do
    {:ok, handle} = ShardExecutor.open(shard)
    {:ok, result} = ShardExecutor.execute(handle, stmt(sql))
    :ok = ShardExecutor.close(handle)
    result.rows
  end

  defp flush!(shard), do: :ok = Shards.drain(shard, 5_000)
end
