defmodule Fathom.TenantsTest do
  @moduledoc """
  Tenant deletion orchestration (expert review 2026-07-14 #15): `Fathom.Tenants.delete/1`
  tombstones + broadcasts + enqueues, and `purge/1` (the DeleteJob body) erases every stored
  object and drains the coordinator. The re-mint refusal itself is pinned in
  `Fathom.Tenants.TombstonesTest`.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.{Directory, ShardExecutor, Shards, Snapshots, Tenants}
  alias Fathom.Shard.Storage
  alias Fathom.Tenants.{DeleteJob, Suspensions, Tombstones}
  alias Filo.Stmt

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    id = "ten_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # The gate ETS tables are app-global — forget this id so it can't gate another test's shard.
      :ets.delete(Tombstones, id)
      :ets.delete(Suspensions, id)
      Shards.drain(id, 2_000)
      Storage.purge_shard(id)

      for path <- Path.wildcard(Path.join([System.tmp_dir!(), "fathom_shards", "#{id}*"])),
          do: File.rm(path)
    end)

    %{id: id}
  end

  describe "delete/1" do
    test "tombstones, sets the re-mint gate, and enqueues DeleteJob", %{id: id} do
      {:ok, _} = Directory.resolve(id)

      assert {:ok, :scheduled} = Tenants.delete(id)

      # Durable tombstone (directory), immediate re-mint block (ETS gate), physical erase queued.
      assert {:ok, %{status: "deleted"}} = Directory.get(id)
      assert Tenants.tombstoned?(id)
      assert_enqueued(worker: DeleteJob, args: %{shard_id: id})
    end

    test "registers + tombstones a shard the directory never recorded", %{id: id} do
      assert {:ok, :scheduled} = Tenants.delete(id)
      assert {:ok, %{status: "deleted"}} = Directory.get(id)
    end

    test "refuses an invalid id" do
      assert {:error, :invalid_shard_id} = Tenants.delete("Not Valid!")
    end
  end

  describe "purge/1 (the DeleteJob body)" do
    test "erases every stored object of an idle tenant", %{id: id} do
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('secret')"])
      flush!(id)
      assert :ok = Storage.retain(id, 1)
      assert {:ok, _snap} = Snapshots.create(id)

      assert :ok = Tenants.purge(id)

      assert {:ok, nil} == Storage.object_etag(id)
      assert {:ok, []} == Snapshots.list(id)
      refute File.exists?(Path.join(@remote_dir, "#{id}@1.db"))
      refute File.exists?(Path.join(@remote_dir, "#{id}.lock"))
    end

    # The regression: purging a shard that is ACTIVELY SERVED must force-stop its coordinator
    # (not graceful-drain, which can't stop a busy one). Pre-fix, the coordinator stayed alive,
    # then self-fenced on its next flush and quarantined the deleted tenant's data to a
    # `.fenced.<ts>` file on local disk — an erased tenant's rows surviving the erase.
    test "force-stops a live coordinator and leaves NO quarantine copy on disk", %{id: id} do
      # Hold an open connection with an uncommitted-to-storage write, so the coordinator is
      # busy (a graceful drain would return :busy and leave it running).
      {:ok, handle} = ShardExecutor.open(id)
      {:ok, _} = ShardExecutor.execute(handle, stmt("CREATE TABLE t (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(handle, stmt("INSERT INTO t VALUES ('pii')"))

      assert :ok = Tenants.purge(id)

      # Coordinator is gone (force-stopped, not left busy) ...
      assert [] == Registry.lookup(Fathom.ShardRegistry, id)
      # ... storage is gone ...
      assert {:ok, nil} == Storage.object_etag(id)
      # ... and NO local copy survives — neither the working file nor a self-fence quarantine.
      local_base = Fathom.Shard.db_path(id)
      assert [] == Path.wildcard(local_base <> "*")
      refute Enum.any?(Path.wildcard(local_base <> "*"), &String.contains?(&1, ".fenced."))
    end

    test "is idempotent on an already-erased tenant", %{id: id} do
      assert :ok = Tenants.purge(id)
      assert :ok = Tenants.purge(id)
      assert {:ok, nil} == Storage.object_etag(id)
    end
  end

  describe "export/1" do
    test "exports the tenant's durable data as an openable SQLite file", %{id: id} do
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('portable')"])
      flush!(id)

      assert {:ok, %{path: path, filename: filename}} = Tenants.export(id)
      on_exit(fn -> File.rm(path) end)

      assert filename == "#{id}.db"
      assert File.exists?(path)

      # It's a real SQLite database carrying exactly this tenant's rows.
      {:ok, db} = Exqlite.Sqlite3.open(path)
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "SELECT v FROM t")
      assert {:row, ["portable"]} = Exqlite.Sqlite3.step(db, stmt)
      :ok = Exqlite.Sqlite3.release(db, stmt)
      :ok = Exqlite.Sqlite3.close(db)
    end

    test "not_stored for a shard that was never flushed", %{id: id} do
      assert {:error, :not_stored} = Tenants.export(id)
    end

    test "refuses an invalid id" do
      assert {:error, :invalid_shard_id} = Tenants.export("Not Valid!")
    end
  end

  describe "suspend/1 and resume/1" do
    test "suspend denies admission (403) and resume restores service", %{id: id} do
      {:ok, _} = Directory.resolve(id)

      assert :ok = Tenants.suspend(id)
      assert Tenants.suspended?(id)
      assert {:ok, %{status: "suspended"}} = Directory.get(id)
      # New streams are refused while suspended.
      assert {:error, :shard_suspended} = Shards.checkout(id)

      assert :ok = Tenants.resume(id)
      refute Tenants.suspended?(id)
      assert {:ok, %{status: "active"}} = Directory.get(id)
      assert {:ok, _pid, _ref, _path} = Shards.checkout(id)
    end

    test "refuses to suspend a deleted (tombstoned) tenant", %{id: id} do
      {:ok, _} = Directory.tombstone(id)
      assert {:error, :deleted} = Tenants.suspend(id)
    end

    test "refuses an unknown tenant and an invalid id", %{id: id} do
      assert {:error, :not_found} = Tenants.suspend(id)
      assert {:error, :invalid_shard_id} = Tenants.suspend("Not Valid!")
    end
  end

  describe "provision/1" do
    test "registers the tenant active and returns a libsql url + token", %{id: id} do
      assert {:ok, tenant} = Tenants.provision(id)

      assert tenant.shard_id == id
      assert tenant.url == "libsql://#{id}.local"
      assert is_binary(tenant.auth_token)
      # test config has :hrana_auth disabled — the token is informational there.
      assert tenant.auth_required == false
      assert {:ok, %{status: "active"}} = Directory.get(id)
    end

    test "refuses a tenant that already exists", %{id: id} do
      {:ok, _} = Directory.resolve(id)
      assert {:error, :already_exists} = Tenants.provision(id)
    end

    test "refuses a tombstoned (deleted) id — no resurrection via provisioning", %{id: id} do
      {:ok, _} = Directory.tombstone(id)
      assert {:error, :tombstoned} = Tenants.provision(id)
    end

    test "refuses an invalid id" do
      assert {:error, :invalid_shard_id} = Tenants.provision("Not Valid!")
    end
  end

  test "the DeleteJob worker runs purge end to end", %{id: id} do
    write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('x')"])
    flush!(id)

    assert :ok = perform_job(DeleteJob, %{shard_id: id})
    assert {:ok, nil} == Storage.object_etag(id)
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp write!(shard, sqls) do
    {:ok, handle} = ShardExecutor.open(shard)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, stmt(s)) end)
    :ok = ShardExecutor.close(handle)
  end

  defp flush!(shard), do: :ok = Shards.drain(shard, 5_000)
end
