defmodule Fathom.Shard.ErasedOpenGateTest do
  @moduledoc """
  Expert review 2026-11-07 #14 (tenant-lifecycle#3). The `Tombstones` set loads asynchronously after
  the Edge starts and reads "not tombstoned" until a load FAILS, and a node that was down for a delete
  (or holds a clean-retained copy) still has the erased tenant's residue on disk. A stray request then
  opened the lease, found NO stored object, and took the deliberate "un-flushed brand-new shard"
  stance: serve the local copy, fence with nil, and let the next flush re-create the object (and its
  `.lock`) — an erased tenant's data served and re-persisted.

  The durable `tombstones/<id>` storage marker needs no Postgres and no loaded set, so the one place
  the open concludes "object absent, local copy present" now asks for it (one HEAD / stat, only on that
  rare branch): present ⇒ the local copy is erased (no quarantine) and the open is refused
  `:shard_tombstoned`. The ETS set is deliberately NOT populated here: it is the thing that was not
  loaded yet.

  What this does NOT change, and the tests pin so the stance is not "simplified" away: an absent
  object with no marker is still a brand-new / operator-deleted shard that serves warm and re-creates
  (that is a LIVE tenant's un-flushed writes), and an unreadable marker neither serves nor deletes.

  Both liveness modes (mode asserted). Not async: app env, real shard files, node-global gates.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Retained, Storage}
  alias Fathom.Tenants.Tombstones
  alias Filo.Stmt

  setup do
    id = "erasedopen_#{System.unique_integer([:positive])}"
    keys = [:shard_retain_local_bytes, :shard_retain_min_bytes, :shard_idle_ms]
    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}
    Application.put_env(:fathom, :shard_retain_local_bytes, 64 * 1024 * 1024)
    Application.put_env(:fathom, :shard_retain_min_bytes, 0)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Retained.reconcile([], Shard.data_dir())
    _ = Retained.stats()

    on_exit(fn ->
      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end

      Shards.drain(id, 2_000)
      Storage.purge_shard(id)
      File.rm(Path.join([Storage.Local.dir(), "tombstones", id]))
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{id}.db*")), do: File.rm(f)
      Retained.reconcile([], Shard.data_dir())
    end)

    %{id: id, path: Shard.db_path(id)}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp write!(id, sqls) do
    {:ok, conn} = ShardExecutor.open(id)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(conn, stmt(s)) end)
    :ok = ShardExecutor.close(conn)
  end

  defp rows!(id) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT v FROM t ORDER BY v"))
    :ok = ShardExecutor.close(conn)
    rows
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(id, mode) do
    {:ok, pid} = Shards.ensure(id)
    gen = :sys.get_state(pid).acquire_gen

    if mode == :heartbeat,
      do: assert(is_integer(gen), "heartbeat mode did not take"),
      else: assert(is_nil(gen), "legacy mode did not take")

    ref = Process.monitor(pid)
    assert :ok = Shards.drain(id, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end

  # A shard with a stored object AND a clean-retained local copy, the shape a node that was down for
  # the delete (or kept a retained copy) wakes up with.
  defp residue!(id, path, mode) do
    set_mode!(mode)
    write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('erased-person')"])
    assert :ok = Shards.drain(id, 5_000)
    assert File.exists?(path), "fixture: the clean drop must have retained the local copy"
    assert File.exists?(Retained.marker(path)), "fixture: ... as a CLEAN-RETAINED one"
    assert {:ok, true} = Storage.any_shard_object?(id), "fixture: the object must be stored"
    assert_mode!(id, mode)
    :ok
  end

  # What the DeleteJob leaves: every object purged. The durable marker is written by `delete/1`
  # BEFORE the purge (`write_early_marker/1`) and again after it.
  defp erase_storage!(id) do
    assert :ok = Storage.put_tombstone(id)
    assert :ok = Storage.purge_shard(id)
    assert {:ok, false} = Storage.any_shard_object?(id), "fixture: nothing may be stored"

    refute Tombstones.tombstoned?(id),
           "fixture: the in-memory gate is the part that was not loaded"
  end

  for mode <- [:legacy, :heartbeat] do
    describe "#{mode}: a local copy, no stored object, and the id was erased" do
      test "a retained copy is erased and the open refused; nothing is re-created",
           %{id: id, path: path} do
        residue!(id, path, unquote(mode))
        erase_storage!(id)

        assert {:error, :shard_tombstoned} = Shards.checkout(id)

        refute File.exists?(path), "the erased tenant's local copy survived the refused open"
        refute File.exists?(path <> ".etag"), "its provenance sidecar survived too"
        refute File.exists?(path <> "-wal")

        # The review's regression: a flush after the (previously served) open re-created the object.
        assert :ok = Shards.flush(id)

        assert {:ok, false} = Storage.any_shard_object?(id),
               "the open re-created an erased tenant's object and lock"
      end

      test "a plain WARM copy (dirty/unretained, with a provenance sidecar) is refused the same way",
           %{id: id, path: path} do
        residue!(id, path, unquote(mode))
        # Not retained any more: an ordinary warm copy that may hold un-flushed writes.
        File.rm!(Retained.marker(path))
        Retained.reconcile([], Shard.data_dir())
        erase_storage!(id)

        assert {:error, :shard_tombstoned} = Shards.checkout(id)

        refute File.exists?(path)
        assert :ok = Shards.flush(id)
        assert {:ok, false} = Storage.any_shard_object?(id)
      end

      test "WITHOUT a marker an absent object is still a brand-new shard: served warm, re-created",
           %{id: id, path: path} do
        residue!(id, path, unquote(mode))
        File.rm!(Retained.marker(path))
        Retained.reconcile([], Shard.data_dir())
        # The object is gone (an operator removed it), but nobody tombstoned the id.
        assert :ok = Storage.purge_shard(id)
        assert {:ok, false} = Storage.any_shard_object?(id)

        assert [["erased-person"]] = rows!(id), "the un-flushed local copy is the tenant's data"
        assert :ok = Shards.flush(id)

        assert {:ok, true} = Storage.any_shard_object?(id),
               "an un-erased tenant's un-flushed writes must still reach the store"
      end

      test "a claimed CLEAN-RETAINED copy whose object vanished (no marker) is discarded, not re-created",
           %{id: id, path: path} do
        residue!(id, path, unquote(mode))
        assert File.exists?(Retained.marker(path))
        assert :ok = Storage.purge_shard(id)
        assert {:ok, false} = Storage.any_shard_object?(id)

        # It holds nothing the store lacks, so there is nothing to protect by re-creating from it:
        # the open pulls (absent ⇒ a born-empty shard) instead of serving the retained bytes.
        assert {:ok, pid, ref, _path} = Shards.checkout(id)
        Shard.checkin(pid, ref)

        assert {:ok, conn} = ShardExecutor.open(id)

        assert {:error, _no_such_table} =
                 ShardExecutor.execute(conn, stmt("SELECT v FROM t")),
               "a retained copy of a vanished object was served"

        :ok = ShardExecutor.close(conn)
      end
    end
  end

  test "an erased id's marker is per shard: a neighbour with the same prefix is unaffected",
       %{id: id, path: path} do
    neighbour = id <> "2"
    on_exit(fn -> Shards.drain(neighbour, 2_000) end)
    on_exit(fn -> Storage.purge_shard(neighbour) end)

    on_exit(fn ->
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{neighbour}.db*")), do: File.rm(f)
    end)

    residue!(id, path, :legacy)
    erase_storage!(id)

    # The neighbour has no marker and no object, and no local copy: an ordinary new shard.
    assert {:ok, pid, ref, _} = Shards.checkout(neighbour)
    Shard.checkin(pid, ref)

    assert {:error, :shard_tombstoned} = Shards.checkout(id)
  end
end
