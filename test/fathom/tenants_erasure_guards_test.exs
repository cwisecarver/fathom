defmodule Fathom.TenantsErasureGuardsTest do
  @moduledoc """
  Expert review 2026-10-15 #12 (restore integrity), #13 (restore vs tombstone), #20 (early durable
  tombstone marker), #26 (fork staleness signal), #29 (fork lease renewer).
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.{Directory, ShardExecutor, Shards, Snapshots, Tenants}
  alias Fathom.Shard.{Integrity, Storage}
  alias Fathom.Tenants.Tombstones
  alias Filo.Stmt

  setup do
    id = "eg_#{System.unique_integer([:positive])}"
    dst = id <> "fork"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_ttl = Application.get_env(:fathom, :shard_lease_ttl_ms)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      restore_env(:shard_storage, prev_storage)
      restore_env(:shard_lease_ttl_ms, prev_ttl)

      for sid <- [id, dst] do
        :ets.delete(Tombstones, sid)
        Shards.drain(sid, 2_000)
        Storage.purge_shard(sid)

        for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{sid}*"])),
            do: File.rm(path)
      end
    end)

    %{id: id, dst: dst}
  end

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, value), do: Application.put_env(:fathom, key, value)

  defp write!(shard, sqls) do
    {:ok, handle} = ShardExecutor.open(shard)

    Enum.each(sqls, fn s ->
      {:ok, _} = ShardExecutor.execute(handle, %Stmt{sql: s, args: []})
    end)

    :ok = ShardExecutor.close(handle)
  end

  defp flush!(shard), do: :ok = Shards.drain(shard, 5_000)

  # --- #20 ------------------------------------------------------------------------------------

  # Symptom: the durable storage tombstone was written only after the purge, so a directory PITR
  # inside the DeleteJob's retry horizon un-tombstoned an id whose objects still existed.
  test "delete/1 writes the durable marker BEFORE the job runs (#20)", %{id: id} do
    {:ok, _} = Directory.resolve(id)
    {:ok, ids} = Storage.tombstoned_ids()
    refute id in ids

    assert {:ok, :scheduled} = Tenants.delete(id)

    # DeleteJob has NOT run (Oban testing mode): the marker must already be durable.
    assert_enqueued(worker: Fathom.Tenants.DeleteJob, args: %{shard_id: id})
    {:ok, ids} = Storage.tombstoned_ids()
    assert id in ids, "no durable tombstone marker until the purge ran"
  end

  # --- #26 ------------------------------------------------------------------------------------

  test "fork reports which source state the clone carries (#26)", %{id: src, dst: dst} do
    {:ok, _} = Directory.resolve(src)
    write!(src, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
    flush!(src)

    {:ok, row} = Directory.get(src)
    assert {:ok, tenant} = Tenants.fork(src, dst)
    assert Map.has_key?(tenant, :source_flushed_at)
    assert tenant.source_flushed_at == row.last_flushed_at

    dst2 = dst <> "2"
    on_exit(fn -> Storage.purge_shard(dst2) end)
    assert {:ok, flushed} = Tenants.fork(src, dst2, flush_source: true)
    assert %DateTime{} = flushed.source_flushed_at
  end

  # --- #29 ------------------------------------------------------------------------------------

  # Symptom: the fork held the dst lease with no renewer; a copy longer than the TTL let a peer
  # steal it mid-copy. The FaultyStorage :fork_shard hook blocks the copy; the lease must be
  # renewed while it is blocked.
  test "the dst lease is renewed for the whole fork copy (#29)", %{id: src, dst: dst} do
    {:ok, _} = Directory.resolve(src)
    write!(src, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
    flush!(src)

    Application.put_env(:fathom, :shard_lease_ttl_ms, 300)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    test = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:fork_shard,
       fn id ->
         if id == dst do
           send(test, {:copying, self()})
           receive do: (:proceed -> :ok)
         end

         :ok
       end}
    )

    fork = Task.async(fn -> Tenants.fork(src, dst) end)
    assert_receive {:copying, hook_pid}, 5_000
    {:held, _owner, base} = Storage.lease_stealable_at(dst)

    assert wait_advanced(dst, base, 3_000),
           "the fork never renewed the dst lease during a long copy (stealable_at frozen)"

    send(hook_pid, :proceed)
    assert {:ok, _} = Task.await(fork, 10_000)
  end

  defp wait_advanced(shard, base, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      case Storage.lease_stealable_at(shard) do
        {:held, _owner, at} when at > base ->
          :advanced

        _ ->
          Process.sleep(10)
          :waiting
      end
    end)
    |> Enum.find(&(&1 == :advanced or System.monotonic_time(:millisecond) > deadline))
    |> Kernel.==(:advanced)
  end

  # --- #13 ------------------------------------------------------------------------------------

  describe "Snapshots.restore/3 vs tombstones (#13)" do
    # Symptom: restore never consulted the tombstone set, so a restore racing (or following) a delete
    # recreated the erased tenant's live object.
    test "refuses a tombstoned tenant up front, leaving live untouched", %{id: id} do
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
      flush!(id)
      {:ok, snap} = Snapshots.create(id)
      {:ok, etag} = Storage.object_etag(id)

      Tombstones.put(id)

      assert {:error, :tombstoned} = Snapshots.restore(id, snap)
      assert {:ok, ^etag} = Storage.object_etag(id)
    end

    test "a delete landing during the copy-back does not leave the live object behind",
         %{id: id} do
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
      flush!(id)
      {:ok, snap} = Snapshots.create(id)

      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
      # The delete's tombstone arrives while the copy-back is in flight.
      Application.put_env(
        :fathom,
        :faulty_before,
        {:restore_snapshot, fn _ -> :ets.insert(Tombstones, {id}) end}
      )

      assert {:error, :tombstoned} = Snapshots.restore(id, snap)

      assert {:ok, nil} == Storage.object_etag(id),
             "the erased tenant's live object was resurrected"
    end
  end

  # --- #12 ------------------------------------------------------------------------------------

  describe "Snapshots.restore/3 integrity gate (#12)" do
    # Symptom: restore checked only the MD5 and user_version, so a snapshot of a damaged object
    # replaced a healthy live one. The fixture asserts its own precondition (it really is corrupt).
    test "refuses a structurally corrupt snapshot unless allow_corrupt", %{id: id} do
      rows = for i <- 1..400, do: "INSERT INTO t VALUES ('row-#{i}-#{String.duplicate("x", 40)}')"
      write!(id, ["CREATE TABLE t (v TEXT)" | rows])
      flush!(id)
      {:ok, snap} = Snapshots.create(id)
      {:ok, etag} = Storage.object_etag(id)

      [file] = Path.wildcard(Path.join(Storage.Local.dir(), "#{id}@snap-*.db"))
      corrupt!(file)

      probe = file <> ".probe"
      File.cp!(file, probe)
      on_exit(fn -> File.rm(probe) end)

      assert {:error, _} = Integrity.verify_full(probe),
             "the fixture did not actually corrupt anything"

      assert {:error, {:snapshot_corrupt, _}} = Snapshots.restore(id, snap)
      assert {:ok, ^etag} = Storage.object_etag(id), "live was replaced by a corrupt snapshot"

      # `force` is schema-skew only and must NOT override the integrity gate.
      assert {:error, {:snapshot_corrupt, _}} = Snapshots.restore(id, snap, force: true)

      assert :ok = Snapshots.restore(id, snap, allow_corrupt: true)
    end
  end

  # Smash the b-tree page header (type flag, cell count, content offset) of pages 2 and 3. Garbling cell
  # text alone is invisible to integrity_check, so the header is what makes the damage structural.
  defp corrupt!(path) do
    bin = File.read!(path)

    bin =
      Enum.reduce([4096, 8192], bin, fn off, acc ->
        <<head::binary-size(^off), _::binary-size(8), tail::binary>> = acc
        head <> :binary.copy(<<0xFF>>, 8) <> tail
      end)

    File.write!(path, bin)
  end
end
