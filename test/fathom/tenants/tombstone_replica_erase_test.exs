defmodule Fathom.Tenants.TombstoneReplicaEraseTest do
  @moduledoc """
  A deleted tenant's A2 REPLICAS are erased on every follower (expert review 2026-09-29 #5).

  `Tenants.purge/1` erases the stored objects and the deleting node's own data dir. With
  replication on (the prod default) each follower in the quorum also holds a complete `.db` +
  `-wal` of the tenant, and nothing erased those: `Follower.forget/2` was called only from
  promotions, and the delete notification's copy-purge had gone with the WarmFollower. So a
  "GDPR-erased" tenant's full database stayed on Q other nodes indefinitely.

  The three ways a follower can meet a deleted tenant, one test each:
    * the delete notification arrives while it holds the replica — erase it;
    * it was DOWN during the delete — erase it on recovery instead of recovering it;
    * a straggling push after the erase provokes the primary's reflex re-seed — refuse it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Tenants.Tombstones

  setup do
    id = "tombrep_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "tombrep_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      :ets.delete(Tombstones, id)
      File.rm_rf(dir)
    end)

    %{id: id, dir: dir}
  end

  # The DEFAULT-named follower: the erase hook in `Tombstones` acts on this node's Follower, which in
  # production is the one started under its module name.
  defp start_follower!(dir) do
    pid = start_supervised!({Follower, name: Follower, port: 0, dir: dir}, id: Follower)
    _ = :sys.get_state(pid)
    pid
  end

  # A replica with real bytes on disk: an empty `.db` and a valid WAL header are all recovery needs,
  # and all `forget/2` must remove.
  defp plant_replica!(id) do
    File.write!(Follower.db_path(Follower, id), "db")
    File.write!(Follower.wal_path(Follower, id), "wal")
    File.write!(Follower.torn_path(Follower, id), "")
    :ok = Follower.seed(Follower, id, 1, 0, 0, 3, 4)

    assert Follower.state_of(Follower, id), "precondition: the replica was not planted"
    assert File.exists?(Follower.lineage_path(Follower, id)), "precondition: no lineage sidecar"
  end

  defp replica_files(id) do
    [
      Follower.db_path(Follower, id),
      Follower.wal_path(Follower, id),
      Follower.torn_path(Follower, id),
      Follower.lineage_path(Follower, id)
    ]
    |> Enum.filter(&File.exists?/1)
  end

  test "the delete notification erases this node's replica of the tenant", %{id: id, dir: dir} do
    start_follower!(dir)
    plant_replica!(id)

    pid = Process.whereis(Tombstones)
    send(pid, {:notification, Tombstones.channel(), %{"shard_id" => id}})
    _ = :sys.get_state(pid)

    assert Follower.state_of(Follower, id) == nil,
           "the follower still tracks a replica of a deleted tenant"

    assert replica_files(id) == [],
           "a deleted tenant's replica is still on this follower's disk: #{inspect(replica_files(id))}"
  end

  test "the deleting node's own put/1 erases a local replica too", %{id: id, dir: dir} do
    start_follower!(dir)
    plant_replica!(id)

    :ok = Tombstones.put(id)

    assert Follower.state_of(Follower, id) == nil
    assert replica_files(id) == []
  end

  test "a follower that was down during the delete erases the replica on recovery",
       %{id: id, dir: dir} do
    start_follower!(dir)
    plant_valid_wal!(id)
    :ok = Follower.seed(Follower, id, 1, 0, 0, 32, 4)
    # Path helpers read the Follower's table, so resolve the path while it is up.
    db = Follower.db_path(Follower, id)
    stop_supervised!(Follower)

    # Tombstoned while the Follower is down — the erase hook finds no Follower to act on.
    :ok = Tombstones.put(id)
    assert File.exists?(db), "precondition: the replica is on disk"

    start_follower!(dir)

    assert Follower.state_of(Follower, id) == nil,
           "recovery brought back a replica of a deleted tenant"

    assert replica_files(id) == [], "recovery left a deleted tenant's replica on disk"
  end

  test "a follower refuses to (re)seed a deleted tenant", %{id: id, dir: dir} do
    start_follower!(dir)
    :ok = Tombstones.put(id)

    begin = %Protocol.SeedBegin{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      wal_offset: 0,
      db_size: 4096,
      wal_size: 0,
      lineage: 1
    }

    assert Follower.begin_seed(Follower, %{}, begin) == %{}, "the seed was opened"
    assert replica_files(id) == []
  end

  # LAZY BOOT + BUSY LOCK (expert review 2026-10-21 rv6 F5). The erase hook forgot the replica only
  # when `state_of/2` returned a row. Since the lazy boot (2026-10-21 #5) a replica on disk is
  # usually not resident, so `state_of/2` re-derives it; with the shard's lock held elsewhere (a
  # push or seed) that re-derive answers nil for a tombstoned shard, the hook skipped the forget,
  # and the deleted tenant's `.db` + `-wal` stayed on disk until the next boot.
  test "a live delete erases a NON-resident replica while its lock is busy", %{id: id, dir: dir} do
    put_env!(:replication_forget_lock_wait_ms, 100)
    {db, wal} = real_replica_bytes!(dir)
    File.write!(Path.join(dir, id <> ".db"), db)
    File.write!(Path.join(dir, id <> ".db-wal"), wal)

    start_follower!(dir)

    assert :ets.lookup(Follower.table(Follower), id) == [],
           "fixture: the row must not be resident"

    test = self()

    holder =
      spawn_link(fn ->
        {:ok, tok} = Follower.lock_shard(Follower, id, 1_000)
        send(test, :held)

        receive do
          :release -> Follower.unlock_shard(Follower, tok)
        end

        send(test, :released)
      end)

    assert_receive :held
    :ok = Tombstones.put(id)
    send(holder, :release)
    assert_receive :released

    assert replica_files(id) == [],
           "a deleted tenant's replica survived a live delete: #{inspect(replica_files(id))}"
  end

  # A SEED IN FLIGHT ACROSS THE DELETE (expert review 2026-10-21 rv6 F5). `begin_seed/3` refuses a
  # tombstoned shard, but a seed that began first holds the shard's lock, so the erase hook's
  # `forget/2` waits out its lock wait and erases without it, and the install then put the deleted
  # tenant's replica back on disk.
  test "a seed that began before the delete is discarded at install", %{id: id, dir: dir} do
    put_env!(:replication_forget_lock_wait_ms, 50)
    start_follower!(dir)
    {db, _wal} = real_replica_bytes!(dir)

    seeds =
      Follower.begin_seed(Follower, %{}, %Protocol.SeedBegin{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: byte_size(db),
        wal_size: 0,
        lineage: 1
      })

    assert Map.has_key?(seeds, id), "fixture: the seed must open before the delete"

    :ok = Tombstones.put(id)

    seeds = Follower.write_chunk(seeds, id, :db, 0, db)
    assert {{:error, :tombstoned}, %{}} = Follower.finish_seed(Follower, seeds, id)
    assert Follower.state_of(Follower, id) == nil
    assert replica_files(id) == [], "the seed re-installed a deleted tenant's replica"
  end

  defp put_env!(key, value) do
    prev = Application.get_env(:fathom, key)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, key),
        else: Application.put_env(:fathom, key, prev)
    end)

    Application.put_env(:fathom, key, value)
  end

  # A real `.db` + committed `-wal` pair, built off to the side.
  defp real_replica_bytes!(dir) do
    src = Path.join(dir, "src_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Fathom.Shard.Connection.open(src)
    {:ok, _} = Fathom.Shard.Connection.query(conn, "PRAGMA wal_autocheckpoint=0", [])
    {:ok, _} = Fathom.Shard.Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Fathom.Shard.Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    bytes = {File.read!(src), File.read!(src <> "-wal")}
    Fathom.Shard.Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(src <> s)
    bytes
  end

  # A shard file whose WAL `Wal.read/1` accepts, so recovery would pick it up if not for the
  # tombstone — otherwise the recovery test passes because the files were unreadable, not because
  # of the guard.
  defp plant_valid_wal!(id) do
    db = Follower.db_path(Follower, id)
    {:ok, conn} = Fathom.Shard.Connection.open(db)
    {:ok, _} = Fathom.Shard.Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Fathom.Shard.Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    File.cp!(db <> "-wal", Follower.wal_path(Follower, id) <> ".keep")
    Fathom.Shard.Connection.close(conn)
    File.rename!(Follower.wal_path(Follower, id) <> ".keep", Follower.wal_path(Follower, id))

    assert {:ok, %{}} = Fathom.Shard.Replication.Wal.read(Follower.wal_path(Follower, id)),
           "precondition: the planted WAL is not readable, so recovery would skip it anyway"
  end
end
