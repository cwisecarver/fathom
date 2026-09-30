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
