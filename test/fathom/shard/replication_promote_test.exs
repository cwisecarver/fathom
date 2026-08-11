defmodule Fathom.Shard.ReplicationPromoteTest do
  @moduledoc """
  Promotion — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  **`promotes_writes_that_never_reached_storage` is the test this whole phase is for.** Everything
  else in A2 moves bytes between nodes; that one asserts the bytes are worth moving, by writing
  rows, replicating them, deliberately never flushing to S3, and then reading them back out of a
  promoted replica. Without it the RPO~0 claim is an argument rather than a measurement — and every
  other A2 test would still pass if promotion silently recovered from the stored object instead of
  the replica, which is exactly the mistake the seed path already had to be defended against.

  The rest pin the ways promotion can be *unsafe* rather than merely broken: taking a shard a live
  node still owns, or serving bytes nothing has asked SQLite to validate.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Storage
  alias Fathom.Shards

  setup do
    id = "repl_promote_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replpromote_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev = %{
      enabled: Application.get_env(:fathom, :replication_enabled),
      followers: Application.get_env(:fathom, :replication_followers),
      quorum: Application.get_env(:fathom, :replication_quorum)
    }

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- [
            replication_enabled: prev.enabled,
            replication_followers: prev.followers,
            replication_quorum: prev.quorum
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"promote_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp enable!(followers, q) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, q)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
    await_connected!()
  end

  defp await_connected!(timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Enum.all?(Fleet.shippers(), &Shipper.connected?/1),
        do: :connected,
        else: Process.sleep(20)
    end)
    |> Enum.find(fn
      :connected -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :connected -> :ok
      _ -> flunk("shippers never connected")
    end
  end

  defp await_seeded(name, id, commit, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Follower.state_of(name, id), do: :seeded, else: commit.() && Process.sleep(25)
    end)
    |> Enum.find(fn
      :seeded -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :seeded -> :ok
      _ -> flunk("follower #{name} was never seeded for #{id}")
    end
  end

  # Drives a real shard through a real coordinator until every follower holds its bytes, then
  # closes the primary down WITHOUT letting anything flush to storage. What is left is a follower
  # with rows that exist nowhere else, which is the situation promotion exists for.
  defp replicate_then_abandon_primary(id, followers, rows) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    {:ok, conn} = Connection.open(path)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)", [])
    wal = path <> "-wal"

    # Seed everyone first, so the rows below are replicated as WAL deltas rather than swept up in
    # a base copy. Otherwise this test could pass with delta shipping completely broken.
    # Previously `{:error, {:no_quorum, _}}`: the commit that triggers a seed now waits for it.
    # See the note at the top of this file's first seed test.
    assert :ok = Session.commit(id, wal, coordinator)

    for {name, _} <- followers,
        do: await_seeded(name, id, fn -> Session.commit(id, wal, coordinator) end)

    for i <- 1..rows do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [i, "row-#{i}"])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    # Everything below this line is the primary going away. Nothing flushes.
    Connection.close(conn)
    Session.stop(id)
    Fathom.Shard.checkin(coordinator, ref)
    Fathom.Shards.stop(id)

    # The coordinator's own shutdown flush is what this test must NOT benefit from, so the local
    # files are removed too: whatever promotion recovers has to come from the follower.
    for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)

    :ok
  end

  defp rows_in(path) do
    {:ok, conn} = Connection.open(path)

    try do
      {:ok, %{rows: rows}} = Connection.query(conn, "SELECT a FROM t ORDER BY a", [])
      Enum.map(rows, fn [a] -> a end)
    after
      Connection.close(conn)
    end
  end

  test "promotes writes that never reached storage", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    :ok = replicate_then_abandon_primary(id, followers, 12)
    [{name, _} | _] = followers

    # Precondition, asserted rather than assumed: the stored object does NOT have these rows. If
    # storage already held them the test would pass with promotion reading from S3, proving
    # nothing about replication at all.
    stored = Path.join(root, "stored.db")

    case Storage.pull(id, stored) do
      {:absent, _} ->
        :ok

      {:ok, _} ->
        refute Enum.member?(rows_in(stored), 12),
               "the shard flushed after all — this test cannot show what replication recovered"
    end

    assert {:ok, %{shard_id: ^id, epoch: epoch}} = Promote.promote(id, follower: name)
    assert epoch > 0

    # THE ASSERTION. Twelve rows that only ever existed on the primary and its replicas are now
    # readable from this node's live shard file.
    assert rows_in(Fathom.Shard.db_path(id)) == Enum.to_list(1..12)

    # And they are durable now: the promoted bytes were published, so a cold open elsewhere gets
    # them too rather than the pre-replication object.
    fresh = Path.join(root, "after.db")
    assert {:ok, _} = Storage.pull(id, fresh)
    assert rows_in(fresh) == Enum.to_list(1..12)
  end

  test "the promoted shard has no WAL left and passes quick_check", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    :ok = replicate_then_abandon_primary(id, followers, 4)
    [{name, _} | _] = followers

    assert {:ok, _} = Promote.promote(id, follower: name)

    path = Fathom.Shard.db_path(id)

    # The replica arrived as raw byte ranges appended to a WAL by a process that never opened the
    # database. The checkpoint is what turns that into a standalone file, and quick_check is the
    # first time anything has asked SQLite whether the bytes make sense.
    assert Fathom.Shard.verify_integrity(path) == :ok
    refute File.exists?(path <> "-wal"), "the WAL was not folded in; offsets still dangle"
  end

  test "the replica is released, not kept, so the shard can be opened normally afterwards", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    :ok = replicate_then_abandon_primary(id, followers, 3)
    [{name, _} | _] = followers

    assert {:ok, _} = Promote.promote(id, follower: name)

    # The follower must stop believing it replicates this shard, or a push from the DEPOSED
    # primary would be measured against a position that no longer describes anything.
    assert is_nil(Follower.state_of(name, id))
    refute File.exists?(Follower.db_path(name, id))

    # And the lease was handed back: an ordinary checkout takes the shard and reads the promoted
    # rows. A lease held by something that is not a coordinator renews nothing yet still reads as
    # a live owner, so it would lock the shard out until it expired.
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    assert rows_in(path) == [1, 2, 3]
  end

  test "refuses to promote a shard a live node still owns", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    :ok = replicate_then_abandon_primary(id, followers, 2)
    [{name, _} | _] = followers

    # Somebody else holds the lease and is alive. Two primaries for one shard is the one outcome
    # worse than a stale one, so this must refuse rather than steal.
    {:ok, other} = Storage.acquire_lease(id, "someone-else#1", 60_000)
    on_exit(fn -> Storage.release_lease(id, other) end)

    assert {:error, {:lease_held, _}} = Promote.promote(id, follower: name)

    # And it changed nothing on the way to refusing.
    refute is_nil(Follower.state_of(name, id))
  end

  test "refuses when there is no replica to promote", ctx do
    %{id: id, root: root} = ctx
    [{name, _} | _] = start_followers!(root, 1)

    # Distinct from a failure: a caller sweeping shards needs "not here" to differ from "broken".
    assert Promote.promote(id, follower: name) == {:error, :no_replica}
  end

  test "refuses while this node is still serving the shard", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    :ok = replicate_then_abandon_primary(id, followers, 2)
    [{name, _} | _] = followers

    # A running coordinator owns these files and renews its own lease, so the acquire below would
    # succeed on the same owner string and notice nothing — promotion would be copying over a live
    # writer's database.
    {:ok, coordinator, ref, _path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)

    assert Promote.promote(id, follower: name) == {:error, :coordinator_running}
  end

  test "a failed promotion does not leak the lease", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    :ok = replicate_then_abandon_primary(id, followers, 2)
    [{name, _} | _] = followers

    # Corrupt the replica so `checkpoint_and_verify` fails after the lease has been taken. This is
    # the shape of every lock leak AGENTS.md records in the coordinator: an error path between
    # acquire and release. The `after` is what makes it impossible rather than unlikely.
    #
    # The WAL must be emptied as well, and finding out why cost a debugging round: with the real
    # replicated `-wal` still in place, SQLite RECOVERED the corrupt base from it and promotion
    # succeeded. The WAL carries page 1, so it can rebuild a database whose main file is garbage.
    # Good behaviour, useless fixture — the first version of this test corrupted only the `.db`
    # and proved nothing.
    File.write!(Follower.db_path(name, id), :crypto.strong_rand_bytes(8_192))
    File.write!(Follower.wal_path(name, id), "")

    assert {:error, {:open_failed, _}} = Promote.promote(id, follower: name)

    # Provable only from a FOREIGN owner's view: a same-node acquire would silently reclaim its own
    # lock and report success whether or not the lease leaked.
    assert {:ok, lease} = Storage.acquire_lease(id, "verifier#1", 10_000)
    Storage.release_lease(id, lease)
  end
end
