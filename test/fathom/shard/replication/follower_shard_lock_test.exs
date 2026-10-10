defmodule Fathom.Shard.Replication.FollowerShardLockTest do
  @moduledoc """
  Expert review 2026-10-08 #5 (tiers b and c): one shard's replica is mutated by ONE process at a
  time, node-wide, not merely one per connection.

  A primary that reconnects gets a new connection with new per-shard workers while the old
  connection's workers may still be mid-operation. Nothing serialized the two: a push on the new
  connection was applied — and ACKED — in the middle of a seed streaming on the old one, which then
  installed over it (the ack described bytes the replica no longer held). And seed temps were named
  per shard (`<db>.seeding`), so the second connection's `seed_begin` truncated the inode the first
  seed's fd was still writing: the first seed's remaining chunks landed inside the second's
  INSTALLED replica, and its install then removed the live `-wal` before failing its own rename.

  These drive two real follower workers (`Follower.spawn_worker/5`, i.e. two connections) over
  socket pairs against one real follower and read the replies the primaries would see. A seed is
  the natural "paused mid-operation" mutation: it holds the shard from `seed_begin` to `seed_end`
  across frames, so the test controls exactly where the other connection's frame lands.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Follower, Protocol}

  setup do
    name = :"shardlock_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    on_exit(fn -> File.rm_rf(dir) end)

    %{name: name, dir: dir, id: "shardlock_#{System.unique_integer([:positive])}"}
  end

  # One "connection": a worker plus the client end of its socket. `tag` is the size the worker
  # reports back in `{:frame_done, tag}`, so the test can tell the two connections' progress apart.
  defp connection(name, tag) do
    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)
    on_exit(fn -> Process.exit(worker, :kill) end)
    %{worker: worker, client: client, tag: tag}
  end

  defp frame(%{worker: worker, tag: tag}, frame) do
    send(worker, {:frame, frame, tag})
    assert_receive {:frame_done, ^tag}, 5_000
    :ok
  end

  defp reply(%{client: client}) do
    {:ok, bytes} = :gen_tcp.recv(client, 0, 5_000)
    {:ok, decoded} = Protocol.decode(bytes)
    decoded
  end

  defp seed_begin(id, db, wal) do
    %Protocol.SeedBegin{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      wal_offset: byte_size(wal),
      db_size: byte_size(db),
      wal_size: byte_size(wal),
      lineage: 0
    }
  end

  defp push(id, offset, payload) do
    %Protocol.Push{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      offset: offset,
      payload: payload
    }
  end

  # A replica the follower already holds at offset 0, so a push at 0 is an accepted append.
  defp existing_replica(name, id) do
    File.write!(Follower.db_path(name, id), "the existing db")
    File.write!(Follower.wal_path(name, id), "")
    :ok = Follower.seed(name, id, 1, 0, 0, 0)
  end

  defp seeding_temps(dir), do: dir |> File.ls!() |> Enum.filter(&(&1 =~ ".seeding"))

  test "a push on another connection cannot land in the middle of a seed", ctx do
    %{name: name, id: id} = ctx
    existing_replica(name, id)
    a = connection(name, 11)
    b = connection(name, 22)

    # Connection A is mid-seed: begun, half the database streamed, not ended.
    seed_db = "SEEDED-DATABASE!"
    :ok = frame(a, seed_begin(id, seed_db, "SEEDED-WAL"))
    :ok = frame(a, {:seed_chunk, id, :db, 0, binary_part(seed_db, 0, 8)})

    # Connection B pushes the same shard.
    :ok = frame(b, push(id, 0, "frames-from-b"))

    assert {:reject, ^id, :internal, _} = reply(b),
           "a push was applied and acked while another connection's seed of the same shard was " <>
             "in flight — the seed's install then replaces the bytes that ack described"

    assert File.read!(Follower.wal_path(name, id)) == "",
           "the refused push still wrote into the replica"

    # A finishes undisturbed and is the replica.
    :ok = frame(a, {:seed_chunk, id, :db, 1, binary_part(seed_db, 8, 8)})
    :ok = frame(a, {:seed_chunk, id, :wal, 0, "SEEDED-WAL"})
    :ok = frame(a, {:seed_end, id})
    assert {:ack, ^id, 10} = reply(a)
    assert File.read!(Follower.db_path(name, id)) == seed_db
    assert File.read!(Follower.wal_path(name, id)) == "SEEDED-WAL"

    # And the lock was released: B's retry now applies.
    :ok = frame(b, push(id, 10, "frames-from-b"))
    assert {:ack, ^id, _} = reply(b)
  end

  test "a second connection's seed cannot clobber a seed in flight, even after a lock takeover",
       ctx do
    %{name: name, id: id, dir: dir} = ctx
    existing_replica(name, id)

    # Force the age takeover, so B's seed genuinely overlaps A's still-live one: this is the case
    # the lock alone cannot cover, and what unique per-seed temp names exist for.
    prev = Application.get_env(:fathom, :replication_shard_lock_max_ms)
    Application.put_env(:fathom, :replication_shard_lock_max_ms, 0)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :replication_shard_lock_max_ms, prev),
        else: Application.delete_env(:fathom, :replication_shard_lock_max_ms)
    end)

    a = connection(name, 11)
    b = connection(name, 22)

    a_db = "AAAAAAAAaaaaaaaa"
    b_db = "BBBBBBBBbbbbbbbb"

    :ok = frame(a, seed_begin(id, a_db, "A-WAL"))
    :ok = frame(a, {:seed_chunk, id, :db, 0, binary_part(a_db, 0, 8)})

    # B seeds the same shard completely and is installed.
    :ok = frame(b, seed_begin(id, b_db, "B-WAL"))
    :ok = frame(b, {:seed_chunk, id, :db, 0, b_db})
    :ok = frame(b, {:seed_chunk, id, :wal, 0, "B-WAL"})
    :ok = frame(b, {:seed_end, id})
    assert {:ack, ^id, 5} = reply(b)

    # A resumes. Pre-fix its fd pointed at the inode B renamed into place, and its install removed
    # the live `-wal` before failing its own rename.
    :ok = frame(a, {:seed_chunk, id, :db, 1, binary_part(a_db, 8, 8)})
    :ok = frame(a, {:seed_chunk, id, :wal, 0, "A-WAL"})
    :ok = frame(a, {:seed_end, id})
    assert {:reject, ^id, :internal, _} = reply(a)

    assert File.read!(Follower.db_path(name, id)) == b_db,
           "the overtaken seed's chunks were written into the INSTALLED replica's database"

    assert File.read(Follower.wal_path(name, id)) == {:ok, "B-WAL"},
           "the overtaken seed removed or overwrote the installed replica's WAL"

    assert %{next_offset: 5, torn: false} = Follower.state_of(name, id)
    assert seeding_temps(dir) == [], "a discarded seed left its temps behind"
  end

  # Passes pre-fix trivially (there was no lock to leak). It discriminates the lock's LIVENESS
  # cleanup: with the dead-holder takeover removed from `abandoned_lock?/3` (age only) B's push is
  # refused and this fails — probed 2026-10-09.
  test "a holder that dies does not leak the lock", ctx do
    %{name: name, id: id, dir: dir} = ctx
    existing_replica(name, id)
    a = connection(name, 11)
    b = connection(name, 22)

    :ok = frame(a, seed_begin(id, "0123456789abcdef", ""))
    ref = Process.monitor(a.worker)
    Process.exit(a.worker, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}

    # Killed mid-seed, as `close_connection/1` does after `@worker_stop_ms`: nothing released the
    # lock, and the next connection must still be able to write.
    :ok = frame(b, push(id, 0, "frames-from-b"))
    assert {:ack, ^id, _} = reply(b)

    # Its temps are left for the boot reaper (they are too fresh to reap here).
    assert [_ | _] = seeding_temps(dir)
  end

  test "a follower start reaps stale seed temps and keeps fresh ones", _ctx do
    # Unique temp names mean a killed seed's files are never overwritten by the shard's next seed,
    # so without a reaper they accumulate forever. Age-gated, because a Follower restart does not
    # stop the old connections' workers — a seed still streaming must keep its files.
    name = :"shardlock_reap_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    stale = Path.join(dir, "victim.db.seeding.123")
    legacy = Path.join(dir, "victim.db-wal.seeding")
    fresh = Path.join(dir, "victim.db.seeding.456")
    for p <- [stale, legacy, fresh], do: File.write!(p, "partial")
    an_hour_ago = System.os_time(:second) - 3_600
    for p <- [stale, legacy], do: File.touch!(p, an_hour_ago)

    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)

    refute File.exists?(stale), "a dead seed's temp survived the follower start"
    refute File.exists?(legacy), "a pre-upgrade fixed-name temp survived the follower start"
    assert File.exists?(fresh), "a possibly-live seed's temp was reaped"
  end

  test "single-connection operation is unchanged: a push during this connection's own seed applies",
       ctx do
    # A GUARD, not a regression test (passes pre-fix too): the lock is re-entrant for its holder, so
    # the worker that is streaming a seed still applies a push for the same shard as it always did.
    %{name: name, id: id} = ctx
    existing_replica(name, id)
    a = connection(name, 11)

    :ok = frame(a, seed_begin(id, "0123456789abcdef", ""))
    :ok = frame(a, push(id, 0, "frames"))
    assert {:ack, ^id, 6} = reply(a)
    :ok = frame(a, {:seed_abort, id})
    assert {:reject, ^id, :internal, _} = reply(a)

    # And the abort released the lock: a fresh connection can push.
    b = connection(name, 22)
    :ok = frame(b, push(id, 6, "more"))
    assert {:ack, ^id, 10} = reply(b)
  end
end
