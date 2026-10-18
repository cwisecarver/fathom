defmodule Fathom.Shard.Replication.FollowerResetOffloadTest do
  @moduledoc """
  Expert review 2026-10-17 #5: a WAL-reset push (offset 0) must not head-of-line block the OTHER
  shards that hash to the same follower worker.

  A reset absorbs the outgoing WAL into the `.db` with `synchronous=FULL` inline before its ack,
  and the worker serves 1/8 of the shards, so every one of them waited out the fsyncs. Offset-0
  pushes now run in a per-shard task; the worker keeps serving other shards, and later frames for
  the SAME shard queue behind the task so per-shard order (which `FollowerLog.decide/2` depends on)
  is unchanged.

  The slow step is simulated deterministically by holding the shard's replica lock from the test
  process: the task blocks in `lock_shard/3` exactly where it would block in the absorb, and a
  worker that ran the push inline would sit there for the whole lock wait (set to 5 s here).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Follower, Protocol}

  setup do
    name = :"resetoffload_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    on_exit(fn -> File.rm_rf(dir) end)

    prior = Application.get_env(:fathom, :replication_shard_lock_wait_ms)
    Application.put_env(:fathom, :replication_shard_lock_wait_ms, 5_000)

    on_exit(fn ->
      if prior,
        do: Application.put_env(:fathom, :replication_shard_lock_wait_ms, prior),
        else: Application.delete_env(:fathom, :replication_shard_lock_wait_ms)
    end)

    uniq = System.unique_integer([:positive])
    %{name: name, a: "resetoffload_a#{uniq}", b: "resetoffload_b#{uniq}"}
  end

  defp connection(name) do
    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)
    on_exit(fn -> Process.exit(worker, :kill) end)
    %{worker: worker, client: client}
  end

  defp reply(%{client: client}, timeout) do
    {:ok, bytes} = :gen_tcp.recv(client, 0, timeout)
    {:ok, decoded} = Protocol.decode(bytes)
    decoded
  end

  defp push(id, offset, payload) do
    %Protocol.Push{shard_id: id, epoch: 1, wal_gen: 0, salt1: 0, offset: offset, payload: payload}
  end

  defp existing_replica(name, id) do
    File.write!(Follower.db_path(name, id), "the existing db")
    File.write!(Follower.wal_path(name, id), "")
    :ok = Follower.seed(name, id, 1, 0, 0, 0)
  end

  defp await_link(worker) do
    case Process.info(worker, :links) do
      {:links, [_ | _]} ->
        :ok

      _ ->
        receive do
        after
          2 -> await_link(worker)
        end
    end
  end

  test "an offset-0 push stuck in its step does not block another shard on the same worker",
       ctx do
    %{name: name, a: a, b: b} = ctx
    existing_replica(name, a)
    existing_replica(name, b)
    conn = connection(name)

    # Shard A's replica is busy (stands in for a slow absorb).
    {:ok, lock} = Follower.lock_shard(name, a, 0)

    send(conn.worker, {:frame, push(a, 0, "frames-a"), 1})
    send(conn.worker, {:frame, push(b, 0, "frames-b"), 1})

    # B is answered while A is still stuck. Run inline, the worker would sit in A's lock wait (5 s).
    assert {:ack, ^b, 8} = reply(conn, 1_000)

    # Per-shard order survives: a LATER frame for A queues behind the stuck one rather than racing
    # it (run immediately it would be refused at offset 8 against an unwritten WAL).
    send(conn.worker, {:frame, push(a, 8, "more"), 1})

    Follower.unlock_shard(name, lock)

    assert {:ack, ^a, 8} = reply(conn, 5_000)
    assert {:ack, ^a, 12} = reply(conn, 5_000)
    assert File.read!(Follower.wal_path(name, a)) == "frames-amore"
  end

  test "a stopping worker finishes the offset-0 push it is in the middle of, then exits", ctx do
    %{name: name, a: a} = ctx
    existing_replica(name, a)
    closed = :atomics.new(1, [])
    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), closed)
    Process.unlink(worker)
    ref = Process.monitor(worker)

    {:ok, lock} = Follower.lock_shard(name, a, 0)
    send(worker, {:frame, push(a, 0, "frames"), 7})
    # The worker has taken the frame once it is linked to the task it spawned for it.
    await_link(worker)
    :atomics.put(closed, 1, 1)
    send(worker, :stop)

    # The worker waits for the frame its task is in the middle of.
    refute_receive {:DOWN, ^ref, _, _, _}, 100
    Follower.unlock_shard(name, lock)
    assert_receive {:DOWN, ^ref, :process, ^worker, :normal}, 5_000
    assert_received {:frame_done, 7}

    {:ok, bytes} = :gen_tcp.recv(client, 0, 1_000)
    assert {:ok, {:ack, ^a, 6}} = Protocol.decode(bytes)
  end
end
