defmodule Fathom.Shard.Replication.FollowerAppendMissingWalTest do
  @moduledoc """
  Expert review 2026-10-08 #21: an append must never create the replica's WAL.

  An append extends a WAL the replica already holds. When that WAL is gone — a worker killed
  mid-install between removing the old WAL and renaming the new one in, or a `forget/2` racing an
  in-flight push — opening for write CREATED it, the pwrite at `offset > 0` left that many zero bytes
  in front (an invalid header SQLite ignores outright), and the state advanced `torn: false`: a
  replica that read as current and promotable while serving its `.db` alone. After a `forget`, it
  also put the state row and a WAL back for a shard the node had dropped.

  These drive a real follower worker (`Follower.spawn_worker/5`) over a socket pair and read the
  reply the primary would see.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Follower, Protocol}

  setup do
    name = :"appendwal_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    on_exit(fn -> File.rm_rf(dir) end)

    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)
    on_exit(fn -> Process.exit(worker, :kill) end)

    %{
      name: name,
      client: client,
      worker: worker,
      id: "appendwal_#{System.unique_integer([:positive])}"
    }
  end

  defp push(id, offset) do
    %Protocol.Push{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      offset: offset,
      payload: "frames"
    }
  end

  defp send_push(%{worker: worker, client: client}, push) do
    send(worker, {:frame, push, 10})
    assert_receive {:frame_done, 10}, 2_000
    {:ok, reply} = :gen_tcp.recv(client, 0, 2_000)
    {:ok, decoded} = Protocol.decode(reply)
    decoded
  end

  test "an append onto a WAL that is gone marks the replica torn and asks for a re-seed", ctx do
    %{name: name, id: id} = ctx
    :ok = Follower.seed(name, id, 1, 0, 0, 4096)
    File.write!(Follower.db_path(name, id), "the replica's database")
    wal = Follower.wal_path(name, id)
    refute File.exists?(wal), "fixture: the WAL must be missing"

    assert {:reject, ^id, :unknown_shard, _} = send_push(ctx, push(id, 4096))

    refute File.exists?(wal), "the append created a WAL with #{4096} zero bytes in front"
    assert %{torn: true} = Follower.state_of(name, id)
    assert File.exists?(Follower.torn_path(name, id)), "the torn mark is not durable"
  end

  # A GUARD, NOT A REGRESSION TEST: this passes against the unfixed follower too, because a push that
  # ARRIVES after `forget/2` is already refused by `FollowerLog.decide/2` (no state). The race the
  # review named — `forget` landing between that decision and the write — is not reproduced here; what
  # closes it is `append_precondition/3` re-reading the state at write time and `held_fd/3` refusing a
  # WAL whose inode changed under the open. This pins that the outcome stays "nothing written back".
  test "an append for a shard forgotten mid-flight writes nothing back", ctx do
    %{name: name, id: id} = ctx
    :ok = Follower.seed(name, id, 1, 0, 0, 4096)
    :ok = Follower.forget(name, id)

    assert {:reject, ^id, :unknown_shard, _} = send_push(ctx, push(id, 4096))

    assert Follower.state_of(name, id) == nil, "the push put a forgotten shard's state back"
    refute File.exists?(Follower.wal_path(name, id))
    refute File.exists?(Follower.torn_path(name, id))
  end

  # Expert review 2026-10-08 #28: the held-fd cache is an fd budget that multiplies across workers
  # and peers, so it is configurable — and 0 must really turn it off, not leave fds held.
  test "with the held-fd cache off, appends still land and no WAL fd is held", ctx do
    %{name: name, id: id, worker: worker} = ctx
    prev = Application.get_env(:fathom, :replication_held_wal_fds)
    Application.put_env(:fathom, :replication_held_wal_fds, 0)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :replication_held_wal_fds, prev),
        else: Application.delete_env(:fathom, :replication_held_wal_fds)
    end)

    :ok = Follower.seed(name, id, 1, 0, 0, 0)
    assert {:ack, ^id, 6} = send_push(ctx, push(id, 0))
    assert {:ack, ^id, 12} = send_push(ctx, %{push(id, 6) | payload: "more!!"})
    assert File.read!(Follower.wal_path(name, id)) == "framesmore!!"

    {:dictionary, dict} = Process.info(worker, :dictionary)

    held =
      Enum.find_value(dict, %{}, fn
        {{Follower, :held_fds}, h} -> h
        _ -> nil
      end)

    assert held == %{}, "the worker still holds #{map_size(held)} WAL fd(s) with the cache off"
  end

  test "an append at offset 0 still starts a WAL (it carries the header)", ctx do
    %{name: name, id: id} = ctx
    :ok = Follower.seed(name, id, 1, 0, 0, 0)

    assert {:ack, ^id, 6} = send_push(ctx, push(id, 0))
    assert File.read!(Follower.wal_path(name, id)) == "frames"
  end
end
