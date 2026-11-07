defmodule Fathom.Shard.Replication.FollowerHeldFdReleaseTest do
  @moduledoc """
  Expert review 2026-11-07 #71: the follower's held WAL file descriptors must not pin an UNLINKED
  inode.

  A worker keeps a raw fd on each `<id>.db-wal` it appends to (`Follower.write_held/4`), checked
  against the path's inode only on the NEXT append for that path. Until then it pins whatever inode it
  was opened on: a reset's absorb unlinks the old `-wal` (SQLite deletes it when the last connection
  closes), a `forget/2` (tenant erase, promotion) unlinks the replica, a seed install renames a new
  one in. `df` then counts bytes `du` cannot find, and a deleted tenant's WAL bytes stay reachable
  through the BEAM's fd for as long as the connection lives. The fd belongs to the worker (the raw file
  is owned by the process that opened it), so only the worker can close it.

  What is pinned:

    * an offset-0 push (the reset) and a seed's end drop the held fd as the worker runs them;
    * an fd unused for `:replication_held_wal_fd_idle_ms` is closed by an idle worker, so a `forget/2`
      — which cannot signal another process — leaves the bytes pinned for the idle window, not forever.

  The held map lives in the worker's process dictionary (`{Follower, :held_fds}`), which is what these
  read; the descriptor itself is not observable portably (no `/proc/self/fd` on macOS).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Follower, Protocol}
  alias Fathom.Test.Await

  setup do
    name = :"heldfd_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)

    prev = Application.get_env(:fathom, :replication_held_wal_fd_idle_ms)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_held_wal_fd_idle_ms),
        else: Application.put_env(:fathom, :replication_held_wal_fd_idle_ms, prev)

      File.rm_rf(dir)
    end)

    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)
    on_exit(fn -> Process.exit(worker, :kill) end)

    id = "heldfd_#{System.unique_integer([:positive])}"
    # A replica with a 6-byte WAL and no `.db` (nothing for a reset to absorb).
    File.write!(Follower.wal_path(name, id), "frames")
    :ok = Follower.seed(name, id, 1, 0, 0, 6)

    %{name: name, client: client, worker: worker, id: id}
  end

  defp push(id, offset, payload, salt1 \\ 0) do
    %Protocol.Push{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: salt1,
      offset: offset,
      payload: payload
    }
  end

  defp send_frame(%{worker: worker, client: client}, frame) do
    send(worker, {:frame, frame, 10})
    assert_receive {:frame_done, 10}, 5_000
    {:ok, reply} = :gen_tcp.recv(client, 0, 5_000)
    {:ok, decoded} = Protocol.decode(reply)
    decoded
  end

  defp held_paths(worker) do
    {:dictionary, dict} = Process.info(worker, :dictionary)

    case List.keyfind(dict, {Follower, :held_fds}, 0) do
      {_key, held} -> Map.keys(held)
      nil -> []
    end
  end

  # Appends once, so the worker holds an fd on the WAL; asserts it does (the fixture).
  defp hold_wal!(ctx) do
    assert {:ack, _, 10} = send_frame(ctx, push(ctx.id, 6, "more"))
    wal = Follower.wal_path(ctx.name, ctx.id)
    assert wal in held_paths(ctx.worker), "fixture: the worker must hold an fd on the WAL"
    wal
  end

  test "a reset (an offset-0 push) drops the held fd instead of leaving it on the old inode",
       ctx do
    wal = hold_wal!(ctx)

    assert {:ack, _, _} = send_frame(ctx, push(ctx.id, 0, "new-generation", 12_345))

    refute wal in held_paths(ctx.worker),
           "the reset left the worker's fd on the outgoing WAL's inode"
  end

  test "a seed's end drops the held fd (the install renames a new WAL into place)", ctx do
    wal = hold_wal!(ctx)

    # No seed is in flight, so the follower refuses it; the drop happens as the worker runs the frame.
    assert {:reject, _, :internal, _} = send_frame(ctx, {:seed_end, ctx.id})

    refute wal in held_paths(ctx.worker), "a seed install left a held fd on the replaced WAL"
  end

  test "an fd unused past the idle window is closed, so a forget does not pin the WAL forever",
       ctx do
    Application.put_env(:fathom, :replication_held_wal_fd_idle_ms, 20)
    wal = hold_wal!(ctx)

    # The erase unlinks the replica; it cannot reach the worker's descriptor.
    :ok = Follower.forget(ctx.name, ctx.id)
    refute File.exists?(wal)

    Await.until(
      fn -> wal not in held_paths(ctx.worker) end,
      "the idle worker kept an fd on a deleted tenant's WAL"
    )
  end

  test "an fd in active use is NOT swept out from under a shard that keeps writing", ctx do
    Application.put_env(:fathom, :replication_held_wal_fd_idle_ms, 60_000)
    wal = hold_wal!(ctx)

    assert {:ack, _, 14} = send_frame(ctx, push(ctx.id, 10, "more"))
    assert wal in held_paths(ctx.worker), "a live shard lost its held fd"
  end
end
