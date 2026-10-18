defmodule Fathom.Shard.Replication.FollowerStatPerPushTest do
  @moduledoc """
  Perf review 2026-10-14 #22: an append push stats the replica WAL ONCE.

  `append_precondition/3` sized the WAL with `File.stat`, and `held_fd/3` then took
  `:file.read_file_info(path, [:raw])` for the inode check: two stats (~13 us each) of the same path
  on the commit path, which waits for this ack. The push now takes one raw `read_file_info` and
  hands it to both. Every verdict is unchanged (`:wal_missing`, `:not_followed`, the refused replaced
  inode), and `follower_append_missing_wal_test.exs` / `wal_held_fd_test.exs` pin those.

  The count is traced on the worker process, so it measures exactly the syscalls the push path makes
  (on the unpatched code the warm count is 2).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Follower, Protocol}

  setup do
    name = :"statpush_#{System.unique_integer([:positive])}"
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

    id = "statpush_#{System.unique_integer([:positive])}"
    :ok = Follower.seed(name, id, 1, 0, 0, 0)
    %{name: name, client: client, worker: worker, id: id}
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

  defp send_push(%{worker: worker, client: client}, push) do
    send(worker, {:frame, push, 10})
    assert_receive {:frame_done, 10}, 2_000
    {:ok, reply} = :gen_tcp.recv(client, 0, 2_000)
    {:ok, decoded} = Protocol.decode(reply)
    decoded
  end

  # Path-level stats of `wal` issued by the worker while `fun` runs — or by the task it spawns for
  # an offset-0 push (`set_on_spawn`; expert review 2026-10-17 #5), which is where a cold append lands.
  defp wal_stats(%{worker: worker, name: name, id: id}, fun) do
    wal = Follower.wal_path(name, id)
    :erlang.trace_pattern({:file, :read_file_info, 2}, true, [:local])
    :erlang.trace_pattern({:file, :read_file_info, 1}, true, [:local])
    :erlang.trace(worker, true, [:call, :set_on_spawn, {:tracer, self()}])
    fun.()
    :erlang.trace(worker, false, [:call])

    receive_all([])
    |> Enum.count(fn
      {:trace, _pid, :call, {:file, :read_file_info, [^wal | _]}} -> true
      _ -> false
    end)
  end

  defp receive_all(acc) do
    receive do
      {:trace, _, _, _} = m -> receive_all([m | acc])
    after
      50 -> acc
    end
  end

  test "a warm append (held fd) stats the WAL once, a cold one once too", ctx do
    %{id: id} = ctx

    cold =
      wal_stats(ctx, fn -> assert {:ack, ^id, 6} = send_push(ctx, push(id, 0, "frames")) end)

    assert cold == 1, "the cold-fd append stat'd the WAL #{cold} times"

    warm =
      wal_stats(ctx, fn -> assert {:ack, ^id, 12} = send_push(ctx, push(id, 6, "more!!")) end)

    assert warm == 1,
           "a warm append stat'd the WAL #{warm} times — the precondition and the held-fd inode " <>
             "check each took their own"

    assert File.read!(Follower.wal_path(ctx.name, id)) == "framesmore!!"
  end

  test "the single stat still feeds the precondition: a WAL shorter than the offset is refused",
       ctx do
    %{id: id, name: name} = ctx
    assert {:ack, ^id, 6} = send_push(ctx, push(id, 0, "frames"))
    # Warm fd now. Cut the WAL under the replica's recorded extent (6): the push's offset is what
    # the state expects, so only the precondition's size check can refuse it.
    File.write!(Follower.wal_path(name, id), "")
    assert {:reject, ^id, :unknown_shard, _} = send_push(ctx, push(id, 6, "more!!"))
    assert %{torn: true} = Follower.state_of(name, id)
    assert File.read!(Follower.wal_path(name, id)) == "", "the refused append wrote bytes"
  end

  test "the single stat still feeds the inode check: a replaced WAL is not written through",
       ctx do
    %{id: id, name: name} = ctx
    wal = Follower.wal_path(name, id)
    assert {:ack, ^id, 6} = send_push(ctx, push(id, 0, "frames"))

    # What a seed install does: a new file renamed over the path. The held fd points at the old one.
    File.write!(wal <> ".new", "FRAMES")
    File.rename!(wal <> ".new", wal)

    assert {:ack, ^id, 12} = send_push(ctx, push(id, 6, "more!!"))
    assert File.read!(wal) == "FRAMESmore!!", "the append went to the unlinked inode"
  end
end
