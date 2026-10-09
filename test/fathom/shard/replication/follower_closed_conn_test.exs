defmodule Fathom.Shard.Replication.FollowerClosedConnTest do
  @moduledoc """
  Expert review 2026-10-08 #5: a closed connection's workers must not keep applying frames.

  `close_connection/1` stops each worker with a `:stop` message, but `:stop` lands BEHIND every
  frame already queued in that worker's mailbox (up to `@max_outstanding_bytes` of them), and the
  worker took messages in order — so it applied all of them first, for up to `@worker_stop_ms`,
  while the primary had already reconnected and was re-sending the same shards to a NEW
  connection's worker. Nothing serializes one shard across two connections, so the old worker could
  truncate a WAL the new one had just appended to and acked.

  The invariant pinned here: once the connection is marked closed, a worker applies NO further
  frame — it neither runs one nor reports one done. This does not cover the single frame a worker
  may already be executing when the flag is set; the cross-connection serialization that would is
  parked in the review's progress file.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.{Follower, Protocol}

  # A socket that is already closed: replies fail, which `reply/2` ignores — the worker's only
  # observable output here is the `{:frame_done, _}` it sends the reader for each frame it ran.
  defp dead_socket do
    {:ok, l} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, s} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    :gen_tcp.close(s)
    :gen_tcp.close(l)
    s
  end

  defp push(n) do
    %Protocol.Push{
      shard_id: "closedconn_#{n}",
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      offset: 0,
      payload: <<>>
    }
  end

  # No follower is running under this name, so every frame meets a missing ETS table and is
  # REFUSED — which still counts as handled (the worker reports it done). That keeps the test off
  # the disk while still distinguishing "ran the frame" from "dropped it".
  @name :"closedconn_follower_#{System.unique_integer([:positive])}"

  test "a worker runs its frames while the connection is open" do
    closed = :atomics.new(1, [])
    worker = Follower.spawn_worker(dead_socket(), @name, nil, self(), closed)
    Process.unlink(worker)

    for n <- 1..3, do: send(worker, {:frame, push(n), 10})
    for _ <- 1..3, do: assert_receive({:frame_done, 10}, 2_000)
  end

  test "frames queued when the connection closes are dropped, not applied" do
    closed = :atomics.new(1, [])
    worker = Follower.spawn_worker(dead_socket(), @name, nil, self(), closed)
    Process.unlink(worker)
    ref = Process.monitor(worker)

    # Suspend it so the frames genuinely QUEUE, then close the connection the way
    # `close_connection/1` does: the flag first, then `:stop` behind the queued frames.
    :erlang.suspend_process(worker)
    for n <- 1..50, do: send(worker, {:frame, push(n), 10})
    :atomics.put(closed, 1, 1)
    send(worker, :stop)
    :erlang.resume_process(worker)

    assert_receive {:DOWN, ^ref, :process, ^worker, :normal}, 5_000

    refute_received {:frame_done, _},
                    "the worker applied frames queued on a connection that had already closed"
  end
end
