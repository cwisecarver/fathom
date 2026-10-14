defmodule Fathom.Shard.ReplicationListenerLimitsTest do
  @moduledoc """
  The replication listener bounds what an unauthenticated peer can hold (expert review 2026-10-13
  #31).

  Symptom: `accept_loop` started one handler per connection with no cap, and after the hello the
  handler's `recv(sock, 0)` had no timeout, so a peer that connected and said nothing held a file
  descriptor and a process for ever; enough of them exhausted the node's fds.

  Invariants: (1) a connection that never delivers a valid frame is closed at the first-frame
  deadline; (2) past `:replication_max_connections` a new connection is refused; (3) neither
  affects a peer that is sending valid frames.

  The default first-frame deadline is deliberately long (see `Follower`), so these tests lower it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.Push

  @keys [:replication_first_frame_timeout_ms, :replication_max_connections]

  setup do
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    root = Path.join(System.tmp_dir!(), "listenerlimits_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)
    :ok = Application.put_env(:fathom, :replication_first_frame_timeout_ms, 300)
    :ok = Application.put_env(:fathom, :replication_max_connections, 3)

    name = :"listenerlimits_f#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f")}, id: name)
    {:ok, port} = Follower.port(pid)
    %{name: name, port: port}
  end

  defp dial(port) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 2_000)

    sock
  end

  defp push(shard),
    do: %Push{shard_id: shard, epoch: 1, wal_gen: 1, salt1: 0, offset: 0, payload: "x"}

  test "a connection that never sends a valid frame is closed at the first-frame deadline", ctx do
    idle = dial(ctx.port)

    assert {:error, :closed} = :gen_tcp.recv(idle, 0, 3_000),
           "an idle unauthenticated connection was held open past the first-frame deadline"
  end

  test "a peer sending valid frames is not cut off by the deadline", ctx do
    peer = dial(ctx.port)
    :ok = :gen_tcp.send(peer, Protocol.encode_push(push("acme")))
    assert {:ok, _reject} = :gen_tcp.recv(peer, 0, 2_000)

    # Well past the 300 ms first-frame deadline: an authenticated connection may idle.
    assert {:error, :timeout} = :gen_tcp.recv(peer, 0, 800)
  end

  test "connections past the cap are refused, and a valid peer's connection is unaffected",
       ctx do
    # A long deadline here so the cap, not the deadline, is what closes the extra connection.
    Application.put_env(:fathom, :replication_first_frame_timeout_ms, 60_000)

    peer = dial(ctx.port)
    :ok = :gen_tcp.send(peer, Protocol.encode_push(push("acme")))
    assert {:ok, _reject} = :gen_tcp.recv(peer, 0, 2_000)

    idle = for _ <- 1..2, do: dial(ctx.port)
    # The cap is 3 and three handlers are live (the accept loop is sequential, so each earlier
    # connection was handed off before this one is accepted): a fourth is accepted by TCP, then
    # closed.
    over = dial(ctx.port)

    assert {:error, :closed} = :gen_tcp.recv(over, 0, 2_000),
           "a connection past :replication_max_connections was served"

    # The valid peer still works.
    :ok = :gen_tcp.send(peer, Protocol.encode_push(push("acme")))
    assert {:ok, _reject} = :gen_tcp.recv(peer, 0, 2_000)
    for s <- idle, do: :gen_tcp.close(s)
  end
end
