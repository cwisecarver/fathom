defmodule Fathom.Shard.ReplicationConnNonceTest do
  @moduledoc """
  Per-connection replay binding on the replication port (expert review 2026-09-29 #17, option B as
  decided 2026-09-29).

  Before it, a signed frame carried no nonce, timestamp or connection binding, and a seed chunk's
  MAC covered only its header. So a passive observer who recorded one signed frame could REPLAY it
  over a socket of its own, and could stream substituted seed bytes under a recorded chunk header.

  With `:replication_conn_nonce` on, each connection opens with a `hello` exchange and every MAC
  covers the nonce the RECEIVER generated for that connection. These tests drive a real `Follower`
  listener over real sockets:

    * a frame accepted on connection 1 is REFUSED when replayed on connection 2 — and the same
      replay is ACCEPTED with the switch off, which is the gap this closes, pinned in-suite;
    * a seed chunk whose payload was altered is refused (its payload is covered now);
    * a peer that skips the handshake is closed before any frame is interpreted;
    * a real `Shipper` still pushes and gets acked with the switch on (the three connection
      owners — Follower, Shipper, Recovery — all run the handshake).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{FrameAuth, Follower, Protocol, Shipper}
  alias Fathom.Shard.Replication.Protocol.Push

  @secret "a-fleet-shared-secret-long-enough-to-be-real"
  @keys [
    :replication_sign_frames,
    :replication_hmac_required,
    :replication_hmac_secret,
    :replication_conn_nonce,
    :replication_dir
  ]

  setup do
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})
    root = Path.join(System.tmp_dir!(), "connnonce_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      FrameAuth.forget_key()
      File.rm_rf(root)
    end)

    Application.put_env(:fathom, :replication_hmac_secret, @secret)
    Application.put_env(:fathom, :replication_sign_frames, true)
    Application.put_env(:fathom, :replication_hmac_required, true)
    Application.put_env(:fathom, :replication_conn_nonce, true)
    Application.put_env(:fathom, :replication_dir, root)
    FrameAuth.forget_key()

    name = :"connnonce_f#{System.unique_integer([:positive])}"

    pid =
      start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f")}, id: name)

    {:ok, port} = Follower.port(pid)
    %{name: name, port: port}
  end

  defp dial(port) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 2_000)

    sock
  end

  # A connection that has run the handshake in THIS process, which is where the binding lives.
  defp dial_bound(port) do
    sock = dial(port)
    :ok = Protocol.handshake_connect(sock, 2_000)
    sock
  end

  defp push(shard),
    do: %Push{shard_id: shard, epoch: 1, wal_gen: 1, salt1: 0, offset: 0, payload: "x"}

  test "a frame recorded on one connection is refused when replayed on another", %{port: port} do
    c1 = dial_bound(port)
    recorded = IO.iodata_to_binary(Protocol.encode_push(push("acme")))
    :ok = :gen_tcp.send(c1, recorded)

    assert {:ok, reply} = :gen_tcp.recv(c1, 0, 2_000),
           "the frame was not accepted on its own connection"

    assert {:ok, {:reject, "acme", :unknown_shard, _}} = Protocol.decode(reply)
    :gen_tcp.close(c1)

    c2 = dial_bound(port)
    :ok = :gen_tcp.send(c2, recorded)
    assert {:error, :closed} = :gen_tcp.recv(c2, 0, 2_000), "the replayed frame was accepted"
  end

  test "with the switch OFF the same replay is accepted — the gap the handshake closes", %{
    name: name
  } do
    Application.put_env(:fathom, :replication_conn_nonce, false)
    stop_supervised!(name)

    pid =
      start_supervised!(
        {Follower, name: name, port: 0, dir: Path.join(System.tmp_dir!(), "#{name}_off")},
        id: name
      )

    {:ok, port} = Follower.port(pid)

    c1 = dial_bound(port)
    recorded = IO.iodata_to_binary(Protocol.encode_push(push("acme")))
    :ok = :gen_tcp.send(c1, recorded)
    assert {:ok, _} = :gen_tcp.recv(c1, 0, 2_000)
    :gen_tcp.close(c1)

    c2 = dial_bound(port)
    :ok = :gen_tcp.send(c2, recorded)
    assert {:ok, _} = :gen_tcp.recv(c2, 0, 2_000)
  end

  test "a seed chunk with an altered payload is refused", %{port: port} do
    sock = dial_bound(port)

    begin = %Protocol.SeedBegin{
      shard_id: "seedy",
      epoch: 1,
      wal_gen: 1,
      salt1: 0,
      wal_offset: 0,
      db_size: 8,
      wal_size: 0
    }

    :ok = :gen_tcp.send(sock, Protocol.encode_seed_begin(begin))
    chunk = IO.iodata_to_binary(Protocol.encode_seed_chunk("seedy", :db, 0, "AAAAAAAA"))
    last = byte_size(chunk) - 1
    <<pre::binary-size(^last), byte::8>> = chunk
    :ok = :gen_tcp.send(sock, <<pre::binary, Bitwise.bxor(byte, 1)::8>>)

    assert {:error, :closed} = :gen_tcp.recv(sock, 0, 2_000),
           "a seed chunk with substituted bytes was accepted"
  end

  test "an untouched seed chunk is accepted (control for the test above)", %{port: port} do
    sock = dial_bound(port)

    begin = %Protocol.SeedBegin{
      shard_id: "seedy",
      epoch: 1,
      wal_gen: 1,
      salt1: 0,
      wal_offset: 0,
      db_size: 8,
      wal_size: 0
    }

    :ok = :gen_tcp.send(sock, Protocol.encode_seed_begin(begin))
    :ok = :gen_tcp.send(sock, Protocol.encode_seed_chunk("seedy", :db, 0, "AAAAAAAA"))
    # A chunk has no reply; an abort does, which proves the connection survived the chunk.
    :ok = :gen_tcp.send(sock, Protocol.encode_seed_abort("seedy"))
    assert {:ok, reply} = :gen_tcp.recv(sock, 0, 2_000)
    assert {:ok, {:reject, "seedy", :internal, 0}} = Protocol.decode(reply)
  end

  test "a peer that skips the handshake is closed before any frame is interpreted", %{port: port} do
    sock = dial(port)
    # Drain the follower's hello, then send a (signed, unbound) frame instead of our own hello.
    assert {:ok, _hello} = :gen_tcp.recv(sock, 0, 2_000)
    :ok = :gen_tcp.send(sock, Protocol.encode_push(push("acme")))
    assert {:error, :closed} = :gen_tcp.recv(sock, 0, 2_000)
  end

  test "a real Shipper completes the handshake and gets its push acked", %{name: name, port: port} do
    ship = :"connnonce_s#{System.unique_integer([:positive])}"
    pid = start_supervised!({Shipper, name: ship, host: ~c"127.0.0.1", port: port}, id: ship)
    assert Shipper.connected?(pid)
    Follower.seed(name, "acme", 1, 1, 0, 0)

    Shipper.push(pid, %Push{push("acme") | payload: :binary.copy(<<0xCD>>, 512)})
    assert_receive {:repl_reply, ^pid, {:ack, "acme", 512}}, 2_000
  end
end
