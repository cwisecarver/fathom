defmodule Fathom.Shard.ReplicationSeedChunkTest do
  @moduledoc """
  Streamed seeding — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  A seed is a whole tenant database, so it travels as `seed_begin` + N `seed_chunk`s + `seed_end`
  rather than one frame. Chunking bounds memory, but the property actually worth testing is what
  happens when a stream does **not** complete.

  **A partial seed must never be installed.** A database missing pages opens cleanly, passes a
  casual look, and is missing rows — the exact silent corruption the whole module is built to
  avoid. So bytes land in `.seeding` temp files and are installed by rename only on `seed_end`,
  and every incomplete path below is asserted to leave the follower reporting `:unknown_shard`,
  which is what gets it re-seeded.

  These drive the wire protocol directly against a real `Follower` over TCP, because a truncated
  or reordered stream is not something `Session` can be asked to produce — it always sends a
  complete one.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.SeedBegin

  setup do
    root = Path.join(System.tmp_dir!(), "seedchunk_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    name = :"chunk_f_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: root}, id: name)
    {:ok, port} = Follower.port(pid)

    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false, nodelay: true])

    on_exit(fn -> :gen_tcp.close(sock) end)

    id = "seedchunk_#{System.unique_integer([:positive])}"
    %{name: name, sock: sock, id: id, dir: root}
  end

  defp send_frame(sock, iodata), do: :ok = :gen_tcp.send(sock, iodata)

  defp recv_reply(sock) do
    {:ok, bytes} = :gen_tcp.recv(sock, 0, 5_000)
    {:ok, decoded} = Protocol.decode(bytes)
    decoded
  end

  defp begin_frame(id, db_size, wal_size) do
    Protocol.encode_seed_begin(%SeedBegin{
      salt1: 0,
      shard_id: id,
      epoch: 7,
      wal_gen: 3,
      wal_offset: wal_size,
      db_size: db_size,
      wal_size: wal_size
    })
  end

  # Split into chunks the way the sender does, so "more than one chunk" is a fact of the fixture
  # rather than something asserted about an implementation detail.
  defp chunks(bin, size) do
    bin |> :binary.bin_to_list() |> Enum.chunk_every(size) |> Enum.map(&:binary.list_to_bin/1)
  end

  defp stream(sock, id, part, bin, chunk_size) do
    bin
    |> chunks(chunk_size)
    |> Enum.with_index()
    |> Enum.each(fn {c, seq} ->
      send_frame(sock, Protocol.encode_seed_chunk(id, part, seq, c))
    end)
  end

  defp installed?(name, id) do
    File.exists?(Follower.db_path(name, id)) or File.exists?(Follower.wal_path(name, id))
  end

  defp leftover_temps(dir) do
    dir |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".seeding"))
  end

  test "a multi-chunk seed installs byte-identical files and starts the shard following", ctx do
    %{name: name, sock: sock, id: id} = ctx

    db = :crypto.strong_rand_bytes(10_000)
    wal = :crypto.strong_rand_bytes(3_000)

    send_frame(sock, begin_frame(id, byte_size(db), byte_size(wal)))
    stream(sock, id, :db, db, 1024)
    stream(sock, id, :wal, wal, 1024)
    send_frame(sock, Protocol.encode_seed_end(id))

    assert {:ack, ^id, 3_000} = recv_reply(sock)

    assert File.read!(Follower.db_path(name, id)) == db
    assert File.read!(Follower.wal_path(name, id)) == wal
    assert %{epoch: 7, wal_gen: 3, next_offset: 3_000} = Follower.state_of(name, id)
  end

  test "nothing is installed until seed_end arrives", ctx do
    %{name: name, sock: sock, id: id, dir: dir} = ctx

    db = :crypto.strong_rand_bytes(8_000)

    send_frame(sock, begin_frame(id, byte_size(db), 0))
    stream(sock, id, :db, db, 1024)

    # Every byte of the database has arrived and the seed is still not committed. Installing early
    # is the tempting simplification and it is what makes an interrupted seed indistinguishable
    # from a complete one.
    refute installed?(name, id)
    assert is_nil(Follower.state_of(name, id))

    send_frame(sock, Protocol.encode_seed_end(id))
    assert {:ack, ^id, 0} = recv_reply(sock)
    assert File.read!(Follower.db_path(name, id)) == db
    assert leftover_temps(dir) == []
  end

  test "a missing middle chunk is refused and installs nothing", ctx do
    %{name: name, sock: sock, id: id, dir: dir} = ctx

    db = :crypto.strong_rand_bytes(4_096)
    [c0, _dropped, c2, c3] = chunks(db, 1024)

    # Skip seq 1 entirely. The bytes that follow are perfectly good bytes at the wrong position,
    # which is precisely how a database ends up with a hole that nothing errors on.
    send_frame(sock, begin_frame(id, byte_size(db), 0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 0, c0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 2, c2))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 3, c3))
    send_frame(sock, Protocol.encode_seed_end(id))

    assert {:reject, ^id, :internal, 0} = recv_reply(sock)
    refute installed?(name, id)
    assert is_nil(Follower.state_of(name, id))
    assert leftover_temps(dir) == []
  end

  # The one above is caught by the DECLARED SIZE (three chunks of 1024 against 4096), so it does
  # not isolate the sequence number — verified by deleting the sequence check and watching it
  # still pass. This one does: every byte arrives and the total is exactly right, so only the
  # sequence can tell that two chunks landed transposed. Without the check the follower installs a
  # database with two pages swapped, which opens cleanly and reads wrong.
  test "chunks that arrive out of order are refused even when the total size is right", ctx do
    %{name: name, sock: sock, id: id, dir: dir} = ctx

    db = :crypto.strong_rand_bytes(4_096)
    [c0, c1, c2, c3] = chunks(db, 1024)

    send_frame(sock, begin_frame(id, byte_size(db), 0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 0, c0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 2, c2))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 1, c1))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 3, c3))
    send_frame(sock, Protocol.encode_seed_end(id))

    assert {:reject, ^id, :internal, 0} = recv_reply(sock)
    refute installed?(name, id)
    assert leftover_temps(dir) == []
  end

  test "a stream that ends short of its declared size is refused", ctx do
    %{name: name, sock: sock, id: id, dir: dir} = ctx

    db = :crypto.strong_rand_bytes(4_096)
    [c0, c1 | _] = chunks(db, 1024)

    # Declares 4096 bytes, sends 2048. Every chunk is in order and valid; only the total is wrong,
    # so the size declared up front is the only thing that can catch it.
    send_frame(sock, begin_frame(id, byte_size(db), 0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 0, c0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 1, c1))
    send_frame(sock, Protocol.encode_seed_end(id))

    assert {:reject, ^id, :internal, 0} = recv_reply(sock)
    refute installed?(name, id)
    assert leftover_temps(dir) == []
  end

  test "WAL bytes before the database is complete are refused", ctx do
    %{name: name, sock: sock, id: id} = ctx

    db = :crypto.strong_rand_bytes(2_048)
    wal = :crypto.strong_rand_bytes(512)

    send_frame(sock, begin_frame(id, byte_size(db), byte_size(wal)))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 0, binary_part(db, 0, 1024)))
    # Interleaving the halves would make `db_written` stop meaning what seed_end checks.
    send_frame(sock, Protocol.encode_seed_chunk(id, :wal, 0, wal))
    send_frame(sock, Protocol.encode_seed_end(id))

    assert {:reject, ^id, :internal, 0} = recv_reply(sock)
    refute installed?(name, id)
  end

  test "an aborted seed is discarded and answered", ctx do
    %{name: name, sock: sock, id: id, dir: dir} = ctx

    db = :crypto.strong_rand_bytes(4_096)

    send_frame(sock, begin_frame(id, byte_size(db), 0))
    stream(sock, id, :db, db, 1024)
    # The primary found a checkpoint had rebuilt the .db under it, so the two halves no longer
    # belong together. The abort both frees the follower and unblocks the sender, which would
    # otherwise wait out its 30 s seed timeout.
    send_frame(sock, Protocol.encode_seed_abort(id))

    assert {:reject, ^id, :internal, 0} = recv_reply(sock)
    refute installed?(name, id)
    assert leftover_temps(dir) == []
  end

  test "a connection that drops mid-seed leaves no temp files behind", ctx do
    %{name: name, sock: sock, id: id, dir: dir} = ctx

    db = :crypto.strong_rand_bytes(4_096)

    send_frame(sock, begin_frame(id, byte_size(db), 0))
    stream(sock, id, :db, db, 1024)
    :gen_tcp.close(sock)

    # The handler Task notices the closed socket and drops the partial seed. Polled rather than
    # slept on: the close and the cleanup are in different processes.
    deadline = System.monotonic_time(:millisecond) + 5_000

    Stream.repeatedly(fn ->
      if leftover_temps(dir) == [], do: :clean, else: Process.sleep(10)
    end)
    |> Enum.find(fn
      :clean -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :clean -> :ok
      _ -> flunk("partial seed temps survived the connection: #{inspect(leftover_temps(dir))}")
    end

    refute installed?(name, id)
    assert is_nil(Follower.state_of(name, id))
  end

  test "a second seed_begin replaces the first without leaking its temps", ctx do
    %{name: name, sock: sock, id: id, dir: dir} = ctx

    abandoned = :crypto.strong_rand_bytes(2_048)
    real = :crypto.strong_rand_bytes(1_024)

    send_frame(sock, begin_frame(id, byte_size(abandoned), 0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 0, abandoned))

    send_frame(sock, begin_frame(id, byte_size(real), 0))
    send_frame(sock, Protocol.encode_seed_chunk(id, :db, 0, real))
    send_frame(sock, Protocol.encode_seed_end(id))

    assert {:ack, ^id, 0} = recv_reply(sock)
    assert File.read!(Follower.db_path(name, id)) == real
    assert leftover_temps(dir) == []
  end
end
