defmodule Fathom.Shard.Replication.CoordinatedShardFenceTest do
  @moduledoc """
  Expert review 2026-10-10 #1: after a promotion the deposed owner must not be able to re-seed the
  new owner's node and count it toward its quorum.

  Promotion fences the replica (lineage raised, torn) and then `Follower.forget/2` deletes the row
  holding that fence. The deposed owner's next push was `:unknown_shard`, its Session re-seeded,
  and the seed was accepted (`stale_lineage_seed?/2` needs a row to compare with) — so the new
  owner's node acked writes the real owner never sees. The fix: while a coordinator for the shard
  is registered on this node, wire pushes and seeds for it are refused.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Follower, Promote, Protocol}

  setup do
    name = :"coordfence_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    # `coordinator_registry:` is what `Fleet` passes in production.
    start_supervised!(
      {Follower, name: name, port: 0, dir: dir, coordinator_registry: Fathom.ShardRegistry},
      id: name
    )

    on_exit(fn -> File.rm_rf(dir) end)

    id = "coordfence-#{System.unique_integer([:positive])}"

    File.write!(Follower.db_path(name, id), database_with(dir, [1]))
    File.write!(Follower.wal_path(name, id), "")
    :ok = Follower.seed(name, id, 1, 0, 0, 0, 3)

    %{name: name, dir: dir, id: id, temp: Path.join(dir, "staged.db")}
  end

  defp database_with(dir, rows) do
    path = Path.join(dir, "build_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)", [])
    for r <- rows, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [r])
    :ok = Connection.close(conn)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
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

  defp frame(%{worker: worker}, frame) do
    send(worker, {:frame, frame, 7})
    assert_receive {:frame_done, 7}, 5_000
    :ok
  end

  defp reply(%{client: client}) do
    {:ok, bytes} = :gen_tcp.recv(client, 0, 5_000)
    {:ok, decoded} = Protocol.decode(bytes)
    decoded
  end

  defp deposed_push(id),
    do: %Protocol.Push{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      offset: 0,
      payload: "a write the real owner will never see",
      lineage: 3
    }

  defp deposed_seed(id, bytes),
    do: %Protocol.SeedBegin{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      wal_offset: 0,
      db_size: byte_size(bytes),
      wal_size: 0,
      lineage: 3
    }

  # What the cold-open promotion leaves behind: replica staged + fenced, then forgotten, with the
  # new owner's coordinator registered (the Registry entry exists from the coordinator's start).
  defp promote_and_forget(%{name: name, id: id, temp: temp}) do
    assert Promote.stage(name, id, temp, lineage: 5) == :ok
    Follower.forget(name, id)
    assert Follower.state_of(name, id) == nil
    {:ok, _} = Registry.register(Fathom.ShardRegistry, id, :lease_held)
  end

  test "after promotion the deposed owner's push is refused, not answered :unknown_shard", ctx do
    promote_and_forget(ctx)
    zombie = connection(ctx.name)

    :ok = frame(zombie, deposed_push(ctx.id))

    assert {:reject, id, :stale_epoch, 0} = reply(zombie)
    assert id == ctx.id
    assert Follower.state_of(ctx.name, ctx.id) == nil
  end

  test "after promotion the deposed owner cannot re-seed this node", ctx do
    promote_and_forget(ctx)
    zombie = connection(ctx.name)
    bytes = database_with(ctx.dir, [1, 2])

    :ok = frame(zombie, deposed_seed(ctx.id, bytes))
    :ok = frame(zombie, {:seed_chunk, ctx.id, :db, 0, bytes})
    :ok = frame(zombie, {:seed_end, ctx.id})

    # Pre-fix this was `{:ack, _, 0}`: the new owner's node counted toward the deposed quorum.
    assert {:reject, id, :internal, 0} = reply(zombie)
    assert id == ctx.id

    assert Follower.state_of(ctx.name, ctx.id) == nil,
           "the deposed owner re-created a replica on the node that now coordinates the shard"

    refute File.exists?(Follower.db_path(ctx.name, ctx.id))
  end

  test "without a coordinator here the same seed is accepted (this node is a follower again)",
       ctx do
    Follower.forget(ctx.name, ctx.id)
    peer = connection(ctx.name)
    bytes = database_with(ctx.dir, [1, 2])

    :ok = frame(peer, deposed_seed(ctx.id, bytes))
    :ok = frame(peer, {:seed_chunk, ctx.id, :db, 0, bytes})
    :ok = frame(peer, {:seed_end, ctx.id})

    assert {:ack, _, 0} = reply(peer)
    assert %{} = Follower.state_of(ctx.name, ctx.id)
  end

  test "a coordinator still mid-open (lease pending) does NOT refuse a legitimate owner", ctx do
    # A coordinator registers BEFORE it holds the lease; the legitimate owner on another node is
    # still shipping to this follower meanwhile. Registered with no `:lease_held` mark = pending.
    {:ok, _} = Registry.register(Fathom.ShardRegistry, ctx.id, nil)
    owner = connection(ctx.name)
    bytes = database_with(ctx.dir, [1, 2])
    Follower.forget(ctx.name, ctx.id)

    :ok = frame(owner, deposed_seed(ctx.id, bytes))
    :ok = frame(owner, {:seed_chunk, ctx.id, :db, 0, bytes})
    :ok = frame(owner, {:seed_end, ctx.id})
    assert {:ack, _, 0} = reply(owner)

    :ok = frame(owner, deposed_push(ctx.id))
    refute match?({:reject, _, :stale_epoch, _}, reply(owner))
  end

  test "a real coordinator marks its registry entry :lease_held once it holds the lease", ctx do
    {:ok, pid, ref, _path} = Fathom.Shards.checkout(ctx.id)
    on_exit(fn -> Fathom.Shard.checkin(pid, ref) end)

    assert [{^pid, :lease_held}] = Registry.lookup(Fathom.ShardRegistry, ctx.id)
  end

  test "the listener still starts when the OS rejects the keepalive options" do
    {:ok, l} =
      Fathom.Shard.Replication.Keepalive.with_fallback([:binary, active: false], fn o ->
        # Reject anything carrying a raw option, as an OS without those option numbers would.
        if Enum.any?(o, &match?({:raw, _, _, _}, &1)),
          do: {:error, :einval},
          else: :gen_tcp.listen(0, o)
      end)

    assert {:ok, [keepalive: true]} = :inet.getopts(l, [:keepalive])
    :gen_tcp.close(l)
  end
end
