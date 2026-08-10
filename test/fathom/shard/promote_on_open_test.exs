defmodule Fathom.Shard.PromoteOnOpenTest do
  @moduledoc """
  A cold open serving a newer local replica — Phase 2 A2, and the switch that makes A2 worth
  having. See `docs/a2-quorum-replication.md`.

  Everything else in A2 gets bytes onto a follower's disk. Until this ran, those bytes sat there
  while a failover cold-opened the last flush, so **node-loss RPO was still ~300 s with replication
  fully working**.

  Two tests carry the weight and they pull in opposite directions, which is the point:

    * `recovers_writes_the_stored_object_never_had` — the win. Rows that only ever existed on the
      primary and its replicas come back through the ordinary `Shards.checkout/1` path.
    * `a_lagging_replica_is_never_promoted` — the loss case, and the one that matters more. A
      follower that fell behind while the primary kept flushing holds an OLDER database; serving it
      would silently discard acknowledged writes. If the comparison is ever inverted, this is what
      fails.

  The rest pin that everything uncertain falls back to the stored object: gate off, no stamp, and
  a replica level with the object.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Storage
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  setup do
    id = "promoteopen_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "promoteopen_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    prev = Application.get_env(:fathom, :replication_promote_on_open)

    on_exit(fn ->
      Shards.stop(id)

      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_promote_on_open),
        else: Application.put_env(:fathom, :replication_promote_on_open, prev)

      File.rm_rf(dir)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    # The follower must be the DEFAULT-named instance, because that is the one a coordinator looks
    # for — a differently-named one would make every test here silently take the no-replica path.
    start_supervised!({Follower, name: Follower, port: 0, dir: dir})

    %{id: id, dir: dir}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp flush!(coordinator) do
    send(coordinator, :durability_flush)
    settle(coordinator, 400)
  end

  defp settle(_c, 0), do: flunk("durability flush task never settled")

  defp settle(c, tries) do
    if :sys.get_state(c).flush_task == nil do
      :ok
    else
      Process.sleep(10)
      settle(c, tries - 1)
    end
  end

  defp rows(conn) do
    {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT a FROM t ORDER BY a"))
    Enum.map(rows, fn [a] -> a end)
  end

  # Build the shard, optionally flush, then take its live bytes as a "replica" and hand them to the
  # follower at `position`. Using the shard's own files is what makes this a real replica rather
  # than a fabrication: the WAL byte offsets refer to the primary's WAL, exactly as shipped frames
  # would leave them.
  defp install_replica(id, position) do
    path = Fathom.Shard.db_path(id)
    File.cp!(path, Follower.db_path(Follower, id))

    case File.stat(path <> "-wal") do
      {:ok, %{size: size}} when size > 0 ->
        File.cp!(path <> "-wal", Follower.wal_path(Follower, id))

      _ ->
        File.write!(Follower.wal_path(Follower, id), "")
    end

    Follower.seed(Follower, id, position.epoch, position.wal_gen, position.offset)
  end

  defp open_and_read(id) do
    {:ok, conn} = ShardExecutor.open(id)
    result = rows(conn)
    :ok = ShardExecutor.close(conn)
    result
  end

  # Writes `rows` rows, flushes at `flush_after`, and leaves the shard stopped with no local files —
  # the state a survivor is in. Returns the position the stored object ended up claiming.
  defp build_shard(id, total, flush_after) do
    {:ok, coordinator} = Shards.ensure(id)
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER PRIMARY KEY)"))

    for i <- 1..total do
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [i]))
      if i == flush_after, do: flush!(coordinator)
    end

    {conn, coordinator}
  end

  # NODE LOSS, not a shutdown. `Shards.stop/1` is a graceful drain and its drop-flush uploads
  # everything on the way out — which would leave the stored object holding all the rows and make
  # every test here vacuous. (It did: the first version used it and three tests failed with the
  # object already complete.) Killing the coordinator is the situation A2 exists for: the writes
  # since the last flush are on disk and in the replicas, and nowhere else.
  defp tear_down_primary(id, conn, coordinator) do
    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 2_000
    # The stream's connection died with it; closing is best-effort bookkeeping and may or may not
    # exit depending on how far the teardown got.
    _ =
      try do
        ShardExecutor.close(conn)
      catch
        :exit, _ -> :ok
      end

    # The dead node's disk is gone too — a survivor elsewhere has only the object and its replica.
    for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    :ok
  end

  test "recovers_writes_the_stored_object_never_had", ctx do
    %{id: id} = ctx
    Application.put_env(:fathom, :replication_promote_on_open, true)

    # Flush at row 3, then write 7 more. The object holds 3; the shard holds 10.
    {conn, coordinator} = build_shard(id, 10, 3)
    assert {:ok, stamp} = Storage.object_position(id)
    refute is_nil(stamp), "no stamp — the comparison could not run and this proves nothing"

    # The replica is the shard's CURRENT bytes, i.e. all 10 rows, at a position past the object's.
    install_replica(id, %{
      epoch: stamp.epoch,
      wal_gen: stamp.wal_gen,
      offset: stamp.offset + 1
    })

    tear_down_primary(id, conn, coordinator)

    # Precondition: what the store holds is genuinely short. Without this the test would pass with
    # promotion doing nothing at all.
    stored = Path.join(ctx.dir, "stored.db")
    assert {:ok, _} = Storage.pull(id, stored)
    {:ok, sconn} = Fathom.Shard.Connection.open(stored)
    {:ok, %{rows: srows}} = Fathom.Shard.Connection.query(sconn, "SELECT a FROM t ORDER BY a", [])
    Fathom.Shard.Connection.close(sconn)
    assert Enum.map(srows, fn [a] -> a end) == [1, 2, 3]

    # THE ASSERTION: the ordinary open path returns all ten.
    assert open_and_read(id) == Enum.to_list(1..10)

    # And the pre-promotion state was preserved before being overwritten.
    assert {:ok, snapshots} = Storage.list_snapshots(id)
    assert Enum.any?(snapshots, &String.contains?(snapshot_id(&1), "pre-promotion"))
  end

  defp snapshot_id(%{id: id}), do: id
  defp snapshot_id(id) when is_binary(id), do: id
  defp snapshot_id(other), do: inspect(other)

  test "a_lagging_replica_is_never_promoted", ctx do
    %{id: id} = ctx
    Application.put_env(:fathom, :replication_promote_on_open, true)

    # Flush LAST, so the object holds all 10 rows.
    {conn, coordinator} = build_shard(id, 10, 10)
    assert {:ok, stamp} = Storage.object_position(id)

    # The replica's bytes are current, but it REPORTS a position behind the object — a follower
    # that fell behind while the primary kept flushing. Serving it would discard the difference,
    # and the only thing standing between that and a tenant is `Promote.fresher?/2`.
    install_replica(id, %{
      epoch: stamp.epoch,
      wal_gen: stamp.wal_gen,
      offset: max(stamp.offset - 1, 0)
    })

    refute Promote.fresher?(Follower.state_of(Follower, id), stamp),
           "the fixture did not actually make the replica lag"

    tear_down_primary(id, conn, coordinator)

    assert open_and_read(id) == Enum.to_list(1..10)

    # Nothing was overwritten, so nothing needed preserving.
    assert {:ok, snapshots} = Storage.list_snapshots(id)
    refute Enum.any?(snapshots, &String.contains?(snapshot_id(&1), "pre-promotion"))
  end

  test "an equal replica is not promoted", ctx do
    %{id: id} = ctx
    Application.put_env(:fathom, :replication_promote_on_open, true)

    {conn, coordinator} = build_shard(id, 6, 6)
    assert {:ok, stamp} = Storage.object_position(id)

    # Same position ⇒ both hold the same history, and the stored object is the one with provenance.
    install_replica(id, %{epoch: stamp.epoch, wal_gen: stamp.wal_gen, offset: stamp.offset})
    tear_down_primary(id, conn, coordinator)

    assert open_and_read(id) == Enum.to_list(1..6)
    assert {:ok, snaps} = Storage.list_snapshots(id)
    refute Enum.any?(snaps, &String.contains?(snapshot_id(&1), "pre-promotion"))
  end

  test "the gate off leaves the open path alone", ctx do
    %{id: id} = ctx
    Application.put_env(:fathom, :replication_promote_on_open, false)

    {conn, coordinator} = build_shard(id, 10, 3)
    assert {:ok, stamp} = Storage.object_position(id)
    install_replica(id, %{epoch: stamp.epoch, wal_gen: stamp.wal_gen, offset: stamp.offset + 1})
    tear_down_primary(id, conn, coordinator)

    # A strictly-newer replica is present and is still ignored: the gate is what decides, not the
    # data. This is the rollback story — turning the flag off restores today's behaviour exactly.
    assert open_and_read(id) == [1, 2, 3]
  end

  test "an object with no stamp is never overridden", ctx do
    %{id: id} = ctx
    Application.put_env(:fathom, :replication_promote_on_open, true)

    {conn, coordinator} = build_shard(id, 10, 3)
    assert {:ok, stamp} = Storage.object_position(id)
    install_replica(id, %{epoch: stamp.epoch, wal_gen: stamp.wal_gen, offset: stamp.offset + 1})
    tear_down_primary(id, conn, coordinator)

    # Re-flush the object WITHOUT a stamp, the way every object written before stamping existed
    # looks. This is the rollout state, and it must be inert rather than dangerous.
    scratch = Path.join(ctx.dir, "unstamped.db")
    assert {:ok, _} = Storage.pull(id, scratch)
    :ok = Storage.flush(id, scratch)
    assert {:ok, nil} = Storage.object_position(id)

    assert open_and_read(id) == [1, 2, 3]
  end
end
