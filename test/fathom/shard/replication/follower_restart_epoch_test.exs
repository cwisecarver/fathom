defmodule Fathom.Shard.Replication.FollowerRestartEpochTest do
  @moduledoc """
  REGRESSION (expert review 2026-10-22 r8 #8, decision Q11 = A): after a follower restart (and
  after every idle-row sweep) the current owner's next push re-shipped the WHOLE current WAL.

  The restarted follower recovered its replica at epoch 0. The owner's next append states its real
  lock epoch at the same lineage, `pushed > epoch` routed it to `decide_fresh/2`, which refuses any
  non-zero offset (`:offset_mismatch`, expecting 0), and the primary answered by re-sending the WAL
  from offset 0 in `max_push_bytes` rounds, the first of them a reset that made the follower run a
  FULL absorb of bytes it already held. Per active followed shard, per deploy: routinely 4-16 MB
  plus two fsyncs each.

  The lease epoch now rides in the lineage sidecar (`"<lineage> <epoch>"`) and is recovered only
  beside a stated lineage. These pin: the append is accepted at the real extent after a restart;
  lineage 0 and an old-format sidecar still recover epoch 0 (the reopen-deadlock escape); and the
  lineage fence still refuses a deposed owner and re-seeds a new one whatever the persisted epoch.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Wal

  @lineage 7
  @epoch 5

  setup do
    root = Path.join(System.tmp_dir!(), "restartepoch_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    # Pushes state their lineage only on the ordinal frame, on in prod (off in dev/test); the
    # epoch is recovered only where they do (see the wire-off test).
    prev_wire = Application.get_env(:fathom, :replication_ordinal_wire)
    Application.put_env(:fathom, :replication_ordinal_wire, true)

    on_exit(fn ->
      if is_nil(prev_wire),
        do: Application.delete_env(:fathom, :replication_ordinal_wire),
        else: Application.put_env(:fathom, :replication_ordinal_wire, prev_wire)

      File.rm_rf(root)
    end)

    # A real SQLite WAL with committed frames, as a replica would hold it.
    src = Path.join(root, "src.db")
    {:ok, conn} = Connection.open(src)
    :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
    :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")

    for i <- 1..64,
        do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i}, randomblob(4000))")

    db = File.read!(src)
    wal = File.read!(src <> "-wal")
    Connection.close(conn)

    probe = Path.join(root, "probe-wal")
    File.write!(probe, wal)
    {:ok, header} = Wal.read(probe)

    %{
      root: root,
      dir: Path.join(root, "f"),
      id: "restart_epoch_#{System.unique_integer([:positive])}",
      db: db,
      wal: wal,
      header: header
    }
  end

  defp start!(ctx) do
    name = :"restartep_f_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: ctx.dir}, id: name)
    {:ok, port} = Follower.port(pid)
    {name, port}
  end

  # The replica files a running follower holds, with its row established by the owner at
  # `@epoch`/`lineage` (what a seed or the owner's pushes leave), then the follower stopped.
  defp replica_then_restart!(ctx, lineage) do
    File.mkdir_p!(ctx.dir)
    File.write!(Path.join(ctx.dir, ctx.id <> ".db"), ctx.db)
    File.write!(Path.join(ctx.dir, ctx.id <> ".db-wal"), ctx.wal)

    {name, _port} = start!(ctx)
    h = ctx.header

    :ok =
      Follower.seed(name, ctx.id, @epoch, h.ckpt_seq, h.salt1, h.commit_extent, lineage)

    stop_supervised!(name)
    start!(ctx)
  end

  defp append(ctx, fields) do
    h = ctx.header

    struct(
      Protocol.Push,
      Map.merge(
        %{
          shard_id: ctx.id,
          epoch: @epoch,
          lineage: @lineage,
          wal_gen: h.ckpt_seq,
          salt1: h.salt1,
          offset: h.commit_extent,
          prev_extent: 0,
          payload: :binary.copy(<<0>>, 64)
        },
        Map.new(fields)
      )
    )
  end

  defp send_frame!(port, frame) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 5_000)

    try do
      :ok = :gen_tcp.send(sock, frame)

      case :gen_tcp.recv(sock, 0, 5_000) do
        {:ok, bytes} -> Protocol.decode(bytes)
        other -> other
      end
    after
      :gen_tcp.close(sock)
    end
  end

  test "the owner's next append after a follower restart is accepted where the replica ends",
       ctx do
    {name, port} = replica_then_restart!(ctx, @lineage)
    extent = ctx.header.commit_extent
    assert extent > 200_000, "precondition: a WAL worth re-shipping (#{extent} B)"

    assert %{epoch: @epoch, lineage: @lineage, next_offset: ^extent} =
             Follower.state_of(name, ctx.id),
           "the restarted follower did not recover the owner's epoch"

    wal_before = File.read!(Follower.wal_path(name, ctx.id))

    # Before the fix: {:reject, id, :offset_mismatch, 0} — "send me the whole WAL again".
    assert {:ok, {:ack, _id, next}} = send_frame!(port, Protocol.encode_push(append(ctx, [])))
    assert next == extent + 64

    # An append, not a reset: the bytes the follower held are untouched (no absorb, no truncate).
    assert binary_part(File.read!(Follower.wal_path(name, ctx.id)), 0, extent) ==
             binary_part(wal_before, 0, extent)
  end

  test "an old-format sidecar (lineage only) recovers epoch 0, as before", ctx do
    {name, _port} = replica_then_restart!(ctx, @lineage)
    File.write!(Follower.lineage_path(name, ctx.id), Integer.to_string(@lineage))
    stop_supervised!(name)
    {name, port} = start!(ctx)

    assert %{epoch: 0, lineage: @lineage} = Follower.state_of(name, ctx.id)

    assert {:ok, {:reject, _id, :offset_mismatch, 0}} =
             send_frame!(port, Protocol.encode_push(append(ctx, [])))
  end

  # With the ordinal wire off (dev/test default) pushes state no lineage, so ownership is decided by
  # the EPOCH fence, where a recovered higher epoch would refuse a reopened owner (lock epoch reset
  # to 1) `:stale_epoch` forever. The restart's epoch 0 stays the escape there.
  test "ordinal wire off: the persisted epoch is not recovered", ctx do
    {name, _port} = replica_then_restart!(ctx, @lineage)
    Application.put_env(:fathom, :replication_ordinal_wire, false)
    stop_supervised!(name)
    {name, port} = start!(ctx)

    assert %{epoch: 0, lineage: @lineage} = Follower.state_of(name, ctx.id)

    # The reopened owner (epoch 1, lineage unstated on this wire) is not fenced out.
    refute match?(
             {:ok, {:reject, _, :stale_epoch, _}},
             send_frame!(port, Protocol.encode_push(append(ctx, epoch: 1, offset: 0)))
           )
  end

  test "lineage 0: the epoch is not persisted or recovered (the reopen-deadlock escape)", ctx do
    {name, _port} = replica_then_restart!(ctx, 0)
    assert %{epoch: 0, lineage: 0} = Follower.state_of(name, ctx.id)
    refute File.exists?(Follower.lineage_path(name, ctx.id))
  end

  test "the lineage fence still decides ownership whatever the persisted epoch", ctx do
    {_name, port} = replica_then_restart!(ctx, @lineage)

    # A deposed owner (lower lineage), even with a higher lock epoch: refused.
    assert {:ok, {:reject, _id, :stale_epoch, 0}} =
             send_frame!(
               port,
               Protocol.encode_push(append(ctx, lineage: @lineage - 1, epoch: @epoch + 10))
             )

    # A new ownership (higher lineage) whose lock epoch reset to 1: asked to re-seed, never
    # fenced `:stale_epoch` by the persisted 5.
    assert {:ok, {:reject, _id, :unknown_shard, 0}} =
             send_frame!(
               port,
               Protocol.encode_push(
                 append(ctx, lineage: @lineage + 1, epoch: 1, offset: 0, salt1: 12_345)
               )
             )
  end

  test "the sidecar is \"<lineage> <epoch>\" and the parser takes the old format too", ctx do
    {name, _port} = replica_then_restart!(ctx, @lineage)
    assert File.read!(Follower.lineage_path(name, ctx.id)) == "#{@lineage} #{@epoch}"
    assert Follower.recovered_ownership(name, ctx.id) == {@lineage, @epoch}

    for {text, want} <- [
          {"7", {7, 0}},
          {"7 ", {7, 0}},
          {"7 x", {7, 0}},
          {"0 9", {0, 0}},
          {"", {0, 0}},
          {"7 9\n", {7, 9}}
        ] do
      File.write!(Follower.lineage_path(name, ctx.id), text)
      assert Follower.recovered_ownership(name, ctx.id) == want, inspect(text)
    end
  end
end
