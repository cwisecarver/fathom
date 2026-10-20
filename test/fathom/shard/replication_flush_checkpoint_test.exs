defmodule Fathom.Shard.ReplicationFlushCheckpointTest do
  @moduledoc """
  The durability flush's post-snapshot PASSIVE: counted by outcome, and under replication skipped
  while the WAL is small (expert review 2026-10-20 #15).

  Under A2 that PASSIVE tied WAL generation resets to the S3 flush interval: on a write-active
  replicated shard a checkpoint that lets the next write restart the log costs every follower an
  absorb (two fsyncs) and the next commit a reset round, once per flush. AGENTS.md measures the
  result as the 22-25% txn/s gap between a 5 s and a 30 s flush interval, and nothing counted it.
  Now every outcome is counted, and under replication the PASSIVE waits for the commit extent to
  reach 1/4 of the Session threshold. Replication OFF keeps the unconditional PASSIVE.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session, Wal}
  alias Fathom.Shards

  @frame 4096 + 24
  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_checkpoint_bytes,
    :replication_flush_checkpoint_min_bytes
  ]

  setup do
    id = "repl_flush_ckpt_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replflushckpt_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})
    # Threshold 80 frames, so the size gate sits at 20 frames.
    Application.put_env(:fathom, :replication_checkpoint_bytes, 80 * @frame)

    test = self()
    handler = "flush-ckpt-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush_checkpoint],
      fn _e, _m, meta, _ -> send(test, {:flush_checkpoint, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Session.stop(id)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp replicated_shard!(%{id: id, root: root}) do
    followers =
      for i <- 1..3 do
        name = :"fc_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "#{name}")},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        port
      end

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for(p <- followers, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    {:ok, flush} = Connection.open(path)

    on_exit(fn ->
      Connection.close(conn)
      Connection.close(flush)
    end)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b BLOB)", [])
    %{id: id, conn: conn, flush: flush, coordinator: coordinator, wal: path <> "-wal"}
  end

  defp write!(s, n) do
    for i <- 1..n do
      {:ok, _} = Connection.query(s.conn, "INSERT INTO t VALUES (#{i}, randomblob(3000))", [])
      assert :ok = Session.commit(s.id, s.wal, s.coordinator)
    end
  end

  defp backfilled(s) do
    {:ok, %{rows: [[_busy, log, ckpt]]}} =
      Connection.query(s.flush, "PRAGMA wal_checkpoint(PASSIVE)", [])

    {log, ckpt}
  end

  test "replicated, small WAL: no PASSIVE, counted :skipped_small", ctx do
    s = replicated_shard!(ctx)
    write!(s, 4)
    {:ok, %{commit_extent: e}} = Wal.read(s.wal)
    assert e < 20 * @frame, "precondition: below the size gate"

    assert :skipped = Session.checkpoint_after_snapshot(s.flush, ctx.id)
    assert_receive {:flush_checkpoint, %{result: :skipped_small, replicated: true}}
  end

  test "replicated, WAL past the gate: the covered PASSIVE runs, counted :ran", ctx do
    s = replicated_shard!(ctx)
    write!(s, 15)
    {:ok, %{commit_extent: e}} = Wal.read(s.wal)
    assert e >= 20 * @frame, "precondition: past the size gate"

    assert {:ok, _} = Session.checkpoint_after_snapshot(s.flush, ctx.id)
    assert_receive {:flush_checkpoint, %{result: :ran, replicated: true}}
    {log, ckpt} = backfilled(s)
    assert ckpt == log
  end

  test "the gate at 0 restores the per-flush PASSIVE on a small WAL", ctx do
    Application.put_env(:fathom, :replication_flush_checkpoint_min_bytes, 0)
    s = replicated_shard!(ctx)
    write!(s, 4)

    assert {:ok, _} = Session.checkpoint_after_snapshot(s.flush, ctx.id)
    assert_receive {:flush_checkpoint, %{result: :ran, replicated: true}}
  end

  test "replication off: the unconditional PASSIVE, counted :ran", ctx do
    path = Path.join(ctx.root, "plain.db")
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])

    assert {:ok, %{rows: [[0, log, log]]}} = Session.checkpoint_after_snapshot(conn, nil)
    assert_receive {:flush_checkpoint, %{result: :ran, replicated: false}}
  end
end
