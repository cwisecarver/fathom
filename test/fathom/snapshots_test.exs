defmodule Fathom.SnapshotsTest do
  @moduledoc """
  Point-in-time snapshot + restore (expert review 2026-07-14 #12), exercised
  end to end on the Local backend: write → flush → snapshot → mutate → restore
  reverts the live object; list/drop; per-shard isolation; and the restore-safety
  refusal when the shard can't be quiesced.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards, Snapshots}
  alias Filo.Stmt

  setup do
    shard = "snap_#{System.unique_integer([:positive])}"
    other = "snap_other_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for id <- [shard, other] do
        Shards.drain(id, 5_000)
        rm_shard(id)
      end
    end)

    %{shard: shard, other: other}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # Run statements on a shard through the real checkout→connection path, then close.
  defp write!(shard, sqls) do
    {:ok, handle} = ShardExecutor.open(shard)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, stmt(s)) end)
    :ok = ShardExecutor.close(handle)
  end

  defp read_one(shard, sql) do
    {:ok, handle} = ShardExecutor.open(shard)
    {:ok, result} = ShardExecutor.execute(handle, stmt(sql))
    :ok = ShardExecutor.close(handle)
    result.rows
  end

  # Flush the shard's in-memory state to the live stored object (snapshots copy the
  # stored object, so a snapshot only sees flushed data).
  defp flush!(shard), do: :ok = Shards.drain(shard, 5_000)

  test "snapshot then restore reverts the live object", %{shard: shard} do
    write!(shard, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('v1')"])
    flush!(shard)

    assert {:ok, snap_id} = Snapshots.create(shard, label: "before-change")

    write!(shard, ["UPDATE t SET v = 'v2'"])
    flush!(shard)
    assert read_one(shard, "SELECT v FROM t") == [["v2"]]

    assert :ok = Snapshots.restore(shard, snap_id)
    assert read_one(shard, "SELECT v FROM t") == [["v1"]]
  end

  test "list returns snapshots newest-first and drop removes one", %{shard: shard} do
    write!(shard, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
    flush!(shard)

    assert {:ok, s1} = Snapshots.create(shard)
    assert {:ok, s2} = Snapshots.create(shard, label: "second")

    assert {:ok, snaps} = Snapshots.list(shard)
    ids = Enum.map(snaps, & &1.id)
    assert s1 in ids and s2 in ids
    # Sorted desc by id (timestamp+uniquifier), so the later snapshot sorts first.
    assert ids == Enum.sort(ids, :desc)
    assert Enum.all?(snaps, &(&1.bytes > 0))

    assert :ok = Snapshots.drop(shard, s1)
    assert {:ok, remaining} = Snapshots.list(shard)
    assert Enum.map(remaining, & &1.id) == [s2]
  end

  test "a snapshot/restore of one shard never touches another", %{shard: a, other: b} do
    write!(a, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('A')"])
    write!(b, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('B')"])
    flush!(a)
    flush!(b)

    assert {:ok, snap_a} = Snapshots.create(a)

    # b has no snapshots of its own.
    assert {:ok, []} = Snapshots.list(b)

    # Restoring a leaves b's live object untouched.
    assert :ok = Snapshots.restore(a, snap_a)
    assert read_one(a, "SELECT v FROM t") == [["A"]]
    assert read_one(b, "SELECT v FROM t") == [["B"]]
  end

  test "restore refuses while the shard is actively served (can't drain)", %{shard: shard} do
    write!(shard, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('v1')"])
    flush!(shard)
    assert {:ok, snap_id} = Snapshots.create(shard)

    # Hold a connection open so the coordinator can't drain within the budget.
    {:ok, handle} = ShardExecutor.open(shard)

    assert {:error, {:shard_busy, :busy}} = Snapshots.restore(shard, snap_id, drain_timeout: 100)

    :ok = ShardExecutor.close(handle)
  end

  test "create refuses an invalid shard id", %{} do
    assert {:error, :invalid_shard_id} = Snapshots.create("Not A Shard!")
  end

  defp rm_shard(id) do
    remote_dir = Path.join(System.tmp_dir!(), "fathom_remote_test")

    for base <- [
          Path.join([System.tmp_dir!(), "fathom_shards", "#{id}.db"]),
          Path.join([remote_dir, "#{id}.db"])
        ],
        suffix <- ["", "-wal", "-shm"] do
      File.rm(base <> suffix)
    end

    for snap <- Path.wildcard(Path.join(remote_dir, "#{id}@snap-*.db")), do: File.rm(snap)
  end
end
