defmodule Fathom.SnapshotsTest do
  @moduledoc """
  Point-in-time snapshot + restore (expert review 2026-07-14 #12), exercised
  end to end on the Local backend: write → flush → snapshot → mutate → restore
  reverts the live object; list/drop; per-shard isolation; and the restore-safety
  refusal when the shard can't be quiesced.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards, Snapshots}
  alias Fathom.Shard.Storage
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

  # Expert review 2026-07-18 #2: restore_snapshot was an UNCONDITIONAL copy, so a write that raced
  # in after Snapshots.restore's drain (a fresh checkout acquiring the freed lease and flushing)
  # was silently clobbered — the exact TOCTOU the fenced migration restore/3 already closed.
  # Snapshots.restore now captures the live etag at the lease-free instant and restores under an
  # If-Match fence. This pins the fenced primitive: a stale etag must abort with :superseded and
  # leave live untouched; the current etag restores.
  test "restore_snapshot/3 is fenced: a stale etag aborts with :superseded and never clobbers live",
       %{shard: shard} do
    write!(shard, ["CREATE TABLE kv (v TEXT)", "INSERT INTO kv VALUES ('a')"])
    flush!(shard)
    assert {:ok, snap} = Snapshots.create(shard)

    # Live advances past the snapshot (now holds a + b), lease free, no coordinator.
    write!(shard, ["INSERT INTO kv VALUES ('b')"])
    flush!(shard)
    assert {:ok, live_etag} = Storage.object_etag(shard)

    # A stale etag stands in for "a writer flushed after I looked" — the restore must NOT clobber.
    assert {:error, :superseded} = Storage.restore_snapshot(shard, snap, "stale-etag")

    assert read_one(shard, "SELECT count(*) FROM kv") == [[2]],
           "a superseded restore must leave live untouched (still a + b)"

    # Quiesce the read's coordinator; the live object (and its etag) is unchanged (reads don't flush).
    flush!(shard)

    # The live etag restores — the fenced happy path.
    assert :ok = Storage.restore_snapshot(shard, snap, live_etag)

    assert read_one(shard, "SELECT count(*) FROM kv") == [[1]],
           "the fenced restore with the live etag reverts to the snapshot (a only)"
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

  # Expert review 2026-07-19 #1: `snapshot_id` is caller-supplied and flows straight into
  # `Path.join(dir(), "<shard>@snap-<snapshot_id>.db")` (Local) and the S3 object key. Without a
  # validation gate, a traversal id escapes the shard's key prefix — restore becomes an arbitrary
  # file read written into the tenant's live object (then downloadable via export), and drop becomes
  # an arbitrary `.db` delete. The invariant: `snapshot_id` is gated like a shard id (no `/`, no
  # `..`), so an escape attempt is refused with `:invalid_snapshot_id` and never touches the fs.
  test "restore/drop reject a path-traversal snapshot_id and never read/delete outside the store",
       %{shard: shard} do
    # A sentinel outside the shard store that a traversal restore could smuggle in / a drop erase.
    outside =
      Path.join(
        System.tmp_dir!(),
        "fathom_snap_traversal_#{System.unique_integer([:positive])}.db"
      )

    File.write!(outside, "SENTINEL-must-not-be-touched")
    on_exit(fn -> File.rm(outside) end)

    write!(shard, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('safe')"])
    flush!(shard)

    # Relative traversal (Local backend) and an absolute-ish key escape (S3-style) are both refused,
    # as is a bare `..` and any id carrying a slash.
    for bad <- [
          "../../../../../etc/hosts",
          "../../fathom_snap_traversal",
          "..",
          "a/b",
          "x@snap-#{Path.basename(outside, ".db")}"
        ] do
      assert {:error, :invalid_snapshot_id} = Snapshots.restore(shard, bad),
             "restore must reject traversal snapshot_id #{inspect(bad)}"

      assert {:error, :invalid_snapshot_id} = Snapshots.drop(shard, bad),
             "drop must reject traversal snapshot_id #{inspect(bad)}"
    end

    # The gate held: the outside sentinel was neither read into the shard nor deleted, and the
    # live object is untouched.
    assert File.read!(outside) == "SENTINEL-must-not-be-touched"
    assert read_one(shard, "SELECT v FROM t") == [["safe"]]

    # A non-binary id is rejected too (untrusted input).
    assert {:error, :invalid_snapshot_id} = Snapshots.drop(shard, nil)
  end

  defp rm_shard(id) do
    remote_dir = Fathom.Shard.Storage.Local.dir()

    for base <- [
          Path.join([Fathom.Shard.data_dir(), "#{id}.db"]),
          Path.join([remote_dir, "#{id}.db"])
        ],
        suffix <- ["", "-wal", "-shm"] do
      File.rm(base <> suffix)
    end

    for snap <- Path.wildcard(Path.join(remote_dir, "#{id}@snap-*.db")), do: File.rm(snap)
  end
end
