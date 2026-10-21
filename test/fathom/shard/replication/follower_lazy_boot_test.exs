defmodule Fathom.Shard.Replication.FollowerLazyBootTest do
  @moduledoc """
  Expert review 2026-10-21 #5: `Follower.init/1` derived a row for EVERY replica on disk before it
  returned — measured 150-300 µs each, 1.2-2.1 s at 8,000 replicas — and because `Fleet` starts in
  the data plane ahead of `Edge`, the node's Hrana and /health listeners waited on it too. At a
  million-shard fleet that is minutes of a node refusing everything after each restart.

  Rows are now derived on first use (`state_of/2` -> the same `derive_state` the boot pass ran),
  and the directory-listing housekeeping (stale seed temps, tombstoned replicas) runs after the
  listener. What these pin:

    * `init/1` derives nothing: the table holds no replica row when the follower starts listening,
      however many replicas are on disk, and the first `state_of/2` answers what the boot pass did;
    * a DELETED tenant's replica whose bytes are still on disk (the erase pass has not reached it)
      is REFUSED, never served — on the locked re-derive AND on the busy-lock path, which used to
      read the files with no tombstone check (user decision 2026-10-21);
    * the post-listen pass erases a tombstoned replica and leaves live ones alone, underived;
    * a write that lands on a replica with no resident row reads the previous torn flag from disk,
      so an untorn install clears a stale torn marker exactly as it did over a boot-derived row.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Tenants.Tombstones

  @lineage 7

  setup do
    root = Path.join(System.tmp_dir!(), "lazyboot_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    # A real SQLite WAL with committed frames, held open so its `-wal` is not checkpointed away.
    src = Path.join(root, "src.db")
    {:ok, conn} = Connection.open(src)
    :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
    :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)")
    for i <- 1..5, do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i})")
    db = File.read!(src)
    wal = File.read!(src <> "-wal")
    Connection.close(conn)

    {:ok, %{commit_extent: extent}} = Wal.read(write_tmp!(root, wal))

    %{root: root, dir: Path.join(root, "f"), db: db, wal: wal, extent: extent}
  end

  defp write_tmp!(root, bytes) do
    p = Path.join(root, "probe-wal")
    File.write!(p, bytes)
    p
  end

  # Replica files written straight into the follower's dir, as a previous incarnation left them.
  defp plant!(ctx, id, opts \\ []) do
    File.mkdir_p!(ctx.dir)
    File.write!(Path.join(ctx.dir, id <> ".db"), ctx.db)
    File.write!(Path.join(ctx.dir, id <> ".db-wal"), ctx.wal)
    File.write!(Path.join(ctx.dir, id <> ".db.lineage"), Integer.to_string(@lineage))
    if opts[:torn], do: File.write!(Path.join(ctx.dir, id <> ".db.torn"), "")
    :ok
  end

  defp start!(ctx) do
    name = :"lazyboot_f_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: ctx.dir}, id: name)
    {name, pid}
  end

  defp resident_ids(name) do
    name
    |> Follower.table()
    |> :ets.tab2list()
    |> Enum.map(&elem(&1, 0))
    |> Enum.reject(&is_atom/1)
  end

  # Holds `id`'s replica lock from another process until told to let go.
  defp hold_lock!(name, id) do
    test = self()

    pid =
      spawn_link(fn ->
        {:ok, token} = Follower.lock_shard(name, id, 1_000)
        send(test, {:held, self()})

        receive do
          :release -> Follower.unlock_shard(name, token)
        end
      end)

    assert_receive {:held, ^pid}, 1_000
    pid
  end

  test "init derives no replica: rows appear on first use, as the boot pass derived them", ctx do
    ids = for i <- 1..40, do: "lazy_#{i}_#{System.unique_integer([:positive])}"
    for id <- ids, do: plant!(ctx, id)
    torn_id = "lazy_torn_#{System.unique_integer([:positive])}"
    plant!(ctx, torn_id, torn: true)

    {name, pid} = start!(ctx)

    # The listener is up (`start_supervised!` returned after init) and NOT ONE replica was derived.
    # Before the fix this was all 41: the work `init/1` did before anything could connect.
    assert resident_ids(name) == [],
           "init/1 derived #{length(resident_ids(name))} replica row(s) before listening"

    _ = :sys.get_state(pid)
    assert resident_ids(name) == [], "the post-listen housekeeping derived replica rows"

    for id <- ids do
      assert %{lineage: @lineage, torn: false, epoch: 0, next_offset: extent} =
               Follower.state_of(name, id)

      assert extent == ctx.extent, "the lazily derived offset is not the WAL's commit extent"
    end

    assert %{torn: true, lineage: @lineage} = Follower.state_of(name, torn_id),
           "the torn marker was not carried by the lazy derive"

    assert Follower.offerable(name, torn_id) == nil
    assert length(resident_ids(name)) == 41
  end

  test "a tombstoned replica still on disk is refused on the busy path and the locked path",
       ctx do
    {name, pid} = start!(ctx)
    _ = :sys.get_state(pid)

    # Planted AFTER the post-listen erase ran, so the bytes are on disk exactly as they are for a
    # deleted tenant that pass has not reached yet. The control is the same bytes, not deleted.
    gone = "lazy_gone_#{System.unique_integer([:positive])}"
    live = "lazy_live_#{System.unique_integer([:positive])}"
    plant!(ctx, gone)
    plant!(ctx, live)
    on_exit(fn -> :ets.delete(Tombstones, gone) end)
    :ok = Tombstones.put(gone)

    # BUSY PATH: a lock held elsewhere makes `state_of/2` answer the unlocked derive.
    holders = for id <- [gone, live], do: hold_lock!(name, id)

    assert %{lineage: @lineage} = Follower.state_of(name, live),
           "precondition: the busy-path derive does not read this fixture at all"

    assert Follower.state_of(name, gone) == nil,
           "a deleted tenant's replica was SERVED through the busy-lock path"

    assert Follower.offerable(name, gone) == nil
    assert File.exists?(Path.join(ctx.dir, gone <> ".db")), "precondition: bytes still on disk"

    for h <- holders, do: send(h, :release)
    # The locked path, once the holder is gone: refused, and erased on contact.
    assert Follower.state_of(name, gone) == nil,
           "a deleted tenant's replica was SERVED through the locked re-derive"

    refute File.exists?(Path.join(ctx.dir, gone <> ".db")),
           "the locked re-derive did not erase it"

    assert %{lineage: @lineage} = Follower.state_of(name, live)
  end

  test "the post-listen pass erases tombstoned replicas and leaves live ones underived", ctx do
    gone = "lazy_gone_#{System.unique_integer([:positive])}"
    live = "lazy_live_#{System.unique_integer([:positive])}"
    plant!(ctx, gone)
    plant!(ctx, live)
    on_exit(fn -> :ets.delete(Tombstones, gone) end)
    :ok = Tombstones.put(gone)

    {name, pid} = start!(ctx)
    _ = :sys.get_state(pid)

    assert Path.wildcard(Path.join(ctx.dir, gone <> ".db*")) == [],
           "a deleted tenant's replica survived the post-listen erase"

    assert File.exists?(Path.join(ctx.dir, live <> ".db"))
    assert resident_ids(name) == [], "the erase pass derived a live replica"
    assert %{lineage: @lineage} = Follower.state_of(name, live)
  end

  test "a write to a replica with no resident row clears a stale torn marker", ctx do
    id = "lazy_install_#{System.unique_integer([:positive])}"
    plant!(ctx, id, torn: true)
    {name, _pid} = start!(ctx)
    refute id in resident_ids(name), "precondition: the row must not be resident"

    # An untorn state written without a prior `state_of/2` — what a lineage-0 seed install does
    # (`do_install/3`; `stale_lineage_seed?/2` reads the row only for a seed stating a lineage).
    # Over a boot-derived torn row this removed the marker; with no row it used to assume "was not
    # torn" and leave it, so the next restart re-derived a freshly seeded replica as torn.
    :ok = Follower.seed(name, id, 1, 0, 0, byte_size(ctx.wal), 0)

    refute File.exists?(Follower.torn_path(name, id)),
           "an untorn install over a non-resident torn replica left the torn marker on disk"
  end
end
