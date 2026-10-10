defmodule Fathom.Shard.Replication.SeedPinnedSnapshotTest do
  @moduledoc """
  A seed's base copy must be CONSISTENT with the WAL prefix it declares, even when a PASSIVE
  checkpoint runs while the `.db` streams (expert review 2026-10-08 #12).

  The symptom this pins: `Session.do_seed/6` read the WAL header, then streamed the `.db` a chunk
  at a time, then re-read the header to detect a checkpoint. A PASSIVE checkpoint — the commit
  hook's at 4000 pages, or a tenant's `PRAGMA wal_checkpoint` — backfills pages into the `.db` and
  leaves the WAL header byte-identical, so that check passed while the later chunks carried pages
  from commits made AFTER the declared extent. The follower replayed the prefix over a base that
  was newer in places: rows the prefix never contained showed up on the replica.

  The invariant: the shipped `.db` + WAL prefix, opened together, is exactly the primary's state at
  the declared extent. The fix pins a read transaction across the stream, and SQLite never
  backfills past an active reader's mark; the second test pins that the read is always released.

  These drive `do_seed/6` against a FAKE shipper rather than a real `Follower`, because the race
  needs a seam between two chunks: the fake runs the writes + checkpoint when the first `.db` chunk
  arrives, i.e. after that chunk was read from disk and before the rest are.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Session

  defmodule FakeShipper do
    @moduledoc false
    # Speaks the `Shipper` calls `do_seed/6` makes. `on_first_db_chunk` runs inside the
    # `seed_chunk` call for the first `.db` chunk; returning `{:error, _}` from it fails that call.
    use GenServer

    def start_link(on_first_db_chunk), do: GenServer.start_link(__MODULE__, on_first_db_chunk)
    def shipped(pid), do: GenServer.call(pid, :shipped)

    @impl true
    def init(fun), do: {:ok, %{fun: fun, fired: false, begin: nil, db: [], wal: []}}

    @impl true
    def handle_cast({:seed_begin, begin, _from}, st), do: {:noreply, %{st | begin: begin}}

    def handle_cast({:seed_frame, id, :end, from}, st) do
      send(from, {:repl_reply, self(), {:ack, id, st.begin.wal_size}})
      {:noreply, st}
    end

    def handle_cast({:seed_frame, id, :abort, from}, st) do
      send(from, {:repl_reply, self(), {:reject, id, :internal, 0}})
      {:noreply, st}
    end

    @impl true
    def handle_call({:seed_chunk, _id, {:chunk, :db, _seq, bytes}}, _from, %{fired: false} = st) do
      reply = st.fun.()
      {:reply, reply, %{st | fired: true, db: [st.db, bytes]}}
    end

    def handle_call({:seed_chunk, _id, {:chunk, :db, _seq, bytes}}, _from, st),
      do: {:reply, :ok, %{st | db: [st.db, bytes]}}

    def handle_call({:seed_chunk, _id, {:chunk, :wal, _seq, bytes}}, _from, st),
      do: {:reply, :ok, %{st | wal: [st.wal, bytes]}}

    def handle_call(:shipped, _from, st),
      do: {:reply, {IO.iodata_to_binary(st.db), IO.iodata_to_binary(st.wal), st.begin}, st}
  end

  @rows 400

  # One throwaway seed + TRUNCATE before any test. Measured, not tidiness: under a probe with the
  # close removed, whichever release test ran FIRST in the VM passed anyway (3 of 3 orderings) —
  # some first-use initialisation on that path reclaims the leaked handle — so without this warm-up
  # the release tests discriminate only when they are not first.
  setup_all do
    dir = Path.join(System.tmp_dir!(), "seedpin_warm_#{System.unique_integer([:positive])}")
    path = Path.join(dir, "warm.db")
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE w (a)", [])
    {:ok, shipper} = FakeShipper.start_link(fn -> :ok end)
    {:ok, _} = Session.do_seed(shipper, "seedpin_warm", path, path <> "-wal", 1, 0)
    :ok = Connection.set_busy_timeout(conn, 200)
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    Connection.close(conn)
    GenServer.stop(shipper)
    File.rm_rf(dir)
    :ok
  end

  setup do
    root = Path.join(System.tmp_dir!(), "seedpin_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Application.get_env(:fathom, :replication_seed_chunk_bytes)
    # Many chunks, so there IS a "rest of the stream" after the first chunk for the checkpoint to
    # land in. At the 4 MiB default this whole database is one chunk and the race cannot happen.
    Application.put_env(:fathom, :replication_seed_chunk_bytes, 4_096)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_seed_chunk_bytes),
        else: Application.put_env(:fathom, :replication_seed_chunk_bytes, prev)

      File.rm_rf(root)
    end)

    path = Path.join(root, "primary.db")
    {:ok, conn} = Connection.open(path)
    # Held open for the whole test, as the coordinator holds its shard: without it, the fake's
    # writer would be the LAST connection and its close would checkpoint and delete the WAL.
    on_exit(fn -> Connection.close(conn) end)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)", [])
    {:ok, _} = Connection.query(conn, "CREATE TABLE u (a INTEGER)", [])

    for i <- 1..@rows do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [i, "orig-#{i}"])
    end

    # Fold everything into the `.db`, then leave a short live WAL that does NOT touch t's pages —
    # the realistic large-base-plus-small-tail shape, and the one where a backfilled page of t has
    # no frame in the shipped prefix to overwrite it on replay.
    {:ok, %{rows: [[0, _, _]]}} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO u VALUES (1)", [])

    assert File.stat!(path).size > 4 * 4_096, "fixture too small to span chunks"
    assert File.stat!(path <> "-wal").size > 0, "fixture has no live WAL to seed"

    %{root: root, path: path, conn: conn}
  end

  # A writer on its own connection rewrites every row of t (which lives only in the `.db`), then
  # runs a PASSIVE checkpoint, and reports what the checkpoint did.
  defp update_and_checkpoint(path, test_pid) do
    fn ->
      {:ok, w} = Connection.open(path)

      try do
        {:ok, _} = Connection.query(w, "UPDATE t SET b = 'late-' || a", [])
        cp = Connection.query(w, "PRAGMA wal_checkpoint(PASSIVE)", [])
        send(test_pid, {:checkpoint, cp})
        :ok
      after
        Connection.close(w)
      end
    end
  end

  defp open_replica(root, db, wal) do
    copy = Path.join(root, "replica_#{System.unique_integer([:positive])}.db")
    File.write!(copy, db)
    File.write!(copy <> "-wal", wal)
    {:ok, r} = Connection.open(copy)
    on_exit(fn -> Connection.close(r) end)
    r
  end

  test "a PASSIVE checkpoint during the .db stream cannot leak later commits into the base",
       ctx do
    %{root: root, path: path} = ctx
    {:ok, shipper} = FakeShipper.start_link(update_and_checkpoint(path, self()))

    result = Session.do_seed(shipper, "seedpin", path, path <> "-wal", 1, 0)

    # PRECONDITIONS — otherwise this proves nothing. The checkpoint ran mid-stream and actually
    # backfilled; and the seed did NOT abort, i.e. the WAL header was unchanged and the old
    # before/after generation check could not see it (that is the gap under test).
    assert_received {:checkpoint, {:ok, %{rows: [[0, log, _ckpt]]}}}
    assert log > 0, "the mid-stream checkpoint had no frames to consider"

    assert {:ok, %{offset: offset}} = result,
           "the seed aborted, so the WAL header moved — this fixture no longer reproduces a " <>
             "header-invisible checkpoint: #{inspect(result)}"

    {db, wal, begin} = FakeShipper.shipped(shipper)
    assert byte_size(db) == begin.db_size
    assert byte_size(wal) == begin.wal_size and offset == begin.wal_size

    {:ok, %{rows: [[late_on_primary]]}} =
      Connection.query(ctx.conn, "SELECT count(*) FROM t WHERE b LIKE 'late-%'", [])

    assert late_on_primary == @rows, "the mid-stream UPDATE did not land on the primary"

    replica = open_replica(root, db, wal)

    assert {:ok, %{rows: [["ok"]]}} = Connection.query(replica, "PRAGMA integrity_check", [])
    assert {:ok, %{rows: [[1]]}} = Connection.query(replica, "SELECT count(*) FROM u", [])

    {:ok, %{rows: [[late_on_replica]]}} =
      Connection.query(replica, "SELECT count(*) FROM t WHERE b LIKE 'late-%'", [])

    # Pre-fix this was 400 of 400: every page of t after the first chunk was read post-checkpoint.
    assert late_on_replica == 0,
           "the replica holds #{late_on_replica} rows from a commit AFTER the declared WAL " <>
             "extent: a PASSIVE checkpoint backfilled them into the .db while it streamed"

    assert {:ok, %{rows: [[@rows]]}} =
             Connection.query(replica, "SELECT count(*) FROM t WHERE b LIKE 'orig-%'", [])
  end

  # The pinned read blocks every TRUNCATE checkpoint on the shard while it is held, so leaking it
  # would stall that shard's durability flush (and grow its WAL) indefinitely. Checked on both a
  # completed seed and one whose stream fails mid-way.
  test "the pinned read is released when the seed completes", ctx do
    {:ok, shipper} = FakeShipper.start_link(fn -> :ok end)

    assert {:ok, _} =
             seed_in_held_process(shipper, ctx.path, fn -> assert_truncate_completes(ctx.conn) end)
  end

  test "the pinned read is released when the stream fails mid-way", ctx do
    {:ok, shipper} = FakeShipper.start_link(fn -> {:error, :disconnected} end)

    assert {:error, :disconnected} =
             seed_in_held_process(shipper, ctx.path, fn -> assert_truncate_completes(ctx.conn) end)
  end

  # Runs the seed in a process that is KEPT ALIVE, and does not garbage-collect, until `check` has
  # run. Without that a leaked handle is closed whenever the GC finalizes its NIF resource, so a
  # missing close passed or failed with the seed (probed: removing the close failed 2 runs of 4).
  defp seed_in_held_process(shipper, path, check) do
    test = self()

    {pid, ref} =
      :erlang.spawn_opt(
        fn ->
          send(
            test,
            {:seeded, self(), Session.do_seed(shipper, "seedpin", path, path <> "-wal", 1, 0)}
          )

          receive do: (:release -> :ok)
        end,
        [:monitor, min_heap_size: 4_000_000, min_bin_vheap_size: 100_000_000]
      )

    result =
      receive do
        {:seeded, ^pid, result} -> result
        {:DOWN, ^ref, :process, ^pid, reason} -> flunk("seed process died: #{inspect(reason)}")
      after
        10_000 -> flunk("seed did not return")
      end

    try do
      check.()
    after
      send(pid, :release)
    end

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    result
  end

  defp assert_truncate_completes(conn) do
    :ok = Connection.set_busy_timeout(conn, 200)

    assert {:ok, %{rows: [[0, _, _]]}} =
             Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", []),
           "a TRUNCATE checkpoint is still blocked after the seed returned: the seed's read " <>
             "transaction was not released"
  end

  test "a seed of a shard with no .db fails without creating one", ctx do
    missing = Path.join(ctx.root, "absent.db")
    {:ok, shipper} = FakeShipper.start_link(fn -> :ok end)

    assert {:error, :enoent} =
             Session.do_seed(shipper, "seedpin", missing, missing <> "-wal", 1, 0)

    refute File.exists?(missing)
  end
end
