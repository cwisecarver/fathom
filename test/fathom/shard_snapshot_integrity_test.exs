defmodule Fathom.ShardSnapshotIntegrityTest do
  @moduledoc """
  The durability snapshot's two integrity holes (expert review 2026-08-01 #5 and #14).

  ## #5 — the live WAL was checkpointed and closed at `synchronous=OFF`

  `snapshot/2` relaxes `synchronous` so the throwaway VACUUM temp is not force-fsynced. That
  relaxation stayed in effect for the `wal_checkpoint(PASSIVE)` and the `Connection.close/1`
  that follow — and the function's own comment states the rule it was breaking: *"Emphatically
  NOT for `checkpoint/1`: a checkpoint at synchronous=OFF can corrupt the main database on
  power loss."*

  `synchronous` is a pager property, and SQLite derives checkpoint sync flags from it, so at
  OFF a checkpoint neither fsyncs the WAL before backfill nor fsyncs the main database after.
  Two checkpoints ran that way — the explicit PASSIVE, and SQLite's close-time checkpoint,
  which on the last connection to a WAL database also UNLINKS `-wal`/`-shm`. Pages written
  without fsync followed by deletion of the only recovery source is a torn database on power
  loss.

  **The #5 tests below do not discriminate, and that is stated rather than hidden.** They pass
  against the unfixed code, for a reason worth recording: the relaxation was always scoped to
  the snapshot's OWN short-lived connection, so no tenant connection and no later opener ever
  observed it. What was wrong is what that one connection then did — checkpoint and close a WAL
  database whose pager was at `synchronous=OFF` — and the damage only materialises on power
  loss during those few milliseconds, which a unit test cannot stage.

  So these are guards on the observable neighbours (the durability setting never escapes to a
  tenant connection; the database stays `quick_check`-clean across repeated flush cycles), and
  the fix itself rests on the ordering argument in `snapshot/2`'s comment. The #14 tests in the
  next block DO discriminate — 3 of them fail on the parent commit.

  ## #14 — the dominant flush path had no integrity gate

  Review 2026-07-14 #4 added a `quick_check` gate so a corrupted database is never flushed over
  the last good stored object — but only on the idle/terminate drop path. The PERIODIC flush,
  which produces most durable objects, went straight to the PUT. `VACUUM INTO` does not cover
  it: it REBUILDS indexes from table content, so index/table divergence is silently normalised
  away rather than detected, and the normalised file becomes the durable truth.

  Not async: shards are global and back onto real files.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.Stmt

  setup do
    shard = "snapint_#{System.unique_integer([:positive])}"
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)

    on_exit(fn ->
      restore(:shard_flush_interval_ms, prev_flush)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp db_path(shard), do: Path.join(Shard.data_dir(), "#{shard}.db")

  defp flush_now(coordinator) do
    send(coordinator, :durability_flush)
    wait_settled(coordinator, 400)
  end

  defp wait_settled(_c, 0), do: flunk("durability flush never settled")

  defp wait_settled(c, n) do
    case :sys.get_state(c) do
      %{flush_task: nil} ->
        :ok

      _ ->
        Process.sleep(5)
        wait_settled(c, n - 1)
    end
  end

  describe "#5 — synchronous must be restored before any checkpoint or close" do
    test "the live database survives a flush with its durability setting intact", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

      # An explicit index is what corrupt_index!/1 targets — see its comment.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE INDEX idx_kv_v ON kv(v)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'a')"))

      :ok = flush_now(coordinator)

      # The tenant's own connection must still be at FULL — the snapshot must never leave a
      # relaxed durability setting behind on anything that outlives it.
      assert {:ok, %{rows: [[2]]}} = ShardExecutor.execute(conn, stmt("PRAGMA synchronous")),
             "the live shard's durability setting was weakened by a durability flush"

      :ok = ShardExecutor.close(conn)
    end

    test "a fresh connection to the flushed shard reads synchronous=FULL", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

      # An explicit index is what corrupt_index!/1 targets — see its comment.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE INDEX idx_kv_v ON kv(v)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'b')"))
      :ok = flush_now(coordinator)
      :ok = ShardExecutor.close(conn)

      {:ok, c} = Connection.open(db_path(shard))
      assert {:ok, %{rows: [[2]]}} = Connection.query(c, "PRAGMA synchronous", [])
      :ok = Connection.close(c)
    end

    test "repeated flushes keep the database readable and intact", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

      # An explicit index is what corrupt_index!/1 targets — see its comment.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE INDEX idx_kv_v ON kv(v)"))

      for i <- 1..5 do
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?, ?)", [i, "v#{i}"]))
        :ok = flush_now(coordinator)
      end

      # Integrity of the live file after repeated snapshot cycles.
      assert Shard.verify_integrity(db_path(shard)) == :ok
      assert {:ok, %{rows: [[5]]}} = ShardExecutor.execute(conn, stmt("SELECT count(*) FROM kv"))

      :ok = ShardExecutor.close(conn)
    end
  end

  describe "#14 — the periodic flush must integrity-gate its snapshot" do
    test "a corrupt shard is not flushed over the last good stored object", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      # Establish a good stored object.
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

      # An explicit index is what corrupt_index!/1 targets — see its comment.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE INDEX idx_kv_v ON kv(v)"))
      seed_rows!(conn)
      :ok = flush_now(coordinator)
      :ok = ShardExecutor.close(conn)

      good_etag = :sys.get_state(coordinator).etag

      # Corrupt the live file underneath the coordinator, and mark it dirty so a flush runs.
      path = db_path(shard)
      corrupt_index!(path)

      # Sanity-check the fixture itself before asserting on behaviour: if this ever stops
      # failing, the test is measuring nothing.
      assert {:error, _} = Shard.verify_integrity(path),
             "the fixture did not actually corrupt the live database"

      send(coordinator, :write_counter_reset)
      _ = :sys.get_state(coordinator)

      log = capture_log(fn -> flush_now(coordinator) end)

      # The good object must still be the stored one.
      assert {:ok, ^good_etag} = Storage.object_etag(shard),
             "a corrupt snapshot was uploaded over the last good stored object"

      assert log =~ "quick_check", "the corrupt flush must be refused loudly"
    end

    # THIS TEST PREVIOUSLY ASSERTED THE OPPOSITE — that the periodic flush renames the live file to
    # `<path>.corrupt.<ts>` — and that mechanism is the data-loss defect expert review 2026-08-20 #2
    # closed. On the periodic path the coordinator keeps running and keeps handing `state.path` to
    # new checkouts, so renaming it means the next connection creates a brand-new EMPTY database at
    # that path; the following flush then snapshots the empty file and PUTs it over the good stored
    # object under a perfectly valid If-Match, destroying the only good copy.
    #
    # The old version never caught that because it closes its connection before corrupting, so
    # nothing re-opened the path. The rename is unsafe regardless of who happens to be connected:
    # the coordinator is still alive and registered, so a checkout can land the instant after the
    # rename. Quarantining is correct only on the DROP path, where the coordinator is terminating —
    # `Fathom.ShardCorruptFlushTest` still pins it there.
    #
    # The INVARIANT this test exists for is unchanged and still asserted: the corrupt bytes must be
    # preserved for forensics, not silently discarded. They now survive in place.
    test "the corrupt local copy is preserved in place while the shard is still served", %{
      shard: shard
    } do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

      # An explicit index is what corrupt_index!/1 targets — see its comment.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE INDEX idx_kv_v ON kv(v)"))
      seed_rows!(conn)
      :ok = flush_now(coordinator)
      :ok = ShardExecutor.close(conn)

      path = db_path(shard)
      corrupt_index!(path)
      send(coordinator, :write_counter_reset)
      _ = :sys.get_state(coordinator)

      capture_log(fn -> flush_now(coordinator) end)

      assert File.exists?(path),
             "the live shard file was moved aside while its coordinator was still serving"

      assert Path.wildcard(path <> ".corrupt.*") == [],
             "the serving path must not rename the live db — a new checkout would recreate it empty"

      assert {:error, _} = Shard.verify_integrity(path),
             "the corrupt copy must be preserved, not silently discarded or repaired"
    end

    test "corrupt-flush telemetry fires from the periodic path too", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)
      test_pid = self()
      handler = "corrupt-periodic-#{shard}"

      :telemetry.attach(
        handler,
        [:fathom, :shard, :corrupt_flush],
        fn _e, _m, meta, _ -> send(test_pid, {:corrupt, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

      # An explicit index is what corrupt_index!/1 targets — see its comment.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE INDEX idx_kv_v ON kv(v)"))
      seed_rows!(conn)
      :ok = flush_now(coordinator)
      :ok = ShardExecutor.close(conn)

      corrupt_index!(db_path(shard))
      send(coordinator, :write_counter_reset)
      _ = :sys.get_state(coordinator)

      capture_log(fn -> flush_now(coordinator) end)

      assert_receive {:corrupt, %{shard_id: ^shard}}, 2_000
    end

    test "a HEALTHY shard still flushes — the gate must not block the happy path", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

      # An explicit index is what corrupt_index!/1 targets — see its comment.
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE INDEX idx_kv_v ON kv(v)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'one')"))
      :ok = flush_now(coordinator)
      first = :sys.get_state(coordinator).etag

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (2, 'two')"))
      :ok = flush_now(coordinator)
      second = :sys.get_state(coordinator).etag

      refute first == second, "a healthy shard must keep flushing normally"
      refute Shard.dirty?(coordinator)

      :ok = ShardExecutor.close(conn)
    end
  end

  # Enough rows that the index's root page is densely packed with cells. With only a row or
  # two the page is mostly free space, and scribbling it changes nothing quick_check looks at —
  # the corruption fixture silently does nothing and the test passes with the gate removed.
  defp seed_rows!(conn) do
    for i <- 1..200 do
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?, ?)", [i, "val#{i}"]))
    end

    :ok
  end

  # Corrupt ONLY an index's root page.
  #
  # This specific shape is the point. Scribbling arbitrary pages makes `VACUUM INTO` itself
  # fail, so the flush aborts on the snapshot and the integrity gate is never reached — a test
  # built that way passes with the gate removed. Index/table divergence is the case the finding
  # is actually about, verified by probe:
  #
  #     quick_check on the live db       -> "Tree 3 page 3 cell 199: Offset … out of range …"
  #     VACUUM INTO                      -> OK
  #     quick_check on the VACUUMed copy -> "ok"   (all rows intact)
  #
  # So the snapshot is CLEAN and would upload happily; only a check against the live file
  # notices. Corrupting the index root also leaves the table b-tree readable, so the shard
  # still opens — real corruption, not a truncated file.
  defp corrupt_index!(path) do
    {:ok, c} = Connection.open(path)
    {:ok, %{rows: [[page_size]]}} = Connection.query(c, "PRAGMA page_size", [])

    {:ok, %{rows: [[rootpage]]}} =
      Connection.query(c, "SELECT rootpage FROM sqlite_master WHERE type='index' LIMIT 1", [])

    # Fold the WAL in first, so the corruption lands in the main file rather than being
    # masked by clean WAL frames.
    {:ok, _} = Connection.query(c, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    :ok = Connection.close(c)

    offset = (rootpage - 1) * page_size
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    # Past the page header, so the page still parses as an index page but its cells do not.
    :ok = :file.pwrite(fd, offset + 12, :binary.copy(<<0x5A>>, page_size - 200))
    :ok = :file.close(fd)
    :ok
  end
end
