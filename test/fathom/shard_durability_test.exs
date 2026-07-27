defmodule Fathom.ShardDurabilityTest do
  # The periodic durability flush: a busy shard (connection still open, never
  # idle) still gets a consistent snapshot pushed to storage, so the data-loss
  # window is bounded to the flush interval instead of the whole session. Fenced
  # like the idle flush. Not async: shards are global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, FlushGate, Storage}
  alias Filo.{Stmt, StmtResult}

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "dur_#{System.unique_integer([:positive])}"
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    on_exit(fn ->
      restore(:shard_flush_interval_ms, prev_flush)
      restore(:shard_idle_ms, prev_idle)

      for dir <- [@local_dir, @remote_dir],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for snap <- Path.wildcard(Path.join(@local_dir, "#{shard}.db.snap.*")), do: File.rm(snap)
      for f <- Path.wildcard(Path.join(@local_dir, "#{shard}.db.fenced.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  # Block until the Registry has processed the monitor DOWNs it owes, so `Registry.lookup/2` no
  # longer reports a process that has already died.
  #
  # A `Registry` unregisters asynchronously: its PID-partition processes monitor every registered
  # pid and remove the entry when they handle the `:DOWN`. That is the `:sys.get_state/1`
  # "ensure the process has handled prior messages" idiom from AGENTS.md — it just has to be
  # applied to the processes that actually do the work, which are neither the caller nor the
  # supervisor. Partition count scales with schedulers, so they're discovered rather than assumed.
  defp await_registry_cleanup do
    for name <- Process.registered(),
        Atom.to_string(name) =~ ~r/^Elixir\.Fathom\.ShardRegistry\.(PID)?Partition\d+$/ do
      _ = :sys.get_state(name)
    end

    :ok
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp now_ms, do: System.system_time(:millisecond)
  defp local_db(shard), do: Path.join(@local_dir, "#{shard}.db")
  defp remote_db(shard), do: Path.join(@remote_dir, "#{shard}.db")

  # Pull the shard's stored object to a fresh path and assert it is a valid, quick_check-clean
  # SQLite db holding `expected_rows` rows of table `t` (expert review #41).
  defp assert_stored_object_valid(shard, expected_rows) do
    dst = Path.join(System.tmp_dir!(), "fathom_verify_#{System.unique_integer([:positive])}.db")
    assert {:ok, _etag} = Storage.pull(shard, dst)
    assert Shard.verify_integrity(dst) == :ok, "the stored object is not a quick_check-clean db"

    {:ok, conn} = Connection.open(dst)
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT count(*) FROM t", [])
    :ok = Connection.close(conn)
    assert n == expected_rows

    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
  end

  defp dirty?(shard) do
    {:ok, pid} = Shards.ensure(shard)
    Shard.dirty?(pid)
  end

  defp close_and_stop(shard, conn) do
    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 2_000
  end

  defp put_raw_lock(shard, owner, epoch, expires_at_ms) do
    File.mkdir_p!(@remote_dir)

    File.write!(
      Path.join(@remote_dir, "#{shard}.lock"),
      Jason.encode!(%{"owner" => owner, "epoch" => epoch, "expires_at_ms" => expires_at_ms})
    )
  end

  # Drive exactly one durability flush and wait for it to settle. The snapshot +
  # upload now run in an off-process task (expert review #27), so syncing the
  # mailbox isn't enough — poll until the task has been consumed (bounded).
  defp flush_now(coordinator) do
    send(coordinator, :durability_flush)
    wait_flush_settled(coordinator, 400)
  end

  defp wait_flush_settled(_coordinator, 0), do: flunk("durability flush task never settled")

  defp wait_flush_settled(coordinator, tries) do
    case :sys.get_state(coordinator) do
      %{flush_task: nil} ->
        :ok

      _ ->
        Process.sleep(5)
        wait_flush_settled(coordinator, tries - 1)
    end
  end

  # Poll a predicate until true (bounded) — for an eventually-consistent effect delivered by an
  # async message (here, a monitor :DOWN the runtime enqueues after a process dies), which has no
  # clean monitor-based synchronization from the test's side. Mirrors wait_flush_settled's shape.
  defp until_true(_fun, 0), do: false

  defp until_true(fun, tries) do
    if fun.() do
      true
    else
      Process.sleep(5)
      until_true(fun, tries - 1)
    end
  end

  test "a durability flush snapshots committed writes to storage while a connection stays open",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))

    {:ok, coordinator} = Shards.ensure(shard)

    # Cold shard, connection still open: nothing has reached storage yet.
    refute File.exists?(remote_db(shard))

    flush_now(coordinator)

    # The consistent snapshot reached storage mid-session, and it captured the
    # committed row.
    assert File.exists?(remote_db(shard))

    {:ok, ro} = Connection.open(remote_db(shard))
    assert {:ok, %{rows: [["alice"]]}} = Connection.query(ro, "SELECT v FROM kv WHERE k = 1", [])
    Connection.close(ro)

    # The live connection is unaffected by the snapshot.
    assert {:ok, %StmtResult{}} =
             ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (2, 'bob')"))

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-14 #1 (residual): an explicit transaction whose LAST data statement
  # modifies 0 rows, straddling a periodic flush, must not lose its committed write on idle drop.
  # The INSERT bumps WriteCounter (uncommitted); a flush landing mid-transaction snapshots WITHOUT
  # the uncommitted row (VACUUM INTO on a separate connection) yet advances the watermark to that
  # count. The 0-row UPDATE and the COMMIT that inherits its changes()=0 both leave wrote?/2 false,
  # so — before the commit-boundary bump — count == flushed_through, the shard reads clean, and the
  # idle drop_clean deletes the local copy without uploading the committed row. End-to-end probe
  # (immune to the WriteCounter-restart generation backstop that confounds the dirty flag): drop
  # the shard, cold-pull from storage, and assert the committed row survived. (The common case,
  # where the last statement's changes()>0, already re-bumps via wrote?/2 — verified empirically;
  # this closes the 0-row-tail residual.)
  test "a committed transaction ending on a 0-row write, straddling a flush, is not lost on drop",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))

    {:ok, coordinator} = Shards.ensure(shard)
    # Clean baseline: flush the CREATE so flushed_through == count.
    flush_now(coordinator)
    refute dirty?(shard)

    # Open a transaction and write a row (bumps WriteCounter; uncommitted, invisible to a
    # separate-connection snapshot). Then a 0-row UPDATE — which still bumps (wrote?/2's DDL branch)
    # but crucially resets sqlite3_changes() to 0, so the COMMIT that later inherits it is 0-change.
    {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("UPDATE kv SET v = 'x' WHERE 1 = 0"))

    # A flush lands mid-transaction, AFTER the last bumping statement: its snapshot can't see the
    # uncommitted row, but it advances the watermark to the current count — so the shard now reads
    # clean past an uncommitted write, with no further statement able to re-dirty it before COMMIT.
    flush_now(coordinator)

    refute dirty?(shard),
           "the mid-transaction flush advances the watermark past the uncommitted row"

    # COMMIT inherits the 0-row UPDATE's changes()=0, so wrote?/2 is false — only the commit-boundary
    # bump re-dirties the shard. Without it, count == flushed_through and the shard reads clean.
    {:ok, _} = ShardExecutor.execute(conn, stmt("COMMIT"))

    # Close the connection: the shard idles (50 ms), flushes-and-drops, and stops. A clean shard
    # would drop WITHOUT uploading the committed row — losing it.
    close_and_stop(shard, conn)
    refute File.exists?(local_db(shard)), "idle drop removed the local copy"

    # Re-open cold-pulls from storage. The committed row must be there.
    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE k = 1")),
           "the committed row must survive the flush-straddle + idle drop"

    ShardExecutor.close(conn2)
  end

  # Expert review 2026-07-24 #3: the commit-boundary bump above used to be UNCONDITIONAL, so a
  # transaction in which NOTHING wrote still dirtied the shard. Django's ATOMIC_REQUESTS (and any
  # transaction.atomic() around a read view) wraps plain reads in BEGIN…COMMIT, so a purely
  # read-only tenant was dirty at every tick and paid a full VACUUM INTO + full-object PUT every
  # :shard_flush_interval_ms — defeating the write-gated flush whose whole point is that
  # "durability PUTs track WRITES, not open-shard count". Pins the read-only half of the boundary
  # rule; the test directly above pins the write half, and both must hold.
  test "a read-only transaction does not dirty the shard", %{shard: shard} do
    {:ok, setup_conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(setup_conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))
    {:ok, _} = ShardExecutor.execute(setup_conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))
    :ok = ShardExecutor.close(setup_conn)

    {:ok, coordinator} = Shards.ensure(shard)
    # Clean baseline: flush the seed so flushed_through == count.
    flush_now(coordinator)
    refute dirty?(shard)

    before = Fathom.Shard.WriteCounter.count(shard)

    # A fresh connection, so sqlite3_changes() starts at 0 and BEGIN cannot inherit the seed's
    # change count. This is the exact shape Django emits for a read view under ATOMIC_REQUESTS.
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE k = 1"))

    {:ok, _} = ShardExecutor.execute(conn, stmt("COMMIT"))

    assert Fathom.Shard.WriteCounter.count(shard) == before,
           "a BEGIN/SELECT/COMMIT wrote nothing, so the COMMIT must not bump the write counter"

    refute dirty?(shard),
           "a read-only transaction must leave the shard clean — otherwise every read-only tenant " <>
             "pays a full-file VACUUM + object PUT every flush interval"

    :ok = ShardExecutor.close(conn)
  end

  # The finding's REAL acceptance test: the same shape on a PERSISTENT connection that already
  # wrote — Django's recommended CONN_MAX_AGE keeps connections alive across requests, so this,
  # not the fresh-connection case, is the primary production topology. sqlite3_changes() is not
  # reset by SELECT/BEGIN/COMMIT/ROLLBACK, so before the classifier reorder the COMMIT inherited
  # changes()>0 from the earlier write and classified as a write forever after — every subsequent
  # read-only transaction re-dirtied the shard. Fails pre-reorder.
  test "a read-only transaction on a connection that already wrote does not dirty the shard", %{
    shard: shard
  } do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("COMMIT"))

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)
    refute dirty?(shard)

    before = Fathom.Shard.WriteCounter.count(shard)

    # Same long-lived connection, now serving a read view.
    {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE k = 1"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("COMMIT"))

    assert Fathom.Shard.WriteCounter.count(shard) == before,
           "a read-only transaction on a persistent connection must not inherit the earlier " <>
             "write's sqlite3_changes() and re-dirty the shard"

    refute dirty?(shard)
    :ok = ShardExecutor.close(conn)
  end

  # THE GUARD ON THE READ-VERB WHITELIST. `wrote?/2` ignores the inherited sqlite3_changes() for a
  # column-returning statement only when its leading verb is whitelisted (select/values/explain).
  # This test exists to fail if anyone ever "simplifies" that whitelist into the inverse blacklist
  # ("returns columns and is not dml? ⇒ read"): `dml?/1` is leading-keyword only and explicitly
  # does NOT detect a CTE-prefixed write, so under a blacklist this statement would classify as a
  # read, the shard would stay clean, and drop_clean would delete the committed rows. The only
  # thing classifying it as a write is the `changes > 0` branch. NEVER add `with` to @read_verbs.
  test "a CTE-prefixed INSERT ... RETURNING dirties the shard and survives an idle drop", %{
    shard: shard
  } do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"))

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)
    refute dirty?(shard)

    # Leading keyword is WITH, so dml?/1 does not see it — only changes>0 marks it a write.
    {:ok, _} =
      ShardExecutor.execute(
        conn,
        stmt("WITH x AS (SELECT 7 AS v) INSERT INTO t (v) SELECT v FROM x RETURNING id")
      )

    assert dirty?(shard), "a CTE-prefixed write must dirty the shard"

    close_and_stop(shard, conn)
    refute File.exists?(local_db(shard)), "idle drop removed the local copy"

    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [[7]]}} =
             ShardExecutor.execute(conn2, stmt("SELECT v FROM t")),
           "the CTE-prefixed write must survive the idle drop"

    ShardExecutor.close(conn2)
  end

  # The whitelist members themselves must stay clean on a connection that already wrote.
  test "whitelisted read verbs leave a written-on connection clean", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)
    refute dirty?(shard)

    before = Fathom.Shard.WriteCounter.count(shard)

    # Each of these inherits the INSERT's sqlite3_changes()=1 and must still classify as a read.
    for sql <- ["SELECT v FROM kv", "VALUES (1)", "EXPLAIN SELECT v FROM kv"] do
      {:ok, _} = ShardExecutor.execute(conn, stmt(sql))

      assert Fathom.Shard.WriteCounter.count(shard) == before,
             "#{sql} must not inherit the earlier write's change count"
    end

    refute dirty?(shard)
    :ok = ShardExecutor.close(conn)
  end

  # Savepoint-only commit boundary. A client using savepoints alone never issues a COMMIT, and
  # `ends_transaction?` matches only commit/end — so a flush straddling `SAVEPOINT a; INSERT;
  # <0-row UPDATE>; RELEASE a` advanced the watermark past the uncommitted INSERT with nothing to
  # re-dirty the shard, and the idle drop deleted it. RELEASE is now a commit boundary. Same
  # end-to-end probe as the 0-row-straddle test above: drop, cold-pull, assert the row survived.
  test "a savepoint released across a flush is not lost on drop", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)
    refute dirty?(shard)

    {:ok, _} = ShardExecutor.execute(conn, stmt("SAVEPOINT a"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))
    # Resets sqlite3_changes() to 0 so the RELEASE cannot ride inherited changes.
    {:ok, _} = ShardExecutor.execute(conn, stmt("UPDATE kv SET v = 'x' WHERE 1 = 0"))

    flush_now(coordinator)

    refute dirty?(shard),
           "the mid-savepoint flush advances the watermark past the uncommitted row"

    {:ok, _} = ShardExecutor.execute(conn, stmt("RELEASE a"))

    close_and_stop(shard, conn)
    refute File.exists?(local_db(shard)), "idle drop removed the local copy"

    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE k = 1")),
           "the released savepoint's row must survive the flush-straddle + idle drop"

    ShardExecutor.close(conn2)
  end

  # `ROLLBACK TO <savepoint>` does NOT end the transaction — it rewinds to the savepoint, leaving
  # every pre-savepoint write live and uncommitted. Treating it as a plain ROLLBACK cleared the
  # per-transaction write flag, so the eventual COMMIT skipped its boundary re-bump and a
  # straddling flush lost the pre-savepoint INSERT. This is Django's nested-atomic-block-raises,
  # outer-block-commits shape.
  test "a rollback to savepoint does not discard the outer transaction's write", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)
    refute dirty?(shard)

    {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("SAVEPOINT s"))
    # The inner block's work, then it "raises" — rewound, and it zeroes sqlite3_changes() so the
    # COMMIT cannot ride inherited changes.
    {:ok, _} = ShardExecutor.execute(conn, stmt("UPDATE kv SET v = 'x' WHERE 1 = 0"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("ROLLBACK TO s"))

    flush_now(coordinator)
    {:ok, _} = ShardExecutor.execute(conn, stmt("COMMIT"))

    close_and_stop(shard, conn)
    refute File.exists?(local_db(shard)), "idle drop removed the local copy"

    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE k = 1")),
           "the outer transaction's row must survive a ROLLBACK TO savepoint"

    ShardExecutor.close(conn2)
  end

  # Expert review 2026-07-14 #14: a stream holding a checked-out connection can be killed in the
  # window between a write's SQLite commit (fsynced to the local WAL) and its post-hoc
  # WriteCounter.bump — leaving a committed write durable-on-disk but UN-counted. The coordinator
  # can't observe the in-flight write, so on an ABNORMAL connection :DOWN it conservatively
  # force-dirties the shard, so an idle drop flushes rather than drop_cleaning the uncounted write.
  test "an abnormal connection death force-dirties the shard (no clean-drop of a possible write)",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)
    refute dirty?(shard)

    # A second stream checks out a connection (the coordinator monitors the caller), then dies
    # ABNORMALLY without a clean checkin — the lost-bump window.
    parent = self()

    holder =
      spawn(fn ->
        {:ok, _pid, _ref, _path} = Shards.checkout(shard)
        send(parent, :held)
        Process.sleep(:infinity)
      end)

    assert_receive :held, 1_000
    Process.exit(holder, :kill)

    # The coordinator force-dirties on the abnormal :DOWN (delivered async → poll).
    assert until_true(fn -> dirty?(shard) end, 100),
           "an abnormal connection death must force the shard dirty"

    ShardExecutor.close(conn)
  end

  test "the durability flush is fenced: a lost lease self-fences instead of clobbering",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    # Another node steals the lease before the periodic flush fires.
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      send(coordinator, :durability_flush)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 1_000
    end)

    refute File.exists?(remote_db(shard)),
           "a fenced durability flush must not clobber the new owner"
  end

  # Expert review 2026-07-14 #5: on a self-fence the coordinator must QUARANTINE its dirty local
  # copy (acked-but-unflushed committed writes) as `.fenced.<ts>`, not drop_local it — the same
  # class of data quarantine_fork! preserves. Requires conns == 0 so the lease_lost terminate clause
  # runs. Pre-fix the local .db was deleted (assertions below fail); post-fix it survives.
  test "a self-fence quarantines the dirty local copy instead of destroying it", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('acked')"))
    # Check the connection in (conns == 0) but keep the shard dirty and the coordinator alive
    # (idle timer is 60s), so the self-fence lands in the lease_lost terminate clause.
    :ok = ShardExecutor.close(conn)

    {:ok, coordinator} = Shards.ensure(shard)
    assert Shard.dirty?(coordinator)
    ref = Process.monitor(coordinator)

    # Another node steals the lease; the next flush's pre-flush fence self-fences.
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      send(coordinator, :durability_flush)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 1_000
    end)

    assert Path.wildcard(local_db(shard) <> ".fenced.*") != [],
           "acked-but-unflushed writes must survive a self-fence as .fenced.<ts>, not be dropped"

    refute File.exists?(local_db(shard)),
           "the superseded local .db was renamed aside, not left in place or deleted"
  end

  # Expert review 2026-07-18 #1: a self-fence with a stream STILL CHECKED OUT (conns > 0) — the
  # BUSY case, which is the likely one (self-fencing peaks under load) — hit the terminate/2
  # catch-all, which abandoned the dirty local <shard>.db in place with no quarantine, log, or
  # telemetry: acked-but-unflushed writes silently stranded at the normal path. The lease_lost
  # quarantine clause must cover conns > 0 too. Here the connection is kept open across the fence.
  test "a BUSY self-fence (connection still checked out) quarantines the dirty local copy",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('acked')"))

    {:ok, coordinator} = Shards.ensure(shard)
    assert Shard.dirty?(coordinator)
    # conns > 0: the connection is NEVER closed before the fence — the busy case.
    ref = Process.monitor(coordinator)

    test_pid = self()
    handler = "fenced-busy-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :fenced_quarantine],
      fn _e, meas, meta, _cfg -> send(test_pid, {:quarantined, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # Another node steals the lease; the durability flush's pre-flush fence self-fences.
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      send(coordinator, :durability_flush)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 1_000
    end)

    assert Path.wildcard(local_db(shard) <> ".fenced.*") != [],
           "a busy self-fence must quarantine acked-but-unflushed writes as .fenced.<ts>, not abandon them"

    refute File.exists?(local_db(shard)),
           "the superseded local .db was renamed aside, not left in place at the normal path"

    assert_receive {:quarantined, %{unflushed: unflushed}, %{shard_id: ^shard}}
    assert unflushed >= 1, "the busy self-fence must be as observable (telemetry) as the idle one"

    refute File.exists?(remote_db(shard)), "a fenced flush must not clobber the new owner"
  end

  # Expert review 2026-07-18 #4: a flush_now caller whose flush is still in flight when the
  # coordinator is force-stopped (a supervisor shutdown with conns > 0) used to get a bare :DOWN —
  # Shards.flush catches the exit and returns a FALSE :ok, and the keystone-fork (Tenants.fork
  # flush_source:) then forks stale bytes on the strength of that swallowed exit. Every terminate/2
  # clause must reply an explicit error to any pending flush_now waiter.
  test "a flush_now caller pending at a force-stop gets an explicit error, not a false :ok",
       %{shard: shard} do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    test_pid = self()

    # Block the flush's PUT so the flush task stays in flight and the flush_now waiter stays pending.
    blocker = fn ->
      send(test_pid, :flush_started)

      receive do
        :release -> :ok
      after
        10_000 -> :ok
      end
    end

    Application.put_env(:fathom, :faulty_before, {:flush, blocker})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('acked')"))
    {:ok, coordinator} = Shards.ensure(shard)

    # A pending flush_now: adds a waiter and kicks a flush that blocks in the PUT. The connection
    # stays open (conns > 0), so a force-stop lands in the catch-all terminate clause.
    flush = Task.async(fn -> Shards.flush(shard) end)
    assert_receive :flush_started, 2_000

    # Force-stop the coordinator while the flush is in flight and the waiter is pending.
    :ok = DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, coordinator)

    # Pre-fix: the waiter got a swallowed exit → Shards.flush returned :ok. Now: an explicit error.
    assert {:error, :coordinator_stopped} = Task.await(flush, 2_000)
  end

  # Expert review #17: after a failover/LB flip re-homes a burst of shards, their phase-aligned
  # flush timers would fire N snapshots + PUTs in lockstep and starve cold-open pulls. The node-wide
  # FlushGate cap bounds concurrent flushes — over the cap a coordinator backs off and STAYS DIRTY
  # (the safe direction, the flush just waits), then flushes once a slot frees.
  test "over the node-wide flush cap a coordinator backs off (stays dirty), then flushes when a slot frees",
       %{shard: shard} do
    prev_cap = Application.get_env(:fathom, :shard_flush_max_concurrency)
    Application.put_env(:fathom, :shard_flush_max_concurrency, 1)
    FlushGate.reset()
    on_exit(fn -> restore(:shard_flush_max_concurrency, prev_cap) end)
    on_exit(fn -> FlushGate.reset() end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('acked')"))
    {:ok, coordinator} = Shards.ensure(shard)
    assert Shard.dirty?(coordinator)

    # Occupy the node's only flush slot, as if a sibling shard were mid-flush.
    assert :ok = FlushGate.try_acquire()
    assert FlushGate.in_flight() == 1

    # This coordinator's flush is now over the cap: it must back off — no flush task spawned, the
    # shard stays dirty, and it does NOT consume a slot (the counter stays at the sibling's 1).
    send(coordinator, :durability_flush)
    _ = :sys.get_state(coordinator)
    assert :sys.get_state(coordinator).flush_task == nil, "a capped flush must not spawn a task"
    assert Shard.dirty?(coordinator), "a backed-off shard stays dirty (the safe direction)"
    assert FlushGate.in_flight() == 1, "a refused flush must not consume a slot"

    # Free the sibling's slot; the coordinator's next flush is admitted and the shard goes durable.
    FlushGate.release()
    flush_now(coordinator)
    refute Shard.dirty?(coordinator), "once a slot frees, the flush runs and the shard is clean"
    assert FlushGate.in_flight() == 0, "the admitted flush released its slot when it settled"
  end

  # Expert review 2026-07-24 #11: the durability snapshot's temp file inherits its connection's
  # safety_level, so at synchronous=FULL every flush force-fsynced a full shard-sized file that is
  # unlinked seconds later. The relaxation is scoped to the snapshot connection ONLY. This pins the
  # blast radius: a SERVING connection must still be FULL (that is the per-commit local RPO), and a
  # flush must not change that. Guards against anyone "simplifying" by hoisting the pragma into
  # Connection.open/1 or onto the checkpoint connection, where it could corrupt on power loss.
  test "the snapshot's synchronous relaxation does not leak to serving connections", %{
    shard: shard
  } do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1)"))

    # 2 == FULL. This is the live shard's per-commit durability guarantee.
    assert {:ok, %StmtResult{rows: [[2]]}} =
             ShardExecutor.execute(conn, stmt("PRAGMA synchronous"))

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)

    assert {:ok, %StmtResult{rows: [[2]]}} =
             ShardExecutor.execute(conn, stmt("PRAGMA synchronous")),
           "a durability flush must leave serving connections at synchronous=FULL"

    # A freshly opened connection too — the default itself must be unchanged.
    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [[2]]}} =
             ShardExecutor.execute(conn2, stmt("PRAGMA synchronous"))

    :ok = ShardExecutor.close(conn2)
    :ok = ShardExecutor.close(conn)
  end

  # Expert review 2026-07-24 #4: nothing truncated the WAL in steady state except SQLite's
  # autocheckpoint, which runs INLINE inside a committing tenant statement — a latency spike billed
  # to an arbitrary client query. Worse, the durability snapshot's VACUUM INTO is a long-lived
  # reader that holds that passive checkpoint back, so the WAL kept growing. The coordinator now
  # checkpoints PASSIVE after each snapshot, where no client is waiting. Observable: the WAL shrinks
  # across a flush rather than only growing.
  test "the durability flush checkpoints the WAL instead of leaving it to a tenant query", %{
    shard: shard
  } do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))

    # Enough writes to grow a WAL worth checkpointing, without tripping the 4000-frame backstop
    # (which would prove nothing — the point is that the COORDINATOR truncates it, not SQLite).
    for i <- 1..400 do
      {:ok, _} =
        ShardExecutor.execute(
          conn,
          stmt("INSERT INTO kv VALUES (?, ?)", [i, String.duplicate("x", 400)])
        )
    end

    wal = local_db(shard) <> "-wal"
    grown = File.stat!(wal).size
    assert grown > 0, "precondition: the writes produced a WAL"

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)

    # A PASSIVE checkpoint does not TRUNCATE the file — SQLite resets the write position and reuses
    # the space — so the observable is not that the WAL shrinks, it is that it STOPS GROWING: the
    # next batch of writes overwrites from the start instead of appending.
    for i <- 401..800 do
      {:ok, _} =
        ShardExecutor.execute(
          conn,
          stmt("INSERT INTO kv VALUES (?, ?)", [i, String.duplicate("x", 400)])
        )
    end

    reused = File.stat!(wal).size

    assert reused <= grown,
           "the WAL grew #{grown} -> #{reused} bytes across a flush, so the coordinator did not " <>
             "checkpoint — leaving it to whichever tenant query crosses the autocheckpoint " <>
             "threshold, inline and with a main-db fsync under synchronous=FULL"

    :ok = ShardExecutor.close(conn)
  end

  # Expert review 2026-07-24 #10: an idle AND clean coordinator used to re-arm the ~5s durability
  # timer forever — ~6,000 pure no-op wakeups/s at 30k coordinators/node, and (the bigger cost) it
  # made hibernate_after unusable at any value because a message always arrived within one interval.
  # Such a coordinator is provably unwritable: WriteCounter.bump/1 is reachable only from paths that
  # require a checked-out connection. Pins both halves — disarmed when idle+clean, re-armed on the
  # checkout grant (not on checkin, so the timer is live for the whole window a connection can write).
  test "an idle, clean coordinator carries no flush timer and re-arms on checkout", %{
    shard: shard
  } do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER)"))

    {:ok, pid} = Shards.ensure(shard)
    flush_now(pid)
    refute dirty?(shard)

    # Still busy (a connection is checked out) ⇒ the timer stays armed.
    assert :sys.get_state(pid).flush_timer != nil,
           "a coordinator with a live checkout must keep its flush timer"

    :ok = ShardExecutor.close(conn)

    # The disarm happens on the next tick, not at checkin: the already-armed timer fires once,
    # observes idle+clean, and declines to re-arm. So a coordinator self-quiesces after at most one
    # extra wakeup rather than paying one every interval forever.
    flush_now(pid)

    assert :sys.get_state(pid).flush_timer == nil,
           "an idle, clean coordinator must not burn a timer — it cannot become dirty until the " <>
             "next checkout"

    # The other half: a new checkout must bring the timer back, or the shard could write with no
    # scheduled flush.
    {:ok, conn2} = ShardExecutor.open(shard)

    assert :sys.get_state(pid).flush_timer != nil,
           "the checkout grant must re-arm the durability timer"

    :ok = ShardExecutor.close(conn2)
  end

  # The dirty counterpart: an idle-but-DIRTY coordinator must keep ticking, or its un-flushed
  # writes would sit unflushed until something else happened to wake it.
  test "an idle but dirty coordinator keeps its flush timer", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER)"))
    :ok = ShardExecutor.close(conn)

    {:ok, pid} = Shards.ensure(shard)
    _ = :sys.get_state(pid)

    assert dirty?(shard), "the CREATE TABLE left the shard dirty"

    assert :sys.get_state(pid).flush_timer != nil,
           "an idle but DIRTY coordinator must keep ticking so its writes still reach storage"
  end

  # Expert review #17: the flush timer is jittered so a mass re-home's phase-aligned timers drift
  # apart instead of firing in lockstep. Pin that a scheduled delay stays within ±ratio of the
  # interval (bounded, never 0/negative) and actually varies across coordinators.
  test "the periodic flush timer is jittered within ±ratio and varies across coordinators" do
    prev_jitter = Application.get_env(:fathom, :shard_flush_jitter_ratio)
    prev_interval = Application.get_env(:fathom, :shard_flush_interval_ms)
    interval = 4_000
    Application.put_env(:fathom, :shard_flush_jitter_ratio, 0.5)
    Application.put_env(:fathom, :shard_flush_interval_ms, interval)

    ids = for i <- 1..6, do: "jit_#{System.unique_integer([:positive])}_#{i}"

    on_exit(fn ->
      restore(:shard_flush_jitter_ratio, prev_jitter)
      restore(:shard_flush_interval_ms, prev_interval)

      for id <- ids, do: _ = Shards.drain(id)

      for id <- ids,
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(@local_dir, id <> suffix))
    end)

    # Hold a connection on each: an idle+clean coordinator deliberately carries NO flush timer
    # (review 2026-07-24 #10), and the jitter this test is about only matters for shards that can
    # actually flush. Opening a stream is also what re-arms the timer, so this covers that path.
    conns =
      for id <- ids do
        {:ok, conn} = ShardExecutor.open(id)
        conn
      end

    on_exit(fn -> for c <- conns, do: ShardExecutor.close(c) end)

    remaining =
      for id <- ids do
        {:ok, pid} = Shards.ensure(id)
        # read_timer returns ms left on the armed flush timer (< the jittered delay by the tiny
        # elapsed time since open) — the observable of the jittered schedule.
        Process.read_timer(:sys.get_state(pid).flush_timer)
      end

    # Bounded: within [interval*(1-ratio), interval*(1+ratio)] = [2000, 6000], minus a little for
    # elapsed time — and never 0/negative.
    for ms <- remaining do
      assert is_integer(ms) and ms > 0,
             "a jittered delay must be positive (never fire immediately)"

      assert ms >= 1_800 and ms <= 6_000, "delay #{ms} outside the ±50% jitter band"
    end

    assert Enum.uniq(remaining) |> length() >= 2,
           "jitter must actually spread the timers, not return one fixed value (#{inspect(remaining)})"
  end

  # Expert review #14: Shards.flush wrapped flush_now in a BLANKET `catch :exit, _ -> :ok`, which
  # also swallowed the GenServer.call TIMEOUT exit — a hung/slow store (or a live-writer livelock)
  # past @flush_now_timeout returned :ok while the shard was NOT durably clean, so a fork/export
  # "after flush" (Tenants.fork flush_source:, the API flush endpoint) cloned stale bytes. The catch
  # now narrows the timeout to {:error, :flush_timeout}, keeping :ok only for the coordinator that
  # legitimately went away.
  test "a hung coordinator makes Shards.flush return {:error, :flush_timeout}, not a false :ok",
       %{shard: shard} do
    prev_timeout = Application.get_env(:fathom, :flush_now_timeout_ms)
    # A short flush_now timeout so a suspended coordinator times out fast (not the 60s default);
    # a high idle so the coordinator can't idle-drop out from under the test.
    Application.put_env(:fathom, :flush_now_timeout_ms, 100)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    on_exit(fn -> restore(:flush_now_timeout_ms, prev_timeout) end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('acked')"))
    {:ok, coordinator} = Shards.ensure(shard)
    assert Shard.dirty?(coordinator), "the shard must be dirty so the flush is meaningful"

    # Suspend the coordinator so flush_now's GenServer.call can NEVER be answered — the hung/slow-
    # store (or live-writer livelock) case. Pre-fix Shards.flush swallowed the {:timeout, _} exit
    # as :ok; now it surfaces the timeout.
    :sys.suspend(coordinator)

    try do
      assert {:error, :flush_timeout} = Shards.flush(shard),
             "a flush that timed out must not report durable success"
    after
      :sys.resume(coordinator)
    end

    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    _ = Shards.drain(shard)

    receive do
      {:DOWN, ^ref, :process, ^coordinator, _} -> :ok
    after
      2_000 -> :ok
    end
  end

  # Expert review 2026-07-14 #41: pin the invariant "every flushed object is an openable,
  # quick_check-clean SQLite database" across the flush paths that run in the default suite —
  # the checkpoint-then-raw-upload (idle drop) and the VACUUM-INTO snapshot (periodic flush).
  # A regression in either (e.g. uploading before a checkpoint folds) would ship corrupt
  # "backups" noticed only at restore time.
  test "the idle-drop checkpoint+raw-upload flush stores a valid, quick_check-clean db", %{
    shard: shard
  } do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES ('a'), ('b')"))
    :ok = ShardExecutor.close(conn)

    # Idle drop → flush_then_drop → upload_for_drop → checkpoint ok → raw upload.
    :ok = Shards.drain(shard, 10_000)

    assert_stored_object_valid(shard, 2)
  end

  test "the periodic VACUUM-INTO durability flush stores a valid, quick_check-clean db", %{
    shard: shard
  } do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES ('a'), ('b'), ('c')"))

    {:ok, coordinator} = Shards.ensure(shard)
    # Busy shard (connection still open) → the periodic flush snapshots via VACUUM INTO.
    flush_now(coordinator)

    assert_stored_object_valid(shard, 3)
    ShardExecutor.close(conn)
  end

  # Finding #15: the LEASE fence (above) passes if the lease is still ours when the flush
  # STARTS — but the data PUT itself was unconditional, so a steal DURING the upload (a long
  # snapshot/PUT outliving the lease margin) still let a stale PUT clobber the new owner. The
  # fenced flush If-Matches the object etag, so a mid-flush change surfaces as :superseded and
  # the coordinator self-fences instead of overwriting. Here the object is stolen from inside
  # the flush call (FaultyStorage :faulty_before hook), after the fence check has passed.
  test "the durability flush is etag-fenced: an object stolen mid-flush self-fences, no clobber",
       %{shard: shard} do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    # During the flush PUT, a REAL stealer takes the lock (a new owner/epoch) AND
    # overwrites the shard object. Both matter now (expert review #2): a 412 with the
    # lock still ours is treated as our own lost-but-applied PUT, so a genuine steal
    # must also bump the lock for the coordinator to self-fence.
    steal = fn ->
      put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)
      File.write!(remote_db(shard), "stolen-by-a-newer-owner")
    end

    Application.put_env(:fathom, :faulty_before, {:flush, steal})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    capture_log(fn ->
      send(coordinator, :durability_flush)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 1_000
    end)

    assert File.read!(remote_db(shard)) == "stolen-by-a-newer-owner",
           "the coordinator must not clobber the object a stealer wrote during the flush"
  end

  # Round-2 expert review #2: a data-PUT 412 was unconditionally read as "a stealer
  # flushed" → self-fence + drop_local (unrecoverable). But our OWN PUT can land
  # server-side while its response is lost (Req retries the idempotent PUT, and the
  # retry 412s against our first attempt's own write); the next flush then fences with
  # our stale etag and 412s against our own durable bytes — with NO steal. The
  # invariant: a 412 with the LOCK still ours must NOT self-fence off a shard we own.
  # Simulated by changing the object etag mid-flush WITHOUT taking the lock.
  test "a durability-flush 412 with the lock still ours does not self-fence", %{shard: shard} do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    # Our own PUT landed (lost response): the object etag changes under us so the fenced
    # flush 412s — but NO stealer took the lock; it is still ours.
    own_put = fn -> File.write!(remote_db(shard), "our-own-landed-snapshot") end
    Application.put_env(:fathom, :faulty_before, {:flush, own_put})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    capture_log(fn ->
      send(coordinator, :durability_flush)
      # Pre-fix: self-fenced ({:shutdown, :lease_lost}) off a shard it still owns.
      refute_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 500
    end)

    assert Process.alive?(coordinator), "the coordinator must keep owning a shard it never lost"

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn, stmt("SELECT v FROM kv")),
           "the shard keeps serving; its acknowledged writes are intact"

    ShardExecutor.close(conn)
  end

  # Finding #27: dirtiness is a write-counter watermark, not a boolean cleared after the flush. A
  # write that lands DURING the (blocking) snapshot/upload must keep the shard dirty — the flush
  # captures the counter BEFORE snapshotting and only advances the watermark to that, so a mid-flush
  # bump stays ahead of it and re-flushes next interval. An unconditional clear would silently drop
  # that write. Here the write is simulated by bumping the counter from inside the flush call.
  test "a write landing during a flush keeps the shard dirty (watermark, not a boolean)",
       %{shard: shard} do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    Application.put_env(
      :fathom,
      :faulty_before,
      {:flush, fn -> Fathom.Shard.WriteCounter.bump(shard) end}
    )

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)

    assert Shard.dirty?(coordinator),
           "a write during the flush must keep the shard dirty, not be cleared by the flush"

    ShardExecutor.close(conn)
  end

  # Expert review #27: the durability flush ran inline in the coordinator's
  # handle_info — fence RTT + full-shard VACUUM INTO + full-object PUT — so every
  # :checkout for the shard queued behind seconds of I/O, a recurring p99 spike on
  # exactly the hot (write-active) shards. The invariant: the coordinator keeps
  # serving checkouts while a flush's upload is in flight.
  test "a checkout is served while a durability flush is in flight", %{shard: shard} do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_timeout = Application.get_env(:fathom, :shard_checkout_timeout_ms)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_checkout_timeout_ms, 500)

    test_pid = self()

    # Block the flush's PUT until released — the coordinator must stay responsive.
    blocker = fn ->
      send(test_pid, :flush_started)

      receive do
        :release_flush -> :ok
      after
        10_000 -> :ok
      end
    end

    Application.put_env(:fathom, :faulty_before, {:flush, blocker})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      restore(:shard_checkout_timeout_ms, prev_timeout)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    send(coordinator, :durability_flush)
    assert_receive :flush_started, 2_000

    # The upload is wedged mid-flight; a checkout must still be served promptly.
    # Pre-fix the coordinator was blocked inside handle_info and this timed out.
    task = Task.async(fn -> Shards.checkout(shard) end)
    assert {:ok, pid, ref, _path} = Task.await(task, 2_000)
    Fathom.Shard.checkin(pid, ref)

    # Release the flush and let it settle; the write reached storage.
    send(flush_task_pid(coordinator), :release_flush)
    wait_flush_settled(coordinator, 400)
    assert File.exists?(remote_db(shard))

    ShardExecutor.close(conn)
  end

  defp flush_task_pid(coordinator) do
    case :sys.get_state(coordinator) do
      %{flush_task: %Task{pid: pid}} when is_pid(pid) -> pid
      _ -> flunk("no flush task in flight")
    end
  end

  # Expert review 2026-07-14 #8: #27 moved the flush's snapshot/upload off-process, but on
  # a data-PUT 412 the RECONCILE (Storage.check_lease GET + object_etag HEAD) still ran IN
  # the coordinator's flush-result handler — so a 412 (correlated with the very S3 flakiness
  # that makes those calls slow) re-blocked the coordinator mailbox for 1-2 S3 RTTs, the
  # exact stall #27 removed, just relocated onto the 412 path. The reconcile now runs inside
  # the flush task. The invariant: the coordinator keeps serving checkouts while a 412
  # reconcile's lock re-check is in flight — and the 412-lock-still-ours outcome (resync,
  # keep serving) is preserved.
  test "a checkout is served while a durability-flush 412 reconcile is in flight",
       %{shard: shard} do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_timeout = Application.get_env(:fathom, :shard_checkout_timeout_ms)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_checkout_timeout_ms, 500)

    test_pid = self()

    # 412 with the lock still ours: our own PUT lands (the object etag changes ⇒ the fenced
    # flush 412s) but NO stealer takes the lock ⇒ the reconcile's check_lease returns :ok and
    # the task resolves {:reconciled, etag} (keep serving).
    own_put = fn -> File.write!(remote_db(shard), "our-own-landed-snapshot") end

    # Wedge the reconcile's lock re-check (the S3 GET) mid-flight: signal the moment it
    # starts, then block until released — a slow/flaky check_lease, held deterministically.
    reconcile_block = fn ->
      send(test_pid, {:reconcile_started, self()})

      receive do
        :release_reconcile -> :ok
      after
        10_000 -> :ok
      end
    end

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      Application.delete_env(:fathom, :faulty_check_lease)
      restore(:shard_checkout_timeout_ms, prev_timeout)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    Application.put_env(:fathom, :faulty_before, {:flush, own_put})
    Application.put_env(:fathom, :faulty_check_lease, reconcile_block)
    send(coordinator, :durability_flush)

    # The task has uploaded (412) and is now blocked IN the reconcile's lock re-check —
    # off the coordinator process. Pre-fix this ran in the flush-result handler.
    assert_receive {:reconcile_started, reconcile_pid}, 2_000

    # The coordinator must serve a checkout promptly despite the wedged reconcile. Pre-fix
    # it was blocked inside handle_info and this timed out (500ms checkout timeout).
    checkout = Task.async(fn -> Shards.checkout(shard) end)
    assert {:ok, pid, checkout_ref, _path} = Task.await(checkout, 2_000)
    Fathom.Shard.checkin(pid, checkout_ref)

    # Release the reconcile: lock still ours ⇒ resolve {:reconciled, etag}, keep serving.
    send(reconcile_pid, :release_reconcile)
    wait_flush_settled(coordinator, 400)

    refute_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 200
    assert Process.alive?(coordinator), "a 412 with the lock still ours must not self-fence"

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn, stmt("SELECT v FROM kv")),
           "the shard keeps serving; its acknowledged writes are intact"

    ShardExecutor.close(conn)
  end

  test "a zero interval disables the periodic flush timer", %{shard: shard} do
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)

    assert %{flush_timer: nil} = :sys.get_state(coordinator)

    ShardExecutor.close(conn)
  end

  # --- dirty-flag / durability-storm fix ---

  test "a read leaves the shard clean; a write dirties it", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, _} = ShardExecutor.execute(conn, stmt("SELECT 1"))
    refute dirty?(shard), "a read must not dirty the shard"

    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    assert dirty?(shard), "a write (DDL here) dirties the shard"

    close_and_stop(shard, conn)
  end

  test "INSERT ... RETURNING dirties the shard so idle-drop flushes it, not loses it",
       %{shard: shard} do
    # Regression: SQLite's RETURNING makes a write return columns, and the classifier
    # tested columns before num_changes — so a RETURNING insert looked like a read, the
    # shard never went dirty, and idle-drop deleted the only copy. Django emits
    # `INSERT ... RETURNING "id"` on every object save, so this lost data on the common path.
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)

    # Reach a clean baseline: create the table and flush it to storage. This isolates
    # the RETURNING insert as the ONLY thing that can re-dirty the shard afterward —
    # otherwise the CREATE TABLE's own dirtying would mask the classifier bug.
    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)"))

    flush_now(coordinator)
    refute dirty?(shard), "baseline: the shard is clean after a flush"

    {:ok, %StmtResult{}} =
      ShardExecutor.execute(conn, stmt("INSERT INTO kv (v) VALUES ('alice') RETURNING id"))

    assert dirty?(shard), "a RETURNING insert must mark the shard dirty"

    # Round-trip: closing the last connection idle-stops the coordinator, which (being
    # dirty) flushes to storage before dropping the local copy; reopening pulls it back.
    # Pre-fix the shard stayed clean, so the local copy was dropped un-flushed and the
    # RETURNING row was lost.
    close_and_stop(shard, conn)

    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn2, stmt("SELECT v FROM kv")),
           "the RETURNING-inserted row must survive an idle-drop/reopen cycle"

    close_and_stop(shard, conn2)
  end

  test "a dirty durability flush uploads and clears the flag; a clean one skips",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))
    assert dirty?(shard)

    {:ok, coordinator} = Shards.ensure(shard)
    flush_now(coordinator)
    assert File.exists?(remote_db(shard)), "a dirty durability flush uploads"
    refute dirty?(shard), "the flush clears the dirty flag"

    # Now clean: remove the object and flush again — a clean flush must NOT recreate it.
    File.rm!(remote_db(shard))
    flush_now(coordinator)
    refute File.exists?(remote_db(shard)), "a clean durability flush skips the upload"

    close_and_stop(shard, conn)
  end

  test "a failed durability upload keeps the shard dirty so a later idle-drop doesn't lose it",
       %{shard: shard} do
    # Regression: the periodic flush cleared `dirty` unconditionally, even when the
    # snapshot/upload failed (the error was only logged). A transient storage blip then
    # left the shard "clean" with un-flushed writes, and idle-drop deleted the only copy.
    Application.put_env(:fathom, :shard_idle_ms, 50)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))
    assert dirty?(shard)

    {:ok, coordinator} = Shards.ensure(shard)

    # The upload fails: the shard MUST stay dirty (retry next interval, flush before drop).
    Application.put_env(:fathom, :storage_fault, :flush)

    capture_log(fn -> flush_now(coordinator) end)

    assert dirty?(shard), "a failed durability upload must NOT clear the dirty flag"
    refute File.exists?(remote_db(shard)), "the failed upload left nothing in storage"

    # Storage recovers: the next flush uploads the still-dirty state and clears the flag.
    Application.delete_env(:fathom, :storage_fault)
    flush_now(coordinator)

    assert File.exists?(remote_db(shard)), "the retry uploads the un-lost writes"
    refute dirty?(shard), "a successful flush clears the flag"

    {:ok, ro} = Connection.open(remote_db(shard))
    assert {:ok, %{rows: [["alice"]]}} = Connection.query(ro, "SELECT v FROM kv", [])
    Connection.close(ro)

    close_and_stop(shard, conn)
  end

  # Expert review #27: a persistent flush failure (S3 auth / bucket-policy change) must be
  # ALERTABLE, not just a per-interval Logger.warning that lets the RPO grow silently. The
  # coordinator counts consecutive failures, emits [:fathom, :shard, :flush, :failed] with the
  # running count, escalates to Logger.error past :flush_failure_alert_threshold, and RESETS the
  # count on a durable flush. Pre-fix there was no telemetry, no escalation, and no counter.
  test "consecutive flush failures emit escalating telemetry and reset on recovery",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_threshold = Application.get_env(:fathom, :flush_failure_alert_threshold)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    # Threshold 2 keeps the test short: escalation fires on the 2nd consecutive failure.
    Application.put_env(:fathom, :flush_failure_alert_threshold, 2)

    test_pid = self()
    handler = "flushfail-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :failed],
      fn _e, meas, meta, _cfg -> send(test_pid, {:flush_failed, meas, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Application.delete_env(:fathom, :storage_fault)
      restore(:flush_failure_alert_threshold, prev_threshold)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))
    {:ok, coordinator} = Shards.ensure(shard)

    Application.put_env(:fathom, :storage_fault, :flush)

    # First failure: consecutive == 1, below threshold (no error escalation).
    capture_log(fn -> flush_now(coordinator) end)
    assert_receive {:flush_failed, %{count: 1, consecutive: 1}, %{shard_id: ^shard}}

    # Second failure: consecutive == 2, hits the threshold ⇒ escalate to Logger.error.
    log = capture_log(fn -> flush_now(coordinator) end)
    assert_receive {:flush_failed, %{count: 1, consecutive: 2}, %{shard_id: ^shard}}
    assert log =~ "FAILED 2× consecutively"

    # Storage recovers: a durable flush RESETS the counter and emits NO failure event.
    Application.delete_env(:fathom, :storage_fault)
    flush_now(coordinator)
    refute_receive {:flush_failed, _, _}, 100

    # A fresh write + failure starts the count over at 1 — proving the reset.
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('bob')"))
    Application.put_env(:fathom, :storage_fault, :flush)
    capture_log(fn -> flush_now(coordinator) end)
    assert_receive {:flush_failed, %{consecutive: 1}, %{shard_id: ^shard}}

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-18 #8: an ownership-unconfirmed flush skip (Fence.check :skip — a
  # transient lease/heartbeat store error, not a lost lease) grows the RPO exactly like a PUT
  # failure, but used to produce only a per-interval Logger.warning: no telemetry, no escalation,
  # nothing alertable. It must now feed the SAME record_flush_failure path — consecutive counter +
  # [:fathom,:shard,:flush,:failed] telemetry + escalation, reset on a durable flush. Test env runs
  # legacy-mode fences (heartbeat off), so faulting renew_lease makes Fence.check return :skip
  # BEFORE any write — the ownership-unconfirmed path, distinct from the post-fence PUT failure the
  # sibling test covers.
  test "an ownership-unconfirmed flush skip feeds the RPO-alerting telemetry and escalates (#8)",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_threshold = Application.get_env(:fathom, :flush_failure_alert_threshold)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :flush_failure_alert_threshold, 2)

    test_pid = self()
    handler = "skipfail-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :failed],
      fn _e, meas, meta, _cfg -> send(test_pid, {:flush_failed, meas, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Application.delete_env(:fathom, :storage_fault)
      restore(:flush_failure_alert_threshold, prev_threshold)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))
    {:ok, coordinator} = Shards.ensure(shard)

    # Fault the lease RENEW (not the flush PUT): in legacy mode the fence renews the lease as its
    # ownership proof, so a transient renew error makes Fence.check return :skip before any write.
    Application.put_env(:fathom, :storage_fault, :renew)

    # First skip: consecutive == 1 (pre-fix: no telemetry at all — assert_receive would time out).
    capture_log(fn -> flush_now(coordinator) end)
    assert_receive {:flush_failed, %{count: 1, consecutive: 1}, %{shard_id: ^shard}}

    # Second skip: consecutive == 2, hits the threshold ⇒ escalate to Logger.error carrying the
    # distinct :ownership_unconfirmed reason (proving it's the skip path, not a PUT failure).
    log = capture_log(fn -> flush_now(coordinator) end)
    assert_receive {:flush_failed, %{count: 1, consecutive: 2}, %{shard_id: ^shard}}
    assert log =~ "FAILED 2× consecutively"
    assert log =~ ":ownership_unconfirmed"

    # Ownership reconfirmed: a durable flush RESETS the counter and emits NO failure event — a skip
    # participates in the same reset-on-recovery contract as a PUT failure. Use the SYNCHRONOUS
    # flush_now (blocks until the shard is durably clean) so the reset is observable deterministically:
    # a windowed `refute_receive` on the async `:durability_flush` was racy under full-suite load (it
    # occasionally caught a re-scheduled skip and flaked). The counter reset is the recovery contract;
    # any failure event would already be in the mailbox by the time the sync flush returns.
    Application.delete_env(:fathom, :storage_fault)
    assert :ok = Shard.flush_now(coordinator)

    assert :sys.get_state(coordinator).flush_failures == 0,
           "a durable flush must reset the consecutive-failure counter (recovery contract)"

    refute_received {:flush_failed, _, _}

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-18 #18: in legacy mode (heartbeat down — the test env default) the
  # pre-flush fence does a renew PUT. Running it inline in handle_info(:durability_flush) blocked the
  # coordinator mailbox a storage RTT every flush — the p99 checkout stall #27 removed, reintroduced
  # for the degraded mode. The fence now runs in the flush task, so a slow renew leaves the
  # coordinator free to serve other messages.
  test "a slow legacy-mode fence renew does not block the coordinator mailbox (#18)",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :storage_renew_delay_ms, 500)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_renew_delay_ms)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, coordinator} = Shards.ensure(shard)

    # Trigger a periodic flush: the coordinator spawns the fence+flush task, whose legacy renew now
    # sleeps 500 ms OFF-process. A :sys.get_state queued right behind must return promptly — pre-fix
    # the inline renew blocked the mailbox for ~500 ms, so it would take that long.
    send(coordinator, :durability_flush)

    started = System.monotonic_time(:millisecond)
    _ = :sys.get_state(coordinator)
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 200,
           "coordinator blocked #{elapsed}ms on the fence renew (should run off-process)"

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-18 #20: the periodic flush emits a cost signal — [:fathom,:shard,:flush]
  # with the VACUUM-INTO+PUT duration + outcome — so operators can watch flush rate/cost and
  # `mix fathom.rpo --cost` can quantify the cost side of the interval knob.
  test "a successful flush emits the flush-cost telemetry with a duration (#20)", %{shard: shard} do
    test_pid = self()
    handler = "flushcost-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush],
      fn _e, meas, meta, _ -> send(test_pid, {:flush, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, coordinator} = Shards.ensure(shard)

    flush_now(coordinator)

    assert_receive {:flush, %{duration: d}, %{shard_id: ^shard, outcome: :uploaded}}
    assert is_integer(d) and d > 0

    ShardExecutor.close(conn)
  end

  test "a supervisor shutdown flushes via terminate/2 instead of losing the write",
       %{shard: shard} do
    # Regression: the coordinator didn't trap exits, so a supervisor `:shutdown` (SIGTERM,
    # rolling deploy) killed it WITHOUT running terminate/2 — the final flush never ran and
    # writes since the last periodic flush were lost. Disable the periodic flush and use a
    # long idle window so ONLY terminate/2 can push the write to storage.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    assert Shard.dirty?(coordinator)

    # Close the connection: conns -> 0, but no idle stop (long window) and no periodic
    # flush (interval 0), so nothing has reached storage yet.
    :ok = ShardExecutor.close(conn)
    _ = :sys.get_state(coordinator)
    refute File.exists?(remote_db(shard)), "nothing flushed before the shutdown"

    # Supervisor-initiated shutdown — the SIGTERM/deploy path (parent terminates child).
    ref = Process.monitor(coordinator)
    :ok = DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, coordinator)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 2_000

    assert File.exists?(remote_db(shard)), "terminate/2 flushed the write on shutdown"

    {:ok, ro} = Connection.open(remote_db(shard))
    assert {:ok, %{rows: [["alice"]]}} = Connection.query(ro, "SELECT v FROM kv", [])
    Connection.close(ro)
  end

  # Expert review 2026-07-19 #2: the terminate/2 catch-all (connections still open) flushed NOTHING.
  # A rolling deploy enters terminate on the supervisor EXIT *before* the killed streams' :DOWNs are
  # processed, so conns > 0 is the COMMON shutdown state — and a dirty shard there lost up to a full
  # flush interval of acked writes on ephemeral disk, the very loss trap_exit + the shutdown budget
  # exist to prevent. The existing shutdown test above closes the connection first (the conns == 0
  # clause). This pins the conns > 0 clause: a clean stop flushes the acked writes to storage whether
  # or not a connection is still checked out.
  test "a supervisor shutdown flushes a dirty shard even with a connection still checked out (#2)",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    _ = :sys.get_state(coordinator)
    assert Shard.dirty?(coordinator)

    # The connection stays CHECKED OUT — the pre-fix data-loss condition. Assert conns > 0 so this
    # genuinely exercises the catch-all clause, not the idle (conns == 0) path, and nothing has
    # reached storage yet (periodic flush off, long idle window).
    assert map_size(:sys.get_state(coordinator).conns) > 0,
           "the connection must still be checked out (conns > 0)"

    refute File.exists?(remote_db(shard)), "nothing flushed before the shutdown"

    ref = Process.monitor(coordinator)
    :ok = DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, coordinator)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000

    assert File.exists?(remote_db(shard)),
           "terminate/2 must flush the write on shutdown even with a connection checked out"

    {:ok, ro} = Connection.open(remote_db(shard))
    assert {:ok, %{rows: [["alice"]]}} = Connection.query(ro, "SELECT v FROM kv", [])
    Connection.close(ro)
  end

  # The other intent of the same clause: a BUSY tenant delete (`Tenants.purge` → `Shards.stop`
  # force-terminates with conns > 0, landing here). The id is tombstoned before the stop, so the
  # flush must be SKIPPED — the object is about to be purged, and re-uploading it is wasted work.
  # Pins that the #2 flush is gated on the tombstone so the delete path is unchanged.
  test "a busy stop of a TOMBSTONED shard skips the flush (delete path unchanged) (#2)",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    _ = :sys.get_state(coordinator)
    assert Shard.dirty?(coordinator)

    # Tombstone AFTER opening (admission refuses a tombstoned open) — the real delete order, where the
    # coordinator is already serving when `Tenants.delete` tombstones it ahead of the force-stop.
    Fathom.Tenants.Tombstones.put(shard)
    on_exit(fn -> :ets.delete(Fathom.Tenants.Tombstones, shard) end)

    refute File.exists?(remote_db(shard)), "nothing flushed before the stop"

    ref = Process.monitor(coordinator)
    :ok = DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, coordinator)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000

    refute File.exists?(remote_db(shard)),
           "a tombstoned (being-deleted) shard must NOT re-upload on stop"
  end

  # Expert review #2: `wal_checkpoint(TRUNCATE)` reports "couldn't fold the WAL" as a
  # busy=1 ROW, not an error — and the pre-drop checkpoint discarded the result. A blocked
  # checkpoint (here: a lingering reader transaction, the zombie-stream class) then let the
  # idle flush upload a main-file image MISSING the committed WAL frames and delete the WAL:
  # silent loss of acknowledged writes with a "successful" flush in the logs. The violated
  # invariant: whatever blocks the checkpoint, a flush-and-drop must persist every
  # acknowledged write (snapshot fallback).
  test "an idle flush with a blocked checkpoint still captures WAL-resident writes",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    # A zombie reader: a second connection holding an open read transaction pins its WAL
    # read mark, so the checkpoint can neither copy frames written after that mark into
    # the main file nor truncate the log (busy=1). Opened directly on the shard file —
    # the coordinator doesn't know about it, like a brutally-killed stream's connection
    # awaiting NIF-resource GC.
    {:ok, zombie} = Connection.open(local_db(shard))
    {:ok, _} = Connection.query(zombie, "BEGIN", [])
    {:ok, _} = Connection.query(zombie, "SELECT count(*) FROM kv", [])
    on_exit(fn -> Connection.close(zombie) end)

    # Committed AFTER the zombie's snapshot: this write is beyond the pinned read mark,
    # so the blocked checkpoint cannot fold it into the main file — it exists only in
    # the WAL that drop_local/1 is about to delete.
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('bob')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    capture_log(fn ->
      :ok = ShardExecutor.close(conn)
      # The blocked checkpoint waits out its (shortened) busy timeout before falling back.
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 10_000
    end)

    assert File.exists?(remote_db(shard)),
           "the idle flush must upload despite the blocked checkpoint"

    {:ok, ro} = Connection.open(remote_db(shard))

    assert {:ok, %{rows: [["alice"], ["bob"]]}} =
             Connection.query(ro, "SELECT v FROM kv ORDER BY v", []),
           "WAL-resident committed rows must be captured by the flush"

    Connection.close(ro)
  end

  # Expert review #18: promote_pull renamed the pulled temp onto `path` without removing
  # pre-existing `-wal`/`-shm` sidecars, and drop_local deletes the db FIRST, wal second —
  # so a crash between those two rm calls leaves an orphan WAL. The next cold open pulled
  # a fresh object, renamed it into place, and SQLite's first open ran WAL recovery
  # against the stale, different-generation WAL — replaying old frames into the freshly
  # pulled database (resurrected data, torn pages, or a malformed db, then flushed back
  # as the durable object). The invariant: a pulled object is a self-contained
  # checkpointed image; stale sidecars must never survive its promotion.
  test "a cold open onto an orphaned stale WAL does not contaminate the pulled object",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # Build a WAL from a PREVIOUS generation of this shard: write through a raw
    # connection (frames live in -wal), snapshot the -wal aside before close
    # (close checkpoints + truncates it), then simulate the crash artifact:
    # db deleted, stale -wal left behind.
    path = local_db(shard)
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE stale (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO stale VALUES ('old-generation')")
    stale_wal = path <> ".stale_wal_copy"
    File.cp!(path <> "-wal", stale_wal)
    Connection.close(c)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    File.rename!(stale_wal, path <> "-wal")

    # The durable object is a different, self-contained image (the truth to serve).
    seed = Path.join(System.tmp_dir!(), "seed18_#{shard}.db")
    {:ok, sc} = Connection.open(seed)
    :ok = Connection.exec(sc, "CREATE TABLE good (v TEXT)")
    :ok = Connection.exec(sc, "INSERT INTO good VALUES ('fresh-pull')")
    :ok = Connection.exec(sc, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(sc)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    # Cold open (db absent ⇒ pull + promote). The pulled image must be served intact.
    {:ok, conn} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [["ok"]]}} =
             ShardExecutor.execute(conn, stmt("PRAGMA integrity_check")),
           "the stale WAL must not corrupt the pulled database"

    assert {:ok, %StmtResult{rows: [["fresh-pull"]]}} =
             ShardExecutor.execute(conn, stmt("SELECT v FROM good")),
           "the pulled object's content must be served, not the old generation's"

    close_and_stop(shard, conn)
  end

  # Expert review #15: `PRAGMA user_version = N` writes the database header but reports
  # num_changes == 0 and no columns, and the blanket "pragma is control" classification
  # treated it as a read — the shard stayed clean, idle drop_clean deleted the local copy
  # without a flush, and the acknowledged version stamp silently vanished (a shard then
  # looks un-migrated after a flush-and-drop cycle). The invariant: durable header writes
  # dirty the shard; the bare read form and connection-local assignments stay clean.
  test "PRAGMA user_version assignment dirties the shard and survives an idle drop",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)

    # Clean baseline so the pragma is the ONLY thing that can re-dirty the shard.
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    flush_now(coordinator)
    refute dirty?(shard), "baseline: clean after a flush"

    # The read form and a connection-local assignment must NOT dirty.
    {:ok, _} = ShardExecutor.execute(conn, stmt("PRAGMA user_version"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("PRAGMA busy_timeout = 9000"))
    refute dirty?(shard), "reads and connection-local pragmas must stay clean"

    # The durable header write must dirty.
    {:ok, _} = ShardExecutor.execute(conn, stmt("PRAGMA user_version = 42"))
    assert dirty?(shard), "a user_version stamp is a durable write"

    # Round-trip: idle drop must flush the stamp, not lose it.
    close_and_stop(shard, conn)
    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [[42]]}} =
             ShardExecutor.execute(conn2, stmt("PRAGMA user_version")),
           "the version stamp must survive an idle-drop/reopen cycle"

    close_and_stop(shard, conn2)
  end

  # Expert review #1 (the panel's one CRITICAL): a warm restart fenced its first flush
  # with the store's CURRENT etag (await_pull's HEAD), not the etag its local copy
  # derives from. Node A crashes with un-flushed writes; its shard is stolen, written,
  # and RELEASED elsewhere; A restarts, sees its local file, adopts the store's (newer)
  # etag, is seeded dirty — and within one flush interval uploads its stale fork with a
  # VALID If-Match, silently destroying the other node's acknowledged, durably-flushed
  # writes. The invariant: the local copy's provenance (an etag sidecar written at pull/
  # flush time) decides — a mismatched lineage is quarantined and re-pulled, never
  # served or flushed.
  test "a warm restart cannot adopt a newer lineage and clobber it", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    on_exit(fn -> Enum.each(Path.wildcard(local_db(shard) <> ".forked*"), &File.rm_rf/1) end)

    # The shard's original lineage in storage.
    seed = Path.join(System.tmp_dir!(), "seed1_#{shard}.db")
    {:ok, c} = Connection.open(seed)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('base')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    # Node "A": cold-pull, write locally, then CRASH — local file (with A's un-flushed
    # write) left on disk, nothing flushed.
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a-fork')"))
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 1_000
    _ = :sys.get_state(Fathom.ShardSupervisor)
    ShardExecutor.close(conn)

    # Node "B" (elsewhere): steals, serves, flushes NEW acknowledged writes, releases.
    b = Path.join(System.tmp_dir!(), "seedb_#{shard}.db")
    {:ok, cb} = Connection.open(b)
    :ok = Connection.exec(cb, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(cb, "INSERT INTO kv VALUES ('b-write')")
    :ok = Connection.exec(cb, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(cb)
    :ok = Storage.flush(shard, b)
    for s <- ["", "-wal", "-shm"], do: File.rm(b <> s)
    File.rm(Path.join(@remote_dir, "#{shard}.lock"))

    # "A" returns: the warm open must detect the fork, quarantine A's copy, and serve
    # B's lineage — pre-fix it adopted B's etag and later flushed A's fork over it.
    capture_log(fn ->
      {:ok, conn2} = ShardExecutor.open(shard)

      assert {:ok, %StmtResult{rows: [["b-write"]]}} =
               ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE v = 'b-write'")),
             "the returning node must serve the stored lineage, not its stale fork"

      assert {:ok, %StmtResult{rows: []}} =
               ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE v = 'a-fork'"))

      assert Path.wildcard(local_db(shard) <> ".forked.*") != [],
             "the forked local copy must be quarantined for recovery, not deleted"

      close_and_stop(shard, conn2)
    end)

    # After the full idle flush cycle, B's acknowledged write is still the durable truth.
    {:ok, ro} = Connection.open(remote_db(shard))

    assert {:ok, %{rows: [["b-write"]]}} =
             Connection.query(ro, "SELECT v FROM kv WHERE v = 'b-write'", []),
           "the stored lineage must never be clobbered by the returning node's fork"

    Connection.close(ro)
  end

  # Expert review #12: the sidecar is a plain O_TRUNC File.write, and power loss
  # classically leaves a ZERO-LENGTH file. read_etag_sidecar mapped {:ok, ""} to
  # :missing — the legacy no-provenance branch that ADOPTS the store's current etag —
  # so a crashed node whose shard was stolen/written/released elsewhere came back,
  # adopted the newer lineage's etag, and its next flush clobbered it with a valid
  # If-Match: the exact #1 clobber, through the crash-consistency hole in #1's own
  # safe-direction argument. The invariant: only a truly ABSENT sidecar is legacy;
  # an empty/unreadable one is unknown provenance and must quarantine.
  test "a torn-to-empty sidecar quarantines the local copy instead of adopting the store's lineage",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    on_exit(fn -> Enum.each(Path.wildcard(local_db(shard) <> ".forked*"), &File.rm_rf/1) end)

    # Node "A": open a brand-new shard, write locally, then CRASH — local file left.
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a-fork')"))
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 1_000
    _ = :sys.get_state(Fathom.ShardSupervisor)
    ShardExecutor.close(conn)

    # Node "B" (elsewhere): steals, flushes NEW acknowledged writes, releases.
    b = Path.join(System.tmp_dir!(), "seedb_#{shard}.db")
    {:ok, cb} = Connection.open(b)
    :ok = Connection.exec(cb, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(cb, "INSERT INTO kv VALUES ('b-write')")
    :ok = Connection.exec(cb, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(cb)
    :ok = Storage.flush(shard, b)
    for s <- ["", "-wal", "-shm"], do: File.rm(b <> s)
    File.rm(Path.join(@remote_dir, "#{shard}.lock"))

    # The crash tore A's sidecar to EMPTY (O_TRUNC + power loss).
    File.write!(local_db(shard) <> ".etag", "")

    # "A" returns: unknown provenance must quarantine and re-pull B's lineage —
    # pre-fix the empty sidecar read as legacy-missing, adopted B's etag, and the
    # idle flush clobbered B's acknowledged write with a valid If-Match.
    capture_log(fn ->
      {:ok, conn2} = ShardExecutor.open(shard)

      assert {:ok, %StmtResult{rows: [["b-write"]]}} =
               ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE v = 'b-write'")),
             "the returning node must serve the stored lineage, not its unknown-provenance fork"

      assert {:ok, %StmtResult{rows: []}} =
               ShardExecutor.execute(conn2, stmt("SELECT v FROM kv WHERE v = 'a-fork'"))

      assert Path.wildcard(local_db(shard) <> ".forked.*") != [],
             "the unknown-provenance local copy must be quarantined for recovery, not deleted"

      close_and_stop(shard, conn2)
    end)

    # After the full idle flush cycle, B's acknowledged write is still the durable truth.
    {:ok, ro} = Connection.open(remote_db(shard))

    assert {:ok, %{rows: [["b-write"]]}} =
             Connection.query(ro, "SELECT v FROM kv WHERE v = 'b-write'", []),
           "the stored lineage must never be clobbered off a torn sidecar"

    Connection.close(ro)
  end

  # Expert review round-2 #14: quarantine_fork! used a FIXED `.forked` name and rm'd it
  # first — a node that forks the same shard twice (crash-looping in exactly the
  # stolen/written/released environment that produces forks) silently destroyed the
  # first quarantine, whose sole purpose is preserving acknowledged writes. The
  # invariant: every quarantine gets a unique name; a later fork never deletes an
  # earlier fork's recovery copy.
  test "a second fork does not destroy the first fork's recovery copy", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    on_exit(fn -> Enum.each(Path.wildcard(local_db(shard) <> ".forked*"), &File.rm_rf/1) end)

    # The stored lineage.
    seed = Path.join(System.tmp_dir!(), "seed14_#{shard}.db")
    {:ok, c} = Connection.open(seed)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('base')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    # Two successive forks: a stale local copy whose provenance sidecar mismatches the
    # stored object. Each open must quarantine it and serve the stored lineage.
    for fork_value <- ["fork-one", "fork-two"] do
      path = local_db(shard)
      File.mkdir_p!(Path.dirname(path))
      {:ok, cf} = Connection.open(path)
      :ok = Connection.exec(cf, "CREATE TABLE kv (v TEXT)")
      :ok = Connection.exec(cf, "INSERT INTO kv VALUES ('#{fork_value}')")
      :ok = Connection.exec(cf, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(cf)
      File.write!(path <> ".etag", "bogus-provenance-#{fork_value}")

      capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(shard)

        assert {:ok, %StmtResult{rows: [["base"]]}} =
                 ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))

        close_and_stop(shard, conn)
      end)
    end

    recovery_copies =
      Path.wildcard(local_db(shard) <> ".forked.*")
      |> Enum.reject(&(String.ends_with?(&1, "-wal") or String.ends_with?(&1, "-shm")))

    assert length(recovery_copies) == 2,
           "both forks' recovery copies must survive; found #{inspect(recovery_copies)}"

    values =
      Enum.map(recovery_copies, fn f ->
        {:ok, ro} = Connection.open(f)
        {:ok, %{rows: [[v]]}} = Connection.query(ro, "SELECT v FROM kv", [])
        Connection.close(ro)
        v
      end)

    assert Enum.sort(values) == ["fork-one", "fork-two"]
  end

  # Round-2 #19 (merged M15): the provenance/fork check runs BEFORE the lease
  # acquire, and in that gap another owner can do a FULL acquire→flush→release
  # cycle. Release deletes the lock, so our acquire is a fresh epoch-1 create with
  # no took_over — the takeover revalidation was skipped entirely, and the warm
  # local file (whose sidecar matched the store at check time) served a STALE
  # lineage over the newer flushed one, losing its first-cycle accepted writes at
  # the eventual self-fence. The invariant: EVERY warm open re-checks provenance
  # after the lease is held; a moved lineage quarantines and re-pulls.
  test "a full owner cycle in the fork-check-to-acquire gap cannot serve the stale warm file",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    on_exit(fn -> Enum.each(Path.wildcard(local_db(shard) <> ".forked*"), &File.rm_rf/1) end)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    # The stored lineage E1, and a warm local copy honestly derived from it.
    seed = Path.join(System.tmp_dir!(), "seed19_#{shard}.db")
    {:ok, c} = Connection.open(seed)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('e1')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, seed)

    File.mkdir_p!(Path.dirname(local_db(shard)))
    File.cp!(seed, local_db(shard))
    {:ok, e1} = Storage.object_etag(shard)
    File.write!(local_db(shard) <> ".etag", e1)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    # In the gap between the fork check and OUR acquire, another owner runs a
    # full cycle: acquire, flush E2, clean release (lock deleted).
    e2_seed = Path.join(System.tmp_dir!(), "seed19b_#{shard}.db")

    cycle = fn ->
      {:ok, c2} = Connection.open(e2_seed)
      :ok = Connection.exec(c2, "CREATE TABLE kv (v TEXT)")
      :ok = Connection.exec(c2, "INSERT INTO kv VALUES ('e2')")
      :ok = Connection.exec(c2, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(c2)
      :ok = Fathom.Shard.Storage.Local.flush(shard, e2_seed)
      for s <- ["", "-wal", "-shm"], do: File.rm(e2_seed <> s)
      File.rm(Path.join(@remote_dir, "#{shard}.lock"))
    end

    Application.put_env(:fathom, :faulty_before, {:acquire, cycle})

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)

      # The store's newer lineage must be served — pre-fix the stale warm file won.
      assert {:ok, %StmtResult{rows: [["e2"]]}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv")),
             "a lineage that moved in the fork-check-to-acquire gap must be re-pulled"

      assert Path.wildcard(local_db(shard) <> ".forked.*") != [],
             "the stale warm copy must be quarantined for recovery"

      close_and_stop(shard, conn)
    end)
  end

  # Expert review round-2 #29: on a takeover, revalidate_takeover's {:ok, nil}
  # branch (the object VANISHED between the speculative pull and the re-check)
  # returned the STALE pulled etag — so the first flush deterministically
  # self-fenced (If-Match against a gone object never succeeds) and every write
  # accepted in that cycle was dropped. The invariant: a vanished object means the
  # brand-new contract — fence with nil and let the first flush RECREATE it.
  test "a takeover whose object vanished mid-open recreates it instead of self-fencing",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    # The old owner's lineage in storage, and its long-dead lock (no heartbeat, TTL
    # far expired) — this open is a TAKEOVER (took_over: true).
    seed = Path.join(System.tmp_dir!(), "seed29_#{shard}.db")
    {:ok, c} = Connection.open(seed)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('old')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)
    put_raw_lock(shard, "dead@node", 3, now_ms() - 60_000)

    # Mid-open, after the speculative pull captured the bytes but before the
    # takeover revalidation: the object vanishes (deleted by an operator/lifecycle).
    pull_temp = local_db(shard) <> ".pull"

    vanish = fn ->
      # The pull runs concurrently with this acquire — wait for its temp, then
      # delete the object so the revalidation's etag read sees nothing.
      Enum.reduce_while(1..400, :ok, fn _, _ ->
        if File.exists?(pull_temp) or File.exists?(local_db(shard)),
          do: {:halt, :ok},
          else: Process.sleep(5) && {:cont, :ok}
      end)

      File.rm(remote_db(shard))
    end

    Application.put_env(:fathom, :faulty_before, {:acquire, vanish})

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)

      # Serving the pulled copy, and accepting writes.
      assert {:ok, %StmtResult{rows: [["old"]]}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('mine')"))

      close_and_stop(shard, conn)
    end)

    # The idle flush must have RECREATED the object with the accepted write —
    # pre-fix it fenced on the vanished etag, self-fenced, and dropped it.
    assert File.exists?(remote_db(shard)), "the first flush must recreate a vanished object"

    {:ok, ro} = Connection.open(remote_db(shard))

    assert {:ok, %{rows: [["mine"]]}} =
             Connection.query(ro, "SELECT v FROM kv WHERE v = 'mine'", []),
           "writes accepted after a vanished-object takeover must be durable"

    Connection.close(ro)
  end

  # Expert review 2026-07-14 #5: promote_pull made the pulled temp authoritative
  # (File.rename temp -> path) and wrote the `.etag` provenance sidecar only AFTERWARD,
  # so a crash in that window left an authoritative warm `.db` with NO sidecar. The next
  # warm open read :missing (quarantined_fork? -> false, await_pull(nil) -> ADOPT the
  # store's CURRENT etag) — so a stale local copy (another node stole+wrote+released in
  # between, reachable on a persisted `:shard_data_dir`) got served and its first flush
  # If-Match-clobbered the newer lineage (the #1 clobber, through the promote's crash
  # hole). The fix writes the sidecar BEFORE the rename, inverting the residue to a
  # harmless orphan sidecar. The observable invariant it establishes: whenever a promote
  # makes a `.db` authoritative, the sidecar exists and records the PULLED object's etag
  # exactly — never missing, never the ambiguous adopt-current fallback.
  test "a cold-open promote establishes the provenance sidecar matching the pulled object",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # Seed the shard's object in storage so opening is a cold PULL + promote.
    seed = Path.join(System.tmp_dir!(), "seed5_#{shard}.db")
    {:ok, c} = Connection.open(seed)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('base')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    {:ok, object_etag} = Storage.object_etag(shard)

    {:ok, conn} = ShardExecutor.open(shard)

    # The authoritative local `.db` landed with its provenance sidecar alongside it —
    # never sidecar-less after a promote (the #5 crash residue is now unreachable).
    assert File.exists?(local_db(shard))

    sidecar = local_db(shard) <> ".etag"
    assert File.exists?(sidecar), "a promoted `.db` must never be sidecar-less (#5)"

    # And the sidecar records the PULLED object's etag exactly — so a later warm restart
    # fences with the real lineage via await_pull(nil), never the adopt-current path the
    # sidecar-less residue forced open.
    assert File.read!(sidecar) == object_etag,
           "the sidecar must record the pulled object's etag, not adopt-current"

    close_and_stop(shard, conn)
  end

  # Expert review 2026-07-14 #21: flush_then_drop's success branch was
  # `{:ok, _new_etag} -> drop_local(...)` — it discarded the new etag because it was
  # about to delete everything. But a crash after the upload lands (object now at
  # new_etag) and before drop_local completes leaves the local `.db` with its sidecar
  # still holding the OLD etag, while the local db equals the object we just uploaded
  # (identical bytes). The next warm open then reads sidecar(old) != object(new) and
  # FALSE-quarantines a FORK — an ERROR log, a [:fathom,:shard,:forked] telemetry, and
  # a leaked `.forked.<ts>` copy — for a clean crash-recovery of identical bytes. This
  # pins the pre-fix residue: a STALE-etag sidecar over identical bytes spuriously forks.
  test "a stale-etag sidecar over identical bytes false-quarantines a fork (the #21 residue)",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    on_exit(fn -> Enum.each(Path.wildcard(local_db(shard) <> ".forked*"), &File.rm_rf/1) end)

    # The flushed bytes B: what BOTH the local `.db` and the object hold after the upload.
    path = local_db(shard)
    File.mkdir_p!(Path.dirname(path))
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('alice')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    for s <- ["-wal", "-shm"], do: File.rm(path <> s)

    # Upload B: the object is now at new_etag = content_etag(B), bytes identical to the
    # local `.db` — the exact post-upload, pre-drop state of flush_then_drop.
    :ok = Storage.flush(shard, path)

    # PRE-FIX residue: the sidecar was never advanced past the OLD pre-flush etag.
    File.write!(path <> ".etag", "stale-pre-flush-etag")

    handler = attach_forked(shard)
    on_exit(fn -> :telemetry.detach(handler) end)

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)

      # The bytes survive (re-pulled after the spurious quarantine), but at the cost of
      # a false fork.
      assert {:ok, %StmtResult{rows: [["alice"]]}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))

      close_and_stop(shard, conn)
    end)

    assert_receive {:forked, ^shard},
                   1_000,
                   "a stale-etag sidecar over identical bytes spuriously forks (the #21 bug)"

    assert Path.wildcard(path <> ".forked.*") != [],
           "the false fork leaks a `.forked` recovery copy"
  end

  # The counterpart to the residue above: what the #21 fix leaves. Stamping new_etag
  # into the sidecar BEFORE drop_local means a crash-after-upload leaves the local `.db`
  # with a FRESH sidecar matching the object — so the next warm open is an ordinary clean
  # restart: no fork telemetry, no quarantine, the bytes served warm.
  test "a fresh-etag sidecar (the #21 fix's residue) restarts clean with no fork",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    on_exit(fn -> Enum.each(Path.wildcard(local_db(shard) <> ".forked*"), &File.rm_rf/1) end)

    path = local_db(shard)
    File.mkdir_p!(Path.dirname(path))
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('alice')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    for s <- ["-wal", "-shm"], do: File.rm(path <> s)

    :ok = Storage.flush(shard, path)
    {:ok, new_etag} = Storage.object_etag(shard)

    # POST-FIX residue: the sidecar was advanced to new_etag before the (interrupted) drop.
    File.write!(path <> ".etag", new_etag)

    handler = attach_forked(shard)
    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, conn} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn, stmt("SELECT v FROM kv")),
           "the fresh-sidecar warm restart serves the recovered bytes"

    refute_receive {:forked, ^shard},
                   200,
                   "a fresh-etag sidecar over identical bytes must NOT false-fork (the #21 fix)"

    refute Path.wildcard(path <> ".forked.*") != [],
           "a clean warm restart must not quarantine a recovery copy"

    close_and_stop(shard, conn)
  end

  # Attach a telemetry handler that forwards each fork quarantine to the test process,
  # tagged with the shard id, so the residue tests can assert on false-fork emission.
  defp attach_forked(shard) do
    test_pid = self()
    handler_id = "forked-#{shard}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:fathom, :shard, :forked],
      fn _event, _measure, %{shard_id: sid}, _cfg -> send(test_pid, {:forked, sid}) end,
      nil
    )

    handler_id
  end

  # Expert review 2026-07-14 #2: the per-open temp reap used to `Path.wildcard`
  # `<base>.{dl,snap,tmp,pull}*`, which can't prefix-optimize a `*`-in-filename pattern
  # and so full-`readdir`d the fleet-sized shard data dir on EVERY open (tens-to-hundreds
  # of ms at 30k–105k shards). A cold open now clears only its shard's DETERMINISTIC
  # `.pull` family by direct name (O(1)) — which is ALSO the collision guard, since a
  # stale `.pull` would be adopted by promote_pull/2 as the shard's db. The
  # uniquely-suffixed `.dl.*`/`.snap.*` orphans (expert review round-2 #27) are the
  # amortized Fathom.Shard.TempReaper's job, NOT the open's — so they must SURVIVE the
  # open, which is exactly how this pins that the open no longer scans the directory.
  test "a coordinator open reaps its shard's stale pull temps by direct name, not a dir scan",
       %{shard: shard} do
    path = local_db(shard)
    File.mkdir_p!(Path.dirname(path))

    stale_pull = path <> ".pull"
    stale_pull_wal = path <> ".pull-wal"
    stale_dl = path <> ".dl.99"
    stale_snap = path <> ".snap.7"

    for f <- [stale_pull, stale_pull_wal, stale_dl, stale_snap],
        do: File.write!(f, "orphaned temp")

    # All predate any plausible in-flight work (age-gated reap).
    for f <- [stale_pull, stale_pull_wal, stale_dl, stale_snap],
        do: File.touch!(f, {{2020, 1, 1}, {0, 0, 0}})

    on_exit(fn -> Enum.each([stale_pull, stale_pull_wal, stale_dl, stale_snap], &File.rm/1) end)

    {:ok, conn} = ShardExecutor.open(shard)

    refute File.exists?(stale_pull),
           "a stale orphaned .pull temp must be reaped at open (direct name + collision guard)"

    refute File.exists?(stale_pull_wal), "the .pull-wal companion must be reaped too"

    # The load-bearing pin: the open must NOT enumerate the directory to find these —
    # they are swept off the hot path by Fathom.Shard.TempReaper. This assertion FAILS
    # against the old glob-based per-open reap and PASSES against the O(1) direct-name reap.
    assert File.exists?(stale_dl),
           "a cold open must NOT dir-scan for uniquely-suffixed temps (that is TempReaper's job)"

    assert File.exists?(stale_snap),
           "same — .snap orphans are swept off the hot path, not at open"

    ShardExecutor.close(conn)
  end

  # Expert review #14: the WriteCounter's ETS table dies with its owner, and a restart
  # handed every open coordinator a FRESH EMPTY table — count(id) = 0 — while each kept
  # its old flushed_through watermark, so `count > flushed_through` classified every
  # dirty shard on the node as clean and the idle path took drop_clean: the local copy
  # deleted WITHOUT an upload, all in-flight unflushed writes silently lost. The
  # invariant: a counter reset is unknown state, and unknown state must flush.
  test "a WriteCounter restart cannot reclassify dirty shards as clean", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))
    assert dirty?(shard)

    # The owner crashes; the supervisor restarts it with a fresh empty table.
    wc = Process.whereis(Fathom.Shard.WriteCounter)
    ref = Process.monitor(wc)
    Process.exit(wc, :kill)
    assert_receive {:DOWN, ^ref, :process, ^wc, :killed}, 1_000

    capture_log(fn ->
      assert wait_restarted(Fathom.Shard.WriteCounter, wc), "WriteCounter should restart"
      # The restarted counter notified registered coordinators during init; sync the
      # coordinator so the reset message has been processed before we ask.
      {:ok, coordinator} = Shards.ensure(shard)
      _ = :sys.get_state(coordinator)

      assert Shard.dirty?(coordinator),
             "a counter reset must leave the shard dirty (unknown state ⇒ flush)"

      # And the idle stop must FLUSH (upload) rather than drop_clean.
      close_and_stop(shard, conn)
    end)

    assert File.exists?(remote_db(shard)),
           "idle stop after a counter reset must upload, not drop the only copy"

    {:ok, ro} = Connection.open(remote_db(shard))
    assert {:ok, %{rows: [["alice"]]}} = Connection.query(ro, "SELECT v FROM kv", [])
    Connection.close(ro)
  end

  # Expert review #11 (#14 × #27): the reset broadcast set flushed_through: -1, but an
  # IN-FLIGHT flush task restores flushed_through from flush_pending when it replies — a
  # watermark captured from the OLD dead table (e.g. 3) while the restarted table counts
  # from 0. The shard then compared clean until that many NEW writes accumulated:
  # durability flushes skipped, and the idle stop took drop_clean — the local copy
  # deleted WITHOUT uploading acknowledged post-restart writes. The invariant: a
  # watermark is only meaningful against the counter table it was captured from
  # (unflushed?/1 compares the WriteCounter generation; any mismatch ⇒ dirty).
  test "a WriteCounter restart during an in-flight flush cannot mark the shard clean",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    test_pid = self()

    # Park the flush task mid-PUT until released.
    blocker = fn ->
      send(test_pid, :flush_started)

      receive do
        :release_flush -> :ok
      after
        10_000 -> :ok
      end
    end

    Application.put_env(:fathom, :faulty_before, {:flush, blocker})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    send(coordinator, :durability_flush)
    assert_receive :flush_started, 2_000
    flush_pid = flush_task_pid(coordinator)

    capture_log(fn ->
      # The counter's owner dies while the flush task is parked; the restarted owner
      # hands every shard a fresh empty table.
      wc = Process.whereis(Fathom.Shard.WriteCounter)
      ref = Process.monitor(wc)
      Process.exit(wc, :kill)
      assert_receive {:DOWN, ^ref, :process, ^wc, :killed}, 1_000
      assert wait_restarted(Fathom.Shard.WriteCounter, wc), "WriteCounter should restart"

      # An acknowledged write lands in the FRESH table — its count sits far below the
      # dead table's watermark the in-flight task is about to restore.
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('bob')"))

      # Release the flush; its reply restores flushed_through from flush_pending.
      send(flush_pid, :release_flush)
      wait_flush_settled(coordinator, 400)

      assert Shard.dirty?(coordinator),
             "a watermark from the dead counter table must not classify the shard clean"

      # And the idle stop must upload bob, not drop_clean the only copy holding it.
      # (Un-park storage first: the terminate drop-flush goes through it too.)
      Application.delete_env(:fathom, :faulty_before)
      close_and_stop(shard, conn)
    end)

    {:ok, ro} = Connection.open(remote_db(shard))

    assert {:ok, %{rows: [["alice"], ["bob"]]}} =
             Connection.query(ro, "SELECT v FROM kv ORDER BY v", [])

    Connection.close(ro)
  end

  defp wait_restarted(name, old, tries \\ 200) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != old -> _ = :sys.get_state(pid) && true
      _ when tries > 0 -> Process.sleep(5) && wait_restarted(name, old, tries - 1)
      _ -> false
    end
  end

  # Expert review #5: the coordinator traps exits so a supervisor :shutdown runs
  # terminate/2's final flush — but with no explicit :shutdown in the child spec, the
  # worker default of 5 000 ms applied, and the supervisor brutal-killed the coordinator
  # mid-flush whenever the fence + checkpoint + full-file S3 PUT + lease release exceeded
  # 5 s (routine for multi-MB shards on a deploy, with every coordinator flushing through
  # one Finch pool). The violated invariant: the shutdown budget must cover the terminate
  # flush, and must be tunable for bigger shards / slower links.
  # Expert review round-2 #18: settle_flush_task's flat 30 s Task.yield (+5 s task
  # shutdown grace) could consume over half the 60 s shutdown budget BEFORE the
  # terminate drop-flush (fence RTT + checkpoint + full-file PUT + lease release)
  # even began — on a rolling deploy every coordinator settles and flushes through
  # one Finch pool at once, reintroducing the #5 brutal-kill-mid-PUT and feeding the
  # #2 poisoned-etag case with the killed task's landed PUT. The invariant: the
  # settle wait is derived from the configured budget and leaves at least two
  # thirds of it for the drop-flush.
  test "the settle yield leaves at least two thirds of the shutdown budget for the drop-flush" do
    prev = Application.get_env(:fathom, :shard_shutdown_ms)
    on_exit(fn -> restore(:shard_shutdown_ms, prev) end)

    # Default budget (60 s): settle caps at 20 s, leaving 40 s for the drop-flush.
    Application.delete_env(:fathom, :shard_shutdown_ms)
    assert Shard.settle_yield_ms() == 20_000

    # A tightened budget scales the settle down with it.
    Application.put_env(:fathom, :shard_shutdown_ms, 9_000)
    assert Shard.settle_yield_ms() == 3_000

    # A huge budget still caps the settle at 30 s — anything the task hasn't
    # finished by then the drop-flush re-uploads itself anyway.
    Application.put_env(:fathom, :shard_shutdown_ms, 300_000)
    assert Shard.settle_yield_ms() == 30_000
  end

  test "the coordinator child spec carries an explicit shutdown budget for the terminate flush" do
    assert %{shutdown: 60_000} = Fathom.Shard.child_spec("any_shard")

    Application.put_env(:fathom, :shard_shutdown_ms, 120_000)
    on_exit(fn -> Application.delete_env(:fathom, :shard_shutdown_ms) end)

    assert %{shutdown: 120_000} = Fathom.Shard.child_spec("any_shard"),
           "the budget must be config-tunable via :shard_shutdown_ms"
  end

  test "a warm restart (local file present) opens dirty", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    # A valid empty local db stands in for un-flushed local state from a prior boot.
    {:ok, c} = Connection.open(local_db(shard))
    Connection.close(c)

    capture_log(fn ->
      {:ok, pid, ref, _path} = Shards.checkout(shard)
      assert Shard.dirty?(pid), "a warm restart may hold un-flushed writes → opens dirty"

      mref = Process.monitor(pid)
      Shard.checkin(pid, ref)
      assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 2_000
    end)
  end

  test "a clean cold-pulled shard idle-stops without re-uploading", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    # Seed the object so opening is a cold PULL (local == storage ⇒ clean).
    seed = Path.join(System.tmp_dir!(), "seed_#{shard}.db")
    {:ok, c} = Connection.open(seed)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('seed')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, %StmtResult{rows: [["seed"]]}} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))
    refute dirty?(shard), "a cold-pulled, read-only shard is clean"

    # Drop the object: a clean idle-stop must NOT recreate it (skip the upload).
    File.rm!(remote_db(shard))
    close_and_stop(shard, conn)

    refute File.exists?(remote_db(shard)), "a clean idle-stop skips the upload"
    refute File.exists?(local_db(shard)), "idle-stop still drops the local copy"
  end

  # Finding #8: the coordinator is restart: :temporary, so a hard crash must NOT auto-restart it.
  # A restarted-but-empty coordinator (conns: %{}) would later flush-and-drop the file under the
  # surviving Filo streams that still hold it open, or hold the shard + lease forever. Instead the
  # next checkout re-creates it fresh, re-adopting the warm local file (the un-flushed write survives).
  test "a crashed coordinator is not auto-restarted; the next checkout re-adopts the warm file",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    # Hard crash: bypasses terminate/2, so no flush, no drop_local, no lease release.
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 1_000

    # Flush the supervisor's mailbox so its (non-)restart decision has been made, then assert no
    # phantom coordinator was restarted. Under :transient this returns the auto-restarted pid.
    _ = :sys.get_state(Fathom.ShardSupervisor)

    # ...and flush the REGISTRY too, which is a different process (2026-07-26).
    #
    # `Registry.lookup/2` only stops reporting the dead coordinator once the Registry's own
    # PID-partition process has handled its `:DOWN` and unregistered the entry. The supervisor
    # flush above proves the SUPERVISOR decided not to restart; it says nothing about the
    # Registry's cleanup, which is asynchronous and owned by an unrelated process. On an idle box
    # that cleanup has invariably already happened, so the gap was invisible — under load it is
    # not, and the assertion reports a "phantom restart" that never occurred.
    await_registry_cleanup()

    assert Registry.lookup(Fathom.ShardRegistry, shard) == [],
           "a :temporary coordinator must not be auto-restarted after a crash"

    ShardExecutor.close(conn)

    # The next checkout re-creates the coordinator, which re-adopts the present local file.
    {:ok, conn2} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: [["alice"]]}} =
             ShardExecutor.execute(conn2, stmt("SELECT v FROM kv")),
           "the un-flushed committed write survived the crash and was re-adopted"

    ShardExecutor.close(conn2)
  end

  # Expert review 2026-07-24 #37. Every other flush failure is transient — auth, bucket policy,
  # reachability all recover — which is why the generic path waits for a consecutive-failure
  # threshold before escalating. `object_too_large` is not: the object is past the 5 GiB single-PUT
  # ceiling and every retry fails identically forever while the shard keeps acking writes it can
  # never make durable. It needs its own event and an immediate log, not a threshold.
  test "an over-ceiling flush emits a distinct event and escalates on the FIRST failure", %{
    shard: shard
  } do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      restore(:shard_storage, prev_storage)
      Application.delete_env(:fathom, :storage_fault)
    end)

    test_pid = self()

    :telemetry.attach(
      "too-large-#{shard}",
      [:fathom, :shard, :flush, :too_large],
      fn _e, measurements, meta, _ -> send(test_pid, {:too_large, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("too-large-#{shard}") end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES ('x')"))

    Application.put_env(:fathom, :storage_fault, :flush_too_large)
    {:ok, pid} = Shards.ensure(shard)

    # The flush runs in a Task, so its failure is reported asynchronously — the telemetry event is
    # the sync point, and capture_log has to span the wait for it.
    log =
      capture_log(fn ->
        send(pid, :durability_flush)
        assert_receive {:too_large, %{size: 6_000_000_000}, %{shard_id: ^shard}}, 5_000
      end)

    # The FIRST failure escalates: waiting for a threshold would be waiting for something that
    # cannot happen, and the message must name the remedy rather than "check S3 reachability".
    assert log =~ "PERMANENTLY failing", "the first over-ceiling flush must escalate immediately"
    assert log =~ "NEVER succeed on retry"

    ShardExecutor.close(conn)
  end

  # The write-time brake that prevents ever reaching that state (#37 / #31's deferred half).
  test "the per-shard page cap is on by DEFAULT, so the brake is at write time" do
    refute Application.get_env(:fathom, :shard_max_page_count),
           "this test asserts the built-in default, so the env must not be set here"

    path = Path.join(System.tmp_dir!(), "fathom_cap_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)

    {:ok, conn} = Connection.open(path)
    {:ok, %{rows: [[cap]]}} = Connection.query(conn, "PRAGMA max_page_count", [])
    :ok = Connection.close(conn)

    assert cap == 1_048_576,
           "unset used to mean UNLIMITED, which put the only brake on the flush — past 5 GiB the " <>
             "shard acks writes it can never upload and its RPO goes unbounded with no remedy"
  end

  # The claim that makes defaulting the cap safe to roll out: SQLite will not set max_page_count
  # below a database's current size, so a shard that is ALREADY over the cap keeps serving and
  # merely stops growing rather than being bricked at open.
  test "a shard already larger than the cap still opens and reads" do
    path = Path.join(System.tmp_dir!(), "fathom_over_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)

    prev = Application.get_env(:fathom, :shard_max_page_count)
    on_exit(fn -> restore(:shard_max_page_count, prev) end)

    # Grow it past a small cap first.
    Application.put_env(:fathom, :shard_max_page_count, 200)
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "CREATE TABLE t (v TEXT)")

    for _ <- 1..300 do
      Connection.query(conn, "INSERT INTO t VALUES (?)", [String.duplicate("x", 400)])
    end

    {:ok, %{rows: [[pages]]}} = Connection.query(conn, "PRAGMA page_count", [])
    :ok = Connection.close(conn)
    assert pages > 20, "the fixture didn't actually grow the db"

    # Now re-open under a cap BELOW its current size.
    Application.put_env(:fathom, :shard_max_page_count, 20)
    assert {:ok, conn2} = Connection.open(path)

    assert {:ok, %{rows: [[_]]}} = Connection.query(conn2, "SELECT count(*) FROM t", []),
           "an already-oversized shard must still open and read — the cap stops growth, it does " <>
             "not brick the tenant"

    :ok = Connection.close(conn2)
  end
end
