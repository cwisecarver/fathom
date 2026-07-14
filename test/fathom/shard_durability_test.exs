defmodule Fathom.ShardDurabilityTest do
  # The periodic durability flush: a busy shard (connection still open, never
  # idle) still gets a consistent snapshot pushed to storage, so the data-loss
  # window is bounded to the flush interval instead of the whole session. Fenced
  # like the idle flush. Not async: shards are global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
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
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp now_ms, do: System.system_time(:millisecond)
  defp local_db(shard), do: Path.join(@local_dir, "#{shard}.db")
  defp remote_db(shard), do: Path.join(@remote_dir, "#{shard}.db")

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
end
