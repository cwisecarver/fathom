defmodule Fathom.ShardsStopTest do
  @moduledoc """
  `Fathom.Shards.stop/1` — the force-stop used by `Tenants.purge/1`, `RestoreDrillJob` and the
  scale harness teardown. Expert review 2026-08-26 #9.

  Two separable defects, one test each:

  1. **The wait ran inside the shard supervisor.** `DynamicSupervisor.terminate_child/2` is a
     `handle_call` served in the supervisor process: it monitors the child, sends the shutdown
     exit, and blocks in a `receive` until the `:DOWN` or the child's `shutdown` budget (60 s by
     default). `Fathom.ShardSupervisor` is the single process every cold open goes through, so for
     the whole of one coordinator's `terminate/2` — settle + checkpoint + `VACUUM INTO` +
     full-object PUT + lease release — no other shard on the node could be STARTED. It surfaced as
     a latency cliff rather than an error rate, because Filo streams sit in `Shard.checkout/1`'s
     75 s call instead of failing.

  2. **The tombstone guard was on one terminate clause only.** The `conns > 0` clause skipped the
     drop-flush for a tombstoned shard; the `conns == 0` clause — the COMMON state for a tenant
     being deleted — did not. So an erase paid a full checkpoint + `VACUUM INTO` + full-object PUT
     for bytes `Storage.purge_shard/1` erases on the very next line.

  Both were verified against the unfixed code before shipping, per AGENTS.md. Test 1 blocked for
  the full artificial flush delay (~2 s) instead of the asserted 1 s; test 2 saw a flush the fix
  removes. Neither passes pre-fix.

  A third test (2026-08-26 #3) pins the drop-flush ROUTING for a busy shard. It asserts the
  decision — a shard with a connection still checked out flushes through `snapshot_and_upload/1`,
  not the live-file fast path — and that the final flush succeeds. It does NOT reproduce the race
  itself: that needs a write to land between `Storage.flush/5`'s two reads of the live file with
  the same size in the same wall-clock second, which is not deterministically forceable from a
  test. The fix rests on removing the window rather than on catching it.

  Not async: shards, the Registry and the shard supervisor are node-global.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    prev = %{
      idle: Application.get_env(:fathom, :shard_idle_ms),
      flush_delay: Application.get_env(:fathom, :storage_flush_delay_ms),
      flush_interval: Application.get_env(:fathom, :shard_flush_interval_ms),
      storage: Application.get_env(:fathom, :shard_storage)
    }

    # THE BACKEND MUST BE FaultyStorage OR THIS WHOLE FILE MEASURES NOTHING.
    #
    # `:storage_flush_delay_ms` is read only by `Fathom.Test.FaultyStorage.flush_delay/0`; the
    # default test backend is `Storage.Local`, which ignores it entirely. The first working draft
    # of the blocking test set the delay without switching the backend, so the terminate finished
    # in milliseconds, the slow flush never existed, and the two operations never overlapped in
    # EITHER tree. AGENTS.md names this exact trap ("the fixture doesn't create the state") — it
    # was caught here only because the overlap precondition is asserted rather than assumed.
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    # High idle so a just-opened coordinator does not idle-stop underneath the assertions, and a
    # high flush interval so the periodic timer does not fire an unrelated (delayed) flush that
    # would itself hold the gate and confuse the measurement.
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

    ids = for i <- 1..3, do: "stopblk_#{System.unique_integer([:positive])}_#{i}"

    on_exit(fn ->
      for {k, v} <- [
            shard_idle_ms: prev.idle,
            storage_flush_delay_ms: prev.flush_delay,
            shard_flush_interval_ms: prev.flush_interval,
            shard_storage: prev.storage
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      for id <- ids do
        # Tombstones has no public `forget/1` — the in-memory set is append-only by design (a
        # tombstone must never be un-set by anything except a directory reload). Its table is
        # `:public`, so a test that PUTS one is responsible for taking it back out, or the id stays
        # refused for the rest of the VM's life.
        :ets.delete(Fathom.Tenants.Tombstones, id)
        Shards.drain(id, 2_000)
      end

      for id <- ids,
          dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          s <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, id <> s))
    end)

    %{ids: ids}
  end

  defp open_dirty!(id) do
    {:ok, conn} = ShardExecutor.open(id)
    # A real write, so the coordinator is genuinely dirty and its terminate/2 has a flush to do.
    # A clean shard short-circuits and would make the delay unreachable — the "measuring nothing"
    # failure mode AGENTS.md warns about.
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE t (id INTEGER PRIMARY KEY)"})
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO t VALUES (1)"})
    :ok = ShardExecutor.close(conn)
    {:ok, pid} = Shards.ensure(id)

    assert Fathom.Shard.dirty?(pid),
           "precondition: the shard must be dirty or terminate/2 skips the flush"

    pid
  end

  test "stopping one shard does not block a cold open of an unrelated shard", %{ids: [a, b | _]} do
    pid_a = open_dirty!(a)

    # Make A's terminate-flush slow. Artificial stand-in for real S3 latency, which the audit
    # measures at seconds for a full-object PUT on a real backend.
    Application.put_env(:fathom, :storage_flush_delay_ms, 3_000)

    me = self()

    stopper =
      spawn(fn ->
        # Signal immediately BEFORE entering stop/1, so the parent can start racing without a
        # blind sleep.
        send(me, :stopping)
        Shards.stop(a)
        send(me, :stopped)
      end)

    stopper_ref = Process.monitor(stopper)
    assert_receive :stopping, 1_000

    # THE ASSERTION. Opening an unrelated, never-seen shard must not queue behind A's flush.
    {elapsed_us, {:ok, _pid_b}} = :timer.tc(fn -> Shards.ensure(b) end)
    elapsed_ms = div(elapsed_us, 1000)

    # Pre-fix this took the full 3 s flush delay, because `DynamicSupervisor.start_child/2` and
    # `terminate_child/2` are both `handle_call`s served by the one supervisor process. This
    # assertion comes FIRST so that the real regression reports the real reason (see below).
    assert elapsed_ms < 1_000,
           "cold open of an unrelated shard took #{elapsed_ms}ms while another shard was stopping " <>
             "with a 3s flush — the stop is blocking the shard supervisor again"

    # The precondition that rules out a VACUOUS pass, checked second and deliberately so.
    #
    # The first draft of this test synchronised with `wait_until(fn -> not Process.alive?(pid_a) end)`
    # before timing the open — i.e. it waited for the ENTIRE terminate, slow flush included, to
    # finish first. Nothing was left to overlap with, so it passed against the unfixed code and
    # measured nothing (verified: 3/4 passed on a reverted tree, the one failure being the
    # unrelated tombstone test). This assertion is what closes that hole: post-fix the stopper must
    # still be inside `Shards.stop/1` when the unrelated open completes.
    #
    # It is ordered AFTER the timing assertion on purpose. Pre-fix the open blocks for ~3 s and the
    # stopper finishes at essentially the same instant, so `Process.alive?(stopper)` is ALSO false
    # then — checking it first reported "the two did not overlap, this test proved nothing" for a
    # run in which the blocking had in fact just been demonstrated. The timing assertion names the
    # defect; this one only guards the setup.
    assert Process.alive?(stopper),
           "the unrelated open was fast, but the stop had already finished — the two never " <>
             "overlapped, so this run did not exercise supervisor blocking at all"

    # The stop still completes and still waits for the coordinator: the ordering guarantee
    # Tenants.purge/1 depends on is unchanged by moving the wait out of the supervisor.
    assert_receive :stopped, 15_000
    assert_receive {:DOWN, ^stopper_ref, :process, ^stopper, :normal}, 5_000
    refute Process.alive?(pid_a), "stop/1 must not return before the coordinator is down"
  end

  test "stop/1 still blocks until the coordinator is fully down", %{ids: [a | _]} do
    pid = open_dirty!(a)
    assert :ok = Shards.stop(a)

    # The guarantee Tenants.purge/1 rests on: by the time stop/1 returns, terminate/2 has run to
    # completion, so the caller can delete storage without racing a flush. THE PROCESS BEING DEAD
    # IS THAT GUARANTEE.
    refute Process.alive?(pid)

    # Registry emptiness is deliberately NOT asserted synchronously here. `Registry` unregisters a
    # dead process from its own partition process, asynchronously, so immediately after the `:DOWN`
    # the lookup can still return the dead pid — observed as `[{#PID<...>, nil}]` while
    # `Process.alive?` was already false. That window is identical under the old
    # `DynamicSupervisor.terminate_child/2` (it also returned on the `:DOWN`), so it is neither
    # introduced nor widened by review #9; asserting it synchronously was simply wrong.
    assert :ok = wait_until(fn -> Registry.lookup(Fathom.ShardRegistry, a) == [] end)
  end

  # Expert review 2026-08-26 #3. The drop-flush had a fast path that uploaded the LIVE database:
  # Storage.flush/5 reads it once for the Content-MD5 and again to stream the body, and on the
  # `conns > 0` terminate clause live streams still hold connections (the stream :DOWN cannot fire
  # until terminate/2 RETURNS). A write landing between the two reads is invisible to the change
  # guard, whose File.stat mtime is second-resolution — so a same-size, same-second in-place page
  # rewrite (an inline autocheckpoint) slipped through. S3 catches it with BadDigest, but that is a
  # FAILED flush, not a saved one, and this is the one flush trap_exit exists to guarantee.
  #
  # This pins the ROUTING, which is the decision: with a stream still checked out the drop must go
  # through snapshot_and_upload/1, whose VACUUM INTO temp is quiescent by construction. Observable
  # via the snapshot path's own telemetry rather than by racing the window, which is not
  # deterministically forceable — see the moduledoc note on what this does and does not prove.
  test "a busy shard's drop-flush takes the snapshot path, not the live-file fast path", %{
    ids: [a | _]
  } do
    pid = open_dirty!(a)

    # Hold a stream open across the stop, which is what makes this the `conns > 0` clause.
    {:ok, conn} = ShardExecutor.open(a)

    on_exit(fn -> ShardExecutor.close(conn) end)

    assert :sys.get_state(pid).conns |> map_size() > 0,
           "precondition: a connection must still be checked out or this is the conns == 0 clause"

    test_pid = self()
    handler = "dropflush-#{a}"

    :telemetry.attach_many(
      handler,
      [
        [:fathom, :shard, :flush, :failed],
        [:fathom, :shard, :drop_flush, :route]
      ],
      fn
        [:fathom, :shard, :flush, :failed], _m, meta, _ -> send(test_pid, {:flush_failed, meta})
        [:fathom, :shard, :drop_flush, :route], _m, meta, _ -> send(test_pid, {:route, meta})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert :ok = Shards.stop(a)
        refute Process.alive?(pid)
      end)

    # THE DISCRIMINATOR, and it took three attempts to find one. Recorded because the two failed
    # attempts each taught something about this path:
    #
    #   1. "assert the snapshot path ran" — does not discriminate. The audit predicted TRUNCATE
    #      would usually return busy with a live reader, so the pre-fix code would already fall
    #      through to the snapshot. It passed against the unfixed tree.
    #   2. "assert the fallback warning is ABSENT" — also does not discriminate, and this is the
    #      interesting one: it passed pre-fix too, which means the checkpoint SUCCEEDED with a
    #      connection still checked out. So the audit's "nearly free in practice" guess is WRONG
    #      for this shape — pre-fix the drop really did take the live-file fast path, which is the
    #      hazard itself, not a rare corner of it.
    #
    # Both routes are silent on success, so the route is now emitted explicitly. Asserting on it
    # pins the decision rather than an outcome that luck also produces.
    assert_receive {:route, %{route: :snapshot, conns: conns}}, 2_000
    assert conns > 0

    refute log =~ "pre-drop checkpoint incomplete",
           "the snapshot path was reached by FALLBACK, not by decision"

    # And the flush must not have failed. Pre-fix the fast path could report
    # {:source_changed_during_upload, _} or an s3_put_status 400 here.
    refute_receive {:flush_failed, _}, 200

    # And the object is present and readable — the acked writes made it out.
    remote = Path.join(Fathom.Shard.Storage.Local.dir(), a <> ".db")
    assert File.exists?(remote), "the busy shard's final flush produced no object"
    assert :ok = Fathom.Shard.verify_integrity(remote)
  end

  test "stop/1 is :ok for a shard that is not open", %{ids: [_, _, c]} do
    assert :ok = Shards.stop(c)
  end

  # The second half of #9. A tombstoned shard is being erased: purge_shard/1 deletes every object
  # including the .lock, and rm_local/1 deletes the local file, so a flush on the way out is pure
  # waste. The `conns > 0` clause already skipped it; `conns == 0` did not.
  test "a tombstoned shard skips the drop-flush on the idle terminate path too", %{ids: [a | _]} do
    pid = open_dirty!(a)
    remote = Path.join(Fathom.Shard.Storage.Local.dir(), a <> ".db")

    # Establish the pre-state: whatever is (or isn't) in storage now.
    before_mtime = mtime(remote)

    Fathom.Tenants.Tombstones.put(a)
    assert Fathom.Tenants.Tombstones.tombstoned?(a), "precondition: the tombstone must be set"

    assert :ok = Shards.stop(a)
    refute Process.alive?(pid)

    # Pre-fix the idle clause called flush_and_drop/1 unconditionally, so a dirty tombstoned shard
    # uploaded its object here. Post-fix storage is untouched on the way out.
    assert mtime(remote) == before_mtime,
           "a tombstoned shard uploaded on the idle terminate path — the drop-flush was not skipped"
  end

  defp mtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, %{mtime: m, size: s}} -> {m, s}
      {:error, _} -> :absent
    end
  end

  # Poll a condition rather than sleeping a fixed amount: AGENTS.md forbids Process.sleep as a
  # synchronisation primitive, and a fixed sleep here would either be flaky or slow.
  defp wait_until(fun, remaining_ms \\ 3_000)
  defp wait_until(_fun, remaining_ms) when remaining_ms <= 0, do: :timeout

  defp wait_until(fun, remaining_ms) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, remaining_ms - 10)
    end
  end
end
