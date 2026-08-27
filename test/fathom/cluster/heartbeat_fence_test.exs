defmodule Fathom.Cluster.HeartbeatFenceTest do
  @moduledoc """
  The coordinator in HEARTBEAT mode (the F1 fix): liveness is the node heartbeat, so
  a coordinator does NO per-shard lease renewal (the storm is gone), and the flush
  fence is the heartbeat's validity + a lock re-check only after a lapse. Proves both
  the win (no renewal timer) and that single-writer safety still holds (a stolen
  shard self-fences instead of clobbering the new owner).
  """
  use Fathom.ClusterShardCase, async: false

  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.WriteFence

  setup %{shard: shard} do
    # Idle-stop fast so the flush path runs during the test.
    Application.put_env(:fathom, :shard_idle_ms, 50)
    # Start the node heartbeat (the app leaves it off in test) → coordinators open in
    # heartbeat mode. Long TTL so it never lapses on its own; tests force a lapse.
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    on_exit(fn -> File.rm(hb_file()) end)
    %{shard: shard, hb: hb}
  end

  test "an open shard schedules NO per-shard renewal (the storm is gone)", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)
    state = :sys.get_state(coordinator)

    assert state.renew_timer == nil, "heartbeat mode must not arm a per-shard renewal timer"
    assert is_integer(state.acquire_gen), "heartbeat mode records the acquire generation"

    close_and_stop(shard, conn)
  end

  test "a clean idle flush still persists data (durability works without renewal)",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    close_and_stop(shard, conn)

    assert File.exists?(remote_db(shard)),
           "a clean idle flush persists the shard in heartbeat mode"
  end

  test "a stolen shard self-fences at flush: revalidate → drop, never clobber the new owner",
       %{shard: shard, hb: hb} do
    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)

      # A thief takes the lock — which is only possible because our heartbeat lapsed.
      put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

      # Force that lapse by bumping the generation DIRECTLY (no renew tick, so no
      # lapse broadcast — the proactive #34 path is tested separately below; this
      # test pins the lazy flush-path fence that guards a missed broadcast). Now the
      # fence can't just trust the heartbeat — it must revalidate the lock.
      # (publish_status syncs the lock-free ETS view the fence reads, round-2 #26.)
      :sys.replace_state(hb, fn s ->
        Heartbeat.publish_status(%{s | generation: s.generation + 1, lapsed: true})
      end)

      # Releasing the last connection idles → flush → fence: :revalidate →
      # check_lease sees the thief → drop local WITHOUT flushing.
      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
    end)

    refute File.exists?(remote_db(shard)), "fenced coordinator must not flush over the new owner"
    refute File.exists?(local_db(shard)), "fenced coordinator drops its local copy"
  end

  # Expert review 2026-08-26 #13a. `handle_info(:revalidate_lapse, …)` called `Fence.check/2`
  # SYNCHRONOUSLY in the coordinator's own process, and in heartbeat mode a lapse routes to
  # `revalidate/2`, which is a real `Storage.check_lease/2` object-store GET plus Req's retry
  # ladder. So the coordinator's mailbox was blocked for a full storage RTT — and the lapse
  # broadcast reaches EVERY open coordinator at the same instant, so at the shipped
  # `:max_open_shards` the whole node stalls its checkout/checkin path at the exact moment it has
  # just proved it could not keep one small object fresh: checkouts queue, checkins queue (idle
  # detection and LRU eviction lag), and `evict/2`'s 2 s probe budget expires against coordinators
  # that are merely blocked.
  #
  # The invariant is RESPONSIVENESS DURING the check, which is why the fixture holds `check_lease`
  # open rather than timing a fast one: with the GET in flight, the coordinator must still answer.
  # Pre-fix it cannot — it is inside `Fence.check/2` — and this times out.
  #
  # `:faulty_check_lease` already exists for exactly this shape; its own comment says it was added
  # to prove the flush's 412-reconcile runs off-process (review 2026-07-14 #8). This is the third
  # inline round trip in the module, after the flush fence (#18) and the legacy renew PUT (#29).
  test "a lapse revalidation does not block the coordinator mailbox", %{shard: shard, hb: hb} do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, coordinator} = Shards.ensure(shard)

    assert is_integer(:sys.get_state(coordinator).acquire_gen),
           "fixture: the coordinator must be in HEARTBEAT mode, or the lapse path is unreachable"

    test_pid = self()

    # Hold check_lease open so the GET is provably in flight when we probe the mailbox.
    blocker = fn ->
      # Hands back the pid that is actually blocked — the TASK, post-fix, and the coordinator
      # itself pre-fix. Either way the release goes to the right process.
      send(test_pid, {:check_lease_started, self()})

      receive do
        :release -> :ok
      after
        10_000 -> :ok
      end
    end

    Application.put_env(:fathom, :faulty_check_lease, blocker)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_check_lease)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    # Bump the generation so the fence takes :revalidate (the branch that does the GET). A lapse at
    # the SAME generation would take :ok and never reach check_lease — AGENTS.md records that
    # exact fixture mistake.
    :sys.replace_state(hb, fn st ->
      Heartbeat.publish_status(%{st | generation: st.generation + 1, lapsed: true})
    end)

    send(coordinator, :revalidate_lapse)
    assert_receive {:check_lease_started, blocked_pid}, 2_000

    # THE ASSERTION: with the storage GET provably in flight, the coordinator still answers.
    # Pre-fix it is sitting inside Fence.check/2 and this exits with :timeout.
    state =
      try do
        :sys.get_state(coordinator, 1_000)
      catch
        :exit, reason ->
          send(blocked_pid, :release)

          flunk(
            "the coordinator mailbox is blocked while the lapse check_lease GET is in flight " <>
              "(#{inspect(reason)})"
          )
      end

    assert is_map(state)

    # Post-fix the blocked process is the task, not the coordinator — so this also proves they are
    # different processes, which is the whole point.
    refute blocked_pid == coordinator,
           "check_lease is running IN the coordinator process; it was not moved off-process"

    send(blocked_pid, :release)
    :ok = ShardExecutor.close(conn)
  end

  # Expert review #34: the Heartbeat moduledoc promised a lapse "broadcasts so
  # coordinators can revalidate proactively instead of waiting for their next flush" —
  # but nothing subscribed, so a superseded coordinator kept accepting writes it would
  # later discard for up to a full flush interval (unboundedly with the durability
  # flush disabled). The invariant: the lapse broadcast alone makes a stolen shard's
  # coordinator revalidate and self-fence, no flush needed.
  test "a lapse broadcast proactively self-fences a stolen shard without waiting for a flush",
       %{shard: shard, hb: hb} do
    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)

      # A thief takes the lock during the lapse.
      put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

      # Force the lapse: the renew tick bumps the generation and broadcasts.
      :sys.replace_state(hb, fn s ->
        %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000}
      end)

      send(hb, :renew)
      _ = :sys.get_state(hb)

      # The broadcast alone must fence the coordinator — the connection stays open,
      # no idle flush runs. Pre-fix nothing subscribed and this timed out.
      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 2_000

      :ok = ShardExecutor.close(conn)
    end)
  end

  # Finding #5: the fence baseline (acquire_gen) must be sampled BEFORE acquire_lease. Force a
  # heartbeat lapse+recover DURING acquire (the VM-pause window the bug rides): if the baseline
  # is captured after acquire it records the post-lapse generation, so the later flush trusts
  # the heartbeat and clobbers a thief; captured before acquire, the lapse trips :revalidate and
  # the coordinator self-fences. (Unlike the test above, this forces the lapse during open, not
  # after — so it fails pre-fix.)
  test "a lapse during open leaves the baseline stale so the flush self-fences", %{
    shard: shard,
    hb: hb
  } do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    lapse = fn ->
      :sys.replace_state(hb, fn s ->
        %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000}
      end)

      send(hb, :renew)
      _ = :sys.get_state(hb)
      :ok
    end

    Application.put_env(:fathom, :faulty_before, {:acquire, lapse})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      # Only wanted the lapse during the open's acquire_lease.
      Application.delete_env(:fathom, :faulty_before)

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)

      # A thief takes the lock (possible only because our heartbeat lapsed during open).
      put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
    end)

    refute File.exists?(remote_db(shard)),
           "a lapse during open must leave the acquire baseline stale so the flush self-fences"
  end

  # Expert review 2026-07-19 #3: a node reachable by clients but cut from object storage keeps its
  # coordinators alive — the heartbeat goes stale and durability flushes skip, but NOTHING on the
  # write path consulted that, so the coordinator kept ACKing writes that are quarantined on
  # partition-heal (loss window = the partition duration, not the flush interval the RPO contract
  # advertises). The coordinator now publishes a write-fence once its heartbeat has been not-valid
  # for margin + steal_margin ("provably stealable" — #13 corrected this from ttl + steal_margin,
  # which double-counted the margin; the describe block at the bottom pins the value); ShardExecutor refuses writes with 503
  # FILO_STALE_LEASE while READS still serve from the local copy, and a reconfirmed fence lifts it.
  test "a provably-stealable node fences writes (reads continue) and lifts it on recovery (#3)",
       %{shard: shard, hb: hb} do
    Application.put_env(:fathom, :fence_writes_when_stealable, true)
    on_exit(fn -> Application.delete_env(:fathom, :fence_writes_when_stealable) end)

    test_pid = self()
    handler = "writefence-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:fathom, :shard, :write_fenced], [:fathom, :shard, :write_unfenced]],
      fn [_, _, event], _m, meta, _ -> send(test_pid, {event, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))

      {:ok, coordinator} = Shards.ensure(shard)

      # Force the node heartbeat NOT comfortably valid (expired deadline, no renew) — the
      # cut-from-storage symptom the flush fence reads — and preset the coordinator's not-valid
      # clock an hour back so the very next fence verdict is already past ttl + steal_margin.
      :sys.replace_state(hb, fn s ->
        forged = %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000}
        Heartbeat.publish_status(forged)
        forged
      end)

      :sys.replace_state(coordinator, fn s ->
        %{s | not_valid_since: System.monotonic_time(:millisecond) - 3_600_000}
      end)

      # Drive the periodic durability flush: its fence returns not-valid → the coordinator fences.
      send(coordinator, :durability_flush)
      assert_receive {:write_fenced, %{shard_id: ^shard}}, 2_000
      assert WriteFence.fenced?(shard)

      # A WRITE is refused with a retryable 503; a READ still serves from the local copy.
      assert {:error, %{code: "FILO_STALE_LEASE", status: 503}} =
               ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('b')"))

      assert {:ok, %{rows: [["a"]]}} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))

      # THE TWO BYPASSES (expert review 2026-08-26 #1), both verified by execution in the audit.
      #
      # The assertion above uses a plain INSERT, so it could not see either. `write?` was
      # `dml?(sql) or ddl?(sql)`, both leading-keyword tests, and the fence has NO engine backstop
      # — a fenced shard's handle is perfectly writable, so the predicate IS the enforcement.
      #
      # 1. A CTE-prefixed write. `lead(sql, 7)` is "with sr", matching no @dml_prefixes and no
      #    @ddl_leads, so the fence was never consulted and the write was ACKed on a node that had
      #    provably lost its lease — un-flushable, quarantined on partition-heal.
      assert {:error, %{code: "FILO_STALE_LEASE", status: 503}} =
               ShardExecutor.execute(
                 conn,
                 stmt("WITH src(v) AS (VALUES ('cte')) INSERT INTO kv SELECT v FROM src")
               ),
             "a CTE-prefixed INSERT slipped past the write fence"

      # 2. executescript(). `execute_sequence/2` never referenced WriteFence at all, while already
      #    presuming a script writes (it bumps WriteCounter unconditionally).
      assert {:error, %{code: "FILO_STALE_LEASE", status: 503}} =
               ShardExecutor.execute_sequence(conn, "INSERT INTO kv VALUES ('script');"),
             "a sequence/executescript request slipped past the write fence"

      # Neither write landed.
      assert {:ok, %{rows: [["a"]]}} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))

      # Recovery: the heartbeat is comfortably valid again → a revalidate reconfirms ownership and
      # lifts the fence.
      #
      # SYNCED ON `:write_unfenced`, not on `:sys.get_state`. This comment used to read
      # "revalidate_lapse runs the fence synchronously in the coordinator, so a :sys.get_state
      # after it is a clean sync point" — true when written, and no longer: expert review
      # 2026-08-26 #13a moved that check into a monitored task, because running a real
      # `check_lease` GET inline blocked the coordinator's mailbox and the lapse broadcast reaches
      # EVERY open coordinator at once. The behaviour under test is unchanged; only the sync point
      # was an implementation detail. The telemetry event is the observable, so this no longer
      # depends on where the check runs.
      :sys.replace_state(hb, fn s ->
        forged = %{s | mono_deadline_ms: System.monotonic_time(:millisecond) + 60_000}
        Heartbeat.publish_status(forged)
        forged
      end)

      send(coordinator, :revalidate_lapse)
      assert_receive {:write_unfenced, %{shard_id: ^shard}}, 2_000
      assert :sys.get_state(coordinator).not_valid_since == nil
      refute WriteFence.fenced?(shard)

      # And a write is accepted again.
      assert {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('c')"))

      # THE GUARD ON THE FIX ITSELF. `write_candidate?/1` is deliberately separate from `dml?/1`
      # because widening `dml?/1` to include "with" would make a read-only CTE report the inherited
      # sqlite3_changes() as affected_row_count — re-opening 2026-07-14 #42 from the other side.
      # Unfenced, a read-only CTE must still classify as a READ.
      assert {:ok, %{affected_row_count: 0}} =
               ShardExecutor.execute(
                 conn,
                 stmt("WITH src(v) AS (VALUES ('x')) SELECT v FROM src")
               ),
             "a read-only CTE is reporting a non-zero affected_row_count — dml?/1 was widened " <>
               "instead of adding a separate fence predicate"

      :ok = ShardExecutor.close(conn)
    end)
  end

  # #6 — the fence must not outlive the coordinator that published it.
  #
  # WriteFence rows are node-global ETS, and every WriteFence.forget/1 call site is inside
  # Fathom.Shard.terminate/2 — which is NOT guaranteed to run. The terminate clause that would run
  # for a fenced shard does a fence + checkpoint + full-object PUT against the object store, and
  # that state is reached precisely when the object store is unreachable: the case most likely to
  # blow the :shard_shutdown_ms budget and be brutally killed. The row then survived, and no
  # successor could clear it either — fresh coordinator state starts `not_valid_since: nil`, and
  # clear_write_fence/1 short-circuits on exactly that. The tenant read fine and every write 503'd
  # forever, across coordinator restarts and across the partition healing.
  #
  # The test above cannot see this: it fences and unfences within ONE coordinator's lifetime. This
  # one kills the coordinator while fenced (`restart: :temporary`, so nothing revives it), asserts
  # the row genuinely survived — otherwise the rest measures nothing — and then proves a fresh
  # coordinator lifts it. Fails against unfixed lib/ at the final write.
  test "a write fence left behind by a killed coordinator is lifted by the next open (#6)",
       %{shard: shard, hb: hb} do
    Application.put_env(:fathom, :fence_writes_when_stealable, true)
    on_exit(fn -> Application.delete_env(:fathom, :fence_writes_when_stealable) end)

    # The fence is published from the flush verdict, which arrives from an off-process Task — so
    # :sys.get_state on the coordinator is NOT a sync point for it (the first draft used one and
    # the precondition assert below caught it). Wait on the telemetry event, as the #3 test does.
    test_pid = self()
    handler = "writefence-kill-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :write_fenced],
      fn _e, _m, meta, _ -> send(test_pid, {:write_fenced, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))
      {:ok, coordinator} = Shards.ensure(shard)

      # Same forging as the test above: heartbeat not comfortably valid, not-valid clock preset an
      # hour back, so the next fence verdict is already past margin + steal_margin.
      :sys.replace_state(hb, fn s ->
        forged = %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000}
        Heartbeat.publish_status(forged)
        forged
      end)

      :sys.replace_state(coordinator, fn s ->
        %{s | not_valid_since: System.monotonic_time(:millisecond) - 3_600_000}
      end)

      send(coordinator, :durability_flush)
      assert_receive {:write_fenced, %{shard_id: ^shard}}, 2_000
      assert WriteFence.fenced?(shard), "the fixture never published a write fence"

      # Brutal kill: terminate/2 does NOT run, so WriteFence.forget/1 never fires.
      ref = Process.monitor(coordinator)
      Process.exit(coordinator, :kill)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 2_000

      # THE PRECONDITION. If the row did not survive the kill, everything below is vacuous.
      assert WriteFence.fenced?(shard),
             "the fixture did not reproduce a leaked fence — the row was cleared by the kill"

      # The partition heals: the heartbeat is comfortably valid again.
      :sys.replace_state(hb, fn s ->
        forged = %{s | mono_deadline_ms: System.monotonic_time(:millisecond) + 60_000}
        Heartbeat.publish_status(forged)
        forged
      end)

      # A fresh coordinator opens the shard. It holds a valid lease, so it is not stealable and
      # must lift the stale fence.
      {:ok, conn2} = ShardExecutor.open(shard)
      {:ok, _} = Shards.ensure(shard)

      refute WriteFence.fenced?(shard),
             "a successor coordinator holding a valid lease left the stale write fence in place"

      assert {:ok, _} = ShardExecutor.execute(conn2, stmt("INSERT INTO kv VALUES ('b')")),
             "writes are still fenced after the shard was re-opened by a healthy coordinator"

      :ok = ShardExecutor.close(conn2)
    end)
  end

  # #28 — the margin covers the START of a write whose duration it does not know.
  #
  # Fence.check runs ONCE, before quick_check + VACUUM INTO + the PUT. `valid_for_write?`'s
  # margin is a hardcoded ttl/3 (10s at the default TTL) and is not derived from that work.
  # Measured against Storage.Local with no network, the flush is dead linear at ~3.8 ms/MB
  # (1.4 MB → 8.9 ms; 284 MB → 1,070 ms), so the LOCAL half alone reaches the margin around
  # 2.6 GB — under the 4 GiB per-shard cap. So the coordinator re-checks immediately before the
  # PUT, and the margin then only has to cover the upload.
  #
  # This is a LOSS bias, not a clobber: the conditional PUT's etag is what actually prevents a
  # double-write. Shards whose flush outruns the margin self-fence and quarantine an interval of
  # acked writes; small ones never do.
  describe "#28 — ownership is re-checked between the snapshot and the PUT" do
    test "a heartbeat that lapses DURING the snapshot aborts before uploading",
         %{shard: shard, hb: hb} do
      # The window this fix exists for is AFTER Fence.check passed and BEFORE the PUT. Forging
      # the heartbeat before driving the flush does not test it — Fence.check catches that case
      # already, and such a test passes against the unfixed code (it did).
      #
      # So widen the window with SIZE, which is the same lever that makes the finding real: a
      # keystone big enough that quick_check + VACUUM INTO take ~100ms, then invalidate the
      # heartbeat partway through. Process.sleep here is STAGING a race, not synchronising on
      # one — there is no event to wait for between the snapshot and the PUT, which is precisely
      # why the exposure existed.
      capture_log(fn ->
        {:ok, pid, ref, path} = Shards.checkout(shard)
        {:ok, _} = Fathom.Keystone.build!(path, rows: 50_000)
        :ok = Fathom.Shard.stamp_local_provenance(shard)
        Fathom.Shard.checkin(pid, ref)

        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))

        # One clean flush first, so there is a stored object whose etag we can watch.
        :ok = Fathom.Shards.flush(shard)
        {:ok, before_etag} = Fathom.Shard.Storage.object_etag(shard)

        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('b')"))

        # Fence.check passes (heartbeat healthy), then the snapshot runs for ~100ms; this lands
        # inside it.
        test_hb = hb

        spawn(fn ->
          Process.sleep(25)

          :sys.replace_state(test_hb, fn s ->
            forged = %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000}
            Heartbeat.publish_status(forged)
            forged
          end)
        end)

        _ = Fathom.Shards.flush(shard)

        assert {:ok, ^before_etag} = Fathom.Shard.Storage.object_etag(shard),
               "the shard uploaded after its liveness went unconfirmed mid-snapshot — the " <>
                 "margin only covered the START of a write it does not know the length of"

        :ok = ShardExecutor.close(conn)
      end)
    end

    test "recheck_before_put is skipped in legacy mode, where it would cost a PUT" do
      # In legacy mode (acquire_gen nil) the equivalent check is a renew PUT — a store round trip
      # per flush. Paying network to shorten a network-bound window is the wrong trade, so the
      # re-check is heartbeat-mode only. Pinned because "just check everywhere" is the obvious
      # simplification and it is wrong.
      #
      # config/test.exs runs heartbeat_server: false, so a coordinator opened WITHOUT the
      # heartbeat above is genuinely in legacy mode — no forcing needed (AGENTS.md).
      id = "legacy_recheck_#{System.unique_integer([:positive])}"

      # This test RACED the file-wide `shard_idle_ms: 50` and failed in CI on 2026-08-14 (run
      # 31770…, OTP 29, seed 80394) with `{:error, :coordinator_stopped}` from the flush below:
      # `ShardExecutor.close/1` checks the connection in, and 50 ms later the coordinator
      # idle-stops on its own — so on a contended runner it was gone before `Shards.flush/1`
      # resolved it. The 50 ms exists for the tests in this file that exercise the IDLE flush path;
      # this one is about the legacy-mode flush and does not care when idle fires. Pin it long
      # enough that only the explicit flush can happen, and restore for the rest of the file.
      prev_idle = Application.get_env(:fathom, :shard_idle_ms)
      Application.put_env(:fathom, :shard_idle_ms, 600_000)

      on_exit(fn ->
        if prev_idle,
          do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
          else: Application.delete_env(:fathom, :shard_idle_ms)

        Shards.drain(id, 5_000)

        for s <- ["", "-wal", "-shm", ".etag", ".lock"],
            do: File.rm(Path.join(Fathom.Shard.data_dir(), "#{id}.db#{s}"))
      end)

      capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(id)
        {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))
        :ok = ShardExecutor.close(conn)

        # A legacy-mode flush must still succeed end to end.
        assert :ok = Fathom.Shards.flush(id)
        assert {:ok, etag} = Fathom.Shard.Storage.object_etag(id)
        assert is_binary(etag), "the legacy-mode flush did not produce a stored object"
      end)
    end
  end

  # WHEN the fence trips, not just that it does (expert review 2026-08-01 #30 item 5).
  #
  # The test above presets `not_valid_since` an HOUR back, so it fires under any plausible
  # threshold — #13's bug (waiting `ttl + steal_margin` from a clock that already started early)
  # passed it unchanged. #13's whole content is the threshold value, so it needs an assertion
  # that can tell the two apart.
  #
  # At the defaults that is: margin = ttl/3 = 10s, steal_margin = 5s.
  #   correct  margin + steal = 15s
  #   #13 bug  ttl    + steal = 35s
  # An age of 20s sits between them: fenced under the fix, NOT fenced under the bug.
  describe "#13 — the write fence trips at margin + steal_margin, not ttl + steal_margin" do
    test "the threshold is strictly tighter than the pre-#13 one" do
      # Pure, and the cheapest statement of the bug: the clock that feeds this already started
      # early by exactly `margin`, so adding a full `ttl` double-counts it.
      # margin is ttl/3 by construction (heartbeat.ex:219), so ttl is 3 * margin — derived rather
      # than hardcoded, so this keeps meaning the same thing if the ratio is ever retuned.
      margin = Heartbeat.margin_ms()
      correct = margin + Fathom.Shard.Storage.steal_margin_ms()
      pre_13 = margin * 3 + Fathom.Shard.Storage.steal_margin_ms()

      assert correct < pre_13,
             "the fence would publish after a peer may already have stolen the shard"
    end

    test "an age between the two thresholds fences", %{shard: shard, hb: hb} do
      assert_fence_at(shard, hb, 20_000, :fenced)
    end

    test "an age below the correct threshold does NOT fence", %{shard: shard, hb: hb} do
      # The other side of the boundary — without this, "fence immediately, always" would pass.
      assert_fence_at(shard, hb, 5_000, :not_fenced)
    end
  end

  # Open a shard, force the heartbeat not-valid, backdate `not_valid_since` by `age_ms`, run one
  # fence verdict, and report whether the write-fence published.
  #
  # The verdict is ASYNC: `:durability_flush` spawns the flush task, and `note_not_valid/1` runs
  # when its `:fence_skip` result comes back through `handle_info({ref, result}, %{flush_task:
  # ...})`. So a `:sys.get_state` right after the send syncs only the spawn, not the decision —
  # the telemetry event is the real signal, which is why the test above uses it too.
  defp assert_fence_at(shard, hb, age_ms, expected) do
    Application.put_env(:fathom, :fence_writes_when_stealable, true)
    on_exit(fn -> Application.delete_env(:fathom, :fence_writes_when_stealable) end)

    test_pid = self()
    handler = "fencetiming-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :write_fenced],
      fn _e, _m, meta, _ -> send(test_pid, {:write_fenced, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))
      {:ok, coordinator} = Shards.ensure(shard)

      :sys.replace_state(hb, fn s ->
        forged = %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000}
        Heartbeat.publish_status(forged)
        forged
      end)

      :sys.replace_state(coordinator, fn s ->
        %{s | not_valid_since: System.monotonic_time(:millisecond) - age_ms}
      end)

      send(coordinator, :durability_flush)

      case expected do
        :fenced ->
          assert_receive {:write_fenced, %{shard_id: ^shard}}, 2_000
          assert WriteFence.fenced?(shard)

        :not_fenced ->
          refute_receive {:write_fenced, %{shard_id: ^shard}}, 1_000

          refute WriteFence.fenced?(shard),
                 "fenced #{age_ms}ms in, before the shard is provably stealable — writes are " <>
                   "being refused while this node is still the legitimate owner"
      end

      :ok = ShardExecutor.close(conn)
    end)
  end

  defp hb_file,
    do: Path.join([Fathom.Shard.Storage.Local.dir(), "heartbeats", to_string(node())])
end
