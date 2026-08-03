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

      # Recovery: the heartbeat is comfortably valid again → a revalidate reconfirms ownership and
      # lifts the fence. revalidate_lapse runs the fence synchronously in the coordinator, so a
      # :sys.get_state after it is a clean sync point (no async flush-task race).
      :sys.replace_state(hb, fn s ->
        forged = %{s | mono_deadline_ms: System.monotonic_time(:millisecond) + 60_000}
        Heartbeat.publish_status(forged)
        forged
      end)

      send(coordinator, :revalidate_lapse)
      assert :sys.get_state(coordinator).not_valid_since == nil
      refute WriteFence.fenced?(shard)

      # And a write is accepted again.
      assert {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('c')"))

      :ok = ShardExecutor.close(conn)
    end)
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
