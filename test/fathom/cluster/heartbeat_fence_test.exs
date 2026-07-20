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

  @hb_file Path.join([System.tmp_dir!(), "fathom_remote_test", "heartbeats", to_string(node())])

  setup %{shard: shard} do
    # Idle-stop fast so the flush path runs during the test.
    Application.put_env(:fathom, :shard_idle_ms, 50)
    # Start the node heartbeat (the app leaves it off in test) → coordinators open in
    # heartbeat mode. Long TTL so it never lapses on its own; tests force a lapse.
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    on_exit(fn -> File.rm(@hb_file) end)
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
  # for ttl + steal_margin ("provably stealable"); ShardExecutor refuses writes with 503
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
end
