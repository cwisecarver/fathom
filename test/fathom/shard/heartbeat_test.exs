defmodule Fathom.Shard.HeartbeatTest do
  @moduledoc """
  The per-node liveness heartbeat: it renews one object per node and exposes the
  fence the coordinator uses (generation + valid_for_write?). Lapse detection is
  driven deterministically via :sys.replace_state (force an expired heartbeat, then
  a renew tick) rather than wall-clock sleeps.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shard.{Heartbeat, Storage}

  @ttl 30_000

  setup do
    # Each test gets its own remote dir so heartbeat objects never collide.
    dir = Path.join(System.tmp_dir!(), "fathom_hbproc_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, Fathom.Shard.Storage.Local, prev),
        else: Application.delete_env(:fathom, Fathom.Shard.Storage.Local)
    end)

    owner = "node_#{System.unique_integer([:positive])}@test"
    pid = start_supervised!({Heartbeat, ttl_ms: @ttl, owner: owner})
    # init does the first renew via handle_continue; sync so it's written.
    _ = :sys.get_state(pid)
    %{pid: pid, owner: owner}
  end

  test "writes a fresh heartbeat on start and reports generation 0", %{owner: owner} do
    assert {:ok, %{owner: ^owner, expires_at_ms: exp}} = Storage.read_heartbeat(owner)
    assert exp > System.system_time(:millisecond)
    assert Heartbeat.generation() == 0
  end

  test "valid_for_write? is :ok when fresh and the generation matches" do
    assert Heartbeat.valid_for_write?(0) == :ok
    # A generation mismatch (without a lapse here) is treated as needing revalidation.
    assert Heartbeat.valid_for_write?(999) == :revalidate
  end

  test "a renew tick advances the heartbeat expiry", %{pid: pid, owner: owner} do
    {:ok, %{expires_at_ms: before}} = Storage.read_heartbeat(owner)
    # Force the stored expiry backwards so a renew is observably newer, then tick.
    :sys.replace_state(pid, fn s -> %{s | expires_at_ms: before - 5_000} end)
    send(pid, :renew)
    _ = :sys.get_state(pid)

    {:ok, %{expires_at_ms: after_exp}} = Storage.read_heartbeat(owner)
    assert after_exp >= before
  end

  test "a lapse bumps the generation and makes prior acquirers revalidate", %{pid: pid} do
    assert Heartbeat.generation() == 0
    assert Heartbeat.valid_for_write?(0) == :ok

    # Simulate a missed renewal: the heartbeat expired before the next tick ran.
    :sys.replace_state(pid, fn s ->
      %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000}
    end)

    capture_log(fn ->
      send(pid, :renew)
      _ = :sys.get_state(pid)
    end)

    # The lapse bumped the generation; the renew recovered a fresh heartbeat.
    assert Heartbeat.generation() == 1
    # A coordinator that acquired at generation 0 must now revalidate before flushing.
    assert Heartbeat.valid_for_write?(0) == :revalidate
    # A coordinator acquiring now (generation 1) is clean.
    assert Heartbeat.valid_for_write?(1) == :ok
  end

  test "not_valid when the heartbeat is not comfortably valid", %{pid: pid} do
    # Drive the confirmed expiry into the past: no comfortable margin ⇒ no write.
    # (publish_status keeps the lock-free ETS view — what valid_for_write? reads
    # post round-2 #26 — in sync with the forged state.)
    :sys.replace_state(pid, fn s ->
      Heartbeat.publish_status(%{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1})
    end)

    assert Heartbeat.valid_for_write?(0) == :not_valid
  end

  # Expert review #21: the local validity/lapse decisions used the wall clock, so a
  # backward clock step (NTP correction, VM live-migration) after a renewal inflated
  # perceived remaining validity — a genuine lapse (during which a peer with a correct
  # clock legitimately stole shards) was never edge-detected: no generation bump,
  # valid_for_write? kept saying :ok, and flushes skipped revalidation against the
  # stealer. The invariant: local expiry is elapsed time (monotonic), immune to steps.
  # Simulated by the state a backward step produces: wall-clock expiry in the future,
  # real (monotonic) deadline already passed.
  test "a backward wall-clock step cannot mask a lapse", %{pid: pid} do
    assert Heartbeat.valid_for_write?(0) == :ok

    :sys.replace_state(pid, fn s ->
      Heartbeat.publish_status(%{
        s
        | expires_at_ms: System.system_time(:millisecond) + 3_600_000,
          mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000
      })
    end)

    assert Heartbeat.valid_for_write?(0) == :not_valid,
           "a stepped-back wall clock must not make an expired heartbeat look valid"

    capture_log(fn ->
      send(pid, :renew)
      _ = :sys.get_state(pid)
    end)

    assert Heartbeat.generation() == 1,
           "the lapse must be edge-detected off elapsed (monotonic) time"

    assert Heartbeat.valid_for_write?(0) == :revalidate
  end

  # Round-2 #26: the lapse broadcast fans a fence check out to every open
  # coordinator at the same instant, and each check was a GenServer.call into this
  # single process — the flood serialized on its mailbox, and a timed-out call
  # degraded to :legacy, resurrecting the per-shard renew_lease PUT storm (the F1
  # regression) exactly when the node was unhealthy. The invariant: the fence
  # reads (generation + validity) answer LOCK-FREE, even while the heartbeat
  # process itself is completely unresponsive.
  test "fence reads answer while the heartbeat process is unresponsive", %{pid: pid} do
    :ok = :sys.suspend(pid)

    task = Task.async(fn -> {Heartbeat.valid_for_write?(0), Heartbeat.generation()} end)

    result = Task.await(task, 500)
    :ok = :sys.resume(pid)

    assert {:ok, 0} == result,
           "fence reads must not block on the heartbeat's mailbox (pre-fix: GenServer.call)"
  end

  test "clears its heartbeat on clean shutdown", %{pid: pid, owner: owner} do
    assert {:ok, _} = Storage.read_heartbeat(owner)
    ref = Process.monitor(pid)
    :ok = stop_supervised(Fathom.Shard.Heartbeat)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    assert Storage.read_heartbeat(owner) == :not_found
  end

  # Expert review #6: the coordinator's lease owner is the incarnation-qualified
  # node()#<nonce>, and a same-machine restart clears the PREVIOUS incarnation's
  # heartbeat object (its nonce is persisted locally) — otherwise the node's own old
  # locks would stay {:held} against it for up to a heartbeat TTL after a crash.
  test "owner is incarnation-qualified and a restart clears the previous incarnation's heartbeat" do
    assert Heartbeat.owner() =~ ~r/##{Heartbeat.incarnation()}\z/

    # Simulate the previous boot: its nonce persisted locally, its object still fresh.
    inc_file = Path.join(Path.dirname(Fathom.Shard.db_path("x")), ".incarnation")
    prev_content = File.read(inc_file)
    File.mkdir_p!(Path.dirname(inc_file))
    File.write!(inc_file, "prevnonce99")
    prev_owner = "#{node()}#prevnonce99"
    {:ok, _} = Storage.renew_heartbeat(prev_owner, 60_000)

    on_exit(fn ->
      case prev_content do
        {:ok, c} -> File.write!(inc_file, c)
        _ -> File.rm(inc_file)
      end
    end)

    # A default-owner boot (the app path). The predecessor's heartbeat is FRESH, so
    # it is not cleared immediately (expert review #16: fresh can't distinguish
    # "crashed seconds ago" from "alive on a shared volume") — the boot schedules a
    # re-check instead.
    :ok = stop_supervised(Fathom.Shard.Heartbeat)
    pid = start_supervised!({Heartbeat, ttl_ms: @ttl}, id: :heartbeat_default_owner)
    _ = :sys.get_state(pid)

    assert {:ok, %{expires_at_ms: prev_exp}} = Storage.read_heartbeat(prev_owner),
           "a still-fresh predecessor heartbeat must not be cleared before the re-check"

    # Drive the scheduled re-check: the predecessor's expiry is FROZEN (no renewer —
    # it is dead), so the second read clears it — the #6 fast restart, one renew
    # interval late instead of a full TTL.
    send(pid, {:clear_previous_incarnation, prev_owner, prev_exp})
    _ = :sys.get_state(pid)

    assert Storage.read_heartbeat(prev_owner) == :not_found,
           "a frozen (dead) previous incarnation's heartbeat must be cleared so its locks are stealable"

    assert File.read!(inc_file) == Heartbeat.incarnation()
  end

  # Expert review round-2 #16: sharing the .incarnation file is NOT proof the
  # predecessor is dead — a persisted/remounted :shard_data_dir (hostPath/PV/NFS) is
  # shared with a node still serving (termination grace, or a PV remounted across
  # machines). Clearing a LIVE node's heartbeat declares its every long-held shard
  # instantly stealable (locks are never renewed in heartbeat mode) while the victim
  # keeps serving — fleet-wide split-brain. The invariant: a heartbeat whose expiry
  # ADVANCES between the boot read and the re-check has a live renewer and must
  # never be cleared.
  test "a live predecessor's renewed heartbeat is refused, not cleared" do
    inc_file = Path.join(Path.dirname(Fathom.Shard.db_path("x")), ".incarnation")
    prev_content = File.read(inc_file)
    File.mkdir_p!(Path.dirname(inc_file))
    File.write!(inc_file, "livenonce42")
    prev_owner = "#{node()}#livenonce42"
    {:ok, _} = Storage.renew_heartbeat(prev_owner, 60_000)

    on_exit(fn ->
      Storage.clear_heartbeat(prev_owner)

      case prev_content do
        {:ok, c} -> File.write!(inc_file, c)
        _ -> File.rm(inc_file)
      end
    end)

    :ok = stop_supervised(Fathom.Shard.Heartbeat)
    pid = start_supervised!({Heartbeat, ttl_ms: @ttl}, id: :heartbeat_live_prev)
    _ = :sys.get_state(pid)

    assert {:ok, %{expires_at_ms: exp_at_boot}} = Storage.read_heartbeat(prev_owner),
           "a fresh predecessor heartbeat must survive the boot read"

    # The predecessor is ALIVE: it renews between the boot read and the re-check.
    {:ok, _} = Storage.renew_heartbeat(prev_owner, 90_000)

    log =
      capture_log(fn ->
        send(pid, {:clear_previous_incarnation, prev_owner, exp_at_boot})
        _ = :sys.get_state(pid)
      end)

    assert {:ok, _} = Storage.read_heartbeat(prev_owner),
           "a heartbeat with a live renewer must never be cleared (split-brain steal window)"

    assert log =~ "refusing to clear previous incarnation"
  end

  # The other side of #16's trade: a predecessor already stale by the steal margin is
  # dead for certain (a live renewer can never leave its heartbeat stale), so the #6
  # fast path clears it immediately at boot — no re-check wait.
  test "a stale previous incarnation's heartbeat is cleared immediately at boot" do
    inc_file = Path.join(Path.dirname(Fathom.Shard.db_path("x")), ".incarnation")
    prev_content = File.read(inc_file)
    File.mkdir_p!(Path.dirname(inc_file))
    File.write!(inc_file, "deadnonce77")
    prev_owner = "#{node()}#deadnonce77"
    # Expired well past the steal margin (a negative TTL back-dates the expiry).
    {:ok, _} = Storage.renew_heartbeat(prev_owner, -60_000)

    on_exit(fn ->
      Storage.clear_heartbeat(prev_owner)

      case prev_content do
        {:ok, c} -> File.write!(inc_file, c)
        _ -> File.rm(inc_file)
      end
    end)

    :ok = stop_supervised(Fathom.Shard.Heartbeat)
    pid = start_supervised!({Heartbeat, ttl_ms: @ttl}, id: :heartbeat_stale_prev)
    _ = :sys.get_state(pid)

    assert Storage.read_heartbeat(prev_owner) == :not_found,
           "a stale-by-margin predecessor heartbeat is provably dead and clears at boot"

    # Round-2 #34: the fast-restart path was incomplete — post-clear, the
    # predecessor's RECENTLY-RENEWED locks (fresh TTL) fell to the :not_found
    # lock-TTL fallback and blocked this node ~TTL+margin per recently-held shard.
    # A PROVEN-dead incarnation's fresh lock must be stealable immediately.
    dir = Application.get_env(:fathom, Fathom.Shard.Storage.Local)[:dir]
    File.mkdir_p!(dir)

    File.write!(
      Path.join(dir, "sh34.lock"),
      Jason.encode!(%{
        "owner" => prev_owner,
        "epoch" => 1,
        "expires_at_ms" => System.system_time(:millisecond) + 60_000
      })
    )

    # UPDATED 2026-08-22 (expert review 2026-08-20 #10). This used to assert the steal succeeds on
    # the FIRST attempt. It no longer does, and the change is deliberate rather than a regression:
    # a cleared heartbeat proves the heartbeat OBJECT stopped, not that the PROCESS did, and a node
    # whose Heartbeat GenServer died while it kept serving reads identically — while going on
    # renewing every lock it holds via the legacy fence. Stealing from it is a double-serve window.
    #
    # So the fast path now additionally requires a two-read renewal probe (`Storage`'s
    # `fast_steal_ok?/4`): one observation starts the clock, and a second of the SAME lock, a probe
    # window later with an unchanged expiry, proves nobody is renewing it.
    #
    # #34's win is REDUCED, not removed, and that trade was made explicitly: the wait goes from
    # `ttl + margin` (~40 s at defaults) to `ttl/2` (~15 s), and it is paid ONCE PER INCARNATION
    # rather than once per shard — so a node holding a thousand shards pays it once.
    assert {:error, {:held, ^prev_owner, _}} =
             Storage.acquire_lease("sh34", "contender@node", 30_000),
           "the first attempt must NOT fast-steal: one observation of a lock proves nothing " <>
             "about whether anyone is renewing it"

    # Second observation, past the probe window, expiry unchanged. Driven by moving the window to
    # zero rather than by sleeping — the state machine takes the clock as an argument precisely so
    # no test has to wait out a real renewal cadence.
    prev_probe = Application.get_env(:fathom, :lease_quiescence_probe_ms)
    Application.put_env(:fathom, :lease_quiescence_probe_ms, 0)

    on_exit(fn ->
      if is_nil(prev_probe),
        do: Application.delete_env(:fathom, :lease_quiescence_probe_ms),
        else: Application.put_env(:fathom, :lease_quiescence_probe_ms, prev_probe)
    end)

    assert {:ok, %{epoch: 2, took_over: true}} =
             Storage.acquire_lease("sh34", "contender@node", 30_000),
           "once the probe has settled, a proven-dead incarnation's fresh lock must be stealable " <>
             "without waiting out the ~40s lock-TTL fallback — that is round-2 #34's win and it " <>
             "must survive #10"
  end

  # Expert review #8: the lapse generation was process-local state resetting to 0 on
  # restart, so a coordinator that acquired at generation 0 of the PREVIOUS heartbeat
  # incarnation compared 0 != 0 → false → :ok, and flushed without revalidating — even
  # though the restart gap is exactly a window in which a lapse (and a steal) may have
  # occurred. The invariant: "no lapse since acquire" must not be forged by a restart;
  # every re-boot of an already-seen owner counts as a lapse episode.
  test "a heartbeat restart bumps the generation so prior acquirers revalidate",
       %{pid: pid, owner: owner} do
    assert Heartbeat.generation() == 0
    assert Heartbeat.valid_for_write?(0) == :ok

    # Subscribe like a coordinator does (expert review #34), so the restart's
    # broadcast delivery is asserted below.
    :ok = Phoenix.PubSub.subscribe(Fathom.PubSub, Heartbeat.topic())

    ref = Process.monitor(pid)
    :ok = stop_supervised(Fathom.Shard.Heartbeat)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

    # The supervisor brings it back with the same owner (crash-restart / new boot).
    pid2 = start_supervised!({Heartbeat, ttl_ms: @ttl, owner: owner}, id: :heartbeat_restarted)
    _ = :sys.get_state(pid2)

    assert Heartbeat.generation() == 1, "a restart must count as a lapse episode"

    assert Heartbeat.valid_for_write?(0) == :revalidate,
           "a coordinator acquired before the restart must revalidate ownership"

    assert Heartbeat.valid_for_write?(1) == :ok

    # Expert review round-2 #15: the restart bumped the generation but never
    # BROADCAST it, so subscribed coordinators (the #34 proactive fence) kept stale
    # acquire_gens until their next flush — unboundedly with the periodic flush
    # disabled — while the restart gap is a real steal window. A restart must
    # proactively notify exactly like a lapse.
    assert_receive {:heartbeat_lapsed, 1},
                   1_000,
                   "a heartbeat restart must broadcast the lapse so coordinators revalidate now"
  end

  # Expert review #7: terminate/2 cleared the heartbeat object for ANY reason —
  # including a crash the supervisor immediately reverses (a raise in a callback runs
  # terminate with the exception reason before exiting). During the restart gap the
  # object was absent, so a contender's owner_live? hit the :not_found fallback,
  # judged every long-lived shard on this perfectly-healthy node dead (locks are
  # never renewed in heartbeat mode, so their TTLs are long stale), and stole them
  # while the node was still serving — a fleet-wide split-brain window per crash.
  # The invariant: only a PLANNED shutdown deletes the liveness object.
  test "a crash does NOT clear the heartbeat object", %{pid: pid, owner: owner} do
    assert {:ok, _} = Storage.read_heartbeat(owner)
    ref = Process.monitor(pid)

    capture_log(fn ->
      # An abnormal stop reason takes the crash path through terminate/2.
      GenServer.stop(pid, :simulated_crash)
      assert_receive {:DOWN, ^ref, :process, ^pid, :simulated_crash}, 1_000
    end)

    assert {:ok, _} = Storage.read_heartbeat(owner),
           "a crash must leave the heartbeat object for the restarted process to re-renew"
  end

  # Expert review 2026-08-26 #14. Four compounding properties on the renewal path, and the finding
  # notes WHY they all survived: nothing in this file exercised a slow or failing renew.
  #
  #   (a) renew_heartbeat is a PUT, and Req's default `retry: :safe_transient` covers only
  #       GET/HEAD — so a transient 5xx or socket error lost a whole renewal cycle, unretried.
  #   (b) the S3 request set connect_timeout but neither receive_timeout (Req default 15 000 ms)
  #       nor pool_timeout (Finch default 5 000 ms), so one stalled attempt could occupy ~23 s
  #       against a 10 s cadence and a 30 s TTL.
  #   (c) schedule_renew armed the next tick renew_ms after the previous attempt RETURNED, so the
  #       cadence degraded in proportion to store latency — backwards for a liveness signal.
  #
  # Two missed renewals put valid_for_write?/1 at :not_valid, which stops EVERY durability flush
  # on the node, starts the write-fence clock, and at margin + steal_margin 503s the node's writes
  # fleet-wide; past ttl + steal_margin peers may steal its whole keyspace slice while it is
  # healthy and serving. The trigger is ordinary pool contention, not a partition.
  describe "renewal is bounded and retried within its cycle (#14)" do
    setup do
      prev_storage = Application.get_env(:fathom, :shard_storage)
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

      on_exit(fn ->
        Application.delete_env(:fathom, :faulty_renew_heartbeat)

        if prev_storage,
          do: Application.put_env(:fathom, :shard_storage, prev_storage),
          else: Application.delete_env(:fathom, :shard_storage)
      end)

      :ok
    end

    test "a transient failure is retried inside the SAME cycle, not lost to the next one",
         %{pid: pid, owner: owner} do
      test_pid = self()
      attempts = :counters.new(1, [])

      # Fail the first attempt, pass the second — the (a) shape exactly.
      Application.put_env(:fathom, :faulty_renew_heartbeat, fn _opts ->
        n = :counters.get(attempts, 1) + 1
        :counters.add(attempts, 1, 1)
        send(test_pid, {:renew_attempt, n})
        if n == 1, do: {:error, {:transient, :s3_unreachable}}, else: :pass
      end)

      {:ok, %{expires_at_ms: before_exp}} = Storage.read_heartbeat(owner)

      capture_log(fn ->
        send(pid, :renew)
        # Sync on the GenServer having finished the whole cycle.
        _ = :sys.get_state(pid)
      end)

      assert_received {:renew_attempt, 1}

      assert_received {:renew_attempt, 2},
                      "a transient renew failure was not retried inside its cycle; " <>
                        "pre-fix this lost the whole cycle and two lost cycles lapse the node"

      {:ok, %{expires_at_ms: after_exp}} = Storage.read_heartbeat(owner)
      assert after_exp >= before_exp, "the retry did not actually renew the stored heartbeat"

      # And the in-cycle recovery means the node never entered a lapse.
      refute :sys.get_state(pid).lapsed
    end

    test "each attempt carries a budget well under the renewal cadence", %{pid: pid} do
      test_pid = self()

      Application.put_env(:fathom, :faulty_renew_heartbeat, fn opts ->
        send(test_pid, {:renew_opts, opts})
        :pass
      end)

      send(pid, :renew)
      _ = :sys.get_state(pid)

      assert_received {:renew_opts, opts}
      budget = Keyword.get(opts, :budget_ms)

      renew_ms = :sys.get_state(pid).renew_ms

      assert is_integer(budget) and budget > 0,
             "the renew PUT carries no per-attempt budget, so Req's 15s receive_timeout and " <>
               "Finch's 5s pool_timeout apply — ~23s against a #{renew_ms}ms cadence"

      assert budget <= div(renew_ms, 3) + 1,
             "the per-attempt budget (#{budget}ms) must leave room for more than one attempt " <>
               "inside the #{renew_ms}ms cycle"
    end

    test "the whole retry loop stays inside one cycle when every attempt fails",
         %{pid: pid} do
      attempts = :counters.new(1, [])

      Application.put_env(:fathom, :faulty_renew_heartbeat, fn _opts ->
        :counters.add(attempts, 1, 1)
        {:error, {:transient, :s3_unreachable}}
      end)

      capture_log(fn ->
        send(pid, :renew)
        _ = :sys.get_state(pid)
      end)

      # Bounded: it must retry, and it must stop. An unbounded loop here would block the
      # heartbeat process past its own TTL, which is the failure it exists to prevent.
      n = :counters.get(attempts, 1)
      assert n > 1, "a failing renew was not retried at all"
      assert n <= 3, "the retry loop is not bounded by the cycle (#{n} attempts)"
    end
  end

  # The (c) half, pinned on the pure function rather than on wall-clock. A timing assertion here
  # is exactly the flake shape AGENTS.md records; `renew_delay_ms/2` is the whole rule.
  describe "renewal is scheduled at a FIXED RATE (#14c)" do
    test "the next tick is measured from the START of the cycle, not from when it returned" do
      # Pre-fix the delay was always renew_ms regardless of elapsed, so a cycle that took 9 s
      # against a 10 s cadence produced a 19 s gap — the cadence degrading in proportion to store
      # latency, which is backwards for a liveness signal.
      assert Heartbeat.renew_delay_ms(10_000, 0) == 10_000
      assert Heartbeat.renew_delay_ms(10_000, 4_000) == 6_000
      assert Heartbeat.renew_delay_ms(10_000, 9_000) == 1_000

      # A cycle that consumed its whole budget re-fires immediately rather than adding another
      # full interval on top. Never negative — Process.send_after/3 rejects that.
      assert Heartbeat.renew_delay_ms(10_000, 10_000) == 0
      assert Heartbeat.renew_delay_ms(10_000, 25_000) == 0
    end
  end

  # Expert review 2026-08-26 #18. `mark_lapse/1` called `broadcast_lapse/1` INLINE, and
  # `Phoenix.PubSub.broadcast/3` runs `Registry.dispatch/3`, which for a `:duplicate` registry
  # dispatches in the CALLING process — an `:ets.lookup` per partition copying one entry per
  # subscribed coordinator into the heartbeat's own heap, then one `send` each. Every open
  # coordinator on the node subscribes to that single topic.
  #
  # MEASURED against a synthetic 30 000-subscriber topic, which is the finding's own falsifying
  # experiment ("if it is microseconds, the dispatch is not the cost"): **p50 35 ms**, range
  # 32-41 ms over ten samples. So the one component whose entire purpose is to be O(1) per node —
  # the F1 fix that replaced ~100k PUT/s with one PUT — was doing 35 ms of O(open-shards) work
  # inside its own critical path, at the moment it is already behind. Every millisecond there
  # extends the lapse, and the lapse is what triggers #13's revalidation fan-out.
  describe "the lapse fan-out does not run in the heartbeat process (#18)" do
    setup do
      test_pid = self()
      handler = "lapsefan-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler,
          [:fathom, :shard, :heartbeat, :lapse_broadcast],
          fn _e, _m, meta, _c -> send(test_pid, {:lapse_broadcast, meta.inline}) end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)
      :ok
    end

    # Forces the lapse edge: an expired deadline plus a renew tick.
    defp force_lapse(pid) do
      capture_log(fn ->
        :sys.replace_state(pid, fn s ->
          %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000, lapsed: false}
        end)

        send(pid, :renew)
        _ = :sys.get_state(pid)
      end)
    end

    test "the fan-out runs OFF the heartbeat process", %{pid: pid} do
      # THE HONEST ASSERTION, after a first draft that was not one. "The heartbeat is not blocked"
      # is NOT observable at test scale: `Registry.dispatch/3`'s cost is per subscriber, and with
      # two subscribers it is microseconds whether it runs inline or in a task. A test that
      # asserted delivery would have passed either way.
      #
      # So assert the structural property instead — the dispatch is performed by a task, reported
      # by the event emitted where the fan-out actually happens.
      force_lapse(pid)

      assert_receive {:lapse_broadcast, false},
                     5_000,
                     "the lapse fan-out ran INLINE in the heartbeat process. Measured at p50 " <>
                       "35 ms against 30 000 subscribers, inside the critical path that ends " <>
                       "the lapse."
    end

    test "the generation bump and publish stay INLINE, so the fence sees the lapse immediately",
         %{pid: pid} do
      # The finding's integrity guard. Moving the bump or `publish_status/1` off-process would let
      # `valid_for_write?/1` keep answering with the pre-lapse generation while the broadcast was
      # still queued — a window where the node believes it is valid and nothing says otherwise.
      before_gen = Heartbeat.generation()

      capture_log(fn ->
        :sys.replace_state(pid, fn s ->
          %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000, lapsed: false}
        end)

        send(pid, :renew)
        _ = :sys.get_state(pid)
      end)

      # Read through the lock-free status table, which is what the coordinators' fence reads.
      assert Heartbeat.generation() > before_gen,
             "the generation bump did not land synchronously with mark_lapse/1 — the fence would " <>
               "still read the pre-lapse generation while the broadcast was in flight"
    end

    test "a missing task supervisor falls back to broadcasting inline, never to silence", %{
      pid: pid
    } do
      # The scale/bench harness runs without the full tree. A lapse that notified nobody would be
      # worse than a slow one, so the fallback is inline rather than skip — and it SAYS so, which
      # is what makes a sustained inline rate alertable rather than invisible.
      sup = Process.whereis(Fathom.TaskSupervisor)

      if sup do
        Process.unregister(Fathom.TaskSupervisor)

        on_exit(fn ->
          if Process.alive?(sup), do: Process.register(sup, Fathom.TaskSupervisor)
        end)
      end

      force_lapse(pid)

      assert_receive {:lapse_broadcast, true},
                     5_000,
                     "with no task supervisor the lapse was not broadcast at all — silence is " <>
                       "worse than a slow heartbeat here"
    end
  end
end
