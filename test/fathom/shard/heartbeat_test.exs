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
    :sys.replace_state(pid, fn s ->
      %{s | mono_deadline_ms: System.monotonic_time(:millisecond) - 1}
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
      %{
        s
        | expires_at_ms: System.system_time(:millisecond) + 3_600_000,
          mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000
      }
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

    assert {:ok, %{epoch: 2, took_over: true}} =
             Storage.acquire_lease("sh34", "contender@node", 30_000),
           "a proven-dead incarnation's fresh lock must be stealable immediately, " <>
             "not after the ~35s lock-TTL fallback"
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
end
