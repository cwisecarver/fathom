defmodule Fathom.ShardLeaseTest do
  # Cross-node single-writer leasing: the Storage lease primitive (acquire /
  # steal-on-expiry / renew / release with an epoch fencing token) and the
  # coordinator's use of it (refuse-to-start on a foreign lease, fence the flush,
  # self-fence on a lost renewal). Not async: shards and the lock files are global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "lease_#{System.unique_integer([:positive])}"

    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_ttl = Application.get_env(:fathom, :shard_lease_ttl_ms)

    on_exit(fn ->
      restore(:shard_idle_ms, prev_idle)
      restore(:shard_lease_ttl_ms, prev_ttl)

      for dir <- [@local_dir, @remote_dir],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      # Heartbeats are keyed by owner (not shard); drop the simulated foreign nodes'.
      # Owner is percent-encoded in the path (round-2 #3).
      for owner <- ["a@node", "b@node", "thief@node"],
          do: File.rm(Path.join([@remote_dir, "heartbeats", URI.encode_www_form(owner)]))
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp now_ms, do: System.system_time(:millisecond)
  defp local_db(shard), do: Path.join(@local_dir, "#{shard}.db")
  defp remote_db(shard), do: Path.join(@remote_dir, "#{shard}.db")
  defp lock_file(shard), do: Path.join(@remote_dir, "#{shard}.lock")

  # Write a lock file directly to simulate another node's lease.
  defp put_raw_lock(shard, owner, epoch, expires_at_ms) do
    File.mkdir_p!(@remote_dir)

    File.write!(
      lock_file(shard),
      Jason.encode!(%{"owner" => owner, "epoch" => epoch, "expires_at_ms" => expires_at_ms})
    )
  end

  # Write a heartbeat directly to simulate another node being alive (liveness is the
  # per-node heartbeat now, not the lock's TTL).
  defp put_raw_heartbeat(owner, expires_at_ms) do
    dir = Path.join(@remote_dir, "heartbeats")
    File.mkdir_p!(dir)

    # Encode the owner to match the backend's heartbeat_path (round-2 #3).
    File.write!(
      Path.join(dir, URI.encode_www_form(owner)),
      Jason.encode!(%{"owner" => owner, "expires_at_ms" => expires_at_ms})
    )
  end

  # ── Storage lease primitive ──

  test "lease_holder: read-only probe — free, held-by-live, free-once-dead (review 2026-07-09 #1)",
       %{shard: shard} do
    # No lock → free.
    assert Storage.lease_holder(shard) == :free

    # A foreign owner with a fresh lock (no heartbeat → TTL fallback → live) → held.
    put_raw_lock(shard, "a@node", 1, now_ms() + 30_000)
    assert {:held, "a@node"} = Storage.lease_holder(shard)

    # A live heartbeat also reports held.
    put_raw_heartbeat("a@node", now_ms() + 30_000)
    assert {:held, "a@node"} = Storage.lease_holder(shard)

    # An expired lock past the steal margin with no fresh heartbeat → dead → free (stealable).
    File.rm(Path.join([@remote_dir, "heartbeats", URI.encode_www_form("a@node")]))
    put_raw_lock(shard, "a@node", 1, now_ms() - 60_000)
    assert Storage.lease_holder(shard) == :free

    # Read-only: the lock is still exactly what we put (owner a@node / epoch 1), not reclaimed.
    assert :ok = Storage.check_lease(shard, %{owner: "a@node", epoch: 1})
  end

  test "acquire on a fresh shard returns epoch 1; a live owner blocks other owners",
       %{shard: shard} do
    assert {:ok, %{owner: "a@node", epoch: 1}} = Storage.acquire_lease(shard, "a@node", 60_000)
    # a is alive: its heartbeat is fresh, so b cannot steal.
    put_raw_heartbeat("a@node", now_ms() + 60_000)
    assert {:error, {:held, "a@node"}} = Storage.acquire_lease(shard, "b@node", 60_000)
  end

  test "an expired lease is stolen and the epoch bumps", %{shard: shard} do
    # Expired past the steal margin: with no heartbeat object, liveness falls back to the lock's
    # own TTL (finding #11), so only a genuinely-lapsed lease is stealable.
    put_raw_lock(shard, "a@node", 5, now_ms() - 60_000)

    assert {:ok, %{owner: "b@node", epoch: 6}} = Storage.acquire_lease(shard, "b@node", 60_000)
  end

  test "reclaiming our own (stale) lease keeps the epoch", %{shard: shard} do
    put_raw_lock(shard, "a@node", 7, now_ms() - 1_000)

    assert {:ok, %{owner: "a@node", epoch: 7, expires_at_ms: exp}} =
             Storage.acquire_lease(shard, "a@node", 60_000)

    assert exp > now_ms()
  end

  test "renew extends the holder's lease but is superseded after a steal", %{shard: shard} do
    {:ok, lease} = Storage.acquire_lease(shard, "a@node", 60_000)
    assert {:ok, %{owner: "a@node", epoch: 1}} = Storage.renew_lease(shard, lease, 60_000)

    # Another node steals it out from under us.
    put_raw_lock(shard, "b@node", 2, now_ms() + 60_000)
    assert {:error, :superseded} = Storage.renew_lease(shard, lease, 60_000)
  end

  test "release removes the holder's lock but not someone else's", %{shard: shard} do
    {:ok, lease} = Storage.acquire_lease(shard, "a@node", 60_000)

    # A non-holder release is a no-op.
    assert :ok = Storage.release_lease(shard, %{owner: "b@node", epoch: 1, expires_at_ms: 0})
    assert File.exists?(lock_file(shard))

    assert :ok = Storage.release_lease(shard, lease)
    refute File.exists?(lock_file(shard))
  end

  # ── Coordinator integration ──

  test "checkout fails when another live node holds the lock", %{shard: shard} do
    put_raw_lock(shard, "thief@node", 3, now_ms() + 60_000)
    # The holder is alive (fresh heartbeat), so we must NOT steal it.
    put_raw_heartbeat("thief@node", now_ms() + 60_000)

    capture_log(fn ->
      assert {:error, {:shard_held, "thief@node"}} = Shards.checkout(shard)
    end)
  end

  test "a held-lease checkout leaves no local copy, even with an object to speculatively pull",
       %{shard: shard} do
    # init overlaps the lease acquire with the pull (into a temp file). With an
    # object in storage AND a foreign live lease, the refused start must leave NO
    # local copy or temp — a stale local file would wrongly win on a later open.
    File.mkdir_p!(@remote_dir)
    File.write!(remote_db(shard), "sqlite-ish bytes")
    put_raw_lock(shard, "thief@node", 3, now_ms() + 60_000)
    put_raw_heartbeat("thief@node", now_ms() + 60_000)

    capture_log(fn ->
      assert {:error, {:shard_held, "thief@node"}} = Shards.checkout(shard)
    end)

    refute File.exists?(local_db(shard)), "refused start must leave no promoted local copy"

    refute File.exists?(local_db(shard) <> ".pull"),
           "the speculative pull temp must be cleaned up"

    assert File.exists?(remote_db(shard)), "the storage object is untouched"
  end

  test "the flush is fenced: a coordinator that lost its lease drops local without flushing",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    # Another node steals the lease while we hold local-only writes.
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      # Releasing the last connection lets the shard idle → flush. The flush
      # re-checks ownership, sees the steal, and drops local WITHOUT flushing.
      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 1_000
    end)

    refute File.exists?(remote_db(shard)), "fenced coordinator must not flush over the new owner"
    refute File.exists?(local_db(shard)), "fenced coordinator drops its local copy"
  end

  test "renewal self-fences: losing the lease stops the coordinator without flushing",
       %{shard: shard} do
    # Short TTL so renewal fires fast; leave idle at its (long) default so the
    # renewal path — not the idle path — is what stops the coordinator.
    Application.put_env(:fathom, :shard_lease_ttl_ms, 60)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    # Drop to zero connections (idle is the long default, so it won't fire), then
    # steal the lease — the next renewal tick detects it and self-fences.
    :ok = ShardExecutor.close(conn)
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      # Drive the renewal check directly rather than waiting on the periodic timer,
      # which can slip past the assert_receive window under heavy machine load.
      send(coordinator, :renew_lease)

      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 1_000
    end)

    refute File.exists?(remote_db(shard)), "self-fenced coordinator must not flush"
    refute File.exists?(local_db(shard)), "self-fenced coordinator drops its local copy"
  end

  # Round-2 #26: the lapse broadcast reaches every open coordinator at the same
  # instant, and each ran Fence.check INLINE in handle_info — a synchronous
  # O(open-shards) storm of storage GETs through one pool, fired exactly when the
  # node just proved unhealthy (mailboxes blocked, checkouts queued fleet-wide).
  # The invariant: the broadcast only SCHEDULES a jittered revalidation, repeat
  # broadcasts coalesce onto the pending timer, the shard keeps serving through
  # the window, and the deferred check still self-fences on a genuine steal.
  test "a lapse broadcast defers revalidation behind jitter and coalesces repeats",
       %{shard: shard} do
    hb = start_supervised!({Fathom.Shard.Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, coordinator} = Shards.ensure(shard)
    %{acquire_gen: gen} = :sys.get_state(coordinator)
    assert gen != nil, "with the heartbeat up, the coordinator must be in heartbeat mode"

    # Two broadcasts: the first schedules, the second coalesces — neither runs the
    # check inline (pre-fix: an immediate Fence.check per broadcast).
    send(coordinator, {:heartbeat_lapsed, gen + 1})
    send(coordinator, {:heartbeat_lapsed, gen + 2})
    state = :sys.get_state(coordinator)
    assert state.lapse_revalidate_pending, "the broadcast must schedule, not revalidate inline"

    # The shard keeps serving through the jitter window.
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('during-jitter')"))

    # A steal really did happen during the lapse; the heartbeat's generation moved.
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    :sys.replace_state(hb, fn s ->
      Fathom.Shard.Heartbeat.publish_status(%{s | generation: s.generation + 1})
    end)

    ref = Process.monitor(coordinator)

    capture_log(fn ->
      # Drive the deferred revalidation directly (the jitter timer is wall-clock).
      send(coordinator, :revalidate_lapse)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 2_000
    end)

    refute File.exists?(remote_db(shard)),
           "the deferred revalidation must still self-fence without flushing"
  end
end
