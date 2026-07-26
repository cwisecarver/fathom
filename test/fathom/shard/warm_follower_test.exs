defmodule Fathom.Shard.WarmFollowerTest do
  @moduledoc """
  The warm-standby follower (Phase 2, A1-H1): pre-pulls the fleet's recently-active
  shards it doesn't own into a cache dir, so a failover finds them warm. Holds no
  lease and never serves — a pure read cache. Not async (global registry + shared
  storage dir); DataCase shared sandbox lets the follower read the directory.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.Shard.{Connection, Storage, WarmFollower}

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")
  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")

  setup do
    cache_dir =
      Path.join(System.tmp_dir!(), "fathom_warm_test_#{System.unique_integer([:positive])}")

    prev = Application.get_env(:fathom, :warm_cache_dir)
    Application.put_env(:fathom, :warm_cache_dir, cache_dir)

    on_exit(fn ->
      File.rm_rf!(cache_dir)

      if prev,
        do: Application.put_env(:fathom, :warm_cache_dir, prev),
        else: Application.delete_env(:fathom, :warm_cache_dir)
    end)

    :ok
  end

  defp uniq, do: System.unique_integer([:positive])

  # Register a shard in the directory (active, last_active_at now) and seed its S3 object.
  defp seed_shard(id, opts \\ []) do
    {:ok, _} = Directory.resolve(id)

    if Keyword.get(opts, :object, true) do
      seed = Path.join(System.tmp_dir!(), "warmseed_#{id}.db")
      {:ok, c} = Connection.open(seed)
      :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
      :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{id}')")
      :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(c)
      :ok = Storage.flush(id, seed)
      for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)
    end

    on_exit(fn ->
      for dir <- [@remote_dir, @local_dir],
          s <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, id <> s))
    end)

    id
  end

  # Drive one refresh cycle synchronously and return the cached set.
  defp refresh(pid), do: GenServer.call(pid, :refresh)

  test "warm_now skips an invalid shard_id gracefully; cache_path refuses to build the path (#6)" do
    # A poller runs warm_now straight from a command's shard_id. A path-traversal id must not
    # escape the cache dir — warm_now skips it (best-effort, no wedge) and cache_path fails
    # closed rather than joining `../...` into cache_dir.
    for bad <- ["../etc/passwd", "a/b", "acme.evil", "has space"] do
      assert {:error, :not_warmable} = WarmFollower.warm_now(bad)
      assert_raise ArgumentError, fn -> WarmFollower.cache_path(bad) end
    end

    # A valid id builds a path inside the cache dir.
    path = WarmFollower.cache_path("acme_1")
    assert Path.dirname(path) == Application.get_env(:fathom, :warm_cache_dir)
  end

  test "pre-pulls recently-active shards it doesn't own" do
    a = seed_shard("warm_a_#{uniq()}")
    b = seed_shard("warm_b_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    cached = refresh(pid)

    assert a in cached and b in cached
    assert WarmFollower.cached?(a)
    assert File.exists?(WarmFollower.cache_path(b))
  end

  test "cached_shard_ids lists the warm set in one directory read, skipping sidecars (#12)" do
    # The Reporter builds this set once per tick instead of N cached?/1 stat calls.
    a = seed_shard("warm_ids_a_#{uniq()}")
    b = seed_shard("warm_ids_b_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    refresh(pid)

    ids = MapSet.new(WarmFollower.cached_shard_ids())
    assert MapSet.member?(ids, a)
    assert MapSet.member?(ids, b)
    # The .db.etag / -wal / -shm sidecars are not mistaken for shard ids.
    refute Enum.any?(ids, &(String.contains?(&1, ".") or String.ends_with?(&1, "-wal")))
    # A missing cache dir is [] (no follower has run), not a crash.
    Application.put_env(:fathom, :warm_cache_dir, Path.join(System.tmp_dir!(), "nope_#{uniq()}"))
    assert WarmFollower.cached_shard_ids() == []
  end

  test "records an etag sidecar per cached shard so a failover can validate freshness" do
    a = seed_shard("warm_etag_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    assert a in refresh(pid)

    # The etag the coordinator presents to Storage.pull_if_changed/3 before promoting.
    assert is_binary(WarmFollower.cached_etag(a))
    assert File.exists?(WarmFollower.cache_path(a) <> ".etag")

    # A revalidation cycle with an unchanged object keeps the same etag (a 304, no churn).
    etag = WarmFollower.cached_etag(a)
    assert a in refresh(pid)
    assert WarmFollower.cached_etag(a) == etag
  end

  # Review 2026-07-23 #15: revalidation used to be one conditional GET per cached shard
  # per poll — request-bound at exactly the scale warm capacity should be disk-bound. A
  # cached shard whose directory last_flushed_at hasn't advanced past its last validation
  # is now skipped without touching storage. Proven behaviorally with FaultyStorage: with
  # a :pull fault injected, a SKIPPED shard stays cached (storage never consulted), while
  # pre-fix the revalidation hit the fault and dropped it from the cache. A flush-signal
  # advance re-checks; shards with no signal (NULL last_flushed_at) revalidate every poll
  # exactly as before (every other test in this file runs that way).
  test "an unflushed-since-validation shard skips the storage round-trip (#15)" do
    # Keep the id out of the rolling force-check slice for the first few cycles (the
    # follower's init refresh is cycle 1; our explicit refreshes are 2..4).
    id =
      Enum.find(Stream.map(1..1000, &"warm_skip_#{uniq()}_#{&1}"), fn cand ->
        :erlang.phash2(cand, 10) not in [1, 2, 3, 4]
      end)

    seed_shard(id)
    assert Directory.record_flush_batch([{id, DateTime.utc_now()}]) >= 1

    # There are TWO independent reasons to skip a cycle, and this test owns the first one
    # (the flush signal). Disable the second — the #26 body re-pull cooldown — so cycle 4
    # genuinely exercises "an advanced signal forces a re-check". Left at its default the
    # cooldown would skip cycle 4 too, and this test would pass for the wrong reason.
    # The cooldown's own behaviour is pinned by the #26 tests below.
    put_env!(:warm_min_repull_ms, 0)

    prev_storage = Application.get_env(:fathom, :shard_storage)
    on_exit(fn -> restore_env(:shard_storage, prev_storage) end)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    pid = start_supervised!(WarmFollower)
    # Cycle 2: normal pull — cached, and the flush stamp is recorded as the baseline.
    assert id in refresh(pid)

    # Cycle 3: storage now FAILS every pull. The flush signal hasn't advanced, so the
    # follower must skip the round-trip entirely — the shard stays cached. Pre-fix this
    # revalidated, hit the fault, and dropped the shard.
    Application.put_env(:fathom, :storage_fault, :pull)
    on_exit(fn -> Application.delete_env(:fathom, :storage_fault) end)

    assert id in refresh(pid),
           "an unflushed-since-validation shard must not touch storage on the poll (#15)"

    # Cycle 4: the owner flushed (signal advances) — the follower must re-check, hit the
    # (still-faulted) storage, and drop the shard: the skip never masks a real change.
    assert Directory.record_flush_batch([{id, DateTime.add(DateTime.utc_now(), 5)}]) >= 1
    refute id in refresh(pid)
  end

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, val), do: Application.put_env(:fathom, key, val)

  # An id whose rolling force-check slice never lands on the cycles a test drives, so the
  # sweep belt can't be mistaken for (or mask) the behaviour under test.
  defp quiet_id(prefix, cycles) do
    Enum.find(Stream.map(1..1000, &"#{prefix}_#{uniq()}_#{&1}"), fn cand ->
      :erlang.phash2(cand, 10) not in cycles
    end)
  end

  defp put_env!(key, value) do
    prev = Application.get_env(:fathom, key)
    on_exit(fn -> restore_env(key, prev) end)
    Application.put_env(:fathom, key, value)
  end

  # Make every storage pull fail, so "did the follower touch storage this cycle?" is
  # observable: a shard that was re-checked gets dropped, one that was skipped stays cached.
  defp fault_pulls! do
    put_env!(:shard_storage, Fathom.Test.FaultyStorage)
    put_env!(:storage_fault, :pull)
  end

  # Review #26. #15 made the GET count track flushes-since-last-poll, which leaves the
  # write-hot case unbounded: a tenant flushing faster than the poll advances its signal every
  # cycle, so every refresh is a 200 with a full body plus an fsync — forever, to save one body
  # transfer at a failover that may never happen. The cooldown is the bound.
  test "a write-hot shard's body is not re-pulled inside the cooldown (#26)" do
    id = quiet_id("warm_cooldown", [1, 2, 3, 4])
    seed_shard(id)
    assert Directory.record_flush_batch([{id, DateTime.utc_now()}]) >= 1

    pid = start_supervised!(WarmFollower)
    # Cycle 2: first pull — transfers a body, which starts this shard's cooldown.
    assert id in refresh(pid)

    # The owner flushes again, so the #15 skip no longer applies: the signal HAS advanced.
    assert Directory.record_flush_batch([{id, DateTime.add(DateTime.utc_now(), 5)}]) >= 1
    fault_pulls!()

    # Cycle 3: pre-fix this re-pulled the whole body every single poll. It must now skip,
    # because the body moved a moment ago — a staler cache costs failover RTO on this shard,
    # never correctness, since promotion revalidates before serving.
    assert id in refresh(pid),
           "a shard whose body transferred inside :warm_min_repull_ms must not re-pull (#26)"
  end

  test "the cooldown expires — a flushed shard is re-checked once it lapses (#26)" do
    id = quiet_id("warm_cdexpire", [1, 2, 3, 4])
    seed_shard(id)
    assert Directory.record_flush_batch([{id, DateTime.utc_now()}]) >= 1

    pid = start_supervised!(WarmFollower)
    assert id in refresh(pid)

    # A zero cooldown is "no floor at all" — the skip must be the cooldown doing its job, not
    # the follower having quietly stopped revalidating write-hot shards.
    put_env!(:warm_min_repull_ms, 0)
    assert Directory.record_flush_batch([{id, DateTime.add(DateTime.utc_now(), 5)}]) >= 1
    fault_pulls!()

    refute id in refresh(pid),
           "past the cooldown a flush-advanced shard must be re-checked, not skipped forever"
  end

  test "the refresh byte budget defers work rather than starving the cache (#26)" do
    cycles = [1, 2, 3, 4]
    a = quiet_id("warm_budget_a", cycles)
    b = quiet_id("warm_budget_b", cycles)
    for id <- [a, b], do: seed_shard(id)

    stamp = DateTime.utc_now()
    assert Directory.record_flush_batch([{a, stamp}, {b, stamp}]) >= 2

    # Isolate the budget from the cooldown — this test is about bytes, not frequency.
    put_env!(:warm_min_repull_ms, 0)

    pid = start_supervised!(WarmFollower)
    cached = refresh(pid)
    assert a in cached and b in cached

    # Both owners flush, so both are due a re-check; storage then fails every pull.
    later = DateTime.add(stamp, 5)
    assert Directory.record_flush_batch([{a, later}, {b, later}]) >= 2
    fault_pulls!()

    # One byte per second cannot fund two whole-object bodies in one poll, so at least one
    # shard must be deferred — and a deferred shard stays cached (it is on disk; we simply
    # didn't spend this cycle's bytes revalidating it) rather than being dropped.
    put_env!(:warm_refresh_bytes_per_s, 1)

    survivors = refresh(pid)

    # Count only OUR two shards: the follower's cached set also holds whatever else the test
    # DB's active set contains, so a bare `length(survivors) >= 1` would pass for the wrong
    # reason no matter what the budget did.
    assert Enum.count([a, b], &(&1 in survivors)) >= 1,
           "a byte budget must DEFER refreshes, not evict the shards it cannot afford"
  end

  test "eviction drops the etag sidecar with the cached file" do
    a = seed_shard("warm_etagevict_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    assert a in refresh(pid)
    assert File.exists?(WarmFollower.cache_path(a) <> ".etag")

    {:ok, _} = Directory.retire(a, DateTime.add(DateTime.utc_now(), 3600))
    refute a in refresh(pid)

    refute WarmFollower.cached?(a)

    refute File.exists?(WarmFollower.cache_path(a) <> ".etag"),
           "eviction must drop the sidecar, not leave a dangling etag"

    assert WarmFollower.cached_etag(a) == nil
  end

  test "skips shards this node already owns" do
    a = seed_shard("warm_owned_#{uniq()}")
    b = seed_shard("warm_other_#{uniq()}")

    # Own `a`: a process registered under it in the shard registry is exactly what
    # `owned_shards` sees for a live coordinator — no full coordinator (lease/pull/
    # timers), so the test stays fully contained.
    test = self()

    owner =
      spawn(fn ->
        {:ok, _} = Registry.register(Fathom.ShardRegistry, a, nil)
        send(test, :owned)
        receive do: (:stop -> :ok)
      end)

    assert_receive :owned

    follower = start_supervised!(WarmFollower)
    cached = refresh(follower)

    refute a in cached, "must not warm a shard this node owns"
    assert b in cached
    refute WarmFollower.cached?(a)

    send(owner, :stop)
  end

  # Register `a`, then let it be unregistered on command (an idle-drop: the coordinator
  # stops, releasing the registry entry AND the lease). Returns the owner pid; drive it
  # with :unregister / :stop, each acked so the registry mutation is observed before the
  # next refresh.
  defp own_shard(id) do
    test = self()

    pid =
      spawn(fn ->
        {:ok, _} = Registry.register(Fathom.ShardRegistry, id, nil)
        send(test, {:owned, id})

        receive do
          :unregister ->
            Registry.unregister(Fathom.ShardRegistry, id)
            send(test, {:unregistered, id})
            receive do: (:stop -> :ok)
        end
      end)

    assert_receive {:owned, ^id}
    pid
  end

  test "does not re-warm a shard it recently owned (an idle-dropped home shard)" do
    a = seed_shard("warm_recent_#{uniq()}")
    b = seed_shard("warm_recent_other_#{uniq()}")
    owner = own_shard(a)

    follower = start_supervised!(WarmFollower)
    refute a in refresh(follower), "excluded while a live coordinator holds it"

    # Idle-drop: coordinator gone, lease released — nothing on-disk/in-S3 marks this
    # node as `a`'s home anymore, but it IS still the LB home, so `a` will route back
    # here. The follower must remember it owned `a` and not re-warm it.
    send(owner, :unregister)
    assert_receive {:unregistered, ^a}

    cached = refresh(follower)
    refute a in cached, "must not re-warm a shard this node recently owned/dropped"
    refute WarmFollower.cached?(a)
    assert b in cached, "a shard this node never owned is still warmed"

    send(owner, :stop)
  end

  test "re-warms a recently-owned shard once the home-retention window lapses (LB remap)" do
    a = seed_shard("warm_lapse_#{uniq()}")
    owner = own_shard(a)

    follower = start_supervised!(WarmFollower)
    refute a in refresh(follower)

    send(owner, :unregister)
    assert_receive {:unregistered, ^a}
    refute a in refresh(follower), "still home within the retention window"

    # Age the last-owned stamp past the window (deterministic, no sleep) — the proxy for
    # `a`'s LB home having genuinely moved to another node, making this node a failover
    # target that SHOULD warm it.
    :sys.replace_state(follower, fn s ->
      old = System.monotonic_time(:millisecond) - 10 * s.home_retention_ms
      %{s | recent_owned: Map.put(s.recent_owned, a, old)}
    end)

    cached = refresh(follower)
    assert a in cached, "after the home window lapses, the shard is a warmable failover target"
    assert WarmFollower.cached?(a)

    send(owner, :stop)
  end

  test "evicts shards that leave the active set" do
    a = seed_shard("warm_evict_#{uniq()}")
    b = seed_shard("warm_keep_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    assert a in refresh(pid)
    assert WarmFollower.cached?(a)

    # `a` is retired ⇒ no longer active ⇒ drops out of the target ⇒ evicted next refresh.
    {:ok, _} = Directory.retire(a, DateTime.add(DateTime.utc_now(), 3600))
    cached = refresh(pid)

    refute a in cached
    refute WarmFollower.cached?(a), "a shard that left the active set is evicted from the cache"
    assert b in cached
  end

  test "skips a directory entry that has no S3 object yet" do
    # Active in the directory but never flushed — nothing to warm.
    d = seed_shard("warm_noobj_#{uniq()}", object: false)

    pid = start_supervised!(WarmFollower)
    cached = refresh(pid)

    refute d in cached
    refute WarmFollower.cached?(d)
  end

  # Finding #23: pull_all used Task.async_stream's default on_timeout: :exit, so one pull
  # past the timeout (a congested >timeout S3 fetch — exactly the failover moment) exited the
  # follower mid-cycle. With on_timeout: :kill_task the slow pull is killed and the shard
  # skipped, and the follower survives. Drive it with a pull slower than a short timeout.
  test "a pull past the timeout is killed and skipped, not fatal to the follower (finding #23)" do
    a = seed_shard("warm_slow_#{uniq()}")

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :storage_pull_delay_ms, 400)
    Application.put_env(:fathom, :warm_pull_timeout_ms, 40)

    on_exit(fn ->
      Application.delete_env(:fathom, :shard_storage)
      Application.delete_env(:fathom, :storage_pull_delay_ms)
      Application.delete_env(:fathom, :warm_pull_timeout_ms)
    end)

    pid = start_supervised!(WarmFollower)
    ref = Process.monitor(pid)

    # refresh/1 returning at all is the proof: with on_timeout: :exit this GenServer.call would
    # exit when the pull timed out. The slow shard must not be recorded as warm.
    cached = refresh(pid)

    refute a in cached
    assert Process.alive?(pid)
    refute_received {:DOWN, ^ref, :process, ^pid, _}
  end
end
