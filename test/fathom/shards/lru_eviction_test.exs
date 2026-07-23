defmodule Fathom.Shards.LruEvictionTest do
  # Idle-eviction at capacity: at :max_open_shards, a new open evicts the
  # least-recently-used IDLE shard (flush + drop + release lease) to make room, rather
  # than refusing with a 503 — because a shard's LB home is one node, so a refused open
  # means that tenant is down, while an idle shard is bottomless-backed and just cold-
  # re-opens if touched again. A BUSY shard (checked-out connections) is never evicted.
  #
  # async: false — shares the global Fathom.ShardRegistry + Fathom.Shards.Lru table, so
  # no other test may open shards concurrently. Each test resets the Lru table so only
  # shards it opens are eviction candidates (a coordinator left alive by a prior test is
  # in the Registry but not this test's reset Lru, so it's never the one evicted).
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shards.Lru
  alias Fathom.Test.FaultyStorage
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    prev_cap = Application.get_env(:fathom, :max_open_shards)
    prev_evict = Application.get_env(:fathom, :evict_idle_at_capacity)
    Lru.reset()

    on_exit(fn ->
      restore(:max_open_shards, prev_cap)
      restore(:evict_idle_at_capacity, prev_evict)
      Lru.reset()

      for dir <- [@local_dir, @remote_dir],
          path <- Path.wildcard(Path.join(dir, "lru_*")),
          do: File.rm(path)
    end)

    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp uniq, do: "lru_#{System.unique_integer([:positive])}"

  # Open a shard, release its connection, and confirm it's idle (0 conns). Returns the
  # coordinator pid. Touched into the Lru table at checkout.
  defp open_idle(id) do
    {:ok, pid, ref, _path} = Shards.checkout(id)
    Fathom.Shard.checkin(pid, ref)
    _ = :sys.get_state(pid)
    pid
  end

  # Open + HOLD a shard (the test process stays its connection, so it never idle-stops
  # and is never evictable). Returns the coordinator pid.
  defp open_held(id) do
    {:ok, pid, _ref, _path} = Shards.checkout(id)
    pid
  end

  test "at capacity, a new open evicts an idle shard instead of refusing" do
    idle = open_idle(uniq())
    _busy = open_held(uniq())
    Application.put_env(:fathom, :max_open_shards, 1)

    ref = Process.monitor(idle)
    # Pre-fix this was {:error, :node_at_capacity}; now the idle shard is evicted to admit.
    assert {:ok, _pid, _r, _p} = Shards.checkout(uniq())
    assert_receive {:DOWN, ^ref, :process, ^idle, _}, 2_000
  end

  test "eviction picks the LEAST-recently-used idle shard, sparing the warmer one" do
    older = open_idle(uniq())
    newer = open_idle(uniq())
    _busy = open_held(uniq())
    Application.put_env(:fathom, :max_open_shards, 1)

    older_ref = Process.monitor(older)
    newer_ref = Process.monitor(newer)

    assert {:ok, _pid, _r, _p} = Shards.checkout(uniq())

    assert_receive {:DOWN, ^older_ref, :process, ^older, _}, 2_000
    refute_received {:DOWN, ^newer_ref, :process, ^newer, _}
    assert Process.alive?(newer)
  end

  test "a busy shard is never evicted — all-busy at capacity still refuses" do
    busy = open_held(uniq())
    Application.put_env(:fathom, :max_open_shards, 1)

    assert {:error, :node_at_capacity} = Shards.checkout(uniq())
    assert Process.alive?(busy), "a shard with a live connection must never be evicted"
  end

  # Expert review #14: the coordinator must publish its checked-out-connection count so the eviction
  # probe (via lru_order) can skip a busy shard. Without it, a long-lived held stream ages to the LRU
  # front but can't be evicted, starving admission (a soft cap degrading to a hard cap). This pins the
  # wiring: a held shard reads busy, a released one reads not-busy.
  test "the coordinator publishes its busy count for eviction (#14)" do
    # A finite cap makes Lru tracking active (enabled?), but large enough that nothing is evicted.
    Application.put_env(:fathom, :max_open_shards, 100)
    id = uniq()

    {:ok, pid, ref, _path} = Shards.checkout(id)
    _ = :sys.get_state(pid)
    assert Lru.busy?(id), "a held shard must publish as busy"

    Fathom.Shard.checkin(pid, ref)
    _ = :sys.get_state(pid)
    refute Lru.busy?(id), "a released shard must publish as not-busy"
  end

  test "with :evict_idle_at_capacity false (hard cap), an idle shard is not evicted" do
    Application.put_env(:fathom, :evict_idle_at_capacity, false)
    idle = open_idle(uniq())
    _busy = open_held(uniq())
    Application.put_env(:fathom, :max_open_shards, 1)

    assert {:error, :node_at_capacity} = Shards.checkout(uniq())
    assert Process.alive?(idle), "eviction is disabled, so the idle shard must survive"
  end

  describe "Lru table" do
    test "forget removes a row; lru_order is bounded by the limit" do
      Application.put_env(:fathom, :max_open_shards, 100)
      Lru.reset()
      for i <- 1..5, do: Lru.touch("t#{i}")

      assert length(Lru.lru_order(3)) == 3
      assert length(Lru.lru_order(100)) == 5

      Lru.forget("t3")
      refute "t3" in Lru.lru_order(100)
      assert length(Lru.lru_order(100)) == 4
    end

    # Review 2026-07-23 #14: lru_order was tab2list |> sort — O(N log N) + a full-table copy
    # into the admitting stream's heap on EVERY at-capacity admission. The ordered-walk
    # replacement requires touch to REPLACE its previous order key, or the order table would
    # grow per checkout instead of per shard. Pre-fix (insert-only order keys) the size
    # assertion fails at 53.
    test "repeated touches keep one order key per shard; walk order is stamp order (#14)" do
      Application.put_env(:fathom, :max_open_shards, 100)
      Lru.reset()

      for _ <- 1..50, do: Lru.touch("hot")
      for i <- 1..3, do: Lru.touch("s#{i}")

      # "hot" was touched last before s1..s3, so it is the coldest; s1..s3 follow in order.
      assert Lru.lru_order(10) == ["hot", "s1", "s2", "s3"]
      assert :ets.info(Fathom.Shards.Lru.Order, :size) == 4
    end

    test "touch is a no-op when eviction is unreachable (no finite cap)" do
      Application.put_env(:fathom, :max_open_shards, :infinity)
      Lru.reset()
      Lru.touch("ghost")
      assert Lru.lru_order(10) == []
      refute Lru.enabled?()
    end

    # Expert review #14: the mixed busy-front + idle-tail case the old suite missed. lru_order must
    # filter BUSY shards BEFORE the limit cut, so a bounded probe never wastes its slots on the busy
    # LRU front and 503s while an evictable idle shard sits just past the probe window.
    test "lru_order returns only IDLE candidates — a busy front never hides the idle tail (#14)" do
      Application.put_env(:fathom, :max_open_shards, 100)
      Lru.reset()

      # 20 BUSY shards are the coldest (touched first); one IDLE shard is the warmest.
      for i <- 1..20 do
        id = "busy#{i}"
        Lru.touch(id)
        Lru.record_conns(id, 1)
      end

      Lru.touch("idle")
      Lru.record_conns("idle", 0)

      # Pre-#14, lru_order(2) returned the 2 coldest (both busy) and the idle was hidden past the
      # window. Now the busy front is filtered out, so even a tight probe reaches the idle tail.
      assert Lru.lru_order(2) == ["idle"]
      assert Lru.lru_order(100) == ["idle"]
    end
  end

  # Expert review 2026-07-14 #7 (Reliability): at capacity, admission runs in the checkout
  # (Filo stream) process and probed up to @max_evict_probes (16) LRU candidates, calling
  # drain(id, 0) on each — a BLOCKING receive whose only bound was drain_timeout + 30_000.
  # An evicted coordinator's terminate runs a synchronous flush-to-S3 with no supervisor
  # timeout, so under slow/hung S3 a single new-tenant checkout could block 16 × 30s ≈ 8 min
  # before finally 503ing — coupling new-tenant admission latency to OTHER tenants' flush
  # latency. The fix bounds the admission-path WAIT (`:evict_budget_ms`), NOT the flush: the
  # evicted coordinator still flushes/drops/releases in full in the background; admission
  # just stops blocking on it and 503s fast (the LB retries).
  describe "eviction is bounded by :evict_budget_ms under a slow-flushing evictable shard" do
    setup do
      prev_storage = Application.get_env(:fathom, :shard_storage)
      prev_delay = Application.get_env(:fathom, :storage_flush_delay_ms)
      prev_budget = Application.get_env(:fathom, :evict_budget_ms)
      Application.put_env(:fathom, :shard_storage, FaultyStorage)

      on_exit(fn ->
        restore(:shard_storage, prev_storage)
        restore(:storage_flush_delay_ms, prev_delay)
        restore(:evict_budget_ms, prev_budget)
      end)

      :ok
    end

    test "admission 503s within the budget instead of blocking on the slow flush" do
      # The evictable idle shard's terminate flush takes 3s (a slow/hung S3); the admission
      # budget is 400ms. Pre-fix the checkout blocked ~3s (the full flush) — up to 16 × 30s
      # in the worst case; post-fix it must refuse within a few × the 400ms budget.
      Application.put_env(:fathom, :storage_flush_delay_ms, 3_000)
      Application.put_env(:fathom, :evict_budget_ms, 400)

      dirty = open_idle_dirty(uniq())
      ref = Process.monitor(dirty)
      Application.put_env(:fathom, :max_open_shards, 1)

      t0 = System.monotonic_time(:millisecond)
      result = Shards.checkout(uniq())
      elapsed = System.monotonic_time(:millisecond) - t0

      # No slot could be freed within the budget (the flush is still running), so admission
      # refuses cleanly — the LB backs off and retries.
      assert result == {:error, :node_at_capacity}

      # The load-bearing assertion: it returned FAST, not after the ~6s flush (and nowhere
      # near the pre-fix 30s-per-probe safety net).
      assert elapsed < 2_000,
             "admission blocked #{elapsed}ms on another tenant's slow flush (budget was 400ms)"

      # The eviction still completes in the background: the coordinator flushed + dropped +
      # released and stopped — durability/lease semantics untouched, just not blocked on.
      assert_receive {:DOWN, ^ref, :process, ^dirty, _}, 8_000
    end

    test "with a fast flush under the budget, eviction still evicts and admits" do
      # Same path, but the flush is fast (no injected delay): the budget must not break the
      # happy path — the idle shard is evicted and the new tenant is admitted.
      Application.put_env(:fathom, :evict_budget_ms, 2_000)

      dirty = open_idle_dirty(uniq())
      ref = Process.monitor(dirty)
      Application.put_env(:fathom, :max_open_shards, 1)

      assert {:ok, _pid, _r, _p} = Shards.checkout(uniq())
      assert_receive {:DOWN, ^ref, :process, ^dirty, _}, 5_000
    end
  end

  # Open a shard, WRITE to it (so it's dirty and its idle drop must flush), release the
  # connection, and confirm it's idle (0 conns). Returns the coordinator pid. Touched into
  # the Lru table at checkout (ShardExecutor.open → Shards.checkout).
  defp open_idle_dirty(id) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE t (x)", args: []})
    :ok = ShardExecutor.close(conn)
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)
    _ = :sys.get_state(pid)
    pid
  end
end
