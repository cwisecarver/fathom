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

  alias Fathom.Shards
  alias Fathom.Shards.Lru

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

    test "touch is a no-op when eviction is unreachable (no finite cap)" do
      Application.put_env(:fathom, :max_open_shards, :infinity)
      Lru.reset()
      Lru.touch("ghost")
      assert Lru.lru_order(10) == []
      refute Lru.enabled?()
    end
  end
end
