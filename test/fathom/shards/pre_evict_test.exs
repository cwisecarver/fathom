defmodule Fathom.Shards.PreEvictTest do
  @moduledoc """
  The low-watermark pre-evictor (perf review 2026-10-21 #14, user decision: headroom 1% of the cap,
  minimum 16).

  Symptom it fixes: at `:max_open_shards`, every admitted cold open first evicted a victim INLINE —
  waiting on the victim's whole terminate, including the S3 DELETE of its lock — so a node sized to
  its cap paid an extra storage round trip on every reopen. Now an admission that leaves the node
  within `headroom` of the cap starts one background eviction that brings it back below
  `cap - headroom`, so the next admissions find free slots.

  Pinned invariants (each against the pre-fix code, the first fails: nothing pre-evicts):
    * an admission near the cap drops LRU idle shards in the background, down below the watermark,
      and no admission had to evict inline;
    * a shard holding a connection is never pre-evicted;
    * a DIRTY idle victim is flushed before it drops — its writes survive the reopen;
    * a cap too small to hold the headroom twice over, or `:pre_evict_headroom` 0, disables it.

  Both liveness modes (eviction is a drop), mode asserted.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shards.Lru
  alias Filo.Stmt

  @moduletag :capture_log
  @modes [:legacy, :heartbeat]

  @env [
    :max_open_shards,
    :pre_evict_headroom,
    :pre_evict_min_idle_ms,
    :shard_idle_ms,
    :evict_idle_at_capacity
  ]

  setup do
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    Lru.reset()
    test_pid = self()
    handler = "pre_evict_test_#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:fathom, :shards, :evicted], [:fathom, :shards, :pre_evicted]],
      fn event, _, %{shard_id: id}, _ -> send(test_pid, {event, id}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Lru.reset()
    end)

    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    :ok
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(id, mode) do
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)
    gen = :sys.get_state(pid).acquire_gen

    if mode == :heartbeat,
      do: assert(is_integer(gen), "heartbeat mode did not take"),
      else: assert(is_nil(gen), "legacy mode did not take")
  end

  defp uniq, do: "preev_#{System.unique_integer([:positive])}"
  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # Open, write one row (so the shard is DIRTY: written, not yet flushed), release the stream.
  defp open_dirty(id) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [id]))
    :ok = ShardExecutor.close(conn)
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)
    _ = :sys.get_state(pid)
    id
  end

  defp open_held(id) do
    {:ok, pid, _ref, _path} = Shards.checkout(id)
    pid
  end

  # The evictor is single-flight under a registered name: wait until no run is in flight.
  defp await_pre_evictor do
    case Process.whereis(Fathom.Shards.PreEvictor) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 30_000
        await_pre_evictor()
    end
  end

  defp open?(id), do: Registry.lookup(Fathom.ShardRegistry, id) != []

  defp wait(ms) do
    receive do
    after
      ms -> :ok
    end
  end

  defp cleanup(ids) do
    Application.put_env(:fathom, :max_open_shards, :infinity)
    Enum.each(ids, &Shards.stop/1)

    for id <- ids,
        f <- Path.wildcard(Fathom.Shard.db_path(id) <> "*"),
        do: File.rm_rf(f)

    for id <- ids, do: Fathom.Shard.Storage.Local.purge_shard(id)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "near the cap, idle shards drop in the background; busy never, dirty flushed first" do
        # Cap 40 over the live population: headroom = 16 (the minimum), watermark = base + 24.
        base = Registry.count(Fathom.ShardRegistry)
        cap = base + 40
        assert Shards.pre_evict_headroom(40) == 16
        Application.put_env(:fathom, :max_open_shards, cap)
        low = cap - 16

        # The coldest shard is HELD: a live connection, never evictable.
        held_id = uniq()
        held = open_held(held_id)
        held_ref = Process.monitor(held)
        assert_mode!(held_id, unquote(mode))

        # 30 dirty idle shards: enough to cross the watermark (base + 1 + 30 > base + 24) while
        # staying under the cap, so the inline path is never needed.
        #
        # The evictor skips shards used within `:pre_evict_min_idle_ms` (expert review 2026-10-21
        # rv6 F10), and these 30 were all opened within milliseconds. So they age past a short
        # window first, and one more admission kicks the evictor.
        Application.put_env(:fathom, :pre_evict_min_idle_ms, 200)
        ids = for _ <- 1..30, do: open_dirty(uniq())
        await_pre_evictor()
        wait(400)
        kick = open_dirty(uniq())
        ids = ids ++ [kick]
        await_pre_evictor()

        assert Registry.count(Fathom.ShardRegistry) < low,
               "the node was left within the headroom of the cap: nothing pre-evicted"

        evicted = Enum.reject(ids, &open?/1)
        assert evicted != [], "fixture: no idle shard was evicted"
        assert_received {[:fathom, :shards, :pre_evicted], _}
        refute_received {[:fathom, :shards, :evicted], _}, "an admission evicted inline"

        refute_received {:DOWN, ^held_ref, :process, ^held, _},
                        "a shard holding a connection was pre-evicted"

        # Dirty victims flushed before dropping: each reopens with its row.
        Application.put_env(:fathom, :max_open_shards, :infinity)

        for id <- evicted do
          {:ok, conn} = ShardExecutor.open(id)
          assert {:ok, %{rows: [[^id]]}} = ShardExecutor.execute(conn, stmt("SELECT a FROM t"))
          :ok = ShardExecutor.close(conn)
        end

        cleanup([held_id | ids])
      end
    end
  end

  # EVICT-BEFORE-CHECKOUT CHURN (expert review 2026-10-21 rv6 F10). The evictor took the LRU-coldest
  # idle shards whatever their age, so in a burst of fresh opens it dropped shards a client had used
  # a moment ago, and their next checkout paid a cold reopen. A shard used within
  # `:pre_evict_min_idle_ms` (default 1 s) is skipped.
  test "shards used within the last second are not pre-evicted" do
    base = Registry.count(Fathom.ShardRegistry)
    Application.put_env(:fathom, :max_open_shards, base + 40)
    ids = for _ <- 1..30, do: open_dirty(uniq())
    await_pre_evictor()

    assert Enum.all?(ids, &open?/1), "a shard used a moment ago was pre-evicted"
    refute_received {[:fathom, :shards, :pre_evicted], _}
    cleanup(ids)
  end

  # ONE FREED SLOT, COUNTED ONCE (expert review 2026-10-21 rv6 F10). The inline admission path and the
  # background evictor can pick the same victim; both counted its slot, and an admission could land
  # one shard over the soft cap. A victim already gone, or being dropped by another probe, must not
  # read as freed.
  describe "evict/2 counts only the slot it frees" do
    test "a victim that is already gone is not a freed slot" do
      assert {:error, :gone} = Shards.evict(uniq(), 1_000)
    end

    test "a victim another probe holds the eviction claim on is left alone" do
      Application.put_env(:fathom, :max_open_shards, :infinity)
      id = open_dirty(uniq())
      assert Lru.claim_eviction(id)

      assert {:error, :claimed} = Shards.evict(id, 1_000)
      assert open?(id), "the claimed victim was evicted twice"

      # Asserted on the coordinator's own DOWN, not `open?/1`: `evict/2` answers on that DOWN, and
      # `Registry` drops the entry later, in its partition process. Under full-suite load the
      # lookup still saw it (seeds 316529 and 181822 failed `refute open?(id)` here).
      [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)
      ref = Process.monitor(pid)
      :ok = Lru.release_eviction(id)
      assert :ok = Shards.evict(id, 5_000)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}
      cleanup([id])
    end

    test "a claim held by a dead process is taken over" do
      id = uniq()
      test_pid = self()

      {pid, ref} =
        spawn_monitor(fn ->
          send(test_pid, {:claimed, Lru.claim_eviction(id)})
        end)

      assert_receive {:claimed, true}
      assert_receive {:DOWN, ^ref, :process, ^pid, _}
      assert Lru.claim_eviction(id)
      :ok = Lru.release_eviction(id)
    end
  end

  describe "headroom" do
    test "1% of the cap, at least 16; off for a cap that cannot hold it twice; 0 disables" do
      assert Shards.pre_evict_headroom(10_000) == 100
      assert Shards.pre_evict_headroom(1_000) == 16
      assert Shards.pre_evict_headroom(33) == 16
      assert Shards.pre_evict_headroom(32) == 0
      assert Shards.pre_evict_headroom(1) == 0

      Application.put_env(:fathom, :pre_evict_headroom, 0)
      assert Shards.pre_evict_headroom(10_000) == 0
      Application.put_env(:fathom, :pre_evict_headroom, 50)
      assert Shards.pre_evict_headroom(10_000) == 50
    end

    test "with :pre_evict_headroom 0, nothing is evicted before the cap" do
      Application.put_env(:fathom, :pre_evict_headroom, 0)
      base = Registry.count(Fathom.ShardRegistry)
      Application.put_env(:fathom, :max_open_shards, base + 40)
      ids = for _ <- 1..30, do: open_dirty(uniq())
      await_pre_evictor()
      assert Enum.all?(ids, &open?/1), "pre-eviction ran with the headroom set to 0"
      refute_received {[:fathom, :shards, :pre_evicted], _}
      cleanup(ids)
    end
  end
end
