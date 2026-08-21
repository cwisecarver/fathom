defmodule Fathom.Shard.FlushGateTest do
  # Expert review #17: the node-wide concurrent-flush cap. After a failover/LB flip re-homes a
  # burst of shards, their phase-aligned flush timers would fire N snapshots + PUTs in lockstep;
  # this counter bounds how many run at once. Unit-level semantics here; the coordinator's
  # back-off-when-full behavior is in shard_durability_test.exs. Not async: the counter is global.
  use ExUnit.Case, async: false

  alias Fathom.Shard.FlushGate

  setup do
    prev = Application.get_env(:fathom, :shard_flush_max_concurrency)
    FlushGate.reset()

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :shard_flush_max_concurrency, prev),
        else: Application.delete_env(:fathom, :shard_flush_max_concurrency)

      FlushGate.reset()
    end)

    :ok
  end

  # This test used to assert the OPPOSITE — "unbounded by default" — which is exactly the
  # defect expert review 2026-08-01 #16 found: the gate's whole purpose is bounding the flush
  # storm its own moduledoc describes, and it shipped inert on every deployment that had not
  # explicitly set the key. The default is now derived from the Finch pool it protects.
  test "BOUNDED by default: an unconfigured node still reserves and counts slots" do
    Application.delete_env(:fathom, :shard_flush_max_concurrency)

    assert is_integer(FlushGate.cap()) and FlushGate.cap() > 0
    assert FlushGate.try_acquire() == :ok
    assert FlushGate.in_flight() == 1
    FlushGate.release()
  end

  # The escape hatch the old default provided is still reachable, explicitly.
  test "explicitly unbounded: try_acquire returns :disabled and never counts" do
    Application.put_env(:fathom, :shard_flush_max_concurrency, 0)

    assert FlushGate.cap() == nil
    assert FlushGate.try_acquire() == :disabled
    assert FlushGate.try_acquire() == :disabled
    assert FlushGate.in_flight() == 0, "with no cap the counter is never touched (zero-cost off)"
  end

  test "caps concurrent slots and frees them on release" do
    Application.put_env(:fathom, :shard_flush_max_concurrency, 2)

    assert FlushGate.try_acquire() == :ok
    assert FlushGate.try_acquire() == :ok
    assert FlushGate.in_flight() == 2

    assert FlushGate.try_acquire() == :full, "a third slot over the cap of 2 is refused"
    assert FlushGate.in_flight() == 2, "a refused acquire rolls back its own increment"

    FlushGate.release()
    assert FlushGate.in_flight() == 1
    assert FlushGate.try_acquire() == :ok, "a freed slot admits the next flush"
    assert FlushGate.in_flight() == 2
  end

  test "release clamps at zero so a stray release can't underflow the counter" do
    Application.put_env(:fathom, :shard_flush_max_concurrency, 1)

    FlushGate.release()
    FlushGate.release()
    assert FlushGate.in_flight() == 0

    assert FlushGate.try_acquire() == :ok, "an underflowed counter must not wedge admission"
    assert FlushGate.in_flight() == 1
  end

  # THE LEAK (expert review 2026-08-20 #15). The counter is node-global and outlives any
  # coordinator, but `release/0` was reachable only from coordinator callbacks — so a coordinator
  # brutally killed mid-flush (shutdown-budget expiry, DynamicSupervisor.terminate_child from
  # Shards.stop/1) leaked a slot permanently, as did one that raised between `try_acquire/0` and
  # recording `flush_slot_held:` in its state.
  #
  # The cap is single digits (max(min(pool/4, schedulers), 4) — 4 at the floor), so a handful of
  # leaks makes try_acquire/0 answer :full FOREVER: every dirty shard on the node reschedules at
  # the 250 ms backoff and never flushes again, and the RPO goes unbounded with no signal, because
  # [:fathom, :shard, :flush, :failed] only fires for a flush that actually RAN.
  describe "reclaiming a slot from a dead holder (#15)" do
    setup do
      prev = Application.get_env(:fathom, :shard_flush_max_concurrency)
      Application.put_env(:fathom, :shard_flush_max_concurrency, 2)
      FlushGate.reset()

      on_exit(fn ->
        FlushGate.reset()

        if is_nil(prev),
          do: Application.delete_env(:fathom, :shard_flush_max_concurrency),
          else: Application.put_env(:fathom, :shard_flush_max_concurrency, prev)
      end)

      :ok
    end

    test "a holder that dies without releasing is swept, and the gate reopens" do
      # Two processes take the only two slots and are killed without releasing — exactly what a
      # brutal terminate_child does to a coordinator mid-flush.
      parent = self()

      holders =
        for _ <- 1..2 do
          {:ok, pid} =
            Task.start(fn ->
              :ok = FlushGate.try_acquire()
              send(parent, {:acquired, self()})
              Process.sleep(:infinity)
            end)

          pid
        end

      for pid <- holders, do: assert_receive({:acquired, ^pid}, 2_000)
      assert FlushGate.in_flight() == 2
      assert FlushGate.try_acquire() == :full, "the fixture did not actually fill the gate"

      for pid <- holders do
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000
      end

      # Pre-fix the counter is still 2 and stays there forever — nothing on any path releases a
      # slot held by a process that no longer exists.
      assert FlushGate.sweep() == 2,
             "the gate did not reclaim slots from dead holders; in_flight is stuck at " <>
               "#{FlushGate.in_flight()} and no shard on this node can flush again"

      assert FlushGate.in_flight() == 0
      assert FlushGate.try_acquire() == :ok, "the gate never reopened"
      FlushGate.release()
    end

    test "a LIVE holder is never swept" do
      parent = self()

      {:ok, live} =
        Task.start(fn ->
          :ok = FlushGate.try_acquire()
          send(parent, :acquired)
          Process.sleep(:infinity)
        end)

      assert_receive :acquired, 2_000
      assert FlushGate.in_flight() == 1

      assert FlushGate.sweep() == 0,
             "the sweep reclaimed a slot from a process that is still flushing — the cap is now " <>
               "over-subscribed and the storm it exists to prevent is back"

      assert FlushGate.in_flight() == 1
      Process.exit(live, :kill)
    end
  end
end
