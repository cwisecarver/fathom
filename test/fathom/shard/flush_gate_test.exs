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
end
