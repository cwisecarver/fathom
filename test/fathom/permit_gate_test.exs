defmodule Fathom.PermitGateTest do
  @moduledoc """
  `Fathom.PermitGate` is the one counting-permit server behind `ScanGate`, `BackgroundScanGate` and
  `SeedGate` (cleanliness audit 2026-11-13 #31). The per-gate suites exercise each gate's public API;
  these pin what the SHARED server owns (queueing, monitors, the cap being re-read at grant time) and
  the three different meanings of "unlimited" the gates map onto it, which the audit found had
  drifted apart and the owner chose to PRESERVE.
  """
  use ExUnit.Case, async: false

  alias Fathom.PermitGate
  alias Fathom.Shard.{BackgroundScanGate, ScanGate}
  alias Fathom.Shard.Replication.SeedGate
  alias Fathom.Test.Env

  defp start_gate(cap_fun) do
    name = :"permit_gate_#{System.unique_integer([:positive])}"
    start_supervised!(%{id: name, start: {PermitGate, :start_link, [[name: name, cap: cap_fun]]}})
    name
  end

  defp spawn_holder(gate, cap) do
    parent = self()

    pid =
      spawn(fn ->
        send(parent, {:acquired, self(), PermitGate.acquire(gate, cap, 1_000)})
        receive do: (:stop -> :ok)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  test "a permit past the cap queues, and a release hands it to the oldest waiter" do
    gate = start_gate(fn -> 1 end)
    first = spawn_holder(gate, 1)
    assert_receive {:acquired, ^first, :ok}

    second = spawn_holder(gate, 1)
    # Not granted yet: it is queued behind `first`.
    refute_receive {:acquired, ^second, _}, 100
    assert %{held: 1, waiting: 1} = PermitGate.stats(gate)

    send(first, :stop)
    # `first` exits, its monitor frees the permit, the queue head gets it.
    assert_receive {:acquired, ^second, :ok}, 1_000
    assert %{held: 1, waiting: 0} = PermitGate.stats(gate)
  end

  test "a holder that is killed frees its permit (the monitor, not the holder's cleanup)" do
    gate = start_gate(fn -> 1 end)
    holder = spawn_holder(gate, 1)
    assert_receive {:acquired, ^holder, :ok}
    assert %{held: 1} = PermitGate.stats(gate)

    ref = Process.monitor(holder)
    Process.exit(holder, :kill)
    assert_receive {:DOWN, ^ref, :process, ^holder, :killed}

    # The gate's own :DOWN handler runs asynchronously to ours; a second acquire is the barrier.
    assert :ok = PermitGate.acquire(gate, 1, 1_000)
    assert %{held: 1} = PermitGate.stats(gate)
  end

  test "a waiter that times out is withdrawn: it neither holds a permit nor keeps its queue slot" do
    gate = start_gate(fn -> 1 end)
    holder = spawn_holder(gate, 1)
    assert_receive {:acquired, ^holder, :ok}

    assert :timeout = PermitGate.acquire(gate, 1, 50)
    # The withdraw is a cast; stats is a call, so it is ordered after it.
    assert %{held: 1, waiting: 0} = PermitGate.stats(gate)
  end

  test "the cap is re-read at grant time: raising it to unlimited releases every waiter" do
    {:ok, cap} = Agent.start_link(fn -> 1 end)
    gate = start_gate(fn -> Agent.get(cap, & &1) end)

    holder = spawn_holder(gate, 1)
    assert_receive {:acquired, ^holder, :ok}
    w1 = spawn_holder(gate, 1)
    w2 = spawn_holder(gate, 1)
    refute_receive {:acquired, _, _}, 100
    assert %{held: 1, waiting: 2} = PermitGate.stats(gate)

    Agent.update(cap, fn _ -> nil end)
    # Any release makes the server run grant/1, which now sees "unlimited".
    send(holder, :stop)
    assert_receive {:acquired, ^w1, :ok}, 1_000
    assert_receive {:acquired, ^w2, :ok}, 1_000
  end

  test "a nil cap is unlimited and never calls the server; a missing gate fails open" do
    assert :ok = PermitGate.acquire(:permit_gate_nobody_home, nil, 10)
    assert :ok = PermitGate.acquire(:permit_gate_nobody_home, 3, 10)
  end

  describe "the three gates' own meanings of unlimited are preserved" do
    # ScanGate and BackgroundScanGate: 0 or false disable the gate (cap/0 = nil); any positive
    # integer is the cap. SeedGate: ANYTHING that is not a positive integer (0, nil, a negative
    # number, junk) is unlimited, and its raw cap/0 is the env value, not nil.
    test "ScanGate and BackgroundScanGate: 0/false mean off, a positive integer is the cap" do
      for {gate, key} <- [
            {ScanGate, :shard_flush_scan_permits},
            {BackgroundScanGate, :background_scan_permits}
          ] do
        for off <- [0, false, nil] do
          Env.put!(key, off)
          assert gate.cap() == nil, "#{inspect(gate)} cap with #{inspect(off)}"
        end

        Env.put!(key, 3)
        assert gate.cap() == 3
      end
    end

    test "SeedGate: every non-positive-integer value is unlimited and acquires instantly" do
      name = :"seed_gate_meanings_#{System.unique_integer([:positive])}"
      start_supervised!({SeedGate, name: name})

      for unlimited <- [0, nil, -1, :junk] do
        Env.put!(:replication_seed_max_concurrency, unlimited)

        assert :ok = SeedGate.acquire(10, name)
        assert :ok = SeedGate.acquire(10, name)
        # Unlimited never reaches the server: nothing is ever held.
        assert %{held: 0, waiting: 0} = SeedGate.stats(name)
      end

      Env.put!(:replication_seed_max_concurrency, 1)
      assert :ok = SeedGate.acquire(100, name)
      assert %{held: 1} = SeedGate.stats(name)
    end

    test "SeedGate keeps its own timeout reply, the other two keep :timeout" do
      Env.put!(:replication_seed_max_concurrency, 1)
      seed = :"seed_gate_timeout_#{System.unique_integer([:positive])}"
      start_supervised!({SeedGate, name: seed})

      holder = spawn_holder_via(fn -> SeedGate.acquire(1_000, seed) end)
      assert_receive {:held, ^holder, :ok}
      assert {:error, :seed_gate_timeout} = SeedGate.acquire(50, seed)

      Env.put!(:shard_flush_scan_permits, 1)
      scan = :"scan_gate_timeout_#{System.unique_integer([:positive])}"
      start_supervised!({ScanGate, name: scan})
      holder2 = spawn_holder_via(fn -> ScanGate.acquire(1_000, scan) end)
      assert_receive {:held, ^holder2, :ok}
      assert :timeout = ScanGate.acquire(50, scan)
    end
  end

  defp spawn_holder_via(acquire) do
    parent = self()

    pid =
      spawn(fn ->
        send(parent, {:held, self(), acquire.()})
        receive do: (:stop -> :ok)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end
end
