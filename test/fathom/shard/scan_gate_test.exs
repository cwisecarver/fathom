defmodule Fathom.Shard.ScanGateTest do
  # Expert review 2026-10-20 #19: FlushGate's default cap can equal the dirty-IO thread count, so a
  # flush wave could hold every dirty-IO thread with quick_check + VACUUM INTO scans. ScanGate is
  # the separate scan-only permit (SDio-2, min 1). Invariants: never more than `cap` holders;
  # a release/death hands the permit to a waiter; a timed-out waiter leaks nothing; off = no-op.
  use ExUnit.Case, async: false

  alias Fathom.Shard.ScanGate

  setup do
    prev = Application.get_env(:fathom, :shard_flush_scan_permits, :unset)
    Application.put_env(:fathom, :shard_flush_scan_permits, 2)

    on_exit(fn ->
      if prev == :unset,
        do: Application.delete_env(:fathom, :shard_flush_scan_permits),
        else: Application.put_env(:fathom, :shard_flush_scan_permits, prev)
    end)

    gate = start_supervised!({ScanGate, name: :scan_gate_under_test})
    %{gate: gate}
  end

  defp hold_in_task(gate) do
    parent = self()

    pid =
      spawn(fn ->
        send(parent, {:got, self(), ScanGate.acquire(1_000, gate)})

        receive do
          :release -> ScanGate.release(gate)
        end

        receive do
          :exit -> :ok
        end
      end)

    assert_receive {:got, ^pid, :ok}
    pid
  end

  test "default cap leaves two dirty-IO threads free and never drops below 1" do
    assert ScanGate.default_cap(10) == 8
    assert ScanGate.default_cap(3) == 1
    assert ScanGate.default_cap(1) == 1
    assert ScanGate.default_cap(2) == 1
  end

  test "cap/0 honours the config and 0/false turn the gate off" do
    assert ScanGate.cap() == 2
    Application.put_env(:fathom, :shard_flush_scan_permits, 0)
    assert ScanGate.cap() == nil
    Application.delete_env(:fathom, :shard_flush_scan_permits)
    assert ScanGate.cap() == ScanGate.default_cap(:erlang.system_info(:dirty_io_schedulers))
  end

  test "at most `cap` holders; a release hands the permit to the waiter", %{gate: gate} do
    a = hold_in_task(gate)
    _b = hold_in_task(gate)
    assert %{held: 2, waiting: 0} = ScanGate.stats(gate)

    # A third caller waits, and times out without holding anything.
    assert ScanGate.acquire(50, gate) == :timeout
    assert %{held: 2, waiting: 0} = ScanGate.stats(gate)

    waiter = Task.async(fn -> ScanGate.acquire(5_000, gate) end)
    # Sync: the waiter's call is queued once stats shows it waiting.
    wait_until(fn -> ScanGate.stats(gate).waiting == 1 end)

    send(a, :release)
    assert Task.await(waiter) == :ok
    # The waiter task has exited, so its permit is freed by its monitor: only `b` still holds.
    wait_until(fn -> ScanGate.stats(gate).held == 1 end)
    assert %{waiting: 0} = ScanGate.stats(gate)
  end

  test "a holder that dies frees its permit without releasing", %{gate: gate} do
    a = hold_in_task(gate)
    b = hold_in_task(gate)
    ref = Process.monitor(a)
    Process.exit(a, :kill)
    assert_receive {:DOWN, ^ref, :process, ^a, :killed}
    wait_until(fn -> ScanGate.stats(gate).held == 1 end)
    _ = b
  end

  test "with_permit/3 releases on raise and runs fun without a permit on timeout", %{gate: gate} do
    assert_raise RuntimeError, fn ->
      ScanGate.with_permit(1_000, fn -> raise "boom" end, gate)
    end

    assert %{held: 0} = ScanGate.stats(gate)

    _a = hold_in_task(gate)
    _b = hold_in_task(gate)
    assert ScanGate.with_permit(30, fn -> :ran_unpermitted end, gate) == :ran_unpermitted
    assert %{held: 2, waiting: 0} = ScanGate.stats(gate)
  end

  test "gate disabled or not running is a no-op that fails open" do
    Application.put_env(:fathom, :shard_flush_scan_permits, 0)
    assert ScanGate.acquire(10, :scan_gate_under_test) == :ok
    Application.put_env(:fathom, :shard_flush_scan_permits, 1)
    assert ScanGate.acquire(10, :scan_gate_not_started) == :ok
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held")

      true ->
        # A bare timed receive: yields without consuming any mailbox message.
        receive do
        after
          5 -> wait_until(fun, tries - 1)
        end
    end
  end
end
