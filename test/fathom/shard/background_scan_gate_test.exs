defmodule Fathom.Shard.BackgroundScanGateTest do
  # Expert review 2026-10-29 #3: migrations, `verify_migrated`, the restore drill and the tenant
  # export scan whole shards on the same `+SDio` dirty-IO threads tenants use, and only the FLUSH
  # scans were gated (`ScanGate`). `BackgroundScanGate` is the separate node-wide permit for the
  # rest. Invariants: never more than `cap` holders; a release or a holder's death hands the permit
  # on; a timed-out waiter leaks nothing and its fun does NOT run; off or not running = no-op; and
  # the two gates never block each other (the flush `ScanGate` is untouched).
  #
  # Not async: app env, and the flush-gate independence test reads `ScanGate`'s config.
  use ExUnit.Case, async: false

  alias Fathom.Shard.{BackgroundScanGate, ScanGate}

  @gate :background_scan_gate_under_test

  setup do
    prev = Application.get_env(:fathom, :background_scan_permits, :unset)
    Application.put_env(:fathom, :background_scan_permits, 2)

    on_exit(fn ->
      if prev == :unset,
        do: Application.delete_env(:fathom, :background_scan_permits),
        else: Application.put_env(:fathom, :background_scan_permits, prev)
    end)

    gate = start_supervised!({BackgroundScanGate, name: @gate})
    %{gate: gate}
  end

  # A process that takes a permit and keeps it until told to `:release` (graceful) or killed.
  defp hold_in_task(gate) do
    parent = self()

    pid =
      spawn(fn ->
        send(parent, {:got, self(), BackgroundScanGate.acquire(1_000, gate)})

        receive do
          :release -> BackgroundScanGate.release(gate)
        end

        receive do
          :exit -> :ok
        end
      end)

    assert_receive {:got, ^pid, :ok}
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  test "default cap is a quarter of the dirty-IO threads and never drops below 1" do
    assert BackgroundScanGate.default_cap(10) == 2
    assert BackgroundScanGate.default_cap(16) == 4
    assert BackgroundScanGate.default_cap(4) == 1
    assert BackgroundScanGate.default_cap(3) == 1
    assert BackgroundScanGate.default_cap(1) == 1
  end

  test "on the default +SDio 10 the flush ScanGate keeps its 8 and the background gate adds 2" do
    # The flush permit count is pinned here as well: this change must not have moved it.
    assert ScanGate.default_cap(10) == 8
    assert BackgroundScanGate.default_cap(10) == 2
  end

  test "cap/0 honours the config and 0/false turn the gate off" do
    assert BackgroundScanGate.cap() == 2
    Application.put_env(:fathom, :background_scan_permits, 0)
    assert BackgroundScanGate.cap() == nil
    Application.put_env(:fathom, :background_scan_permits, false)
    assert BackgroundScanGate.cap() == nil
    Application.delete_env(:fathom, :background_scan_permits)

    assert BackgroundScanGate.cap() ==
             BackgroundScanGate.default_cap(:erlang.system_info(:dirty_io_schedulers))
  end

  test "N concurrent holders: never more than `cap` inside at once, every one gets through",
       %{gate: gate} do
    parent = self()
    n = 7
    inside = :atomics.new(1, [])

    for i <- 1..n do
      spawn_link(fn ->
        result =
          BackgroundScanGate.with_permit(
            fn ->
              now_inside = :atomics.add_get(inside, 1, 1)
              send(parent, {:in, i, self(), now_inside})

              receive do
                :leave -> :atomics.sub(inside, 1, 1)
              end

              i
            end,
            gate,
            5_000
          )

        send(parent, {:done, i, result})
      end)
    end

    # Exactly `cap` are inside and every other caller is queued, not inside.
    wait_until(fn -> BackgroundScanGate.stats(gate) == %{held: 2, waiting: n - 2} end)
    assert :atomics.get(inside, 1) == 2

    # Let them out one at a time. Each freed permit goes to the next waiter, and no one ever sees
    # more than `cap` callers inside when it entered.
    for _ <- 1..n do
      assert_receive {:in, i, pid, seen}
      assert seen <= 2
      send(pid, :leave)
      assert_receive {:done, ^i, ^i}
    end

    wait_until(fn -> BackgroundScanGate.stats(gate) == %{held: 0, waiting: 0} end)
  end

  test "a release hands the permit to the oldest waiter", %{gate: gate} do
    a = hold_in_task(gate)
    _b = hold_in_task(gate)
    assert %{held: 2, waiting: 0} = BackgroundScanGate.stats(gate)

    waiter = Task.async(fn -> BackgroundScanGate.acquire(5_000, gate) end)
    wait_until(fn -> BackgroundScanGate.stats(gate).waiting == 1 end)

    send(a, :release)
    assert Task.await(waiter) == :ok
  end

  test "a holder that dies frees its permit without releasing", %{gate: gate} do
    a = hold_in_task(gate)
    b = hold_in_task(gate)
    assert %{held: 2} = BackgroundScanGate.stats(gate)

    waiter = Task.async(fn -> BackgroundScanGate.acquire(5_000, gate) end)
    wait_until(fn -> BackgroundScanGate.stats(gate).waiting == 1 end)

    ref = Process.monitor(a)
    Process.exit(a, :kill)
    assert_receive {:DOWN, ^ref, :process, ^a, :killed}

    # The dead holder's permit went to the queued waiter by its monitor alone.
    assert Task.await(waiter) == :ok
    _ = b
  end

  test "a waiter that dies leaves the queue and takes nothing", %{gate: gate} do
    _a = hold_in_task(gate)
    _b = hold_in_task(gate)

    waiter = spawn(fn -> BackgroundScanGate.acquire(:infinity, gate) end)
    wait_until(fn -> BackgroundScanGate.stats(gate).waiting == 1 end)

    ref = Process.monitor(waiter)
    Process.exit(waiter, :kill)
    assert_receive {:DOWN, ^ref, :process, ^waiter, :killed}

    wait_until(fn -> BackgroundScanGate.stats(gate) == %{held: 2, waiting: 0} end)
  end

  test "with_permit/3 times out with a clear error, never runs fun, and leaks nothing",
       %{gate: gate} do
    _a = hold_in_task(gate)
    _b = hold_in_task(gate)

    test_pid = self()

    assert {:error, :background_scan_busy} =
             BackgroundScanGate.with_permit(
               fn ->
                 send(test_pid, :ran)
                 :ran
               end,
               gate,
               30
             )

    refute_received :ran
    assert %{held: 2, waiting: 0} = BackgroundScanGate.stats(gate)
  end

  test "with_permit/3 returns fun's result and releases on raise", %{gate: gate} do
    assert BackgroundScanGate.with_permit(fn -> :result end, gate, 1_000) == :result
    assert %{held: 0} = BackgroundScanGate.stats(gate)

    assert_raise RuntimeError, fn ->
      BackgroundScanGate.with_permit(fn -> raise "boom" end, gate, 1_000)
    end

    wait_until(fn -> BackgroundScanGate.stats(gate).held == 0 end)
  end

  test "queued/2 has no deadline of its own: it waits, then runs, then releases", %{gate: gate} do
    a = hold_in_task(gate)
    _b = hold_in_task(gate)

    parent = self()

    task =
      Task.async(fn ->
        BackgroundScanGate.queued(
          fn ->
            send(parent, :ran)
            :ok
          end,
          gate
        )
      end)

    wait_until(fn -> BackgroundScanGate.stats(gate).waiting == 1 end)
    refute_received :ran

    send(a, :release)
    assert Task.await(task) == :ok
    assert_received :ran
    wait_until(fn -> BackgroundScanGate.stats(gate).held == 1 end)
  end

  test "with_wait_ms/2 sets the default wait for this process, nests, and restores" do
    base = BackgroundScanGate.wait_ms()

    BackgroundScanGate.with_wait_ms(7, fn ->
      assert BackgroundScanGate.wait_ms() == 7
      BackgroundScanGate.with_wait_ms(9, fn -> assert BackgroundScanGate.wait_ms() == 9 end)
      assert BackgroundScanGate.wait_ms() == 7
    end)

    assert BackgroundScanGate.wait_ms() == base

    assert_raise RuntimeError, fn ->
      BackgroundScanGate.with_wait_ms(7, fn -> raise "boom" end)
    end

    assert BackgroundScanGate.wait_ms() == base
  end

  test "gate disabled or not running is a no-op that fails open" do
    Application.put_env(:fathom, :background_scan_permits, 0)
    assert BackgroundScanGate.acquire(10, @gate) == :ok
    assert BackgroundScanGate.with_permit(fn -> :ran end, @gate, 10) == :ran
    Application.put_env(:fathom, :background_scan_permits, 1)
    assert BackgroundScanGate.acquire(10, :background_scan_gate_not_started) == :ok
    assert BackgroundScanGate.queued(fn -> :ran end, :background_scan_gate_not_started) == :ran
  end

  describe "independence from the flush ScanGate" do
    setup do
      prev = Application.get_env(:fathom, :shard_flush_scan_permits, :unset)
      Application.put_env(:fathom, :shard_flush_scan_permits, 2)

      on_exit(fn ->
        if prev == :unset,
          do: Application.delete_env(:fathom, :shard_flush_scan_permits),
          else: Application.put_env(:fathom, :shard_flush_scan_permits, prev)
      end)

      %{flush_gate: start_supervised!({ScanGate, name: :flush_gate_under_test})}
    end

    test "a saturated background gate does not block a flush ScanGate acquire",
         %{gate: gate, flush_gate: flush_gate} do
      _a = hold_in_task(gate)
      _b = hold_in_task(gate)

      assert {:error, :background_scan_busy} =
               BackgroundScanGate.with_permit(fn -> :x end, gate, 20)

      assert ScanGate.acquire(100, flush_gate) == :ok
      assert %{held: 1} = ScanGate.stats(flush_gate)
      ScanGate.release(flush_gate)
    end

    test "a saturated flush ScanGate does not block a background acquire",
         %{gate: gate, flush_gate: flush_gate} do
      parent = self()

      for _ <- 1..2 do
        spawn_link(fn ->
          :ok = ScanGate.acquire(1_000, flush_gate)
          send(parent, :flush_held)

          receive do
            :never -> :ok
          end
        end)
      end

      assert_receive :flush_held
      assert_receive :flush_held
      assert ScanGate.acquire(20, flush_gate) == :timeout

      assert BackgroundScanGate.with_permit(fn -> :ran end, gate, 100) == :ran
    end

    test "the background permit count does not move the flush ScanGate's" do
      Application.delete_env(:fathom, :shard_flush_scan_permits)
      flush_default = ScanGate.cap()

      Application.put_env(:fathom, :background_scan_permits, 5)
      assert ScanGate.cap() == flush_default
      assert flush_default == ScanGate.default_cap(:erlang.system_info(:dirty_io_schedulers))
    end
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
