defmodule Fathom.Shard.ReplicationSeedGateTest do
  @moduledoc """
  The node-wide seed cap (expert review 2026-10-01 #6). Seeds arrive in bursts — a failover
  reopens ~1/N of the fleet and each reopen seeds every follower — and each one holds its
  follower's single link for the whole transfer. These pin the cap, the FIFO hand-off, and the
  property that matters most: a seed task KILLED as wedged (`Session.expire_seeds/1` uses
  `Process.exit(pid, :kill)`, which runs no cleanup) still gives its slot back.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.SeedGate

  setup do
    prev = Application.get_env(:fathom, :replication_seed_max_concurrency)
    Application.put_env(:fathom, :replication_seed_max_concurrency, 2)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_seed_max_concurrency),
        else: Application.put_env(:fathom, :replication_seed_max_concurrency, prev)
    end)

    name = :"seed_gate_#{System.unique_integer([:positive])}"
    gate = start_supervised!({SeedGate, name: name})
    %{gate: gate}
  end

  # A process that takes a slot, reports, and holds it until told to stop.
  defp holder(gate, timeout_ms \\ 5_000) do
    test = self()

    spawn(fn ->
      result = SeedGate.acquire(timeout_ms, gate)
      send(test, {:acquired, self(), result})

      receive do
        :release -> SeedGate.release(gate)
      end

      receive do
        :exit -> :ok
      end
    end)
  end

  test "no more than the cap hold a slot; the next waits", %{gate: gate} do
    a = holder(gate)
    b = holder(gate)
    assert_receive {:acquired, ^a, :ok}
    assert_receive {:acquired, ^b, :ok}

    c = holder(gate)
    refute_receive {:acquired, ^c, _}, 100
    assert %{held: 2, waiting: 1} = SeedGate.stats(gate)

    send(a, :release)
    assert_receive {:acquired, ^c, :ok}, 1_000
  end

  test "a holder KILLED without releasing frees its slot", %{gate: gate} do
    a = holder(gate)
    b = holder(gate)
    assert_receive {:acquired, ^a, :ok}
    assert_receive {:acquired, ^b, :ok}
    c = holder(gate)
    refute_receive {:acquired, ^c, _}, 100

    # What `expire_seeds/1` does to a wedged seed. No `after`, no release.
    Process.exit(a, :kill)
    assert_receive {:acquired, ^c, :ok}, 1_000
  end

  test "waiters are served oldest first", %{gate: gate} do
    a = holder(gate)
    b = holder(gate)
    assert_receive {:acquired, ^a, :ok}
    assert_receive {:acquired, ^b, :ok}

    c = holder(gate)
    refute_receive {:acquired, ^c, _}, 50
    d = holder(gate)
    refute_receive {:acquired, ^d, _}, 50

    send(a, :release)
    assert_receive {:acquired, ^c, :ok}, 1_000
    refute_receive {:acquired, ^d, _}, 100
  end

  test "a waiter that times out gets an error and does not keep a slot", %{gate: gate} do
    a = holder(gate)
    b = holder(gate)
    assert_receive {:acquired, ^a, :ok}
    assert_receive {:acquired, ^b, :ok}

    late = holder(gate, 100)
    assert_receive {:acquired, ^late, {:error, :seed_gate_timeout}}, 1_000

    send(a, :release)
    # The slot a freed must go back to the pool, not to the waiter that already gave up.
    e = holder(gate)
    assert_receive {:acquired, ^e, :ok}, 1_000
    assert %{held: 2, waiting: 0} = SeedGate.stats(gate)
  end

  test "a cap of 0 disables the gate", %{gate: gate} do
    Application.put_env(:fathom, :replication_seed_max_concurrency, 0)
    for _ <- 1..5, do: assert(:ok = SeedGate.acquire(100, gate))
  end
end
