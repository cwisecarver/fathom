defmodule Fathom.ShardsOpenCountTest do
  @moduledoc """
  `Shards.open_count/0` survives a dead Registry partition.

  Symptom (CI run 38106971382, OTP 28): a Registry partition died, its ETS table went with it, and
  until the partition restarted `Registry.count/1` raised (`0 + :undefined`). The admission check
  `at_capacity?/0` called it on every new shard open, so every open on the node raised in that
  window. Invariant: counting open shards never raises; while a partition is down it falls back to
  the coordinator supervisor's child count.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards

  setup do
    shard = "opencount_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for s <- ["", "-wal", "-shm"],
          do: File.rm(Path.join(Fathom.Shard.data_dir(), "#{shard}.db") <> s)
    end)

    %{shard: shard}
  end

  # Kill one Registry partition while its supervisor is suspended, so the partition (and its ETS
  # table) stays dead until we resume: this makes the restart window deterministic instead of
  # the few microseconds CI happened to land in.
  defp with_dead_partition(fun) do
    registry_sup = Process.whereis(Fathom.ShardRegistry)
    assert is_pid(registry_sup)

    partition =
      Enum.find_value(Supervisor.which_children(registry_sup), fn
        {_id, pid, :worker, _} when is_pid(pid) -> pid
        _ -> nil
      end)

    assert is_pid(partition)
    :ok = :sys.suspend(registry_sup)
    ref = Process.monitor(partition)
    Process.exit(partition, :kill)
    assert_receive {:DOWN, ^ref, :process, ^partition, :killed}

    try do
      assert_raise ArithmeticError, fn -> Registry.count(Fathom.ShardRegistry) end
      fun.()
    after
      :ok = :sys.resume(registry_sup)
      _ = :sys.get_state(registry_sup)
      Fathom.Test.RegistryKill.settle()
    end
  end

  test "open_count/0 does not raise while a Registry partition is down", %{shard: shard} do
    {:ok, _pid} = Shards.ensure(shard)

    with_dead_partition(fn ->
      count = Shards.open_count()
      assert is_integer(count) and count >= 1
    end)
  end

  # expert review 2026-10-12 #14: only open_count/0 survived a dead partition; checkout, drain,
  # evict and stop raised ArgumentError out of Registry.lookup/2. Pick an id that hashes to the dead
  # partition (a lookup for any other id still works), then pin each path.
  defp id_on_dead_partition do
    Enum.find_value(1..500, fn i ->
      id = "deadpart_#{System.unique_integer([:positive])}_#{i}"

      try do
        Registry.match(Fathom.ShardRegistry, id, :_)
        nil
      rescue
        ArgumentError -> id
      end
    end) || flunk("no shard id hashed to the dead partition")
  end

  # expert review 2026-10-12 #37: `flush/2` used a bare `Registry.lookup/2`. A dead partition answers
  # `[]` to that, so with no local file the `remote: false` flush answered `:ok` for an id whose
  # coordinator could not be seen. The id is fresh (never opened, no local file), so only the
  # registry read decides the answer.
  test "checkout/drain/stop/flush on a dead partition return errors instead of raising", %{
    shard: shard
  } do
    {:ok, _pid} = Shards.ensure(shard)

    with_dead_partition(fn ->
      id = id_on_dead_partition()
      refute File.exists?(Fathom.Shard.db_path(id))
      assert {:error, :unavailable} = Shards.ensure(id)
      assert {:error, :unavailable} = Shards.drain(id, 100)
      assert {:error, :unavailable} = Shards.flush(id, remote: false)
      assert :ok = Shards.stop(id)
    end)
  end

  # expert review 2026-10-12 #36: on the dead-partition fallback the drain reads each coordinator's id
  # with `:sys.get_state/2`. A child that cannot answer it (here a plain process that never handles
  # system messages, so the call exits `:timeout` after 5 s) used to escape the async stream and kill
  # the caller, which is the whole node drain. It must be counted `:timed_out` instead.
  # (`:sys.suspend/1` does NOT make a GenServer unreadable: system messages still get answered, so
  # the probe for this test has to be a non-OTP process.)
  test "drain_all still drains open coordinators while a partition is down, and counts an unreadable one as timed out",
       %{shard: shard} do
    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)

    {:ok, unreadable} =
      DynamicSupervisor.start_child(Fathom.ShardSupervisor, %{
        id: make_ref(),
        start:
          {Task, :start_link,
           [
             fn ->
               receive do
                 :never -> :ok
               end
             end
           ]},
        restart: :temporary
      })

    try do
      with_dead_partition(fn ->
        assert %{drained: drained, timed_out: timed_out} = Shards.drain_all(10_000)
        assert drained >= 1
        assert timed_out >= 1
      end)
    after
      _ = DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, unreadable)
      # drain_all/1 marks the node draining (a node-global persistent_term). Left set, every later
      # test that reads HealthPlug.draining?/0 sees a draining node (it broke reporter_test's
      # health-snapshot assertion under CI seed 609064).
      Fathom.HealthPlug.end_draining()
    end

    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end
end
