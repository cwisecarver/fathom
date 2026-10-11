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
    end
  end

  test "open_count/0 does not raise while a Registry partition is down", %{shard: shard} do
    {:ok, _pid} = Shards.ensure(shard)

    with_dead_partition(fn ->
      count = Shards.open_count()
      assert is_integer(count) and count >= 1
    end)
  end
end
