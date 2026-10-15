defmodule Fathom.ShardsStaleRegistryEntryTest do
  @moduledoc """
  `Shards.ensure/1` must never hand back a DEAD coordinator pid.

  Symptom (CI 2026-10-15, seed 906875, `pre_touch_etag_flush_test` and `shard_coordinator_open_test`):
  a test kills a coordinator and calls `Shards.ensure/1` for the same shard; `ensure` returned the
  dead pid and the caller's first `:sys.get_state`/`Shard.checkout` exited `:noproc`.

  Cause: the Registry partition removes a dead pid's entry asynchronously, and `Registry.lookup/2`
  (and `match/3`) return the stale `{dead_pid, _}` in that window; `ensure/1` returned it.

  The fixture suspends the partition so the stale entry is held for as long as the test needs.
  Not async: the Registry is node-global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards

  # Opens `shard`, suspends its Registry partition, kills the coordinator and returns
  # `{old_pid, partition}` with the stale entry still registered.
  defp stale_entry(shard) do
    {:ok, old} = Shards.ensure(shard)
    _ = :sys.get_state(old)
    {:links, links} = Process.info(old, :links)

    partition =
      Enum.find(links, fn pid ->
        match?({:registered_name, n} when is_atom(n), Process.info(pid, :registered_name)) and
          String.starts_with?(
            Atom.to_string(elem(Process.info(pid, :registered_name), 1)),
            "Elixir.Fathom.ShardRegistry."
          )
      end)

    assert is_pid(partition)
    :sys.suspend(partition)
    ref = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old, _}, 5_000

    # Precondition: the suspended partition still holds the dead entry, which is what
    # `Registry.lookup/2` returns unfiltered. Without it the test would not exercise the bug.
    assert [{^old, _}] = Registry.lookup(Fathom.ShardRegistry, shard)
    refute Process.alive?(old)
    {old, partition}
  end

  defp cleanup(shard) do
    on_exit(fn ->
      Shards.drain(shard, 2_000)
      for f <- Path.wildcard(Path.join(Fathom.Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
    end)
  end

  test "ensure/1 starts a fresh coordinator when the registered one is dead but not yet cleaned up" do
    shard = "stalereg_#{System.unique_integer([:positive])}"
    cleanup(shard)
    {old, partition} = stale_entry(shard)

    try do
      assert {:ok, new} = Shards.ensure(shard)
      assert new != old
      assert Process.alive?(new)
    after
      :sys.resume(partition)
    end
  end

  # The request path: the first attempt may land on the dead pid (the hot path deliberately does no
  # liveness check, which cost -47% same_shard_checkout_per_s in d3e5fc0b); the bounded retry must
  # re-resolve past the stale entry instead of returning {:error, :unavailable}.
  test "checkout/1 and checkout/2 recover from a dead-but-registered coordinator" do
    for scope <- [nil, :rw] do
      shard = "stalereg_#{System.unique_integer([:positive])}"
      cleanup(shard)
      {old, partition} = stale_entry(shard)

      try do
        result = if scope, do: Shards.checkout(shard, scope), else: Shards.checkout(shard)
        assert {:ok, new, ref, _path} = normalize(result)
        assert new != old
        assert Process.alive?(new)
        Fathom.Shard.checkin(new, ref)
      after
        :sys.resume(partition)
      end
    end
  end

  defp normalize({:ok, pid, ref, path}), do: {:ok, pid, ref, path}
  defp normalize({:ok, pid, ref, path, _reuse}), do: {:ok, pid, ref, path}
  defp normalize(other), do: other
end
