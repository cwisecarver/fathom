defmodule Fathom.ShardAdmissionTest do
  # Per-node admission control: at :max_open_shards, a NEW shard open is refused
  # cleanly with {:error, :node_at_capacity} (no crash / no fd-cliff), while an
  # already-open shard keeps serving. async: false — it reads the shared
  # Fathom.ShardRegistry count, so no other test may open shards concurrently.
  use ExUnit.Case, async: false

  alias Fathom.Shards

  setup do
    prev = Application.get_env(:fathom, :max_open_shards)
    prev_evict = Application.get_env(:fathom, :evict_idle_at_capacity)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :max_open_shards),
        else: Application.put_env(:fathom, :max_open_shards, prev)

      if prev_evict == nil,
        do: Application.delete_env(:fathom, :evict_idle_at_capacity),
        else: Application.put_env(:fathom, :evict_idle_at_capacity, prev_evict)

      # This test's shards all share the "adm_" prefix, so a glob cleans them without
      # tracking each one (an ETS table owned by the test process dies before on_exit).
      for dir <- [local_dir(), remote_dir()],
          path <- Path.wildcard(Path.join(dir, "adm_*")),
          do: File.rm(path)
    end)

    :ok
  end

  defp unique_shard, do: "adm_#{System.unique_integer([:positive])}"

  test "with idle-eviction off (hard cap), a new open is refused; an already-open shard bypasses it" do
    # The default now SOFTENS the cap (evict an idle shard to admit — see the LRU
    # eviction suite). This pins the hard-cap path: with eviction disabled, at the cap a
    # new open is refused outright, so operators who want strict backpressure still get it.
    Application.put_env(:fathom, :evict_idle_at_capacity, false)

    a = unique_shard()
    # Open + HOLD shard A (the test process is its connection, so it never idle-stops).
    # A held coordinator guarantees Registry.count >= 1, so with a cap of 1 any NEW
    # shard is refused regardless of coordinators left over from prior tests.
    assert {:ok, pid_a, _ref, _path} = Shards.checkout(a)
    Application.put_env(:fathom, :max_open_shards, 1)

    assert {:error, :node_at_capacity} = Shards.checkout(unique_shard())

    # The cap only gates NEW starts — the already-open shard still checks out.
    assert {:ok, ^pid_a, _ref2, _path2} = Shards.checkout(a)
  end

  test "explicitly disabling the cap (:infinity) never refuses an open" do
    Application.delete_env(:fathom, :max_open_shards)
    assert {:ok, _pid, _ref, _path} = Shards.checkout(unique_shard())
  end

  # Finding #14: the SHIPPED default must be a finite cap, not :infinity — otherwise unauthenticated
  # novel Host ids create shards unbounded up to the emfile cliff. The cap mechanism (above) already
  # enforces; this pins that the default is actually on.
  test "the default open-shard cap is a finite integer, not unbounded" do
    cap = Application.get_env(:fathom, :max_open_shards, :infinity)

    assert is_integer(cap) and cap > 0,
           "an unbounded default lets arbitrary Host ids exhaust the node's fds (got #{inspect(cap)})"
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
