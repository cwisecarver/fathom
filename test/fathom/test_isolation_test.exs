defmodule Fathom.TestIsolationTest do
  @moduledoc """
  Guards the per-run cleanup in `test/test_helper.exs`.

  Shard ids in this suite come from `System.unique_integer/1`, which RESTARTS every VM and whose
  values overlap across runs (measured: three fresh VMs opened at 11907, 7877 and 4103). The
  storage dir, meanwhile, persists. So a leftover object from an earlier run can belong to a later
  run's test.

  A stale `.lock` is the worst version of that, because it does not merely add data — it makes the
  shard **unopenable**. `acquire_lease` reports `{:held, <previous run's owner>}` and the checkout
  fails with `FILO_SHARD_OPEN`. And a lock reads as LIVE for `shard_lease_ttl_ms + steal_margin_ms`
  (30s + 5s) after it was written, so a lock left in the closing seconds of one run is still held
  when the next starts — precisely the window that back-to-back runs sit in. That is why it only
  ever appeared during rapid successive runs and never reproduced on a re-run afterwards.

  Found 2026-07-26: `Fathom.ShardExecutorTest` failed on seed 126081 with 2,050 leaked
  `test_exec_*.lock` files on disk.
  """
  use ExUnit.Case, async: true

  @remote_dir Application.compile_env(:fathom, [Fathom.Shard.Storage.Local, :dir]) ||
                Path.join(System.tmp_dir!(), "fathom_remote_test")

  # Wall-clock instant this VM booted. Anything on disk older than it cannot belong to this run.
  defp boot_time_ms do
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    System.os_time(:millisecond) - uptime_ms
  end

  test "no lock object predates this VM — test_helper's per-run cleanup ran" do
    boot = boot_time_ms()

    stale =
      @remote_dir
      |> Path.join("*.lock")
      |> Path.wildcard()
      |> Enum.filter(fn f ->
        case File.stat(f, time: :posix) do
          # A small grace: the cleanup runs a moment after boot, and mtime is second-resolution.
          {:ok, %{mtime: mtime}} -> mtime * 1000 < boot - 2_000
          _ -> false
        end
      end)

    assert stale == [],
           "#{length(stale)} lock object(s) survived from an earlier run, e.g. " <>
             "#{inspect(Enum.take(stale, 3))}. A leftover lock makes a colliding shard id " <>
             "UNOPENABLE for 35s (lease TTL + steal margin) — the shard reports " <>
             "{:shard_held, <a previous run's owner>}. test/test_helper.exs is supposed to clear " <>
             "these at startup."
  end
end
