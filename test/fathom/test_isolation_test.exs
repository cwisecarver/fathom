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

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
  defp local_dir, do: Fathom.Shard.data_dir()

  # Wall-clock instant this VM booted. Anything on disk older than it cannot belong to this run.
  defp boot_time_ms do
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    System.os_time(:millisecond) - uptime_ms
  end

  # Files in `dir` matching `glob` whose mtime predates this VM's boot.
  defp survivors(dir, glob) do
    boot = boot_time_ms()

    dir
    |> Path.join(glob)
    |> Path.wildcard()
    |> Enum.filter(fn f ->
      case File.stat(f, time: :posix) do
        # A small grace: the cleanup runs a moment after boot, and mtime is second-resolution.
        {:ok, %{mtime: mtime}} -> mtime * 1000 < boot - 2_000
        _ -> false
      end
    end)
  end

  test "no lock object predates this VM — test_helper's per-run cleanup ran" do
    stale = survivors(remote_dir(), "*.lock")

    assert stale == [],
           "#{length(stale)} lock object(s) survived from an earlier run, e.g. " <>
             "#{inspect(Enum.take(stale, 3))}. A leftover lock makes a colliding shard id " <>
             "UNOPENABLE for 35s (lease TTL + steal margin) — the shard reports " <>
             "{:shard_held, <a previous run's owner>}. test/test_helper.exs is supposed to clear " <>
             "these at startup."
  end

  test "no local shard file predates this VM — test_helper clears the shard data dir" do
    stale = survivors(local_dir(), "*")

    assert stale == [],
           "#{length(stale)} local shard file(s) survived from an earlier run, e.g. " <>
             "#{inspect(Enum.take(stale, 3))}. A leftover <shard>.db is treated as an " <>
             "authoritative un-flushed local copy (Fathom.Shard pulls only on cold start), so a " <>
             "colliding id serves that file instead of cold-opening from storage. " <>
             "test/test_helper.exs is supposed to rm_rf this dir at startup."
  end

  test "the shard data dir is test-owned, not shared with dev" do
    # The dir accumulated 5,047 files before 2026-07-27 precisely because it defaulted to the same
    # `fathom_shards` dev uses, where a stray file can hold real un-flushed writes — so the suite
    # could never wipe it. The per-run rm_rf above is only safe while this stays true.
    assert local_dir() != Path.join(System.tmp_dir!(), "fathom_shards"),
           "config/test.exs must point :shard_data_dir at a test-only directory — test_helper.exs " <>
             "rm_rf's it every run, which would destroy un-flushed dev shard files."

    refute local_dir() == remote_dir(),
           "the local shard dir and the storage backend's dir must not be the same directory"
  end
end
