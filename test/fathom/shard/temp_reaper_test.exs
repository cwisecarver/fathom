defmodule Fathom.Shard.TempReaperTest do
  # Expert review 2026-07-14 #2: the amortized janitor that took the uniquely-suffixed
  # orphan-temp cleanup OFF the cold-open hot path. A cold open no longer `Path.wildcard`s
  # the fleet-sized shard data dir; instead this process does that one directory scan on
  # its own timer, so an individual open is O(1). The invariant pinned here: a sweep reaps
  # the STALE `.dl.*`/`.snap.*`/`.tmp.*`/`.pull*` orphans (the disk-leak safety the per-open
  # reap used to provide) while the age gate protects a concurrent pull/snapshot's fresh temp.
  #
  # Not async: it touches the shared shard data dir and the singleton reaper process.
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shard.TempReaper

  setup do
    # Unique shard id so this test's temps never collide with a co-resident file.
    shard = "reaper_#{System.unique_integer([:positive])}"
    path = Shard.db_path(shard)
    File.mkdir_p!(Path.dirname(path))
    %{path: path}
  end

  test "a sweep reaps stale orphan temps but spares fresh sibling work", %{path: path} do
    stale_dl = path <> ".pull.dl.99"
    stale_snap = path <> ".snap.7"
    stale_tmp = path <> ".pull.tmp.3"
    fresh_dl = path <> ".pull.dl.100"

    on_exit(fn ->
      Enum.each([stale_dl, stale_snap, stale_tmp, fresh_dl], &File.rm/1)
    end)

    # temp_reaper is gated off in test; start it ourselves. Drain its boot sweep with one
    # synchronous call (a call is ordered after init's {:continue, :sweep}) BEFORE seeding
    # our temps, so the count from the sweep below is exactly our three stale orphans.
    start_supervised!(TempReaper)
    _ = TempReaper.sweep()

    for f <- [stale_dl, stale_snap, stale_tmp, fresh_dl], do: File.write!(f, "orphaned temp")
    # The orphans predate any plausible in-flight work; fresh_dl keeps its fresh mtime.
    for f <- [stale_dl, stale_snap, stale_tmp], do: File.touch!(f, {{2020, 1, 1}, {0, 0, 0}})

    assert TempReaper.sweep() == 3, "the sweep reaps exactly the three stale orphans"

    refute File.exists?(stale_dl), "a stale orphaned .dl temp must be reaped by the sweep"
    refute File.exists?(stale_snap), "a stale orphaned .snap temp must be reaped by the sweep"
    refute File.exists?(stale_tmp), "a stale orphaned .tmp temp must be reaped by the sweep"
    assert File.exists?(fresh_dl), "the age gate must protect a concurrent pull's fresh temp"
  end
end
