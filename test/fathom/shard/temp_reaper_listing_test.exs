defmodule Fathom.Shard.TempReaperListingTest do
  @moduledoc """
  The reaper must not copy or retain the fleet-sized name list (expert review 2026-11-04 #16).

  Symptom: each sweep received every directory name through the side process's exit reason and kept
  the garbage on its heap until the next GC (34 MB after a 400k-entry sweep; the `:bench` guard in
  `temp_reaper_memory_bench_test.exs` measures that size). Invariants: `interesting/1` keeps every
  name a consumer of the listing looks at and drops the rest; `list_dir_isolated/3` applies the
  filter in the side process; and after a sweep of a dir full of unrelated entries the reaper's heap
  is small (the 20k-entry version fails on the unfiltered, non-hibernating reaper).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.TempReaper

  test "interesting/1 keeps temps, quarantine copies and retained markers, drops the rest" do
    keep = [
      "a.db.snap.7",
      "b.db.pull.dl.99",
      "c.db.fenced.123",
      "d.db.forked.1",
      "e.db.corrupt.9",
      "f.db.retained"
    ]

    drop = ["plain.db", "plain.db-wal", "plain.db.etag", ".hidden.db.fenced.1", "notes.txt"]

    kept = TempReaper.interesting(keep ++ drop)

    for n <- keep, do: assert(n in kept, "#{n} was filtered out but a sweep consumer needs it")
    for n <- ["plain.db", "plain.db-wal", "plain.db.etag", "notes.txt"], do: refute(n in kept)
  end

  test "list_dir_isolated applies the filter inside the listing" do
    dir = Path.join(System.tmp_dir!(), "fathom_fs_filter_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    for n <- ["a.db", "b.db", "c.txt"], do: File.write!(Path.join(dir, n), "")

    assert {:ok, ["c.txt"]} =
             Fathom.FS.list_dir_isolated(dir, 5_000, fn names ->
               Enum.filter(names, &String.ends_with?(&1, ".txt"))
             end)

    assert {:ok, all} = Fathom.FS.list_dir_isolated(dir)
    assert Enum.sort(all) == ["a.db", "b.db", "c.txt"]
  end

  test "a sweep over many unrelated entries leaves the reaper with a small heap" do
    prev = Application.get_env(:fathom, :shard_data_dir)
    dir = Path.join(System.tmp_dir!(), "fathom_reaper_heap_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    Application.put_env(:fathom, :shard_data_dir, dir)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_data_dir),
        else: Application.put_env(:fathom, :shard_data_dir, prev)

      File.rm_rf(dir)
    end)

    pid = start_supervised!(TempReaper)
    _ = TempReaper.sweep()
    for i <- 1..20_000, do: :ok = Fathom.FS.write(Path.join(dir, "stub#{i}.db"), "")

    assert TempReaper.sweep() == 0
    {:memory, bytes} = Process.info(pid, :memory)
    assert bytes < 500_000, "the reaper retains #{bytes} bytes after the sweep"
  end
end
