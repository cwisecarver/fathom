defmodule Fathom.Host.FsyncProbeTest do
  # Expert review 2026-10-29 #17c: the opt-in (`FSYNC_PROBE`) 1 Hz 4 KiB write + fdatasync probe.
  #
  # Two things are pinned: the probe measures and reports, and its file can never be mistaken for a
  # shard by anything that enumerates the data dir. The second is what made (c) safe to ship at all.
  #
  # Not async: the enumeration test points `:shard_data_dir` at a private dir and drives the
  # singleton-named temp reaper.
  use ExUnit.Case, async: false

  alias Fathom.Host.FsyncProbe
  alias Fathom.Shard
  alias Fathom.Shard.{Storage, TempReaper}

  setup do
    dir =
      Path.join(System.tmp_dir!(), "fathom_fsync_probe_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  describe "probe/1" do
    test "writes one 4 KiB block, reports microseconds, and overwrites rather than grows", %{
      dir: dir
    } do
      path = FsyncProbe.probe_path(dir)

      assert {:ok, us} = FsyncProbe.probe(path)
      assert is_integer(us) and us >= 0
      assert File.stat!(path).size == 4096

      assert {:ok, _} = FsyncProbe.probe(path)
      assert File.stat!(path).size == 4096, "a probe round must overwrite, not append"
    end

    test "a failure is an error tuple, not a raise", %{dir: dir} do
      assert {:error, _} = FsyncProbe.probe(Path.join([dir, "no_such_dir", ".fsync_probe"]))
    end
  end

  describe "the process" do
    test "records a duration sample every interval", %{dir: dir} do
      ref = make_ref()
      test = self()

      :telemetry.attach(
        {__MODULE__, ref},
        [:fathom, :node, :fsync_probe],
        fn _, m, _, _ -> send(test, {ref, m}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      start_supervised!({FsyncProbe, dir: dir, interval_ms: 5})

      assert_receive {^ref, %{duration: us}}, 5_000
      assert is_integer(us)
      assert_receive {^ref, %{duration: _}}, 5_000
      assert File.exists?(FsyncProbe.probe_path(dir))
    end

    test "a failing probe counts an error instead of a sample", %{dir: dir} do
      ref = make_ref()
      test = self()

      :telemetry.attach_many(
        {__MODULE__, ref},
        [[:fathom, :node, :fsync_probe], [:fathom, :node, :fsync_probe_error]],
        fn event, m, _, _ -> send(test, {ref, event, m}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      # The "dir" is a regular file, so mkdir_p fails and every round's open fails.
      blocker = Path.join(dir, "blocker")
      File.write!(blocker, "x")

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({FsyncProbe, dir: blocker, interval_ms: 5})
        assert_receive {^ref, [:fathom, :node, :fsync_probe_error], %{count: 1}}, 5_000
      end)

      refute_received {^ref, [:fathom, :node, :fsync_probe], _}
    end
  end

  describe "the probe file is not a shard file" do
    test "no data-dir name predicate matches it", %{dir: dir} do
      name = Path.basename(FsyncProbe.probe_path(dir))

      refute Storage.temp_orphan?(name)
      assert Shard.quarantine_files_in([name], dir) == []
      refute String.contains?(name, ".db"), "tombstone/tenant sweeps key on `<id>.db`"
      refute Fathom.ShardId.valid?(name), "a shard id can never be a dotfile"
    end

    test "a stale probe file survives a real temp-reaper sweep that does reap a stale temp", %{
      dir: dir
    } do
      prev = Application.get_env(:fathom, :shard_data_dir)
      Application.put_env(:fathom, :shard_data_dir, dir)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, :shard_data_dir),
          else: Application.put_env(:fathom, :shard_data_dir, prev)
      end)

      probe = FsyncProbe.probe_path(dir)

      # Drain the reaper's boot sweep first (a call is ordered after init's continue), so the sweep
      # below sees exactly the files seeded after it.
      start_supervised!(TempReaper)
      _ = TempReaper.sweep()

      {:ok, _} = FsyncProbe.probe(probe)

      # The control: an orphan temp of the same age, which the sweep must reap, proves the sweep
      # looked at this dir and would have taken the probe file if its name matched.
      orphan = Path.join(dir, "ctl.db.pull.dl.1")
      File.write!(orphan, "orphan")
      for f <- [probe, orphan], do: File.touch!(f, {{2020, 1, 1}, {0, 0, 0}})

      assert TempReaper.sweep() == 1

      refute File.exists?(orphan)
      assert File.exists?(probe), "the reaper swept the fsync probe file"
    end
  end
end
