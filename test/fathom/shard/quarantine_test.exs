defmodule Fathom.Shard.QuarantineTest do
  # Expert review #23: the coordinator preserves acked-but-unflushed / corrupt local copies in
  # uniquely-named .db.fenced/.forked/.corrupt files instead of dropping them — but with no
  # enumeration, no recovery tooling, and no retention they were unrecoverable by a normal operator
  # and an unbounded local-disk leak. This pins the enumeration + the TempReaper retention sweep +
  # the standing-count gauge. Not async — the reaper + data dir are global.
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shard.TempReaper

  setup do
    dir = Path.join(System.tmp_dir!(), "fathom_quar_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    prev_data = Application.get_env(:fathom, :shard_data_dir)
    prev_ret = Application.get_env(:fathom, :quarantine_retention_ms)

    on_exit(fn ->
      restore(:shard_data_dir, prev_data)
      restore(:quarantine_retention_ms, prev_ret)
      File.rm_rf(dir)
    end)

    %{dir: dir}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp touch!(path, body \\ "x") do
    File.write!(path, body)
    path
  end

  test "quarantine_files/1 enumerates the three kinds and nothing else", %{dir: dir} do
    fenced = touch!(Path.join(dir, "acme.db.fenced.123-1"))
    forked = touch!(Path.join(dir, "beta.db.forked.456-2"))
    corrupt = touch!(Path.join(dir, "gamma.db.corrupt.789"))
    # Not quarantines: a live shard, its provenance sidecar, and an in-flight pull temp.
    touch!(Path.join(dir, "acme.db"))
    touch!(Path.join(dir, "acme.db.etag"))
    touch!(Path.join(dir, "acme.db.pull"))

    found = Shard.quarantine_files(dir)

    assert Enum.sort(found) == Enum.sort([fenced, forked, corrupt])
  end

  test "the reaper sweeps quarantines past retention, keeps fresh ones, and emits the count gauge",
       %{dir: dir} do
    Application.put_env(:fathom, :shard_data_dir, dir)
    # A 1-minute retention; the old file (mtime 1h ago) is past it, the fresh one is not.
    Application.put_env(:fathom, :quarantine_retention_ms, 60_000)

    old = touch!(Path.join(dir, "old.db.fenced.1-1"))
    fresh = touch!(Path.join(dir, "fresh.db.forked.2-2"))
    :ok = File.touch(old, System.os_time(:second) - 3600)

    ref = make_ref()
    test = self()

    :telemetry.attach(
      {__MODULE__, ref},
      [:fathom, :shard, :quarantines],
      fn _e, meas, _meta, _ -> send(test, {:gauge, ref, meas.count}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

    start_supervised!(TempReaper)
    _ = TempReaper.sweep()

    refute File.exists?(old), "a quarantine older than the retention cap must be swept"
    assert File.exists?(fresh), "a fresh quarantine must NOT be swept"
    assert_received {:gauge, ^ref, count} when count >= 1
  end

  test "retention 0 keeps quarantines forever (the leak-off escape hatch)", %{dir: dir} do
    Application.put_env(:fathom, :shard_data_dir, dir)
    Application.put_env(:fathom, :quarantine_retention_ms, 0)

    old = touch!(Path.join(dir, "keep.db.fenced.1-1"))
    :ok = File.touch(old, System.os_time(:second) - 365 * 24 * 3600)

    start_supervised!(TempReaper)
    _ = TempReaper.sweep()

    assert File.exists?(old), "retention 0 must never delete a quarantine, no matter how old"
  end
end
