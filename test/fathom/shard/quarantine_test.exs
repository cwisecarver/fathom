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
    # A 1-minute retention; the old file (quarantined 1h ago, per its name) is past it, the fresh
    # one is not. This used to set the age with `File.touch` on the mtime and name the files with
    # ms stamps of 1 and 2 — i.e. it pinned the mtime ageing that expert review 2026-09-29 #32
    # showed is wrong (rename preserves mtime). The name's stamp is the quarantine time now.
    Application.put_env(:fathom, :quarantine_retention_ms, 60_000)
    now = System.system_time(:millisecond)

    old = touch!(Path.join(dir, "old.db.fenced.#{now - 3_600_000}-1"))
    fresh = touch!(Path.join(dir, "fresh.db.forked.#{now}-2"))

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

  # Expert review 2026-09-29 #32. A quarantine is a RENAME and rename preserves mtime, so a copy of
  # content last written long ago carries an ancient mtime the moment it is quarantined. Ageing by
  # mtime swept that recovery copy within one reaper interval. Invariant: age = the quarantine time
  # in the name, and a `.db` + `-wal` + `-shm` set is kept or swept as a unit.
  test "age is the quarantine time in the name, not the content's mtime; sets age as a unit",
       %{dir: dir} do
    Application.put_env(:fathom, :shard_data_dir, dir)
    Application.put_env(:fathom, :quarantine_retention_ms, 60_000)
    now = System.system_time(:millisecond)
    ancient = System.os_time(:second) - 60 * 24 * 3600

    # Just quarantined, but the content (and so the mtime, carried by the rename) is 60 days old.
    fresh =
      for s <- ["", "-wal", "-shm"], do: touch!(Path.join(dir, "idle.db.forked.#{now}-7#{s}"))

    for f <- fresh, do: :ok = File.touch(f, ancient)

    # Quarantined an hour ago; its -wal was appended to just before, so its mtime is recent.
    stale =
      for s <- ["", "-wal", "-shm"],
          do: touch!(Path.join(dir, "busy.db.corrupt.#{now - 3_600_000}-8#{s}"))

    :ok = File.touch(Enum.at(stale, 1), System.os_time(:second))

    start_supervised!(TempReaper)
    _ = TempReaper.sweep()

    for f <- fresh,
        do: assert(File.exists?(f), "#{Path.basename(f)}: a fresh quarantine was swept by mtime")

    for f <- stale,
        do: refute(File.exists?(f), "#{Path.basename(f)}: a stale set was split or kept")
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
