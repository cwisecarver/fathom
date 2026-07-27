defmodule Fathom.DirectoryWiringTest do
  # Exercises the real shard machinery, so not async. The directory touch is
  # enabled here (it is off by default in test config).
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, Shards}
  alias Fathom.Directory.Recorder

  setup do
    prev = Application.get_env(:fathom, :directory_touch)
    Application.put_env(:fathom, :directory_touch, true)
    shard = "wire_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :directory_touch),
        else: Application.put_env(:fathom, :directory_touch, prev)

      for dir <- [local_dir(), remote_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard}
  end

  test "checking out a shard records it, but off the synchronous hot path", %{shard: shard} do
    assert Directory.get(shard) == :error

    {:ok, pid, ref, _path} = Shards.checkout(shard)

    # S7 regression pin: the directory write is DEFERRED off the checkout path —
    # the access is buffered, not yet in Postgres. Pre-S7 (synchronous resolve)
    # the row existed here immediately; that synchronous Postgres round-trip per
    # checkout is exactly what this change removes.
    assert Directory.get(shard) == :error

    # The buffered access lands once the recorder flushes (here driven
    # synchronously; in prod a ~1s periodic timer does the same).
    assert Recorder.flush() >= 1

    assert {:ok, entry} = Directory.get(shard)
    assert entry.shard_id == shard
    assert entry.schema_version == 0
    assert entry.status == "active"

    Fathom.Shard.checkin(pid, ref)
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
