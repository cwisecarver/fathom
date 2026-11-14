defmodule Fathom.Tenants.TombstonesDrTest do
  @moduledoc """
  The cross-store DR backstop (expert review 2026-07-19 #6). The re-mint guard is the Postgres
  directory loaded into an ETS set — but a Postgres point-in-time restore rolls the directory back
  and can un-tombstone a deleted tenant, so a stray request re-mints an empty shard for a subdomain
  that was erased (a GDPR failure). A durable storage tombstone under a `tombstones/` namespace —
  untouched by `purge_shard` and not rolled back with Postgres — keeps the guard complete across a
  directory restore: a booting node unions it into the ETS.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Test.Await
  alias Fathom.Tenants.Tombstones

  setup do
    id = "tomb_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      File.rm(Path.join([remote_dir(), "tombstones", id]))

      for s <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(remote_dir(), id <> s))
    end)

    %{id: id}
  end

  test "the storage tombstone is listed and survives a full purge (#6)", %{id: id} do
    assert :ok = Storage.put_tombstone(id)
    assert {:ok, ids} = Storage.tombstoned_ids()
    assert id in ids

    # A full erasure of the tenant must NOT sweep the tombstone marker — it lives in a distinct
    # `tombstones/` namespace, so a delete-job retry (which re-runs purge_shard) can't erase the proof.
    File.write!(Path.join(remote_dir(), id <> ".db"), "data")
    assert :ok = Storage.purge_shard(id)
    refute File.exists?(Path.join(remote_dir(), id <> ".db")), "purge must erase the live object"

    assert {:ok, ids2} = Storage.tombstoned_ids()
    assert id in ids2, "the tombstone marker must outlive a full purge"
  end

  test "a fresh Tombstones boot repopulates the ETS from storage (survives a directory restore) (#6)",
       %{id: id} do
    # The id is tombstoned in STORAGE only — absent from the directory and from the running ETS. This
    # stands in for "Postgres was restored to before the delete, but object storage was not."
    refute Tombstones.tombstoned?(id)
    assert :ok = Storage.put_tombstone(id)

    # Restart the guard (a node boot): init unions the directory with the storage scan.
    pid = Process.whereis(Tombstones)
    ref = Process.monitor(pid)
    :ok = GenServer.stop(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 2_000

    # A supervisor restart is async with no signal to await, so poll on a real time budget — a tight
    # spin-loop finishes in microseconds, far faster than the restart, and spuriously "times out"
    # under suite contention.
    Await.until(
      fn -> match?(p when is_pid(p) and p != pid, Process.whereis(Tombstones)) end,
      "Tombstones did not restart within 2s",
      2_000
    )

    new_pid = Process.whereis(Tombstones)

    # Sync on init having run (load_from_directory + load_from_storage complete before it handles this).
    _ = :sys.get_state(new_pid)

    assert Tombstones.tombstoned?(id),
           "a booted node must refuse a storage-tombstoned id even after the directory forgot it"
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
