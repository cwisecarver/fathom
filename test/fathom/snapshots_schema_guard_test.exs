defmodule Fathom.SnapshotsSchemaGuardTest do
  @moduledoc """
  The snapshot-restore schema-version guard (expert review 2026-07-19 #7). A snapshot carries the
  schema version its bytes were captured at (the file's `PRAGMA user_version`). Restoring one across a
  migration boundary — a `vN-1` snapshot after the fleet cut to `vN` — would leave the directory
  claiming `vN` over a `vN-1` file: the laggard sweep believes it's migrated and never converges it,
  and `vN` app code reads a `vN-1` schema. `Snapshots.restore` now refuses that without `force`, and
  on `force` reconciles the directory to the restored version so the laggard sweep re-migrates forward.
  Directory is sandboxed (DataCase); storage is real files seeded directly.
  """
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, Snapshots}
  alias Fathom.Shard.{Connection, Storage}

  defp uniq, do: "snapguard_#{System.unique_integer([:positive])}"

  # Push a real SQLite file at `user_version` as the shard's live stored object (no coordinator).
  defp seed_object(shard, user_version) do
    tmp = Path.join(System.tmp_dir!(), "seed_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(tmp)
    {:ok, _} = Connection.query(conn, "PRAGMA user_version = #{user_version}", [])
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (x)", [])
    :ok = Connection.close(conn)
    :ok = Storage.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
  end

  defp cleanup(shard) do
    for s <- [".db", ".db-wal", ".db-shm", ".lock"],
        do: File.rm(Path.join(remote_dir(), shard <> s))

    for snap <- Path.wildcard(Path.join(remote_dir(), "#{shard}@snap-*.db")), do: File.rm(snap)
  end

  # HOLDING the lease, not merely probing it (expert review 2026-08-20 #12).
  #
  # restore/3 used to read `lease_holder/1` and proceed on `:free`. A request landing on another
  # node between that probe and the copy-back cold-opens, acquires the freed lease, pulls the
  # object at etag E and starts serving. Our If-Match: E copy-back then SUCCEEDS, and that node's
  # next flush 412s, re-checks the lock, finds it still its own, resyncs to the new etag and
  # re-uploads its PRE-RESTORE bytes. The restore is silently undone within one flush interval —
  # at maximum operator pressure, with :ok already reported.
  #
  # THE PROPERTY IS THE WINDOW, not the refusal. A probe-only implementation ALSO returns
  # {:error, {:held, _}} when someone already holds the lease, so asserting that discriminates
  # nothing — the first draft of this test did exactly that and passed against the bug. What has
  # to be pinned is that no one can ACQUIRE mid-restore, so this injects a competing acquire into
  # the window itself, through FaultyStorage's `run_before(:object_etag)` hook, which fires
  # between the lease decision and the copy-back.
  test "no one can acquire the lease mid-restore (#12)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    {:ok, snap} = Snapshots.create(shard)

    prev_backend = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    test_pid = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:object_etag,
       fn ->
         # Exactly what a cold open on another node does in this window.
         send(
           test_pid,
           {:mid_restore_acquire, Storage.acquire_lease(shard, "intruder@node", 30_000)}
         )

         :ok
       end}
    )

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if is_nil(prev_backend),
        do: Application.delete_env(:fathom, :shard_storage),
        else: Application.put_env(:fathom, :shard_storage, prev_backend)
    end)

    result = Snapshots.restore(shard, snap)

    assert_receive {:mid_restore_acquire, acquired},
                   2_000,
                   "the hook never fired, so the window was never opened and this proves nothing"

    # THE ASSERTION. Pre-fix the restore only PROBED, so the lease was free in this window and the
    # intruder gets it — then serves the shard and re-uploads pre-restore bytes over the snapshot.
    assert match?({:error, _}, acquired),
           "another owner acquired the lease DURING the restore (#{inspect(acquired)}); its next " <>
             "flush will 412, reconcile to the restored etag, and re-upload its pre-restore bytes"

    assert result == :ok

    assert Storage.lease_holder(shard) == :free,
           "restore/3 held the lease and never released it — the shard is now unopenable"
  end

  # The ordinary refusal still holds: a lease already held by someone else is not stolen.
  test "a restore refuses while another owner holds the lease (#12)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    {:ok, snap} = Snapshots.create(shard)

    {:ok, other} = Storage.acquire_lease(shard, "someone-else@node", 30_000)
    assert {:error, {:held, _}} = Snapshots.restore(shard, snap)
    :ok = Storage.release_lease(shard, other)

    assert :ok = Snapshots.restore(shard, snap)
    assert Storage.lease_holder(shard) == :free
  end

  test "a cross-version restore is refused without force, and with force reconciles the directory (#7)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    {:ok, snap} = Snapshots.create(shard)

    # The fleet cut over to v2 and this shard migrated: the directory now says 2, the snapshot is v0.
    {:ok, _} = Directory.cutover(shard, 2)
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)

    # A restore across the boundary is refused — it would leave the directory claiming v2 over a v0 file.
    assert {:error, {:schema_version_mismatch, %{snapshot: 0, directory: 2}}} =
             Snapshots.restore(shard, snap)

    assert {:ok, %{schema_version: 2}} = Directory.get(shard), "a refused restore changes nothing"

    # With force, the restore proceeds AND reconciles the directory to the restored version, so the
    # laggard sweep (schema_version 0 < head) converges it forward instead of believing it is migrated.
    assert :ok = Snapshots.restore(shard, snap, force: true)
    assert {:ok, %{schema_version: 0}} = Directory.get(shard)
  end

  test "a same-version restore needs no force and leaves the directory unchanged (#7)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    {:ok, snap} = Snapshots.create(shard)

    # A within-version restore (the common "undo the bad data change" case) is unaffected.
    seed_object(shard, 0)
    assert :ok = Snapshots.restore(shard, snap)
    assert {:ok, %{schema_version: 0}} = Directory.get(shard)
  end

  test "restoring a missing snapshot id returns :snapshot_not_found (#7)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)

    assert {:error, :snapshot_not_found} = Snapshots.restore(shard, "20260101T000000Z-0000")
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
