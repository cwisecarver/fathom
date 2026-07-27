defmodule Fathom.Directory.ReconcileTest do
  @moduledoc """
  Cross-store DR reconcile (expert review 2026-07-19 #6): after a Postgres directory restore rewinds
  it out of sync with storage, the sweep realigns the directory to the authoritative durable facts —
  the file's `user_version` (schema_version), the storage revocation floor (token_version), and the
  storage tombstones (deleted status). Directory is sandboxed (DataCase); storage is real files, so
  each test seeds the stored object directly and cleans up the storage keys.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.Directory.Reconcile
  alias Fathom.Shard.{Connection, Storage}

  defp uniq, do: "recon_#{System.unique_integer([:positive])}"

  # Build a real SQLite file at `user_version` and push it as the shard's stored object (no
  # coordinator needed — this is the durable object reconcile reads).
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

    File.rm(Path.join([remote_dir(), "tokenfloors", shard]))
    File.rm(Path.join([remote_dir(), "tombstones", shard]))
  end

  test "aligns directory schema_version to the file's user_version; dry run reports, --fix applies (#6)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 5)

    # Dry run reports the drift and changes nothing (the directory was rolled back below the file).
    findings = Reconcile.run()

    assert Enum.any?(
             findings,
             &match?(%{shard_id: ^shard, kind: :schema_drift, from: 0, to: 5, fixed: false}, &1)
           )

    assert {:ok, %{schema_version: 0}} = Directory.get(shard),
           "dry run must not modify the directory"

    # --fix aligns the directory to the file's authoritative version.
    fixed = Reconcile.run(fix: true)

    assert Enum.any?(
             fixed,
             &match?(%{shard_id: ^shard, kind: :schema_drift, to: 5, fixed: true}, &1)
           )

    assert {:ok, %{schema_version: 5}} = Directory.get(shard)
  end

  test "raises token_version to the durable storage floor (#6)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    :ok = Storage.put_token_floor(shard, 7)

    _ = Reconcile.run(fix: true)

    assert Directory.token_version(shard) >= 7,
           "the directory floor must be raised to the storage floor"
  end

  test "re-tombstones a storage-tombstoned id the directory forgot (#6)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    # The directory says active (a restore un-deleted it), but storage says deleted.
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    :ok = Storage.put_tombstone(shard)

    _ = Reconcile.run(fix: true)
    assert {:ok, %{status: "deleted"}} = Directory.get(shard)
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
