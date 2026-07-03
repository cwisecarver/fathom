defmodule Fathom.Migrator.ShardMigrationTest do
  # End-to-end per-shard migration over filesystem storage + the directory. Not
  # async (shared sandbox so Postgres ops run in the test process; shards/storage
  # are global).
  use Fathom.DataCase, async: false

  alias Fathom.Migrator.ShardMigration
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.{Directory, Migrator}

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002_add_created_at', 'now')"
  ]

  setup do
    shard = "mig_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for path <- Path.wildcard(Path.join(@remote_dir, "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join([System.tmp_dir!(), "fathom_shards", "#{shard}*"])),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  # Seed a v1 shard: its live storage object holds the schema/data, the directory
  # records it at v1.
  defp seed_v1!(shard) do
    seed = Path.join(System.tmp_dir!(), "seed_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(seed)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)")
    :ok = Connection.exec(conn, "INSERT INTO app_thing (id, name) VALUES (1, 'alice')")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"
      )

    :ok =
      Connection.exec(
        conn,
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001_initial', 'now')"
      )

    :ok = Connection.exec(conn, "PRAGMA user_version = 1")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)

    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, 1)
    :ok
  end

  defp query_live!(shard, sql) do
    tmp = Path.join(System.tmp_dir!(), "check_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, _etag} = Storage.pull(shard, tmp)
    {:ok, conn} = Connection.open(tmp)
    {:ok, result} = Connection.query(conn, sql, [])
    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    result
  end

  defp retained?(shard, version),
    do: File.exists?(Path.join(@remote_dir, "#{shard}@#{version}.db"))

  # Apply `sql` to the live object and flush it back — simulate a tenant write on the live version.
  defp write_live!(shard, sql) do
    tmp = Path.join(System.tmp_dir!(), "wl_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, _etag} = Storage.pull(shard, tmp)
    {:ok, conn} = Connection.open(tmp)
    :ok = Connection.exec(conn, sql)
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    :ok
  end

  # Query a retained version object (`<shard>@<version>.db`) via a throwaway copy.
  defp query_version!(shard, version, sql) do
    src = Path.join(@remote_dir, "#{shard}@#{version}.db")

    tmp =
      Path.join(
        System.tmp_dir!(),
        "qv_#{shard}_#{version}_#{System.unique_integer([:positive])}.db"
      )

    File.cp!(src, tmp)
    {:ok, conn} = Connection.open(tmp)
    {:ok, result} = Connection.query(conn, sql, [])
    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    result
  end

  test "migrates to v2 preserving data + bookkeeping, retains v1, then reverts", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)

    assert {:ok, %{schema_version: 2, status: "active"}} = Directory.get(shard)

    assert %{rows: [[1, "alice", nil]]} =
             query_live!(shard, "SELECT id, name, created_at FROM app_thing")

    assert %{rows: [["0001_initial"], ["0002_add_created_at"]]} =
             query_live!(shard, "SELECT name FROM django_migrations ORDER BY name")

    assert %{rows: [[2]]} = query_live!(shard, "PRAGMA user_version")
    assert retained?(shard, 1)

    # Revert: back up live v2, restore the retained v1 over live, cut the directory back.
    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1)
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    assert %{columns: ["id", "name"]} = query_live!(shard, "SELECT * FROM app_thing")
    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")
  end

  test "run is a no-op once the shard is already at the target", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    {:ok, _} = ShardMigration.run(shard, 2)
    assert ShardMigration.run(shard, 2) == :ok
  end

  test "crash-forward: live already at target but directory behind → cutover only", %{
    shard: shard
  } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    # Simulate a crash between flush (live = v2) and cutover: directory back to 1.
    {:ok, _} = Directory.cutover(shard, 1)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)
    assert %{rows: [[2]]} = query_live!(shard, "PRAGMA user_version")
  end

  # Finding #13: a revert overwrites the live vN object with the vN-1 copy. Without a backup, all
  # post-cutover writes on vN (and vN itself) are destroyed unrecoverably. The revert now retains
  # vN first, so those writes survive at <shard>@<vN> for the retention window.
  test "revert backs up the live vN object so post-cutover writes are recoverable", %{
    shard: shard
  } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    # A tenant writes on live v2 AFTER cutover.
    write_live!(shard, "INSERT INTO app_thing (id, name) VALUES (2, 'bob')")
    assert %{rows: [[2]]} = query_live!(shard, "SELECT count(*) FROM app_thing")

    # Revert to v1: pre-fix this destroys bob unrecoverably.
    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1)
    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")

    # The v2 object (with bob) is retained and recoverable.
    assert retained?(shard, 2), "revert must back up the live vN object before restoring"

    assert %{rows: [[2, "bob"]]} =
             query_version!(shard, 2, "SELECT id, name FROM app_thing WHERE id = 2")
  end

  # Expert review #10: a revert retry after a failed cutover clobbered the vN backup. Attempt 1
  # runs retain(vN) -> restore(vN-1) -> cutover, and if the cutover fails (Postgres blip) Oban
  # retries; on attempt 2 the directory STILL says current = vN, so retain(vN) copied the live
  # object — now holding vN-1 bytes from attempt 1's restore — over <shard>@vN, destroying the
  # only copy of the post-cutover vN writes. The invariant: a revert retry must converge
  # (crash-forward off the live file's user_version) without ever overwriting the backup.
  test "a revert retry after a failed cutover does not clobber the vN backup", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    # Post-cutover tenant write on v2 — the data the backup exists to preserve.
    write_live!(shard, "INSERT INTO app_thing (id, name) VALUES (2, 'bob')")

    # Attempt 1, up to the crash: retain the real v2 bytes, restore v1 over live — and die
    # before Directory.cutover (the directory still says v2).
    :ok = Storage.retain(shard, 2)
    :ok = Storage.restore(shard, 1)
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)
    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")

    # Attempt 2 (Oban retry). Pre-fix this re-ran retain(2), copying the v1 bytes now in
    # live over the <shard>@2 backup — bob destroyed unrecoverably.
    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1, "retry-token", force: true)

    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")

    assert %{rows: [[2, "bob"]]} =
             query_version!(shard, 2, "SELECT id, name FROM app_thing WHERE id = 2"),
           "the retry must not overwrite the vN backup with the already-restored vN-1 bytes"
  end

  # Finding #13 (force-guard): a revert discards every write made on the live version since its
  # cutover. Pre-guard, `revert/2` silently proceeded no matter how long the shard had been live
  # (the review's "a revert issued days into the retention window silently discards days of tenant
  # writes"). The invariant pinned: a shard the directory shows ACTIVE since cutover_at refuses
  # the revert — before anything touches storage — unless the operator passes force: true.
  test "revert refuses a shard active since cutover unless forced", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    # Tenant traffic after the cutover: a checkout access bumps last_active_at past cutover_at.
    {:ok, _} = Directory.resolve(shard)

    assert {:error, {:writes_since_cutover, %{last_active_at: la, cutover_at: co}}} =
             ShardMigration.revert(shard, 1)

    assert DateTime.compare(la, co) == :gt

    # The refusal happened before retain/restore: live is untouched (still v2), no v2 backup
    # object was created, and the directory still points at v2.
    assert %{rows: [[2]]} = query_live!(shard, "PRAGMA user_version")
    refute retained?(shard, 2), "a refused revert must not touch storage"
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)

    # force: true is the operator's explicit confirmation — the same revert proceeds.
    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1, "force-token", force: true)
    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")
    assert retained?(shard, 2), "a forced revert still backs up the live version"
  end

  # Expert review #11 (guard-input freshness): a checkout sitting in the Recorder's ≤1s
  # buffer was invisible to the write-age guard — the guard read the directory while the
  # touch that should refuse the revert hadn't flushed yet, so an unforced revert passed
  # and discarded real post-cutover activity. The revert must flush this node's buffer
  # before reading the guard's inputs.
  test "the write-age guard sees touches still sitting in the Recorder buffer", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    # Post-cutover tenant activity via the ASYNC path: buffered, not yet in Postgres.
    :ok = Fathom.Directory.Recorder.record(shard)

    assert {:error, {:writes_since_cutover, _}} = ShardMigration.revert(shard, 1),
           "a buffered touch must refuse the unforced revert"

    assert %{rows: [[2]]} = query_live!(shard, "PRAGMA user_version")
  end

  # A quiet shard (no directory activity since cutover) reverts without force: cutover stamps
  # last_active_at and cutover_at with the same instant, so "no activity" is exactly equality.
  test "revert of a quiet shard needs no force", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1)
  end

  # A row with no cutover stamp (predates the cutover_at column, or the directory lost it) has
  # an UNKNOWN write-age — fail closed and make the operator confirm, rather than assuming
  # "no writes" on missing data.
  test "revert fails closed when the cutover age is unknown", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    {1, _} =
      Repo.update_all(
        from(s in Fathom.Directory.Shard, where: s.shard_id == ^shard),
        set: [cutover_at: nil]
      )

    assert {:error, {:unknown_write_age, _}} = ShardMigration.revert(shard, 1)
    assert %{rows: [[2]]} = query_live!(shard, "PRAGMA user_version")

    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1, "force-token", force: true)
  end

  # Finding #7: the migrator holds a lease but never re-checks it before flushing. If the shard
  # is stolen mid-copy (its lock lapsed / a checkout took over), the pre-flush fence (check_lease)
  # must abort the migration so it doesn't clobber the new owner with the migrated file. Steal the
  # lock during the migrator's retain (just before the copy + fence) via the FaultyStorage hook.
  test "a shard stolen mid-migration self-fences before flush and does not clobber", %{
    shard: shard
  } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :faulty_before, {:retain, fn -> write_thief_lock(shard) end})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    assert {:error, :superseded} = ShardMigration.run(shard, 2)

    # The migrator did NOT flush v2 over the stealer — the live object is still v1.
    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")
  end

  # A foreign owner takes the shard's lock (a different owner/epoch than the migrator's), so the
  # migrator's check_lease reports :superseded. No heartbeat needed — check_lease compares the
  # lock, not liveness.
  defp write_thief_lock(shard) do
    File.mkdir_p!(@remote_dir)

    File.write!(
      Path.join(@remote_dir, "#{shard}.lock"),
      Jason.encode!(%{
        "owner" => "thief@node",
        "epoch" => 999,
        "expires_at_ms" => System.system_time(:millisecond) + 60_000
      })
    )
  end
end
