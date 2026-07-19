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

  # Round-2 #9 (Critical): do_run fetched only statements(target) and stamped
  # user_version = target — a shard 2+ behind jumped straight to HEAD applying only
  # HEAD's DDL, silently missing every intermediate version's CREATE/ALTER and
  # django_migrations rows, while all three version stamps agreed it was current.
  # Capture records one fleet version per Django migration transaction, so
  # multi-step laggards are ROUTINE on the cold tail. The invariant: a v1→v3
  # migration applies v2's AND v3's statements, in order.
  test "a multi-step laggard replays every intermediate version, not just the target",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    {:ok, _} =
      Migrator.release(3, "add tags", [
        "CREATE TABLE app_tag (id INTEGER PRIMARY KEY, label TEXT)",
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0003_add_tags', 'now')"
      ])

    assert {:ok, %{from: 1, to: 3}} = ShardMigration.run(shard, 3)

    # v2's DDL landed (pre-fix: only v3's did — created_at was silently missing) ...
    assert %{rows: [[1, "alice", nil]]} =
             query_live!(shard, "SELECT id, name, created_at FROM app_thing")

    # ... and v3's, and both bookkeeping rows, in order.
    assert %{rows: [[0]]} = query_live!(shard, "SELECT count(*) FROM app_tag")

    assert %{rows: [["0001_initial"], ["0002_add_created_at"], ["0003_add_tags"]]} =
             query_live!(shard, "SELECT name FROM django_migrations ORDER BY name")

    assert %{rows: [[3]]} = query_live!(shard, "PRAGMA user_version")
    assert {:ok, %{schema_version: 3}} = Directory.get(shard)
  end

  # The chain's fail-closed half: a missing/yanked INTERMEDIATE makes the chain
  # unbuildable — the migration must error with the shard untouched at its old
  # version, never stamp target having skipped a step (pre-fix it "succeeded",
  # applying only v3 — and a yanked middle version made the corruption
  # unrecoverable).
  test "a yanked intermediate version fails the chain, leaving the shard untouched",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "bad", @v2_statements)
    {:ok, _} = Migrator.release(3, "good", ["CREATE TABLE app_ok (id INTEGER PRIMARY KEY)"])
    assert :ok = Migrator.yank(2)

    assert {:error, {:unknown_version, 2}} = ShardMigration.run(shard, 3)

    assert {:ok, %{schema_version: 1}} = Directory.get(shard),
           "the shard must stay at its old version, not half-migrate"

    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")
  end

  # Expert review 2026-07-18 #10: a `requires_review` version was ceilinged only in head/0, so a
  # DIRECT run(shard, N) at the flagged version bypassed the review floor and replayed the flagged
  # data-migration. statements/1 now refuses it structurally (like yanked), so the chain is
  # unbuildable and the shard stays untouched until an operator approves the version.
  test "a requires_review target is refused by run, leaving the shard untouched until approved",
       %{shard: shard} do
    seed_v1!(shard)

    # v2 flagged requires_review (release/5's 5th arg) — a captured data migration held for review.
    {:ok, _} = Migrator.release(2, "data backfill", @v2_statements, nil, true)

    assert {:error, {:unknown_version, 2}} = ShardMigration.run(shard, 2)

    assert {:ok, %{schema_version: 1}} = Directory.get(shard),
           "a flagged version must not be replayed by a direct run"

    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")

    # After an operator clears the flag, the same run applies normally.
    assert :ok = Migrator.approve_review(2)
    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)
  end

  # Expert review #40: run/3 used Directory.resolve — which upserts last_active_at and
  # registers unknown ids — as its version read. Every sweep attempt phantom-bumped
  # recency on shards no client touched (corrupting warm-follower targeting, laggard
  # ordering, and over-refusing the revert write-age guard), and a mistyped shard id
  # minted a bogus active v0 directory row. The invariant: the control plane READS the
  # directory; only the checkout path registers and touches.
  test "run reads the directory without touching recency or minting rows", %{shard: shard} do
    seed_v1!(shard)
    {:ok, before} = Directory.get(shard)

    # The already-at-target no-op path must not bump last_active_at.
    assert :ok = ShardMigration.run(shard, 1)
    {:ok, unchanged} = Directory.get(shard)
    assert DateTime.compare(unchanged.last_active_at, before.last_active_at) == :eq

    # A mistyped id errors instead of minting an active v0 row.
    assert {:error, :unknown_shard} = ShardMigration.run("no_such_shard_mig40", 1)
    assert Directory.get("no_such_shard_mig40") == :error
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

  # Round-2 expert review #5: the read-only `fence/2` above only proves the LOCK is ours
  # at that instant. The migrator can then stall and a new owner can flush different bytes
  # to the live object before the migrator's PUT lands — and the migrator flushed
  # UNCONDITIONALLY (Storage.flush/2), clobbering the new owner's object with a migrated
  # copy of the OLD lineage, with zero error signal. The flush is now If-Match-fenced on
  # the object's pull-time etag. Here the object (not the lock) is overwritten inside the
  # flush call, AFTER the lock-fence check passed.
  test "a live-object change after the fence check aborts the migrator flush, no clobber",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    # A new owner flushes different bytes to the live object during the migrator's flush,
    # after its lock-fence passed — changing the object etag but NOT the lock.
    overwrite = fn -> File.write!(Path.join(@remote_dir, "#{shard}.db"), "new-owner-bytes") end
    Application.put_env(:fathom, :faulty_before, {:flush, overwrite})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    # Pre-fix: the unconditional PUT clobbered the object with the migrated v2.
    assert {:error, :superseded} = ShardMigration.run(shard, 2)

    assert File.read!(Path.join(@remote_dir, "#{shard}.db")) == "new-owner-bytes",
           "the migrator must not clobber the new owner's object"
  end

  # Expert review 2026-07-14 #4 (the REVERT counterpart of the forward flush fence above): the
  # forward flush was made If-Match-fenced but the revert's restore was NOT — do_revert guarded
  # with a read-only fence/2 (check_lease) then performed an UNCONDITIONAL Storage.restore. A
  # steal landing between the fence read and the copy-back was not caught: restore clobbered the
  # stealer's freshly-flushed live object with the reverted lineage, and the stealer's next flush
  # 412s while check_lease still returns :ok (the restore touched the DATA object, not the lock),
  # so the stealer concludes "durable" and drops its local copy — discarding acknowledged
  # post-steal writes. The restore is now If-Match-fenced on the live object's pull-time etag.
  # Here a new owner overwrites the live object AFTER the migrator's lock-fence passed (and after
  # the retain backup) but BEFORE the restore copy-back — changing the object etag, not the lock.
  test "a live-object change after the fence check aborts the revert restore, no clobber",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    # A new owner flushes different bytes to the live object during the migrator's restore, after
    # its lock-fence (and the retain backup) passed — changing the object etag but NOT the lock.
    overwrite = fn -> File.write!(Path.join(@remote_dir, "#{shard}.db"), "new-owner-bytes") end
    Application.put_env(:fathom, :faulty_before, {:restore, overwrite})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    # force: true isolates the restore fence from the write-age guard (not under test here).
    # Pre-fix: the unconditional restore clobbered the object with the reverted v1.
    assert {:error, :superseded} =
             ShardMigration.revert(shard, 1, "revert-fence-token", force: true)

    assert File.read!(Path.join(@remote_dir, "#{shard}.db")) == "new-owner-bytes",
           "the migrator must not clobber the new owner's object on revert"
  end

  # Expert review 2026-07-14 #9: the migrator's lease renewer stopped on ANY non-{:ok,_},
  # conflating a TRANSIENT store blip ({:error, reason}) with real ownership loss
  # ({:error, :superseded}). A single S3 hiccup during a long copy would then silently END
  # renewal — lapsing the lock's TTL and letting a client steal the shard MID-MIGRATION. The
  # decision is now split into renew_continue?/1: continue on {:ok,_} AND on transient errors,
  # stop ONLY on :superseded (mirroring the coordinator's own renewal in Fathom.Shard).
  describe "renew_continue?/1 (the renew loop's continue/stop decision)" do
    test "keeps the loop alive on a successful renew" do
      lease = %{owner: "migrator@n@1", epoch: 3, expires_at_ms: 0}
      assert ShardMigration.renew_continue?({:ok, lease})
    end

    test "keeps the loop alive on a TRANSIENT store error (retry, don't fence)" do
      # A store blip is not loss of ownership — the loop must retry after the interval,
      # exactly as the storage behaviour contract (renew_lease/3) documents.
      assert ShardMigration.renew_continue?({:error, :timeout})
      assert ShardMigration.renew_continue?({:error, {:transient_lookup, :econnrefused}})
      assert ShardMigration.renew_continue?({:error, %RuntimeError{message: "boom"}})
    end

    test "STOPS the loop only on :superseded (real ownership loss)" do
      refute ShardMigration.renew_continue?({:error, :superseded})
    end
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
