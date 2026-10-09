defmodule Fathom.Migrator.ShardMigrationTest do
  # End-to-end per-shard migration over filesystem storage + the directory. Not
  # async (shared sandbox so Postgres ops run in the test process; shards/storage
  # are global).
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Migrator.{RetirementJob, ShardMigration}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.{Directory, Migrator}
  alias Fathom.Tenants.Tombstones

  # A data transform that rewrites VALUES in place — the class of change DDL cannot express, and
  # whose revert is the leg expert review 2026-09-05 #28 found untested. Allowlisted per-test.
  defmodule UppercaseName do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform

    @impl true
    def run(conn, _shard_id) do
      case Fathom.Shard.Connection.query(conn, "UPDATE app_thing SET name = upper(name)", []) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002_add_created_at', 'now')"
  ]

  setup do
    shard = "mig_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for path <- Path.wildcard(Path.join(remote_dir(), "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{shard}*"])),
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
    do: File.exists?(Path.join(remote_dir(), "#{shard}@#{version}.db"))

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
    src = Path.join(remote_dir(), "#{shard}@#{version}.db")

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

  # Expert review 2026-07-18 #5 (the retirement outbox): the migration's cutover now enqueues the
  # old version's retention deletion ATOMICALLY (same Postgres transaction), so a Postgres blip can
  # never leave a cut-over shard whose @prev object is never scheduled for deletion — a silent,
  # un-self-healing storage leak. Pre-fix the enqueue was a separate Oban.insert in the JOB's
  # perform, so run/3 itself enqueued nothing.
  test "a forward migration enqueues the old version's retirement atomically with cutover",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)

    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 1})
  end

  # The leak's mechanism: a Postgres blip on the (pre-fix) separate retirement enqueue crashed the
  # job; its Oban retry saw cutover already landed and short-circuited to a bare :ok that skipped
  # re-enqueuing — so @prev leaked forever. With the outbox, the FIRST run enqueued it atomically, so
  # the crash-forward retry neither loses nor duplicates it.
  test "a crash-forward retry neither loses nor duplicates the retirement", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)

    # The Oban retry after a crashed perform: the directory is already at v2, so run short-circuits.
    assert :ok = ShardMigration.run(shard, 2)

    # Exactly one retirement of @1 survives. Pre-fix run/3 enqueued nothing (the whole path relied
    # on perform, lost on its retry), so this would be 0.
    assert length(
             all_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 1})
           ) ==
             1
  end

  # The revert half (finding #5 names RevertJob too): the revert's cutover enqueues the backed-up
  # version's retirement atomically, so a blip can't strand the @from backup with no scheduled drop.
  test "a revert enqueues the backed-up version's retirement atomically with its cutover",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1)

    assert_enqueued(worker: RetirementJob, args: %{"shard_id" => shard, "version" => 2})
  end

  # Expert review 2026-09-05 #28: the migration gate requires forward + revert + cross-version for a
  # transform-carrying version, but transforms were only tested FORWARD (Copy.migrate_chain). A
  # transform rewrites VALUES in place — a change DDL cannot make — so its revert is a distinct
  # property: the pointer-flip must restore the retained PRE-transform bytes, not the rewritten ones.
  # This is a coverage/gate test (it pins existing behaviour; there is no bug being fixed), the
  # transform half of the three-way requirement #11's directory-guard and #13's rollout tests cover
  # for DDL.
  test "reverting a transform-carrying version restores the pre-transform rows (#28)", %{
    shard: shard
  } do
    seed_v1!(shard)
    assert %{rows: [[1, "alice"]]} = query_live!(shard, "SELECT id, name FROM app_thing")

    prev = Application.get_env(:fathom, :migration_transforms)
    Application.put_env(:fathom, :migration_transforms, [UppercaseName])

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :migration_transforms, prev),
        else: Application.delete_env(:fathom, :migration_transforms)
    end)

    {:ok, _} = Migrator.release(2, "v2 + uppercase", @v2_statements)
    :ok = Migrator.attach_transform(2, UppercaseName)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)

    # Forward: the DDL added created_at AND the transform rewrote the values in place.
    assert %{rows: [[1, "ALICE", nil]]} =
             query_live!(shard, "SELECT id, name, created_at FROM app_thing")

    # Revert to v1: a pointer-flip to the retained pre-transform copy.
    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1)

    # The transform's value rewrite is GONE (name back to 'alice') and the pre-transform rows are
    # exactly as seeded — the property the three-way gate exists to guarantee for a Transform.
    assert %{rows: [[1, "alice"]]} = query_live!(shard, "SELECT id, name FROM app_thing")
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

  # Expert review 2026-08-26 #22. `statement_chain/2` looped `Migrator.statement_step/1` over
  # every version in the range, and each call was its own `Repo.get_by(Release, ...)` selecting
  # the WHOLE row — `statements` (the full captured DDL) and `statement_args` (jsonb) included.
  # Per shard, per rollout: a 1M-shard rollout shipped the same blob 1M times, holding a
  # `Fathom.Repo` connection from the `:migrations` queue for each read.
  #
  # The invariant pinned here is COST, not correctness — the three tests around it already pin
  # that a missing / yanked / requires_review version fails the chain closed. It is one query for
  # the whole chain regardless of how many versions the chain spans, so the count must not scale
  # with the range. Pre-fix this counted 3.
  #
  # IT NOW COUNTS 2, AND THAT IS INTENDED (expert review 2026-09-29 #12). This test used to assert
  # exactly 1, which held only because `do_run` REUSED the pre-flight chain read before the drain —
  # and that reuse is what let an `attach_transform` accepted during the pull window be skipped.
  # The chain is now read twice: once for buildability before the lease (2026-09-18 #11), once,
  # authoritatively, after `mark_migrating`. Both are single queries, so the property this test
  # exists for — the count does not scale with the range (3 versions here) — still holds.
  test "the whole chain costs a CONSTANT number of shard_migrations queries, not one per version",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    {:ok, _} = Migrator.release(3, "v3", ["CREATE TABLE app_tag (id INTEGER PRIMARY KEY)"])
    {:ok, _} = Migrator.release(4, "v4", ["CREATE TABLE app_note (id INTEGER PRIMARY KEY)"])

    counter = :counters.new(1, [])
    handler = "chain-query-count-#{System.unique_integer([:positive])}"

    # SCOPED TO THIS PROCESS. A telemetry handler is VM-global, and a background coordinator or
    # recorder querying the same table would be counted as ours. Ecto emits
    # `[:fathom, :repo, :query]` inline in the process that ran the query, so `self()` isolates it.
    # (`Fathom.DirectoryTest`'s row counter went red exactly this way before it was scoped.)
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler,
        [:fathom, :repo, :query],
        fn _event, _measurements, meta, _config ->
          # `source` is the schema's table; the chain read is the only thing that touches it on
          # this path. Counting the SOURCE rather than grepping SQL keeps this from breaking on a
          # formatting change (AGENTS.md: never hand-roll SQL parsing).
          if self() == test_pid and meta[:source] == "shard_migrations",
            do: :counters.add(counter, 1, 1)
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:ok, %{from: 1, to: 4}} = ShardMigration.run(shard, 4)

    # FIVE since the ledger names were memoised (expert review 2026-10-01 perf #12): pre-flight +
    # post-mark chain read, ONE narrow (id/version/count) release read for each ledger check, and
    # ONE payload fetch for the releases this node had never derived names for — this test's
    # releases are new, so the first check pays it and the second finds them cached. Every later
    # shard on the node pays four, with narrow rows instead of full payloads. Still constant in the
    # range, which is what this test exists to catch.
    assert :counters.get(counter, 1) == 5,
           "the 3-version chain issued #{:counters.get(counter, 1)} shard_migrations queries; " <>
             "it must be five (pre-flight + post-mark + two narrow ledger reads + one payload " <>
             "fetch for never-seen releases) regardless of the range"
  end

  # Expert review 2026-08-26 #28. One forward migration cost ~9 Postgres round trips, three of them
  # avoidable, ALL INSIDE A TRANSACTION HOLDING A POOL CONNECTION — against the table
  # `20260726023618` calls "the worst table in the system" for write amplification. At a 1M-shard
  # rollout that is ~9M round trips.
  #
  # This pins the one that was free to remove: `forward/9` needed the DIRECTORY's `schema_version`
  # to report a stamp divergence and re-read the row a THIRD time to get it, after `run/3` read it
  # and after `mark_migrating/1` had just written and RETURNED it. The update does not touch
  # `schema_version`, so the returned row carries the same value — sampled at the same instant and
  # inside the lease, which is strictly fresher than the separate read it replaces.
  #
  # Measured both ways on this exact fixture: 6 queries against `shards` before, 5 after.
  test "a forward migration does not re-read the directory row a third time", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    counter = :counters.new(1, [])
    handler = "migration-roundtrips-#{System.unique_integer([:positive])}"

    # Scoped to this process, for the same reason as the chain-query counter above.
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler,
        [:fathom, :repo, :query],
        fn _e, _m, meta, _c ->
          if self() == test_pid and meta[:source] == "shards", do: :counters.add(counter, 1, 1)
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)

    n = :counters.get(counter, 1)

    assert n <= 5,
           "one forward migration made #{n} round trips to `shards`; it was 6 before #28 and " <>
             "must not go back up — every one of these is inside a transaction holding a pool " <>
             "connection, multiplied by the fleet size on a rollout"
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

  # Expert review 2026-09-18 #11: an unbuildable chain (a yanked intermediate) used to be
  # discovered only AFTER with_lease drained the live coordinator and do_run pulled the full object
  # from S3 — so every hourly reconcile re-drained a served tenant and re-pulled, forever, all
  # discarded. The pre-flight refuses from the release rows alone, before touching the lease/storage.
  #
  # Discriminator: HOLD the lease from another owner. Pre-fix, run/3 reaches with_lease first and
  # returns {:retry, {:held, _}} (the drain+pull already happened; the chain was never checked).
  # Post-fix, the pre-flight refuses with {:unknown_version, 2} regardless of the held lease.
  test "a yanked intermediate is refused BEFORE draining/acquiring the lease (#11)", %{
    shard: shard
  } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "bad", @v2_statements)
    {:ok, _} = Migrator.release(3, "good", ["CREATE TABLE app_ok (id INTEGER PRIMARY KEY)"])
    assert :ok = Migrator.yank(2)

    # Someone else holds the lease, un-stealably (long TTL).
    {:ok, _held} = Storage.acquire_lease(shard, "someone_else@node", 60_000)

    assert {:error, {:unknown_version, 2}} = ShardMigration.run(shard, 3),
           "the pre-flight must refuse before with_lease (pre-fix a held lease made this {:retry, {:held, _}})"

    assert {:ok, %{schema_version: 1}} = Directory.get(shard),
           "the shard must stay untouched at its old version"
  end

  # Expert review 2026-09-29 #12. The 2026-09-18 #11 pre-flight read the replay chain BEFORE the
  # drain, the lease and the full S3 pull, and `do_run` REUSED it. The shard stays `active` for that
  # whole window, so the attach_transform guard (which counts shards `migrating` below the version)
  # saw zero and accepted a transform — and this shard then replayed the stale chain WITHOUT it and
  # cut over: a silently split fleet with all three stamps agreeing. The transform is attached here
  # from inside the migrator's own pull, i.e. squarely in that window. Pre-fix the row reads
  # 'alice' (transform skipped); post-fix the chain is read after `mark_migrating` and it is 'ALICE'.
  test "a transform attached during the pull window is applied, not skipped (#12)", %{
    shard: shard
  } do
    seed_v1!(shard)

    for {key, val} <- [
          migration_transforms: [UppercaseName],
          shard_storage: Fathom.Test.FaultyStorage
        ] do
      prev = Application.get_env(:fathom, key)
      Application.put_env(:fathom, key, val)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, key, prev),
          else: Application.delete_env(:fathom, key)
      end)
    end

    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    test_pid = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:pull,
       fn sid ->
         if sid == shard and self() == test_pid and Process.get(:attached_12) == nil do
           Process.put(:attached_12, true)

           assert :ok = Migrator.attach_transform(2, UppercaseName),
                  "fixture: the guard refused the attach, so the window was not reached"
         end

         :ok
       end}
    )

    on_exit(fn -> Application.delete_env(:fathom, :faulty_before) end)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
    assert Process.get(:attached_12), "fixture: the pull hook never ran"

    assert %{rows: [[1, "ALICE"]]} = query_live!(shard, "SELECT id, name FROM app_thing"),
           "a transform accepted mid-migration was skipped by this shard's replay"
  end

  # Expert review 2026-09-29 #20. `run/3` checked only `schema_version`, so a SUSPENDED tenant's
  # queued job drained, retained, copied and fence-flushed the migrated file over its live object —
  # only the cutover refused — leaving the file at v2 and the directory at v1. Pre-fix both tests
  # below see the stored object rewritten (a retained @1 copy and the v2 column appear).
  test "a suspended tenant is not migrated (#20)", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    {:ok, before} = Storage.object_etag(shard)
    {:ok, _} = Directory.suspend(shard)

    assert {:error, {:not_active, "suspended"}} = ShardMigration.run(shard, 2)

    assert {:ok, ^before} = Storage.object_etag(shard),
           "the suspended tenant's object was rewritten"

    refute retained?(shard, 1)
  end

  test "a tenant suspended mid-run (after run/3 looked) stops at mark_migrating (#20)", %{
    shard: shard
  } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    {:ok, before} = Storage.object_etag(shard)

    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    test_pid = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:pull,
       fn sid ->
         if sid == shard and self() == test_pid, do: {:ok, _} = Directory.suspend(shard)
         :ok
       end}
    )

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    assert {:error, {:not_active, _}} = ShardMigration.run(shard, 2)
    assert {:ok, ^before} = Storage.object_etag(shard), "the object was rewritten after a suspend"
    refute retained?(shard, 1)
    assert {:ok, %{status: "suspended", schema_version: 1}} = Directory.get(shard)
  end

  # Expert review 2026-10-08 #7: an attempt that marked the row and then died before unmarking it
  # (node kill, Lifeline rescue, a raise) leaves it `migrating`. `run/3` lets that through, but
  # `mark_migrating` accepted only `active`, so the retry got `{:not_active, :status_conflict}` and
  # the job CANCELLED instead of resuming — the retry was the whole point.
  test "a retry after a crash that left the row migrating resumes and migrates", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, %{status: "migrating"}} = Directory.mark_migrating(shard, 1)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
    assert {:ok, %{schema_version: 2, status: "active"}} = Directory.get(shard)
    assert %{rows: [[2]]} = query_live!(shard, "PRAGMA user_version")
  end

  test "re-marking still refuses a suspended tenant", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Directory.suspend(shard)
    assert {:error, :status_conflict} = Directory.mark_migrating(shard, 1)
  end

  # ---- the ledger leg (Fathom.Migrator.Ledger), 2026-10-01 -------------------------------------
  #
  # Releases that carry Django's bookkeeping rows, as captured ones do: v1 adds 0001_initial, v2 adds
  # a column + 0002_add_created_at, v3 adds a table + 0003_tag.
  @l1 [
    "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001_initial', 'now')"
  ]
  @l3 [
    "CREATE TABLE app_tag (id INTEGER PRIMARY KEY)",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0003_tag', 'now')"
  ]

  defp release_ledger_chain! do
    {:ok, _} = Migrator.release(1, "v1", @l1)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    {:ok, _} = Migrator.release(3, "v3", @l3)
  end

  # A stored object whose LABEL is `label` but whose schema and ledger are v1's plus `extra_names`.
  defp seed_labelled!(shard, label, extra_names) do
    seed = Path.join(System.tmp_dir!(), "lseed_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(seed)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)")
    :ok = Connection.exec(conn, "INSERT INTO app_thing (id, name) VALUES (1, 'alice')")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"
      )

    for name <- ["0001_initial" | extra_names] do
      {:ok, _} =
        Connection.query(
          conn,
          "INSERT INTO django_migrations (app, name, applied) VALUES ('app', ?1, 'now')",
          [name]
        )
    end

    :ok = Connection.exec(conn, "PRAGMA user_version = #{label}")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)

    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)
    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, label)
    :ok
  end

  test "a label AHEAD of its Django ledger is migrated from the real version, in order", %{
    shard: shard
  } do
    release_ledger_chain!()
    # The file says v2, but v2 was never applied: no created_at column, no 0002 row.
    seed_labelled!(shard, 2, [])

    assert {:ok, %{from: 2, to: 3}} = ShardMigration.run(shard, 3)

    # v2 was replayed (from the real version, v1), then v3. Pre-fix the chain started at the
    # label, so v2 was skipped: created_at never existed and 0002 never landed, under a v3 stamp.
    assert %{rows: [[1, "alice", nil]]} =
             query_live!(shard, "SELECT id, name, created_at FROM app_thing")

    assert %{rows: [["0001_initial"], ["0002_add_created_at"], ["0003_tag"]]} =
             query_live!(shard, "SELECT name FROM django_migrations ORDER BY name")
  end

  test "a ledger carrying a LATER release's migration is refused and left untouched", %{
    shard: shard
  } do
    release_ledger_chain!()
    # Labelled v1, but 0003_tag is already in the ledger: someone ran migrate against the shard.
    seed_labelled!(shard, 1, ["0003_tag"])
    {:ok, before} = Storage.object_etag(shard)

    assert {:error, {:ledger_mismatch, _}} = ShardMigration.run(shard, 2)
    assert {:ok, ^before} = Storage.object_etag(shard), "the object was rewritten"
    refute retained?(shard, 1)

    assert {:ok, %{schema_version: 1, status: "active", last_verify_status: "ledger_mismatch"}} =
             Directory.get(shard)
  end

  test ":migration_ledger_check :warn lets a mismatched shard migrate from its label", %{
    shard: shard
  } do
    release_ledger_chain!()
    seed_labelled!(shard, 1, ["0003_tag"])
    Application.put_env(:fathom, :migration_ledger_check, :warn)
    on_exit(fn -> Application.delete_env(:fathom, :migration_ledger_check) end)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
  end

  # Expert review 2026-09-29 #20, the purge half. A job already past `run/3` and `mark_migrating`
  # kept going when the tenant was DELETED under it, and `retain` copied the live object to
  # `<shard>@1` — an object a concurrent purge may never see. The hook tombstones the tenant in the
  # fleet-wide gate during the migrator's pull; the directory row is left `active` so
  # `mark_migrating` still passes, which is the state a delete landing just AFTER the mark produces.
  # Pre-fix the copy is made and `retained?/2` is true.
  test "a tenant deleted mid-run gets no retained copy (#20)", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "v2", @v2_statements)
    {:ok, before} = Storage.object_etag(shard)

    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    test_pid = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:pull,
       fn sid -> if sid == shard and self() == test_pid, do: Tombstones.put(shard), else: :ok end}
    )

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      :ets.delete(Tombstones, shard)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    assert {:error, {:not_active, "deleted"}} = ShardMigration.run(shard, 2)
    refute retained?(shard, 1), "a copy of a deleted tenant was created"

    assert {:ok, ^before} = Storage.object_etag(shard),
           "the deleted tenant's object was rewritten"
  end

  # Expert review 2026-08-26 #8. The forward path was the ONLY durable-object producer in the
  # system that did not validate what it was about to publish. Every other one does — the
  # coordinator's periodic flush gates on quick_check, the GDPR export refuses a corrupt export,
  # the restore drill verifies twice — and docs/migration.md's step 3 already SAYS "Validate, then
  # cutover". Storage.pull/2 verifies Content-MD5, so TRANSPORT corruption was caught; a logical
  # SQLite corruption already in the source object was not. This path copies it forward AND
  # advances all three version stamps to say the shard is healthy at HEAD.
  #
  # The VACUUM INTO caveat AGENTS.md records does not apply here, which is what makes the check
  # meaningful: Copy.copy_file/2 is a plain File.cp, so the migrated file is a byte copy of the
  # pulled source rather than a rebuilt-from-table-content VACUUM product.
  test "a corrupt stored object is refused before cutover, not migrated forward", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    # Corrupt the STORED object — what a migration will pull and copy forward.
    remote = Path.join(remote_dir(), "#{shard}.db")
    assert File.exists?(remote), "fixture: the stored object is not where expected"
    corrupt_page!(remote)

    # Precondition: the fixture really corrupted something. Without this the test passes vacuously
    # on a fixture that only scribbled free space — AGENTS.md records that exact trap.
    assert {:error, _} = Fathom.Shard.verify_integrity(remote),
           "the fixture did not actually corrupt anything"

    assert {:error, {:migrated_copy_corrupt, _}} = ShardMigration.run(shard, 2),
           "a corrupt shard was migrated forward and cut over"

    # The shard stays where it was, and all three stamps stay consistent — which is the point.
    # Pre-fix the cutover succeeded and stamped HEAD on corrupt bytes.
    assert {:ok, %{schema_version: 1}} = Directory.get(shard),
           "the directory advanced despite the copy being corrupt"
  end

  defp corrupt_page!(path) do
    size = File.stat!(path).size
    assert size > 8192, "seeded db too small (#{size}B) to corrupt a data page"
    {:ok, fd} = :file.open(path, [:read, :write, :binary, :raw])
    # Page 2 (offset 4096) is a b-tree data page; page 1's header stays intact so the file still
    # opens and quick_check is what finds the damage.
    :ok = :file.pwrite(fd, 4096, :binary.copy(<<0xEF>>, 4096))
    :file.close(fd)
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

  # Expert review 2026-10-08 #14: a crash-forward finalize stamped `cutover_at == last_active_at ==
  # now`, but the target bytes had been live since the earlier attempt's flush — clients wrote on
  # them in between. Those writes then looked pre-cutover, the write-age guard passed, and an
  # unforced revert restored v1 over them.
  test "writes made between a crashed flush and its crash-forward cutover still block a revert",
       %{
         shard: shard
       } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)

    # A crash between the flush (live = v2) and the cutover: the directory is back at 1 …
    {:ok, _} = Directory.cutover(shard, 1)
    # … and a client uses the v2 bytes before the retry finalizes.
    {:ok, _} = Directory.resolve(shard)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)

    assert {:error, {:writes_since_cutover, _}} = ShardMigration.revert(shard, 1),
           "an unforced revert discarded writes made on v2 before its crash-forward cutover"

    assert %{rows: [[2]]} = query_live!(shard, "PRAGMA user_version")
  end

  # Expert review 2026-09-05 #18: forward/9 retains and records against the FILE version (#22), but
  # the crash-forward finalize/2 stamped retained_version from the (possibly stale) DIRECTORY stamp.
  # With file=v3, directory behind at v1 and a retained <shard>@2 present, finalize recorded
  # retained_version: 1 — naming an object that is gone/pre-v2 while the real backup <shard>@2 is
  # orphaned — so a later revert(3,2) would restore v1 bytes and silently discard the v2-era writes.
  # The fix reads what STORAGE actually holds (the highest retained @v below target).
  test "crash-forward finalize records the retained version storage holds, not the stale stamp",
       %{
         shard: shard
       } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    {:ok, _} =
      Migrator.release(3, "add tags", [
        "CREATE TABLE app_tag (id INTEGER PRIMARY KEY, label TEXT)",
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0003_add_tags', 'now')"
      ])

    # Migrate v1->v2 (retains @1), then v2->v3 (retains @2) — so <shard>@2 is the backup a
    # revert(3, 2) would need, and the file lands at v3.
    {:ok, _} = ShardMigration.run(shard, 2)
    {:ok, _} = ShardMigration.run(shard, 3)
    assert retained?(shard, 2), "the run v2->v3 retains <shard>@2"

    # Simulate a crash-forward whose DIRECTORY stamp is stale (two behind the v3 file): a failed
    # cutover transaction or a Postgres PITR does this.
    {:ok, _} = Directory.cutover(shard, 1)

    # The crash-forward retry: file == target, so finalize runs. It must record the version storage
    # holds (@2), not the stale directory stamp (1).
    assert {:ok, %{from: 2, to: 3}} = ShardMigration.run(shard, 3),
           "finalize's `from` is the retained version, which must be 2 (storage), not 1 (stale stamp)"

    assert {:ok, %{schema_version: 3, retained_version: 2}} = Directory.get(shard),
           "finalize must retain against the object storage actually holds, not the stale stamp"
  end

  # Expert review 2026-09-29 #27. The storage heuristic above picks the highest `<shard>@v` below
  # target, and a REVERT leaves a same-named `@N` holding pre-revert bytes for the retention window.
  # So: v1 → v2, revert to v1 (leaves @2), then a v1 → v3 migration that retains @1, flushes v3 and
  # crashes before cutover. Finalize found the stale @2 first, recorded retained_version: 2,
  # scheduled its retirement, and orphaned the real @1 — a later revert restores the old generation.
  # Invariant: finalize records what the crashed attempt RETAINED, which it wrote down beforehand.
  test "crash-forward finalize records the recorded retain intent, not a revert's leftover @N", %{
    shard: shard
  } do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)
    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1)
    assert retained?(shard, 2), "fixture: the revert must leave its @2 backup"

    {:ok, _} =
      Migrator.release(3, "add tags", [
        "CREATE TABLE app_tag (id INTEGER PRIMARY KEY, label TEXT)",
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0003_add_tags', 'now')"
      ])

    {:ok, _} = ShardMigration.run(shard, 3)
    assert {:ok, %{retaining_version: nil}} = Directory.get(shard), "cutover clears the intent"

    # The crash: v3 flushed, cutover never landed. The directory is still at v1 and the intent row
    # that mark_migrating wrote before the retain survives (status reclaimed to active).
    {:ok, _} = Directory.cutover(shard, 1)

    {1, _} =
      Fathom.Repo.update_all(
        from(s in Fathom.Directory.Shard, where: s.shard_id == ^shard),
        set: [retaining_version: 1]
      )

    assert retained?(shard, 1) and retained?(shard, 2), "fixture: both backups must be present"

    assert {:ok, %{from: 1, to: 3}} = ShardMigration.run(shard, 3),
           "finalize picked the revert's stale @2 instead of the @1 this migration retained"

    assert {:ok, %{schema_version: 3, retained_version: 1, retaining_version: nil}} =
             Directory.get(shard)
  end

  test "mark_migrating records the retain intent the migration then acts on", %{shard: shard} do
    seed_v1!(shard)

    assert {:ok, %{status: "migrating", retaining_version: 1}} =
             Directory.mark_migrating(shard, 1)
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
  # Expert review 2026-10-08 #18: reverting a suspended tenant is a deliberate operator restore —
  # retain and restore ran for one — but the cutover accepted only active/migrating and rolled back
  # AFTER the destructive restore, leaving live at v1 under a directory stamp of v2, no retirement
  # for the v2 backup, and five retries each re-pulling the shard.
  test "reverting a suspended tenant lands the cutover and keeps it suspended", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    {:ok, _} = ShardMigration.run(shard, 2)
    {:ok, _} = Directory.suspend(shard)

    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1)

    assert %{rows: [[1]]} = query_live!(shard, "PRAGMA user_version")

    assert {:ok, %{schema_version: 1, status: "suspended", retained_version: 2}} =
             Directory.get(shard),
           "the live file and the directory stamp disagree after the revert"
  end

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
    overwrite = fn -> File.write!(Path.join(remote_dir(), "#{shard}.db"), "new-owner-bytes") end
    Application.put_env(:fathom, :faulty_before, {:flush, overwrite})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    # Pre-fix: the unconditional PUT clobbered the object with the migrated v2.
    assert {:error, :superseded} = ShardMigration.run(shard, 2)

    assert File.read!(Path.join(remote_dir(), "#{shard}.db")) == "new-owner-bytes",
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
    overwrite = fn -> File.write!(Path.join(remote_dir(), "#{shard}.db"), "new-owner-bytes") end
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

    assert File.read!(Path.join(remote_dir(), "#{shard}.db")) == "new-owner-bytes",
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
    File.mkdir_p!(remote_dir())

    File.write!(
      Path.join(remote_dir(), "#{shard}.lock"),
      Jason.encode!(%{
        "owner" => "thief@node",
        "epoch" => 999,
        "expires_at_ms" => System.system_time(:millisecond) + 60_000
      })
    )
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
