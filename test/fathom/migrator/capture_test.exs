defmodule Fathom.Migrator.CaptureTest do
  # Drives the capture state machine directly (the running Capture GenServer +
  # Postgres). Not async so DataCase runs the sandbox in shared mode, letting the
  # Capture process use the test's connection.
  use Fathom.DataCase, async: false

  alias Fathom.Migrator
  alias Fathom.Migrator.Capture

  describe "classify/1" do
    test "recognizes transaction-control verbs, treating savepoints as :other" do
      assert Capture.classify("BEGIN") == :begin
      assert Capture.classify("begin transaction") == :begin
      assert Capture.classify("COMMIT") == :commit
      assert Capture.classify("END") == :commit
      assert Capture.classify("ROLLBACK") == :rollback
      assert Capture.classify("ROLLBACK TO sp1") == :other
      assert Capture.classify("SAVEPOINT sp1") == :other
      assert Capture.classify("CREATE TABLE t (x)") == :other
    end
  end

  # Django's schema editor sends introspection along with the DDL, and capture recorded it as if it
  # were part of the migration. The allowlist is what keeps this safe: dropping a statement that
  # turns out to matter loses DDL permanently, so anything not provably a read is KEPT.
  describe "pure_read?/1" do
    test "the reads a real Django migrate actually sends" do
      # Verbatim from test/support/fixtures/django_migrate_capture.json.
      assert Capture.pure_read?("""
                 SELECT name, type FROM sqlite_master
                 WHERE type in ('table', 'view') AND NOT name='sqlite_sequence'
             """)

      assert Capture.pure_read?("SELECT QUOTE(?), QUOTE(?), QUOTE(?)")
      assert Capture.pure_read?("PRAGMA foreign_key_check")
    end

    test "case and leading whitespace do not matter" do
      assert Capture.pure_read?("  \n\t select 1")
      assert Capture.pure_read?("pragma FOREIGN_KEY_CHECK")
      assert Capture.pure_read?("PRAGMA main.table_info(\"finance_account\")")
      assert Capture.pure_read?("PRAGMA quick_check;")
    end

    # The half that matters. Every one of these CHANGES something, and dropping any of them would
    # silently alter what a replayed tenant ends up with.
    test "a PRAGMA that assigns is never a read" do
      refute Capture.pure_read?("PRAGMA foreign_keys = OFF")
      refute Capture.pure_read?("PRAGMA foreign_keys=ON")
      refute Capture.pure_read?("PRAGMA legacy_alter_table = ON")
      refute Capture.pure_read?("PRAGMA user_version = 7")
      refute Capture.pure_read?("PRAGMA journal_mode = WAL")
    end

    # These are the cases where the ALLOWLIST alone is not enough: the name IS allowlisted, and only
    # the assignment check keeps them. The first version of this code tested for `=` before pulling
    # the name out, which made that check unreachable — the allowlist had already rejected
    # `"foreign_keys = OFF"` as a whole string. Breaking the check changed no test, which is how the
    # dead branch was found.
    test "an assignment disqualifies even an allowlisted pragma name" do
      refute Capture.pure_read?("PRAGMA quick_check = 1")
      refute Capture.pure_read?("PRAGMA main.integrity_check = 0")
      refute Capture.pure_read?("PRAGMA table_info = x")
    end

    test "an unrecognized PRAGMA is kept, even with no assignment" do
      # Fails closed: `optimize` runs ANALYZE and `wal_checkpoint` moves the WAL. Neither is in the
      # allowlist, so neither is dropped — and neither would be if Django started sending something
      # new tomorrow.
      refute Capture.pure_read?("PRAGMA optimize")
      refute Capture.pure_read?("PRAGMA wal_checkpoint(TRUNCATE)")
      refute Capture.pure_read?("PRAGMA shrink_memory")
    end

    test "DDL and DML are never reads" do
      refute Capture.pure_read?("CREATE TABLE t (x)")
      refute Capture.pure_read?("INSERT INTO django_migrations (app) VALUES (?)")
      refute Capture.pure_read?("DROP TABLE t")
      refute Capture.pure_read?("ALTER TABLE new__t RENAME TO t")
    end

    # `WITH … INSERT` is valid SQLite, so a `WITH` lead is a data migration wearing a read's
    # clothes. Matching on it would drop real writes.
    test "a WITH lead is not treated as a read" do
      refute Capture.pure_read?("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x")
      refute Capture.pure_read?("WITH x AS (SELECT 1) SELECT * FROM x")
    end
  end

  describe "recording" do
    test "records a version when the migration count rises on commit" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY)", [])
      Capture.append(conn, "INSERT INTO django_migrations (app, name) VALUES ('app', '0001')", [])

      assert {:recorded, 1} = Capture.commit(conn, 1)
      assert Migrator.head() == 1

      assert Migrator.statements(1) == [
               "CREATE TABLE app_thing (id INTEGER PRIMARY KEY)",
               "INSERT INTO django_migrations (app, name) VALUES ('app', '0001')"
             ]
    end

    # Django's schema editor interleaves introspection with the DDL, so a captured version used to
    # carry `SELECT … sqlite_master`, `SELECT QUOTE(?)…` and `PRAGMA foreign_key_check` as if they
    # were migration steps — and every tenant re-ran them. `foreign_key_check` scans every FK'd
    # table, which is free on the empty template and roughly DOUBLES the replay step on a tenant
    # with data (measured 6→13→29→109 ms at 4/19/40/160 MB, vs 4→7→17→57 ms without).
    test "Django's introspection is dropped, and the args stay lined up with what is left" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE app_reads (id INTEGER PRIMARY KEY)", [])
      Capture.append(conn, "SELECT name, type FROM sqlite_master WHERE type='table'", [])
      Capture.append(conn, "PRAGMA foreign_key_check", [])
      Capture.append(conn, "SELECT QUOTE(?), QUOTE(?)", ["q1", "q2"])
      Capture.append(conn, "CREATE INDEX app_reads_id ON app_reads (id)", [])

      Capture.append(
        conn,
        "INSERT INTO django_migrations (app, name, applied) VALUES (?, ?, ?)",
        ["app", "0001_initial", "2026-08-14"]
      )

      assert {:recorded, 1} = Capture.commit(conn, 1)

      assert Migrator.statements(1) == [
               "CREATE TABLE app_reads (id INTEGER PRIMARY KEY)",
               "CREATE INDEX app_reads_id ON app_reads (id)",
               "INSERT INTO django_migrations (app, name, applied) VALUES (?, ?, ?)"
             ]

      # THE invariant. Statements and args are stored as PARALLEL lists and zipped at replay, so
      # filtering the statements without filtering the args in lockstep would hand the bookkeeping
      # INSERT the dropped SELECT's `["q1", "q2"]` — reviving the NULL-binding class of bug that
      # `beff929` fixed, but silently and only for migrations that contain a parameterized read.
      assert Migrator.statement_pairs(1) == [
               {"CREATE TABLE app_reads (id INTEGER PRIMARY KEY)", []},
               {"CREATE INDEX app_reads_id ON app_reads (id)", []},
               {"INSERT INTO django_migrations (app, name, applied) VALUES (?, ?, ?)",
                ["app", "0001_initial", "2026-08-14"]}
             ]
    end

    # A PRAGMA that assigns is a real step: Django wraps table rebuilds in `legacy_alter_table`, and
    # dropping that would change what the rebuild produces on a tenant.
    test "a mutating PRAGMA survives capture" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "PRAGMA legacy_alter_table = ON", [])
      Capture.append(conn, "ALTER TABLE app_old RENAME TO app_new", [])
      Capture.append(conn, "PRAGMA legacy_alter_table = OFF", [])
      Capture.append(conn, "INSERT INTO django_migrations (app) VALUES ('a')", [])

      assert {:recorded, 1} = Capture.commit(conn, 1)

      assert Migrator.statements(1) == [
               "PRAGMA legacy_alter_table = ON",
               "ALTER TABLE app_old RENAME TO app_new",
               "PRAGMA legacy_alter_table = OFF",
               "INSERT INTO django_migrations (app) VALUES ('a')"
             ]
    end

    # Expert review #19: capture was fire-and-forget at the exact moment durability
    # matters — on a failed Release insert (Postgres blip) the buffered statements were
    # already popped from state, and the Django migration has ALREADY committed on the
    # template shard, so re-running `manage.py migrate` is a no-op: the fleet version
    # was permanently uncapturable, silently forking template schema from fleet schema
    # (every later capture assumes DDL the fleet never received). The invariant: a
    # capture survives a control-plane outage and records once it recovers.
    test "a failed release keeps the captured statements and records them on retry" do
      import ExUnit.CaptureLog

      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE app_kept (id INTEGER PRIMARY KEY)", [])

      # The outage: cut the Capture process off from Postgres for the commit.
      Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

      log = capture_log(fn -> assert {:error, _} = Capture.commit(conn, 1) end)
      assert log =~ "failed to record captured version"

      # Postgres recovers; drive the retry tick deterministically (no sleep).
      owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true)
      on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)

      send(Capture, :retry_pending)
      _ = :sys.get_state(Capture)

      assert Migrator.head() == 1, "the pending capture must record once Postgres recovers"
      assert Migrator.statements(1) == ["CREATE TABLE app_kept (id INTEGER PRIMARY KEY)"]
    end

    # Expert review 2026-07-14 #1: a Django RunPython backfill crosses the wire as template-literal
    # INSERT/UPDATE/DELETE on a tenant table and is captured into the fleet version — replayed
    # verbatim it corrupts or silently skips tenant data. The version is still recorded (refusing
    # would fork the template from the fleet), but the previously-SILENT case must now alarm loudly.
    # Pre-fix commit only logs the :info "captured..." line, so the "data-migration" assertion fails.
    test "flags a captured template-literal data migration instead of recording it silently" do
      import ExUnit.CaptureLog

      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, slug TEXT)", [])
      Capture.append(conn, "UPDATE app_thing SET slug = 'from-template' WHERE id = 1", [])
      Capture.append(conn, "INSERT INTO django_migrations (app, name) VALUES ('app', '0002')", [])

      {result, log} = with_log(fn -> Capture.commit(conn, 1) end)
      assert {:recorded, version} = result
      assert log =~ "DATA-MIGRATION"

      # Still RECORDED — refusing would fork the template from the fleet (#19) …
      assert [%{version: ^version, requires_review: true}] = Migrator.list()
      # … but FLAGGED requires_review (expert review #1), so it is held below HEAD (the rollout
      # can't replay the template-literal DML) until an operator reviews it.
      assert version in Enum.map(Migrator.pending_review(), & &1.version)

      assert Migrator.head() == 0,
             "a flagged data migration must not become HEAD until reviewed"
    end

    test "does not flag a pure-DDL migration (only django_migrations bookkeeping DML)" do
      import ExUnit.CaptureLog

      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE app_plain (id INTEGER PRIMARY KEY)", [])
      Capture.append(conn, "INSERT INTO django_migrations (app, name) VALUES ('app', '0003')", [])

      log = capture_log(fn -> assert {:recorded, _} = Capture.commit(conn, 1) end)
      refute log =~ "DATA-MIGRATION"
    end

    # Django's SQLite backend cannot ALTER most things in place, so it REBUILDS the table
    # (`_remake_table`) — and the copy step is an INSERT, which this lint flagged as a
    # template-literal data migration. Consequence: the release was held requires_review, which
    # CAPS the fleet HEAD below it, so the unattended rollout stopped dead on almost every real
    # Django migration (AlterField, AddField-with-default, unique_together, index changes all take
    # the rebuild path). The engine's primary use case was effectively unusable.
    #
    # It was invisible because every existing test above writes SQL by hand. These statements are
    # the VERBATIM capture of a real `manage.py migrate` of a Django 5.0 app whose 0001_initial
    # declares `Meta.indexes` (djathom's finance app, captured live 2026-07-30) — a plain initial
    # migration with no data migration anywhere in it.
    #
    # The invariant: an INSERT whose rows come from a SELECT is a SHARD-LOCAL row copy (a shard is
    # one SQLite file, never ATTACHed to another), so it carries no template values and replays
    # safely. Pre-fix this test fails on the requires_review assertion.
    test "does not flag Django's SQLite table-rebuild copy (INSERT ... SELECT is shard-local)" do
      import ExUnit.CaptureLog

      conn = make_ref()
      Capture.begin(conn, 0)

      Capture.append(
        conn,
        ~s|CREATE TABLE "finance_transaction" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "date" date NOT NULL)|,
        []
      )

      Capture.append(
        conn,
        ~s|CREATE TABLE "new__finance_transaction" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "date" date NOT NULL, "account_id" bigint NOT NULL REFERENCES "finance_account" ("id") DEFERRABLE INITIALLY DEFERRED)|,
        []
      )

      Capture.append(
        conn,
        ~s|INSERT INTO "new__finance_transaction" ("id", "date", "account_id") SELECT "id", "date", NULL FROM "finance_transaction"|,
        []
      )

      Capture.append(conn, ~s|DROP TABLE "finance_transaction"|, [])

      Capture.append(
        conn,
        ~s|ALTER TABLE "new__finance_transaction" RENAME TO "finance_transaction"|,
        []
      )

      Capture.append(
        conn,
        ~s|CREATE INDEX "finance_tra_date_f21d66_idx" ON "finance_transaction" ("date")|,
        []
      )

      Capture.append(
        conn,
        ~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?) RETURNING "django_migrations"."id"|,
        []
      )

      {result, log} = with_log(fn -> Capture.commit(conn, 1) end)
      assert {:recorded, version} = result
      refute log =~ "DATA-MIGRATION"

      assert [%{version: ^version, requires_review: false}] = Migrator.list()
      assert Migrator.pending_review() == []

      assert Migrator.head() == version,
             "an ordinary Django migration must become HEAD — a rebuild copy is not a data migration"
    end

    # Django's SQLite backend must COMMIT before it can re-enable FK checks, so for a migration that
    # disables them — a CreateModel carrying a ForeignKey, for instance — it records the
    # `django_migrations` row AFTER the transaction:
    #
    #   BEGIN / CREATE TABLE ... / CREATE INDEX ... / COMMIT / PRAGMA foreign_keys = ON /
    #   INSERT INTO django_migrations ...
    #
    # The count-rose-by-COMMIT boundary test therefore saw NO rise and returned :noop, so the version
    # was NEVER recorded: the template's schema advanced, the fleet silently never heard, and nothing
    # alarmed — the exact template↔fleet fork this engine exists to prevent, on an ordinary migration
    # (not an `atomic = False` one). The #6 gap detector does not save it: that only fires on a LATER
    # capture, and there may not be one.
    #
    # Found live 2026-07-30 by running a real `manage.py migrate` (djathom finance.0002_budget)
    # through capture — the template ended with both migrations applied while Migrator.list() held
    # only v1. Statements below are that run's shape. Pre-fix this test fails: nothing is recorded.
    test "records a migration whose django_migrations row lands AFTER the commit" do
      import ExUnit.CaptureLog

      conn = make_ref()
      Capture.begin(conn, 0)

      Capture.append(
        conn,
        ~s|CREATE TABLE "finance_budget" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "category_id" bigint NOT NULL REFERENCES "finance_category" ("id"))|,
        []
      )

      Capture.append(
        conn,
        ~s|CREATE INDEX "finance_budget_category_id_3ead498a" ON "finance_budget" ("category_id")|,
        []
      )

      # The count has NOT risen yet — Django has not written the bookkeeping row.
      assert Capture.commit(conn, 0) == :noop
      assert Migrator.list() == [], "nothing is recorded until the migration is marked applied"

      insert = ~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?)|

      {result, log} =
        with_log(fn -> Capture.bookkeeping(conn, insert, [], 1) end)

      assert {:recorded, version} = result
      refute log =~ "DATA-MIGRATION"

      # The bookkeeping INSERT is part of the recorded version, so a replayed shard gets its own
      # django_migrations row — the same shape the in-transaction path produces.
      assert Migrator.statements(version) == [
               ~s|CREATE TABLE "finance_budget" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "category_id" bigint NOT NULL REFERENCES "finance_category" ("id"))|,
               ~s|CREATE INDEX "finance_budget_category_id_3ead498a" ON "finance_budget" ("category_id")|,
               insert
             ]

      assert Migrator.head() == version
      assert Migrator.pending_review() == []
    end

    test "statements between the commit and the bookkeeping row are not captured" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, ~s|CREATE TABLE "t" ("id" integer PRIMARY KEY)|, [])
      assert Capture.commit(conn, 0) == :noop

      # Django sends this between the COMMIT and the bookkeeping row. It is not part of the
      # migration, and replaying it onto every tenant would be noise in the fleet version.
      Capture.append(conn, "PRAGMA foreign_keys = ON", [])

      insert = ~s|INSERT INTO "django_migrations" ("app", "name") VALUES (?, ?)|
      assert {:recorded, version} = Capture.bookkeeping(conn, insert, [], 1)

      statements = Migrator.statements(version)
      refute Enum.any?(statements, &(&1 =~ "foreign_keys"))
      assert statements == [~s|CREATE TABLE "t" ("id" integer PRIMARY KEY)|, insert]
    end

    test "a genuinely empty transaction is still a no-op, not a phantom version" do
      # The awaiting-bookkeeping buffer must not turn every no-rise commit into a release. A new
      # BEGIN supersedes it (the previous transaction never marked a migration applied).
      c1 = make_ref()
      Capture.begin(c1, 0)
      Capture.append(c1, "SELECT 1", [])
      assert Capture.commit(c1, 0) == :noop

      Capture.begin(c1, 0)
      Capture.append(c1, "SELECT 2", [])
      assert Capture.commit(c1, 0) == :noop

      assert Migrator.list() == []
      assert Migrator.head() == 0
    end

    # Expert review 2026-08-31 #18: an awaiting buffer that still holds DDL, dropped by a new BEGIN
    # before its django_migrations bookkeeping row arrives, is the silent-fork condition #6's
    # awaiting mechanism exists to prevent. It is now ALERTABLE (warn + telemetry) rather than
    # invisible — the FK-disabling migrate path depends on the recorder INSERT arriving in
    # autocommit, and a future Django emitting it inside a transaction would silently stop recording
    # versions.
    test "a new BEGIN dropping awaiting DDL warns and emits telemetry (#18)" do
      import ExUnit.CaptureLog
      test_pid = self()
      handler = "capture-dropped-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :migrator, :capture_awaiting_dropped],
        fn _e, m, _md, _ -> send(test_pid, {:dropped, m.count}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      conn = make_ref()
      # BEGIN / DDL / COMMIT with NO count rise → the FK-disabling shape parks the DDL awaiting.
      Capture.begin(conn, 0)
      Capture.append(conn, ~s|CREATE TABLE "t" ("id" integer PRIMARY KEY)|, [])
      assert Capture.commit(conn, 0) == :noop

      # A new BEGIN arrives before the bookkeeping row — the awaiting DDL is dropped.
      log =
        capture_log(fn ->
          Capture.begin(conn, 0)
          _ = :sys.get_state(Capture)
        end)

      assert log =~ "AWAITING their django_migrations bookkeeping row"
      assert_receive {:dropped, 1}
    end

    # The other half: a genuinely empty transaction (only pure reads buffered) superseded by a new
    # BEGIN is the ordinary no-op and must NOT warn — a false alarm there defeats the point.
    test "a new BEGIN dropping a pure-read awaiting buffer does NOT warn (#18)" do
      import ExUnit.CaptureLog
      test_pid = self()
      handler = "capture-noread-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :migrator, :capture_awaiting_dropped],
        fn _e, m, _md, _ -> send(test_pid, {:dropped, m.count}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "SELECT 1", [])
      assert Capture.commit(conn, 0) == :noop

      log =
        capture_log(fn ->
          Capture.begin(conn, 0)
          _ = :sys.get_state(Capture)
        end)

      refute log =~ "AWAITING"
      refute_receive {:dropped, _}, 50
    end

    test "a second bookkeeping row does not re-record the same version" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, ~s|CREATE TABLE "t" ("id" integer PRIMARY KEY)|, [])
      assert Capture.commit(conn, 0) == :noop

      insert = ~s|INSERT INTO "django_migrations" ("app", "name") VALUES (?, ?)|
      assert {:recorded, _} = Capture.bookkeeping(conn, insert, [], 1)

      # The buffer is consumed; a repeat (or Django's follow-up SELECT-then-INSERT) records nothing.
      assert Capture.bookkeeping(conn, insert, [], 2) == :noop

      assert length(Migrator.list()) == 1
    end

    # Django sends parameterized SQL, and capture used to record the statement TEXT only — so the
    # values were dropped on the floor and replay bound NULL, killing every rollout (see
    # Fathom.Migrator.CopyTest). Pin the whole round trip: args survive capture → jsonb → decode,
    # including a blob (which plain JSON cannot carry) and a nil.
    test "captured bind values round-trip through storage" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, blob BLOB)", [])

      Capture.append(conn, "INSERT INTO app_thing (id, blob) SELECT ?, ? FROM app_thing", [
        7,
        {:blob, <<0, 255>>}
      ])

      insert = ~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?)|
      args = ["finance", "0002_budget", nil]

      # In-transaction shape: the bookkeeping row is buffered and the COMMIT records the version.
      assert Capture.bookkeeping(conn, insert, args, 1) == :noop
      assert {:recorded, version} = Capture.commit(conn, 1)

      pairs = Migrator.statement_pairs(version)

      assert [
               {"CREATE TABLE app_thing (id INTEGER PRIMARY KEY, blob BLOB)", []},
               {"INSERT INTO app_thing (id, blob) SELECT ?, ? FROM app_thing",
                [7, {:blob, <<0, 255>>}]},
               {^insert, ["finance", "0002_budget", nil]}
             ] = pairs

      # statements/1 keeps its old text-only contract for callers that only want the SQL.
      assert Migrator.statements(version) == Enum.map(pairs, &elem(&1, 0))
    end

    test "a release stored without args replays with none (pre-feature rows keep working)" do
      {:ok, _} = Migrator.release(3, "hand-authored", ["CREATE TABLE t (id INTEGER)"])

      assert Migrator.statement_pairs(3) == [{"CREATE TABLE t (id INTEGER)", []}]
    end

    test "bookkeeping?/1 matches Django's applied-migration INSERT only" do
      assert Capture.bookkeeping?(~s|INSERT INTO "django_migrations" ("app") VALUES (?)|)
      assert Capture.bookkeeping?("insert into django_migrations (app) values (?)")

      # A backwards migrate DELETEs the row — commit/3's shrink branch alarms on that; it must not
      # be mistaken for a migration being applied.
      refute Capture.bookkeeping?(~s|DELETE FROM "django_migrations" WHERE "id" = 1|)
      refute Capture.bookkeeping?(~s|INSERT INTO "finance_budget" ("id") VALUES (1)|)
      refute Capture.bookkeeping?(~s|SELECT * FROM "django_migrations"|)
      refute Capture.bookkeeping?(nil)
    end

    # The exemption must not become a hole: a RunPython backfill that happens to read a table is
    # still template data crossing the wire, and anything with a VALUES row-source stays flagged.
    test "still flags real DML: VALUES inserts, UPDATE, and DELETE on tenant tables" do
      import ExUnit.CaptureLog

      for dml <- [
            ~s|INSERT INTO "app_thing" ("slug") VALUES ('from-template')|,
            ~s|INSERT INTO "app_thing" ("slug") VALUES ((SELECT "slug" FROM "app_other"))|,
            ~s|UPDATE "app_thing" SET "slug" = 'from-template' WHERE "id" = 1|,
            ~s|DELETE FROM "app_thing" WHERE "id" = 1|,
            ~s|REPLACE INTO "app_thing" ("id", "slug") VALUES (1, 'x')|
          ] do
        conn = make_ref()
        Capture.begin(conn, 0)

        Capture.append(
          conn,
          ~s|CREATE TABLE "app_thing" ("id" integer PRIMARY KEY, "slug" text)|,
          []
        )

        Capture.append(conn, dml, [])

        Capture.append(
          conn,
          ~s|INSERT INTO django_migrations (app, name) VALUES ('app', '0002')|,
          []
        )

        {result, log} = with_log(fn -> Capture.commit(conn, 1) end)
        assert {:recorded, version} = result

        assert log =~ "DATA-MIGRATION", "expected #{dml} to be flagged"

        assert version in Enum.map(Migrator.pending_review(), & &1.version),
               "expected #{dml} to be held for review"
      end
    end

    # Expert review #6: a non-atomic (`atomic = False`) migration runs autocommit — no tracked
    # BEGIN/COMMIT — so capture never sees it, the template schema moves, and the fleet never hears.
    # Caught at the NEXT capture: its pre-transaction count exceeds the last captured count (the gap).
    # Detected from DURABLE per-release counts (the shared-singleton false-alarm the earlier attempt
    # hit is gone). The gap version is flagged requires_review so the rollout freezes below it.
    test "a non-atomic-migration gap (pre-count exceeds the last captured count) is flagged" do
      import ExUnit.CaptureLog

      # First capture: the template goes 14 → 15, recorded as v1 (count 15), pure DDL.
      c1 = make_ref()
      Capture.begin(c1, 14)
      Capture.append(c1, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)", [])
      Capture.append(c1, "INSERT INTO django_migrations (app, name) VALUES ('app', '0001')", [])
      assert {:recorded, 1} = Capture.commit(c1, 15)

      # An `atomic = False` migration ran on the template OUTSIDE a tracked transaction (count is
      # now 16, uncaptured). The NEXT captured migration begins at 16 (> the last captured 15).
      c2 = make_ref()
      Capture.begin(c2, 16)
      Capture.append(c2, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)", [])
      Capture.append(c2, "INSERT INTO django_migrations (app, name) VALUES ('app', '0003')", [])

      test_pid = self()
      handler = "gap-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :migrator, :migration_gap],
        fn _e, meas, meta, _cfg -> send(test_pid, {:gap, meas, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      {result, log} = with_log(fn -> Capture.commit(c2, 17) end)
      assert {:recorded, version} = result
      assert log =~ "OUTSIDE capture"
      assert_received {:gap, %{gap: 1}, %{version: ^version}}

      # Flagged requires_review → HEAD frozen at the last SAFE version (v1) until reviewed.
      assert version in Enum.map(Migrator.pending_review(), & &1.version)
      assert Migrator.head() == 1
    end

    # Expert review 2026-07-14 #6: a backwards migrate deletes a django_migrations row (count falls),
    # which the count-rose boundary silently treated as :noop — the template schema moved and the
    # fleet never heard. It must alarm now. Fresh Capture instance so the :baseline is isolated.
    test "a backwards Django migrate (django_migrations shrinks) is flagged, not silently ignored" do
      import ExUnit.CaptureLog

      cap = start_supervised!({Capture, name: :"cap_back_#{System.unique_integer([:positive])}"})
      conn = make_ref()
      Capture.begin(conn, 1, cap)

      Capture.append(
        conn,
        "DELETE FROM django_migrations WHERE app='app' AND name='0002'",
        [],
        cap
      )

      log = capture_log(fn -> assert :noop = Capture.commit(conn, 0, cap) end)
      assert log =~ "BACKWARDS"
    end

    test "records nothing when the count doesn't rise" do
      conn = make_ref()
      Capture.begin(conn, 5)
      Capture.append(conn, "SELECT 1", [])

      assert Capture.commit(conn, 5) == :noop
      assert Migrator.head() == 0
    end

    test "rollback discards the buffered transaction" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE oops (x)", [])
      Capture.rollback(conn)

      assert Capture.commit(conn, 1) == :noop
      assert Migrator.head() == 0
    end

    test "successive migrations get successive versions" do
      conn = make_ref()

      Capture.begin(conn, 0)
      Capture.append(conn, "stmt-a", [])
      assert {:recorded, 1} = Capture.commit(conn, 1)

      Capture.begin(conn, 1)
      Capture.append(conn, "stmt-b", [])
      assert {:recorded, 2} = Capture.commit(conn, 2)

      assert Migrator.head() == 2
      assert Migrator.statements(1) == ["stmt-a"]
      assert Migrator.statements(2) == ["stmt-b"]
    end

    test "a commit with no tracked transaction is a no-op" do
      assert Capture.commit(make_ref(), 9) == :noop
    end

    # Expert review #10: `head/0` excludes yanked releases (so a revert sticks, #12),
    # but capture allocated `head() + 1` — after any yank the yanked row still
    # occupies its version number, so the next capture collided on the unique
    # version index FOREVER: the 5 s retry recomputed the same colliding number,
    # `drain_pending` stopped at the first failure, and every later capture wedged
    # behind it (template schema silently forks from fleet schema, the #19 failure,
    # via the routine revert-yank path). The invariant: a yanked version is a
    # tombstone, not a free slot — capture allocates PAST it (next_version/0).
    test "capture after a yank allocates past the yanked version, not onto it" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "stmt-v1", [])
      assert {:recorded, 1} = Capture.commit(conn, 1)

      assert :ok = Migrator.yank(1)
      assert Migrator.head() == 0, "yank must drop the version from HEAD"

      Capture.begin(conn, 1)
      Capture.append(conn, "stmt-v2", [])
      assert {:recorded, 2} = Capture.commit(conn, 2)

      assert Migrator.head() == 2
      assert Migrator.statements(2) == ["stmt-v2"]
      assert Migrator.statements(1) == nil, "the yanked version stays unappliable"
    end
  end
end
