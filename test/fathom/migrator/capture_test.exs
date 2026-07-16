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

  describe "recording" do
    test "records a version when the migration count rises on commit" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY)")
      Capture.append(conn, "INSERT INTO django_migrations (app, name) VALUES ('app', '0001')")

      assert {:recorded, 1} = Capture.commit(conn, 1)
      assert Migrator.head() == 1

      assert Migrator.statements(1) == [
               "CREATE TABLE app_thing (id INTEGER PRIMARY KEY)",
               "INSERT INTO django_migrations (app, name) VALUES ('app', '0001')"
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
      Capture.append(conn, "CREATE TABLE app_kept (id INTEGER PRIMARY KEY)")

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
      Capture.append(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, slug TEXT)")
      Capture.append(conn, "UPDATE app_thing SET slug = 'from-template' WHERE id = 1")
      Capture.append(conn, "INSERT INTO django_migrations (app, name) VALUES ('app', '0002')")

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
      Capture.append(conn, "CREATE TABLE app_plain (id INTEGER PRIMARY KEY)")
      Capture.append(conn, "INSERT INTO django_migrations (app, name) VALUES ('app', '0003')")

      log = capture_log(fn -> assert {:recorded, _} = Capture.commit(conn, 1) end)
      refute log =~ "DATA-MIGRATION"
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
      Capture.append(c1, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
      Capture.append(c1, "INSERT INTO django_migrations (app, name) VALUES ('app', '0001')")
      assert {:recorded, 1} = Capture.commit(c1, 15)

      # An `atomic = False` migration ran on the template OUTSIDE a tracked transaction (count is
      # now 16, uncaptured). The NEXT captured migration begins at 16 (> the last captured 15).
      c2 = make_ref()
      Capture.begin(c2, 16)
      Capture.append(c2, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
      Capture.append(c2, "INSERT INTO django_migrations (app, name) VALUES ('app', '0003')")

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
      Capture.append(conn, "DELETE FROM django_migrations WHERE app='app' AND name='0002'", cap)

      log = capture_log(fn -> assert :noop = Capture.commit(conn, 0, cap) end)
      assert log =~ "BACKWARDS"
    end

    test "records nothing when the count doesn't rise" do
      conn = make_ref()
      Capture.begin(conn, 5)
      Capture.append(conn, "SELECT 1")

      assert Capture.commit(conn, 5) == :noop
      assert Migrator.head() == 0
    end

    test "rollback discards the buffered transaction" do
      conn = make_ref()
      Capture.begin(conn, 0)
      Capture.append(conn, "CREATE TABLE oops (x)")
      Capture.rollback(conn)

      assert Capture.commit(conn, 1) == :noop
      assert Migrator.head() == 0
    end

    test "successive migrations get successive versions" do
      conn = make_ref()

      Capture.begin(conn, 0)
      Capture.append(conn, "stmt-a")
      assert {:recorded, 1} = Capture.commit(conn, 1)

      Capture.begin(conn, 1)
      Capture.append(conn, "stmt-b")
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
      Capture.append(conn, "stmt-v1")
      assert {:recorded, 1} = Capture.commit(conn, 1)

      assert :ok = Migrator.yank(1)
      assert Migrator.head() == 0, "yank must drop the version from HEAD"

      Capture.begin(conn, 1)
      Capture.append(conn, "stmt-v2")
      assert {:recorded, 2} = Capture.commit(conn, 2)

      assert Migrator.head() == 2
      assert Migrator.statements(2) == ["stmt-v2"]
      assert Migrator.statements(1) == nil, "the yanked version stays unappliable"
    end
  end
end
