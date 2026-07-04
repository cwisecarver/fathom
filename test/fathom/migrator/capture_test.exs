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
