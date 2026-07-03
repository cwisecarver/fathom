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
  end
end
