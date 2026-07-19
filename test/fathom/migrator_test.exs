defmodule Fathom.MigratorTest do
  use Fathom.DataCase, async: true

  alias Fathom.Migrator

  describe "release/2 and head/0" do
    test "head is 0 before anything is released" do
      assert Migrator.head() == 0
    end

    test "release records a version and head tracks the max" do
      assert {:ok, _} = Migrator.release(1, "initial schema")
      assert {:ok, _} = Migrator.release(2, "add email index")
      assert Migrator.head() == 2
    end

    test "a duplicate version is rejected" do
      {:ok, _} = Migrator.release(1, "initial")
      assert {:error, changeset} = Migrator.release(1, "again")
      refute changeset.valid?
    end

    test "version must be positive" do
      assert {:error, changeset} = Migrator.release(0, "bad")
      refute changeset.valid?
    end
  end

  describe "list/0" do
    test "returns releases oldest first" do
      {:ok, _} = Migrator.release(2, "second")
      {:ok, _} = Migrator.release(1, "first")
      assert Enum.map(Migrator.list(), & &1.version) == [1, 2]
    end
  end

  describe "statements" do
    test "release stores the captured SQL and statements/1 returns it" do
      sql = [
        "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002', 'now')"
      ]

      {:ok, _} = Migrator.release(2, "add created_at", sql)
      assert Migrator.statements(2) == sql
    end

    test "release without statements defaults to an empty list" do
      {:ok, _} = Migrator.release(1, "initial")
      assert Migrator.statements(1) == []
    end

    test "statements/1 is nil for an unreleased version" do
      assert Migrator.statements(99) == nil
    end

    # Expert review 2026-07-18 #10: `requires_review` was enforced only in head/0 (a rollout
    # ceiling), not in statements/1, so a DIRECT ShardMigration.run/enqueue_migration at a flagged
    # version bypassed the review floor and replayed the flagged DML. statements/1 must refuse it
    # the way it refuses yanked — a structural gate — and lift once approved.
    test "statements/1 refuses a requires_review version until it is approved" do
      sql = ["INSERT INTO app_thing (name) SELECT email FROM auth_user"]
      {:ok, _} = Migrator.release(2, "data backfill", sql, nil, true)

      # head/0 ceilings the automated rollout below it ...
      assert Migrator.head() == 0
      # ... and statements/1 refuses the direct replay path (the bypass this closes).
      assert Migrator.statements(2) == nil

      # Once an operator clears the flag, the version is replayable again.
      assert :ok = Migrator.approve_review(2)
      assert Migrator.statements(2) == sql
      assert Migrator.head() == 2
    end
  end
end
