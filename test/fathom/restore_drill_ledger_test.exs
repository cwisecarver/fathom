defmodule Fathom.RestoreDrillLedgerTest do
  @moduledoc """
  The third leg of the three-place version stamp (AGENTS.md / todo #25): `django_migrations` (Django's
  own ledger — the TRUTH) vs `PRAGMA user_version` (the O(1) gate). `verify/2` already checks
  user_version vs the Postgres directory; nothing checked the ledger against user_version, so a shard
  whose fast gate drifted from what Django actually applied would be trusted anyway.

  These exercise `RestoreDrillJob.ledger_status_for_path/2` on REAL crafted SQLite files (a shard is a
  real file, never the Repo sandbox — AGENTS Testing), with the release registry in Postgres
  (DataCase). The mapping version → expected count is `Migrator.Release.template_migration_count`,
  looked up by `Migrator.expected_migration_count/1`.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Migrator
  alias Fathom.Migrator.Release
  alias Fathom.RestoreDrillJob
  alias Fathom.Shard.Connection

  # A real SQLite file stamped `user_version = v`, holding `ledger_rows` django_migrations rows.
  # `ledger_rows: nil` means: do NOT create the django_migrations table at all (a born-empty shard).
  # Registers its own cleanup so each test drops exactly the files it made.
  defp make_shard_file(user_version, ledger_rows) do
    path =
      Path.join(
        System.tmp_dir!(),
        "ledgertest_#{System.unique_integer([:positive])}.db"
      )

    on_exit(fn ->
      for suffix <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> suffix)
    end)

    {:ok, conn} = Connection.open(path)

    try do
      if is_integer(ledger_rows) do
        {:ok, _} =
          Connection.query(
            conn,
            "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT NOT NULL, name TEXT NOT NULL, applied TEXT NOT NULL)",
            []
          )

        for i <- 1..ledger_rows//1 do
          {:ok, _} =
            Connection.query(
              conn,
              "INSERT INTO django_migrations (app, name, applied) VALUES (?, ?, ?)",
              ["app", "0#{i}_migration", "2026-01-01"]
            )
        end
      else
        # A non-django table so the file is a valid, non-empty SQLite db with NO ledger.
        {:ok, _} = Connection.query(conn, "CREATE TABLE widgets (id INTEGER PRIMARY KEY)", [])
      end

      {:ok, _} = Connection.query(conn, "PRAGMA user_version = #{user_version}", [])
    after
      Connection.close(conn)
    end

    path
  end

  defp release!(version, template_migration_count) do
    %Release{}
    |> Release.changeset(%{
      version: version,
      name: "v#{version}",
      template_migration_count: template_migration_count
    })
    |> Repo.insert!()
  end

  describe "expected_migration_count/1" do
    test "returns the release's template_migration_count" do
      release!(3, 7)
      assert {:ok, 7} = Migrator.expected_migration_count(3)
    end

    test ":unknown when the version has no release row" do
      assert :unknown = Migrator.expected_migration_count(99)
    end

    test ":unknown when the release predates template_migration_count (NULL)" do
      release!(4, nil)
      assert :unknown = Migrator.expected_migration_count(4)
    end
  end

  describe "ledger_status_for_path/2" do
    test "ledger count matches the version's expected count -> :ok" do
      release!(5, 12)
      path = make_shard_file(5, 12)
      assert :ok = RestoreDrillJob.ledger_status_for_path("s", path)
    end

    test "ledger BEHIND the stamp -> :ledger_mismatch" do
      # user_version says v5 (expects 12 migrations) but only 9 were actually applied: the fast gate
      # is ahead of what Django really did.
      release!(5, 12)
      path = make_shard_file(5, 9)
      assert :ledger_mismatch = RestoreDrillJob.ledger_status_for_path("s", path)
    end

    test "ledger AHEAD of the stamp -> :ledger_mismatch" do
      # 15 migrations applied but user_version still says v5 (expects 12): the gate lagged the ledger.
      release!(5, 12)
      path = make_shard_file(5, 15)
      assert :ledger_mismatch = RestoreDrillJob.ledger_status_for_path("s", path)
    end

    test "stamped as migrated but NO django_migrations table -> :ledger_mismatch" do
      # A missing ledger counts as zero rows: user_version=5 expects 12, the ledger vanished. That is
      # a real mismatch (its truth is gone), NOT a benign absence.
      release!(5, 12)
      path = make_shard_file(5, nil)
      assert :ledger_mismatch = RestoreDrillJob.ledger_status_for_path("s", path)
    end

    test "born-empty shard (version 0, no ledger) -> :ok" do
      # user_version 0 has no release expectation, so zero rows / no table is correct — this is the
      # ordinary state of every non-Django and never-migrated shard, and must not false-alarm.
      path = make_shard_file(0, nil)
      assert :ok = RestoreDrillJob.ledger_status_for_path("s", path)
    end

    test "unreleased version (no row) -> :ok, never a manufactured mismatch" do
      # A version the registry doesn't know: nothing to compare, so the drill must NOT invent a
      # failure from a gap in its own registry.
      path = make_shard_file(7, 3)
      assert :ok = RestoreDrillJob.ledger_status_for_path("s", path)
    end

    test "release with NULL template_migration_count -> :ok (nothing to compare)" do
      release!(6, nil)
      path = make_shard_file(6, 42)
      assert :ok = RestoreDrillJob.ledger_status_for_path("s", path)
    end
  end
end
