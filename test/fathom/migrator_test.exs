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

  # Expert review 2026-07-24 #20: four hand-written queries filter Oban jobs by the shard id inside
  # `args`, and none could use an index — Oban's own GIN index on `args` is jsonb_ops, which serves
  # containment (@>) but NOT the `->>` extraction these use, and there was no index on `worker`
  # either. During a rollout the live job set is hundreds of thousands of rows, and
  # `enqueue_unique_chunk/1` runs this once per 5,000-shard chunk.
  #
  # A partial expression index is only worth anything if the planner can actually match it, and on a
  # small table a seq scan wins regardless — so force the choice and read the plan, exactly as the
  # shards status indexes are pinned.
  describe "oban_jobs shard-id index (#20)" do
    test "the per-shard job dedup query can use the expression index" do
      %{rows: [[exists]]} =
        Fathom.Repo.query!(
          "SELECT count(*) FROM pg_indexes WHERE tablename = 'oban_jobs' AND indexname = $1",
          ["oban_jobs_worker_shard_id_live_index"]
        )

      assert exists == 1, "the index is missing — the migration did not apply"

      Fathom.Repo.query!("SET LOCAL enable_seqscan = off")

      %{rows: rows} =
        Fathom.Repo.query!(
          """
          EXPLAIN SELECT j0.args->>'shard_id' FROM oban_jobs AS j0
          WHERE j0.worker = $1 AND j0.state = ANY($2) AND (j0.args->>'shard_id') = ANY($3)
          """,
          [
            "Fathom.Migrator.ShardMigrationJob",
            ~w(scheduled available executing retryable suspended),
            ["acme"]
          ]
        )

      plan = rows |> List.flatten() |> Enum.join("\n")

      assert plan =~ "oban_jobs_worker_shard_id_live_index",
             "the dedup query cannot use the index, so it still scans every live job row " <>
               "per rollout chunk. Plan:\n#{plan}"
    end
  end
end
