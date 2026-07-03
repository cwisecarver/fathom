defmodule Fathom.Migrator.RolloutTest do
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Migrator
  alias Fathom.Migrator.{ReconcileJob, RevertJob, ShardMigration, ShardMigrationJob}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Directory

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  describe "rollout/1" do
    test "enqueues a migration job for each active shard behind HEAD" do
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      {:ok, _} = Directory.resolve("a")
      {:ok, _} = Directory.resolve("b")
      {:ok, _} = Directory.resolve("c")
      {:ok, _} = Directory.cutover("c", 2)

      assert {:ok, 2} = Migrator.rollout()
      assert_enqueued(migration_job_args("a"))
      assert_enqueued(migration_job_args("b"))
      refute_enqueued(worker: Fathom.Migrator.ShardMigrationJob, args: %{"shard_id" => "c"})
    end

    test "is a no-op when no version is released (HEAD 0)" do
      {:ok, _} = Directory.resolve("a")
      assert {:ok, 0} = Migrator.rollout()
      refute_enqueued(worker: Fathom.Migrator.ShardMigrationJob)
    end

    # Finding #21: the sweep bulk-enqueues with Oban.insert_all, whose basic-engine variant
    # does NOT honor the worker's per-shard :unique. Without the manual dedup, every hourly
    # reconcile would re-enqueue already-queued shards and pile up jobs. Pin idempotency.
    test "is idempotent: a second sweep re-enqueues nothing while jobs are in flight" do
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      {:ok, _} = Directory.resolve("a")
      {:ok, _} = Directory.resolve("b")

      assert {:ok, 2} = Migrator.rollout()
      assert {:ok, 0} = Migrator.rollout()

      assert length(all_enqueued(worker: ShardMigrationJob)) == 2
    end

    # Found by scripts/directory_scale.exs at 3.1M directory rows: Postgres caps a statement
    # at 65,535 bind parameters, so one unpartitioned Oban.insert_all crashed past ~7,281
    # jobs (9 params each) — which a fleet revert (unbounded: every shard at a version) or a
    # rollout limit above that hits at real fleet size. enqueue_unique must chunk both the
    # dedup pre-check and the insert.
    test "a sweep bigger than the bind-parameter cap enqueues in chunks, not one statement" do
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      rows =
        for i <- 1..8_000 do
          %{
            shard_id: "bulk#{i}",
            schema_version: 0,
            status: "active",
            last_active_at: now,
            inserted_at: now,
            updated_at: now
          }
        end

      rows |> Enum.chunk_every(4_000) |> Enum.each(&Repo.insert_all("shards", &1))

      assert {:ok, 8_000} = Migrator.rollout(8_000)
      assert length(all_enqueued(worker: ShardMigrationJob)) == 8_000
    end
  end

  describe "reconcile" do
    test "re-runs the rollout sweep" do
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      {:ok, _} = Directory.resolve("a")

      assert :ok = perform_job(ReconcileJob, %{})
      assert_enqueued(migration_job_args("a"))
    end
  end

  describe "revert/2" do
    test "enqueues a revert job for each active shard at the from-version" do
      for id <- ~w(a b) do
        {:ok, _} = Directory.resolve(id)
        {:ok, _} = Directory.cutover(id, 2)
      end

      {:ok, _} = Directory.resolve("c")

      assert {:ok, 2} = Migrator.revert(2, 1)
      assert_enqueued(worker: RevertJob, args: %{"shard_id" => "a", "to_version" => 1})
      assert_enqueued(worker: RevertJob, args: %{"shard_id" => "b", "to_version" => 1})
      refute_enqueued(worker: RevertJob, args: %{"shard_id" => "c"})
    end

    # Finding #21: same insert_all uniqueness caveat as rollout — a re-issued revert must not
    # duplicate in-flight revert jobs.
    test "is idempotent: a second revert re-enqueues nothing while jobs are in flight" do
      for id <- ~w(a b) do
        {:ok, _} = Directory.resolve(id)
        {:ok, _} = Directory.cutover(id, 2)
      end

      assert {:ok, 2} = Migrator.revert(2, 1)
      assert {:ok, 0} = Migrator.revert(2, 1)

      assert length(all_enqueued(worker: RevertJob)) == 2
    end

    # Expert review #23: both dedup layers keyed on shard_id only, ignoring `force`. The
    # intended operator flow — non-force sweep, the guard cancels shards with post-cutover
    # writes, re-issue with force: true — silently dropped any shard whose first RevertJob
    # was still in flight (snoozing on :shard_busy / {:held, _}): the force sweep skipped
    # it, the surviving non-force job hit the guard and cancelled, and the shard was never
    # reverted despite the explicit force. The invariant: a forced re-issue reaches every
    # shard, upgrading in-flight jobs' args instead of skipping them.
    test "a forced re-issue upgrades in-flight revert jobs instead of skipping them" do
      for id <- ~w(a b) do
        {:ok, _} = Directory.resolve(id)
        {:ok, _} = Directory.cutover(id, 2)
      end

      assert {:ok, 2} = Migrator.revert(2, 1)

      refute Enum.any?(all_enqueued(worker: RevertJob), &(&1.args["force"] == true))

      # Pre-fix this returned {:ok, 0} and left both in-flight jobs non-force.
      assert {:ok, 2} = Migrator.revert(2, 1, force: true)

      jobs = all_enqueued(worker: RevertJob)
      assert length(jobs) == 2

      assert Enum.all?(jobs, &(&1.args["force"] == true)),
             "in-flight revert jobs must be upgraded to force: true"
    end

    test "RevertJob reverts a migrated shard back to the prior version" do
      shard = "revert_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        for p <- Path.wildcard(Path.join(@remote_dir, "#{shard}*")), do: File.rm(p)
      end)

      seed_v1!(shard)
      {:ok, _} = Migrator.release(2, "v2", ["ALTER TABLE app_thing ADD COLUMN x TEXT"])
      {:ok, _} = ShardMigration.run(shard, 2)
      assert {:ok, %{schema_version: 2}} = Directory.get(shard)

      assert :ok = perform_job(RevertJob, %{"shard_id" => shard, "to_version" => 1})
      assert {:ok, %{schema_version: 1}} = Directory.get(shard)
    end
  end

  defp migration_job_args(shard) do
    [worker: Fathom.Migrator.ShardMigrationJob, args: %{"shard_id" => shard, "target" => 2}]
  end

  defp seed_v1!(shard) do
    seed = Path.join(System.tmp_dir!(), "seedr_#{shard}.db")
    {:ok, conn} = Connection.open(seed)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY)")
    :ok = Connection.exec(conn, "INSERT INTO app_thing (id) VALUES (1)")
    :ok = Connection.exec(conn, "PRAGMA user_version = 1")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)
    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, 1)
    :ok
  end
end
