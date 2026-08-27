defmodule Fathom.Migrator.RolloutTest do
  use Fathom.DataCase, async: false

  # These build fixtures by releasing versions with a capture template configured, which is
  # exactly the configuration `Migrator.release/6` warns about (novel tenants born empty).
  # The warning is correct here and not what these tests are about, so capture it: ExUnit
  # still prints captured logs when a test FAILS, so this hides noise without hiding signal.
  @moduletag :capture_log
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Migrator
  alias Fathom.Migrator.{ReconcileJob, RevertJob, ShardMigration, ShardMigrationJob}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Directory

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

    # Expert review 2026-07-14 #8: the capture template (config :template_shard_id) is migrated
    # directly by Django, so its directory stamp never advances and it perpetually reads as a
    # laggard. Left in the sweep, the reconcile job drains it + replays its OWN captured DDL onto
    # itself → quarantine (and a drain racing an in-flight migrate can fork the fleet). It must be
    # excluded. Pre-fix rollout enqueues it too ({:ok, 2}, and refute below fails).
    test "the capture template is excluded from the rollout sweep" do
      prev = Application.get_env(:fathom, :template_shard_id)
      Application.put_env(:fathom, :template_shard_id, "tmpl_ex")

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, :template_shard_id, prev),
          else: Application.delete_env(:fathom, :template_shard_id)
      end)

      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      {:ok, _} = Directory.resolve("tmpl_ex")
      {:ok, _} = Directory.resolve("tenant_x")

      assert {:ok, 1} = Migrator.rollout()
      assert_enqueued(migration_job_args("tenant_x"))
      refute_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => "tmpl_ex"})
      assert Directory.count_laggards(2) == 1
    end

    test "is a no-op when no version is released (HEAD 0)" do
      {:ok, _} = Directory.resolve("a")
      assert {:ok, 0} = Migrator.rollout()
      refute_enqueued(worker: Fathom.Migrator.ShardMigrationJob)
    end

    # Expert review 2026-07-18 #19: the hourly ReconcileJob swept a hardcoded 100 shards/run, so a
    # deep cold tail took months to converge. The cap is now :reconcile_batch_size (default 100) so
    # an operator can size convergence to the fleet.
    test "the reconcile sweep honors :reconcile_batch_size" do
      prev = Application.get_env(:fathom, :reconcile_batch_size)
      Application.put_env(:fathom, :reconcile_batch_size, 2)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, :reconcile_batch_size, prev),
          else: Application.delete_env(:fathom, :reconcile_batch_size)
      end)

      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      for id <- ~w(rb_a rb_b rb_c rb_d), do: Directory.resolve(id)

      assert :ok = perform_job(ReconcileJob, %{})

      # Only the configured 2 are enqueued this run, not all four laggards.
      assert length(all_enqueued(worker: ShardMigrationJob)) == 2
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

  # Expert review #12: a revert flipped shard pointers back but HEAD never dropped
  # (max(version) over Release rows), so every reverted shard was immediately a laggard
  # — the hourly ReconcileJob re-enqueued migrations to the same bad version within the
  # hour, and lazy migrate re-applied it on the next checkout. Revert was effectively
  # unusable without hand-deleting the shard_migrations row. The invariant: a fleet
  # revert yanks the release — HEAD drops, the sweep re-applies nothing, and the yanked
  # version's statements can never be replayed again.
  describe "yank/1" do
    test "a fleet revert yanks the release so the sweep cannot re-apply it" do
      {:ok, _} = Migrator.release(1, "good", ["SELECT 1"])
      {:ok, _} = Migrator.release(2, "bad", ["SELECT 1"])

      for id <- ~w(a b) do
        {:ok, _} = Directory.resolve(id)
        {:ok, _} = Directory.cutover(id, 2)
      end

      assert Migrator.head() == 2
      assert {:ok, 2} = Migrator.revert(2, 1)

      assert Migrator.head() == 1, "the revert must yank the from-version out of HEAD"
      assert Migrator.statements(2) == nil, "a yanked version must never be appliable again"

      # The shards flip back (the revert jobs' effect, applied here directly)…
      for id <- ~w(a b), do: {:ok, _} = Directory.cutover(id, 1)

      # …and the hourly sweep must NOT re-enqueue forward migrations to the yanked v2.
      assert {:ok, 0} = Migrator.rollout()
      refute_enqueued(worker: ShardMigrationJob)
    end

    test "yank cancels pending forward jobs targeting the version" do
      {:ok, _} = Migrator.release(2, "bad", ["SELECT 1"])
      {:ok, _} = Directory.resolve("c")
      assert {:ok, 1} = Migrator.rollout()
      assert_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => "c", "target" => 2})

      assert :ok = Migrator.yank(2)

      refute_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => "c", "target" => 2})
      assert Migrator.yank(9) == {:error, :unknown_version}
    end

    test "revert with yank: false keeps the release live" do
      {:ok, _} = Migrator.release(2, "canary", ["SELECT 1"])
      {:ok, _} = Directory.resolve("d")
      {:ok, _} = Directory.cutover("d", 2)

      assert {:ok, 1} = Migrator.revert(2, 1, yank: false)
      assert Migrator.head() == 2, "a canary revert must be able to keep the release live"
    end

    # Round-2 #22: yank excluded executing/suspended, so a job mid-copy (statements
    # already fetched) survived the yank, fenced, and cut its shard over to the
    # yanked version AFTER revert/3 read shards_at_version — stranding it above HEAD
    # where no laggard sweep converges it. Cancelling an executing job kills it
    # safely: the copy's `after` releases the lease and no cutover has happened.
    test "yank cancels EXECUTING forward jobs targeting the version" do
      {:ok, _} = Migrator.release(2, "bad", ["SELECT 1"])
      {:ok, _} = Directory.resolve("e")
      assert {:ok, 1} = Migrator.rollout()

      # The job dequeues (executing) just before the yank.
      {1, _} =
        Repo.update_all(
          from(j in Oban.Job,
            where: j.worker == "Fathom.Migrator.ShardMigrationJob",
            where: fragment("?->>'shard_id' = 'e'", j.args)
          ),
          set: [state: "executing", attempted_at: DateTime.utc_now()]
        )

      assert :ok = Migrator.yank(2)

      assert [%{state: "cancelled"}] =
               Repo.all(
                 from(j in Oban.Job,
                   where: j.worker == "Fathom.Migrator.ShardMigrationJob",
                   where: fragment("?->>'shard_id' = 'e'", j.args)
                 )
               ),
             "an executing forward job must be cancelled by the yank, not left to cut over"
    end

    # Round-2 #22 (the belt): a migration that completed IN the yank-cancel race
    # window leaves its shard active at the yanked version, above HEAD — invisible
    # to shards_at_version at revert time and to every laggard sweep after. The
    # reconcile sweep must enqueue its revert.
    test "the reconcile sweep enqueues reverts for shards stranded on a yanked version" do
      {:ok, _} = Migrator.release(1, "good", ["SELECT 1"])
      {:ok, _} = Migrator.release(2, "bad", ["SELECT 1"])

      # The fleet reverted 2 → 1 (yanking 2) while this shard's migration was
      # completing: it lands on v2 AFTER the revert's shard-set read.
      assert :ok = Migrator.yank(2)
      {:ok, _} = Directory.resolve("stranded")
      {:ok, _} = Directory.cutover("stranded", 2)
      assert Migrator.head() == 1

      assert :ok = perform_job(ReconcileJob, %{})

      assert_enqueued(
        worker: RevertJob,
        args: %{"shard_id" => "stranded", "to_version" => 1}
      )
    end
  end

  # Expert review #32: after a fleet revert (yank vN, HEAD → vN-1) the TEMPLATE still has vN's
  # Django migration applied, so the next makemigrations builds on schema the fleet reverted away
  # from → fleet-wide replay failure. The check compares the template's captured django_migrations
  # count (recorded per release) against HEAD's, and yank/1 alarms when the template is left ahead.
  # Expert review #1: a captured version carrying template-literal DATA migrations is flagged
  # requires_review, which must CAP the fleet HEAD below it (blocking the rollout from replaying the
  # dangerous DML fleet-wide) until an operator reviews and approves it — then HEAD advances.
  describe "requires_review (data-migration review gate)" do
    test "a flagged version caps HEAD below it until approved, then HEAD advances" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      # v2 carries a data migration → flagged requires_review (5th arg).
      {:ok, _} = Migrator.release(2, "v2-data", ["UPDATE app_thing SET x = 1"], nil, true)
      {:ok, _} = Migrator.release(3, "v3", ["SELECT 1"])

      # HEAD is capped at v1 — the flagged v2 blocks v2 AND v3 (linear graph, no skipping past it).
      assert Migrator.head() == 1
      assert [%{version: 2}] = Migrator.pending_review()

      # The operator reviews and approves v2 → HEAD advances to v3, review queue clears.
      assert :ok = Migrator.approve_review(2)
      assert Migrator.head() == 3
      assert Migrator.pending_review() == []
    end

    test "approve_review on an unknown version errors" do
      assert Migrator.approve_review(99) == {:error, :unknown_version}
    end
  end

  describe "template_drift/0" do
    test "reports :aligned when no yanked version is above HEAD" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"], 10)
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"], 11)

      assert Migrator.head() == 2
      assert Migrator.template_drift() == :aligned
    end

    test "a yank leaves the template ahead: drift result + telemetry + escalating log" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"], 10)
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"], 11)

      test_pid = self()
      handler = "drift-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :migrator, :template_drift],
        fn _e, meas, meta, _cfg -> send(test_pid, {:drift, meas, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      # yank/1 runs check_template_drift/0, which alarms because the template (still at v2, count 11)
      # is ahead of the reverted-to HEAD v1 (count 10).
      log = ExUnit.CaptureLog.capture_log(fn -> assert :ok = Migrator.yank(2) end)

      assert log =~ "template migration drift after revert"
      assert_received {:drift, %{count: 1}, %{template_version: 2, head_version: 1}}

      assert Migrator.head() == 1

      assert {:drift,
              %{
                template_version: 2,
                template_migration_count: 11,
                head_version: 1,
                head_migration_count: 10
              }} = Migrator.template_drift()
    end

    test "reports :unknown when the relevant releases predate template_migration_count (nil)" do
      # Hand-authored / pre-feature releases carry no count — the check can't decide, so it must
      # not false-alarm.
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      assert :ok = Migrator.yank(2)

      assert Migrator.template_drift() == :unknown
    end
  end

  # Expert review #25: migration_failed was a terminal state with no exit path —
  # quarantined shards were excluded from laggards and every sweep forever, so a wave
  # of transient failures froze a slice of the fleet at the old version even after the
  # cause was fixed; un-quarantining took hand-written SQL against the shards table.
  # The invariant: an operator API returns quarantined shards to the rollout.
  describe "retry_failed/0" do
    test "un-quarantines failed shards and re-enqueues their migration to HEAD" do
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      {:ok, _} = Directory.resolve("fq")
      {:ok, _} = Directory.mark_failed("fq")

      # Quarantined: invisible to the sweep.
      assert {:ok, 0} = Migrator.rollout()
      refute_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => "fq"})

      assert {:ok, 1} = Migrator.retry_failed()

      assert {:ok, %{status: "active"}} = Directory.get("fq")
      assert_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => "fq", "target" => 2})

      # Idempotent: nothing left to requeue.
      assert {:ok, 0} = Migrator.retry_failed()
    end

    # Expert review 2026-08-26 #21, the half that is REAL. `retry_failed/0` did
    # `Directory.failed_shards()` — a bare `Repo.all` with no `select` and no limit — materializing
    # every quarantined shard as a full struct in one heap before doing anything, the same
    # O(fleet) blowup #12 and #15 closed on two other doors.
    #
    # The finding's OTHER half — that `in ^ids` would blow Postgres's 65 535-bind-parameter cap —
    # was measured and is false; see `Directory.requeue_failed/1` and
    # `directory_bind_parameter_test.exs`.
    test "the whole quarantined slice is swept in chunks, and each chunk converges as it goes" do
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])

      # More than one chunk's worth would take 5 001 shards; the invariant that is actually worth
      # pinning at test size is that the sweep is driven by the keyset STREAM (which requeues rows
      # it has already read while continuing to page) rather than by one materialized list.
      ids = for i <- 1..25, do: "fqs_#{i}"

      for id <- ids do
        {:ok, _} = Directory.resolve(id)
        {:ok, _} = Directory.mark_failed(id)
      end

      assert Directory.count_failed() >= 25

      assert {:ok, n} = Migrator.retry_failed()
      assert n >= 25

      for id <- ids do
        assert {:ok, %{status: "active"}} = Directory.get(id),
               "#{id} was left quarantined — the scan and the requeue disagree about the set"

        assert_enqueued(worker: ShardMigrationJob, args: %{"shard_id" => id, "target" => 2})
      end
    end

    # The pre-existing contract, pinned because the #21 rewrite could easily have dropped it: with
    # no released version there is no target to enqueue, but un-quarantining is the OTHER half of
    # what this API does and must still happen.
    test "un-quarantines even when no version is released" do
      {:ok, _} = Directory.resolve("fq_nohead")
      {:ok, _} = Directory.mark_failed("fq_nohead")

      assert Migrator.head() == 0, "fixture: a release exists, so this tests nothing"
      assert {:ok, 0} = Migrator.retry_failed()

      assert {:ok, %{status: "active"}} = Directory.get("fq_nohead"),
             "the shard stayed quarantined; requeue must not be gated on a release existing"
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

    # Round-2 #21b: the force upgrade set only `force: true`, keeping the in-flight
    # job's OLD to_version — so `revert(5, 3, force: true)` while a non-forced
    # revert(5→4) snoozed would force-revert the shard (a destructive discard of
    # post-cutover writes) to v4, never reaching v3, with the →3 job deduped away.
    # The invariant: a force sweep retargets the WHOLE operation — last command wins.
    test "a forced re-issue retargets in-flight revert jobs to its own to_version" do
      {:ok, _} = Directory.resolve("retarget")
      {:ok, _} = Directory.cutover("retarget", 5)

      # A non-forced revert 5→4 is in flight (never executed — manual testing mode).
      assert {:ok, 1} = Migrator.revert(5, 4)
      assert [%{args: %{"to_version" => 4}}] = all_enqueued(worker: RevertJob)

      # The operator force-reverts 5→3 instead.
      assert {:ok, 1} = Migrator.revert(5, 3, force: true)

      assert [job] = all_enqueued(worker: RevertJob)
      assert job.args["to_version"] == 3, "the retarget must carry the NEW to_version"
      assert job.args["force"] == true
    end

    test "RevertJob reverts a migrated shard back to the prior version" do
      shard = "revert_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        for p <- Path.wildcard(Path.join(remote_dir(), "#{shard}*")), do: File.rm(p)
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

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
