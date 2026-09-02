defmodule Fathom.MigratorTest do
  use Fathom.DataCase, async: true

  # These build fixtures by releasing versions with a capture template configured, which is
  # exactly the configuration `Migrator.release/6` warns about (novel tenants born empty).
  # The warning is correct here and not what these tests are about, so capture it: ExUnit
  # still prints captured logs when a test FAILS, so this hides noise without hiding signal.
  @moduletag :capture_log

  alias Fathom.Migrator
  alias Fathom.Shard.Storage

  describe "release/2 and head/0" do
    test "head is 0 before anything is released" do
      assert Migrator.head() == 0
    end

    # Releasing a version means new tenants need a schema, and fork-from-template is the only
    # thing that supplies one. With the flag off a novel tenant is born EMPTY and the rollout
    # cannot heal it (replay onto an empty file dies on `no such table: django_migrations`).
    # `Shards.fork_novel/1` alarms only when the flag is ON, so the flag-OFF case — the actual
    # misconfiguration — used to be completely silent. Release is where it gets caught.
    test "releasing with :fork_from_template OFF warns that novel tenants are born empty" do
      with_template(fn ->
        log =
          ExUnit.CaptureLog.capture_log(fn ->
            assert {:ok, _} = Migrator.release(1, "initial schema")
          end)

        assert log =~ "fork_from_template is OFF"
        assert log =~ "born EMPTY"
        assert log =~ "FORK_FROM_TEMPLATE=true"
      end)
    end

    # Without a capture template there is nothing to fork FROM, so the flag could not help even
    # if it were on. Warning there would fire on every release in every such deployment and train
    # operators to ignore it.
    test "releasing with NO capture template is silent, flag or not" do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, _} = Migrator.release(1, "initial schema")
        end)

      refute log =~ "born EMPTY"
    end

    # Expert review 2026-08-31 #11. This test PREVIOUSLY asserted "fork ON ⇒ silent", which pinned
    # an incomplete assumption: fork ON is not sufficient — fork_from_template/1 forks from the
    # `template@HEAD` snapshot, created out-of-band by `mix fathom.snapshot template-head` with no
    # automatic caller after a release. So the moment a release advances HEAD, that snapshot is
    # stale and every novel tenant is born empty until it is refreshed. Release now warns for that
    # gap too — the fork-ON case is silent ONLY when the snapshot for the new head is present.
    test "fork ON but the template snapshot is STALE for the new head still warns (#11)" do
      Application.put_env(:fathom, :fork_from_template, true)
      on_exit(fn -> Application.put_env(:fathom, :fork_from_template, false) end)

      test_pid = self()
      handler = "tmpl-stale-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :migrator, :template_snapshot_stale],
        fn _e, m, _meta, _ -> send(test_pid, {:stale, m.version}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      with_template(fn ->
        log =
          ExUnit.CaptureLog.capture_log(fn ->
            assert {:ok, _} = Migrator.release(1, "initial schema")
          end)

        assert log =~ "no template@v1 snapshot",
               "fork ON with no refreshed template snapshot must warn (novel tenants born empty)"

        assert_receive {:stale, 1}
      end)
    end

    # The other half of #11: with the snapshot present, the fork WILL succeed, so the release is
    # silent. This is the assertion the old test MEANT to make — the flag silences it — but it only
    # holds once the fork source actually exists.
    test "fork ON WITH the template snapshot present for the new head is silent (#11)" do
      Application.put_env(:fathom, :fork_from_template, true)
      on_exit(fn -> Application.put_env(:fathom, :fork_from_template, false) end)

      with_template(fn ->
        # Create template@1 the way `mix fathom.snapshot template-head` would: a live template
        # object, then retain it at the released version.
        tmp =
          Path.join(System.tmp_dir!(), "tmpl_present_#{System.unique_integer([:positive])}.db")

        File.write!(tmp, "template-bytes")
        :ok = Storage.flush("tmpl_warn_test", tmp)
        :ok = Storage.retain("tmpl_warn_test", 1)
        File.rm(tmp)

        on_exit(fn ->
          for path <- Path.wildcard(Path.join(Storage.Local.dir(), "tmpl_warn_test*")),
              do: File.rm(path)
        end)

        assert Storage.version_present?("tmpl_warn_test", 1),
               "fixture: the fork source must exist"

        log =
          ExUnit.CaptureLog.capture_log(fn ->
            assert {:ok, _} = Migrator.release(1, "initial schema")
          end)

        refute log =~ "born EMPTY"
      end)
    end

    test "a REJECTED release does not warn" do
      {:ok, _} = Migrator.release(1, "initial")

      # No version was released, so nothing changed about how tenants are born. Warning here
      # would be noise attached to an operation that did not happen.
      with_template(fn ->
        log =
          ExUnit.CaptureLog.capture_log(fn ->
            assert {:error, _} = Migrator.release(1, "duplicate")
          end)

        refute log =~ "born EMPTY"
      end)
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

      # Either expression index on (worker, args->>'shard_id') serves this query and keeps it off a
      # seq scan — the intent here. The non-unique oban_jobs_worker_shard_id_live_index (#20) covers
      # ALL workers; the scoped-unique oban_jobs_bulk_shard_unique_index (#26) covers exactly the
      # three BulkEnqueue workers, of which ShardMigrationJob is one, so the planner may legitimately
      # prefer the smaller unique index for this worker. This assertion previously named only the
      # first; #26 added the second, and the point was always "an index, not a seq scan".
      assert plan =~ "oban_jobs_worker_shard_id_live_index" or
               plan =~ "oban_jobs_bulk_shard_unique_index",
             "the dedup query cannot use any expression index, so it still scans every live job " <>
               "row per rollout chunk. Plan:\n#{plan}"
    end
  end

  # The born-empty warning only fires for a fleet that HAS a capture template, so every test
  # about it must configure one or it passes for the wrong reason.
  defp with_template(fun) do
    prev = Application.get_env(:fathom, :template_shard_id)
    Application.put_env(:fathom, :template_shard_id, "tmpl_warn_test")

    try do
      fun.()
    after
      if prev,
        do: Application.put_env(:fathom, :template_shard_id, prev),
        else: Application.delete_env(:fathom, :template_shard_id)
    end
  end
end
