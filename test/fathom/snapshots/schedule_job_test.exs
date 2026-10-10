defmodule Fathom.Snapshots.ScheduleJobTest do
  @moduledoc """
  Expert review 2026-08-01 #18: `Snapshots.create/2` was a correct primitive that nothing ever
  called on a schedule, so for LOGICAL corruption — a bad deploy, a bad backfill — fathom's
  practical recovery capability was zero: the live object is overwritten every 5 s and the last-good
  state was gone with it.

  These are the integration tests the pure `Retention.plan/3` suite cannot cover: the SELECTION
  predicate (which shards get snapshotted and, more importantly, which do NOT), the stamping that
  drives rotation, and the end-to-end restore that makes the whole thing worth having.

  Directory rows are Postgres (DataCase sandbox); durable objects are the `Local` storage backend.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Directory
  alias Fathom.ShardExecutor
  alias Fathom.Shards
  alias Fathom.Snapshots
  alias Fathom.Snapshots.RetentionJob
  alias Fathom.Snapshots.ScheduleJob
  alias Filo.Stmt

  setup do
    prev = %{
      sample: Application.get_env(:fathom, :snapshot_schedule_sample),
      retention: Application.get_env(:fathom, :snapshot_retention),
      retention_sample: Application.get_env(:fathom, :snapshot_retention_sample)
    }

    ids = ["snapsched_#{System.unique_integer([:positive])}"]

    on_exit(fn ->
      restore(:snapshot_schedule_sample, prev.sample)
      restore(:snapshot_retention, prev.retention)
      restore(:snapshot_retention_sample, prev.retention_sample)

      for id <- ids do
        Shards.drain(id, 2_000)

        for dir <- [remote_dir(), Fathom.Shard.data_dir()],
            suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
            do: File.rm(Path.join(dir, id <> suffix))
      end
    end)

    %{id: hd(ids)}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp remote_dir do
    Application.get_env(:fathom, Fathom.Shard.Storage.Local, [])
    |> Keyword.get(:dir, Path.join(System.tmp_dir!(), "fathom_shard_objects"))
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # Write rows and flush a durable object, then record the flush in the directory.
  #
  # `last_flushed_at` reaches Postgres through `Fathom.Directory.Recorder`, which COALESCES in ETS
  # and batch-flushes off the hot path (#28) — so it is not in the row when `drain/2` returns, and
  # the scheduler's selection query would see a never-flushed shard. Driving
  # `record_flush_batch/1` directly is the same call the recorder eventually makes; the buffering
  # itself has its own suite, and waiting on it here would make these tests time-dependent for no
  # added coverage.
  defp seed(id, sqls) do
    {:ok, _} = Directory.resolve(id)
    {:ok, conn} = ShardExecutor.open(id)
    for s <- sqls, do: {:ok, _} = ShardExecutor.execute(conn, stmt(s))
    :ok = ShardExecutor.close(conn)
    :ok = Shards.drain(id, 5_000)
    Directory.record_flush_batch([{id, DateTime.utc_now()}])
    :ok
  end

  defp row(id), do: Repo.get_by(Fathom.Directory.Shard, shard_id: id)

  # Expert review 2026-10-01 perf #27. The scheduler snapshotted its sample ONE SHARD AT A TIME,
  # each a HEAD + CopyObject plus two Postgres calls, so a fleet-singleton run managed roughly
  # 1/(2 RTT + copy) shards per second and an hourly cron at 1M tenants overran its own slot.
  describe "concurrency" do
    test "a run snapshots its sample concurrently, not one shard at a time" do
      ids = for i <- 1..6, do: "snapschedc#{i}_#{System.unique_integer([:positive])}"

      prev_backend = Application.get_env(:fathom, :shard_storage)

      on_exit(fn ->
        Application.delete_env(:fathom, :faulty_before)
        restore(:shard_storage, prev_backend)

        for id <- ids do
          Shards.drain(id, 2_000)

          for dir <- [remote_dir(), Fathom.Shard.data_dir()],
              suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
              do: File.rm(Path.join(dir, id <> suffix))
        end
      end)

      for id <- ids, do: seed(id, ["CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (1)"])

      # Each of OUR copies takes 300 ms, the way a real CopyObject takes real time. Six serial
      # copies need >= 1.8 s; concurrent ones finish in roughly one copy's time.
      mine = MapSet.new(ids)
      # The suite's default backend is `Local`, which has no hook; the double delegates to it.
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

      Application.put_env(
        :fathom,
        :faulty_before,
        {:snapshot, fn id -> if MapSet.member?(mine, id), do: Process.sleep(300) end}
      )

      {us, {:ok, _counts}} = :timer.tc(fn -> ScheduleJob.run(50) end)

      for id <- ids, do: assert({:ok, [_ | _]} = Snapshots.list(id))

      assert us < 1_200_000,
             "six 300 ms snapshot copies took #{div(us, 1000)} ms: the run is serial"
    end

    # Second half of the same finding: RetentionJob swept its sample one shard at a time (a LIST
    # plus ~one DELETE each), so it could not keep up with the snapshots the now-concurrent
    # scheduler creates.
    test "a retention run sweeps its sample concurrently, not one shard at a time" do
      ids = for i <- 1..6, do: "snapretc#{i}_#{System.unique_integer([:positive])}"
      prev_backend = Application.get_env(:fathom, :shard_storage)

      on_exit(fn ->
        Application.delete_env(:fathom, :faulty_before)
        restore(:shard_storage, prev_backend)

        for id <- ids do
          Shards.drain(id, 2_000)
          _ = Fathom.Shard.Storage.Local.purge_shard(id)

          for dir <- [remote_dir(), Fathom.Shard.data_dir()],
              suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
              do: File.rm(Path.join(dir, id <> suffix))
        end
      end)

      for id <- ids, do: seed(id, ["CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (1)"])
      {:ok, _} = ScheduleJob.run(50)
      for id <- ids, do: assert({:ok, [_]} = Snapshots.list(id))

      # Each of OUR retention LISTs takes 300 ms. Six serial sweeps need >= 1.8 s.
      mine = MapSet.new(ids)
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

      Application.put_env(
        :fathom,
        :faulty_before,
        {:list_snapshots, fn id -> if MapSet.member?(mine, id), do: Process.sleep(300) end}
      )

      {us, {:ok, totals}} = :timer.tc(fn -> RetentionJob.run(50, %{}) end)
      Application.delete_env(:fathom, :faulty_before)

      # The sweep really ran on our shards: the keep-nothing policy expired each auto snapshot.
      for id <- ids, do: assert({:ok, []} = Snapshots.list(id))
      assert totals.errors == 0

      assert us < 1_200_000,
             "six 300 ms retention sweeps took #{div(us, 1000)} ms: the run is serial"
    end
  end

  # Expert review 2026-10-08 #19: the retention sweep took the n shards with the OLDEST
  # `last_snapshot_at` and stamped nothing, so the same coldest shards — nothing to expire — headed
  # every run, and the hot shard accumulating snapshots (newest `last_snapshot_at`) was never reached
  # once more than n shards had any. Storage grew without bound under a green `dropped: 0`.
  describe "retention rotation" do
    test "every snapshotted shard is swept within ceil(count / n) runs, hot ones included" do
      now = DateTime.utc_now()
      cold = for i <- 1..4, do: "retrot_cold_#{i}_#{System.unique_integer([:positive])}"
      hot = "retrot_hot_#{System.unique_integer([:positive])}"

      for {id, age_h} <- Enum.zip(cold, [40, 30, 20, 10]) ++ [{hot, 0}] do
        {:ok, _} = Directory.resolve(id)
        1 = Directory.record_snapshot(id, DateTime.add(now, -age_h * 3600, :second))
      end

      prev_storage = Application.get_env(:fathom, :shard_storage)
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
      test_pid = self()

      Application.put_env(
        :fathom,
        :faulty_before,
        {:list_snapshots, fn id -> send(test_pid, {:swept, id}) end}
      )

      on_exit(fn ->
        Application.delete_env(:fathom, :faulty_before)

        if prev_storage,
          do: Application.put_env(:fathom, :shard_storage, prev_storage),
          else: Application.delete_env(:fathom, :shard_storage)
      end)

      # 5 snapshotted shards, 2 per run: three runs must reach all of them.
      for _ <- 1..3, do: {:ok, _} = RetentionJob.run(2, %{weekly: 1})

      swept = collect_swept(MapSet.new())

      assert hot in swept,
             "the hot shard was never swept: #{inspect(MapSet.to_list(swept))}"

      assert MapSet.subset?(MapSet.new(cold), swept)
    end
  end

  defp collect_swept(acc) do
    receive do
      {:swept, id} -> collect_swept(MapSet.put(acc, id))
    after
      0 -> acc
    end
  end

  describe "selection" do
    test "snapshots a shard that has flushed and never been snapshotted", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (1)"])

      assert [%{shard_id: ^id}] =
               Enum.filter(Directory.sample_for_snapshot(50), &(&1.shard_id == id))

      assert {:ok, counts} = ScheduleJob.run(50)
      assert Map.get(counts, :ok, 0) >= 1

      assert {:ok, [_ | _]} = Snapshots.list(id)
      assert row(id).last_snapshot_at != nil
    end

    test "does NOT re-snapshot a shard that has not flushed since its last snapshot", %{id: id} do
      # The predicate that makes the cost track WRITES rather than fleet size. Without it a million
      # cold tenants would be re-copied every run for bytes that did not change, and the feature
      # would cost more than the data it protects.
      seed(id, ["CREATE TABLE t (v INTEGER)"])

      {:ok, _} = ScheduleJob.run(50)
      {:ok, first} = Snapshots.list(id)

      refute id in Enum.map(Directory.sample_for_snapshot(50), & &1.shard_id)

      {:ok, _} = ScheduleJob.run(50)
      {:ok, second} = Snapshots.list(id)

      assert length(second) == length(first), "a clean shard was snapshotted again"
    end

    test "snapshots it again once it flushes new writes", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      {:ok, _} = ScheduleJob.run(50)
      {:ok, first} = Snapshots.list(id)

      seed(id, ["INSERT INTO t VALUES (42)"])

      assert id in Enum.map(Directory.sample_for_snapshot(50), & &1.shard_id)
      {:ok, _} = ScheduleJob.run(50)
      {:ok, second} = Snapshots.list(id)

      assert length(second) == length(first) + 1
    end

    # Expert review 2026-08-31 #7: record_snapshot stamped wall-clock now(), which is LATER than the
    # flush the snapshot reflects (Snapshots.create copies the last-flushed state). A flush landing
    # between the copy and the stamp was then marked "already snapshotted" (the sample predicate is
    # last_flushed_at > last_snapshot_at) though its bytes are in no snapshot. The stamp must be the
    # captured watermark, so it equals last_flushed_at and any later flush stays selectable.
    test "credits the snapshot with the flush watermark, not wall-clock now()", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (1)"])
      flushed_at = row(id).last_flushed_at
      assert flushed_at

      Application.put_env(:fathom, :snapshot_schedule_sample, 50)
      assert {:ok, counts} = ScheduleJob.run(50)
      assert Map.get(counts, :ok, 0) >= 1

      snap_at = row(id).last_snapshot_at

      assert DateTime.compare(snap_at, flushed_at) == :eq,
             "last_snapshot_at (#{inspect(snap_at)}) is not the flush watermark " <>
               "(#{inspect(flushed_at)}) — a flush landing during the copy would be marked " <>
               "snapshotted though its bytes are in no snapshot"
    end

    test "never selects a shard that has never flushed", %{id: id} do
      # No durable object exists yet, so a snapshot would copy nothing. Excluded at the query
      # rather than allowed to fail per-shard every run.
      {:ok, _} = Directory.resolve(id)

      refute id in Enum.map(Directory.sample_for_snapshot(50), & &1.shard_id)
    end

    test "never selects a non-active shard", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      {:ok, _} = Directory.suspend(id)

      refute id in Enum.map(Directory.sample_for_snapshot(50), & &1.shard_id)
    end
  end

  # Symptom (expert review 2026-10-10 #14): the rotation ordered by `last_snapshot_at NULLS FIRST`,
  # which is stamped only on SUCCESS, so a shard whose snapshot fails every time stayed at the head
  # and held a per-run slot forever — with a sample of 1, nothing else was ever snapshotted. The
  # rotation now orders by an ATTEMPT stamp, so a poison shard goes to the back after one try.
  describe "poison shards (#14)" do
    test "a shard whose snapshot always fails does not starve the rest of the queue", %{id: id} do
      # Good shard: snapshotted once, then flushed again, so it is selectable with a NON-null
      # last_snapshot_at — which sorts AFTER the poison shard's NULL in the old ordering.
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      {:ok, _} = ScheduleJob.run(50)
      seed(id, ["INSERT INTO t VALUES (1)"])
      {:ok, before} = Snapshots.list(id)

      # Poison: flushed per the directory, but no durable object exists, so the copy fails.
      poison = "snappoison_#{System.unique_integer([:positive])}"
      {:ok, _} = Directory.resolve(poison)
      Directory.record_flush_batch([{poison, DateTime.utc_now()}])

      assert {:ok, %{error: 1}} = ScheduleJob.run(1)
      assert row(poison).last_snapshot_attempt_at != nil
      assert row(poison).last_snapshot_at == nil

      assert {:ok, %{ok: 1}} = ScheduleJob.run(1)
      {:ok, after_run} = Snapshots.list(id)
      assert length(after_run) == length(before) + 1, "the poison shard held the only slot"
    end
  end

  # Symptom (expert review 2026-10-10 #14): no `unique:` on the cron workers, so an overlapping run
  # (slow tick, or a leadership change) double-ran the COPY/sweep work.
  describe "overlapping cron runs (#14)" do
    for worker <- [ScheduleJob, RetentionJob] do
      test "#{inspect(worker)} dedups while one is pending" do
        worker = unquote(worker)
        {:ok, first} = Oban.insert(worker.new(%{}))
        refute first.conflict?
        {:ok, second} = Oban.insert(worker.new(%{}))
        assert second.conflict?, "a second #{inspect(worker)} was enqueued while one was pending"
      end
    end
  end

  describe "gating" do
    test "perform/1 is inert with no sample size configured", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      Application.delete_env(:fathom, :snapshot_schedule_sample)

      assert :ok = perform_job(ScheduleJob, %{})
      assert {:ok, []} = Snapshots.list(id)
      assert row(id).last_snapshot_at == nil
    end

    test "perform/1 runs when sized", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      Application.put_env(:fathom, :snapshot_schedule_sample, 50)

      assert :ok = perform_job(ScheduleJob, %{})
      assert {:ok, [_ | _]} = Snapshots.list(id)
    end
  end

  describe "labelling and retention interaction" do
    test "scheduled snapshots carry the auto label so retention can expire them", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      {:ok, _} = ScheduleJob.run(50)
      {:ok, [snap]} = Snapshots.list(id)

      assert String.ends_with?(snap.id, "-auto")
      assert Fathom.Snapshots.Retention.auto?(snap.id)
    end

    test "retention never deletes an operator's manual snapshot", %{id: id} do
      # The end-to-end version of the safety property. A policy that keeps nothing is used
      # deliberately: even then, the manual snapshot survives.
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      {:ok, manual} = Snapshots.create(id, label: "pre-migration")
      {:ok, _} = ScheduleJob.run(50)

      {:ok, before} = Snapshots.list(id)
      assert length(before) == 2

      {:ok, totals} = RetentionJob.run(50, %{})

      {:ok, remaining} = Snapshots.list(id)
      ids = Enum.map(remaining, & &1.id)

      assert manual in ids, "retention deleted a manual snapshot"
      assert totals.dropped == 1, "it should still have expired the automatic one"
      refute Enum.any?(ids, &String.ends_with?(&1, "-auto"))
    end

    test "a dry run reports what it would delete and deletes nothing", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      {:ok, _} = ScheduleJob.run(50)
      {:ok, before} = Snapshots.list(id)

      {:ok, totals} = RetentionJob.run(50, %{}, dry_run: true)

      {:ok, after_} = Snapshots.list(id)
      assert length(after_) == length(before), "a dry run deleted something"
      assert totals.dropped == 0
    end

    test "retention is inert without both a policy and a sample size", %{id: id} do
      seed(id, ["CREATE TABLE t (v INTEGER)"])
      {:ok, _} = ScheduleJob.run(50)

      # A sample size with no policy would be a delete sweep with no rule — the one
      # half-configuration here that destroys data.
      Application.put_env(:fathom, :snapshot_retention_sample, 50)
      Application.delete_env(:fathom, :snapshot_retention)
      assert :ok = perform_job(RetentionJob, %{})
      assert {:ok, [_]} = Snapshots.list(id)

      # And a policy with no sample size does nothing rather than looking enabled.
      Application.put_env(:fathom, :snapshot_retention, %{hourly: 0})
      Application.delete_env(:fathom, :snapshot_retention_sample)
      assert :ok = perform_job(RetentionJob, %{})
      assert {:ok, [_]} = Snapshots.list(id)
    end
  end

  describe "the point of the whole feature" do
    test "a scheduled snapshot restores data a later logical error destroyed", %{id: id} do
      # The scenario #18 exists for: not a lost node, but a bad write that the 5 s flush made
      # durable seconds later. Without a schedule there is nothing to go back to.
      seed(id, ["CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (1), (2), (3)"])

      Application.put_env(:fathom, :snapshot_schedule_sample, 50)
      {:ok, _} = ScheduleJob.run(50)
      {:ok, [snap]} = Snapshots.list(id)

      # The "logical error": everything deleted, then flushed durably.
      seed(id, ["DELETE FROM t"])

      {:ok, conn} = ShardExecutor.open(id)
      assert {:ok, %{rows: [[0]]}} = ShardExecutor.execute(conn, stmt("SELECT COUNT(*) FROM t"))
      :ok = ShardExecutor.close(conn)
      :ok = Shards.drain(id, 5_000)

      assert :ok = Snapshots.restore(id, snap.id)

      {:ok, conn} = ShardExecutor.open(id)
      assert {:ok, %{rows: [[3]]}} = ShardExecutor.execute(conn, stmt("SELECT COUNT(*) FROM t"))
      :ok = ShardExecutor.close(conn)
    end
  end
end
