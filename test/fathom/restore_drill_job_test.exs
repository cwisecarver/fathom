defmodule Fathom.RestoreDrillJobTest do
  # Expert review #24: "a backup you haven't restored is a hypothesis." The drill samples
  # least-recently-verified shards, pulls + integrity-checks their durable objects, cross-checks the
  # schema version, flags sentinels, and records the outcome — so a bad stored object on a dormant
  # tenant's cold tail is caught by a drill instead of when the tenant returns. Directory rows are
  # Postgres (DataCase sandbox); the durable objects are the Local storage backend. Not async.
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  import Ecto.Query

  alias Fathom.{Directory, RestoreDrillJob, ShardExecutor, Shards}
  alias Fathom.Directory.Shard, as: DirShard
  alias Filo.Stmt

  setup do
    id = "drill_#{System.unique_integer([:positive])}"
    prev_sample = Application.get_env(:fathom, :restore_drill_sample)

    on_exit(fn ->
      restore(:restore_drill_sample, prev_sample)
      Shards.drain(id, 2_000)

      for dir <- [remote_dir(), Fathom.Shard.data_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, id <> suffix))
    end)

    %{id: id}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # Resolve the directory row (schema_version 0) and flush a durable object with the given rows.
  defp seed(id, sqls) do
    {:ok, _} = Directory.resolve(id)
    {:ok, conn} = ShardExecutor.open(id)
    for s <- sqls, do: {:ok, _} = ShardExecutor.execute(conn, stmt(s))
    :ok = ShardExecutor.close(conn)
    :ok = Shards.drain(id, 5_000)
  end

  describe "the FULL restore drill (#48) — rehearsing the procedure, not just the object" do
    # verify/2 pulls, quick_checks and compares user_version — it proves the stored BYTES are
    # readable. It never invoked Tenants.fork/2, never cold-opened a coordinator from those bytes,
    # and never touched the directory reconcile, so the chain an operator actually runs under
    # maximum pressure had only ever executed in unit tests. "A backup you haven't restored is a
    # hypothesis" was the drill's own framing; this closes the gap between verifying a file and
    # rehearsing the recovery.
    setup do
      prev = Application.get_env(:fathom, :restore_drill_full_sample)
      on_exit(fn -> restore(:restore_drill_full_sample, prev) end)
      :ok
    end

    test "restores a sampled shard to a scratch tenant and matches its row counts", %{id: id} do
      seed(id, [
        "CREATE TABLE kv (v TEXT)",
        "INSERT INTO kv VALUES ('a')",
        "INSERT INTO kv VALUES ('b')"
      ])

      assert {:ok, summary} = RestoreDrillJob.run_full_drill(5)
      assert summary[:ok] >= 1, "the restore rehearsal failed: #{inspect(summary)}"
    end

    # The assertion that makes the rehearsal worth running. A fork that produced an EMPTY database
    # would pass quick_check and prove nothing, so the drill compares row counts against the source
    # — that is the difference between "the file opens" and "the data is there".
    test "a restored copy carries the source's rows, not merely a valid file", %{id: id} do
      seed(id, [
        "CREATE TABLE kv (v TEXT)",
        "INSERT INTO kv VALUES ('a')",
        "INSERT INTO kv VALUES ('b')",
        "INSERT INTO kv VALUES ('c')"
      ])

      scratch = "restoredrillcmp#{System.unique_integer([:positive])}"
      on_exit(fn -> Fathom.Tenants.delete(scratch) end)

      assert {:ok, _} = Fathom.Tenants.fork(id, scratch)

      {:ok, conn} = ShardExecutor.open(scratch)
      {:ok, result} = ShardExecutor.execute(conn, stmt("SELECT count(*) FROM kv"))
      :ok = ShardExecutor.close(conn)

      assert [[3]] = result.rows,
             "the fork did not carry the source's rows — a restore that loses data would still " <>
               "pass a quick_check-only drill"
    end

    # Scratch tenants must not accumulate: one per sample per run would grow the directory and the
    # object store without bound.
    test "the scratch tenant is cleaned up", %{id: id} do
      seed(id, ["CREATE TABLE kv (v TEXT)", "INSERT INTO kv VALUES ('a')"])

      before = drill_scratch_rows()
      assert {:ok, _} = RestoreDrillJob.run_full_drill(5)

      assert drill_scratch_rows() == before,
             "a drill fork was left behind; repeated runs would grow the directory unbounded"
    end

    # Off by default: a fork is a full object copy, so this costs real storage I/O per sample where
    # the read-only drill costs a GET. Raising read-only coverage must not silently multiply that.
    test "it is inert unless :restore_drill_full_sample is set", %{id: id} do
      seed(id, ["CREATE TABLE kv (v TEXT)"])
      Application.delete_env(:fathom, :restore_drill_full_sample)

      before = drill_scratch_rows()
      assert :ok = perform_job(RestoreDrillJob, %{})
      assert drill_scratch_rows() == before, "the full drill ran without being enabled"
    end
  end

  # Counts scratch forks only. The prefix is distinct from this file's own `drill_<n>` source ids,
  # and counts ALL statuses on purpose: if cleanup ever regressed to `Tenants.delete/1` the row
  # would survive as a tombstone, which is precisely the unbounded growth this asserts against.
  describe "snapshot health (#48)" do
    # Snapshots had NO health signal at all: `sample_for_drill/1` samples directory ROWS, and a
    # snapshot is a storage object with no row. So the one class of stored data an operator reaches
    # for during a point-in-time recovery was the one class nothing ever checked — discovered
    # corrupt at exactly the moment it was needed.
    test "a corrupt snapshot is reported, not silently passed over", %{id: id} do
      seed(id, ["CREATE TABLE kv (v TEXT)", "INSERT INTO kv VALUES ('a')"])
      :ok = Fathom.Shard.Storage.snapshot(id, "snap1")

      test_pid = self()
      handler = "snapdrill-#{id}"

      :telemetry.attach(
        handler,
        [:fathom, :restore_drill, :snapshot_result],
        fn _e, _m, meta, _ -> send(test_pid, {:snapshot, meta.status, meta.snapshot_id}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, _} = RestoreDrillJob.run_drill(5)
      assert_receive {:snapshot, :ok, "snap1"}, 2_000

      # Now corrupt the stored snapshot and prove the drill NOTICES. Without this the test would
      # only show the happy path, which any no-op implementation also passes.
      snap_path = Path.join(remote_dir(), "#{id}@snap-snap1.db")
      assert File.exists?(snap_path), "fixture: the snapshot object is not where expected"
      File.write!(snap_path, String.duplicate("garbage", 500))

      assert {:ok, _} = RestoreDrillJob.run_drill(5)
      assert_receive {:snapshot, status, "snap1"}, 2_000
      assert status in [:corrupt, :error], "a corrupt snapshot passed the drill as #{status}"

      # THE DURABLE COLUMN, not just the telemetry (expert review 2026-08-26 #37).
      #
      # record_verification/2 used to run BEFORE verify_snapshots/1, and the snapshot statuses were
      # discarded — so a shard whose only snapshot is corrupt recorded last_verify_status = "ok".
      # This module's moduledoc claims "a failure is queryable"; that held for the live object
      # only, and a DR audit query returned a clean fleet while snapshots rotted.
      {:ok, row} = Directory.get(id)

      refute row.last_verify_status == "ok",
             "the live object is fine but its only snapshot is corrupt, and the shard still " <>
               "records \"ok\" — a DR audit query would call this fleet healthy"

      assert row.last_verify_status in ["corrupt", "error"]
    end

    # The other swallow on the same path. `{:error, {:s3_list_status, 403}}` — a bucket-policy
    # change, an endpoint misconfig, a credential rotation — used to match a bare `_` and return
    # :ok with no log, no telemetry and no counter, so snapshot verification could be dead
    # FLEET-WIDE while every run reported every shard fine.
    test "a failed snapshot LIST is reported, not swallowed as healthy", %{id: id} do
      seed(id, ["CREATE TABLE kv (v TEXT)", "INSERT INTO kv VALUES ('a')"])

      test_pid = self()
      handler = "snaplist-#{id}"

      :telemetry.attach(
        handler,
        [:fathom, :restore_drill, :snapshot_result],
        fn _e, _m, meta, _ -> send(test_pid, {:snapshot, meta.status}) end,
        nil
      )

      prev_storage = Application.get_env(:fathom, :shard_storage)

      on_exit(fn ->
        :telemetry.detach(handler)
        Application.delete_env(:fathom, :storage_list_snapshots_error)

        if is_nil(prev_storage),
          do: Application.delete_env(:fathom, :shard_storage),
          else: Application.put_env(:fathom, :shard_storage, prev_storage)
      end)

      # Make list_snapshots/1 fail the way a bucket-policy change does. FaultyStorage delegates
      # everything else to Local, so the seeded object above is still the one the drill pulls.
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
      Application.put_env(:fathom, :storage_list_snapshots_error, {:s3_list_status, 403})

      assert {:ok, _} = RestoreDrillJob.run_drill(5)

      assert_receive {:snapshot, :list_failed}, 2_000

      {:ok, row} = Directory.get(id)

      refute row.last_verify_status == "ok",
             "snapshot verification could not run at all, and the shard still records \"ok\""
    end
  end

  defp drill_scratch_rows do
    Fathom.Repo.aggregate(from(s in DirShard, where: like(s.shard_id, "restoredrill%")), :count)
  end

  defp attach do
    ref = make_ref()
    test = self()

    :telemetry.attach(
      {__MODULE__, ref},
      [:fathom, :restore_drill, :result],
      fn _e, _m, %{status: s}, _ -> send(test, {:drill, ref, s}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)
    ref
  end

  test "a healthy, schema-matching object verifies :ok and stamps the row", %{id: id} do
    seed(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('x')"])
    ref = attach()

    assert {:ok, %{ok: 1}} = RestoreDrillJob.run_drill(10)
    assert_received {:drill, ^ref, :ok}

    row = fetch(id)
    assert row.last_verify_status == "ok"
    assert row.last_verified_at != nil
  end

  test "a corrupt stored object is flagged :corrupt", %{id: id} do
    seed(id, ["CREATE TABLE t (v TEXT)"])
    File.write!(Path.join(remote_dir(), "#{id}.db"), "definitely not a sqlite database")

    ref = attach()
    assert {:ok, summary} = RestoreDrillJob.run_drill(10)
    assert summary[:corrupt] == 1
    assert_received {:drill, ^ref, :corrupt}
    assert fetch(id).last_verify_status == "corrupt"
  end

  test "a directory row with no stored object is :absent (not a failure)", %{id: id} do
    {:ok, _} = Directory.resolve(id)

    ref = attach()
    assert {:ok, %{absent: 1}} = RestoreDrillJob.run_drill(10)
    assert_received {:drill, ^ref, :absent}
    assert fetch(id).last_verify_status == "absent"
  end

  test "a valid object whose user_version disagrees with the directory is :schema_mismatch",
       %{id: id} do
    seed(id, ["CREATE TABLE t (v TEXT)"])
    # The object's user_version is 0 (never migrated); make the directory claim v5.
    {1, _} =
      Repo.update_all(from(s in DirShard, where: s.shard_id == ^id), set: [schema_version: 5])

    ref = attach()
    assert {:ok, %{schema_mismatch: 1}} = RestoreDrillJob.run_drill(10)
    assert_received {:drill, ^ref, :schema_mismatch}
    assert fetch(id).last_verify_status == "schema_mismatch"
  end

  test "sample_for_drill orders least-recently-verified first (NULLS, then oldest)", %{id: id} do
    {:ok, _} = Directory.resolve(id)
    older = "#{id}_older"
    newer = "#{id}_newer"
    {:ok, _} = Directory.resolve(older)
    {:ok, _} = Directory.resolve(newer)
    on_exit(fn -> for s <- [older, newer], do: Shards.drain(s, 1_000) end)

    now = DateTime.utc_now()
    stamp(older, DateTime.add(now, -3600, :second))
    stamp(newer, now)
    # `id` is left never-verified (NULL).

    ordered = Enum.map(Directory.sample_for_drill(3), & &1.shard_id)

    assert ordered == [id, older, newer],
           "NULL first, then oldest last_verified_at (#{inspect(ordered)})"
  end

  test "perform is a no-op when :restore_drill_sample is unset (gated off)", %{id: id} do
    Application.delete_env(:fathom, :restore_drill_sample)
    seed(id, ["CREATE TABLE t (v TEXT)"])

    ref = attach()
    assert :ok = RestoreDrillJob.perform(%Oban.Job{})
    refute_received {:drill, ^ref, _}
    assert fetch(id).last_verified_at == nil, "the gated-off drill must not touch any row"
  end

  defp fetch(id), do: Repo.get_by!(DirShard, shard_id: id)

  defp stamp(id, at) do
    {1, _} =
      Repo.update_all(from(s in DirShard, where: s.shard_id == ^id), set: [last_verified_at: at])
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
