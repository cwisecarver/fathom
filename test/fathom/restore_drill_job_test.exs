defmodule Fathom.RestoreDrillJobTest do
  # Expert review #24: "a backup you haven't restored is a hypothesis." The drill samples
  # least-recently-verified shards, pulls + integrity-checks their durable objects, cross-checks the
  # schema version, flags sentinels, and records the outcome — so a bad stored object on a dormant
  # tenant's cold tail is caught by a drill instead of when the tenant returns. Directory rows are
  # Postgres (DataCase sandbox); the durable objects are the Local storage backend. Not async.
  use Fathom.DataCase, async: false

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
