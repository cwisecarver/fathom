defmodule Fathom.Migrator.ShardMigrationReplicaTest do
  @moduledoc """
  The migrator x A2 replication interaction (expert review 2026-10-15 #2, user decision Q2 = A).

  Symptom: the owner dies with an acked-but-unflushed tail, followers hold a replica FRESHER than
  the stored object, and a migration/revert that reaches the shard before any client reopens it
  pulled the STALE object, transformed it and flushed it unstamped — after which no replica could
  ever override it (`Promote.fresher?(_, nil)` is false). The acked tail was lost permanently.

  Invariant: while a replica is strictly ahead of the object's stamp, `ShardMigration.run/3` and
  `revert/4` answer `{:retry, :replica_ahead}` and leave the object untouched, so the job snoozes
  and the next client cold open runs the normal promote. A replica that is level with the object,
  no follower at all, or promote-on-open switched off all migrate as before.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Migrator.ShardMigration
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.{Directory, Migrator}

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002_add_created_at', 'now')"
  ]

  # An object stamp a rankable replica can be compared against.
  @stamp %{epoch: 5, wal_gen: 1, wal_ordinal: 3, offset: 100}

  setup do
    shard = "migrep_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "migrep_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    prev = Application.get_env(:fathom, :replication_promote_on_open)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_promote_on_open),
        else: Application.put_env(:fathom, :replication_promote_on_open, prev)

      File.rm_rf(dir)

      for path <- Path.wildcard(Path.join(Storage.Local.dir(), "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join(Fathom.Shard.data_dir(), "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard, dir: dir}
  end

  defp start_follower(%{dir: dir}),
    do: start_supervised!({Follower, name: Follower, port: 0, dir: dir})

  # A v1 shard whose stored object carries a rankable position stamp.
  defp seed_stamped_v1!(shard) do
    seed = Path.join(System.tmp_dir!(), "seed_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(seed)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)")
    :ok = Connection.exec(conn, "INSERT INTO app_thing (id, name) VALUES (1, 'alice')")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"
      )

    :ok =
      Connection.exec(
        conn,
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001_initial', 'now')"
      )

    :ok = Connection.exec(conn, "PRAGMA user_version = 1")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)

    restamp!(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, 1)
    :ok
  end

  defp restamp!(shard, path) do
    etag = object_etag!(shard)
    assert {:ok, _, _} = Storage.flush(shard, path, etag, @stamp, @stamp.epoch)
    assert {:ok, %{epoch: 5, wal_ordinal: 3}} = Storage.object_position(shard)
  end

  # The replica a surviving follower would hold, `delta` bytes past (or before) the object's stamp.
  defp seed_replica!(shard, delta) do
    Follower.seed(Follower, shard, 1, @stamp.wal_gen, 0, @stamp.offset + delta, @stamp.epoch)
    :ok = Follower.note_ordinal(Follower, shard, @stamp.wal_ordinal)

    assert Promote.fresher?(Follower.state_of(Follower, shard), @stamp) == delta > 0,
           "fixture: the replica's rank against the stamp is not what this test claims"
  end

  defp object_etag!(shard) do
    case Storage.object_etag(shard) do
      {:ok, etag} -> etag
    end
  end

  defp live_version(shard) do
    tmp = Path.join(System.tmp_dir!(), "lv_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, _} = Storage.pull(shard, tmp)
    {:ok, conn} = Connection.open(tmp)
    {:ok, %{rows: [[v]]}} = Connection.query(conn, "PRAGMA user_version", [])
    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    v
  end

  test "run: a replica ahead of the object defers the migration and leaves the object untouched",
       %{shard: shard} = ctx do
    start_follower(ctx)
    seed_stamped_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    seed_replica!(shard, 1)
    before = object_etag!(shard)

    assert {:retry, :replica_ahead} = ShardMigration.run(shard, 2)

    assert object_etag!(shard) == before, "the stale object must not have been rewritten"
    assert live_version(shard) == 1
    assert {:ok, %{schema_version: 1, status: "active"}} = Directory.get(shard)
  end

  test "run: a replica level with the object migrates, and the result reverts",
       %{shard: shard} = ctx do
    start_follower(ctx)
    seed_stamped_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    seed_replica!(shard, 0)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
    assert live_version(shard) == 2

    # The revert-flip leg of the migration gate.
    assert {:ok, %{from: 2, to: 1}} = ShardMigration.revert(shard, 1, "tok-flip", force: true)
    assert live_version(shard) == 1
  end

  test "run: no follower on this node is the unchanged migration", %{shard: shard} do
    seed_stamped_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
  end

  test "run: promote-on-open off means a client open would not promote, so do not defer",
       %{shard: shard} = ctx do
    start_follower(ctx)
    Application.put_env(:fathom, :replication_promote_on_open, false)
    seed_stamped_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    seed_replica!(shard, 1)

    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
  end

  test "revert: a replica ahead of the object defers the revert and leaves the object untouched",
       %{shard: shard} = ctx do
    start_follower(ctx)
    seed_stamped_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)

    # The migration's flush is unstamped (that is #7's business); re-stamp as a coordinator flush
    # would, then have a follower hold a fresher replica.
    tmp = Path.join(System.tmp_dir!(), "rs_#{shard}.db")
    {:ok, _} = Storage.pull(shard, tmp)
    restamp!(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    seed_replica!(shard, 1)
    before = object_etag!(shard)

    assert {:retry, :replica_ahead} = ShardMigration.revert(shard, 1, "tok-rev", force: true)

    assert object_etag!(shard) == before
    assert live_version(shard) == 2
    assert {:ok, %{schema_version: 2}} = Directory.get(shard)
  end
end
