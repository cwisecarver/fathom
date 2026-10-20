defmodule Fathom.Shard.ReplicationSeedBytesTest do
  @moduledoc """
  The seed's reported size is what it SENDS: the `.db` plus the WAL's commit extent, not the `-wal`
  file size (expert review 2026-10-20 #8).

  SQLite restarts a WAL in place and keeps the file's high-water mark, so on a hot shard the file
  sits near the 4000-frame threshold (~16.5 MB) while the extent after a restart is a few hundred KB.
  `stream_seed` always sent `[0, commit_extent)`, but `[:fathom, :replication, :seed]` `bytes` and the
  "seed ok … bytes=" log line summed the FILE sizes, so the rig's 2026-10-19 pool-fleet collapse read
  as "14 seeds of 16.5 MB" when each was ~0.6 MB (the followers' own "seeded … 20480B db + 618032B
  wal" lines). That misdirected the diagnosis toward seed bandwidth.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Session, Wal}

  setup do
    dir = Path.join(System.tmp_dir!(), "seedbytes_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{db: Path.join(dir, "s.db")}
  end

  test "a WAL restarted in place reports its commit extent, not its file size", %{db: db} do
    {:ok, conn} = Connection.open(db)
    on_exit(fn -> Connection.close(conn) end)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b BLOB)", [])

    for i <- 1..40,
        do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (#{i}, randomblob(3000))", [])

    # Backfill everything; the next write restarts the log IN PLACE (the file keeps its size).
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(PASSIVE)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (0, x'00')", [])

    wal = db <> "-wal"
    {:ok, %{commit_extent: extent}} = Wal.read(wal)
    %{size: wal_file} = File.stat!(wal)
    %{size: db_size} = File.stat!(db)

    assert wal_file > 4 * extent, "precondition: the restarted WAL's file is far past its extent"
    assert Session.seed_bytes(db, wal) == db_size + extent
  end

  test "an absent WAL reports the .db alone", %{db: db} do
    File.write!(db, "")
    assert Session.seed_bytes(db, db <> "-wal") == 0
  end
end
