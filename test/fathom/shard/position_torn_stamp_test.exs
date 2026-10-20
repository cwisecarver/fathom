defmodule Fathom.Shard.PositionTornStampTest do
  @moduledoc """
  Round-4 durability review 2026-10-20 F1: the object position stamp reads the primary's live WAL
  from the `-shm` wal-index (`Wal.read(path, index: true)`), not the frame walk.

  Symptom this pins: a WAL restarted in place keeps a stale tail (`journal_size_limit`). A commit
  caught mid-`walWriteOneFrame` has its commit-frame HEADER on a stale slot while its page is not
  yet written and `mxFrame` is not yet published. The walk counts that frame, so the stamp claimed
  a commit the snapshot (which began before `mxFrame` moved) does not hold; the commit is then
  acked via the replicas and an object that over-claims it beats them on failover. `mxFrame` is
  only published after the whole commit is written, so the index read cannot count it.

  Covers `Position.flush_position/2`. `shard.ex`'s two `pre` reads (`snapshot_and_upload`,
  `upload_for_drop`) pass the same option to the same `Wal.read`; they have no seam that holds a
  torn header open, so they rest on `Wal.read(index: true)`'s own tests (`wal_index_test.exs`).
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Position

  @fh 24

  setup do
    dir = Path.join(System.tmp_dir!(), "postorn_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  defp exec!(db, sql), do: :ok = Exqlite.Sqlite3.execute(db, sql)

  test "the stamp offset does not count a torn commit-frame header", %{dir: dir} do
    path = Path.join(dir, "c.db")
    {:ok, db} = Exqlite.Sqlite3.open(path)
    on_exit(fn -> Exqlite.Sqlite3.close(db) end)
    exec!(db, "PRAGMA journal_mode=WAL")
    exec!(db, "PRAGMA wal_autocheckpoint=0")
    exec!(db, "PRAGMA journal_size_limit=67108864")
    exec!(db, "CREATE TABLE t (a INTEGER, b BLOB)")

    # Stale tail: a long generation, checkpointed, then a restart by the next write.
    exec!(db, "BEGIN")
    for i <- 1..300, do: exec!(db, "INSERT INTO t VALUES (#{i}, randomblob(3500))")
    exec!(db, "COMMIT")
    exec!(db, "PRAGMA wal_checkpoint(PASSIVE)")
    exec!(db, "INSERT INTO t VALUES (-1, NULL)")

    wal = path <> "-wal"
    state = %{path: path, lease: %{epoch: 1}}
    committed = Position.flush_position(state, {:ok, :empty})
    assert is_map(committed)

    # The writer's intermediate state: this generation's salts + nTruncate over a stale slot.
    {:ok, fd} = :file.open(wal, [:read, :write, :raw, :binary])
    {:ok, <<salts::binary-8>>} = :file.pread(fd, 16, 8)
    {:ok, size} = :file.position(fd, :eof)
    assert size > committed.offset + 10 * @fh, "fixture: no stale tail"
    {:ok, <<_::32, _::32, old::binary-8, _::binary>>} = :file.pread(fd, committed.offset, @fh)
    assert old != salts, "fixture: the slot past the extent is not stale"
    :ok = :file.pwrite(fd, committed.offset, <<1::32, 9_999::32, salts::binary, 0::64>>)
    :file.close(fd)

    assert Position.flush_position(state, {:ok, :empty}).offset == committed.offset,
           "the stamp counted a commit frame whose page is not written and whose mxFrame is not " <>
             "published: the object would claim a write its snapshot does not hold"
  end
end
