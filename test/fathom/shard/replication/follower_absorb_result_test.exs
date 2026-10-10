defmodule Fathom.Shard.Replication.FollowerAbsorbResultTest do
  @moduledoc """
  The follower's absorb-before-reset must not report success when the checkpoint stopped short.

  `PRAGMA wal_checkpoint(TRUNCATE)` returns a result row even when a reader pinned at an older
  snapshot stopped the backfill at its mark (`checkpointed < log`). The follower counted any result
  as absorbed, cleared `torn`, and the reset then truncated the WAL still holding the frames that
  never reached the `.db`: a replica marked whole and promotable, missing pages. Found 2026-10-09
  while fixing expert review 2026-10-08 #5; the same class as the migration copy's fold (#25).

  Deterministic: a reader pins a snapshot, more frames are committed after it, and the checkpoint
  can then only backfill up to the reader's mark.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower

  setup do
    db = Path.join(System.tmp_dir!(), "absorb_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(db <> s) end)

    {:ok, w} = Connection.open(db)
    :ok = Connection.exec(w, "PRAGMA wal_autocheckpoint=0")
    :ok = Connection.exec(w, "CREATE TABLE t (x BLOB)")
    :ok = Connection.exec(w, "INSERT INTO t VALUES (randomblob(10000))")
    %{db: db, writer: w}
  end

  test "a checkpoint stopped short by a pinned reader is not an absorb", %{db: db, writer: w} do
    {:ok, reader} = Connection.open(db)
    :ok = Connection.exec(reader, "BEGIN")
    {:ok, _} = Connection.query(reader, "SELECT count(*) FROM t", [])

    # Frames the reader's snapshot does not cover, so the backfill must stop before them.
    :ok = Connection.exec(w, "INSERT INTO t VALUES (randomblob(10000))")
    :ok = Connection.exec(w, "INSERT INTO t VALUES (randomblob(10000))")

    try do
      assert {:error, {:absorb_incomplete, [_busy, log, ckpt]}} = Follower.checkpoint_into_db(db),
             "the absorb reported success with frames still only in the WAL"

      assert ckpt < log
    after
      Connection.exec(reader, "ROLLBACK")
      Connection.close(reader)
      Connection.close(w)
    end
  end

  test "with no reader in the way the absorb succeeds", %{db: db, writer: w} do
    Connection.close(w)
    assert :ok = Follower.checkpoint_into_db(db)
  end
end
