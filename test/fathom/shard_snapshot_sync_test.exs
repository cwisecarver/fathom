defmodule Fathom.ShardSnapshotSyncTest do
  @moduledoc """
  Expert review 2026-09-18 #7: `do_snapshot/2` flips `synchronous=OFF` for the throwaway VACUUM temp,
  then must restore `FULL`. Pre-fix these were three flat statements, so a VACUUM step that RAISES
  (rather than returning `{:error}` — `Connection.query` rescues only `ArgumentError`, so a different
  exception propagates) skipped the restore, and the exception unwound through
  `verify_and_snapshot/2`'s `after Connection.close(conn)` — which, on the last connection to a WAL
  database, runs the close-time checkpoint that UNLINKS `-wal`/`-shm`, at `synchronous=OFF`: a torn
  database on power loss, uploaded as the durable object.

  The fix wraps the VACUUM step in `try/after` so `FULL` is always restored. We drive the raise with
  a non-binary `dest` (the dest interpolation raises inside the try), standing in for a VACUUM that
  raises past the rescue — a real SQLite VACUUM failure returns `{:error}` and cannot be forced to
  raise here. `do_snapshot/2` is `@doc false` public precisely so this can observe the restored
  connection (its real caller closes it).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  test "a raising VACUUM step still restores synchronous=FULL (#7)" do
    path = Path.join(System.tmp_dir!(), "snapsync_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)

    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    :ok = Connection.exec(conn, "PRAGMA synchronous=FULL")

    # do_snapshot sets synchronous=OFF, then the VACUUM step raises (non-binary dest). The raise must
    # NOT leave the connection at OFF.
    try do
      Fathom.Shard.do_snapshot(conn, :not_a_binary)
      flunk("expected do_snapshot to raise on a non-binary dest")
    rescue
      _ -> :ok
    end

    # PRAGMA synchronous: 0=OFF, 1=NORMAL, 2=FULL. Pre-fix this stayed 0 (OFF).
    assert {:ok, %{rows: [[2]]}} = Connection.query(conn, "PRAGMA synchronous", []),
           "synchronous must be restored to FULL (2) after a raising VACUUM step"
  end
end
