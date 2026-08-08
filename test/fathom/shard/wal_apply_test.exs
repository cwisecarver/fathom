defmodule Fathom.Shard.WalApplyTest do
  @moduledoc """
  Gate 1, receive half — Phase 2 A2 (quorum replication). See `docs/a2-quorum-replication.md`.

  `wal_hook_test.exs` proved the SEND side: `sqlite3_wal_hook` fires on the primary, so committed
  frames can be observed and WAL truncation controlled. That says nothing about whether a follower
  can **apply** them, which is the half A2 actually needs and the half stock SQLite has no public
  API for. libSQL added `libsql_wal_insert_frame`; ordinary SQLite did not.

  The question this file answers is narrow and load-bearing:

  > Can a follower consume **incrementally appended WAL bytes** and see the new rows, using stock
  > SQLite and no frame-insert API?

  If yes, A2's shipper is "read `-wal` from the last shipped offset, send the bytes, append them on
  the follower" — no NIF, no engine swap. If no, A2 needs libSQL's engine and the cost of the whole
  project changes.

  ## A closing connection CHECKPOINTS — the constraint this test discovered

  The first version of this test opened the follower to read after the base copy, closed it, then
  appended the next delta. It failed, and the failure message confidently blamed stock SQLite.
  That was wrong, and worth recording because the wrong conclusion was "adopt libSQL".

  Measured: after one open-and-close of the follower, its `.db` went **4096 → 8192 bytes** and its
  `-wal` was **deleted**. A clean close checkpoints and removes the WAL. The delta was then appended
  to a file that no longer had the lineage it was computed against.

  That is a real A2 design constraint, not a test artifact: **a follower cannot casually open and
  close its database.** Any clean close silently checkpoints, moving pages into the `.db` and
  resetting the WAL, which desynchronizes it from the primary's byte offsets. A follower is a
  passive recipient of frames until it is promoted, and the promote path has to account for the
  checkpoint that its first clean close will perform.

  So the test below models that honestly: the follower is **never opened** until every delta has
  been shipped, which is also what a real standby does.

  ## Why the assertions are shaped this way

  The trap is proving the wrong thing. Copying `.db` + `-wal` wholesale and seeing the rows would
  demonstrate only that a full file copy works — which we already knew, it is what `Storage.pull/2`
  does. So this ships **two successive deltas** as byte ranges and asserts the primary's main
  database never changed: if it had, data would have moved into the `.db` via a checkpoint and the
  deltas would be carrying nothing. The follower's `.db` is likewise asserted byte-identical to the
  base copy, proving every row after the first arrived purely as WAL bytes.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  setup do
    base = Path.join(System.tmp_dir!(), "fathom_walapply_#{System.unique_integer([:positive])}")
    primary = base <> "_p.db"
    follower = base <> "_f.db"

    on_exit(fn ->
      for p <- [primary, follower], s <- ["", "-wal", "-shm"], do: File.rm(p <> s)
    end)

    %{primary: primary, follower: follower}
  end

  defp wal(path), do: path <> "-wal"

  # The whole A2 shipper: send the bytes the primary produced since our last offset, append them
  # on the follower. Nothing rewrites headers, recomputes checksums, or touches the main database.
  defp ship_delta!(primary, follower, from_offset) do
    current = File.read!(wal(primary))

    assert byte_size(current) > from_offset,
           "the WAL did not grow past #{from_offset} — there is no delta to ship"

    delta = binary_part(current, from_offset, byte_size(current) - from_offset)
    File.write!(wal(follower), delta, [:append])
    byte_size(current)
  end

  test "a follower applies INCREMENTALLY shipped WAL bytes with stock SQLite", ctx do
    %{primary: primary, follower: follower} = ctx

    {:ok, pconn} = Connection.open(primary)
    on_exit(fn -> Connection.close(pconn) end)

    {:ok, _} = Connection.query(pconn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", [])
    {:ok, _} = Connection.query(pconn, "INSERT INTO t VALUES (1, 'first')", [])

    # --- base copy: the S3 pull a follower starts from --------------------------------------
    db_at_base = File.read!(primary)
    File.write!(follower, db_at_base)
    File.write!(wal(follower), File.read!(wal(primary)))
    offset = byte_size(File.read!(wal(primary)))

    # --- two successive deltas, follower never opened in between ------------------------------
    {:ok, _} = Connection.query(pconn, "INSERT INTO t VALUES (2, 'second')", [])
    offset = ship_delta!(primary, follower, offset)

    {:ok, _} = Connection.query(pconn, "INSERT INTO t VALUES (3, 'third')", [])
    _offset = ship_delta!(primary, follower, offset)

    # If the primary checkpointed at any point, the rows moved into the main db and the deltas
    # carried nothing — the test would "pass" while proving nothing about incremental shipping.
    assert File.read!(primary) == db_at_base,
           "the primary checkpointed mid-test, so these commits are no longer WAL-only and this " <>
             "is not an incremental-shipping test any more"

    assert File.read!(follower) == db_at_base,
           "the follower's main db changed, so something other than WAL bytes was shipped"

    # Opened exactly once, after all shipping. See the moduledoc: opening and closing earlier
    # checkpoints the follower and breaks the offset lineage.
    {:ok, fconn} = Connection.open(follower)
    on_exit(fn -> Connection.close(fconn) end)

    assert {:ok, %{rows: rows}} = Connection.query(fconn, "SELECT v FROM t ORDER BY id", [])

    assert rows == [["first"], ["second"], ["third"]],
           "the follower did NOT pick up the appended frames — incremental WAL shipping does not " <>
             "work on stock SQLite, and A2 would need libSQL's frame-insert API (see " <>
             "docs/a2-quorum-replication.md). Got: #{inspect(rows)}"
  end
end
