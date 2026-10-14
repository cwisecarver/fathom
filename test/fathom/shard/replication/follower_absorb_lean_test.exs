defmodule Fathom.Shard.Replication.FollowerAbsorbLeanTest do
  @moduledoc """
  Perf review 2026-10-14 #13: the follower's absorb-before-reset uses a LEAN handle.

  `checkpoint_into_db/1` ran `Connection.open/1` (extension load+disable, pragma guard, page_size,
  WAL enable, five pragmas) just to TRUNCATE-checkpoint: ~450 us p50 inline on the reset push's ack
  path, 361 us of it the open. It now opens raw with the busy timeout and `synchronous=FULL` only.

  What must NOT change, and is pinned here and in `follower_absorb_result_test.exs`:

    * the absorbed pages really reach the `.db` (a promotable replica holds the rows);
    * `log == checkpointed` is still the success test (a short checkpoint is an error);

  The `:bench` test prices both shapes alternating on the same fixture (medians, noisy under load)
  and counts the Sqlite3 calls each makes (56 -> 15 measured). THE FLOOR IS NOT THE OPEN: the first
  statement on a handle over a non-empty `-wal` rebuilds the WAL index (recovery + a fresh `-shm`),
  ~300 us of the ~390 us left; `open+select+close` on the same fixture costs the same as the
  checkpoint, and `synchronous=OFF` is no cheaper. The `-shm` cannot be kept, because the follower
  appends to the WAL outside SQLite and a kept index would describe a WAL that no longer exists.
  """
  use ExUnit.Case, async: true

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower

  setup do
    db = Path.join(System.tmp_dir!(), "absorb_lean_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(db <> s) end)
    %{db: db}
  end

  # A WAL-mode db with committed frames left in the -wal (no checkpoint), as a follower holds one.
  # The writer is left OPEN so close does not checkpoint; the files are copied out as the replica.
  defp replica_with_wal!(db) do
    src = db <> ".src"
    for s <- ["", "-wal", "-shm"], do: File.rm(src <> s)
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(src <> s) end)
    {:ok, w} = Connection.open(src)
    :ok = Connection.exec(w, "PRAGMA wal_autocheckpoint=0")
    :ok = Connection.exec(w, "CREATE TABLE t (x BLOB)")
    :ok = Connection.exec(w, "INSERT INTO t VALUES (randomblob(3000))")
    :ok = Connection.exec(w, "INSERT INTO t VALUES (randomblob(3000))")
    File.cp!(src, db)
    File.cp!(src <> "-wal", db <> "-wal")
    Connection.close(w)
    assert File.stat!(db <> "-wal").size > 0, "fixture: no frames in the WAL to absorb"
  end

  test "the absorb moves the WAL's frames into the .db and empties the WAL", %{db: db} do
    replica_with_wal!(db)
    assert :ok = Follower.checkpoint_into_db(db)
    refute File.exists?(db <> "-shm")
    assert not File.exists?(db <> "-wal") or File.stat!(db <> "-wal").size == 0

    # The .db ALONE now holds both rows (read with the WAL removed).
    File.rm(db <> "-wal")
    {:ok, c} = Sqlite3.open(db, mode: :readonly)
    {:ok, stmt} = Sqlite3.prepare(c, "SELECT count(*) FROM t")
    assert {:ok, [[2]]} = Sqlite3.fetch_all(c, stmt)
    Sqlite3.release(c, stmt)
    Sqlite3.close(c)
  end

  @tag :bench
  test "BENCH: lean absorb vs the Connection.open shape, alternating", %{db: db} do
    n = 300

    old = fn ->
      {:ok, conn} = Connection.open(db)

      try do
        {:ok, %{rows: [[_, log, log]]}} =
          Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])

        :ok
      after
        Connection.close(conn)
        File.rm(db <> "-shm")
      end
    end

    new = fn -> :ok = Follower.checkpoint_into_db(db) end

    time = fn f ->
      replica_with_wal!(db)
      {us, _} = :timer.tc(f)
      us
    end

    # Warm-up, then alternate so drift hits both equally.
    for f <- [old, new], _ <- 1..20, do: time.(f)

    samples =
      for _ <- 1..n, reduce: %{old: [], new: []} do
        acc -> %{old: [time.(old) | acc.old], new: [time.(new) | acc.new]}
      end

    med = fn l -> l |> Enum.sort() |> Enum.at(div(length(l), 2)) end
    {m_old, m_new} = {med.(samples.old), med.(samples.new)}

    # Deterministic: Sqlite3 calls per absorb, traced on a worker (a process cannot trace itself).
    count = fn f ->
      replica_with_wal!(db)
      parent = self()

      worker =
        spawn(fn ->
          receive do
            :go ->
              f.()
              send(parent, :done)
          end
        end)

      :erlang.trace_pattern({Sqlite3, :_, :_}, true, [:local])
      :erlang.trace(worker, true, [:call, {:tracer, self()}])
      send(worker, :go)
      assert_receive :done, 5_000
      :erlang.trace_pattern({Sqlite3, :_, :_}, false, [:local])
      length(Enum.filter(drain([]), &match?({:trace, _, :call, {Sqlite3, _, _}}, &1)))
    end

    {calls_old, calls_new} = {count.(old), count.(new)}

    IO.puts(
      "absorb p50: Connection.open shape #{m_old} us -> lean #{m_new} us; " <>
        "Sqlite3 calls #{calls_old} -> #{calls_new} (#{n} alternating samples, noisy)"
    )

    assert calls_new < calls_old
    assert m_old - m_new > 0, "the lean absorb is not faster than the full open"
    # Order-of-magnitude ceiling (target <= 150 us idle; loose under load).
    assert m_new < 2_000
  end

  defp drain(acc) do
    receive do
      {:trace, _, _, _} = m -> drain([m | acc])
    after
      20 -> acc
    end
  end
end
