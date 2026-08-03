defmodule Fathom.Shard.SnapshotSynchronousTest do
  @moduledoc """
  `synchronous` around the durability snapshot (expert review 2026-08-01 #5, and #30 item 7,
  which observed that `grep -rl synchronous test/` returned nothing under `test/fathom/shard/` —
  the invariant existed only as a comment).

  ## What #5 was

  `Fathom.Shard.snapshot/2` relaxes the SNAPSHOT connection to `synchronous=OFF` so the throwaway
  `VACUUM INTO` temp is not force-fsynced (2026-07-24 #11 — ~128 MB/interval of pure-waste device
  writes at 1,000 write-active shards). The relaxation then stayed in effect for the `PASSIVE`
  checkpoint and the close. `synchronous` is a pager property from which SQLite derives its
  checkpoint sync flags, so at OFF a checkpoint neither fsyncs the WAL before backfill nor the
  main database after — and the close-time checkpoint on the last connection to a WAL database
  also UNLINKS `-wal`/`-shm`. Main-database pages written without fsync, then the only recovery
  source deleted, is a torn database on power loss, which is then uploaded as the durable object.

  ## WHAT THESE TESTS DO NOT DO — read this before trusting them

  **They do not reproduce #5.** The bug's consequence is visible only if the machine loses power
  between the un-fsynced checkpoint and the next fsync. Absent a crash, a database checkpointed
  at `synchronous=OFF` and one checkpointed at `FULL` are byte-identical — SQLite writes the same
  pages either way, it just does not durably commit them. Deleting the `PRAGMA synchronous=FULL`
  restore in `shard.ex` leaves every assertion here green. That is stated plainly rather than
  dressed up: the fix rests on the SQLite pager semantics argued in the comment at that line, and
  these tests are invariant guards around it, not a regression test for it.

  What they DO pin, all of which was previously unassertable because pragma state is
  per-connection and invisible from outside:

    * the serving connection's `synchronous` is `FULL` (2) and stays there across flushes — the
      local half of the RPO contract, and the thing the relaxation must never leak into;
    * `Connection.open/1` establishes `FULL` rather than inheriting a process/global default;
    * a real flush leaves the live database structurally intact (`quick_check`) with its rows —
      which is the observable that WOULD break if the sequence damaged the file for any reason
      other than power loss.
  """
  use Fathom.ClusterShardCase, async: false

  alias Fathom.Shard.Connection

  # SQLite's numeric mapping: 0 = OFF, 1 = NORMAL, 2 = FULL, 3 = EXTRA.
  @full 2

  test "a fresh connection is opened at synchronous=FULL" do
    path =
      Path.join(Fathom.Shard.data_dir(), "syncopen_#{System.unique_integer([:positive])}.db")

    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)

    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    assert {:ok, @full} = Connection.pragma(conn, "synchronous"),
           "the per-commit fsync that bounds local RPO below the flush interval is not on"
  end

  test "the snapshot's relaxation never reaches a serving connection, across repeated flushes",
       %{shard: shard} do
    # The relaxation is scoped to the snapshot's OWN connection. A serving stream must observe
    # FULL before and after — including after several flush cycles, since the failure mode would
    # be a leak that only appears once a flush has actually run.
    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)

      assert {:ok, %{rows: [[@full]]}} = ShardExecutor.execute(conn, stmt("PRAGMA synchronous"))

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))

      for i <- 1..3 do
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?)", ["v#{i}"]))
        :ok = Fathom.Shards.flush(shard)

        assert {:ok, %{rows: [[@full]]}} = ShardExecutor.execute(conn, stmt("PRAGMA synchronous")),
               "the snapshot's synchronous=OFF leaked into a serving connection after flush #{i}"
      end

      :ok = ShardExecutor.close(conn)
    end)
  end

  test "the live database survives the snapshot's checkpoint + close intact", %{shard: shard} do
    # The structural observable. It cannot see the missing fsync (see the moduledoc), but it does
    # catch the sequence damaging the file for any reason that does not need a power cut.
    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))

      for i <- 1..20 do
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?)", ["row#{i}"]))
      end

      :ok = Fathom.Shards.flush(shard)
      :ok = ShardExecutor.close(conn)
      :ok = Shards.drain(shard, 5_000)

      # Re-open cold from the stored object and verify both structure and content.
      {:ok, conn2} = ShardExecutor.open(shard)

      assert {:ok, %{rows: [["ok"]]}} =
               ShardExecutor.execute(conn2, stmt("PRAGMA quick_check")),
             "the snapshot's checkpoint/close left the database malformed"

      assert {:ok, %{rows: [[20]]}} =
               ShardExecutor.execute(conn2, stmt("SELECT count(*) FROM kv"))

      :ok = ShardExecutor.close(conn2)
    end)
  end
end
