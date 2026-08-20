defmodule Fathom.ShardCorruptFlushTest do
  @moduledoc """
  Pre-flush page-integrity gate (expert review 2026-07-14 #4): a locally corrupted db must never
  be flushed over the last-good stored object via the checkpoint-then-raw-upload fast path (the
  legitimate owner's If-Match succeeds, so the fence can't help — the corruption would become the
  permanent, only truth). Not async: real shard files + the global registry.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Connection
  alias Filo.Stmt

  @seed_insert "INSERT INTO t (v) SELECT 'row' || i || '_pad_pad_pad_pad_pad_pad_pad' " <>
                 "FROM (WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 500) " <>
                 "SELECT i FROM s)"

  setup do
    shard = "corrupt_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 5_000)
      rm_shard(shard)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # Write through the coordinator's checkout path and fold the WAL into the main .db, so the data
  # lives in the file the drop will upload (and the WAL is empty ⇒ the drop's checkpoint is a no-op
  # that won't touch the corrupt data page).
  defp write_and_checkpoint(shard, sqls) do
    {:ok, handle} = ShardExecutor.open(shard)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, stmt(s)) end)
    {:ok, _} = ShardExecutor.execute(handle, stmt("PRAGMA wal_checkpoint(TRUNCATE)"))
    :ok = ShardExecutor.close(handle)
  end

  defp corrupt_page!(path) do
    size = File.stat!(path).size
    assert size > 8192, "seeded db too small (#{size}B) to corrupt a data page"
    {:ok, fd} = :file.open(path, [:read, :write, :binary, :raw])
    # Overwrite page 2 (offset 4096) with garbage — a b-tree data page. Page 1's header stays
    # intact so the file still opens; quick_check then finds the corrupt page.
    :ok = :file.pwrite(fd, 4096, :binary.copy(<<0xEF>>, 4096))
    :file.close(fd)
  end

  describe "verify_integrity/1" do
    test "a healthy db passes" do
      path = tmp_db()
      {:ok, conn} = Connection.open(path)
      :ok = Connection.exec(conn, "CREATE TABLE t (v TEXT)")
      :ok = Connection.exec(conn, @seed_insert)
      {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
      :ok = Connection.close(conn)

      assert :ok = Shard.verify_integrity(path)
      rm_file(path)
    end

    test "a page-corrupted db fails" do
      path = tmp_db()
      {:ok, conn} = Connection.open(path)
      :ok = Connection.exec(conn, "CREATE TABLE t (v TEXT)")
      :ok = Connection.exec(conn, @seed_insert)
      {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
      :ok = Connection.close(conn)

      corrupt_page!(path)
      assert {:error, _} = Shard.verify_integrity(path)
      rm_file(path)
    end
  end

  test "a corrupt local db is quarantined, not flushed over the good stored object", %{
    shard: shard
  } do
    path = Shard.db_path(shard)
    remote = Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])

    # 1. Seed 500 good rows and flush them to storage → the stored object is good.
    write_and_checkpoint(shard, ["CREATE TABLE t (v TEXT)", @seed_insert])
    :ok = Shards.drain(shard, 10_000)
    assert File.exists?(remote)
    good_bytes = File.read!(remote)

    # 2. Re-open (cold-pull the good object), add a row, fold into the main .db (dirty, WAL empty).
    write_and_checkpoint(shard, ["INSERT INTO t VALUES ('more')"])

    # 3. Corrupt a data page of the LOCAL db.
    corrupt_page!(path)

    # 4. Drop: the pre-flush quick_check must catch the corruption and refuse the flush.
    :ok = Shards.drain(shard, 10_000)

    # 5. The good stored object was NOT clobbered — byte-identical and still quick_check-clean.
    assert File.read!(remote) == good_bytes
    assert :ok = Shard.verify_integrity(remote)

    # 6. The corrupt local copy was quarantined for forensics (not silently dropped).
    assert Path.wildcard(path <> ".corrupt.*") != []
  end

  # THE SERVING PATH (expert review 2026-08-20 #2). Everything above drives `Shards.drain/2` — the
  # DROP path, where quarantining is correct because the coordinator is terminating. The PERIODIC
  # path, which runs while the coordinator stays alive and keeps handing `state.path` to new
  # checkouts, had never been driven. It used to call the same quarantine_corrupt!/2, which renamed
  # the live file and unlinked its -shm, so the next connection created a brand-new EMPTY database
  # at that path — and the next flush PUT that empty db over the good stored object under a valid
  # If-Match, destroying the only good copy.
  #
  # Discriminates: against the unfixed lib/ this fails at the very first assertion (the live file
  # is renamed to .corrupt.<ts>), and if that were somehow tolerated it fails again on the empty-db
  # and clobbered-object assertions.
  test "a corrupt live db is REFUSED but left in place while the shard is still being served", %{
    shard: shard
  } do
    path = Shard.db_path(shard)
    remote = Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])

    # 1. Seed and flush → the stored object is good.
    write_and_checkpoint(shard, ["CREATE TABLE t (v TEXT)", @seed_insert])
    :ok = Shards.drain(shard, 10_000)
    good_bytes = File.read!(remote)

    # 2. Re-open (cold-pull), write, fold into the main .db. The coordinator stays up from here on.
    write_and_checkpoint(shard, ["INSERT INTO t VALUES ('more')"])
    size_before = File.stat!(path).size

    # 3. Corrupt a data page of the LIVE db.
    corrupt_page!(path)

    # 4. Flush through the SERVING path (Shards.flush/1 → :flush_now → :durability_flush), twice.
    #    The second call is the one that used to upload the empty database.
    _ = Shards.flush(shard)
    _ = Shards.flush(shard)

    # 5. The live file was NOT moved aside. This is the assertion that fails pre-fix.
    assert Path.wildcard(path <> ".corrupt.*") == [],
           "the serving path quarantined the live db out from under its own checkouts"

    # 6. No brand-new empty database was created at the live path.
    assert File.exists?(path), "the live shard file disappeared while the shard was being served"

    assert File.stat!(path).size == size_before,
           "the live path holds a different (empty) database than the one we corrupted"

    # 7. The good stored object was never clobbered.
    assert File.read!(remote) == good_bytes
    assert :ok = Shard.verify_integrity(remote)
  end

  defp tmp_db,
    do: Path.join(System.tmp_dir!(), "fathom_qc_#{System.unique_integer([:positive])}.db")

  defp rm_file(path), do: for(s <- ["", "-wal", "-shm"], do: File.rm(path <> s))

  defp rm_shard(id) do
    for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
        s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([dir, "#{id}.db"]) <> s)
    end

    for f <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{id}.db.corrupt.*"])),
        do: File.rm(f)
  end
end
