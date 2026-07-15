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
    remote = Path.join([System.tmp_dir!(), "fathom_remote_test", "#{shard}.db"])

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

  defp tmp_db,
    do: Path.join(System.tmp_dir!(), "fathom_qc_#{System.unique_integer([:positive])}.db")

  defp rm_file(path), do: for(s <- ["", "-wal", "-shm"], do: File.rm(path <> s))

  defp rm_shard(id) do
    for dir <- ["fathom_shards", "fathom_remote_test"], s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([System.tmp_dir!(), dir, "#{id}.db"]) <> s)
    end

    for f <- Path.wildcard(Path.join([System.tmp_dir!(), "fathom_shards", "#{id}.db.corrupt.*"])),
        do: File.rm(f)
  end
end
