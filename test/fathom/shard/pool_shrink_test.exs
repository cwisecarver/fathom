defmodule Fathom.Shard.PoolShrinkTest do
  @moduledoc """
  `PRAGMA shrink_memory` on POOLED IDLE handles (expert review 2026-10-21 #15).

  SYMPTOM. An idle connection keeps whatever pages its last statement touched in its SQLite page
  cache (up to `cache_size`, 2 MiB) for as long as it lives; exqlite is built without
  `SQLITE_ENABLE_MEMORY_MANAGEMENT`, so the soft heap limit never takes them back. The pool's sweep
  timer now also finds handles idle past `:shrink_idle_ms` and shrinks them on a side process.

  MEASURE. `fathom_cache_used()` (the extension; `sqlite3_db_status(CACHE_USED)`) on the pooled
  handle itself: a real SQLite number, not a proxy.

  INVARIANT. A handle being shrunk is never lent (`HandlePool.take/2` skips `:shrinking`), and is
  lendable and correct afterwards.

  Not async: shards are global and file-backed.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard
  alias Fathom.Shard.{HandlePool, Storage, WriteFence}
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @scan "SELECT sum(length(b)) FROM big"
  @rows 3_000

  describe "HandlePool (pure)" do
    test "begin_shrink marks only handles idle past the threshold; take/2 skips :shrinking" do
      pool = HandlePool.new(max_per_scope: 4, shrink_idle_ms: 1_000)
      {pool, []} = HandlePool.put(pool, :rw, :old, 0)
      {pool, []} = HandlePool.put(pool, :rw, :young, 900)

      assert {pool, [:old]} = HandlePool.begin_shrink(pool, 1_000)
      # :young is the MRU and lendable; :old is mid-shrink and must not be lent.
      assert {:hit, :young, pool} = HandlePool.take(pool, :rw)
      assert {:miss, pool} = HandlePool.take(pool, :rw)
      assert HandlePool.count(pool) == 1, "a :shrinking handle stays pooled so drain closes it"

      # Not marked twice, and lendable once the shrink is done.
      assert {^pool, []} = HandlePool.begin_shrink(pool, 5_000)
      pool = HandlePool.end_shrink(pool, [:old])
      refute HandlePool.shrink_pending?(pool)
      assert {:hit, :old, _} = HandlePool.take(pool, :rw)
    end

    test "0 turns it off; sweep keeps a :shrinking handle past ttl; drain returns it" do
      off = HandlePool.new(shrink_idle_ms: 0)
      {off, []} = HandlePool.put(off, :ro, :h, 0)
      assert {^off, []} = HandlePool.begin_shrink(off, 1_000_000)
      refute HandlePool.shrink_pending?(off)

      pool = HandlePool.new(ttl_ms: 100, shrink_idle_ms: 10)
      {pool, []} = HandlePool.put(pool, :ro, :h, 0)
      {pool, [:h]} = HandlePool.begin_shrink(pool, 50)
      assert {pool, []} = HandlePool.sweep(pool, 10_000)
      assert {[:h], _} = HandlePool.drain(pool)
    end
  end

  describe "a pooled idle handle sheds its page cache" do
    setup do
      keys = [:connection_pool, :connection_pool_opts, :shard_idle_ms]
      prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})
      Application.put_env(:fathom, :connection_pool, true)
      Application.put_env(:fathom, :shard_idle_ms, 60_000)
      shard = "pool_shrink_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        _ = Shards.stop(shard)
        WriteFence.forget(shard)

        Enum.each(prev, fn
          {k, nil} -> Application.delete_env(:fathom, k)
          {k, v} -> Application.put_env(:fathom, k, v)
        end)

        for dir <- [Shard.data_dir(), Storage.Local.dir()],
            suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
            do: File.rm(Path.join(dir, shard <> suffix))
      end)

      %{shard: shard}
    end

    defp opts(shrink_ms) do
      Application.put_env(:fathom, :connection_pool_opts,
        drain_grace_ms: 60_000,
        shrink_idle_ms: shrink_ms
      )
    end

    defp stmt(sql), do: %Stmt{sql: sql, args: []}

    # Write a >2 MiB table, scan it on a stream, close the stream: the handle is pooled holding the
    # scan's pages. Returns {coordinator, the pooled conn}.
    defp scanned_and_pooled!(shard) do
      {:ok, h} = ShardExecutor.open(shard)

      {:ok, _} =
        ShardExecutor.execute(h, stmt("CREATE TABLE big (id INTEGER PRIMARY KEY, b BLOB)"))

      {:ok, _} =
        ShardExecutor.execute(
          h,
          stmt(
            "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < #{@rows}) " <>
              "INSERT INTO big(b) SELECT randomblob(1024) FROM s"
          )
        )

      {:ok, _} = ShardExecutor.execute(h, stmt(@scan))
      :ok = ShardExecutor.close(h)
      {pid, _ref, conn, _id, _sc, _v, _o} = h
      assert HandlePool.count(:sys.get_state(pid).pool) == 1, "precondition: handle is pooled"
      {pid, conn}
    end

    defp cache_used(conn) do
      {:ok, stmt} = Sqlite3.prepare(conn, "SELECT fathom_cache_used()")
      {:row, [n]} = Sqlite3.step(conn, stmt)
      :ok = Sqlite3.release(conn, stmt)
      n
    end

    defp eventually(fun, tries \\ 300) do
      cond do
        fun.() ->
          true

        tries == 0 ->
          false

        true ->
          Process.sleep(10)
          eventually(fun, tries - 1)
      end
    end

    defp shrinking?(pid),
      do: Enum.any?(:sys.get_state(pid).pool.idle.rw, &match?({_, _, :shrinking}, &1))

    test "shrink OFF (0): the idle pooled handle keeps its scan cache (control)", %{shard: shard} do
      opts(0)
      {pid, conn} = scanned_and_pooled!(shard)
      _ = :sys.get_state(pid)
      Process.sleep(150)
      assert cache_used(conn) > 1_000_000, "the fixture did not fill the cache; nothing to shrink"
    end

    test "shrink ON: cache is released after the idle threshold; the next checkout is correct",
         %{shard: shard} do
      opts(50)
      {pid, conn} = scanned_and_pooled!(shard)

      assert eventually(fn -> cache_used(conn) < 128 * 1024 end),
             "the idle pooled handle kept #{cache_used(conn)} bytes of page cache"

      assert eventually(fn -> not shrinking?(pid) end)

      # Checkout after the shrink: the pooled handle is lent and answers correctly.
      {:ok, h2} = ShardExecutor.open(shard)
      assert {^pid, _, ^conn, _, _, _, _} = h2, "expected the shrunk handle to be reused"
      assert {:ok, res} = ShardExecutor.execute(h2, stmt(@scan))
      assert inspect(res) =~ "#{@rows * 1024}"
      :ok = ShardExecutor.close(h2)
    end

    test "checkouts racing the shrink never see a broken handle", %{shard: shard} do
      opts(1)
      {_pid, _conn} = scanned_and_pooled!(shard)

      for _ <- 1..200 do
        {:ok, h} = ShardExecutor.open(shard)
        assert {:ok, res} = ShardExecutor.execute(h, stmt(@scan))
        assert inspect(res) =~ "#{@rows * 1024}"
        :ok = ShardExecutor.close(h)
        Process.sleep(1)
      end
    end
  end
end
