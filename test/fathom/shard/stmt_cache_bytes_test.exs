defmodule Fathom.Shard.StmtCacheBytesTest do
  @moduledoc """
  The prepared-statement cache is bounded by SQLite statement BYTES, not only by count (expert
  review 2026-10-20-2027 #7), and a pooled handle carries it in a nearly copy-free form (#20).

  Symptom pinned (#7): the cache was capped at 64 entries whatever their width, so 64 Django-shaped
  29-column SELECTs held ~640 KiB of SQLite heap per connection (measured with
  `fathom_stmt_memused`), multiplied by every open stream on the node. Invariant: after any miss the
  measured bytes of the cached statements fit `:stmt_cache_bytes`, except that the statement just
  prepared is never evicted.

  Symptom pinned (#20): the carried cache crossed two process boundaries per pooled stream cycle,
  each a full heap copy of every entry's column list and wire encoding (110 KiB / ~66 us at 64 wide
  entries). Invariant: the carried form holds no per-column terms, and the next stream still gets
  the exact columns and wire encoding back.

  Not async: these tests set the node-global `:stmt_cache_bytes` / `:sqlite_extension` env.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  @wide_cols 29

  setup do
    path = Path.join(System.tmp_dir!(), "stmtbytes_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)

    prev = Application.fetch_env(:fathom, :stmt_cache_bytes)

    on_exit(fn ->
      case prev do
        {:ok, v} -> Application.put_env(:fathom, :stmt_cache_bytes, v)
        :error -> Application.delete_env(:fathom, :stmt_cache_bytes)
      end
    end)

    {:ok, path: path}
  end

  defp open_wide(path) do
    {:ok, conn} = Connection.open(path)
    cols = Enum.map_join(1..@wide_cols, ", ", &"c#{&1} TEXT")
    :ok = Connection.exec(conn, "CREATE TABLE IF NOT EXISTS w (id INTEGER PRIMARY KEY, #{cols})")
    conn
  end

  # Django-shaped: an explicit qualified column list; `i` makes each statement distinct.
  defp wide_sql(i) do
    sel = Enum.map_join(1..@wide_cols, ", ", &~s("w"."c#{&1}"))
    ~s(SELECT "w"."id", #{sel} FROM "w" WHERE "w"."id" = ? AND #{i} = #{i} LIMIT 21)
  end

  defp entries(conn) do
    {_seq, cache} = Process.get({Connection, :stmt_cache, conn})
    cache
  end

  defp entry_bytes({_stmt, _stamp, _gen, bytes, _packed}), do: bytes

  defp bytes(conn), do: entries(conn) |> Enum.map(fn {_k, e} -> entry_bytes(e) end) |> Enum.sum()

  test "fathom_stmt_memused reports this connection's statement bytes", %{path: path} do
    conn = open_wide(path)
    sql = wide_sql(1)
    {:ok, _} = Connection.query(conn, sql, [1])

    assert {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT fathom_stmt_memused(?1)", [sql])
    assert is_integer(n) and n > 4096, "a 30-column SELECT measured #{inspect(n)} bytes"

    assert {:ok, %{rows: [[nil]]}} =
             Connection.query(conn, "SELECT fathom_stmt_memused(?1)", ["SELECT 'never prepared'"])

    assert {:ok, %{rows: [[total]]}} =
             Connection.query(conn, "SELECT fathom_stmt_memused(NULL)", [])

    assert total > n

    # The cache recorded the same figure the function reports.
    assert entry_bytes(entries(conn)[sql]) == n
    Connection.close(conn)
  end

  test "a miss evicts LRU entries until the cached bytes fit the budget", %{path: path} do
    conn = open_wide(path)
    {:ok, _} = Connection.query(conn, wide_sql(0), [1])
    one = entry_bytes(entries(conn)[wide_sql(0)])
    assert one > 4096, "precondition: a wide statement measured only #{one} bytes"

    budget = 4 * one + div(one, 2)
    Application.put_env(:fathom, :stmt_cache_bytes, budget)

    for i <- 1..20 do
      {:ok, r} = Connection.query(conn, wide_sql(i), [1])
      assert length(r.columns) == @wide_cols + 1
      assert bytes(conn) <= budget, "cache holds #{bytes(conn)} bytes over a #{budget} budget"
    end

    cache = entries(conn)
    assert Map.has_key?(cache, wide_sql(20)), "the statement just prepared was evicted"
    overlay = Process.get({Connection, :stmt_cols, conn})
    assert map_size(overlay) <= map_size(cache), "evicted statements left decoded columns behind"
    refute Map.has_key?(cache, wide_sql(1)), "the least recently used statement survived"
    assert map_size(cache) < 20

    # Without the byte bound the same workload keeps every statement (the count cap is 64).
    Application.put_env(:fathom, :stmt_cache_bytes, nil)
    for i <- 21..40, do: {:ok, _} = Connection.query(conn, wide_sql(i), [1])
    assert bytes(conn) > budget
    Connection.close(conn)
  end

  test "a statement wider than the whole budget still caches", %{path: path} do
    Application.put_env(:fathom, :stmt_cache_bytes, 1)
    conn = open_wide(path)
    {:ok, _} = Connection.query(conn, wide_sql(1), [1])
    {:ok, _} = Connection.query(conn, wide_sql(2), [1])
    assert Map.has_key?(entries(conn), wide_sql(2))
    refute Map.has_key?(entries(conn), wide_sql(1))
    Connection.close(conn)
  end

  test "without the extension the size is estimated from the SQL, keeping the bound", %{
    path: path
  } do
    prev = Application.fetch_env(:fathom, :sqlite_extension)
    Application.put_env(:fathom, :sqlite_extension, false)

    on_exit(fn ->
      case prev do
        {:ok, v} -> Application.put_env(:fathom, :sqlite_extension, v)
        :error -> Application.delete_env(:fathom, :sqlite_extension)
      end
    end)

    conn = open_wide(path)
    sql = wide_sql(1)
    {:ok, _} = Connection.query(conn, sql, [1])
    assert entry_bytes(entries(conn)[sql]) == 16 * byte_size(sql)
    Connection.close(conn)
  end

  describe "the carried (pooled) form (#20)" do
    test "holds no per-column terms and round-trips columns and wire encoding", %{path: path} do
      Application.put_env(:fathom, :stmt_cache_bytes, nil)
      conn = open_wide(path)
      sqls = for i <- 1..16, do: wide_sql(i)

      before =
        Map.new(sqls, fn sql ->
          {:ok, r} = Connection.query(conn, sql, [1])
          {sql, {r.columns, r.cols_encoded}}
        end)

      live_words = :erts_debug.size(Process.get({Connection, :stmt_cache, conn}))
      carried = Connection.detach_statements(conn)
      carried_words = :erts_debug.size(carried)
      assert Process.get({Connection, :stmt_cols, conn}) == nil

      # A 30-column entry's column names and wire form are ~1.7 KiB of heap; carried, an entry is
      # the statement ref plus two refc binaries. Measured 28 KiB -> 4 KiB at 16 entries. The
      # decoded form stays behind in this process and is dropped with the detach.
      assert carried_words * 8 < 400 * map_size(elem(carried, 1)),
             "carried cache is #{carried_words * 8} bytes for #{map_size(elem(carried, 1))} " <>
               "entries (live #{live_words * 8}): it still copies the column lists"

      # The next stream: a fresh process adopts it, as a pooled reuse does.
      task =
        Task.async(fn ->
          :ok = Connection.adopt_statements(conn, carried)

          Map.new(sqls, fn sql ->
            {:ok, r} = Connection.query(conn, sql, [1])
            {sql, {r.columns, r.cols_encoded}}
          end)
          |> then(&{&1, Connection.detach_statements(conn)})
        end)

      {after_adopt, carried_again} = Task.await(task)
      assert after_adopt == before

      # Every carried hit was a hit: no entry re-prepared, and the statements are the same refs.
      stmts = fn {_seq, c} -> Map.new(c, fn {k, e} -> {k, elem(e, 0)} end) end
      assert stmts.(carried_again) == stmts.(carried)

      :ok = Connection.close_pooled({conn, carried_again})
    end

    test "a carried entry whose schema moved re-prepares and reports the new columns", %{
      path: path
    } do
      conn = open_wide(path)
      sql = ~s(SELECT * FROM "w" WHERE "w"."id" = ?)
      {:ok, r0} = Connection.query(conn, sql, [1])
      carried = Connection.detach_statements(conn)

      :ok = Connection.exec(conn, ~s(ALTER TABLE "w" ADD COLUMN extra TEXT))
      Fathom.Shard.SchemaGen.bump()

      :ok = Connection.adopt_statements(conn, carried)
      {:ok, r1} = Connection.query(conn, sql, [1])
      assert length(r1.columns) == length(r0.columns) + 1
      assert List.last(r1.columns) == "extra"
      assert r1.cols_encoded == Filo.StmtResult.encode_cols(r1.columns) |> flatten()
      Connection.close(conn)
    end
  end

  defp flatten(%{json: json, protobuf: pb}), do: %{json: json, protobuf: IO.iodata_to_binary(pb)}
end
