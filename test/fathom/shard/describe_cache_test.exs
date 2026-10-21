defmodule Fathom.Shard.DescribeCacheTest do
  @moduledoc """
  `Connection.describe/2` goes through the statement cache (expert review 2026-10-21 #16a).

  Symptom: the libSQL Rust/Python SDKs send one Hrana `describe` before every statement, and
  describe used to prepare + `columns` + release a fresh statement every time (3 dirty dispatches,
  the compiled statement thrown away) while the cache held the same compiled statement. The
  invariants pinned here, by deterministic dispatch counts rather than timings:

    * a describe HIT is zero dirty dispatches (the non-dirty `bind_parameter_count` only);
    * a describe MISS populates the cache, so the execute that follows pays exactly what an execute
      after another execute pays (no prepare), and the reverse (execute then describe) is free;
    * a stale-generation hit must not leave an entry that looks fresh with stale columns: the next
      execute must still report the post-DDL column list (`cached_stmt/3` `refresh?` contract).

  Not pinned (parked, #16b): `describe("X")` and `execute("X;")` stay two cache entries.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Test.NifDispatchCounter, as: Counter

  @sel "SELECT id, x FROM t WHERE id = ?"

  setup do
    path = Path.join(System.tmp_dir!(), "desc_cache_#{System.unique_integer([:positive])}.db")
    {:ok, a} = Connection.open(path)
    {:ok, b} = Connection.open(path)

    on_exit(fn ->
      Connection.close(a)
      Connection.close(b)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    :ok = Connection.exec(a, "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")
    :ok = Connection.exec(a, "INSERT INTO t VALUES (1, 'hello')")
    %{a: a, b: b}
  end

  defp dispatches(fun) do
    {res, counts} = Counter.measure(fun)
    {res, counts.total}
  end

  # A binary that is not the cached key's own term, like a decoded Hrana frame.
  defp fresh(sql), do: :binary.copy(sql)

  test "describe -> execute -> describe: the miss populates the cache, the repeat is free", %{
    b: b
  } do
    # Steady-state cost of one execute (warm cache), measured on a different statement text of the
    # same shape so it does not populate the entry under test.
    warm = "SELECT id, x FROM t WHERE id = ? AND 1 = 1"
    {:ok, _} = Connection.query(b, warm, [1], read?: true, dml?: false)
    {{:ok, _}, steady_exec} = dispatches(fn -> Connection.query(b, warm, [1]) end)
    assert steady_exec > 0

    assert {{:ok, %{params: [nil], cols: ["id", "x"]}}, miss} =
             dispatches(fn -> Connection.describe(b, fresh(@sel)) end)

    assert miss > 0, "the first describe has to prepare"

    # THE ASSERTION: execute after a describe miss is a cache hit (pre-fix it re-prepared).
    assert {{:ok, %{rows: [[1, "hello"]]}}, exec} =
             dispatches(fn -> Connection.query(b, fresh(@sel), [1]) end)

    assert exec == steady_exec,
           "an execute after describe cost #{exec} dispatches, a warm execute costs " <>
             "#{steady_exec}: describe did not populate the statement cache"

    # THE ASSERTION: a describe hit pays no dirty dispatch at all (pre-fix: 3).
    assert {{:ok, %{params: [nil], cols: ["id", "x"]}}, 0} =
             dispatches(fn -> Connection.describe(b, fresh(@sel)) end)
  end

  test "execute -> describe: an executed statement is described for free", %{b: b} do
    {:ok, _} = Connection.query(b, @sel, [1])

    assert {{:ok, %{params: [nil], cols: ["id", "x"]}}, 0} =
             dispatches(fn -> Connection.describe(b, fresh(@sel)) end)
  end

  test "describe of an invalid statement is an error and caches nothing", %{b: b} do
    assert {:error, _} = Connection.describe(b, "SELEKT nonsense")
    assert {:error, _} = Connection.describe(b, "SELECT * FROM no_such_table")
    assert {:ok, %{params: [], cols: ["id", "x"]}} = Connection.describe(b, "SELECT * FROM t")
  end

  test "a describe over a moved SchemaGen does not mask the stale column list", %{a: a, b: b} do
    # B caches `SELECT *` (two columns).
    assert {:ok, %{columns: ["id", "x"]}} = Connection.query(b, "SELECT * FROM t", [])

    :ok = Connection.exec(a, "ALTER TABLE t ADD COLUMN y TEXT DEFAULT ('0')")
    Fathom.Shard.SchemaGen.bump()

    # The describe sees the moved generation. If it re-prepared in place and re-stamped the entry
    # with the CURRENT generation, the execute below would take the ordinary hit path and report the
    # pre-DDL two-column list over three-value rows.
    assert {:ok, %{params: []}} = Connection.describe(b, "SELECT * FROM t")

    assert {:ok, %{columns: cols, rows: [row]}} = Connection.query(b, "SELECT * FROM t", [])
    assert cols == ["id", "x", "y"]
    assert length(cols) == length(row)
  end

  test "a describe over the byte budget evicts like an execute (the entry still caches)", %{b: b} do
    prev = Application.get_env(:fathom, :stmt_cache_bytes)
    Application.put_env(:fathom, :stmt_cache_bytes, 1)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :stmt_cache_bytes),
        else: Application.put_env(:fathom, :stmt_cache_bytes, prev)
    end)

    # A statement wider than the budget still caches (the just-prepared one is never evicted), and
    # the next distinct statement evicts it rather than growing the cache.
    assert {:ok, _} = Connection.describe(b, @sel)
    assert {{:ok, _}, 0} = dispatches(fn -> Connection.describe(b, @sel) end)
    assert {:ok, _} = Connection.describe(b, "SELECT x FROM t")
    {_seq, cache} = Process.get({Connection, :stmt_cache, b})
    assert map_size(cache) == 1
  end
end
