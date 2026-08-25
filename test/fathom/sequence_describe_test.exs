defmodule Fathom.SequenceDescribeTest do
  @moduledoc """
  Hrana `sequence` + `describe` (expert review 2026-07-14 #34): `executescript()` runs a script,
  and `describe` introspects a statement without running it. Pins the durability trap the fix note
  calls out — a script bypasses the `wrote?` WriteCounter bump, so `execute_sequence` must bump
  unconditionally or a script-only session's writes are dropped on the next idle flush.
  """
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, HranaAuth, ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    shard = "seq_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for dir <- [local_dir(), remote_dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql}

  test "executescript runs a multi-statement script and its writes SURVIVE an idle flush",
       %{shard: shard} do
    {:ok, h} = ShardExecutor.open(shard)

    script = """
    CREATE TABLE t (v INTEGER);
    INSERT INTO t VALUES (1);
    INSERT INTO t VALUES (2);
    """

    assert :ok = ShardExecutor.execute_sequence(h, script)
    :ok = ShardExecutor.close(h)

    # Drain flushes only a DIRTY shard, then drops the local copy — if the script hadn't bumped the
    # WriteCounter the flush would be skipped and the rows lost. Reopen cold and confirm they're there.
    :ok = Shards.drain(shard, 5_000)

    {:ok, h2} = ShardExecutor.open(shard)
    assert {:ok, %{rows: [[2]]}} = ShardExecutor.execute(h2, stmt("SELECT count(*) FROM t"))
    :ok = ShardExecutor.close(h2)
  end

  test "a bad script surfaces an error the stream survives", %{shard: shard} do
    {:ok, h} = ShardExecutor.open(shard)
    assert {:error, %Filo.Error{}} = ShardExecutor.execute_sequence(h, "NOT VALID SQL;")
    :ok = ShardExecutor.close(h)
  end

  describe "describe" do
    test "reports columns, param count, and read-only-ness of a SELECT", %{shard: shard} do
      {:ok, h} = ShardExecutor.open(shard)
      :ok = ShardExecutor.execute_sequence(h, "CREATE TABLE t (a INTEGER, b TEXT);")

      assert {:ok, desc} = ShardExecutor.describe(h, "SELECT a, b FROM t WHERE a = ?")
      assert desc.cols == ["a", "b"]
      assert desc.params == [nil]
      assert desc.is_readonly == true
      assert desc.is_explain == false

      :ok = ShardExecutor.close(h)
    end

    test "a write statement is not read-only and has no result columns", %{shard: shard} do
      {:ok, h} = ShardExecutor.open(shard)
      :ok = ShardExecutor.execute_sequence(h, "CREATE TABLE t (a INTEGER);")

      assert {:ok, desc} = ShardExecutor.describe(h, "INSERT INTO t VALUES (?)")
      assert desc.is_readonly == false
      assert desc.params == [nil]
      assert desc.cols == []

      :ok = ShardExecutor.close(h)
    end

    test "flags an EXPLAIN", %{shard: shard} do
      {:ok, h} = ShardExecutor.open(shard)
      :ok = ShardExecutor.execute_sequence(h, "CREATE TABLE t (a INTEGER);")

      assert {:ok, desc} = ShardExecutor.describe(h, "EXPLAIN SELECT * FROM t")
      assert desc.is_explain == true

      :ok = ShardExecutor.close(h)
    end

    # DESCRIBE IS NOT SIDE-EFFECT-FREE (expert review 2026-08-24 #2, verified by execution).
    # "Introspect without running it" is true of the VDBE, not of SQLite: several pragmas are
    # implemented outside it and take effect during `sqlite3_prepare`, with no step at all. So a
    # describe path that skipped `blocked_statement/1` — as this one did — handed every tenant an
    # ungated setter for fathom's own safety pragmas, and one that needed no parser trick to reach.
    # Measured before the fix, on a real shard with the tenant authorizer set:
    #
    #     before:                               writable_schema = 0, synchronous = 2
    #     describe "PRAGMA writable_schema=ON"  -> {:ok, %Describe{is_readonly: true}}
    #     describe "PRAGMA synchronous=OFF"     -> {:ok, %Describe{is_readonly: true}}
    #     after:                                writable_schema = 1, synchronous = 0
    test "a blocked pragma is refused, and does not take effect at PREPARE time", %{shard: shard} do
      {:ok, h} = ShardExecutor.open(shard)
      :ok = ShardExecutor.execute_sequence(h, "CREATE TABLE t (a INTEGER);")

      for sql <- [
            "PRAGMA writable_schema=ON",
            "PRAGMA synchronous=OFF",
            "PRAGMA max_page_count=999999999",
            # The qualifier bypass (#1) is reachable through describe too.
            "PRAGMA main . writable_schema = ON"
          ] do
        assert {:error, %Filo.Error{code: "FILO_PRAGMA_BLOCKED"}} =
                 ShardExecutor.describe(h, sql),
               "describe #{inspect(sql)} was not refused"
      end

      # THE EFFECT, which is the whole finding: an error return that still mutated the connection
      # would satisfy the assertions above and miss the bug entirely.
      assert {:ok, %{rows: [[0]]}} = ShardExecutor.execute(h, stmt("PRAGMA writable_schema"))
      assert {:ok, %{rows: [[2]]}} = ShardExecutor.execute(h, stmt("PRAGMA synchronous"))

      # And describing something legitimate still works — the gate governs the statement, not the
      # request type. A `:ro` client describing a SELECT must not be caught by this.
      assert {:ok, %{is_readonly: true}} = ShardExecutor.describe(h, "SELECT a FROM t")
      assert {:ok, %{}} = ShardExecutor.describe(h, "PRAGMA table_info(t)")

      :ok = ShardExecutor.close(h)
    end
  end

  test "a read-only token cannot run a script", %{shard: shard} do
    prev = Application.get_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_auth, :required)
    on_exit(fn -> Application.put_env(:fathom, :hrana_auth, prev) end)

    {:ok, _} = Directory.resolve(shard)
    {:ok, ro} = HranaAuth.token_for(shard, scope: :ro)

    # authorize returns the token's scope; Filo threads it to open as the connection context
    # (Filo.Executor.open/2), and the executor rides it in the handle.
    assert {:ok, {:ro, _}} = HranaAuth.authorize(shard, ro)
    {:ok, h} = ShardExecutor.open(shard, :ro)

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute_sequence(h, "CREATE TABLE t (v);")

    :ok = ShardExecutor.close(h)
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
