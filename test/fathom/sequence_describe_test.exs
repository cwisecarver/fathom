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

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "seq_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for dir <- [@local_dir, @remote_dir],
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
  end

  test "a read-only token cannot run a script", %{shard: shard} do
    prev = Application.get_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_auth, :required)
    on_exit(fn -> Application.put_env(:fathom, :hrana_auth, prev) end)

    {:ok, _} = Directory.resolve(shard)
    {:ok, ro} = HranaAuth.token_for(shard, scope: :ro)

    # authorize returns the token's scope; Filo threads it to open as the connection context
    # (Filo.Executor.open/2), and the executor rides it in the handle.
    assert {:ok, :ro} = HranaAuth.authorize(shard, ro)
    {:ok, h} = ShardExecutor.open(shard, :ro)

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute_sequence(h, "CREATE TABLE t (v);")

    :ok = ShardExecutor.close(h)
  end
end
