defmodule Fathom.ShardExecutorFailedWriteTest do
  @moduledoc """
  Expert review 2026-11-07 #5. The executor's dirty accounting (`WriteCounter` bump, the
  `:became_dirty` signal, the per-transaction write mark, the A2 ship) lived only in the SUCCESS
  branches, on the assumption that an error return means nothing was committed. Two shapes make that
  false:

    * a `sequence` script: `sqlite3_exec` stops at the first error, but in autocommit every
      statement before it has already COMMITTED (`CREATE TABLE …; INSERT …; INSERT <bad>`);
    * an `INSERT/UPDATE/DELETE … RETURNING` that SQLite performs in full on its first step and the
      `:query_max_rows` cap then fails: `Connection`'s `reset_if_abandoned` `sqlite3_reset`s the
      statement, which in autocommit commits it.

  In both the shard stayed "clean", so the idle drop (`drop_clean`) deleted the local copy of data
  the client had seen and read back, with nothing uploaded. Each scenario asserts the committed data
  is visible first (so the fixture really produced the commit), that the counter moved, and that the
  data survives drain + cold reopen.

  Parameterized over both coordinator liveness modes (the drop path differs); the mode is asserted.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage, WriteCounter}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  # A 10-row INSERT … RETURNING, over a `query_max_rows` of 5.
  @ten_row_insert "INSERT INTO t SELECT x FROM " <>
                    "(WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < 10) " <>
                    "SELECT x FROM g) RETURNING v"

  setup do
    shard = "failedw_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Application.delete_env(:fathom, :query_max_rows)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql}

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(),
           "legacy mode needs the heartbeat OFF; config/test.exs default is heartbeat_server: false"
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "the mode setup did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "something started the heartbeat")

  defp count!(h, table \\ "t") do
    {:ok, %{rows: [[n]]}} = ShardExecutor.execute(h, stmt("SELECT count(*) FROM #{table}"))
    n
  end

  # Seed an empty table through the executor, drain, and reopen cold, so the shard starts CLEAN with
  # the table already in storage: the only thing that can dirty it afterwards is the scenario.
  defp open_clean_with_table!(shard) do
    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (v INTEGER NOT NULL)"))
    :ok = ShardExecutor.close(h)
    :ok = Shards.drain(shard, 5_000)

    {:ok, h} = ShardExecutor.open(shard)
    assert WriteCounter.count(shard) == 0, "fixture: the cold reopen did not start clean"
    h
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      test "a failed sequence keeps the statements that committed before the error",
           %{shard: shard} do
        set_mode!(unquote(mode))
        {:ok, {pid, _, _, _, _, _, _} = h} = ShardExecutor.open(shard)
        assert_mode!(pid, unquote(mode))
        assert WriteCounter.count(shard) == 0, "fixture: a fresh shard must start clean"

        assert {:error, %Filo.Error{code: "SQLITE_CONSTRAINT_NOTNULL"}} =
                 ShardExecutor.execute_sequence(
                   h,
                   "CREATE TABLE t (v INTEGER NOT NULL); INSERT INTO t VALUES (1); " <>
                     "INSERT INTO t VALUES (NULL);"
                 )

        # The fixture really committed the first two statements (autocommit, exec stops at the error).
        assert count!(h) == 1, "fixture: the script did not commit before it failed"

        assert WriteCounter.count(shard) > 0,
               "a failed script that committed left the shard clean"

        :ok = ShardExecutor.close(h)
        :ok = Shards.drain(shard, 5_000)

        # Pre-fix: the clean drop deleted the file with nothing uploaded, so this reopened an empty
        # shard and the SELECT failed with "no such table".
        {:ok, h2} = ShardExecutor.open(shard)
        assert count!(h2) == 1
        :ok = ShardExecutor.close(h2)
      end

      test "a capped INSERT … RETURNING that committed is counted and survives the drop",
           %{shard: shard} do
        set_mode!(unquote(mode))
        Application.put_env(:fathom, :query_max_rows, 5)
        {pid, _, _, _, _, _, _} = h = open_clean_with_table!(shard)
        assert_mode!(pid, unquote(mode))

        assert {:error, %Filo.Error{code: "FILO_RESULT_TOO_LARGE"}} =
                 ShardExecutor.execute(h, stmt(@ten_row_insert))

        # SQLite performed (and, on reset, committed) the whole DML.
        assert count!(h) == 10, "fixture: the capped statement did not commit its rows"
        assert WriteCounter.count(shard) > 0, "a capped RETURNING DML left the shard clean"

        :ok = ShardExecutor.close(h)
        :ok = Shards.drain(shard, 5_000)

        {:ok, h2} = ShardExecutor.open(shard)
        assert count!(h2) == 10
        :ok = ShardExecutor.close(h2)
      end

      test "a capped RETURNING DML inside a transaction is re-counted at COMMIT", %{shard: shard} do
        set_mode!(unquote(mode))
        Application.put_env(:fathom, :query_max_rows, 5)
        h = open_clean_with_table!(shard)

        {:ok, _} = ShardExecutor.execute(h, stmt("BEGIN"))

        assert {:error, %Filo.Error{code: "FILO_RESULT_TOO_LARGE"}} =
                 ShardExecutor.execute(h, stmt(@ten_row_insert))

        # Pre-fix this transaction looked write-free, so the COMMIT's boundary re-bump never ran.
        {:ok, _} = ShardExecutor.execute(h, stmt("COMMIT"))
        assert count!(h) == 10, "fixture: the capped statement's rows were not in the transaction"
        assert WriteCounter.count(shard) > 0

        :ok = ShardExecutor.close(h)
        :ok = Shards.drain(shard, 5_000)

        {:ok, h2} = ShardExecutor.open(shard)
        assert count!(h2) == 10
        :ok = ShardExecutor.close(h2)
      end
    end
  end

  test "a failed sequence of one statement, or of reads only, stays clean", %{shard: shard} do
    h = open_clean_with_table!(shard)

    # One statement is atomic: its failure committed nothing, so it must not cost a flush.
    assert {:error, _} = ShardExecutor.execute_sequence(h, "INSERT INTO t VALUES (NULL);")
    assert WriteCounter.count(shard) == 0

    assert {:error, _} = ShardExecutor.execute_sequence(h, "SELECT 1; SELECT * FROM nope;")
    assert WriteCounter.count(shard) == 0

    :ok = ShardExecutor.close(h)
  end
end
