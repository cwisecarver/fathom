defmodule Fathom.ShardFlushSpacePreflightTest do
  @moduledoc """
  Expert review 2026-10-12 #26: no free-space preflight before the `VACUUM INTO` flush temp.

  Every flush writes a temp about the size of the shard next to the live file. On a near-full volume
  that failed late (SQLITE_FULL, after the whole `quick_check` page scan) and escalated only on the
  third consecutive failure. The flush now compares the poller's cached free-space reading
  (`Fathom.Admin.Measurements.data_free_bytes/0`, never a per-flush `df`) with the shard's size plus
  a margin, fails fast with `{:flush_no_space, need, free}`, and alerts on the FIRST occurrence.

  The reader is stood in for with `Measurements.put_data_free_bytes/1`; the real disk has plenty of
  room, so without the preflight these flushes SUCCEED and the tests below fail.
  """
  # Not async: the cached reading is node-global and would refuse other tests' flushes.
  use ExUnit.Case, async: false

  alias Fathom.Admin.Measurements
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    shard = "flush_space_#{System.unique_integer([:positive])}"
    Measurements.clear_data_free_bytes()
    :telemetry_test.attach_event_handlers(self(), [[:fathom, :shard, :flush, :no_space]])

    on_exit(fn ->
      Measurements.clear_data_free_bytes()
      _ = Shards.stop(shard)

      for base <- [
            Fathom.Shard.db_path(shard),
            Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}.db")
          ],
          suffix <- ["", "-wal", "-shm", ".etag"],
          do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp flush!(coordinator) do
    send(coordinator, :durability_flush)
    settle(coordinator, 400)
  end

  defp settle(_c, 0), do: flunk("durability flush task never settled")

  defp settle(c, tries) do
    if :sys.get_state(c).flush_task == nil do
      :ok
    else
      Process.sleep(10)
      settle(c, tries - 1)
    end
  end

  defp dirty_shard(shard) do
    {:ok, coordinator} = Shards.ensure(shard)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))
    {coordinator, conn}
  end

  test "a flush below the free-space floor is refused up front and alerts on the first occurrence",
       %{shard: shard} do
    {coordinator, conn} = dirty_shard(shard)
    assert :sys.get_state(coordinator).flush_failures == 0

    Measurements.put_data_free_bytes(0)
    flush!(coordinator)

    assert_receive {[:fathom, :shard, :flush, :no_space], _ref, %{need: need, free: 0},
                    %{shard_id: ^shard}},
                   2_000,
                   "a flush with no free space on the volume was attempted instead of refused"

    assert need > 0

    state = :sys.get_state(coordinator)
    assert state.flush_failures == 1, "the refusal must count as a failed flush"
    assert Fathom.Shard.dirty?(coordinator), "a refused flush must leave the shard dirty"

    # Space came back (a fresh reading): the next flush goes through and clears the failure.
    Measurements.put_data_free_bytes(10 * 1024 * 1024 * 1024)
    flush!(coordinator)
    state = :sys.get_state(coordinator)
    assert state.flush_failures == 0
    refute Fathom.Shard.dirty?(coordinator)
    :ok = ShardExecutor.close(conn)
  end

  test "an unknown reading never refuses a flush", %{shard: shard} do
    {coordinator, conn} = dirty_shard(shard)

    assert Measurements.data_free_bytes() == :unknown
    flush!(coordinator)

    refute_received {[:fathom, :shard, :flush, :no_space], _, _, _}
    refute Fathom.Shard.dirty?(coordinator), "with no reading the flush must proceed as before"
    :ok = ShardExecutor.close(conn)
  end

  test "a cleared reading reads as unknown" do
    Measurements.put_data_free_bytes(123)
    assert Measurements.data_free_bytes() == {:ok, 123}
    Measurements.clear_data_free_bytes()
    assert Measurements.data_free_bytes() == :unknown
  end
end
