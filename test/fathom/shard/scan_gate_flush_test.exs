defmodule Fathom.Shard.ScanGateFlushTest do
  # Expert review 2026-10-20 #19: the durability flush's scan (quick_check + VACUUM INTO) takes a
  # ScanGate permit. Wiring test: with every permit held elsewhere, a dirty shard's flush task
  # queues on the gate and does NOT upload; releasing the permit lets it finish.
  use ExUnit.Case, async: false

  alias Fathom.Shard.{ScanGate, Storage}
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  setup do
    id = "scangate_#{System.unique_integer([:positive])}"
    prev = Application.get_env(:fathom, :shard_flush_scan_permits, :unset)
    Application.put_env(:fathom, :shard_flush_scan_permits, 1)

    on_exit(fn ->
      if prev == :unset,
        do: Application.delete_env(:fathom, :shard_flush_scan_permits),
        else: Application.put_env(:fathom, :shard_flush_scan_permits, prev)

      Shards.stop(id)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    {:ok, coordinator} = Shards.ensure(id)
    {:ok, conn} = ShardExecutor.open(id)
    on_exit(fn -> ShardExecutor.close(conn) end)
    %{id: id, coordinator: coordinator, conn: conn}
  end

  test "a flush waits for the scan permit and completes once it is released", ctx do
    %{id: id, coordinator: coordinator, conn: conn} = ctx

    {:ok, _} =
      ShardExecutor.execute(conn, %Filo.Stmt{sql: "CREATE TABLE t (a INTEGER)", args: []})

    assert ScanGate.cap() == 1
    assert ScanGate.acquire(1_000) == :ok

    send(coordinator, :durability_flush)
    wait_until(fn -> ScanGate.stats().waiting == 1 end)

    assert :sys.get_state(coordinator).flush_task != nil
    assert {:ok, nil} = Storage.object_position(id), "flushed while the scan permit was held"

    ScanGate.release()
    wait_until(fn -> :sys.get_state(coordinator).flush_task == nil end)
    assert {:ok, %{}} = Storage.object_position(id)
  end

  defp wait_until(fun, tries \\ 400) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held")

      true ->
        receive do
        after
          10 -> wait_until(fun, tries - 1)
        end
    end
  end
end
