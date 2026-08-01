defmodule Fathom.ShardFlushReconcileTest do
  @moduledoc """
  Two ways the coordinator concluded "my writes are durable" without evidence, and then
  deleted them (expert review 2026-08-01 #3 and #4). Both end in `drop_local/1` unlinking
  acknowledged, never-uploaded writes; both are reachable on an ordinary rolling deploy.

  ## #3 — the settle path matched a reply shape the flush task stopped producing

  `fenced_flush/2` replies `{:fenced, verdict, updates}`. `settle_flush_task/1` matched
  `{:ok, {:ok, etag}}` — the shape from before `f8ecf63` ("run the periodic-flush fence
  off-process"), which changed the reply and updated `handle_info/2` but not the settle. The
  match was unreachable, so every terminate fell through with a STALE `state.etag` and a stale
  `flushed_through`. The drop-flush then PUT with the pre-task etag against an object the task
  had already advanced — a deterministic 412 — and the "lock still ours, object is durable"
  branch called `drop_local`, deleting every write committed after the task's snapshot. The
  log line said the object was durable.

  ## #4 — a 412 with the lock still ours is not proof the object holds THIS attempt's bytes

  A PUT can land server-side with its response lost. The shard stays dirty at the old etag,
  the tenant writes more, and the NEXT flush 412s against our own earlier write. The reconcile
  concluded "our bytes are the live object" and advanced `flushed_through`, marking the shard
  clean while the object held only the earlier snapshot. The next idle `drop_clean` unlinked
  the rest — undetectably, since the watermark and `loss-report` both then read clean.

  Not async: shards are global and back onto real files.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.{Stmt, StmtResult}

  setup do
    shard = "reconcile_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      Application.delete_env(:fathom, :storage_flush_delay_ms)
      restore(:shard_storage, prev_storage)
      restore(:shard_flush_interval_ms, prev_flush)
      restore(:shard_idle_ms, prev_idle)

      Shards.drain(shard, 2_000)

      for dir <- [local_dir(), remote_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock", ".db.etag"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(local_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp local_dir, do: Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()

  defp flush_now(coordinator) do
    send(coordinator, :durability_flush)
    wait_flush_settled(coordinator, 400)
  end

  defp wait_flush_settled(_coordinator, 0), do: flunk("durability flush task never settled")

  defp wait_flush_settled(coordinator, tries) do
    case :sys.get_state(coordinator) do
      %{flush_task: nil} ->
        :ok

      _ ->
        Process.sleep(5)
        wait_flush_settled(coordinator, tries - 1)
    end
  end

  # Read the stored object back — the only thing that matters. A local file is not durability.
  defp stored_rows(shard) do
    dst =
      Path.join(System.tmp_dir!(), "reconcile_verify_#{System.unique_integer([:positive])}.db")

    assert {:ok, _etag} = Storage.pull(shard, dst)
    {:ok, conn} = Connection.open(dst)
    {:ok, %{rows: rows}} = Connection.query(conn, "SELECT v FROM kv ORDER BY v", [])
    :ok = Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    List.flatten(rows)
  end

  describe "#3 — an in-flight flush task settled at terminate" do
    test "writes committed after the task's snapshot survive the stop", %{shard: shard} do
      # Never flush on a timer; every flush in this test is driven explicitly.
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('before')"))

      # Establish a stored object so the drop-flush is a conditional PUT with a real etag.
      :ok = flush_now(coordinator)
      assert stored_rows(shard) == ["before"]

      # Now make the flush slow, so the task is genuinely in flight when we stop the
      # coordinator — this is the window settle_flush_task/1 exists to close.
      Application.put_env(:fathom, :storage_flush_delay_ms, 250)
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('snapshotted')"))
      send(coordinator, :durability_flush)

      # Give the task time to take its snapshot but not to finish the upload.
      Process.sleep(60)

      # A write the in-flight task's snapshot does NOT contain. This is the row the bug ate.
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('after_snapshot')"))

      # Stop the coordinator the way a rolling deploy does: force-stop with the connection
      # still checked out. terminate/2 settles the in-flight task, then flush_and_drop.
      ref = Process.monitor(coordinator)
      :ok = Shards.stop(shard)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000

      Application.delete_env(:fathom, :storage_flush_delay_ms)

      # THE ASSERTION: every acknowledged write reached storage.
      assert stored_rows(shard) == ["after_snapshot", "before", "snapshotted"],
             "a write acked after the in-flight flush task's snapshot did not reach storage"
    end

    test "a settled task's etag is folded, so the drop-flush does not 412 against itself",
         %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))
      :ok = flush_now(coordinator)

      etag_before = :sys.get_state(coordinator).etag
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('b')"))
      :ok = flush_now(coordinator)
      etag_after = :sys.get_state(coordinator).etag

      # The coordinator's fence etag tracks the object it just wrote. If the fold is dropped,
      # this stays stale and the next conditional PUT 412s against our own write.
      refute etag_after == etag_before,
             "the flush task's new etag was not folded into coordinator state"

      assert {:ok, object_etag} = Storage.object_etag(shard)
      assert etag_after == object_etag, "coordinator fence etag diverged from the stored object"

      :ok = ShardExecutor.close(conn)
    end
  end

  describe "#4 — a 412 with the lock still ours" do
    test "the shard stays DIRTY rather than being marked clean on an unproven reconcile",
         %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('first')"))
      :ok = flush_now(coordinator)
      assert stored_rows(shard) == ["first"]

      # Interval 1: the PUT LANDS but the response is lost. The coordinator believes the flush
      # failed, so it keeps its old fence etag and stays dirty. The object has advanced.
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('landed_silently')"))
      Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
      :ok = flush_now(coordinator)
      Application.delete_env(:fathom, :storage_fault)

      assert Shard.dirty?(coordinator), "a failed-looking flush must leave the shard dirty"

      # The tenant keeps writing. THIS is the row that the old reconcile discarded.
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('after_lost_ack')"))

      # Interval 2: PUT If-Match <stale etag> ⇒ 412 ⇒ lock re-check says still ours ⇒ reconcile.
      :ok = flush_now(coordinator)

      # THE ASSERTION. Pre-fix this advanced flushed_through and the shard read CLEAN, while
      # the stored object held only 'landed_silently'. An idle drop_clean then deleted
      # 'after_lost_ack' with no error anywhere.
      assert Shard.dirty?(coordinator),
             "a 412 is proof the object moved, not proof it holds this attempt's bytes — " <>
               "the shard must stay dirty and re-flush"

      :ok = ShardExecutor.close(conn)
    end

    test "the re-flush after a reconcile actually lands every acked write", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('first')"))
      :ok = flush_now(coordinator)

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('landed_silently')"))
      Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
      :ok = flush_now(coordinator)
      Application.delete_env(:fathom, :storage_fault)

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('after_lost_ack')"))

      # Reconcile interval, then the recovery interval the fix relies on.
      :ok = flush_now(coordinator)
      :ok = flush_now(coordinator)

      assert stored_rows(shard) == ["after_lost_ack", "first", "landed_silently"],
             "the post-reconcile re-flush did not carry every acknowledged write to storage"

      :ok = ShardExecutor.close(conn)
    end

    test "an idle drop after a reconcile does not delete un-stored writes", %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)
      # idle_ms is captured into coordinator state at init, so it must be set BEFORE the open.
      # The held connection keeps the shard busy until we close it.
      Application.put_env(:fathom, :shard_idle_ms, 100)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('first')"))
      :ok = flush_now(coordinator)

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('landed_silently')"))
      Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
      :ok = flush_now(coordinator)
      Application.delete_env(:fathom, :storage_fault)

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('after_lost_ack')"))
      :ok = flush_now(coordinator)

      # Close the connection and let the coordinator idle-stop on its OWN timer — the drop
      # decision (flush_then_drop vs drop_clean) is exactly what this test is about, so it has
      # to reach that path naturally rather than be forced or crashed into terminate.
      ref = Process.monitor(coordinator)
      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 5_000

      assert stored_rows(shard) == ["after_lost_ack", "first", "landed_silently"],
             "the idle drop deleted acknowledged writes the stored object never received"
    end
  end

  describe "the reconcile path still converges — it must not merely refuse forever" do
    test "a shard that reconciles once returns to clean on the next successful flush",
         %{shard: shard} do
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))
      :ok = flush_now(coordinator)

      Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('y')"))
      :ok = flush_now(coordinator)
      Application.delete_env(:fathom, :storage_fault)

      # Reconcile (resyncs the etag, stays dirty), then a clean interval.
      :ok = flush_now(coordinator)
      :ok = flush_now(coordinator)

      refute Shard.dirty?(coordinator),
             "staying dirty is the safe direction, but the shard must still converge to clean"

      assert {:ok, %StmtResult{}} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))
      :ok = ShardExecutor.close(conn)
    end
  end
end
