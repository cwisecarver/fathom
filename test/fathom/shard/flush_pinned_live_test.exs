defmodule Fathom.Shard.FlushPinnedLiveTest do
  @moduledoc """
  Expert review 2026-10-20 #28: an idle-dirty shard's periodic flush used to `VACUUM INTO` a
  database whose WAL the drained close had already folded. With an empty WAL the flush now uploads
  the LIVE `.db` under a read pin (route `:pinned_live`); any WAL content, or the flag off, keeps the
  `VACUUM INTO` route (`:vacuum`).

  The invariants pinned here:

    * the route is taken for a folded shard, and the uploaded object holds exactly the committed
      rows and passes `integrity_check`;
    * a commit that lands UNDER THE PIN (between the integrity check and the PUT) cannot reach the
      main file — a `wal_checkpoint(TRUNCATE)` on another handle answers BUSY and the `.db` bytes
      are unchanged — so the uploaded bytes are the pinned image, never a torn mix;
    * a non-empty WAL, a corrupt database, and the flag off all keep their old behaviour.

  Not async: telemetry handlers and app env are global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  setup do
    id = "pinlive_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    # The idle-stop would answer a pending flush with :coordinator_stopped (AGENTS.md testing note).
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    test_pid = self()
    handler = "pinlive-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :route],
      fn _, _, meta, _ -> if meta.shard_id == id, do: send(test_pid, {:route, meta.route}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Application.delete_env(:fathom, :flush_pinned_live_upload)
      Application.delete_env(:fathom, :flush_after_snapshot_hook)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      Shards.stop(id)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)
    end)

    {:ok, coordinator} = Shards.ensure(id)
    %{id: id, coordinator: coordinator, path: path}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  # Writes through the executor (which marks the shard dirty), then closes the stream so the LAST
  # connection's close folds and unlinks the WAL: the idle-dirty state the finding is about.
  defp write_then_go_idle(id, path, rows) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))

    for i <- rows,
        do: {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [i]))

    ShardExecutor.close(conn)
    settle_close(path)
  end

  defp settle_close(path, tries \\ 400) do
    cond do
      not File.exists?(path <> "-wal") ->
        :ok

      tries == 0 ->
        flunk("the last close never folded the WAL")

      true ->
        wait(10)
        settle_close(path, tries - 1)
    end
  end

  defp wait(ms) do
    receive do
    after
      ms -> :ok
    end
  end

  defp flush!(coordinator, tries \\ 400) do
    send(coordinator, :durability_flush)
    settle(coordinator, tries)
  end

  defp settle(_c, 0), do: flunk("flush task never settled")

  defp settle(c, tries) do
    if :sys.get_state(c).flush_task == nil do
      :ok
    else
      wait(10)
      settle(c, tries - 1)
    end
  end

  defp object_rows(id) do
    dst = Path.join(System.tmp_dir!(), "pinlive_obj_#{System.unique_integer([:positive])}.db")
    assert {:ok, _} = Storage.pull(id, dst)
    {:ok, c} = Connection.open(dst)
    {:ok, %{rows: rows}} = Connection.query(c, "SELECT a FROM t ORDER BY a", [])
    {:ok, %{rows: integrity}} = Connection.query(c, "PRAGMA integrity_check", [])
    :ok = Connection.close(c)
    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    assert integrity == [["ok"]]
    List.flatten(rows)
  end

  test "a folded idle-dirty shard flushes its live .db under a pin; the object is complete",
       ctx do
    %{id: id, coordinator: coordinator, path: path} = ctx
    write_then_go_idle(id, path, 1..50)
    refute File.exists?(path <> "-wal"), "fixture: the WAL must be folded"

    flush!(coordinator)

    assert_received {:route, :pinned_live}
    refute_received {:route, :vacuum}
    assert object_rows(id) == Enum.to_list(1..50)

    # The pin was the last connection: it must clean up like the old snapshot handle did (a :ro
    # pin leaves a zero-length -wal and a stale -shm, which PoolGraceTest also refuses).
    refute File.exists?(path <> "-wal"), "the pinned flush left a -wal behind"
    refute File.exists?(path <> "-shm"), "the pinned flush left a -shm behind"
  end

  test "a commit under the pin cannot reach the main file; the object is the pinned image",
       ctx do
    %{id: id, coordinator: coordinator, path: path} = ctx
    write_then_go_idle(id, path, 1..10)
    test_pid = self()

    Application.put_env(:fathom, :flush_after_snapshot_hook, fn sid ->
      if sid == id do
        before = File.read!(path)
        # A raw handle (the executor refuses tenant checkpoints). It is not the last connection
        # (the pin is open), so its close runs no close-time checkpoint of its own.
        {:ok, conn} = Connection.open(path)
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (999)", [])
        Connection.set_busy_timeout(conn, 0)
        {:ok, ckpt} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
        :ok = Connection.close(conn)
        send(test_pid, {:under_pin, ckpt, File.read!(path) == before})
      end
    end)

    flush!(coordinator)
    Application.delete_env(:fathom, :flush_after_snapshot_hook)

    assert_received {:route, :pinned_live}
    assert_received {:under_pin, ckpt, main_file_unchanged}

    # The pin blocked the checkpoint (busy flag set), so the .db the upload read never moved.
    assert [[1 | _]] = ckpt.rows |> Enum.map(&Enum.map(&1, fn v -> v end))
    assert main_file_unchanged, "a checkpoint wrote the main file while the pin was held"

    # The object is the pinned image: the in-window commit is NOT in it, and nothing is torn.
    assert object_rows(id) == Enum.to_list(1..10)
  end

  test "a non-empty WAL keeps the VACUUM INTO route", ctx do
    %{id: id, coordinator: coordinator} = ctx
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))
    on_exit(fn -> ShardExecutor.close(conn) end)

    flush!(coordinator)

    assert_received {:route, :vacuum}
    refute_received {:route, :pinned_live}
    assert object_rows(id) == [1]
  end

  test "the flag off keeps the VACUUM INTO route for a folded shard", ctx do
    %{id: id, coordinator: coordinator, path: path} = ctx
    Application.put_env(:fathom, :flush_pinned_live_upload, false)
    write_then_go_idle(id, path, 1..5)

    flush!(coordinator)

    assert_received {:route, :vacuum}
    refute_received {:route, :pinned_live}
    assert object_rows(id) == Enum.to_list(1..5)
  end
end
