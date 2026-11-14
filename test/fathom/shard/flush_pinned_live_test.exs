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
  alias Fathom.Test.Await
  alias Fathom.Test.Env
  alias Fathom.Test.ShardFlush

  setup do
    id = "pinlive_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)

    # The idle-stop would answer a pending flush with :coordinator_stopped (AGENTS.md testing note).
    Env.put!(:shard_idle_ms, 600_000)

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

  defp settle_close(path) do
    Await.until(
      fn -> not File.exists?(path <> "-wal") end,
      "the last close never folded the WAL",
      4_000
    )
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

    ShardFlush.flush!(coordinator)

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

    Env.put!(:flush_after_snapshot_hook, fn sid ->
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

    ShardFlush.flush!(coordinator)
    Application.delete_env(:fathom, :flush_after_snapshot_hook)

    assert_received {:route, :pinned_live}
    assert_received {:under_pin, ckpt, main_file_unchanged}

    # The pin blocked the checkpoint (busy flag set), so the .db the upload read never moved.
    assert [[1 | _]] = ckpt.rows |> Enum.map(&Enum.map(&1, fn v -> v end))
    assert main_file_unchanged, "a checkpoint wrote the main file while the pin was held"

    # The object is the pinned image: the in-window commit is NOT in it, and nothing is torn.
    assert object_rows(id) == Enum.to_list(1..10)
  end

  # This test used to assert that ANY non-empty WAL keeps the VACUUM route. That pinned the defect
  # expert review 2026-10-21 #11 fixed: a persistent stream's WAL is never zero-length, so such a
  # shard never took the cheap route even when fully checkpointed. The route now needs the WAL fully
  # BACKFILLED (replication off: the flush's PASSIVE runs before the pin to get it there). What still
  # forces VACUUM is a WAL holding frames the main file lacks; a reader on an older snapshot keeps
  # them there. `FlushPinnedBackfilledTest` covers the new route.
  test "a WAL holding un-backfilled frames keeps the VACUUM INTO route", ctx do
    %{id: id, coordinator: coordinator, path: path} = ctx
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
    on_exit(fn -> ShardExecutor.close(conn) end)

    {:ok, reader} = Connection.open(path)
    :ok = Connection.exec(reader, "BEGIN")
    {:ok, _} = Connection.query(reader, "SELECT count(*) FROM t", [])

    on_exit(fn ->
      _ = Connection.rollback_if_open(reader)
      Connection.close(reader)
    end)

    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))

    ShardFlush.flush!(coordinator)

    assert_received {:route, :vacuum}
    refute_received {:route, :pinned_live}
    assert object_rows(id) == [1]
  end

  # Expert review 2026-10-20 #32: mmap for the flush scan is DEFAULT OFF and only ever applied to a
  # flush-scan handle (never a tenant's). Off => the pragma stays at SQLite's own default.
  test "mmap_size is untouched by default and set only when :flush_scan_mmap_bytes is" do
    {:ok, conn} =
      Connection.open(
        Path.join(System.tmp_dir!(), "mmap_#{System.unique_integer([:positive])}.db")
      )

    on_exit(fn -> Connection.close(conn) end)

    {:ok, default} = Connection.pragma(conn, "mmap_size")
    Fathom.Shard.apply_scan_mmap(conn)
    assert Connection.pragma(conn, "mmap_size") == {:ok, default}, "applied with the flag unset"

    Env.put!(:flush_scan_mmap_bytes, 8 * 1_048_576)
    Fathom.Shard.apply_scan_mmap(conn)
    assert {:ok, n} = Connection.pragma(conn, "mmap_size")
    assert n == 8 * 1_048_576 or n == 0, "pragma not applied (n=#{n})"
  end

  test "with mmap on, both flush routes still upload a complete, sound object", ctx do
    %{id: id, coordinator: coordinator, path: path} = ctx
    Env.put!(:flush_scan_mmap_bytes, 8 * 1_048_576)

    write_then_go_idle(id, path, 1..20)
    ShardFlush.flush!(coordinator)
    assert_received {:route, :pinned_live}
    assert object_rows(id) == Enum.to_list(1..20)

    # The flag off forces the VACUUM route: a live stream's WAL no longer does on its own, since
    # the pre-pin PASSIVE folds it (expert review 2026-10-21 #11).
    Env.put!(:flush_pinned_live_upload, false)
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (21)"))
    on_exit(fn -> ShardExecutor.close(conn) end)
    ShardFlush.flush!(coordinator)
    assert_received {:route, :vacuum}
    assert object_rows(id) == Enum.to_list(1..21)
  end

  test "the flag off keeps the VACUUM INTO route for a folded shard", ctx do
    %{id: id, coordinator: coordinator, path: path} = ctx
    Env.put!(:flush_pinned_live_upload, false)
    write_then_go_idle(id, path, 1..5)

    ShardFlush.flush!(coordinator)

    assert_received {:route, :vacuum}
    refute_received {:route, :pinned_live}
    assert object_rows(id) == Enum.to_list(1..5)
  end
end
