defmodule Fathom.Shard.DropCompactionTest do
  @moduledoc """
  Expert review 2026-10-20-2027 #9 and #10.

  #9: the idle-drop route (`checkpoint_then_upload`) and the pinned-live periodic route uploaded
  the live `.db` as-is, freelist pages included. A tenant that deleted 90% of its rows (nothing
  shrinks a SQLite file without VACUUM) kept paying for the dead pages on every flush and every
  cold pull, and the deleted rows stayed readable in the stored object. A mostly-free file
  (default > 25% free pages AND > 1 MiB free, read from the SQLite header) now takes the existing
  VACUUM route, which compacts. A normal file keeps the fast routes.

  The drop scenarios run in BOTH lease modes (legacy / heartbeat, see
  `Fathom.ShardLeaseReleaseTest`): the reroute sits on the drop path, whose fence and release
  differ per mode.

  #10: the drop route's TRUNCATE checkpoint + `quick_check` take a `ScanGate` permit with a bounded
  wait, then scan anyway; a held-up gate must never fail the flush.

  Not async: telemetry handlers, app env and the global ScanGate.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{ScanGate, Storage}
  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.Heartbeat

  @modes [:legacy, :heartbeat]

  setup do
    id = "dropcompact_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    test_pid = self()
    handler = "dropcompact-#{id}"

    :telemetry.attach_many(
      handler,
      [[:fathom, :shard, :drop_flush, :route], [:fathom, :shard, :flush, :route]],
      fn
        [:fathom, :shard, :drop_flush, :route], _, %{shard_id: ^id, route: r}, _ ->
          send(test_pid, {:drop_route, r})

        [:fathom, :shard, :flush, :route], _, %{shard_id: ^id, route: r}, _ ->
          send(test_pid, {:flush_route, r})

        _, _, _, _ ->
          :ok
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Application.delete_env(:fathom, :shard_flush_scan_permits)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      Shards.stop(id)

      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)

      for suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(Storage.Local.dir(), id <> suffix))
    end)

    %{id: id, path: path}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  # ~3.2 MB of random (incompressible) rows, then optionally delete 90% of them. The last stream
  # closing folds and unlinks the WAL, so the header counts are current. `fixture_ok!` asserts the
  # precondition: a "mostly free" fixture that is not mostly free tests nothing.
  defp seed!(id, path, delete_most?) do
    {:ok, coordinator} = Shards.ensure(id)
    {:ok, conn} = ShardExecutor.open(id)

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB)"))

    {:ok, _} =
      ShardExecutor.execute(
        conn,
        stmt(
          "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 800) " <>
            "INSERT INTO t SELECT x, randomblob(4096) FROM c"
        )
      )

    if delete_most? do
      {:ok, _} = ShardExecutor.execute(conn, stmt("DELETE FROM t WHERE id % 10 != 0"))
    end

    ShardExecutor.close(conn)
    settle_close(path)

    {:ok, %{freelist: free, page_count: count}} = Fathom.Shard.SqliteHeader.free_pages(path)

    if delete_most?,
      do: assert(free / count > 0.5, "fixture: the file is not mostly free (#{free}/#{count})"),
      else:
        assert(free / count < 0.1, "fixture: control file unexpectedly free (#{free}/#{count})")

    coordinator
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
      ms -> true
    end
  end

  # Stops the shard while tracing (all processes: the drop-flush runs in a Task) the calls that
  # define the cost: `quick_check` queries, `Connection.open`, and `Storage.flush` (the upload).
  # Returns them in trace order.
  defp trace_drop(id) do
    alias Fathom.Shard.Connection
    me = self()

    qc_ms = [{[:_, :"$1", :_], [{:==, :"$1", "PRAGMA quick_check"}], []}]
    :erlang.trace_pattern({Connection, :query, 3}, qc_ms, [])
    :erlang.trace_pattern({Connection, :open, 1}, true, [])
    :erlang.trace_pattern({Connection, :open, 2}, true, [])
    :erlang.trace_pattern({Storage, :flush, 6}, true, [])
    :erlang.trace(:all, true, [:call, {:tracer, me}])

    try do
      assert :ok = Shards.stop(id)
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Connection, :query, 3}, false, [])
      :erlang.trace_pattern({Connection, :open, 1}, false, [])
      :erlang.trace_pattern({Connection, :open, 2}, false, [])
      :erlang.trace_pattern({Storage, :flush, 6}, false, [])
    end

    collect_trace([])
  end

  defp collect_trace(acc) do
    receive do
      {:trace, _pid, :call, {Fathom.Shard.Connection, :query, _}} ->
        collect_trace([{:qc, 1} | acc])

      {:trace, _pid, :call, {Fathom.Shard.Connection, :open, _}} ->
        collect_trace([{:open, 1} | acc])

      {:trace, _pid, :call, {Fathom.Shard.Storage, :flush, _}} ->
        collect_trace([{:flush, 1} | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp object(id), do: Path.join(Storage.Local.dir(), id <> ".db")

  defp rows(file) do
    {:ok, c} = Fathom.Shard.Connection.open(file)
    {:ok, %{rows: [[n]]}} = Fathom.Shard.Connection.query(c, "SELECT count(*) FROM t", [])
    :ok = Fathom.Shard.Connection.close(c)
    n
  end

  for mode <- @modes do
    test "[#{mode}] idle drop of a mostly-free file uploads the compacted object", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      coordinator = seed!(id, path, true)
      assert_mode!(coordinator, unquote(mode))
      local_size = File.stat!(path).size

      assert :ok = Shards.stop(id)

      # Pre-fix: route :checkpoint and the object is the live file (same size, freelist included).
      assert_received {:drop_route, :snapshot_compact}

      # (`:checkpoint` is emitted first, then the compaction reroute; the size is the discriminator.)
      remote = object(id)
      assert File.stat!(remote).size < div(local_size, 3)
      assert rows(remote) == 80
    end

    # Expert review 2026-10-21 #21. SYMPTOM: the compaction reroute re-ran `quick_check` (a full
    # page scan) and opened a read-pin connection that could only answer :fallback, on a file the
    # drop's checkpoint+quick_check had verified moments earlier with nothing able to touch it.
    # INVARIANT: integrity is verified EXACTLY ONCE, and BEFORE the upload; connection opens are
    # checkpoint + snapshot only (2, not 3).
    test "[#{mode}] compaction reroute verifies integrity once, before the upload", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      coordinator = seed!(id, path, true)
      assert_mode!(coordinator, unquote(mode))

      events = trace_drop(id)

      assert_received {:drop_route, :snapshot_compact}
      qc = for {:qc, _} = e <- events, do: e
      opens = for {:open, _} = e <- events, do: e
      assert length(qc) == 1, "quick_check ran #{length(qc)} times: #{inspect(events)}"
      assert length(opens) == 2, "Connection.open ran #{length(opens)} times"

      # ordering: the one quick_check precedes the upload (Storage.flush)
      kinds = Enum.map(events, &elem(&1, 0))
      assert Enum.find_index(kinds, &(&1 == :qc)) < Enum.find_index(kinds, &(&1 == :flush))
      assert rows(object(id)) == 80
    end

    test "[#{mode}] idle drop of a normal file keeps the fast checkpoint route", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      coordinator = seed!(id, path, false)
      assert_mode!(coordinator, unquote(mode))
      local_size = File.stat!(path).size

      assert :ok = Shards.stop(id)

      assert_received {:drop_route, :checkpoint}
      refute_received {:drop_route, :snapshot_compact}
      assert File.stat!(object(id)).size == local_size
      assert rows(object(id)) == 800
    end
  end

  test "a periodic flush of a mostly-free folded file takes the VACUUM route, not pinned-live",
       %{id: id, path: path} do
    coordinator = seed!(id, path, true)
    local_size = File.stat!(path).size

    send(coordinator, :durability_flush)
    settle(coordinator)

    assert_received {:flush_route, :vacuum}
    refute_received {:flush_route, :pinned_live}
    assert File.stat!(object(id)).size < div(local_size, 3)
    assert rows(object(id)) == 80
  end

  test "a periodic flush of a normal folded file still takes the pinned-live route",
       %{id: id, path: path} do
    coordinator = seed!(id, path, false)

    send(coordinator, :durability_flush)
    settle(coordinator)

    assert_received {:flush_route, :pinned_live}
    refute_received {:flush_route, :vacuum}
    assert rows(object(id)) == 800
  end

  defp settle(c, tries \\ 400) do
    cond do
      :sys.get_state(c).flush_task == nil ->
        :ok

      tries == 0 ->
        flunk("flush task never settled")

      true ->
        wait(10)
        settle(c, tries - 1)
    end
  end

  test "#10: the drop checkpoint+quick_check waits on the ScanGate, then scans anyway", %{
    id: id,
    path: path
  } do
    # settle_yield_ms = min(30_000, shutdown/3); the permit wait is a quarter of it.
    prev = Application.get_env(:fathom, :shard_shutdown_ms)
    Application.put_env(:fathom, :shard_shutdown_ms, 2_400)
    on_exit(fn -> restore(:shard_shutdown_ms, prev) end)

    seed!(id, path, false)

    # Occupy the only permit from another process so the drop must wait, then time out and proceed.
    Application.put_env(:fathom, :shard_flush_scan_permits, 1)
    parent = self()

    holder =
      spawn_link(fn ->
        :ok = ScanGate.acquire(1_000)
        send(parent, :held)

        receive do
          :release -> ScanGate.release()
        end
      end)

    assert_receive :held, 2_000
    assert %{held: 1} = ScanGate.stats()

    t0 = System.monotonic_time(:millisecond)
    assert :ok = Shards.stop(id)
    elapsed = System.monotonic_time(:millisecond) - t0

    # Pre-fix there is no wait at all (elapsed ~ms); post-fix it waited the bounded 200 ms
    # (2_400 / 3 / 4) for a permit that never came, then scanned and flushed anyway.
    assert elapsed >= 150, "the drop checkpoint never waited on the ScanGate (#{elapsed} ms)"
    assert_received {:drop_route, :checkpoint}
    assert rows(object(id)) == 800

    send(holder, :release)
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)
end
