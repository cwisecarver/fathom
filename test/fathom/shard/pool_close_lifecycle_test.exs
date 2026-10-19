defmodule Fathom.Shard.PoolCloseLifecycleTest do
  @moduledoc """
  The pool/close lifecycle residuals of c2737f4 (expert review 2026-10-12 #9, #16, #18, #19).

  c2737f4 moved the idle transition's pooled-handle close off the coordinator but left four holes,
  each pinned here by the invariant it violated:

    * #16 — `drain_pool` AWAITED the previous close inline, so back-to-back idle transitions on a hot
      shard stalled the mailbox again. Closes now chain off the coordinator.
    * #18 — `close_lent` (a stream died holding a lent handle) and the draining/no-pool checkin
      still closed inline, and either handle can be the shard's LAST connection (the checkpoint).
    * #19 — two terminate clauses called `close_pool` (which can wait) BEFORE publishing the write
      fence.
    * #9  — a `checkin/4` cast still in a terminating coordinator's mailbox was never closed.

  Not async: pooling, the close hook and shard files are global.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard
  alias Fathom.Shard.WriteFence
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    prev_pool = Application.get_env(:fathom, :connection_pool)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    Application.put_env(:fathom, :connection_pool, true)
    shard = "pool_close_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      restore(:connection_pool, prev_pool)
      restore(:shard_idle_ms, prev_idle)
      Application.delete_env(:fathom, :shard_pool_close_hook)
      :erlang.trace_pattern({Sqlite3, :close, 1}, false, [])

      # The coordinator may still be running: stop it before the files go.
      _ = Shards.stop(shard)
      WriteFence.forget(shard)

      for base <- [
            Path.join(Shard.data_dir(), "#{shard}.db"),
            Path.join(Shard.Storage.Local.dir(), "#{shard}.db")
          ],
          suffix <- ["", "-wal", "-shm", ".etag"],
          do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)
  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp conn_of({_pid, _ref, conn, _id, _sc, _v, _o}), do: conn
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid
  defp ref_of({_pid, ref, _conn, _id, _sc, _v, _o}), do: ref

  defp wal(shard), do: Path.join(Shard.data_dir(), "#{shard}.db-wal")

  # Wait for the tail of the close chain (the pid in `closing`), if any.
  defp await_closes(pid) do
    case :sys.get_state(pid).closing do
      tpid when is_pid(tpid) ->
        mref = Process.monitor(tpid)
        assert_receive {:DOWN, ^mref, :process, ^tpid, _}, 5_000

      nil ->
        :ok
    end
  end

  defp closed?(conn) do
    # A closed exqlite handle refuses to prepare a statement.
    not match?({:ok, _}, Sqlite3.prepare(conn, "SELECT 1"))
  end

  defp trace_closes(coordinator) do
    :erlang.trace_pattern({Sqlite3, :close, 1}, true, [])
    1 = :erlang.trace(coordinator, true, [:call, :set_on_spawn])
  end

  # Expert review 2026-10-12 #16. c2737f4's `drain_pool` called `await_closing/1` on the PREVIOUS
  # close before starting the next, inline in the coordinator. Idle transitions that arrive within
  # one close duration (a hot shard under CONN_MAX_AGE=0) therefore blocked the mailbox for the
  # remainder of the previous close: the stall #15 set out to remove. The c2737f4 test and probe
  # were sequential, so they never saw it. A slow-close seam (50-80 ms) makes the overlap
  # deterministic; the invariant is the latency of a message queued behind the SECOND checkin.
  test "back-to-back idle transitions never block the coordinator on the previous close (#16)", %{
    shard: shard
  } do
    test_pid = self()

    Application.put_env(:fathom, :shard_pool_close_hook, fn conn ->
      send(test_pid, {:close_started, conn, System.monotonic_time(:millisecond)})
      Process.sleep(80)
    end)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('a')"))
    pid = pid_of(ha)
    conn_a = conn_of(ha)

    # Idle transition 1: starts a (slow) close of A.
    :ok = ShardExecutor.close(ha)
    assert_receive {:close_started, ^conn_a, started_a}, 2_000

    # Idle transition 2 while close 1 is still running.
    {:ok, hb} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(hb, stmt("INSERT INTO kv VALUES ('b')"))
    conn_b = conn_of(hb)
    :ok = ShardExecutor.close(hb)

    # How long was the coordinator blocked behind the second checkin? (:sys.get_state is queued
    # right behind the cast.) Pre-fix this waited out the rest of close 1: tens of milliseconds.
    t0 = System.monotonic_time(:microsecond)
    _ = :sys.get_state(pid)
    blocked_ms = (System.monotonic_time(:microsecond) - t0) / 1000

    assert blocked_ms < 25,
           "the coordinator was blocked #{Float.round(blocked_ms, 1)} ms behind the second idle " <>
             "close: it awaited the previous close inline (expert review 2026-10-12 #16)"

    # ORDERING survives: close 2 waits for close 1 to finish (two closes must never race each
    # other's close-time checkpoint), so it starts at least one hook duration after close 1 did.
    assert_receive {:close_started, ^conn_b, started_b}, 2_000

    assert started_b - started_a >= 75,
           "close 2 started #{started_b - started_a} ms after close 1: the chain did not order them"

    await_closes(pid)
    refute File.exists?(wal(shard)), "the chained closes must still checkpoint+unlink the WAL"
  end

  # Expert review 2026-10-12 #18. A stream that dies holding a handle the coordinator lent it
  # (`close_lent/2`) left that close inline in the coordinator, and the handle can be the shard's
  # last connection, whose close is the close-time checkpoint.
  test "closing a lent handle after its stream died runs off the coordinator (#18)", %{
    shard: shard
  } do
    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    # B overlaps A so the shard stays busy and A's handle is POOLED rather than closed.
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    :ok = ShardExecutor.close(ha)

    # A stream that checks out the pooled handle (it is LENT to it) and dies without checking in.
    parent = self()

    {holder, mref} =
      spawn_monitor(fn ->
        {:ok, hc} = ShardExecutor.open(shard)
        send(parent, {:holding, conn_of(hc)})
        Process.sleep(:infinity)
      end)

    assert_receive {:holding, ^conn_a},
                   2_000,
                   "precondition: the stream was lent the pooled handle"

    trace_closes(pid)
    Process.exit(holder, :kill)
    assert_receive {:DOWN, ^mref, :process, ^holder, _}, 2_000

    assert_receive {:trace, closer, :call, {Sqlite3, :close, [^conn_a]}},
                   5_000,
                   "the lent handle of a dead stream was never closed"

    refute closer == pid,
           "the coordinator closed a dead stream's lent handle inline (expert review 2026-10-12 #18)"

    :ok = ShardExecutor.close(hb)
  end

  # Expert review 2026-10-12 #18, second site: the checkin of a handle to a coordinator that will not
  # pool it (pooling off for this coordinator, or the shard is draining) closed it inline.
  test "a checkin the coordinator will not pool is closed off the coordinator (#18)", %{
    shard: shard
  } do
    # The coordinator starts with pooling OFF, so its pool is nil; the stream then pools anyway.
    Application.put_env(:fathom, :connection_pool, false)
    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('a')"))
    pid = pid_of(ha)
    conn = conn_of(ha)
    assert :sys.get_state(pid).pool == nil, "precondition: this coordinator has no pool"
    assert File.exists?(wal(shard)), "precondition: the write is in the -wal"

    Application.put_env(:fathom, :connection_pool, true)
    trace_closes(pid)
    :ok = ShardExecutor.close(ha)

    assert_receive {:trace, closer, :call, {Sqlite3, :close, [^conn]}},
                   5_000,
                   "the handle checked in to a pool-less coordinator was never closed"

    refute closer == pid,
           "the coordinator closed a no-pool checkin inline (expert review 2026-10-12 #18)"

    await_closes(pid)

    refute File.exists?(wal(shard)),
           "the off-coordinator close must still checkpoint+unlink the WAL"
  end

  # Expert review 2026-10-17 #19. With K concurrent streams on a shard and a small `max_per_scope`
  # (default 1), a checkin evicts a pooled handle on nearly every cycle, and the eviction closed
  # inline in the coordinator (finalize cache + sqlite3_close, a checkpoint when it is the last
  # connection) while every other checkout/checkin queued behind it. The invariant is WHICH PROCESS
  # closes it, plus that the shard's close chain (which terminate/flush await) owns it.
  test "a handle evicted by a full pool bucket is closed off the coordinator (2026-10-17 #19)", %{
    shard: shard
  } do
    # Pinned to a cap of 1 so the test does not depend on the prod default for `max_per_scope`.
    prev_opts = Application.get_env(:fathom, :connection_pool_opts)
    Application.put_env(:fathom, :connection_pool_opts, max_per_scope: 1)
    on_exit(fn -> restore(:connection_pool_opts, prev_opts) end)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    {:ok, hc} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    assert :sys.get_state(pid).pool.max_per_scope == 1, "precondition: bucket cap of 1"

    trace_closes(pid)
    # A is pooled (the shard is busy). B's checkin overflows the bucket and evicts A.
    :ok = ShardExecutor.close(ha)
    refute_receive {:trace, _, :call, {Sqlite3, :close, [^conn_a]}}, 50
    :ok = ShardExecutor.close(hb)

    assert_receive {:trace, closer, :call, {Sqlite3, :close, [^conn_a]}},
                   5_000,
                   "the evicted handle was never closed"

    refute closer == pid,
           "the coordinator closed an evicted pooled handle inline (expert review 2026-10-17 #19)"

    assert is_pid(:sys.get_state(pid).closing), "the eviction close must ride the close chain"
    await_closes(pid)
    :ok = ShardExecutor.close(hc)
  end

  test "handles shed by the ttl sweep are closed off the coordinator (2026-10-17 #19)", %{
    shard: shard
  } do
    prev_opts = Application.get_env(:fathom, :connection_pool_opts)
    Application.put_env(:fathom, :connection_pool_opts, ttl_ms: 50)
    on_exit(fn -> restore(:connection_pool_opts, prev_opts) end)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    assert :sys.get_state(pid).pool.ttl_ms == 50, "precondition: short ttl took"

    trace_closes(pid)
    :ok = ShardExecutor.close(ha)

    assert_receive {:trace, closer, :call, {Sqlite3, :close, [^conn_a]}},
                   5_000,
                   "the expired pooled handle was never swept"

    refute closer == pid,
           "the coordinator closed a swept pooled handle inline (expert review 2026-10-17 #19)"

    await_closes(pid)
    :ok = ShardExecutor.close(hb)
  end

  # Expert review 2026-10-12 #19. Two terminate clauses (a lost lease, and a force-stop with streams
  # checked out) called `close_pool/1` BEFORE `WriteFence.fence/1`. Since c2737f4 close_pool waits
  # for the close chain, so the write fence stayed down for the whole wait while streams kept
  # acking writes into a shard whose lease was gone. A stuck close stands in for a wedged
  # filesystem; the invariant is that the fence is already up while terminate is blocked.
  for clause <- [:lease_lost, :force_stop_with_streams] do
    test "the write fence is published before terminate waits on a stuck close (#19, #{clause})",
         %{shard: shard} do
      clause = Function.identity(unquote(clause))
      {:ok, ha} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
      pid = pid_of(ha)

      # Clause 1 has no streams: close ours so conns == 0 (lease_lost matches first regardless).
      if clause == :lease_lost do
        :ok = ShardExecutor.close(ha)
        await_closes(pid)
      end

      stuck = spawn(fn -> Process.sleep(:infinity) end)

      :sys.replace_state(pid, fn st ->
        %{st | closing: stuck, lease_lost: clause == :lease_lost}
      end)

      refute WriteFence.fenced?(shard), "precondition: not fenced yet"
      pref = Process.monitor(pid)
      terminator = Task.async(fn -> :sys.terminate(pid, :shutdown) end)

      assert eventually(fn -> WriteFence.fenced?(shard) end),
             "terminate waited on the stuck close without publishing the write fence first " <>
               "(expert review 2026-10-12 #19)"

      assert Process.alive?(pid), "precondition: terminate is still blocked in close_pool"

      # Unblock it: terminate proceeds and the coordinator exits.
      Process.exit(stuck, :kill)
      assert_receive {:DOWN, ^pref, :process, ^pid, _}, 10_000
      Task.await(terminator, 10_000)

      # The stream's own handle: close it (its coordinator is gone, so it closes locally).
      if clause == :force_stop_with_streams, do: ShardExecutor.close(ha)
    end
  end

  # Expert review 2026-10-12 #9. `Shard.checkin/4` is a cast. One still in the mailbox when the
  # coordinator terminates is a handle NOBODY closes; when its NIF resource is garbage collected,
  # `sqlite3_close_v2` runs against a path a successor coordinator may have recreated, and SQLite
  # (tracking connections per inode) can unlink the successor's -wal. A suspended coordinator keeps
  # the cast unprocessed deterministically; `terminate` must close it.
  test "a checkin cast left in a terminating coordinator's mailbox is closed (#9)", %{
    shard: shard
  } do
    {:ok, ha} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn = conn_of(ha)
    ref = ref_of(ha)
    refute closed?(conn), "precondition: the handle starts open"

    :ok = :sys.suspend(pid)
    :ok = Shard.checkin(pid, ref, conn, :rw)
    pref = Process.monitor(pid)
    terminator = Task.async(fn -> :sys.terminate(pid, :shutdown) end)
    assert_receive {:DOWN, ^pref, :process, ^pid, _}, 10_000
    Task.await(terminator, 10_000)

    assert closed?(conn),
           "the handle checked in to a terminating coordinator was never closed (expert review 2026-10-12 #9)"
  end

  defp eventually(fun, tries \\ 100) do
    cond do
      fun.() ->
        true

      tries == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, tries - 1)
    end
  end
end
