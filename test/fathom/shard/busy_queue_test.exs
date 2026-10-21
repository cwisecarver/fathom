defmodule Fathom.Shard.BusyQueueTest do
  # Expert review 2026-10-20-2027 #13 (the time suffix separates it from the earlier 2026-10-20
  # series). A lock wait the busy handler yields to the BEAM used to back off on SQLite's 1..100 ms
  # ladder, per waiter. That is ~LIFO: the longest waiter, on 50-100 ms steps, loses the freed lock
  # to the newest one, still on 1-2 ms steps. The hot shard's write tail showed it (K=32 writers:
  # p99 38 ms, max 2.2 s; `busy_fairness_bench_test.exs`), and nothing counted the retries.
  #
  # Now: a per-shard FIFO queue, woken by the statement that leaves the write lock free, and a
  # `[:fathom, :shard, :busy_retry]` telemetry event per wait episode. These tests pin the
  # invariants: waiters acquire in ARRIVAL order, a waiter killed mid-wait does not strand the
  # queue, nothing leaks into a waiter's mailbox, and every episode is counted.
  #
  # async: false — the queue's waiter count is node-wide.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  @queue Fathom.Shard.Connection.BusyQueue

  setup do
    unless Fathom.Shard.Extension.available?(),
      do: flunk("fathom_udf extension not built: these tests would measure nothing")

    prev = Application.get_env(:fathom, :query_timeout_ms)
    Application.delete_env(:fathom, :query_timeout_ms)

    on_exit(fn ->
      if prev, do: Application.put_env(:fathom, :query_timeout_ms, prev)
    end)

    :ok
  end

  defp db_path!(tag) do
    path =
      Path.join(System.tmp_dir!(), "busy_queue_#{tag}_#{System.unique_integer([:positive])}.db")

    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    path
  end

  defp open!(path) do
    {:ok, conn} = Connection.open(path, tenant?: true)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  defp q(conn, sql, args \\ []) do
    Connection.query(conn, sql, args, deadline: true, dml?: String.starts_with?(sql, "INSERT"))
  end

  # `holder` has the write lock inside BEGIN IMMEDIATE, taken through `query/4` so its COMMIT is
  # the statement that hands the lock on.
  defp locked_shard!(tag) do
    path = db_path!(tag)
    holder = open!(path)
    :ok = Connection.exec(holder, "CREATE TABLE t (a INTEGER)")
    assert {:ok, _} = q(holder, "BEGIN IMMEDIATE")
    assert {:ok, _} = q(holder, "INSERT INTO t VALUES (0)")
    {path, holder}
  end

  defp spawn_writer(path, i) do
    test = self()

    spawn(fn ->
      {:ok, conn} = Connection.open(path, tenant?: true)
      send(test, {:writing, i})
      result = q(conn, "INSERT INTO t VALUES (?1)", [i])
      {:message_queue_len, mq} = Process.info(self(), :message_queue_len)
      send(test, {:wrote, i, result, mq})
      Connection.close(conn)
    end)
  end

  # Arrival is staggered by a fixed gap so each writer is queued (its first step yields BUSY at
  # once) before the next one starts. Under the old ladder the gap is what made the order LIFO.
  defp gap do
    receive do
    after
      15 -> :ok
    end
  end

  test "lock waiters acquire the freed lock in arrival order (FIFO)" do
    {path, holder} = locked_shard!("fifo")
    n = 8

    for i <- 1..n do
      spawn_writer(path, i)
      assert_receive {:writing, ^i}, 2_000
      gap()
    end

    assert {:ok, _} = q(holder, "COMMIT")

    for i <- 1..n do
      assert_receive {:wrote, ^i, result, mailbox_len}, 6_000
      assert {:ok, %{num_changes: 1}} = result
      # A wake that loses the race with the dequeue is dropped (the waiter's alias is gone), not
      # left in the stream process's mailbox.
      assert mailbox_len == 0
    end

    assert {:ok, %{rows: rows}} = q(holder, "SELECT a FROM t ORDER BY rowid")

    assert List.flatten(rows) == Enum.to_list(0..n),
           "writers acquired the lock out of arrival order: #{inspect(List.flatten(rows))}"

    assert :ets.info(@queue, :size) == 0
  end

  test "a waiter killed mid-wait does not strand the queue behind it" do
    {path, holder} = locked_shard!("dead")

    victim = spawn_writer(path, 1)
    assert_receive {:writing, 1}, 2_000
    gap()
    spawn_writer(path, 2)
    assert_receive {:writing, 2}, 2_000
    gap()

    ref = Process.monitor(victim)
    Process.exit(victim, :kill)
    assert_receive {:DOWN, ^ref, :process, ^victim, :killed}

    {us, {:ok, _}} = :timer.tc(fn -> q(holder, "COMMIT") end)
    t0 = System.monotonic_time(:millisecond)
    assert_receive {:wrote, 2, {:ok, %{num_changes: 1}}, 0}, 2_000
    waited = System.monotonic_time(:millisecond) - t0

    # The dead head is reaped by the COMMIT's handoff, so writer 2 is woken at once rather than
    # finding the lock free on its next 100 ms backstop poll.
    assert waited < 50, "writer 2 took #{waited} ms after the COMMIT (#{div(us, 1000)} ms)"
    assert :ets.info(@queue, :size) == 0, "the killed waiter's queue row leaked"
  end

  test "each yielded wait is one [:fathom, :shard, :busy_retry] event" do
    test = self()
    handler = "busy-queue-test-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler,
        [:fathom, :shard, :busy_retry],
        fn _, m, md, _ -> send(test, {:busy_retry, m, md}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    # A wait that succeeds once the holder commits.
    {path, holder} = locked_shard!("tel")
    spawn_writer(path, 1)
    assert_receive {:writing, 1}, 2_000
    gap()
    assert {:ok, _} = q(holder, "COMMIT")
    assert_receive {:wrote, 1, {:ok, _}, 0}, 2_000

    assert_receive {:busy_retry, %{attempts: a, wait_us: w, oversleep_us: o},
                    %{kind: :step, result: :ok}}

    assert a >= 1 and w >= 10_000 and o >= 0

    # A wait that gives up at the busy timeout.
    {path2, _holder2} = locked_shard!("tel2")
    blocked = open!(path2)
    :ok = Connection.set_busy_timeout(blocked, 100)
    assert {:error, :busy} = q(blocked, "INSERT INTO t VALUES (9)")

    assert_receive {:busy_retry, %{attempts: a2, wait_us: w2}, %{kind: :step, result: :busy}}
    assert a2 >= 1 and w2 >= 90_000
  end
end
