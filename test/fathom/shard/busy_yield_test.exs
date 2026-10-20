defmodule Fathom.Shard.BusyYieldTest do
  # Expert review 2026-10-20 #2: lock waiters slept INSIDE the dirty-IO NIF.
  #
  # Every exqlite call that steps a statement is `ERL_NIF_DIRTY_JOB_IO_BOUND`, and the busy handler
  # (native/fathom_udf/src/backstop.rs) slept its 5 s ladder on that thread. With more same-lock
  # waiters than dirty-IO threads (+SDio, 10), EVERY SQLite op and file I/O on the node queued
  # behind sleepers: measured, an unrelated shard's SELECT took ~2.1 s at 11 waiters (0 ms at 9),
  # and the lock holder's own COMMIT queued behind its waiters (a convoy).
  #
  # Fix: tenant handles yield a lock wait to the BEAM (`fathom_busy_yield(1)`), and
  # `Connection.query/4` retries the statement from the owner process with a `receive after`
  # backoff, bounded by the busy timeout and the statement deadline. Only a BUSY the busy handler
  # yielded is retried: autocommit, BEGIN IMMEDIATE, and an explicit transaction's FIRST lock
  # (Django's `transaction.atomic`: BEGIN then a write). A transaction that already read and then
  # cannot become a writer is never retried, and the tenant script path keeps the in-handler wait.
  #
  # async: false — the first test's whole point is the node-wide dirty-IO pool, and a concurrent
  # suite's NIF traffic would only add noise to the latency it measures.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  setup do
    unless Fathom.Shard.Extension.available?(),
      do: flunk("fathom_udf extension not built: these tests would measure nothing")

    prev = Application.get_env(:fathom, :query_timeout_ms)
    Application.delete_env(:fathom, :query_timeout_ms)
    on_exit(fn -> restore(:query_timeout_ms, prev) end)
    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, v), do: Application.put_env(:fathom, key, v)

  defp db_path!(tag) do
    path =
      Path.join(System.tmp_dir!(), "busy_yield_#{tag}_#{System.unique_integer([:positive])}.db")

    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    path
  end

  defp open!(path) do
    {:ok, conn} = Connection.open(path, tenant?: true)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  defp q(conn, sql, args \\ []), do: Connection.query(conn, sql, args, deadline: true)

  # `holder` has the write lock of a fresh shard, inside BEGIN IMMEDIATE, and keeps it.
  defp locked_shard! do
    path = db_path!("hot")
    holder = open!(path)
    :ok = Connection.exec(holder, "CREATE TABLE t (a INTEGER)")
    :ok = Connection.exec(holder, "BEGIN IMMEDIATE")
    :ok = Connection.exec(holder, "INSERT INTO t VALUES (1)")
    {path, holder}
  end

  # Two waiter shapes: a bare autocommit INSERT, and Django's `transaction.atomic` (BEGIN, then the
  # INSERT is the transaction's first lock, then COMMIT). The Django shape is the follow-up
  # (approved 2026-10-20): e5fa30d9 yielded only in autocommit, and against it this variant still
  # stalled the unrelated shard.
  for shape <- [:autocommit, :django] do
    @tag shape: shape
    test "more lock waiters than dirty-IO threads do not stall an unrelated shard (#{shape})",
         %{shape: shape} do
      lock_waiters_do_not_stall_an_unrelated_shard(shape)
    end
  end

  defp waiter_write(conn, i, :autocommit), do: q(conn, "INSERT INTO t VALUES (?1)", [i])

  defp waiter_write(conn, i, :django) do
    with {:ok, _} <- q(conn, "BEGIN"),
         {:ok, inserted} <- q(conn, "INSERT INTO t VALUES (?1)", [i]),
         {:ok, _} <- q(conn, "COMMIT") do
      {:ok, inserted}
    end
  end

  defp lock_waiters_do_not_stall_an_unrelated_shard(shape) do
    other = open!(db_path!("other"))
    :ok = Connection.exec(other, "CREATE TABLE u (x INTEGER); INSERT INTO u VALUES (1)")
    assert {:ok, %{rows: [[1]]}} = q(other, "SELECT x FROM u")

    {path, holder} = locked_shard!()
    waiters = :erlang.system_info(:dirty_io_schedulers) + 2
    test = self()

    # Open every waiter's handle BEFORE any waits, so the opens themselves are not part of what
    # is measured. Each waiter then signals and immediately blocks on the held lock.
    pids =
      for i <- 1..waiters do
        spawn_link(fn ->
          {:ok, conn} = Connection.open(path, tenant?: true)
          send(test, {:opened, self()})

          receive do
            :go -> :ok
          end

          send(test, {:waiting, self()})
          send(test, {:result, self(), waiter_write(conn, i, shape)})
          Connection.close(conn)
        end)
      end

    for pid <- pids, do: assert_receive({:opened, ^pid}, 5_000)
    for pid <- pids, do: send(pid, :go)
    for pid <- pids, do: assert_receive({:waiting, ^pid}, 5_000)

    # Probe the unrelated shard for 400 ms while every waiter is blocked. Unfixed, the first probe
    # queues behind `waiters` sleepers holding all +SDio threads, for the whole busy timeout.
    worst_us = probe_worst_us(other, System.monotonic_time(:millisecond) + 400, 0)

    assert worst_us < 200_000,
           "an unrelated shard's SELECT took #{div(worst_us, 1000)} ms while #{waiters} writers " <>
             "waited on another shard's lock: the waiters are holding the dirty-IO threads"

    # The holder's COMMIT must not convoy behind its own waiters either.
    {commit_us, :ok} = :timer.tc(fn -> Connection.exec(holder, "COMMIT") end)
    assert commit_us < 200_000, "the lock holder's COMMIT took #{div(commit_us, 1000)} ms"

    # Every waiter's write lands once the lock is free: a yielded wait is retried, not failed.
    for pid <- pids do
      assert_receive {:result, ^pid, result}, 6_000
      assert {:ok, %{num_changes: 1}} = result
    end

    assert {:ok, %{rows: [[n]]}} = q(holder, "SELECT count(*) FROM t")
    assert n == waiters + 1
  end

  defp probe_worst_us(conn, until_ms, worst) do
    if System.monotonic_time(:millisecond) >= until_ms do
      worst
    else
      {us, {:ok, _}} = :timer.tc(fn -> q(conn, "SELECT x FROM u") end)
      probe_worst_us(conn, until_ms, max(us, worst))
    end
  end

  test "an autocommit write retried from the BEAM still gives up at the busy timeout" do
    {path, _holder} = locked_shard!()
    blocked = open!(path)
    :ok = Connection.set_busy_timeout(blocked, 300)

    {us, result} = :timer.tc(fn -> q(blocked, "INSERT INTO t VALUES (2)") end)
    assert {:error, :busy} = result
    # Gave up at the 300 ms budget, not at once (no retry) and not at the 5 s default.
    assert us >= 250_000, "gave up after #{div(us, 1000)} ms: the lock wait was not retried"
    assert us < 1_500_000, "gave up only after #{div(us, 1000)} ms"
  end

  test "the BEAM retry stops at the statement deadline and reports a timeout" do
    Application.put_env(:fathom, :query_timeout_ms, 200)
    {path, _holder} = locked_shard!()
    blocked = open!(path)

    {us, result} = :timer.tc(fn -> q(blocked, "INSERT INTO t VALUES (2)") end)
    assert {:error, :query_timeout} = result, "got #{inspect(result)} after #{div(us, 1000)} ms"
    assert us < 1_500_000, "stopped only after #{div(us, 1000)} ms"
  end

  test "an explicit transaction's first write is retried within the busy budget, transaction intact" do
    {path, _holder} = locked_shard!()
    blocked = open!(path)
    :ok = Connection.set_busy_timeout(blocked, 300)
    assert {:ok, _} = q(blocked, "BEGIN")

    {us, result} = :timer.tc(fn -> q(blocked, "INSERT INTO t VALUES (2)") end)
    assert {:error, :busy} = result
    assert us >= 250_000, "gave up after #{div(us, 1000)} ms: the first lock was not retried"
    assert us < 1_500_000, "gave up only after #{div(us, 1000)} ms"
    refute Connection.autocommit?(blocked), "the BUSY must not have ended the transaction"
  end

  # The BUSY a transaction gets when it already READ and then tries to become a writer while
  # another connection holds the write lock never reaches the busy handler, and re-stepping it can
  # never succeed (once the holder commits, the snapshot is stale). It must fail at once, as before;
  # a retry would burn the whole 5 s busy timeout. This is what the extension's "which statement
  # yielded" bookkeeping exists for: exqlite reports both BUSYs as the same `:busy`.
  test "a transaction that already read is not retried when its first write hits BUSY" do
    {path, _holder} = locked_shard!()
    reader = open!(path)

    assert {:ok, _} = q(reader, "BEGIN")
    assert {:ok, %{rows: [[0]]}} = q(reader, "SELECT count(*) FROM t")

    {us, result} = :timer.tc(fn -> q(reader, "INSERT INTO t VALUES (2)") end)
    assert {:error, :busy} = result
    assert us < 200_000, "an upgrade BUSY took #{div(us, 1000)} ms: it was retried"
    refute Connection.autocommit?(reader)
  end

  test "a stale-snapshot write inside a transaction fails at once (not retried)" do
    path = db_path!("snap")
    writer = open!(path)
    reader = open!(path)
    :ok = Connection.exec(writer, "CREATE TABLE t (a INTEGER)")

    assert {:ok, _} = q(reader, "BEGIN")
    assert {:ok, %{rows: [[0]]}} = q(reader, "SELECT count(*) FROM t")
    assert {:ok, _} = q(writer, "INSERT INTO t VALUES (1)")

    # SQLITE_BUSY_SNAPSHOT: retrying can never succeed (the snapshot stays stale), so it must
    # surface immediately, as before.
    {us, result} = :timer.tc(fn -> q(reader, "INSERT INTO t VALUES (2)") end)
    assert {:error, :busy} = result
    assert us < 200_000, "a stale-snapshot BUSY took #{div(us, 1000)} ms"
    refute Connection.autocommit?(reader)
  end

  test "a tenant script keeps waiting in the handler: a mid-script retry is not valid" do
    {path, _holder} = locked_shard!()
    blocked = open!(path)
    :ok = Connection.set_busy_timeout(blocked, 300)

    {us, result} =
      :timer.tc(fn ->
        Connection.exec_script(blocked, "INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)")
      end)

    assert {:error, _} = result
    assert us >= 250_000, "the script's lock wait returned after #{div(us, 1000)} ms"

    # ...and yielding is back on for the next ordinary statement.
    assert {:ok, %{rows: [[1]]}} = q(blocked, "SELECT fathom_busy_yield(-1)")
  end

  # Round-4 durability review 2026-10-20 F3 (claim DISPROVEN; this is an invariant guard, not a
  # regression test). The review predicted that in rollback-journal mode (a tenant can pick it) a
  # COMMIT needing EXCLUSIVE yields at once and is never retried, so the client gets an instant
  # BUSY. Measured: the same test passes with and without the prescribed `txn_state < WRITE`
  # check, because `fathom_busy_retry_ms()` grants the retry whenever the previous statement
  # yielded, and re-stepping a BUSY COMMIT is valid. This pins the behaviour the review feared
  # losing, so a future change to the retry bookkeeping cannot silently drop it.
  test "a rollback-journal COMMIT waits for readers instead of failing BUSY at once" do
    path = db_path!("rollback")
    {:ok, writer} = Connection.open(path, tenant?: true, wal?: false)
    on_exit(fn -> Connection.close(writer) end)
    {:ok, reader} = Connection.open(path, tenant?: true, wal?: false)
    on_exit(fn -> Connection.close(reader) end)

    assert {:ok, %{rows: [["delete"]]}} = q(writer, "PRAGMA journal_mode")
    :ok = Connection.exec(writer, "CREATE TABLE t (a INTEGER)")

    # The reader holds a SHARED lock (explicit transaction that has read).
    assert {:ok, _} = q(reader, "BEGIN")
    assert {:ok, %{rows: [[0]]}} = q(reader, "SELECT count(*) FROM t")

    assert {:ok, _} = q(writer, "BEGIN")
    assert {:ok, _} = q(writer, "INSERT INTO t VALUES (1)")

    release =
      Task.async(fn ->
        Process.sleep(300)
        q(reader, "COMMIT")
      end)

    {us, result} = :timer.tc(fn -> q(writer, "COMMIT") end)
    assert {:ok, _} = result, "COMMIT failed after #{div(us, 1000)} ms: #{inspect(result)}"
    assert us >= 250_000, "COMMIT returned after #{div(us, 1000)} ms: it did not wait for readers"
    assert {:ok, _} = Task.await(release)
    assert Connection.autocommit?(writer)
  end

  test "fathom's own (non-tenant) handles never yield" do
    conn = Connection.open(db_path!("own")) |> then(fn {:ok, c} -> c end)
    on_exit(fn -> Connection.close(conn) end)
    assert {:ok, %{rows: [[0]]}} = Connection.query(conn, "SELECT fathom_busy_yield(-1)", [])
  end
end
