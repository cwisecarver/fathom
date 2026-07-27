defmodule Fathom.ShardDrainTest do
  # Fathom.Shards.drain/2 stands a coordinator down (refuse new checkouts, drain
  # in-flight, flush, release the lease, stop) so the migrator can take over. Uses
  # the real coordinator + filesystem storage; not async (shards are global).
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Connection
  alias Filo.{Error, Stmt}

  setup do
    shard = "drain_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for dir <- [local_dir(), remote_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp local_db(shard), do: Path.join(local_dir(), "#{shard}.db")
  defp remote_db(shard), do: Path.join(remote_dir(), "#{shard}.db")
  defp lock_file(shard), do: Path.join(remote_dir(), "#{shard}.lock")

  test "drain on a shard with no coordinator is a no-op", %{shard: shard} do
    assert Shards.drain(shard) == :ok
  end

  # Review 2026-07-23 #19: drop_clean gated BOTH drop_local and release_lease behind
  # File.exists?(path) — but a brand-new shard whose checkout never opened a connection has
  # no local file (the pull found no object; exqlite creates the file on first connection
  # open), so a clean stop leaked the lock object. The next same-owner open then silently
  # downgraded from the optimistic 1-RTT create to the reclaim path, and a foreign node saw
  # {:held, us} while our heartbeat stayed fresh. Pre-fix the final assertion fails: the
  # .lock survives the drain.
  test "a clean stop with no local file still releases the lease (#19)", %{shard: shard} do
    {:ok, pid, ref, path} = Shards.checkout(shard)
    Shard.checkin(pid, ref)
    _ = :sys.get_state(pid)

    # Preconditions that make this the leak scenario: the lease was acquired, but no local
    # file was ever created (no connection opened on the brand-new shard).
    refute File.exists?(path)
    assert File.exists?(lock_file(shard)), "checkout must have acquired the lease"

    assert :ok = Shards.drain(shard, 5_000)

    refute File.exists?(lock_file(shard)),
           "a clean stop must release the lock even when no local file exists (#19)"
  end

  # Expert review #41 (contract test): drain/2 must leave the caller's mailbox free of
  # {:drain_aborted, _} whatever branch resolved it — a stale abort pins a DIFFERENT
  # coordinator pid on any later call, so it would sit in a long-lived caller's mailbox
  # forever. The specific leak window (an abort racing the +30s safety-net timeout)
  # isn't reproducible in-suite; this pins the observable hygiene contract.
  test "drain/2 leaves no stray drain_aborted in the caller's mailbox", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)

    # Busy path: the coordinator aborts the 50ms drain; drain/2 consumes the message.
    assert {:error, :busy} = Shards.drain(shard, 50)
    refute_receive {:drain_aborted, _}, 100

    # Completed path: the coordinator stops; no abort left behind either.
    :ok = ShardExecutor.close(conn)
    assert :ok = Shards.drain(shard, 1_000)
    refute_receive {:drain_aborted, _}, 100
  end

  # Expert review 2026-07-14 #20: an evict-if-idle probe (drain timeout 0, from the at-capacity
  # eviction path) against a BUSY shard must leave it untouched — the coordinator can't evict a
  # shard with checked-out connections, and flipping it to :draining (even for the 0 ms window
  # before the timer resumes) spuriously 503s a concurrent checkout of this healthy bystander.
  test "an evict-if-idle drain probe does not draining-refuse a concurrent checkout on a busy shard",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)

    # The eviction path's probe (a cast). A checkout enqueued right after it (FIFO, before the
    # pre-fix 0 ms drain-timer would resume) must NOT be refused with :draining.
    Shard.request_drain(pid, 0, self())
    result = Shard.checkout(pid)

    refute match?({:error, :draining}, result),
           "an evict-if-idle probe must not draining-503 a busy bystander"

    assert_receive {:drain_aborted, ^pid}, 500
    ShardExecutor.close(conn)
  end

  # Expert review #33: a checkout hitting a draining coordinator returned
  # {:error, :draining}, and open_error had no clause for it — the generic fallthrough
  # carried NO status, i.e. the transport-default 500. A drain is a routine, short-lived
  # migration state; client SDKs treat 500 as failure, not back-off, turning every
  # planned blue/green window into visible errors. The invariant: checkout-during-drain
  # surfaces as a retryable 503.
  test "opening a draining shard surfaces as a retryable 503, not a 500", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)

    # Held connection keeps the drain pending; new opens are refused meanwhile.
    Shard.request_drain(pid, 10_000, self())

    assert {:error, %Error{code: "FILO_DRAINING", status: 503}} = ShardExecutor.open(shard)

    ref = Process.monitor(pid)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 2_000
  end

  # Expert review #29: a second overlapping drain overwrote the first's timer and
  # reply_to WITHOUT cancelling the timer. The orphaned first timer then fired,
  # spuriously aborting the second drain long before its own window, while the first
  # caller was never notified at all (stuck on its 30 s safety net) — and the leaked
  # live timer could survive into a resumed-serving state and stop a serving shard
  # early. The invariant: a concurrent drain is refused immediately (drain_aborted),
  # and the first drain runs to completion undisturbed.
  test "a concurrent drain is refused; the first completes undisturbed", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)

    # First drain waits on the held connection (long window).
    Shard.request_drain(pid, 10_000, self())

    # Overlapping drain: refused immediately, no timer overwritten.
    Shard.request_drain(pid, 10_000, self())
    assert_receive {:drain_aborted, ^pid}, 1_000

    # The first drain still completes normally when the connection closes.
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 2_000
  end

  test "drain flushes the latest data, releases the lease, and stops", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))
    :ok = ShardExecutor.close(conn)

    assert :ok = Shards.drain(shard)

    assert File.exists?(remote_db(shard)), "data was flushed to storage"
    refute File.exists?(local_db(shard)), "local copy was dropped"
    refute File.exists?(lock_file(shard)), "lease was released"

    {:ok, ro} = Connection.open(remote_db(shard))
    assert {:ok, %{rows: [["alice"]]}} = Connection.query(ro, "SELECT v FROM kv", [])
    Connection.close(ro)
  end

  test "draining lets an in-flight connection finish, then flushes and stops", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('bob')"))

    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)

    Shard.request_drain(pid, 5_000, self())
    _ = :sys.get_state(pid)
    assert :sys.get_state(pid).draining

    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    refute File.exists?(lock_file(shard))

    {:ok, ro} = Connection.open(remote_db(shard))
    assert {:ok, %{rows: [["bob"]]}} = Connection.query(ro, "SELECT v FROM kv", [])
    Connection.close(ro)
  end

  test "checkout is refused while the shard is draining", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)

    Shard.request_drain(pid, 5_000, self())
    _ = :sys.get_state(pid)

    # Refused with the retryable drain code (expert review #33), not a generic 500.
    assert {:error, %Error{code: "FILO_DRAINING", status: 503}} = ShardExecutor.open(shard)

    ref = Process.monitor(pid)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
  end

  test "drain times out and the coordinator resumes when connections don't drain",
       %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)

    assert {:error, :busy} = Shards.drain(shard, 100)

    assert Process.alive?(pid)
    refute :sys.get_state(pid).draining

    ShardExecutor.close(conn)
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
