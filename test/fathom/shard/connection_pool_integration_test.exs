defmodule Fathom.Shard.ConnectionPoolIntegrationTest do
  @moduledoc """
  End-to-end connection pooling through the real request path (`Fathom.ShardExecutor` → the
  coordinator's per-shard `HandlePool`).

  The invariant that shapes every test: **an idle shard holds ZERO pooled connections.** A pooled
  handle keeps a SQLite connection open, and the durability / position-stamp / flush-timer machinery
  assumes an idle shard's last stream already checkpointed+unlinked the WAL — so the pool is drained
  the instant `conns` hits 0 (`release/1`). Pooling therefore serves OVERLAPPING streams: a handle
  checked in while the shard is still busy is reused by the next stream. The reuse observable is the
  SQLite connection reference itself. Not async — shards are global and file-backed.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.HandlePool
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.{Stmt, StmtResult}

  setup do
    prev_pool = Application.get_env(:fathom, :connection_pool)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    shard = "pool_it_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      restore(:connection_pool, prev_pool)
      restore(:shard_idle_ms, prev_idle)

      for base <- [
            Path.join(Fathom.Shard.data_dir(), "#{shard}.db"),
            Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}.db")
          ],
          suffix <- ["", "-wal", "-shm"],
          do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)
  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp pool_of(pid), do: :sys.get_state(pid).pool
  defp conn_of({_pid, _ref, conn, _id, _sc, _v, _o}), do: conn
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid

  # Expert review 2026-09-18 #16: pool_take/2 was a bare GenServer.call with NO :exit rescue (unlike
  # do_checkout/2). The grant from checkout/1 already succeeded, so a coordinator briefly blocked or
  # dead between the two calls turned a request whose slot is held into a 5xx. It must degrade to
  # :open (a fresh handle) on any exit. Pre-fix this RAISES an exit into the caller.
  test "pool_take degrades to :open when the coordinator is gone (#16)" do
    {dead, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}

    assert Fathom.Shard.pool_take(dead, :rw) == :open
    assert Fathom.Shard.pool_take(dead, :ro) == :open
  end

  # Expert review 2026-09-18 #18: checkout/2 used to make TWO coordinator round-trips (:checkout,
  # then a separate :pool_take). Fathom.Shard.checkout/2 now folds the pool take into the grant
  # reply — one call. This pins that the folded primitive returns the pooled handle in the reply.
  test "Fathom.Shard.checkout/2 returns a pooled handle in the grant reply, in one call (#18)", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, pid, ref, path, :open} = Shards.checkout(shard, :rw)
    on_exit(fn -> Fathom.Shard.checkin(pid, ref) end)

    # Inject a pooled :rw handle; the shard stays busy (ref held), so it is not drained.
    {:ok, h} = Fathom.Shard.Connection.open(path)

    :sys.replace_state(pid, fn s ->
      {pool, []} = HandlePool.put(s.pool, :rw, h, System.monotonic_time(:millisecond))
      %{s | pool: pool}
    end)

    # The folded grant returns the pooled handle as its 4th element — no separate pool_take call.
    assert {:ok, ref2, ^path, {:reuse, ^h}} = Fathom.Shard.checkout(pid, :rw)
    Fathom.Shard.checkin(pid, ref2)
  end

  # Expert review 2026-09-18 #19: HandlePool.sweep/2 + ttl_ms existed and were documented as the
  # density bound, but NOTHING called sweep/2 — the ttl was inert. The coordinator now arms a
  # ttl-cadence :sweep_pool timer. Long ttl here so the coordinator's OWN auto-sweep can't fire
  # during the test; we inject a pre-aged handle and drive one sweep manually.
  test "pooling ON: the coordinator sweeps a pooled handle idle past the ttl (#19)", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)
    prev_opts = Application.get_env(:fathom, :connection_pool_opts)
    Application.put_env(:fathom, :connection_pool_opts, max_per_scope: 4, ttl_ms: 60_000)
    on_exit(fn -> restore(:connection_pool_opts, prev_opts) end)

    {:ok, pid, ref, path} = Shards.checkout(shard)

    on_exit(fn ->
      Fathom.Shard.checkin(pid, ref)
      Shards.drain(shard, 2_000)
    end)

    # A real idle handle aged 100s (well past the 60s ttl), injected into the coordinator's pool.
    {:ok, h} = Fathom.Shard.Connection.open(path)
    now = System.monotonic_time(:millisecond)

    :sys.replace_state(pid, fn s ->
      {pool, []} = HandlePool.put(s.pool, :rw, h, now - 100_000)
      %{s | pool: pool}
    end)

    assert HandlePool.count(pool_of(pid), :rw) == 1

    # Pre-fix nothing handled :sweep_pool, so the aged handle stayed pooled indefinitely.
    send(pid, :sweep_pool)
    _ = :sys.get_state(pid)

    assert HandlePool.count(pool_of(pid), :rw) == 0,
           "a pooled handle idle past the ttl must be swept (pre-fix sweep/2 had no call site)"
  end

  test "pooling ON: a handle checked in while the shard is still busy is reused by the next stream",
       %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, %StmtResult{}} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, %StmtResult{}} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('a')"))

    # A SECOND stream keeps the shard busy, so closing A pools its handle instead of idle-draining.
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)

    :ok = ShardExecutor.close(ha)
    assert HandlePool.count(pool_of(pid)) == 1, "A's handle was pooled (the shard is still busy)"

    {:ok, hc} = ShardExecutor.open(shard)
    assert conn_of(hc) == conn_a, "the next stream reused A's pooled handle"
    assert HandlePool.count(pool_of(pid)) == 0

    assert {:ok, %StmtResult{rows: [["a"]]}} = ShardExecutor.execute(hc, stmt("SELECT v FROM kv"))
    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  # Expert review 2026-09-29 #6: reset_for_reuse/2 rolls back and re-runs configure/1 — it does not
  # touch the TEMP schema, nor connection-local switches configure/1 never sets. A pooled handle
  # therefore carried a stream's TEMP table (shadowing the main table of the same name for the next
  # stream's unqualified SQL — reads saw the temp rows, writes went to a table that is never flushed)
  # and its `ignore_check_constraints`. Pre-fix both handles were pooled and reused.
  test "pooling ON: a handle holding a TEMP object is closed, not pooled", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('real')"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TEMP TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('fake')"))

    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    :ok = ShardExecutor.close(ha)

    assert HandlePool.count(pool_of(pid)) == 0, "a handle holding a TEMP table was pooled"

    {:ok, hc} = ShardExecutor.open(shard)
    refute conn_of(hc) == conn_a, "the next stream reused the handle holding a TEMP table"

    assert {:ok, %StmtResult{rows: [["real"]]}} =
             ShardExecutor.execute(hc, stmt("SELECT v FROM kv")),
           "an unqualified read saw another stream's TEMP table instead of the main one"

    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: a handle a stream set a session-only pragma on is closed, not pooled", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE c (n INTEGER CHECK (n >= 0))"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("PRAGMA ignore_check_constraints = ON"))

    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    :ok = ShardExecutor.close(ha)

    assert HandlePool.count(pool_of(pid)) == 0,
           "a handle with ignore_check_constraints=ON was pooled for the next stream"

    {:ok, hc} = ShardExecutor.open(shard)

    assert {:error, _} = ShardExecutor.execute(hc, stmt("INSERT INTO c VALUES (-5)")),
           "a later stream inherited ignore_check_constraints and wrote a CHECK-violating row"

    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  # Expert review 2026-09-29 #11: a pooled handle does not see a sibling's DDL until it re-reads the
  # schema cookie, and a bare `prepare` does not — so the statement-cache miss path took the OLD
  # column list and `SELECT *` returned two names over three values. Pre-fix the column count is 1
  # over rows of width 2. `:ro` is the case that matters: its reset never ran the `max_page_count`
  # pragma that incidentally refreshed `:rw`.
  test "pooling ON: a reused :ro handle sees a sibling's ADD COLUMN in its column names", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, w} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(w, stmt("CREATE TABLE s (a INTEGER)"))
    {:ok, _} = ShardExecutor.execute(w, stmt("INSERT INTO s VALUES (1)"))

    {:ok, ra} = ShardExecutor.open(shard, :ro)
    assert {:ok, %StmtResult{cols: [_]}} = ShardExecutor.execute(ra, stmt("SELECT * FROM s"))
    pid = pid_of(ra)
    conn_a = conn_of(ra)
    :ok = ShardExecutor.close(ra)
    assert HandlePool.count(pool_of(pid), :ro) == 1, "precondition: the :ro handle was not pooled"

    {:ok, _} = ShardExecutor.execute(w, stmt("ALTER TABLE s ADD COLUMN b INTEGER"))

    {:ok, rc} = ShardExecutor.open(shard, :ro)
    assert conn_of(rc) == conn_a, "precondition: the :ro handle was not reused"

    assert {:ok, %StmtResult{cols: cols, rows: [row]}} =
             ShardExecutor.execute(rc, stmt("SELECT * FROM s"))

    assert length(cols) == length(row),
           "a reused handle reported #{length(cols)} column names over rows of width " <>
             "#{length(row)} after a sibling's ALTER TABLE"

    :ok = ShardExecutor.close(rc)
    :ok = ShardExecutor.close(w)
  end

  # The availability direction: Django sends `PRAGMA foreign_keys = ON` on every new connection. It
  # is re-applied by configure/1 on reuse, so it must NOT defeat pooling — otherwise the fix would
  # silently turn pooling off for every Django client.
  test "pooling ON: a foreign_keys pragma (reset on reuse) does not defeat pooling", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("PRAGMA foreign_keys = ON"))

    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    :ok = ShardExecutor.close(ha)

    assert HandlePool.count(pool_of(pid)) == 1, "a foreign_keys pragma stopped the handle pooling"
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: draining a pooled handle checkpoints+unlinks the WAL (A2 durability invariant)",
       %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('a')"))
    pid = pid_of(ha)
    wal = Path.join(Fathom.Shard.data_dir(), "#{shard}.db-wal")
    assert File.exists?(wal), "precondition: the write is in the -wal"

    # Last stream closes → checkin/4 → drain → Connection.close. This MUST checkpoint+unlink the WAL
    # exactly as a non-pooled stream close does; if the stream's prepared statements were not
    # finalized in the stream process first (release_owner_state/1), sqlite3_close_v2 would defer the
    # close and skip the checkpoint, leaving the WAL and desyncing the empty-WAL position stamp A2
    # promote-on-open ranks on.
    :ok = ShardExecutor.close(ha)
    _ = :sys.get_state(pid)

    refute File.exists?(wal),
           "draining a pooled handle left the -wal in place — the deferred-close / WAL-checkpoint bug"
  end

  test "pooling ON: an idle shard drains its pool — the durability invariant", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    # A is the ONLY stream — closing it takes the shard idle, which must drain the pool.
    :ok = ShardExecutor.close(ha)

    assert HandlePool.count(pool_of(pid)) == 0,
           "an idle shard must hold no pooled connection, or the next flush stamps the wrong position"

    {:ok, hb} = ShardExecutor.open(shard)
    assert conn_of(hb) != conn_a, "the drained handle was closed; the next stream opens fresh"
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: an uncommitted write from the reused stream does not survive", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('ghost')"))
    :ok = ShardExecutor.close(ha)

    {:ok, hc} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{rows: []}} = ShardExecutor.execute(hc, stmt("SELECT v FROM kv")),
           "reset_for_reuse rolled back the reused handle's open transaction"

    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  # Expert review 2026-09-18 #3: a stream that checks in MID-TRANSACTION (BEGIN;INSERT, no commit)
  # used to pool the handle STILL holding the SQLite write lock — the rollback was deferred to the
  # next reuse (reset_for_reuse/2). During the idle interval every OTHER :rw stream on the shard
  # blocked on the 5s busy_timeout then hit SQLITE_BUSY, and the WAL grew unbounded. close/1 now rolls
  # the handle back at checkin, in the stream process, before it is pooled. This is the
  # concurrent-writer case the reuse-time rollback test above does NOT cover: there the SAME handle is
  # reused, so the lazy rollback hides the stranded lock; here a SEPARATE handle competes for it.
  test "pooling ON: a mid-transaction checkin does not strand the write lock for a concurrent writer (#3)",
       %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))

    # A SECOND concurrent :rw stream on its OWN handle (the pool is empty while A is checked out), so
    # it cannot reuse A's handle — it competes for the shard's single SQLite write lock.
    {:ok, hb} = ShardExecutor.open(shard)

    # A opens a write transaction and checks in WITHOUT committing.
    {:ok, _} = ShardExecutor.execute(ha, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(ha, stmt("INSERT INTO kv VALUES ('a')"))
    :ok = ShardExecutor.close(ha)

    # THE ASSERTION: B takes the write lock immediately. Pre-fix A's pooled handle held it, so this
    # blocked for the full 5s busy_timeout and returned SQLITE_BUSY.
    assert {:ok, %StmtResult{}} = ShardExecutor.execute(hb, stmt("INSERT INTO kv VALUES ('b')")),
           "a concurrent writer was blocked by the checked-in handle's stranded write lock"

    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: scope isolation — a :ro stream is NOT handed the :rw pooled handle", %{
    shard: shard
  } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_rw = conn_of(ha)
    :ok = ShardExecutor.close(ha)
    assert HandlePool.count(pool_of(pid), :rw) == 1

    {:ok, hc} = ShardExecutor.open(shard, :ro)
    assert conn_of(hc) != conn_rw, "a :ro stream must never reuse the :rw handle"
    assert HandlePool.count(pool_of(pid), :rw) == 1, "the :rw handle stayed in its own bucket"
    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  test "pooling OFF (default): the handle is closed on checkin, never pooled", %{shard: shard} do
    Application.put_env(:fathom, :connection_pool, false)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    :ok = ShardExecutor.close(ha)

    assert pool_of(pid) == nil, "pooling off ⇒ the coordinator holds no pool at all"

    {:ok, hc} = ShardExecutor.open(shard)
    assert conn_of(hc) != conn_a, "each stream opens its own fresh handle when pooling is off"
    :ok = ShardExecutor.close(hc)
    :ok = ShardExecutor.close(hb)
  end

  test "pooling ON: a busy shard terminating closes its pooled handles (none outlive the lease)",
       %{
         shard: shard
       } do
    Application.put_env(:fathom, :connection_pool, true)

    {:ok, ha} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(ha, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_a = conn_of(ha)
    :ok = ShardExecutor.close(ha)
    assert HandlePool.count(pool_of(pid)) == 1, "A pooled while B keeps the shard busy"

    # Force-stop with B still checked out: the busy terminate clause runs close_pool/1.
    mref = Process.monitor(pid)
    Shards.stop(shard)
    assert_receive {:DOWN, ^mref, :process, ^pid, _}, 5_000

    assert {:error, _} = Sqlite3.execute(conn_a, "SELECT 1"),
           "close_pool/1 closed the pooled handle on terminate"
  end
end
