defmodule Fathom.Shard.PoolGraceTest do
  @moduledoc """
  The pool DRAIN GRACE, the statement-cache CARRY and the node-wide idle BUDGET (perf review
  2026-10-14 #4, #10, #16).

  THE WIN. Until 2026-10-14 the coordinator drained its handle pool the moment the shard's last
  stream checked in, so a SERIAL client (an HTTP SDK, Django's `CONN_MAX_AGE=0`) never reused a
  handle: every request paid a fresh open (the `-shm` rebuild, ~176 of the ~230 us) and a close-time
  checkpoint, and every reused handle re-prepared every statement. The counts below are the
  deterministic form of that win: handle opens and close-time checkpoints per N serial requests, and
  prepares per pooled request.

  THE RISK. An idle shard may now hold a SQLite connection, and its WAL, for up to the grace. Every
  barrier that relied on "an idle shard holds no connections" must drain the pool first: the
  durability flush (and the position stamp it writes), every stopping path (idle-stop, drain, lease
  loss), and the write fence. Each barrier has a test here, parameterized over both liveness modes
  where the path is lease-adjacent, with the mode asserted through `acquire_gen`.

  Not async: shards are global and file-backed, and the prepare counts trace this process.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard
  alias Fathom.Shard.{Connection, HandlePool, Heartbeat, PoolBudget, Storage, WriteFence}
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]
  @long_grace 60_000

  setup do
    keys = [
      :connection_pool,
      :connection_pool_opts,
      :connection_pool_idle_budget,
      :shard_idle_ms,
      :fence_writes_when_stealable,
      :shard_pool_close_hook
    ]

    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})
    Application.put_env(:fathom, :connection_pool, true)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    grace(@long_grace)
    shard = "pool_grace_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      _ = Shards.stop(shard)
      WriteFence.forget(shard)
      Enum.each(prev, fn {k, v} -> restore(k, v) end)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      # The lease-loss tests quarantine the local copy aside (`<shard>.db.fenced.*`).
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)
  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp conn_of({_pid, _ref, conn, _id, _sc, _v, _o}), do: conn
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid
  defp wal(shard), do: Path.join(Shard.data_dir(), "#{shard}.db-wal")
  defp pool_count(pid), do: HandlePool.count(:sys.get_state(pid).pool)

  # The pool reads its options at coordinator init, so set them before the shard opens.
  defp grace(ms), do: Application.put_env(:fathom, :connection_pool_opts, drain_grace_ms: ms)

  # One serial request: open a stream, run `sqls`, close it. Returns the stream handle.
  defp request(shard, sqls) do
    {:ok, h} = ShardExecutor.open(shard)
    for sql <- sqls, do: {:ok, _} = ShardExecutor.execute(h, stmt(sql))
    :ok = ShardExecutor.close(h)
    h
  end

  defp closed?(conn), do: not match?({:ok, _}, Sqlite3.prepare(conn, "SELECT 1"))

  defp await_closes(pid) do
    case :sys.get_state(pid).closing do
      tpid when is_pid(tpid) ->
        mref = Process.monitor(tpid)
        assert_receive {:DOWN, ^mref, :process, ^tpid, _}, 5_000

      nil ->
        :ok
    end
  end

  defp eventually(fun, tries \\ 200) do
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

  defp count_pool_takes do
    counts = :counters.new(2, [])
    handler = "pool-grace-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :pool_take],
      fn _e, _m, %{result: r}, _ -> :counters.add(counts, if(r == :hit, do: 1, else: 2), 1) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    counts
  end

  # Every close the coordinator's close chain runs is reported here. With the shard idle, each one
  # is the shard's LAST connection closing, i.e. one close-time checkpoint.
  defp count_chain_closes do
    test = self()

    Application.put_env(:fathom, :shard_pool_close_hook, fn conn ->
      send(test, {:chain_close, conn})
    end)
  end

  defp drain_chain_closes(n \\ 0) do
    receive do
      {:chain_close, _} -> drain_chain_closes(n + 1)
    after
      0 -> n
    end
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF (config/test.exs default)"
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  defp assert_mode!(pid, :heartbeat),
    do: assert(is_integer(:sys.get_state(pid).acquire_gen), "expected heartbeat mode")

  defp assert_mode!(pid, :legacy),
    do: assert(is_nil(:sys.get_state(pid).acquire_gen), "expected legacy mode")

  # A shard whose last stream wrote and closed INSIDE the grace: the pooled handle holds a live WAL,
  # and the coordinator stands on wal_ordinal 1 for a salt the WAL does not carry. That is the
  # fixture ShardPositionSeedTest uses: a stamp taken over the live WAL reads a salt the coordinator
  # never numbered and so carries NO ordinal, while a stamp taken after the close checkpointed the
  # WAL is the empty-WAL over-claim, ordinal 2. Returns {coordinator, the pooled conn}.
  defp written_and_pooled!(shard, mode) do
    set_mode!(mode)
    h = request(shard, ["CREATE TABLE kv (k INTEGER, v TEXT)", "INSERT INTO kv VALUES (1, 'a')"])
    pid = pid_of(h)
    assert_mode!(pid, mode)
    assert 1 = Shard.wal_ordinal(pid, 987_654_321)

    assert pool_count(pid) == 1, "precondition: the closed stream's handle is pooled in the grace"
    assert File.exists?(wal(shard)), "precondition: the pooled handle keeps the WAL alive"
    {pid, conn_of(h)}
  end

  describe "the win: serial requests inside the grace" do
    # The deterministic form of hrana_oneshot_rt 431 -> ~230 us. Both arms run here so the test is
    # its own discrimination proof: grace 0 is the pre-2026-10-14 behaviour.
    test "N serial requests make 1 handle open and 1 close-time checkpoint (grace 0: N and N)", %{
      shard: shard
    } do
      n = 20

      for {ms, opens, closes} <- [{0, n, n}, {200, 1, 1}] do
        grace(ms)
        id = "#{shard}_#{ms}"
        on_exit(fn -> Shards.stop(id) end)
        counts = count_pool_takes()
        count_chain_closes()

        hs =
          for i <- 1..n, do: request(id, ["INSERT INTO kv VALUES (#{i}, 'x')"] |> with_table(i))

        pid = pid_of(hd(hs))

        # Wait out the grace (if any) and the close it ends in.
        assert eventually(fn -> pool_count(pid) == 0 end),
               "the pool never drained after the grace"

        await_closes(pid)

        assert :counters.get(counts, 2) == opens,
               "grace #{ms} ms: #{:counters.get(counts, 2)} fresh opens for #{n} serial requests"

        assert :counters.get(counts, 1) == n - opens
        assert drain_chain_closes() == closes, "grace #{ms} ms: wrong close-time checkpoint count"

        refute File.exists?(wal(id)), "the last close must still checkpoint+unlink the WAL"
        Application.delete_env(:fathom, :shard_pool_close_hook)
        Shards.stop(id)

        for dir <- [Shard.data_dir(), Storage.Local.dir()],
            s <- [".db", ".db-wal", ".db-shm"],
            do: File.rm(Path.join(dir, id <> s))
      end
    end

    test "a pooled request prepares nothing: the statement cache and the temp probe travel", %{
      shard: shard
    } do
      sqls = [
        "SELECT v FROM kv WHERE k = 1",
        "INSERT INTO kv VALUES (2, 'b')",
        "SELECT count(*) FROM kv"
      ]

      request(shard, ["CREATE TABLE kv (k INTEGER, v TEXT)"])

      # A global call counter, not a per-process call trace: a process tracing ITSELF receives no
      # trace messages, and a first draft that did so read zero prepares on every code (probed
      # 2026-10-14). The module is not async, so nothing else is preparing meanwhile.
      mfa = {Sqlite3, :prepare, 2}
      on_exit(fn -> :erlang.trace_pattern(mfa, false, [:call_count]) end)

      count = fn fun ->
        :erlang.trace_pattern(mfa, true, [:call_count])
        result = fun.()
        {:call_count, n} = :erlang.trace_info(mfa, :call_count)
        :erlang.trace_pattern(mfa, false, [:call_count])
        {result, n}
      end

      # Warm: prepares the three statements (and the probe, the CREATE moved the schema generation),
      # then pools them all with the handle. It is also the counter's own precondition.
      {warm, warm_prepares} = count.(fn -> request(shard, sqls) end)
      assert warm_prepares >= 3, "precondition: the counter saw #{warm_prepares} prepares"

      {measured, prepares} = count.(fn -> request(shard, sqls) end)

      assert conn_of(measured) == conn_of(warm),
             "precondition: the request reused the pooled handle"

      assert prepares == 0,
             "a pooled request prepared #{prepares} statements; before the carry it re-prepared " <>
               "all three plus the temp-schema probe (4)"
    end

    test "a reused handle arrives with the previous stream's prepared statements", %{shard: shard} do
      request(shard, ["CREATE TABLE t (a INTEGER)"])
      first = request(shard, ["SELECT count(*) FROM t"])

      {:ok, second} = ShardExecutor.open(shard)
      conn = conn_of(second)
      assert conn == conn_of(first), "precondition: the serial stream reused the pooled handle"

      assert {_seq, cache} = Process.get({Connection, :stmt_cache, conn}),
             "the reused handle arrived with an empty statement cache"

      assert Map.has_key?(cache, "SELECT count(*) FROM t")
      :ok = ShardExecutor.close(second)
    end

    # The invariant the old purge protected. An unfinalized statement makes `sqlite3_close_v2` defer
    # the close, and a deferred close of the LAST connection skips the WAL checkpoint the position
    # stamp depends on. The test holds the cache itself, so GC cannot finalize the statements behind
    # the close's back (a first draft without that passed even when close_pooled skipped the
    # release, probed 2026-10-04).
    test "closing a pooled handle finalizes its carried statements first" do
      path = Path.join(System.tmp_dir!(), "pooledclose_#{System.unique_integer([:positive])}.db")
      on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)

      {:ok, conn} = Connection.open(path)
      :ok = Connection.exec(conn, "CREATE TABLE t (a INTEGER)")
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?)", [1])
      {:ok, _} = Connection.query(conn, "SELECT a FROM t", [])

      assert File.exists?(path <> "-wal"),
             "precondition: no WAL, so a skipped checkpoint shows nothing"

      cache = Connection.detach_statements(conn)
      assert {_seq, entries} = cache
      assert map_size(entries) == 2

      :ok = Connection.close_pooled({conn, cache})

      refute File.exists?(path <> "-wal"),
             "the WAL survived the last close: a carried statement was not finalized, so " <>
               "sqlite3_close_v2 deferred the close and skipped the checkpoint"

      assert map_size(elem(cache, 1)) == 2
    end
  end

  describe "the grace timer" do
    test "expires into a drain, re-arms after a newer idle transition, and no-ops while busy", %{
      shard: shard
    } do
      h = request(shard, ["SELECT 1"])
      pid = pid_of(h)
      assert pool_count(pid) == 1

      assert is_reference(:sys.get_state(pid).pool_grace_timer),
             "the idle transition arms the grace"

      # A fire inside the grace (idle_since is recent): no drain, re-armed for the remainder.
      send(pid, :pool_grace)
      assert pool_count(pid) == 1
      assert is_reference(:sys.get_state(pid).pool_grace_timer)

      # Busy again: a fire is a no-op and leaves no timer; the next idle transition re-arms.
      {:ok, busy} = ShardExecutor.open(shard)
      send(pid, :pool_grace)
      assert :sys.get_state(pid).pool_grace_timer == nil
      :ok = ShardExecutor.close(busy)
      assert is_reference(:sys.get_state(pid).pool_grace_timer)

      # Past the grace: the fire drains, and the close runs off the coordinator.
      conn =
        case :sys.get_state(pid).pool |> HandlePool.drain() |> elem(0) |> hd() do
          {c, _cache} -> c
          c -> c
        end

      # The node's live SQLite connections with the pooled handle still held; after the grace it
      # must be one fewer — idle residency returns to the pre-grace level.
      live = Connection.live_count()

      :sys.replace_state(
        pid,
        &%{&1 | idle_since: System.monotonic_time(:millisecond) - 2 * @long_grace}
      )

      send(pid, :pool_grace)
      assert pool_count(pid) == 0
      assert :sys.get_state(pid).pool_grace_timer == nil
      await_closes(pid)
      assert closed?(conn)
      refute File.exists?(wal(shard))
      assert Connection.live_count() == live - 1, "the grace drain did not release the connection"
    end
  end

  describe "barriers inside the grace" do
    for mode <- @modes do
      # Barrier 1. The pre-grace invariant (ShardPositionSeedTest) with the pool's WAL still live:
      # a flush inside the grace must drain first and stamp the empty-WAL over-claim, not a live read.
      test "#{mode}: a flush drains the pool first and stamps the empty-WAL position", %{
        shard: shard
      } do
        {pid, conn} = written_and_pooled!(shard, unquote(mode))

        assert :ok = Shards.flush(shard)

        assert closed?(conn), "the flush ran with the pooled handle still open"
        refute File.exists?(wal(shard)), "the flush's snapshot read a live WAL"
        assert {:ok, pos} = Storage.object_position(shard)

        assert is_map(pos) and Map.get(pos, :wal_ordinal) == 2,
               "a flush inside the grace stamped #{inspect(pos)}, not the empty-WAL over-claim " <>
                 "(wal_ordinal 1 + 1): it snapshotted over the pooled handle's live WAL"

        assert :sys.get_state(pid).pool_flush_hold == false, "the hold outlived its flush"
      end

      # Barrier 2: idle-stop. ShardPositionSeedTest with pooling and the grace on.
      test "#{mode}: an idle-stop inside the grace closes the handle before the drop flush stamps",
           %{shard: shard} do
        {pid, conn} = written_and_pooled!(shard, unquote(mode))
        ref = Process.monitor(pid)
        :sys.replace_state(pid, &%{&1 | idle_ms: 1})
        send(pid, :idle_timeout)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

        assert closed?(conn)
        assert {:ok, %{wal_ordinal: 2}} = Storage.object_position(shard)
      end

      # Barrier 2: a migration / node drain (and so a rebalancer handoff, which drains).
      test "#{mode}: a drain inside the grace closes the handle and drops a checkpointed file", %{
        shard: shard
      } do
        {_pid, conn} = written_and_pooled!(shard, unquote(mode))

        assert :ok = Shards.drain(shard, 5_000)
        assert closed?(conn)
        refute File.exists?(wal(shard))
        assert {:ok, %{wal_ordinal: 2}} = Storage.object_position(shard)
      end

      # Barrier 2: lease loss. The lease_lost clause publishes the write fence, then close_pool
      # closes the pooled handle before the shard's files are renamed aside.
      test "#{mode}: a lease loss inside the grace closes the pooled handle", %{shard: shard} do
        {pid, conn} = written_and_pooled!(shard, unquote(mode))
        ref = Process.monitor(pid)
        :sys.replace_state(pid, &%{&1 | lease_lost: true})
        GenServer.stop(pid, {:shutdown, :lease_lost})
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
        assert closed?(conn), "a pooled handle outlived the lease"
      end

      # Barrier 3: publishing the write fence drains the pool.
      test "#{mode}: publishing the write fence inside the grace drains the pool", %{shard: shard} do
        Application.put_env(:fathom, :fence_writes_when_stealable, true)
        {pid, conn} = written_and_pooled!(shard, unquote(mode))

        # Make the re-check answer :not_valid. Legacy: always (no lock-free read). Heartbeat: the
        # process ALIVE with a past deadline at the SAME generation (AGENTS.md § Testing).
        if unquote(mode) == :heartbeat do
          hb = :sys.get_state(Heartbeat)

          Heartbeat.publish_status(%{
            hb
            | mono_deadline_ms: System.monotonic_time(:millisecond) - 1
          })

          assert Heartbeat.valid_for_write?(:sys.get_state(pid).acquire_gen) == :not_valid,
                 "the fixture did not produce :not_valid"
        end

        since = System.monotonic_time(:millisecond) - 3_600_000
        :sys.replace_state(pid, &%{&1 | not_valid_since: since})
        send(pid, {:write_fence_check, since})

        assert pool_count(pid) == 0, "the write fence was published with an idle handle pooled"
        assert WriteFence.fenced?(shard), "precondition: the fence was published"
        await_closes(pid)
        assert closed?(conn)
      end
    end

    # The hold: while a flush waits for the idle pool's close, the grace is suspended, so a serial
    # client cannot keep re-filling the pool in front of the flush (each re-fill would defer it).
    test "a flush waiting on the idle pool's close suspends the grace until it runs", %{
      shard: shard
    } do
      {pid, _conn} = written_and_pooled!(shard, :legacy)

      send(pid, :durability_flush)
      st = :sys.get_state(pid)
      assert st.pool_flush_hold, "the flush drained the pool but did not hold the grace off"
      assert HandlePool.count(st.pool) == 0

      request(shard, ["SELECT 1"])
      assert pool_count(pid) == 0, "an idle transition re-filled the pool in front of the flush"

      assert :ok = Shards.flush(shard)
      refute :sys.get_state(pid).pool_flush_hold
      request(shard, ["SELECT 1"])
      assert pool_count(pid) == 1, "the grace did not come back after the flush"
    end
  end

  describe "the node-wide idle budget (#16)" do
    test "over the budget a checkin is closed, not pooled, and the next checkout still opens", %{
      shard: shard
    } do
      # A negative total (an earlier test mutated a pool without PoolBudget.adjust) is under ANY
      # budget, so admit?/0 would say yes here; name that instead of timing out below.
      assert PoolBudget.total() >= 0,
             "PoolBudget.total went negative: a test pooled a handle without accounting for it"

      Application.put_env(:fathom, :connection_pool_idle_budget, 0)
      test = self()
      handler = "pool-budget-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :shard, :pool_budget_full],
        fn _, m, _, _ ->
          send(test, {:budget_full, m})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      h = request(shard, ["SELECT 1"])
      pid = pid_of(h)
      # The checkin is a cast: sync on the coordinator before looking for its telemetry.
      _ = :sys.get_state(pid)
      assert_receive {:budget_full, %{count: 1}}, 1_000
      assert pool_count(pid) == 0, "a handle was pooled over the budget"
      await_closes(pid)
      assert closed?(conn_of(h)), "the refused handle leaked (never closed)"

      h2 = request(shard, ["SELECT 1"])
      assert conn_of(h2) != conn_of(h), "the next stream must open fresh (never refused)"
    end

    test "pooling counts against the budget, the drain gives it back, a killed coordinator's share is reclaimed",
         %{shard: shard} do
      base = PoolBudget.total()
      h = request(shard, ["SELECT 1"])
      pid = pid_of(h)
      # The checkin is a cast: sync on the coordinator before reading the count it adjusts.
      _ = :sys.get_state(pid)
      assert PoolBudget.total() == base + 1

      # The grace drain gives it back.
      :sys.replace_state(
        pid,
        &%{&1 | idle_since: System.monotonic_time(:millisecond) - 2 * @long_grace}
      )

      send(pid, :pool_grace)
      _ = :sys.get_state(pid)
      assert PoolBudget.total() == base

      # Pool again, then kill the coordinator without terminate: the DOWN reclaims its share.
      request(shard, ["SELECT 1"])
      _ = :sys.get_state(pid)
      assert PoolBudget.total() == base + 1
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

      assert eventually(fn -> PoolBudget.total() == base end),
             "a killed coordinator's pooled handle stayed counted against the node budget"
    end
  end

  defp with_table(sqls, 1), do: ["CREATE TABLE kv (k INTEGER, v TEXT)" | sqls]
  defp with_table(sqls, _), do: sqls
end
