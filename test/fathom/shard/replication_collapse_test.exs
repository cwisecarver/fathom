defmodule Fathom.Shard.ReplicationCollapseTest do
  @moduledoc """
  THE SAME-SHARD COLLAPSE, reproduced in `mix test` (expert review 2026-10-20 #11).

  On the chaos rig (2026-10-19, `pool-fleet` 1x16 then 1x32 on the SAME shard) one shard went from
  ~1,700 ops/s to 64-170 ops/s with 288-1,822 `session_down` per run. The per-invocation namespace
  fix (cbbe3066) stopped the arm from reusing a shard and with it removed the only reproducer, so no
  fix to the commit path could be judged. This file is that reproducer, one mechanism per test, each
  driven deterministically (`Fathom.Test.PausablePeer` holds a follower's replies; a small
  `:replication_max_push_bytes` makes "far behind" cheap to reach):

    * #3 — ONE follower held (or being seeded) past the per-push cap made EVERY commit on the shard
      run its catch-up loop to the deadline and fail `{:no_quorum, :catching_up}`, although the
      other two followers (a quorum) had acked every byte of it in the first round.
    * #4 — each `ship_quorum` round waited a fresh `timeout()`, not the commit's remaining deadline,
      so the handler replied after the caller had given up: `{:session_down, :timeout}` instead of a
      clean `{:no_quorum, _}`, and the Session stayed busy past the caller.
    * #5(a) — commit calls whose callers had already timed out were still executed, each with a
      fresh deadline — the metastable queue that kept the shard collapsed for minutes.
    * #6 — a commit that missed its quorum waited for EVERY seed in flight, including ones it did not
      start, burning the whole deadline behind a seed it could not use.

  Each test asserts the INVARIANT the collapse violated (a quorum that holds the bytes acks promptly;
  a failure is reported inside the caller's timeout; an abandoned call costs nothing), and each was
  run against the unfixed `session.ex` and seen to fail — see the commit that added it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shards
  alias Fathom.Test.PausablePeer

  setup do
    id = "repl_collapse_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replcollapse_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
      :replication_max_push_bytes,
      :replication_timeout_ms,
      :replication_catchup_ms
    ]

    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"col_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  # A PausablePeer in front of follower `port`; returns `{peer, proxied_port}`.
  defp proxy!(port) do
    peer =
      start_supervised!({PausablePeer, upstream_port: port},
        id: {:peer, System.unique_integer([:positive])}
      )

    {peer, PausablePeer.port(peer)}
  end

  defp enable!(ports, opts) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, Keyword.get(opts, :timeout, 1_000))

    if cap = opts[:cap], do: Application.put_env(:fathom, :replication_max_push_bytes, cap)

    if Keyword.has_key?(opts, :catchup),
      do: Application.put_env(:fathom, :replication_catchup_ms, opts[:catchup])

    Application.put_env(
      :fathom,
      :replication_followers,
      for(p <- ports, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await_connected!(Keyword.get(opts, :connected, length(ports)))
  end

  # At least `n` shippers connected (a test that points one at a closed port expects fewer).
  defp await_connected!(n, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Enum.count(Fleet.shippers(), &Shipper.connected?/1) >= n,
        do: :connected,
        else: Process.sleep(20)
    end)
    |> Enum.find(fn
      :connected -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :connected -> :ok
      _ -> flunk("shippers never connected")
    end
  end

  defp open_shard!(id) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {coordinator, conn, path}
  end

  # Seeded as the CURRENT owner would have (real epoch + lineage), so the first push is an ordinary
  # append rather than an ownership-change re-seed request.
  defp preseed_current!(names, id, coordinator) do
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for name <- names, do: Follower.seed(name, id, epoch, 0, 0, 0, lineage)
  end

  defp session!(id) do
    [{pid, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)
    pid
  end

  defp timed_commit(id, wal, coordinator) do
    t0 = System.monotonic_time(:millisecond)
    reply = Session.commit(id, wal, coordinator)
    {reply, System.monotonic_time(:millisecond) - t0}
  end

  # ------------------------------------------------------------------------------------------
  # #4
  # ------------------------------------------------------------------------------------------

  # Two of three followers hold their replies, so the quorum cannot form. The handler's deadline is
  # `commit_deadline_budget()` (timeout - margin), precisely so it answers BEFORE the caller's
  # `GenServer.call` gives up (2026-08-31 #21) — but `ship_quorum` was handed a fresh `timeout()`,
  # so the round outlived the budget and the caller exited first.
  test "#4: a quorum that cannot form is reported inside the caller's timeout", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {c, pc}] = start_followers!(root, 3)
    {peer_b, pbx} = proxy!(pb)
    {peer_c, pcx} = proxy!(pc)
    enable!([pa, pbx, pcx], timeout: 600, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    preseed_current!([a, b, c], id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    :ok = PausablePeer.pause(peer_b)
    :ok = PausablePeer.pause(peer_c)
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])

    {reply, ms} = timed_commit(id, wal, coordinator)

    assert match?({:error, {:no_quorum, _}}, reply),
           "expected a clean no_quorum inside the caller's timeout, got #{inspect(reply)} " <>
             "after #{ms} ms — the quorum wait ignored the commit deadline"

    assert ms <= Session.commit_deadline_budget() + 50
  end

  # ------------------------------------------------------------------------------------------
  # #5(a)
  # ------------------------------------------------------------------------------------------

  # A stall queues callers; each gives up at `timeout()`, but its request stays in the Session's
  # mailbox. Executing it anyway (with a fresh deadline) is what turned one stall into minutes: the
  # queue drains at one caller-timeout per entry while new entries keep arriving.
  test "#5(a): a commit whose caller already gave up is not executed", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(for({_, p} <- followers, do: p), timeout: 300, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    preseed_current!(for({n, _} <- followers, do: n), id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    session = session!(id)
    before = :sys.get_state(session).commits
    :ok = :sys.suspend(session)

    callers =
      for _ <- 1..8, do: Task.async(fn -> Session.commit(id, wal, coordinator) end)

    replies = Task.await_many(callers, 5_000)
    assert Enum.all?(replies, &match?({:error, {:session_down, _}}, &1)), inspect(replies)

    :ok = :sys.resume(session)
    # `get_state` is answered after every queued call has been handled (mailbox order).
    executed = :sys.get_state(session).commits - before

    assert executed == 0,
           "#{executed} of 8 abandoned commit calls were executed after their callers gave up"

    # And the Session still serves a live caller.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)
  end
end
