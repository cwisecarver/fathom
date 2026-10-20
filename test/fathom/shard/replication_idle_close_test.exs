defmodule Fathom.Shard.ReplicationIdleCloseTest do
  @moduledoc """
  The pool drain grace's close-time checkpoint must not fold a WAL that a CONNECTED follower has
  not been sent yet (expert review 2026-10-20 #7).

  ## The bug

  When a shard's last stream checks in, the coordinator keeps its pooled handle for `drain_grace_ms`
  (250 ms in prod) and then closes it. That close is SQLite's last-connection checkpoint: every
  frame is backfilled into the `.db` and `-wal` is unlinked, so the next write starts a NEW WAL.
  The replication `Session` survives the gap (it lives as long as the coordinator) and documented
  that close as deliberately uncovered.

  A follower whose push was SKIPPED (`:already_in_flight` — its shipper still held this shard's one
  waiter) is caught up by the Session's catch-up retry, which fires `replication_catchup_ms`
  (1 s) after the refusal. The drain fires after 250 ms. So on any shard that goes quiet right
  after a burst, the WAL holding the frames the laggard was never sent is folded away first; the
  retry then reads a WAL with nothing to ship, and the next write's reset tells the laggard
  (`prev_extent`) that it is short. It answers with a seed request — a full db+wal re-seed — for
  bytes it would have received a few hundred ms later. On the rig (2026-10-19, pool-fleet reuse)
  this is how the collapse starts: every idle gap and every later restart ends in a seed.

  ## The fix under test

  The grace's drain is now a COVERED close: before closing, the coordinator asks the Session
  (asynchronously — the Session calls the coordinator itself, so a synchronous ask could deadlock),
  which reads the WAL header, ships to any follower that has not been sent everything (re-using its
  catch-up path), and answers `:ready` only when nothing a connected follower lacks would be folded.
  The coordinator re-asks each grace period and, after `:shard_idle_close_attempts`, drains anyway
  (today's behaviour, now with every frame READ by the Session, so a follower it could not reach is
  marked short and re-seeded, never laundered).

  Why not the finding's option (a), "stop the Session on idle drain": a NEW Session has no record
  of any follower, so its first reset states `prev_extent: 0` — "no evidence of a gap" — and the
  laggard ABSORBS its incomplete WAL as complete (`Follower.complete_through_reset?/2`). That is
  the laundering the `prev_extent` field exists to prevent, not a fix for it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shard.Replication.{Fleet, Follower, Session}
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_catchup_ms,
    :connection_pool,
    :connection_pool_opts,
    :shard_idle_ms,
    :shard_idle_close_attempts,
    :replication_ordinal_wire
  ]

  setup do
    id = "repl_idle_close_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replidle_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    Application.put_env(:fathom, :connection_pool, true)
    Application.put_env(:fathom, :connection_pool_opts, drain_grace_ms: 100)

    # Never idle-stop inside the test: the bug needs the coordinator (and Session) to SURVIVE the gap.
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    Application.put_env(:fathom, :replication_catchup_ms, 1_000)

    # PROD'S WIRE. Without the ordinal frame (off in dev/test) a push carries no `prev_extent`, so a
    # short follower cannot tell it is short and silently absorbs the incomplete WAL — no seed to
    # count. (Here the next generation's full-page images happen to re-carry the missing row, which
    # is why the row check below is a sanity check, not the discriminating assertion.)
    Application.put_env(:fathom, :replication_ordinal_wire, true)

    on_exit(fn ->
      _ = Shards.stop(id)
      Session.stop(id)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"ic_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp enable!(followers) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid

  defp open!(id) do
    {:ok, h} = ShardExecutor.open(id)
    h
  end

  defp exec!(h, sql), do: {:ok, _} = ShardExecutor.execute(h, stmt(sql))

  defp count_seeds do
    test = self()
    handler = "idle-close-seeds-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :replication, :seed],
      fn _e, m, meta, _ -> send(test, {:seed, m, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp seeds_received(acc \\ []) do
    receive do
      {:seed, m, meta} -> seeds_received([{m, meta} | acc])
    after
      0 -> acc
    end
  end

  defp eventually(fun, tries \\ 300) do
    cond do
      fun.() -> true
      tries == 0 -> false
      true -> Process.sleep(10) && eventually(fun, tries - 1)
    end
  end

  defp wal(id), do: Shard.db_path(id) <> "-wal"

  # A follower's replica read the way a promotion would: its `.db` plus its WAL, copied aside first
  # so opening them cannot run recovery or a checkpoint on the follower's own files.
  defp replica_rows(name, id, root) do
    dir = Path.join(root, "probe_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    db = Path.join(dir, "r.db")

    for {src, dst} <- [
          {Follower.db_path(name, id), db},
          {Follower.wal_path(name, id), db <> "-wal"}
        ] do
      case File.read(src) do
        {:ok, bin} -> File.write!(dst, bin)
        _ -> :ok
      end
    end

    {:ok, conn} = Fathom.Shard.Connection.open(db)

    try do
      case Fathom.Shard.Connection.query(conn, "SELECT a FROM t ORDER BY a", []) do
        {:ok, %{rows: rows}} -> List.flatten(rows)
        {:ok, rows} when is_list(rows) -> List.flatten(rows)
        other -> other
      end
    after
      Fathom.Shard.Connection.close(conn)
    end
  end

  test "a laggard skipped just before an idle gap is caught up, not re-seeded, after the gap", %{
    id: id,
    root: root
  } do
    followers = start_followers!(root, 3)
    enable!(followers)

    # Burst 1: every follower seeded and current.
    h = open!(id)
    exec!(h, "CREATE TABLE t (a INTEGER, b BLOB)")
    for i <- 1..3, do: exec!(h, "INSERT INTO t VALUES (#{i}, randomblob(2000))")
    coordinator = pid_of(h)

    # Make one follower a LAGGARD the way the rig does: its shipper holds this shard's waiter (here,
    # suspended), so the next commit pushes to it and the one after SKIPS it (`:already_in_flight`).
    [_s1, _s2, s3] = Fleet.shippers()
    :ok = :sys.suspend(s3)
    exec!(h, "INSERT INTO t VALUES (10, randomblob(2000))")
    exec!(h, "INSERT INTO t VALUES (11, randomblob(2000))")
    :ok = :sys.resume(s3)

    count_seeds()

    # The burst ends: the stream checks in and the shard goes idle inside the drain grace (100 ms),
    # well before the Session's 1 s catch-up retry for the skipped follower.
    :ok = ShardExecutor.close(h)

    # The grace's close ran: the pool is empty and the close-time checkpoint unlinked the WAL.
    assert eventually(fn ->
             st = :sys.get_state(coordinator)
             # `closing` keeps the finished tail's pid until a flush reaps it.
             Shard.HandlePool.count(st.pool) == 0 and
               (st.closing == nil or not Process.alive?(st.closing))
           end),
           "the idle drain never closed the pooled handle"

    refute File.exists?(wal(id)), "precondition: the idle close checkpointed and unlinked the WAL"

    # Let the 1 s catch-up retry run (it is what made the laggard good before the gap existed).
    Process.sleep(1_300)

    # Burst 2 on the SAME coordinator and Session.
    h2 = open!(id)
    assert pid_of(h2) == coordinator, "precondition: the coordinator survived the gap"
    for i <- 20..22, do: exec!(h2, "INSERT INTO t VALUES (#{i}, randomblob(2000))")
    :ok = ShardExecutor.close(h2)

    # No follower may have needed a re-seed: the laggard was owed only frames the Session could
    # still have sent it. Pre-fix, the laggard's first post-gap reset carried a `prev_extent` it was
    # short of, so it asked for (and got) a full db+wal seed.
    # A seed runs in its own task after the commit that asked for it returns; give it time to land
    # (it takes milliseconds) so the assertion cannot pass by racing it.
    Process.sleep(500)
    _ = Session.lag_info(id)
    assert seeds_received() == []

    for {name, _} <- followers do
      assert %{torn: false} = Follower.state_of(name, id),
             "follower #{name} went torn across the idle gap"
    end

    # Sanity: every replica holds every acked row, including the laggard's.
    expected = [1, 2, 3, 10, 11, 20, 21, 22]

    assert eventually(fn ->
             Enum.all?(followers, fn {name, _} -> replica_rows(name, id, root) == expected end)
           end),
           "a replica is missing acked rows: " <>
             inspect(for {n, _} <- followers, do: {n, replica_rows(n, id, root)})
  end
end
