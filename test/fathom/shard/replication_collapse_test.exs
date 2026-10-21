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
    * #5(b) — every queued same-shard commit re-read the WAL to plan nothing (no group commit).
    * #9 — a WAL restart inside a push round (`checkpoint_during_push`, 13 in the rig run) failed
      the tenant's commit instead of re-planning.

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

  @small_cap 16_384

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

  defp holders(names, id, expected) do
    Enum.count(names, fn name -> File.read(Follower.wal_path(name, id)) == {:ok, expected} end)
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

  # A seed entry the Session believes is in flight, for a follower nothing is streaming to — the
  # state a large or wedged seed leaves behind. Installed through `:sys.replace_state/2` so the
  # monitor is the SESSION's (its in-call loops consume the DOWN of exactly these refs).
  defp fake_seed_in_flight!(session, shipper) do
    dummy = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(dummy, :kill) end)

    :sys.replace_state(session, fn state ->
      entry = %{
        ref: Process.monitor(dummy),
        token: make_ref(),
        pid: dummy,
        started_at: System.monotonic_time(:millisecond)
      }

      %{state | seeding: Map.put(state.seeding, shipper, entry)}
    end)
  end

  defp await_no_inflight!(session, timeout \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        :sys.get_state(session).inflight == %{} -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(10) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("the released follower's reply never settled")
  end

  defp await_queue!(pid, n, timeout \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        elem(Process.info(pid, :message_queue_len), 1) >= n -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(5) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("the callers never queued")
  end

  defp shipper_for(port) do
    Enum.find(Fleet.shippers(), fn s -> :sys.get_state(s).port == port end) ||
      flunk("no shipper for port #{port}")
  end

  # ------------------------------------------------------------------------------------------
  # #3
  # ------------------------------------------------------------------------------------------

  # The rig's dominant failure: with 16-32 writers the WAL grows MB/s, one follower (a non-quorum
  # laggard, here held by a PausablePeer) falls more than one per-push cap behind, and from then on
  # `complete?/2` — "every planned follower reached the CURRENT extent" — is false every round, so
  # every commit waited out its deadline for a follower the quorum did not need.
  test "#3: one follower held past the per-push cap does not fail every commit", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {c, pc}] = start_followers!(root, 3)
    {peer, pcx} = proxy!(pc)
    enable!([pa, pb, pcx], cap: @small_cap, timeout: 1_000, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    preseed_current!([a, b, c], id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    :ok = PausablePeer.pause(peer)
    blob = String.duplicate("x", 3_000)

    for i <- 1..8 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?, ?)", [i, blob])
      {reply, ms} = timed_commit(id, wal, coordinator)

      assert reply == :ok,
             "commit #{i} failed #{inspect(reply)} after #{ms} ms although two followers (a " <>
               "quorum) are live and only the third is held — the catch-up loop waited for a " <>
               "follower the quorum does not need"

      assert ms < 500, "commit #{i} took #{ms} ms: it waited on the held follower"

      assert holders([a, b], id, File.read!(wal)) == 2,
             "the commit acked before the quorum held its bytes"
    end

    # PRECONDITION: the held follower really is more than one cap behind, so the capped-plan path
    # the bug lived on was exercised (otherwise this passes both ways).
    gap = byte_size(File.read!(wal)) - Follower.state_of(c, id).next_offset

    assert gap > @small_cap,
           "the held follower is only #{gap} B behind — the test measured nothing"
  end

  # Same mechanism, reached through a seed in flight: a follower being seeded is planned (it counts
  # in the quorum's denominator) but not pushed to, and once the WAL outgrew the cap its capped
  # plan kept `complete?/2` false — the loop then SPUN (nothing left to send) until the deadline.
  test "#3: a follower being seeded does not fail a commit the other two acked", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {c, pc}] = start_followers!(root, 3)
    enable!([pa, pb, pc], cap: @small_cap, timeout: 1_000, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    preseed_current!([a, b, c], id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    fake_seed_in_flight!(session!(id), shipper_for(pc))

    blob = String.duplicate("y", 3_000)

    for i <- 1..12,
        do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?, ?)", [i, blob])

    assert byte_size(File.read!(wal)) > 3 * @small_cap, "the WAL never outgrew the cap"

    {reply, ms} = timed_commit(id, wal, coordinator)
    assert reply == :ok, "failed #{inspect(reply)} after #{ms} ms behind a seeding follower"
    assert ms < 500, "took #{ms} ms: the catch-up loop waited on the seeding follower"
    assert holders([a, b], id, File.read!(wal)) == 2
  end

  # THE SAFETY HALF of #3: stopping at the quorum must not become stopping SHORT of it. `b` is more
  # than a push behind, `c` is mid-seed, `a` is current: the commit's first round is acked by `a`
  # and `b` (a quorum of ACKS) while `b` still holds only a capped prefix. The commit may return
  # only once BOTH hold every byte of it — a rule that counted acks, or one follower too few, would
  # ack here with one replica of the write. (Checked: `quorum() - 1` in `quorum_holds?/2` fails it.)
  test "#3: a commit is acked only once a quorum holds ALL of its bytes", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {c, pc}] = start_followers!(root, 3)
    {peer_b, pbx} = proxy!(pb)
    enable!([pa, pbx, pc], cap: @small_cap, timeout: 1_000, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    preseed_current!([a, b, c], id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    # Build `b` up as a laggard while `a` and `c` carry the quorum.
    :ok = PausablePeer.pause(peer_b)
    blob = String.duplicate("z", 3_000)

    for i <- 1..8 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?, ?)", [i, blob])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    {:ok, _} = PausablePeer.release(peer_b)
    session = session!(id)
    await_no_inflight!(session)
    assert byte_size(File.read!(wal)) - Follower.state_of(b, id).next_offset > 2 * @small_cap

    # Now `c` cannot help, so the quorum is exactly {a, b}.
    fake_seed_in_flight!(session, shipper_for(pc))
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (99, 'tail')", [])
    assert :ok = Session.commit(id, wal, coordinator)

    assert holders([a, b], id, File.read!(wal)) == 2,
           "the commit was acked while `b` held only part of it: one replica, reported as a quorum"
  end

  # The other half: the commit no longer waits for the laggard, so something else must bring it
  # current — on a QUIET shard, with no further write. `arm_if_short/1` arms the catch-up.
  test "#3: a follower the commit did not wait for still converges with no further writes", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {c, pc}] = start_followers!(root, 3)
    {peer, pcx} = proxy!(pc)
    # No catch-up while the laggard is built up, so nothing is armed by the held phase's
    # `:already_in_flight` refusals; the property is exercised by ONE clean commit afterwards.
    enable!([pa, pb, pcx], cap: @small_cap, timeout: 1_000, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    preseed_current!([a, b, c], id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    :ok = PausablePeer.pause(peer)
    blob = String.duplicate("w", 3_000)

    for i <- 1..8 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?, ?)", [i, blob])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    target = File.read!(wal)
    {:ok, _} = PausablePeer.release(peer)
    session = session!(id)
    await_no_inflight!(session)

    # PRECONDITION: the laggard is more than one push behind and nothing is scheduled for it.
    assert byte_size(target) - Follower.state_of(c, id).next_offset > @small_cap
    assert :sys.get_state(session).catchup_ref == nil

    # One commit with nothing new: the other two plan nothing, the laggard gets ONE capped push
    # without a reply anyone waits for, and no follower refuses. Before #3 the commit itself looped
    # the laggard current; now `arm_if_short/1` must schedule the rest.
    Application.put_env(:fathom, :replication_catchup_ms, 200)
    assert :ok = Session.commit(id, wal, coordinator)

    deadline = System.monotonic_time(:millisecond) + 8_000

    converged =
      Stream.repeatedly(fn ->
        cond do
          File.read(Follower.wal_path(c, id)) == {:ok, target} -> true
          System.monotonic_time(:millisecond) > deadline -> false
          true -> Process.sleep(25) && nil
        end
      end)
      |> Enum.find(&is_boolean/1)

    assert converged,
           "the laggard never caught up on a quiet shard: the commits acked without it and " <>
             "nothing re-shipped to it (#{Follower.state_of(c, id).next_offset} of " <>
             "#{byte_size(target)} B)"
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

  # ------------------------------------------------------------------------------------------
  # #5(b)
  # ------------------------------------------------------------------------------------------

  # Group commit: N same-shard commits queued behind one cycle are answered by ONE ship (the first
  # cycle's header covers all their frames), not N cycles that each re-read the WAL to plan nothing.
  test "#5(b): queued commits for the same WAL are answered by one commit cycle", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    names = for {n, _} <- followers, do: n
    enable!(for({_, p} <- followers, do: p), timeout: 5_000, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    preseed_current!(names, id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    session = session!(id)
    before = :sys.get_state(session).commits
    :ok = :sys.suspend(session)

    # Eight streams each commit a row, then queue their replication call behind the stall.
    callers =
      for i <- 1..8 do
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?)", [i])
        Task.async(fn -> Session.commit(id, wal, coordinator) end)
      end

    await_queue!(session, 8)
    :ok = :sys.resume(session)

    assert Enum.all?(Task.await_many(callers, 10_000), &(&1 == :ok))
    cycles = :sys.get_state(session).commits - before
    assert cycles <= 2, "8 queued commits took #{cycles} commit cycles"

    assert holders(names, id, File.read!(wal)) >= 2,
           "the batch was acked before a quorum held every caller's frames"
  end

  # ------------------------------------------------------------------------------------------
  # #9
  # ------------------------------------------------------------------------------------------

  # The rig logged `checkpoint_during_push` 13 times in the collapse run: a WAL restart landed
  # between a round's header read and its re-check, and the stability check — correctly refusing
  # to ship bytes stamped with the old generation — then FAILED the tenant's commit, a write durable
  # locally. The round now re-plans. The restart here is the dangerous kind (it folds this very
  # commit's frames into the `.db` before any follower has them), so success must come through a
  # re-seed, and the followers must end up holding every row, not just the bytes after the restart.
  test "#9: a WAL restart inside a push round re-plans instead of failing the commit", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    names = for {n, _} <- followers, do: n
    enable!(for({_, p} <- followers, do: p), timeout: 3_000, catchup: 0)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    # REAL seeds (a `.db` on every follower), not `preseed_current!/3`: the rows check below reads
    # the replica's database, which a state-only preseed does not have.
    assert :ok = Session.commit(id, wal, coordinator)
    await_seeds_landed!(session!(id))

    for i <- 1..5, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?)", [i])
    assert :ok = Session.commit(id, wal, coordinator)
    for i <- 6..8, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?)", [i])

    # Fires ONCE, inside the Session, after the round read its payloads and before the re-check:
    # backfill everything (no reader holds a mark) and write, which restarts the log in place.
    fired = :counters.new(1, [])

    Application.put_env(:fathom, :replication_build_hook, fn ->
      if :counters.get(fired, 1) == 0 do
        :counters.add(fired, 1, 1)
        {:ok, c} = Connection.open(path)
        {:ok, _} = Connection.query(c, "PRAGMA wal_checkpoint(PASSIVE)", [])
        {:ok, _} = Connection.query(c, "INSERT INTO t VALUES (100)", [])
        :ok = Connection.close(c)
      end
    end)

    on_exit(fn -> Application.delete_env(:fathom, :replication_build_hook) end)
    {:ok, %{salt1: salt_before}} = Fathom.Shard.Replication.Wal.read(wal)

    reply = Session.commit(id, wal, coordinator)

    {:ok, %{salt1: salt_after, commit_extent: extent}} = Fathom.Shard.Replication.Wal.read(wal)
    assert :counters.get(fired, 1) == 1, "PRECONDITION: the restart hook never ran"
    assert salt_after != salt_before, "PRECONDITION: the hook did not restart the WAL"

    assert reply == :ok,
           "the commit failed #{inspect(reply)} because its WAL restarted mid-round; the write " <>
             "is durable locally and re-planning would have shipped it"

    # A quorum holds the new generation's committed bytes AND every row (the seed carried the
    # frames the restart folded into the `.db`).
    expected = binary_part(File.read!(wal), 0, extent)

    holding =
      Enum.filter(names, fn n -> File.read(Follower.wal_path(n, id)) == {:ok, expected} end)

    assert length(holding) >= 2, "only #{length(holding)} followers hold the new generation"

    for n <- holding do
      assert follower_rows(n, id) == 9,
             "#{n} holds the WAL bytes but not every row: the restart's folded frames were lost"
    end
  end

  # The UNLINK+RECREATE variant of #9 (expert review 2026-10-20-2027 #16). The pre-send recheck now
  # reads the held `-shm` wal-index instead of stat'ing the path, so a recreate between the round's
  # header read and its recheck is no longer seen by the recheck: the held fds still point at the
  # OLD, unlinked inodes. That is safe only because SQLite unlinks them in the last connection's
  # close AFTER checkpointing every frame, and nothing writes them again — so the round ships a
  # frozen generation's committed bytes under that generation's own header, never a splice, and the
  # next commit's stat sees the new inode and resets. This pins the outcome: the commit succeeds,
  # no replica goes torn, and after the next commit a quorum holds every row of both generations.
  test "#9 (unlink+recreate): a WAL recreated inside a push round never ships a splice", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    names = for {n, _} <- followers, do: n
    enable!(for({_, p} <- followers, do: p), timeout: 3_000, catchup: 0)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    wal = path <> "-wal"

    # The shard's only SQLite connection lives in its own process: `Connection` caches prepared
    # statements in the OWNER's dictionary, and a close from any other process is a deferred
    # `sqlite3_close_v2` that skips the last-close checkpoint and unlink — the very event this test
    # needs to land inside the Session's round.
    owner = sql_owner!(path)
    sql!(owner, "CREATE TABLE t (a INTEGER)")
    assert :ok = Session.commit(id, wal, coordinator)
    await_seeds_landed!(session!(id))

    for i <- 1..5, do: sql!(owner, "INSERT INTO t VALUES (#{i})")
    assert :ok = Session.commit(id, wal, coordinator)
    for i <- 6..8, do: sql!(owner, "INSERT INTO t VALUES (#{i})")

    inode_before = File.stat!(wal).inode
    fired = :counters.new(1, [])

    # Fires ONCE, inside the Session, after the round read its payloads and before the recheck:
    # close the shard's only connection (SQLite checkpoints and unlinks -wal and -shm), then reopen
    # and write, which creates a NEW -wal/-shm pair.
    Application.put_env(:fathom, :replication_build_hook, fn ->
      if :counters.get(fired, 1) == 0 do
        :counters.add(fired, 1, 1)
        :ok = call_owner(owner, :reopen)
        refute File.exists?(wal) and File.stat!(wal).inode == inode_before
        sql!(owner, "INSERT INTO t VALUES (100)")
      end
    end)

    on_exit(fn -> Application.delete_env(:fathom, :replication_build_hook) end)

    assert :ok = Session.commit(id, wal, coordinator)
    assert :counters.get(fired, 1) == 1, "PRECONDITION: the recreate hook never ran"

    assert File.stat!(wal).inode != inode_before,
           "PRECONDITION: the hook did not recreate the WAL"

    # The next commit stats the path, sees the new inode, and ships the new generation.
    sql!(owner, "INSERT INTO t VALUES (101)")
    assert :ok = Session.commit(id, wal, coordinator)

    for n <- names do
      refute match?(%{torn: true}, Follower.state_of(n, id)), "#{n} went torn across the recreate"
    end

    {:ok, %{commit_extent: extent}} = Fathom.Shard.Replication.Wal.read(wal)
    expected = binary_part(File.read!(wal), 0, extent)

    holding =
      Enum.filter(names, fn n -> File.read(Follower.wal_path(n, id)) == {:ok, expected} end)

    assert length(holding) >= 2, "only #{length(holding)} followers hold the new generation"

    for n <- holding do
      assert follower_rows(n, id) == 10,
             "#{n} holds the new WAL but not every row of both generations"
    end
  end

  defp sql_owner!(path) do
    test = self()

    pid =
      spawn_link(fn ->
        {:ok, conn} = Connection.open(path)
        send(test, {:owner_ready, self()})
        sql_owner_loop(path, conn)
      end)

    assert_receive {:owner_ready, ^pid}, 5_000
    on_exit(fn -> if Process.alive?(pid), do: call_owner(pid, :stop) end)
    pid
  end

  defp sql_owner_loop(path, conn) do
    receive do
      {from, ref, {:sql, sql}} ->
        send(from, {ref, Connection.query(conn, sql, [])})
        sql_owner_loop(path, conn)

      {from, ref, :reopen} ->
        :ok = Connection.close(conn)
        {:ok, conn} = Connection.open(path)
        send(from, {ref, :ok})
        sql_owner_loop(path, conn)

      {from, ref, :stop} ->
        Connection.close(conn)
        send(from, {ref, :ok})
    end
  end

  defp call_owner(pid, msg) do
    ref = make_ref()
    send(pid, {self(), ref, msg})

    receive do
      {^ref, reply} -> reply
    after
      5_000 -> flunk("the SQL owner did not answer #{inspect(msg)}")
    end
  end

  defp sql!(owner, sql), do: {:ok, _} = call_owner(owner, {:sql, sql})

  defp await_seeds_landed!(session, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        :sys.get_state(session).seeding == %{} -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(10) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("the initial seeds never landed")
  end

  # Count the rows a follower's replica holds, on a COPY (opening the replica itself could
  # checkpoint it under the follower).
  defp follower_rows(name, id) do
    src = String.replace_suffix(Follower.wal_path(name, id), "-wal", "")
    dst = Path.join(System.tmp_dir!(), "rows_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s) end)
    File.cp!(src, dst)
    File.cp!(src <> "-wal", dst <> "-wal")
    {:ok, c} = Connection.open(dst)
    {:ok, %{rows: [[n]]}} = Connection.query(c, "SELECT count(*) FROM t", [])
    :ok = Connection.close(c)
    n
  end

  # ------------------------------------------------------------------------------------------
  # #6
  # ------------------------------------------------------------------------------------------

  # One follower is down (closed port), one is mid-seed (a seed NOT started by this commit), one is
  # live: the quorum is impossible. `await_seeds` used to wait for every seed in flight, so the
  # commit sat out its whole deadline behind a seed it could not use.
  test "#6: a commit that misses quorum does not wait for a seed it did not start", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {c, pc}] = start_followers!(root, 2)
    # The middle follower is a closed port: every push to it is `:disconnected` at once.
    enable!([pa, 1, pc], timeout: 1_000, catchup: 0, connected: 2)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    preseed_current!([a, c], id, coordinator)
    assert :ok = Session.commit(id, wal, coordinator)

    fake_seed_in_flight!(session!(id), shipper_for(pc))
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])

    {reply, ms} = timed_commit(id, wal, coordinator)
    assert match?({:error, {:no_quorum, _}}, reply), inspect(reply)

    assert ms < 300,
           "the failing commit took #{ms} ms: it waited on a seed some earlier commit started"
  end

  # The second half of #6: a seed THIS commit started is waited on (that is what makes a tenant's
  # first write succeed), but not when the Session's observed seed rate says it cannot land inside
  # the time left — the rig's 16.5 MB seeds ran 145 s+, so the wait could only burn the deadline.
  test "#6: a seed this commit started is not awaited when it cannot land in time", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {_c, pc}] = start_followers!(root, 2)
    enable!([pa, 1, pc], timeout: 1_000, catchup: 0, connected: 2)

    # Hold the node's only seed slot, so the seed this commit starts cannot make progress.
    prev_cap = Application.get_env(:fathom, :replication_seed_max_concurrency)
    Application.put_env(:fathom, :replication_seed_max_concurrency, 1)

    on_exit(fn ->
      if is_nil(prev_cap),
        do: Application.delete_env(:fathom, :replication_seed_max_concurrency),
        else: Application.put_env(:fathom, :replication_seed_max_concurrency, prev_cap)
    end)

    test_pid = self()

    holder =
      spawn(fn ->
        :ok = Fathom.Shard.Replication.SeedGate.acquire(1_000)
        send(test_pid, :slot_held)
        Process.sleep(:infinity)
      end)

    on_exit(fn -> Process.exit(holder, :kill) end)
    assert_receive :slot_held, 2_000

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    # Only `a` is seeded: `c` answers :unknown_shard and this commit starts its seed.
    preseed_current!([a], id, coordinator)

    # The Session exists after a first (failing) commit; give it an observed rate so slow that no
    # seed can land in a second. Then the commit under test starts a fresh seed for `c`.
    _ = Session.commit(id, wal, coordinator)
    session = session!(id)
    :sys.replace_state(session, &%{&1 | seed_rate: 0.001, seeding: %{}})

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    {reply, ms} = timed_commit(id, wal, coordinator)
    assert match?({:error, {:no_quorum, _}}, reply), inspect(reply)

    assert map_size(:sys.get_state(session).seeding) == 1,
           "PRECONDITION: the commit under test should have started a seed for the unseeded follower"

    assert ms < 300, "the commit waited #{ms} ms for a seed it could not finish in time"
  end
end
