defmodule Fathom.Shard.ReplicationCheckpointLaggardTest do
  @moduledoc """
  A Q-covered checkpoint must not restart the WAL out from under a CONNECTED follower that has not
  been sent everything yet (expert review 2026-10-20 #8).

  ## The bug

  The Session's threshold checkpoint (and the flush's post-snapshot one) asked only "does a write
  QUORUM hold the WAL?". With N followers and quorum Q that leaves up to N−Q followers whose bytes
  were never SENT — a push skipped `:already_in_flight` because the follower's shipper still held
  this shard's one waiter. The checkpoint then backfilled, the next write restarted the log, and the
  next reset told each such follower (`prev_extent`) that it was short: a full db+wal seed per
  laggard per restart. On a hot shard that is every ~2 s (the rig's pool-fleet collapse,
  2026-10-19; tpc-fleet 1024 logs ~52 "short reset" seed requests per run).

  ## The rule under test

  Q-covered but a connected laggard is still owed frames ⇒ DEFER one `checkpoint_step_bytes` and
  kick the laggard; past 2x the threshold fall back to Q-coverage (the 4x force cap still bounds
  the WAL). "Connected" = holds a position in the current WAL generation, so a dead follower drops
  out at the next restart instead of holding every WAL at 2x.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session, Wal}
  alias Fathom.Shards

  @frame 4096 + 24
  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_ordinal_wire,
    :replication_checkpoint_bytes,
    :replication_checkpoint_step_bytes,
    :replication_catchup_ms
  ]

  setup do
    id = "repl_ckpt_lag_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replckptlag_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    # Prod's wire, so a short follower can tell it is short (see replication_idle_close_test).
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    Application.put_env(:fathom, :replication_checkpoint_bytes, 40 * @frame)
    Application.put_env(:fathom, :replication_checkpoint_step_bytes, 8 * @frame)
    # The catch-up kick must not do the test's work for it while the laggard is held.
    Application.put_env(:fathom, :replication_catchup_ms, 60_000)

    on_exit(fn ->
      Session.stop(id)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"cl_f#{i}_#{System.unique_integer([:positive])}"
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

  defp listen(events) do
    test = self()
    handler = "ckpt-lag-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      events,
      fn e, m, meta, _ -> send(test, {:ev, e, m, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp events(name, acc \\ []) do
    receive do
      {:ev, ^name, m, meta} -> events(name, [{m, meta} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp write!(s, i) do
    {:ok, _} = Connection.query(s.conn, "INSERT INTO t VALUES (#{i}, randomblob(3000))", [])
    assert :ok = Session.commit(s.id, s.wal, s.coordinator)
  end

  defp extent(s) do
    {:ok, %{commit_extent: e}} = Wal.read(s.wal)
    e
  end

  defp salt(s) do
    {:ok, %{salt1: salt}} = Wal.read(s.wal)
    salt
  end

  test "a checkpoint at the threshold waits for a connected laggard; the restart then needs no seed",
       %{id: id, root: root} do
    followers = start_followers!(root, 3)
    enable!(followers)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    s = %{id: id, conn: conn, coordinator: coordinator, wal: path <> "-wal"}

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b BLOB)", [])
    write!(s, 0)

    listen([
      [:fathom, :replication, :seed],
      [:fathom, :replication, :checkpoint],
      [:fathom, :replication, :checkpoint_deferred]
    ])

    # Hold one follower's shipper: the next commit's push to it stays in flight, and every commit
    # after that SKIPS it (`:already_in_flight`) — bytes it is never sent while the quorum carries on.
    [_s1, _s2, s3] = Fleet.shippers()
    :ok = :sys.suspend(s3)
    salt0 = salt(s)

    # Cross the threshold (40 frames) but stay below 2x, so the laggard rule applies.
    # Paced like the catch-up loop below, so a checkpoint that DOES run here completes and the next
    # write restarts the log — the pre-fix outcome this must rule out.
    Enum.reduce_while(1..200, nil, fn i, _ ->
      write!(s, i)
      Process.sleep(10)
      # Or at the first restart (pre-fix): ONE generation under the laggard, so the Session still
      # remembers how far it got and the reset can tell the laggard it is short. (A follower that
      # misses SEVERAL generations is not even detected as short — see the report's lead.)
      if extent(s) >= 48 * @frame or salt(s) != salt0, do: {:halt, i}, else: {:cont, i}
    end)

    assert extent(s) < 80 * @frame, "precondition: still below 2x the threshold"

    assert events([:fathom, :replication, :checkpoint_deferred]) != [],
           "the threshold checkpoint did not defer for the laggard"

    assert events([:fathom, :replication, :checkpoint]) == [],
           "a checkpoint ran while a connected follower was owed frames"

    assert salt(s) == salt0, "the WAL restarted under the laggard"

    # Release the laggard: its held push lands, and the next commits ship it the rest.
    :ok = :sys.resume(s3)

    # Keep writing until the deferred checkpoint has run and a write has restarted the log. Paced:
    # the checkpoint is PINNED at its triggering commit and runs in a task, so back-to-back writes
    # keep it one commit short of a full backfill forever and the log never restarts.
    restarted =
      Enum.reduce_while(1_000..1_200, false, fn i, _ ->
        write!(s, i)
        Process.sleep(10)
        if salt(s) != salt0, do: {:halt, true}, else: {:cont, false}
      end)

    assert restarted, "the checkpoint never ran once the laggard caught up"
    assert [_ | _] = events([:fathom, :replication, :checkpoint])

    # The first reset after the restart has gone to every follower. None was short.
    write!(s, 2_000)
    Process.sleep(300)
    assert events([:fathom, :replication, :seed]) == []

    for {name, _} <- followers do
      assert %{torn: false} = Follower.state_of(name, id), "follower #{name} went torn"
    end
  end
end
