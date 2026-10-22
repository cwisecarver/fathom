defmodule Fathom.Shard.ReplicationCatchupBackoffTest do
  @moduledoc """
  Expert review 2026-10-21 r7 #9 (panel 4 F4). SYMPTOM: on a shard committing faster than its
  stragglers' round trip (N > Q), every commit skips the N-Q followers whose previous push is still
  in flight and reports them `{s, :already_in_flight, 0}`. `arm_if_rejected/2` counted that
  skip-only list as a catch-up FAILURE, so `catchup_fails` climbed by one per healthy commit
  (measured 16 in 300 commits with two followers 3 ms slower, 95 with capped pushes). At depth >= 6
  `retry_delay_ms/2` is already its cap, `shard_flush_interval_ms` (30 s in prod), so a follower that
  was really refused after such a burst waited up to the flush interval for its retry instead of
  `catchup_ms` (1 s), and a follower still short at the covered idle close could be forced into a
  full seed.

  INVARIANT pinned here: a round whose only rejects are `:already_in_flight` skips is not a failure
  and leaves the backoff depth where it was; a real refusal still counts, exactly once.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session, Shipper}
  alias Fathom.Shards
  alias Fathom.Test.PausablePeer

  @registry Fathom.Shard.Replication.SessionRegistry
  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_timeout_ms,
    :replication_catchup_ms
  ]

  setup do
    id = "repl_cubo_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replcubo_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

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

  defp await!(fun, what, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        fun.() -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(10) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("never: #{what}")
  end

  # N=4, Q=2: three direct followers and a fourth behind a PausablePeer, so the fourth can be made
  # a straggler (it receives and applies every push; only its reply is late).
  defp start!(%{id: id, root: root}) do
    followers =
      for i <- 1..4 do
        name = :"cubo_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    [{_, p1}, {_, p2}, {_, p3}, {_, p4}] = followers
    peer = start_supervised!({PausablePeer, upstream_port: p4}, id: :cubo_peer)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    # Long enough that the held push stays "in flight" (skipped, not re-pushed) for the whole test.
    Application.put_env(:fathom, :replication_timeout_ms, 30_000)
    # Long enough that the straggler never ages into real lag (`lagging?/1`) and no timer fires
    # mid-test: the only thing that can move `catchup_fails` is `arm_if_rejected/2`.
    Application.put_env(:fathom, :replication_catchup_ms, 60_000)

    Application.put_env(
      :fathom,
      :replication_followers,
      for(p <- [p1, p2, p3, PausablePeer.port(peer)], do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await!(fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end, "shippers connect")

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for {n, _} <- followers, do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)
    wal = path <> "-wal"
    assert :ok = Session.commit(id, wal, coordinator)
    [{session, _}] = Registry.lookup(@registry, id)
    await!(fn -> :sys.get_state(session).inflight == %{} end, "the first commit settles")
    %{peer: peer, conn: conn, wal: wal, coordinator: coordinator, session: session}
  end

  defp commit!(ctx, i) do
    {:ok, _} = Connection.query(ctx.conn, "INSERT INTO t VALUES (?1)", [i])
    assert :ok = Session.commit(ctx.id, ctx.wal, ctx.coordinator)
  end

  test "skip-only :already_in_flight rounds leave catchup_fails alone; a real refusal counts once",
       ctx do
    ctx = Map.merge(ctx, start!(ctx))
    assert Session.lag_info(ctx.id).catchup_fails == 0

    # The straggler: its first push is applied but the reply is held, so every later commit skips it.
    :ok = PausablePeer.pause(ctx.peer)
    for i <- 1..20, do: commit!(ctx, i)

    # Not a vacuous pass: the paused follower's reply is really held (so its push stayed in flight
    # and commits 2..20 each skipped it `:already_in_flight`).
    assert PausablePeer.held(ctx.peer) >= 1, "the straggler's reply was not held"
    assert :sys.get_state(ctx.session).inflight != %{}, "no follower was left in flight"

    assert Session.lag_info(ctx.id).catchup_fails == 0,
           "#{Session.lag_info(ctx.id).catchup_fails} healthy commits that only skipped an " <>
             "in-flight follower were counted as catch-up failures"

    # A REAL refusal (here a node-budget `:overloaded`, as our own shipper would report it) still
    # backs off: it counts, once per refusal.
    [shipper | _] = Fleet.shippers()
    send(ctx.session, {:repl_reply, shipper, {:reject, ctx.id, :overloaded, 0}})
    send(ctx.session, {:repl_reply, shipper, {:reject, ctx.id, :overloaded, 0}})
    _ = :sys.get_state(ctx.session)
    assert Session.lag_info(ctx.id).catchup_fails == 2

    {:ok, _} = PausablePeer.release(ctx.peer)
  end
end
