defmodule Fathom.Shard.ReplicationCatchupGaugeTest do
  @moduledoc """
  Expert review 2026-10-21 #25 (P4 F7). SYMPTOM: with N > Q every commit replies at the Q-th ack,
  so the other N-Q followers still have a push in flight at reply time, `arm_if_short/1` counted
  them as short, and `arm_catchup/2` marked the Session in `LagGauge` and emitted
  `[:fathom, :replication, :catchup]` — on nearly every commit of every healthy shard. The
  `catching_up_shards` gauge and the `catchup.fails` metric then read "every active shard is
  lagging", which hides a real laggard.

  INVARIANT pinned here: a follower whose only shortfall is a FRESH in-flight push (younger than
  one catch-up interval) is not lag. The safety timer is still armed for it — a lost reply must
  still age into a retry — but the Session is neither marked nor counted. A follower that stays
  behind for a catch-up interval (its push held, or nothing in flight) IS marked and counted, and
  stays marked until every follower is current again.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, LagGauge, Session, Shipper}
  alias Fathom.Shards
  alias Fathom.Test.PausablePeer

  @registry Fathom.Shard.Replication.SessionRegistry

  setup do
    id = "repl_cugauge_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replcugauge_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
      :replication_timeout_ms,
      :replication_catchup_ms
    ]

    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}
    me = self()
    hid = "cugauge-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      hid,
      [:fathom, :replication, :catchup],
      fn _, m, _, _ -> send(me, {:catchup_event, m}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(hid)
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

  # N=4, Q=2: three direct followers and a fourth behind a PausablePeer.
  defp start!(%{id: id, root: root}, catchup_ms) do
    followers =
      for i <- 1..4 do
        name = :"cug_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    [{_, p1}, {_, p2}, {_, p3}, {_, p4}] = followers
    peer = start_supervised!({PausablePeer, upstream_port: p4}, id: :cug_peer)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, 2_000)
    Application.put_env(:fathom, :replication_catchup_ms, catchup_ms)

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
    await!(fn -> all_current?(session) end, "every follower current after the first commit")
    flush_events()
    %{peer: peer, conn: conn, wal: wal, coordinator: coordinator, session: session}
  end

  defp marked?(session), do: :ets.member(LagGauge, session)

  defp all_current?(session) do
    st = :sys.get_state(session)

    case st.seen do
      [{g, s, e} | _] ->
        st.inflight == %{} and map_size(st.followers) == 4 and
          Enum.all?(
            Map.values(st.followers),
            &match?(%{wal_gen: ^g, salt1: ^s, offset: o} when o >= e, &1)
          )

      _ ->
        false
    end
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

  defp flush_events(acc \\ 0) do
    receive do
      {:catchup_event, _} -> flush_events(acc + 1)
    after
      0 -> acc
    end
  end

  defp commit!(ctx, i) do
    {:ok, _} = Connection.query(ctx.conn, "INSERT INTO t VALUES (?1)", [i])
    assert :ok = Session.commit(ctx.id, ctx.wal, ctx.coordinator)
  end

  test "N=4/Q=2, every follower healthy: 100 commits mark nothing and emit no catchup event",
       ctx do
    # The prod default interval, so a slow ack on a loaded host is not mistaken for lag.
    ctx = Map.merge(ctx, start!(ctx, 1_000))

    # Sampled right after each reply, when the two non-quorum pushes are still in flight — the
    # moment the unfixed `arm_if_short/1` marked the Session.
    samples =
      for i <- 1..100 do
        commit!(ctx, i)
        {marked?(ctx.session), :sys.get_state(ctx.session).catchup_ref != nil}
      end

    marked = Enum.count(samples, &elem(&1, 0))
    armed = Enum.count(samples, &elem(&1, 1))

    await!(fn -> all_current?(ctx.session) end, "every follower current after the stream")
    # Let any timer armed by the last commit fire and clear.
    await!(fn -> :sys.get_state(ctx.session).catchup_ref == nil end, "the safety timer cleared")

    events = flush_events()

    assert {marked, events} == {0, 0},
           "a healthy shard was marked catching-up after #{marked}/100 commits and emitted " <>
             "#{events} :catchup event(s)"

    refute marked?(ctx.session)

    # Not a vacuous pass: the scenario must actually leave stragglers in flight at reply. The
    # safety timer is still armed for them (a lost reply must age into a retry); it is just not lag.
    assert armed > 0, "no commit left a straggler in flight at reply; the test exercised nothing"
  end

  test "a follower held past one catch-up interval is marked and counted, then cleared", ctx do
    ctx = Map.merge(ctx, start!(ctx, 200))

    :ok = PausablePeer.pause(ctx.peer)
    commit!(ctx, 1)
    assert :sys.get_state(ctx.session).catchup_ref != nil, "the safety timer was not armed"
    refute marked?(ctx.session), "a fresh in-flight push was counted as lag at reply time"

    # The held push outlives a catch-up interval: now it is lag.
    await!(fn -> marked?(ctx.session) end, "the held follower was never marked", 3_000)
    assert flush_events() >= 1, "no :catchup event for a real laggard"

    # More commits while it is held do not unmark it (no flicker on a real laggard).
    for i <- 2..5 do
      commit!(ctx, i)
      assert marked?(ctx.session), "a commit unmarked a Session whose follower is still held"
    end

    {:ok, _} = PausablePeer.release(ctx.peer)
    await!(fn -> all_current?(ctx.session) end, "the released follower caught up", 10_000)
    await!(fn -> not marked?(ctx.session) end, "the mark cleared once every follower was current")
  end
end
