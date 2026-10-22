defmodule Fathom.Shard.ReplicationCatchupHolTest do
  @moduledoc """
  Expert review 2026-10-21 r7 #8 (panel 4 F2): a catch-up cycle must not hold the Session.

  SYMPTOM: when the catch-up timer found a follower short and free, `handle_info(:catch_up, ...)`
  ran a full multi-round cycle inline: its stopping rule was "every follower current", and between
  rounds `settle_inflight/3` waited for every outstanding push's reply, up to `catchup_ms` (1 s). A
  tenant commit arriving meanwhile sat in the mailbox until the cycle ended (loopback: catch-up
  cycle p50 44.6 ms, max 1,004.8 ms, against 3.4 ms tenant cycles). The common trigger is a
  follower restart, which re-ships each shard's whole WAL in 1 MiB rounds.

  INVARIANT pinned here: a catch-up cycle is ONE non-blocking round, so a tenant commit queued
  behind it completes in about one round whatever the laggard's ack latency; and the laggard still
  converges without any tenant traffic, round by round as its acks land (not one round per
  `catchup_ms`).
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
    :replication_catchup_ms,
    :replication_max_push_bytes
  ]

  setup do
    id = "repl_cuhol_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replcuhol_#{System.unique_integer([:positive])}")
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

  defp all_current?(session) do
    st = :sys.get_state(session)

    case st.seen do
      [{g, s, e} | _] ->
        st.inflight == %{} and map_size(st.followers) == 3 and
          Enum.all?(
            Map.values(st.followers),
            &match?(%{wal_gen: ^g, salt1: ^s, offset: o} when o >= e, &1)
          )

      _ ->
        false
    end
  end

  test "a tenant commit behind a catch-up of a slow, far-behind follower waits one round, and " <>
         "the follower still converges with no tenant traffic",
       %{id: id, root: root} do
    [{a, pa}, {b, pb}, {c, pc}] =
      for i <- 1..3 do
        name = :"cuhol_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    peer = start_supervised!({PausablePeer, upstream_port: pc}, id: :cuhol_peer)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, 2_000)
    # Long, so that neither the catch-up's own deadline nor its retry timer can explain a fast
    # tenant commit or a fast convergence below.
    Application.put_env(:fathom, :replication_catchup_ms, 5_000)
    # Makes "far behind" (more than one push) cheap to reach.
    Application.put_env(:fathom, :replication_max_push_bytes, 16_384)

    Application.put_env(
      :fathom,
      :replication_followers,
      for(p <- [pa, pb, PausablePeer.port(peer)], do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await!(fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end, "shippers connect")

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b BLOB)", [])
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for n <- [a, b, c], do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)
    assert :ok = Session.commit(id, wal, coordinator)
    [{session, _}] = Registry.lookup(@registry, id)
    await!(fn -> :sys.get_state(session).inflight == %{} end, "first commit settles")

    # C falls far behind: its replies are held, so after its first push every commit skips it.
    :ok = PausablePeer.pause(peer)

    for i <- 1..10 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?, randomblob(10000))", [i])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    # C answers its one push and is FREE but ~80 KB (5+ capped pushes) behind; then slow again.
    {:ok, _} = PausablePeer.release(peer)
    await!(fn -> :sys.get_state(session).inflight == %{} end, "C's held reply settles")
    :ok = PausablePeer.pause(peer)
    refute all_current?(session), "C is not behind; the test exercises nothing"

    # The catch-up fires (C is short and free: a cycle runs), and a tenant commit lands right
    # behind it in the mailbox.
    send(session, :catch_up)
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (99, randomblob(100))", [])
    t0 = System.monotonic_time(:millisecond)
    assert :ok = Session.commit(id, wal, coordinator)
    ms = System.monotonic_time(:millisecond) - t0

    assert ms < 500,
           "a tenant commit waited #{ms} ms behind a catch-up cycle (it settled on a slow " <>
             "follower between rounds instead of shipping one round)"

    assert PausablePeer.held(peer) >= 1, "the catch-up sent C nothing; the test exercised nothing"

    # C answers again. With no tenant traffic it must still be driven to current, at its ack pace:
    # one round per catch-up interval (5 s here) would need 25 s or more.
    {:ok, _} = PausablePeer.release(peer)
    await!(fn -> all_current?(session) end, "C converges with no tenant traffic", 3_000)
  end
end
