defmodule Fathom.Shard.ReplicationIdleCloseVerdictTest do
  @moduledoc """
  Expert review 2026-10-20-2027 #15: the covered-idle-close ask must not hold the Session.

  SYMPTOM: the coordinator asks the Session `{:idle_close?, ...}` ~250 ms after a shard's last
  checkin. With a connected follower owed frames, the Session answered by running a full catch-up
  cycle INLINE — up to `catchup_ms` (1 s) of rounds with `settle_inflight/3` waiting on that
  follower's acks between them. A bursty tenant (Django CONN_MAX_AGE=0) opening its next connection
  and committing right then queued behind the whole thing.

  INVARIANT pinned here: the verdict is answered within one non-blocking round whatever the
  followers' ack latency (`:wait`, with the catch-up armed, when a follower is still owed frames),
  and repeated asks still converge to `:ready` once the follower answers — so the covered close
  keeps working, it just stops blocking.

  Run against the unfixed `session.ex` and seen to fail (the verdict took ~`catchup_ms`).
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
    id = "repl_icv_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replicv_#{System.unique_integer([:positive])}")
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

  defp ask(session) do
    token = make_ref()
    t0 = System.monotonic_time(:millisecond)
    send(session, {:idle_close?, self(), token})
    assert_receive {:idle_close_verdict, ^token, verdict}, 5_000
    {verdict, System.monotonic_time(:millisecond) - t0}
  end

  test "an idle-close ask with a far-behind, slow follower answers at once and still converges",
       %{id: id, root: root} do
    [{a, pa}, {b, pb}, {c, pc}] =
      for i <- 1..3 do
        name = :"icv_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    peer = start_supervised!({PausablePeer, upstream_port: pc}, id: :icv_peer)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, 2_000)
    Application.put_env(:fathom, :replication_catchup_ms, 1_000)
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

    {verdict, ms} = ask(session)
    assert verdict == :wait
    assert ms < 300, "the idle-close verdict held the Session #{ms} ms (catch-up ran inline)"
    assert :sys.get_state(session).catchup_ref != nil, "a still-owed follower was not kicked"

    # The covered close still converges: with C answering again, repeated asks reach :ready.
    {:ok, _} = PausablePeer.release(peer)

    ready? =
      Enum.reduce_while(1..40, false, fn _, _ ->
        case ask(session) do
          {:ready, _} ->
            {:halt, true}

          _ ->
            Process.sleep(50)
            {:cont, false}
        end
      end)

    assert ready?, "repeated idle-close asks never reached :ready"
  end
end
