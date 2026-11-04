defmodule Fathom.Shard.ReplicationSeedQuorumReleaseTest do
  @moduledoc """
  ONE SLOW SEED MUST NOT FAIL A COMMIT A QUORUM ALREADY HOLDS (expert review 2026-11-04 #5).

  A reopened shard's first commit seeds every follower (`seed_first/4`, or the seed-after-refusal
  path), then `await_seeds/5` waits for those seeds before re-shipping. It used to wait for EVERY
  seed it started, and `hopeless_seed?` gave up when ANY one was predicted to miss the deadline, so
  one slow follower link turned a commit that two of three followers (a quorum) had already
  received into `{:no_quorum, :impossible}` — an error on a tenant's first write after every wake.

  The fixture: three real followers, quorum 2, the third behind a `PausablePeer` whose replies are
  held. The third follower RECEIVES its seed (the proxy forwards primary -> follower) but its answer
  never reaches the Session until the test releases it, which is a seed that is slow past the
  commit deadline — deterministic, no timing race.

  What is pinned:

    * the commit acks once two seeds have landed (latency 2nd-of-3, not max-of-3);
    * at ack time the two seeded followers hold every row of it (RPO: the release never acks on
      fewer than Q durable followers — the ack is still `quorum_holds?/2` + `ack_if_owned/1`);
    * the third seed keeps its bookkeeping and lands later through `handle_info/2`, after which the
      follower catches up with the next commit;
    * with only ONE fast follower the commit still fails: one seed is not a quorum.

  The first test was run against the unfixed `session.ex` (all-of-N wait) and seen to fail:
  `{:no_quorum, :impossible}` after 1,402 ms. The second passes either way BY DESIGN: it is not a
  regression test but the guard that the release did not weaken the ack — it fails if the release
  path ever acks on one seeded follower.
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
    id = "repl_seedrel_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replseedrel_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
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
      name = :"seedrel_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp proxy!(port) do
    peer =
      start_supervised!({PausablePeer, upstream_port: port},
        id: {:peer, System.unique_integer([:positive])}
      )

    {peer, PausablePeer.port(peer)}
  end

  defp enable!(ports, timeout) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, timeout)
    Application.put_env(:fathom, :replication_catchup_ms, 0)

    Application.put_env(
      :fathom,
      :replication_followers,
      for(p <- ports, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)

    await!(
      fn -> Enum.count(Fleet.shippers(), &Shipper.connected?/1) >= length(ports) end,
      "shippers never connected"
    )
  end

  defp await!(fun, msg, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        fun.() -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(10) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert(msg)
  end

  defp open_shard!(id) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {coordinator, conn, path}
  end

  defp session!(id) do
    [{pid, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)
    pid
  end

  defp shipper_for(port) do
    Enum.find(Fleet.shippers(), fn s -> :sys.get_state(s).port == port end) ||
      flunk("no shipper for port #{port}")
  end

  defp timed_commit(id, wal, coordinator) do
    t0 = System.monotonic_time(:millisecond)
    reply = Session.commit(id, wal, coordinator)
    {reply, System.monotonic_time(:millisecond) - t0}
  end

  # Rows in a follower's replica, read from a COPY (opening the replica itself could checkpoint it
  # under the follower).
  defp follower_rows(name, id) do
    src = String.replace_suffix(Follower.wal_path(name, id), "-wal", "")
    dst = Path.join(System.tmp_dir!(), "seedrel_rows_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s) end)
    File.cp!(src, dst)
    if File.exists?(src <> "-wal"), do: File.cp!(src <> "-wal", dst <> "-wal")
    {:ok, c} = Connection.open(dst)
    {:ok, %{rows: [[n]]}} = Connection.query(c, "SELECT count(*) FROM t", [])
    :ok = Connection.close(c)
    n
  end

  # The first commit of a fresh shard seeds every connected follower (`seed_first/4`). `c`'s
  # replies are held, so its seed cannot complete inside the commit's deadline.
  test "two seeds landed, third pending: the commit acks on the two, holding every row", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {c, pc}] = start_followers!(root, 3)
    {peer, pcx} = proxy!(pc)
    enable!([pa, pb, pcx], 1_500)
    slow = shipper_for(pcx)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    for i <- 1..5, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?)", [i])

    :ok = PausablePeer.pause(peer)
    {reply, ms} = timed_commit(id, wal, coordinator)

    session = session!(id)
    state = :sys.get_state(session)

    # PRECONDITION: the slow follower's seed really was started by this commit and is still in
    # flight — otherwise the wait this test is about never happened.
    assert Map.has_key?(state.seeding, slow),
           "PRECONDITION: no seed in flight for the held follower: #{inspect(Map.keys(state.seeding))}"

    assert reply == :ok,
           "the first commit failed #{inspect(reply)} after #{ms} ms although two of three " <>
             "followers were seeded: it waited for the held third seed"

    assert ms < 1_000, "the commit took #{ms} ms: it waited on the held seed"

    # RPO: at ack time a quorum of followers OTHER than the held one hold every row.
    for n <- [a, b] do
      assert follower_rows(n, id) == 5,
             "#{n} does not hold every acked row: the commit acked on fewer than Q followers"
    end

    # The third seed keeps its bookkeeping: released, it lands through `handle_info/2`, and the next
    # commit carries the follower forward with everyone else.
    {:ok, _} = PausablePeer.release(peer)
    await!(fn -> :sys.get_state(session).seeding == %{} end, "the held seed never landed")
    assert Map.has_key?(:sys.get_state(session).followers, slow), "the late seed was not recorded"

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (6)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    await!(
      fn -> File.read(Follower.wal_path(c, id)) == File.read(wal) end,
      "the late-seeded follower never caught up with the primary's WAL"
    )

    assert follower_rows(c, id) == 6
  end

  # The other side of the release: it is a QUORUM rule, not "any seed landed". With two of the three
  # followers held, only one seed can land in time, and that is not a quorum — the commit must still
  # fail rather than ack with one replica.
  test "one seed landed, two pending: the commit still does not ack", ctx do
    %{id: id, root: root} = ctx
    [{_a, pa}, {_b, pb}, {_c, pc}] = start_followers!(root, 3)
    {peer_b, pbx} = proxy!(pb)
    {peer_c, pcx} = proxy!(pc)
    enable!([pa, pbx, pcx], 800)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])

    :ok = PausablePeer.pause(peer_b)
    :ok = PausablePeer.pause(peer_c)
    {reply, ms} = timed_commit(id, wal, coordinator)

    assert match?({:error, {:no_quorum, _}}, reply),
           "acked #{inspect(reply)} with one seeded follower: one replica reported as a quorum"

    assert ms <= Session.commit_deadline_budget() + 100

    {:ok, _} = PausablePeer.release(peer_b)
    {:ok, _} = PausablePeer.release(peer_c)
  end
end
