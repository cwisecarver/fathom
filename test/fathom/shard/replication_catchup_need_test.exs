defmodule Fathom.Shard.ReplicationCatchupNeedTest do
  @moduledoc """
  Expert review 2026-10-20-2027 #6 and #17: the catch-up timer and the empty round.

  #6 — SYMPTOM: on a BUSY shard the catch-up timer fired about once per `catchup_ms` and ran a full
  commit cycle (dirty-IO header read, plan, ownership gate) that planned nothing. Every commit stops
  at its quorum, so the non-quorum straggler's ack is still unread at reply; `arm_if_short/1` armed
  (or refreshed `catchup_at` on) the timer, the refresh kept `commits == catchup_at`, and by the time
  the timer fired the straggler's ack had landed. Measured at prod settings: +15% Session cycles at
  3 commits/s. INVARIANT pinned here: a fired timer runs a cycle only when some follower is behind
  AND free to push to; with every follower current it reads nothing, and with the only laggard's
  push still in flight it re-arms without reading. A genuinely short free follower still converges
  with no further writes (pinned by `replication_commit_test.exs`).

  #17 — SYMPTOM: a round whose every planned follower was skipped (push in flight / seeding) still
  paid `build_pushes/5`'s header re-check to build zero pushes. INVARIANT: a round that sends
  nothing reads nothing past its header.

  Both were run against the unfixed `session.ex` and seen to fail (see the commit).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session, Shipper, Wal}
  alias Fathom.Shards
  alias Fathom.Test.PausablePeer

  @registry Fathom.Shard.Replication.SessionRegistry

  setup do
    id = "repl_cuneed_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replcuneed_#{System.unique_integer([:positive])}")
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
    hid = "cuneed-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      hid,
      [:fathom, :replication, :commit],
      fn _, _, md, _ -> send(me, {:cycle, md.mode}) end,
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

  # 3 followers, Q=2, the third behind a PausablePeer; a current, seeded shard with one table.
  defp start!(%{id: id, root: root}, catchup_ms) do
    [{a, pa}, {b, pb}, {c, pc}] =
      for i <- 1..3 do
        name = :"cun_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    peer = start_supervised!({PausablePeer, upstream_port: pc}, id: :cun_peer)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, 2_000)
    Application.put_env(:fathom, :replication_catchup_ms, catchup_ms)

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
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for n <- [a, b, c], do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)
    wal = path <> "-wal"
    assert :ok = Session.commit(id, wal, coordinator)
    [{session, _}] = Registry.lookup(@registry, id)
    await!(fn -> all_current?(session) end, "every follower current after the first commit")
    flush_cycles()
    %{peer: peer, conn: conn, wal: wal, coordinator: coordinator, session: session}
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

  defp flush_cycles do
    receive do
      {:cycle, _} -> flush_cycles()
    after
      0 -> :ok
    end
  end

  # Calls `fun` with `mfa` call-counted in `pid` only; returns `{result, calls}`.
  defp count_calls(pid, mfa, fun) do
    :erlang.trace_pattern(mfa, true, [:call_count])
    :erlang.trace(pid, true, [:call])

    try do
      result = fun.()
      {:call_count, n} = :erlang.trace_info(mfa, :call_count)
      {result, n}
    after
      :erlang.trace(pid, false, [:call])
      :erlang.trace_pattern(mfa, false, [:call_count])
    end
  end

  test "#6: a timer that fires after the straggler's ack landed runs no cycle", ctx do
    %{peer: peer, conn: conn, wal: wal, coordinator: coordinator, session: session} =
      start!(ctx, 300)

    # The busy-shard shape: the quorum (a, b) acks, the straggler's (c) reply is held past the
    # reply, so the commit leaves it short and arms the timer.
    :ok = PausablePeer.pause(peer)
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(ctx.id, wal, coordinator)
    assert :sys.get_state(session).catchup_ref != nil, "the commit did not arm the catch-up"
    assert_receive {:cycle, :tenant}

    # The straggler's ack lands while the Session is idle (well before the 300 ms timer).
    {:ok, _} = PausablePeer.release(peer)
    await!(fn -> all_current?(session) end, "the straggler's held ack settled")

    {_, reads} =
      count_calls(session, {Wal, :read_held, 2}, fn ->
        # The timer fires at ~300 ms; nothing is short, so nothing runs.
        await!(fn -> :sys.get_state(session).catchup_ref == nil end, "the timer fired")
      end)

    refute_received {:cycle, :catch_up}, "the catch-up ran a cycle with every follower current"
    assert reads == 0
  end

  test "#6: a timer whose only laggard is still in flight re-arms without reading", ctx do
    %{peer: peer, conn: conn, wal: wal, coordinator: coordinator, session: session} =
      start!(ctx, 200)

    :ok = PausablePeer.pause(peer)
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(ctx.id, wal, coordinator)
    assert_receive {:cycle, :tenant}
    first = :sys.get_state(session).catchup_ref
    assert first != nil

    {_, reads} =
      count_calls(session, {Wal, :read_held, 2}, fn ->
        # Fired and re-armed: a new timer ref, the straggler still held.
        await!(
          fn ->
            ref = :sys.get_state(session).catchup_ref
            ref != nil and ref != first
          end,
          "the timer re-armed"
        )
      end)

    refute_received {:cycle, :catch_up}
    assert reads == 0

    # Released: the ack lands, the next fire finds nothing short and clears.
    {:ok, _} = PausablePeer.release(peer)
    await!(fn -> :sys.get_state(session).catchup_ref == nil end, "the timer cleared")
    assert all_current?(session)
    refute_received {:cycle, :catch_up}
  end

  test "#17: a round whose every planned follower is skipped does not re-check the header", ctx do
    %{session: session, wal: wal} = start!(ctx, 60_000)

    # One follower recorded a byte short of the extent AND with a push in flight: it is planned
    # (an append) and skipped `:already_in_flight`; the other two are current and carry the quorum.
    [{g, s, e} | _] = :sys.get_state(session).seen
    [victim | _] = Fleet.shippers()

    :sys.replace_state(session, fn st ->
      %{
        st
        | followers: Map.put(st.followers, victim, %{wal_gen: g, salt1: s, offset: e - 1}),
          inflight: Map.put(st.inflight, victim, %{wal_gen: g, salt1: s, offset: e}),
          inflight_at: Map.put(st.inflight_at, victim, System.monotonic_time(:millisecond))
      }
    end)

    {reply, rechecks} =
      count_calls(session, {Wal, :recheck_held, 2}, fn ->
        GenServer.call(session, {:commit, wal, :no_checkpoint})
      end)

    assert reply == :ok
    assert rechecks == 0, "an empty round re-read the WAL header to build zero pushes"
  end
end
