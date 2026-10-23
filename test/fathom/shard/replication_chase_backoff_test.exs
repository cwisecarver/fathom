defmodule Fathom.Shard.ReplicationChaseBackoffTest do
  @moduledoc """
  Expert review 2026-10-21 rv7 F1: the ack-pace chase (r7 #8) must not bypass the catch-up backoff.

  SYMPTOM: with one follower DOWN, `catch_up_need/1` is `:short` forever. Once a catch-up episode
  set `catchup_chase`, every late ack from a HEALTHY straggler (N > Q leaves one per commit)
  cancelled the timer `retry_delay_ms/2` had armed at the `:disconnected` cap and fired `:catch_up`
  at 0 — one full catch-up cycle per tenant commit against a peer that is not coming back (probe:
  13 cycles in 20 commits; 0 before r7 #8). That is the cascading 1 Hz retry 2026-08-20 #25 removed,
  back at commit rate.

  INVARIANT: while a refusal is outstanding (`catchup_fails > 0`) — or when the acking follower is
  itself current — a late ack does not schedule a catch-up; the armed (backed-off) timer stands.
  The laggard-converges-at-ack-pace half of r7 #8 is pinned by `replication_catchup_hol_test.exs`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session, Shipper}
  alias Fathom.Shards

  @registry Fathom.Shard.Replication.SessionRegistry
  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_timeout_ms,
    :replication_catchup_ms,
    :shard_flush_interval_ms,
    :replication_max_queue_bytes
  ]

  setup do
    id = "repl_chasebo_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replchasebo_#{System.unique_integer([:positive])}")
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

  # Three live followers and one dead one at Q=2, the shard written once and settled.
  defp fleet!(id, root) do
    live =
      for i <- 1..3 do
        name = :"chasebo_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    # A dead port: nothing listens there.
    {:ok, ls} = :gen_tcp.listen(0, [])
    {:ok, dead} = :inet.port(ls)
    :gen_tcp.close(ls)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, 2_000)
    Application.put_env(:fathom, :replication_catchup_ms, 1_000)
    # The `:disconnected` cap: long, so no timer of its own fires during the test.
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

    Application.put_env(
      :fathom,
      :replication_followers,
      for(p <- Enum.map(live, &elem(&1, 1)) ++ [dead], do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)

    await!(
      fn -> Enum.count(Fleet.shippers(), &Shipper.connected?/1) == 3 end,
      "the three live shippers connect"
    )

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    wal = path <> "-wal"
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for {n, _} <- live, do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)
    _ = Session.commit(id, wal, coordinator)
    [{session, _}] = Registry.lookup(@registry, id)
    await!(fn -> :sys.get_state(session).inflight == %{} end, "first commit settles")
    %{session: session, conn: conn, wal: wal, coordinator: coordinator}
  end

  test "late straggler acks do not run a catch-up per commit while a dead follower is backed off",
       %{id: id, root: root} do
    %{session: session, conn: conn, wal: wal, coordinator: coordinator} = fleet!(id, root)

    # Enter a catch-up episode the way the (capped) timer would. The cycle is one non-blocking round
    # (r7 #8): the dead follower's `:disconnected` reject arrives after it, as a late reply.
    send(session, :catch_up)
    await!(fn -> :sys.get_state(session).inflight == %{} end, "the catch-up round settles")
    st = :sys.get_state(session)
    assert st.catchup_chase, "no catch-up episode: the test exercises nothing"
    assert st.catchup_fails > 0, "the dead follower was not refused: the test exercises nothing"

    :erlang.trace(session, true, [:receive])

    for i <- 1..20 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?)", [i])
      _ = Session.commit(id, wal, coordinator)
      # Let this commit's straggler ack land (and any chase it schedules be received).
      await!(fn -> :sys.get_state(session).inflight == %{} end, "commit #{i} settles")
    end

    _ = :sys.get_state(session)
    :erlang.trace(session, false, [:receive])

    cycles =
      Stream.repeatedly(fn ->
        receive do
          {:trace, ^session, :receive, :catch_up} -> :catch_up
          {:trace, _, _, _} -> :other
        after
          0 -> nil
        end
      end)
      |> Enum.take_while(& &1)
      |> Enum.count(&(&1 == :catch_up))

    assert cycles <= 1,
           "#{cycles} catch-up cycles in 20 commits with a dead follower backed off at the cap: " <>
             "late straggler acks bypassed retry_delay_ms/2"
  end

  # A REFUSAL THAT LANDS BEFORE THE POST-SHIP DRAIN STILL COUNTS (CI run 39889287951, OTP 28: the
  # test above failed its own precondition, `catchup_fails > 0`, with `inflight` already empty).
  # The catch-up round ships fire-and-forget to the dead follower (the live three are current), so
  # its `:disconnected` reply races the Session's `drain_late_replies` right after the round. When
  # the drain won, the refusal was reconciled and FORGOTTEN: the round's own reject list was `[]`,
  # so `arm_if_rejected/2` reset `catchup_fails` to 0, which reopens the rv7 F1 chase gate.
  #
  # Reproduced deterministically with the `:overloaded` refusal, which `Shipper.push/2` sends to
  # the Session itself before the round returns, so it is ALWAYS in the mailbox at the drain: a
  # byte budget of 1 refuses the only push the round makes. No push reaches a shipper, so no other
  # reply can arrive afterwards and count it instead.
  test "a refusal drained right after a round still deepens the catch-up backoff",
       %{id: id, root: root} do
    %{session: session} = fleet!(id, root)

    test_pid = self()
    handler = "chasebo-overloaded-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :replication, :reject],
      fn _, _, %{reason: reason}, _ ->
        if self() == session, do: send(test_pid, {:session_refused, reason})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    Application.put_env(:fathom, :replication_max_queue_bytes, 1)
    send(session, :catch_up)
    st = :sys.get_state(session)
    Application.delete_env(:fathom, :replication_max_queue_bytes)

    assert_received {:session_refused, :overloaded},
                    "fixture: the catch-up round pushed nothing for the budget to refuse"

    assert st.catchup_fails > 0,
           "the round's :overloaded refusal was drained and forgotten: catchup_fails reset to 0"

    assert st.catchup_ref != nil, "the refused follower has no catch-up armed"
  end
end
