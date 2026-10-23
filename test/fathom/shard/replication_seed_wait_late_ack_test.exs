defmodule Fathom.Shard.ReplicationSeedWaitLateAckTest do
  @moduledoc """
  REGRESSION (expert review 2026-10-22 r8 #27): a mismatched late ACK consumed inside a commit's
  seed wait left its `inflight` entry behind forever.

  `inflight` means "this follower still owes a reply". Every other consumer of a late
  `{:repl_reply, _, {:ack, _, _}}` — `handle_info/2`, `drain_late_replies/2`, `settle_inflight/3` —
  runs `settle_late_ack/3` AND `forget_inflight/2`: the first declines to advance an ack for a
  position that was not recorded, the second ends the expectation either way, because the follower
  HAS answered. `await_seed_messages/5`'s ack clause ran only the first, so an ack whose position
  disagreed with the record (the real shape: a straggler's answer to a push the record has since
  moved past) was swallowed with the expectation still standing. The follower then looked
  mid-flight until its `inflight_at` aged out, and an `await_no_inflight!` in
  `replication_collapse_test` #6 timed out once under CI load (27bc300d / 299b1eba).

  Deterministic, not a race: the only `SeedGate` slot is held by the test, so the commit's seeds
  block and the Session sits in `await_seed_messages/5` until the commit deadline; the test waits
  for exactly that frame and only then sends the ack, so it can only be consumed by that clause.
  The ack's sender is a stand-in pid with an injected expectation, because a real shipper's entry
  is consumed by `collect/4` inside the same call and cannot be set up from outside it; the clause
  under test does not look at who the sender is beyond the `inflight` key.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.SeedGate
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shards

  setup do
    id = "repl_seedwait_ack_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replseedwait_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
      :replication_timeout_ms,
      :replication_catchup_ms,
      :replication_seed_max_concurrency
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

  test "a mismatched ack consumed during a seed wait no longer leaves the follower owing a reply",
       %{id: id, root: root} do
    followers =
      for i <- 1..3 do
        name = :"sw_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "#{name}")},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    [a, b, c] = Enum.map(followers, &elem(&1, 0))

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, 2_000)
    Application.put_env(:fathom, :replication_catchup_ms, 0)
    Application.put_env(:fathom, :replication_seed_max_concurrency, 1)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_, p} <- followers, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await_connected!(3)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    wal = path <> "-wal"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for name <- [a, b, c], do: Follower.seed(name, id, epoch, 0, 0, 0, lineage)
    assert :ok = Session.commit(id, wal, coordinator)

    [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)

    # Hold the node's only seed slot: the seeds the next commit starts block in `SeedGate.acquire`,
    # so the commit waits in `await_seed_messages/5` until its deadline.
    test_pid = self()

    holder =
      spawn(fn ->
        :ok = SeedGate.acquire(5_000)
        send(test_pid, :slot_held)
        Process.sleep(:infinity)
      end)

    on_exit(fn -> Process.exit(holder, :kill) end)
    assert_receive :slot_held, 2_000

    # b and c lose their replicas, so the next push is refused `:unknown_shard` by both: the quorum
    # is impossible and the commit seeds them.
    :ok = Follower.forget(b, id)
    :ok = Follower.forget(c, id)

    # The expectation a late ack will disagree with.
    straggler = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(straggler, :kill) end)

    :sys.replace_state(session, fn s ->
      %{
        s
        | inflight: Map.put(s.inflight, straggler, %{offset: 4_096}),
          inflight_at: Map.put(s.inflight_at, straggler, System.monotonic_time(:millisecond))
      }
    end)

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    commit = Task.async(fn -> Session.commit(id, wal, coordinator) end)

    await_frame!(session, {Session, :await_seed_messages, 5})

    # The call is now blocked inside the seed wait, so this ack can only be consumed by that
    # clause (were it left to `handle_info/2` after the call, which forgets, the test would pass
    # pre-fix — it does not: checked against the unfixed session.ex).
    send(session, {:repl_reply, straggler, {:ack, id, 8_192}})

    assert {:error, {:no_quorum, _}} = Task.await(commit, 5_000)

    inflight = :sys.get_state(session).inflight

    refute Map.has_key?(inflight, straggler),
           "the follower answered (a mismatched ack, consumed inside the seed wait) but is still " <>
             "recorded as owing a reply: #{inspect(inflight)}"
  end

  defp await_connected!(n, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        Enum.count(Fleet.shippers(), &Shipper.connected?/1) >= n -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(20) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("shippers never connected")
  end

  # Poll the Session's current function until it is `frame` — i.e. blocked in that receive.
  defp await_frame!(pid, frame, timeout \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        Process.info(pid, :current_function) == {:current_function, frame} -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(5) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("the Session never entered #{inspect(frame)}")
  end
end
