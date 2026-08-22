defmodule Fathom.Shard.ReplicationShortResetTest do
  @moduledoc """
  `prev_extent` and the torn rule at a generation boundary (expert review 2026-08-20 #11a).

  ## The bug #11b did NOT fix

  `absorb_before_reset/4` runs a local checkpoint to fold the WAL it holds into its `.db` before a
  reset discards that WAL, and on success it cleared `torn`. But a follower that was BEHIND when
  the generation changed never received that generation's tail — so the pages it absorbs are
  incomplete, and the result is a database that opens cleanly, passes `quick_check`, and is quietly
  missing writes. Clearing `torn` then laundered it into a promotable replica.

  **The follower cannot detect this alone**, which is why the fix needed the wire: a reset says
  nothing about the generation it replaces, so "my offset is 4096" is indistinguishable from
  complete and from four frames short. `prev_extent` is the primary's statement of how far that
  generation actually got.

  ## Why these drive a RAW SOCKET into the follower

  There is no public push seam — pushes arrive over TCP — and staging "one follower legitimately
  lagged at exactly a generation boundary" through two live primaries is a race, not a test. A raw
  socket gives exact control over `offset` and `prev_extent`, which are the only two values the
  rule reads.

  It is also, not coincidentally, the same injection path #3's HMAC closes: with
  `:replication_hmac_required` on, every frame these tests send would be refused. That is asserted
  at the bottom, because a test suite that can only reach this code with authentication OFF should
  say so.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Shards

  setup do
    id = "repl_short_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replshort_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev = %{
      enabled: Application.get_env(:fathom, :replication_enabled),
      followers: Application.get_env(:fathom, :replication_followers),
      quorum: Application.get_env(:fathom, :replication_quorum),
      sign: Application.get_env(:fathom, :replication_sign_frames),
      required: Application.get_env(:fathom, :replication_hmac_required)
    }

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- [
            replication_enabled: prev.enabled,
            replication_followers: prev.followers,
            replication_quorum: prev.quorum,
            replication_sign_frames: prev.sign,
            replication_hmac_required: prev.required
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Shards.drain(id, 5_000)
      for e <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> e)
      File.rm_rf(root)
    end)

    %{id: id, root: root}
  end

  defp start_follower!(root) do
    name = :"short_f_#{System.unique_integer([:positive])}"
    dir = Path.join(root, to_string(name))
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    {:ok, port} = Follower.port(pid)
    {name, port}
  end

  defp enable!(followers, q) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, q)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
    await_connected!()
  end

  defp await_connected!(timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Enum.all?(Fleet.shippers(), &Shipper.connected?/1),
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

  defp await_seeded(name, id, commit, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      case Follower.state_of(name, id) do
        nil ->
          commit.()
          Process.sleep(20)
          :waiting

        state ->
          state
      end
    end)
    |> Enum.find(fn
      :waiting -> System.monotonic_time(:millisecond) > deadline
      _ -> true
    end)
    |> case do
      :waiting -> flunk("follower never got seeded")
      state -> state
    end
  end

  # One frame in, one reply out, on a socket of our own — the follower's listener speaks
  # `packet: 4`, so this is the whole client.
  defp send_frame!(port, frame) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 5_000)

    try do
      :ok = :gen_tcp.send(sock, frame)

      case :gen_tcp.recv(sock, 0, 5_000) do
        {:ok, bytes} -> Protocol.decode(bytes)
        other -> other
      end
    after
      :gen_tcp.close(sock)
    end
  end

  # A reset push: offset 0, a generation and salt that differ from what the follower holds, so
  # `FollowerLog.decide/2` returns `{:reset_then_append, _}` and `absorb_before_reset/4` runs.
  defp reset_push(id, state, prev_extent) do
    %Protocol.Push{
      shard_id: id,
      epoch: state.epoch,
      wal_gen: state.wal_gen + 1,
      salt1: state.salt1 + 1,
      offset: 0,
      payload: :binary.copy(<<0>>, 32),
      prev_extent: prev_extent
    }
  end

  # A follower holding a real seeded replica with a non-zero offset, which is the precondition
  # every case below needs — `absorb_before_reset/4` short-circuits to "not torn, simply new" when
  # there is no `.db` or an empty WAL, and a test that landed there would prove nothing.
  defp seeded_follower!(ctx) do
    %{id: id, root: root} = ctx
    Application.put_env(:fathom, :replication_sign_frames, true)

    # TWO followers with q=1, not one with q=1: `Fleet.validate_quorum!/0` refuses Q=N, because a
    # quorum that needs every follower tolerates zero failures and inherits the slowest replica.
    # Only the first is driven; the second exists to make the configuration legal.
    [follower | _] = followers = [start_follower!(root), start_follower!(root)]
    {name, port} = follower
    enable!(followers, 1)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    await_seeded(name, id, fn -> Session.commit(id, wal, coordinator) end)

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    # QUIESCE THE REAL PRIMARY BEFORE READING STATE, and this is load-bearing rather than tidy.
    #
    # `ship_quorum/4` returns at the Q-th ack — here q=1 of 2 followers — so `Session.commit/3`
    # can return having been satisfied by the OTHER follower while this one still has a push in
    # flight. That push lands whenever it lands. If it arrives after the read below and before the
    # crafted frame, `next_offset` has moved and the "short" case is no longer short: the test
    # fails, intermittently, under load, in CI and never here.
    #
    # AGENTS.md records this exact class twice (the straggler note in the seed tests, and three of
    # the #38/#39 tests that CI caught and the local run did not). Stopping the session makes the
    # crafted frames the only ones the follower will ever see again, which removes the class rather
    # than widening a window.
    Session.stop(id)

    state = Follower.state_of(name, id)

    assert state.next_offset > 0,
           "the follower holds nothing, so the reset would take the 'simply new' branch and this " <>
             "test would pass without exercising the rule"

    assert Wal.read(Follower.wal_path(name, id)) != {:ok, :empty},
           "the follower's WAL is empty, so there is nothing to absorb"

    %{name: name, port: port, state: state, id: id}
  end

  test "a follower SHORT of the outgoing generation stays torn through a reset", ctx do
    %{name: name, port: port, state: state, id: id} = seeded_follower!(ctx)

    # The primary says the generation being discarded reached FURTHER than this follower got. That
    # is positive evidence of frames this replica never received — the pages it is about to absorb
    # are incomplete.
    ahead = state.next_offset + 4096

    assert {:ok, _} = send_frame!(port, Protocol.encode_push(reset_push(id, state, ahead)))

    assert Follower.state_of(name, id).torn,
           "the follower absorbed a SHORT WAL and cleared torn anyway. That replica is missing " <>
             "the tail of the generation it just discarded, it opens cleanly and passes " <>
             "quick_check, and Promote.fresher?/2 will now hand it to a tenant."
  end

  test "a follower that held the WHOLE outgoing generation clears torn", ctx do
    %{name: name, port: port, state: state, id: id} = seeded_follower!(ctx)

    # THE OTHER DIRECTION, and it is the one that matters for availability. A rule that marked
    # every replica torn would also pass the test above while making the whole fleet
    # un-promotable — which is the failure A2 exists to prevent, arriving through the door marked
    # safety.
    assert {:ok, _} =
             send_frame!(port, Protocol.encode_push(reset_push(id, state, state.next_offset)))

    refute Follower.state_of(name, id).torn,
           "a follower that held everything the primary shipped was still marked torn"
  end

  test "prev_extent 0 means NO STATEMENT and clears torn — the rolling-upgrade case", ctx do
    %{name: name, port: port, state: state, id: id} = seeded_follower!(ctx)

    # An un-upgraded primary sets no field, so it arrives as 0. Reading absence as a gap would
    # mark every replica in the fleet torn for the length of a rolling upgrade.
    assert {:ok, _} = send_frame!(port, Protocol.encode_push(reset_push(id, state, 0)))

    refute Follower.state_of(name, id).torn,
           "a reset carrying no prev_extent was read as evidence of a gap; during a rolling " <>
             "upgrade that marks the entire fleet un-promotable"
  end

  test "the raw-socket injection these tests rely on is what #3 closes", ctx do
    %{port: port, state: state, id: id} = seeded_follower!(ctx)

    # Not a property of #11a — a statement about the reach of this file. Every frame above is
    # accepted because this node does not REQUIRE authentication; with it on, the same socket is
    # refused and the frames never reach the torn rule at all.
    Application.put_env(:fathom, :replication_sign_frames, false)
    Application.put_env(:fathom, :replication_hmac_required, true)

    unsigned = Protocol.encode_push(reset_push(id, state, 999_999))
    result = send_frame!(port, unsigned)

    assert match?({:error, :closed}, result) or match?({:error, _}, result),
           "an unsigned frame was answered while :replication_hmac_required was on: #{inspect(result)}"
  end
end
