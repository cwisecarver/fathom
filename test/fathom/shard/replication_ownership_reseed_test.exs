defmodule Fathom.Shard.ReplicationOwnershipReseedTest do
  @moduledoc """
  A new ownership re-seeds its followers instead of absorbing onto them (expert review 2026-09-29
  #1, and the two #24 tests that were waiting on it).

  ## The bug

  A push from a higher lineage (or, unstated, a higher lock epoch) routes through
  `FollowerLog.decide_fresh/2`, and `Follower.absorb_before_reset/4` then checkpointed the follower's
  OLD WAL into its `.db` and cleared `torn` whenever the push's `prev_extent` was 0 — which it always
  is from a new owner, whose session has no record of the follower. That is sound only if the new
  owner's base IS the old owner's final state. It is not after a snapshot restore or migration revert
  (the object now holds OLDER bytes), after a takeover the old owner never flushed for, or for a
  follower that lagged at the handoff. The follower then appended the new owner's WAL onto a `.db`
  it was never built from: a promotable hybrid of two ownerships.

  ## The fix, and why it re-seeds rather than only marking torn

  Marking the replica torn alone (the finding's option A) is safe, but nothing re-seeds a torn
  replica, so every replica of every shard would go un-promotable after the shard's first
  idle-drop + reopen — A2 promotion off fleet-wide. So the follower answers `:unknown_shard` ("I hold
  nothing you can build on"), which `Session` already seeds on, and marks itself torn in the
  meantime as the safety net. The seed cost was measured first (`./chaos.sh seed-rate`, 2026-09-30):
  link speed, ~290 ms for a 32 MiB shard on 1 Gbit/s.

  The raw-socket tests drive exact frames into a real follower, the same technique and the same
  reasons as `replication_short_reset_test.exs`. The end-to-end tests run real ownership cycles.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.FollowerLog
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Storage
  alias Fathom.Shards

  @gates [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_lineage_wire,
    :replication_ordinal_wire,
    :replication_reseed
  ]

  setup do
    id = "repl_reseed_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replreseed_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@gates, &{&1, Application.get_env(:fathom, &1)})

    # Both wire gates ON: the lineage rides the seed only with the lineage gate and the push only in
    # the ordinal frame shape. Off, every lineage is 0 ("unstated") and the rule falls back to the
    # lock epoch, which a clean reopen RESETS — the tests would be measuring the wrong clause.
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Shards.drain(id, 5_000)
      for e <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> e)
      File.rm_rf(root)
    end)

    %{id: id, root: root}
  end

  # ---------------------------------------------------------------------------------------------
  # the classification (pure)
  # ---------------------------------------------------------------------------------------------

  defp st(fields), do: Map.merge(FollowerLog.seeded(3, 1, 7, 4096, 5), Map.new(fields))

  defp push(fields),
    do: struct(Protocol.Push, Map.merge(%{epoch: 3, lineage: 5}, Map.new(fields)))

  describe "FollowerLog.reseed?/2" do
    test "a higher stated lineage is a new ownership, whatever the lock epoch says" do
      # A clean reopen RESETS the lock epoch to 1 while the lineage rises — the #12 case.
      assert FollowerLog.reseed?(st([]), push(lineage: 6, epoch: 1))
    end

    test "an EQUAL stated lineage is the same ownership even with a higher epoch" do
      # A restarted follower recovers at epoch 0 with its persisted lineage; the owner's next push
      # states a higher epoch at the same lineage. Reading that as an ownership change would
      # re-seed every replica on every follower restart.
      refute FollowerLog.reseed?(st(epoch: 0), push(lineage: 5, epoch: 3))
    end

    test "with no stated lineage, a higher lock epoch is the only evidence and counts" do
      assert FollowerLog.reseed?(st(lineage: 0), push(lineage: 0, epoch: 4))
      refute FollowerLog.reseed?(st(lineage: 0), push(lineage: 0, epoch: 3))
    end

    test "a torn replica always asks, and an unknown shard never does (it is already refused)" do
      assert FollowerLog.reseed?(st(torn: true), push([]))
      refute FollowerLog.reseed?(st([]), push([]))
      refute FollowerLog.reseed?(nil, push([]))
    end
  end

  # ---------------------------------------------------------------------------------------------
  # a real follower, exact frames
  # ---------------------------------------------------------------------------------------------

  defp start_follower!(root) do
    name = :"reseed_f_#{System.unique_integer([:positive])}"
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

    await!(
      fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end,
      "shippers never connected"
    )
  end

  defp await!(fun, msg, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn -> fun.() || Process.sleep(20) end)
    |> Enum.find(fn
      v when v in [nil, false, :ok] -> System.monotonic_time(:millisecond) > deadline
      _ -> true
    end)
    |> case do
      v when v in [nil, false, :ok] -> flunk(msg)
      v -> v
    end
  end

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

  # A follower holding a REAL seeded replica — a `.db`, a non-empty WAL and a stated lineage — then
  # the real primary quiesced, so the crafted frames below are the only ones it will ever see.
  # (`replication_short_reset_test.exs` records at length why stopping the Session alone is not
  # enough: the shippers outlive it and a queued push still lands.)
  defp seeded_follower!(%{id: id, root: root}) do
    [{name, port} | _] = followers = [start_follower!(root), start_follower!(root)]
    enable!(followers, 1)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, path <> "-wal", coordinator)

    await!(
      fn ->
        _ = Session.commit(id, path <> "-wal", coordinator)
        match?(%{next_offset: o} when o > 0, Follower.state_of(name, id))
      end,
      "the follower was never seeded"
    )

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])
    assert :ok = Session.commit(id, path <> "-wal", coordinator)

    Session.stop(id)
    stop_supervised!(Fleet)
    Connection.close(conn)
    Fathom.Shard.checkin(coordinator, ref)

    state = Follower.state_of(name, id)
    assert state.lineage > 0, "precondition: the seed stated no lineage, so the rule is untested"
    assert state.next_offset > 0, "precondition: the follower holds no WAL to absorb"
    refute state.torn, "precondition: the replica is already torn"

    %{name: name, port: port, state: state, id: id}
  end

  defp files(name, id),
    do: {File.read!(Follower.db_path(name, id)), File.read!(Follower.wal_path(name, id))}

  # What a new owner's FIRST push looks like: a higher lineage, a lock epoch reset to 1 by the clean
  # release, a fresh WAL (new salt) from offset 0, and `prev_extent` 0 because its session has never
  # heard of this follower.
  defp new_owner_push(id, state) do
    %Protocol.Push{
      shard_id: id,
      epoch: 1,
      lineage: state.lineage + 1,
      wal_gen: 0,
      salt1: state.salt1 + 1,
      offset: 0,
      prev_extent: 0,
      payload: :binary.copy(<<0>>, 32)
    }
  end

  test "a push from a NEW ownership asks for a seed and leaves the replica files alone", ctx do
    %{name: name, port: port, state: state, id: id} = seeded_follower!(ctx)
    before = files(name, id)

    assert {:ok, {:reject, ^id, :unknown_shard, 0}} =
             send_frame!(port, Protocol.encode_push(new_owner_push(id, state))),
           "the follower ACCEPTED the new owner's first push. It then absorbs the old owner's WAL " <>
             "and appends the new owner's onto it — a base the new owner never built from after " <>
             "a restore, a revert, an unflushed takeover or a lagging handoff."

    after_ = Follower.state_of(name, id)

    assert after_.torn,
           "the replica is not marked torn while it waits for the seed. If the seed never comes " <>
             "(disk headroom, deadline, the primary dying) it must be un-promotable, not a hybrid."

    assert File.exists?(Follower.torn_path(name, id)), "the torn marker is not durable"

    assert files(name, id) == before,
           "the replica's files changed — the old WAL was absorbed or the new one written over it"

    # THE FENCE ADVANCED with the new ownership, so the DEPOSED owner — still shipping at the old
    # lineage — is refused rather than accepted as an equal and allowed to seed its own state in.
    assert after_.lineage == state.lineage + 1

    deposed = %{new_owner_push(id, state) | lineage: state.lineage, epoch: state.epoch}

    assert {:ok, {:reject, ^id, :stale_epoch, _}} =
             send_frame!(port, Protocol.encode_push(deposed))
  end

  test "a torn replica keeps asking for a seed on every push", ctx do
    %{name: name, port: port, state: state, id: id} = seeded_follower!(ctx)
    push = new_owner_push(id, state)

    assert {:ok, {:reject, ^id, :unknown_shard, 0}} =
             send_frame!(port, Protocol.encode_push(push))

    # The SAME ownership now, and a push decide/2 would accept — but the replica is torn and only a
    # seed can clear that, so it asks again rather than appending onto files it cannot vouch for.
    assert {:ok, {:reject, ^id, :unknown_shard, 0}} =
             send_frame!(port, Protocol.encode_push(push))

    assert Follower.state_of(name, id).torn
  end

  test "with :replication_reseed OFF an ownership change still never absorbs — it stays torn",
       ctx do
    %{name: name, port: port, state: state, id: id} = seeded_follower!(ctx)
    Application.put_env(:fathom, :replication_reseed, false)
    {db_before, _wal} = files(name, id)

    # The switch restores "accept and replicate while torn", NOT the hybrid: the old WAL is not
    # absorbed and the flag does not clear.
    assert {:ok, {:ack, ^id, _}} =
             send_frame!(port, Protocol.encode_push(new_owner_push(id, state)))

    assert Follower.state_of(name, id).torn,
           "an ownership-change reset cleared torn — that is the hybrid #1 is about"

    assert File.read!(Follower.db_path(name, id)) == db_before,
           "the old owner's WAL was absorbed into the .db across an ownership change"
  end

  @t "SELECT a FROM t ORDER BY a"
  @u "SELECT v FROM u"

  # ---------------------------------------------------------------------------------------------
  # real ownership cycles (#24's two waiting tests, plus the availability direction)
  # ---------------------------------------------------------------------------------------------

  defp col(path, sql) do
    {:ok, conn} = Connection.open(path)

    try do
      {:ok, %{rows: rows}} = Connection.query(conn, sql, [])
      List.flatten(rows)
    after
      Connection.close(conn)
    end
  end

  # Read a follower's replica WITHOUT touching it: opening checkpoints, which breaks the follower's
  # byte alignment with its primary, so the pair is copied out and the copy is opened.
  defp replica_col(name, id, root, sql) do
    dir = Path.join(root, "peek_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    db = Path.join(dir, "r.db")
    File.cp!(Follower.db_path(name, id), db)

    if File.exists?(Follower.wal_path(name, id)),
      do: File.cp!(Follower.wal_path(name, id), db <> "-wal")

    col(db, sql)
  end

  defp write!(id, values) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE IF NOT EXISTS t (a INTEGER)", [])
    for v <- values, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [v])
    Fathom.Shard.WriteCounter.bump(id)

    # The connection stays OPEN until `stop_shard!/2`: closing the last one checkpoints and unlinks
    # the WAL, which leaves `Session.commit/3` nothing to ship and the followers nothing to hold.
    assert :ok = Session.commit(id, path <> "-wal", coordinator)
    {coordinator, ref, path, conn}
  end

  defp stop_shard!(id, {coordinator, ref, _path, conn}) do
    Connection.close(conn)
    Fathom.Shard.checkin(coordinator, ref)

    # The Session monitors its coordinator and stops ITSELF on the DOWN, so it is monitored here
    # and awaited — never stopped. This used to call `Session.stop/1` after the coordinator's DOWN,
    # which raced the Session's own exit: on a loaded CI runner the Registry still named the pid
    # and `GenServer.stop/3` exited `:normal` (CI, OTP 27, run 36672054563, seed 517105).
    session =
      case Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id) do
        [{pid, _}] -> {pid, Process.monitor(pid)}
        [] -> nil
      end

    mon = Process.monitor(coordinator)
    :ok = Shards.stop(id)
    assert_receive {:DOWN, ^mon, :process, ^coordinator, _}, 10_000

    with {pid, smon} <- session do
      assert_receive {:DOWN, ^smon, :process, ^pid, _}, 10_000
    end

    # Cold-open from the stored object on the next checkout, which is what makes a restore take.
    for e <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> e)
  end

  # Every follower CURRENT with the primary: re-seeded at the new lineage, untorn, holding exactly
  # the primary's WAL. Commits are re-driven while waiting, since seeding is write-gated.
  defp await_current!(followers, id, lineage, {coordinator, _ref, path, _conn}) do
    await!(
      fn ->
        _ = Session.commit(id, path <> "-wal", coordinator)
        wal = File.read!(path <> "-wal")

        Enum.all?(followers, fn {n, _} ->
          match?(%{torn: false, lineage: ^lineage}, Follower.state_of(n, id)) and
            File.read(Follower.wal_path(n, id)) == {:ok, wal}
        end)
      end,
      "the followers were never re-seeded to the new ownership (still torn, stale lineage or " <>
        "behind): #{inspect(Enum.map(followers, fn {n, _} -> Follower.state_of(n, id) end))}"
    )
  end

  test "RESTORE → commit: followers end up a copy of the RESTORED shard, not a hybrid", ctx do
    %{id: id, root: root} = ctx
    followers = [start_follower!(root), start_follower!(root)]
    enable!(followers, 1)

    # Ownership 1, part one: `t` rows 1..5 and a marker row in `u`, flushed and snapshotted.
    one = write!(id, 1..5)
    {c1, _, _, conn1} = one
    {:ok, _} = Connection.query(conn1, "CREATE TABLE u (v TEXT)", [])
    {:ok, _} = Connection.query(conn1, "INSERT INTO u VALUES ('restored')", [])
    write_more!(id, one, [])
    :ok = Shards.flush(id)
    :ok = Storage.snapshot(id, "before")
    lineage1 = Fathom.Shard.lineage(c1)

    # Part two, which the followers receive and the restore will throw away. `u` is changed here
    # and NEVER again, which is what makes the hybrid visible. The first version of this test
    # checked `t` alone and passed against the unfixed code: `t` fits on one page, the new owner's
    # insert rewrites that page, and a hybrid built on the wrong base read back exactly right.
    # A page the new owner does NOT rewrite comes from whatever base the follower holds — the
    # restored one after a seed, the pre-restore one after an absorb.
    {:ok, _} = Connection.query(conn1, "UPDATE u SET v = 'pre-restore'", [])
    write_more!(id, one, 6..10)

    await!(
      fn -> Enum.all?(followers, fn {n, _} -> Follower.state_of(n, id) end) end,
      "no replica"
    )

    stop_shard!(id, one)

    # The operator restores the snapshot: the object now holds OLDER bytes than every replica.
    snap = Path.join(System.tmp_dir!(), "reseed_snap_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> File.rm(snap) end)
    {:ok, _} = Storage.pull_snapshot(id, "before", snap)
    {:ok, live} = Storage.object_etag(id)
    :ok = Storage.restore_snapshot_from_file(id, snap, live)

    # Ownership 2 builds on the restored base, and touches `t` only.
    two = write!(id, [100])
    {c2, _, path, _} = two
    lineage2 = Fathom.Shard.lineage(c2)
    assert lineage2 > lineage1, "precondition: the reopen did not claim a new lineage"
    assert col(path, @t) == [1, 2, 3, 4, 5, 100], "precondition: the restore did not take"
    assert col(path, @u) == ["restored"], "precondition: the restore did not take"

    await_current!(followers, id, lineage2, two)

    for {n, _} <- followers do
      assert replica_col(n, id, root, @u) == ["restored"],
             "follower #{n} is a HYBRID: it absorbed the pre-restore WAL into its .db and applied " <>
               "the new owner's frames on top, so every page the new owner did not rewrite still " <>
               "holds pre-restore data. Promoting it would silently undo the restore."

      assert replica_col(n, id, root, @t) == [1, 2, 3, 4, 5, 100]
    end

    stop_shard!(id, two)
  end

  test "a clean REOPEN re-seeds the followers, so A2 stays promotable across ownerships", ctx do
    # THE AVAILABILITY DIRECTION. A fix that only marked replicas torn would pass the test above and
    # leave every follower of every shard un-promotable after the shard's first idle-drop — A2
    # promotion off fleet-wide, arriving by the door marked safety.
    #
    # NOT a regression test for #1, stated plainly: it PASSES against the unfixed code too, because a
    # clean reopen's base really is the old owner's final state and the old absorb got it right. It
    # is the guard that the fix did not buy safety with availability — revert `reseed?/2` to "mark
    # torn only" and it fails, since nothing else would ever clear the flag.
    %{id: id, root: root} = ctx
    followers = [start_follower!(root), start_follower!(root)]
    enable!(followers, 1)

    one = write!(id, 1..3)
    {c1, _, _, _} = one
    lineage1 = Fathom.Shard.lineage(c1)

    await!(
      fn -> Enum.all?(followers, fn {n, _} -> Follower.state_of(n, id) end) end,
      "no replica"
    )

    stop_shard!(id, one)

    two = write!(id, [4])
    {c2, _, _, _} = two
    lineage2 = Fathom.Shard.lineage(c2)
    assert lineage2 > lineage1, "precondition: the reopen did not claim a new lineage"

    await_current!(followers, id, lineage2, two)

    for {n, _} <- followers do
      refute File.exists?(Follower.torn_path(n, id)), "the seed did not clear the durable marker"
      assert replica_col(n, id, root, @t) == [1, 2, 3, 4]
    end

    stop_shard!(id, two)
  end

  defp write_more!(id, {coordinator, _ref, path, conn}, values) do
    for v <- values, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [v])
    Fathom.Shard.WriteCounter.bump(id)
    assert :ok = Session.commit(id, path <> "-wal", coordinator)
  end
end
