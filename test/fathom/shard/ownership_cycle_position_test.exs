defmodule Fathom.Shard.OwnershipCyclePositionTest do
  @moduledoc """
  `Storage.position` across a REAL ownership cycle (expert review 2026-08-20 #38).

  `Promote.fresher?/2` compares `{epoch, wal_gen, offset}` and decides whether a node's local
  REPLICA outranks the stored object — i.e. whether a failover serves the replica or the object.
  It is the most safety-critical comparison in the codebase, and every test of the ordering it
  depends on was either:

    * a pure-function test over hand-built maps (`flush_position_test.exs` constructs
      `replica(e,g,o)` / `stamp(e,g,o)` literals), or
    * a structural source-string assertion scoped to one function, or
    * `promote_on_open_test.exs`, which explicitly EXCLUDES the graceful-drop path because
      including it made the tests vacuous, and installs its "lagging" replica by hand at
      `offset: stamp.offset - 1` rather than letting a real follower lag.

  So nothing drove **acquire → write → ship → flush → drop → release → re-acquire → compare**,
  which is the only sequence in which the two ordering assumptions are actually exercised. That is
  why #4 and #8 both survived: the coverage structurally could not fail for either.

  Run over BOTH liveness modes, asserting the mode actually took — a scenario that silently ran
  legacy twice looks like two-mode coverage and is one (AGENTS.md).

  ## What this does NOT do, stated plainly

  The graceful-drop case does **not** discriminate #4. At drop time the WAL has already been
  folded in and unlinked, so `position_after_checkpoint/2` takes its AMBIGUOUS branch and answers
  `nil` — and the pre-#4 code reached `nil` there too, by a different route. The read-order guard
  in `flush_position_test.exs` is what pins #4 and does fail against the old code. What this file
  adds is the end-to-end outcome over a real ownership cycle, which nothing covered at all, plus
  the `nil`-means-un-overridable property asserted rather than assumed.

  The epoch case is a **characterization**: #8 is parked, the epoch really does reset on a clean
  release, and this file says so out loud so the next reader does not learn it from a replica
  being promoted over a good object.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Storage
  alias Fathom.Shards

  setup do
    root = Path.join(System.tmp_dir!(), "owncycle_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev = %{
      enabled: Application.get_env(:fathom, :replication_enabled),
      followers: Application.get_env(:fathom, :replication_followers),
      quorum: Application.get_env(:fathom, :replication_quorum)
    }

    on_exit(fn ->
      for {k, v} <- [
            replication_enabled: prev.enabled,
            replication_followers: prev.followers,
            replication_quorum: prev.quorum
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
    end)

    %{root: root}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(),
           "legacy mode needs the heartbeat OFF; config/test.exs default is heartbeat_server: false"

    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  defp assert_mode!(coordinator, :heartbeat) do
    assert is_integer(:sys.get_state(coordinator).acquire_gen),
           "expected heartbeat mode (non-nil acquire_gen) — the mode setup did not take"
  end

  defp assert_mode!(coordinator, :legacy) do
    assert is_nil(:sys.get_state(coordinator).acquire_gen),
           "expected legacy mode (nil acquire_gen) — something started the heartbeat"
  end

  # Shippers connect in `handle_continue`, so without this the first commit RACES the connect and
  # every follower answers `:disconnected` — nothing ships, the follower records offset 0, and the
  # comparison below has no replica to make. Wins the race when this file runs alone and loses it
  # under a full-suite load, which is how it reached CI green locally and red there.
  defp await_connected!(timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Enum.all?(Fleet.shippers(), &Fathom.Shard.Replication.Shipper.connected?/1),
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

  # The first follower with a real recorded position, or fail loudly. Any of them proves the
  # point: the comparison below is "did a follower that watched this shard end up outranking the
  # object", and which follower answered first is not part of it.
  defp await_replica!(followers, id, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      found =
        Enum.find_value(followers, fn {n, _} ->
          case Follower.state_of(n, id) do
            %{next_offset: o} = st when o > 0 -> st
            _ -> nil
          end
        end)

      found || Process.sleep(20)
    end)
    |> Enum.find(fn
      %{} -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      %{} = st -> st
      _ -> flunk("no follower ever recorded a position — nothing was actually shipped")
    end
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"oc_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  for mode <- [:legacy, :heartbeat] do
    @mode mode

    describe "#{mode} mode" do
      # THE GRACEFUL-DROP PATH, which promote_on_open_test.exs deliberately avoids.
      #
      # A clean stop CHECKPOINTS the WAL and then closes the connection, which on the last one
      # unlinks `-wal`. Reading the WAL after that finds nothing, and the object was stamped
      # `{epoch, 0, 0}` — the LOWEST position for its epoch, on the most complete copy of the
      # shard that will ever exist. Every clean idle-drop, graceful drain and rebalance handoff
      # went out that way, and any lagging replica then outranked it (#4).
      test "a gracefully dropped object is NOT outranked by the follower that watched it", ctx do
        %{root: root} = ctx
        set_mode!(@mode)

        id = "occ_#{System.unique_integer([:positive])}"
        followers = start_followers!(root, 2)

        Application.put_env(:fathom, :replication_enabled, true)
        Application.put_env(:fathom, :replication_quorum, 1)

        Application.put_env(
          :fathom,
          :replication_followers,
          for({_n, p} <- followers, do: {~c"127.0.0.1", p})
        )

        start_supervised!(Fleet)
        await_connected!()

        {:ok, coordinator, ref, path} = Shards.checkout(id)
        assert_mode!(coordinator, @mode)

        on_exit(fn ->
          Shards.drain(id, 5_000)
          for e <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> e)
        end)

        {:ok, conn} = Connection.open(path)
        {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
        for n <- 1..20, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [n])

        for {n, _} <- followers, do: Follower.seed(n, id, 0, 0, 0, 0)
        assert :ok = Session.commit(id, path <> "-wal", coordinator)

        # A real follower's real recorded position — not a hand-installed map.
        #
        # POLLED, and for a reason that is the whole design of A2: `ship_quorum/4` returns at the
        # Q-th ack, so with q=1 the OTHER follower may be the one that answered and this one is
        # still a straggler when the commit returns. Reading a fixed follower once made this pass
        # alone and fail under a full-suite load.
        replica = await_replica!(followers, id)

        Connection.close(conn)
        Fathom.Shard.checkin(coordinator, ref)

        # THE GRACEFUL PATH. Checkpoint, flush, drop, release — all of it.
        mon = Process.monitor(coordinator)
        :ok = Shards.stop(id)
        assert_receive {:DOWN, ^mon, :process, ^coordinator, _}, 10_000

        assert {:ok, stamp} = Storage.object_position(id)

        # PRECONDITION, and it is the whole reason this comparison is meaningful: the follower
        # really did record a position, from real shipped bytes, rather than an empty map.
        assert replica.next_offset > 0,
               "the follower recorded offset 0 — nothing was shipped, so there is no replica to " <>
                 "outrank anything"

        refute Promote.fresher?(Map.put(replica, :torn, false), stamp),
               "a follower's replica outranked the object written by the graceful drop that " <>
                 "included every byte the follower ever saw. That object is the most complete " <>
                 "copy of the shard that will ever exist; promoting a replica over it discards " <>
                 "everything the checkpoint folded in."

        # WHY it is not outranked, asserted rather than left implicit. At drop time the WAL has
        # already been folded in and unlinked, so `position_after_checkpoint/2` is in its
        # AMBIGUOUS case and deliberately answers `nil` — "unknown", which makes the object
        # un-overridable. `{epoch, 0, 0}` (the pre-#4 answer) would have lost to every replica.
        #
        # Stating it here because it is also why this test CANNOT discriminate #4: both the fixed
        # and the unfixed code reach `nil` on this path, by different routes. The read-order
        # guard in `flush_position_test.exs` is what pins #4 and DOES fail against the old code;
        # what this test pins is the end-to-end outcome over a real ownership cycle, which
        # nothing covered at all.
        assert is_nil(stamp),
               "the drop stamped a concrete position (#{inspect(stamp)}). That is not wrong, but " <>
                 "it means this path no longer takes the ambiguous branch — re-read the note " <>
                 "above, because the refute below it is then testing something different."
      end
    end
  end

  # A REAL PUSH-DERIVED REPLICA MEETING A REAL LINEAGE STAMP (expert review 2026-08-24 #12).
  #
  # This is the test the finding said did not exist anywhere, and the reason it did not is subtle:
  # on a shard's FIRST open the lock epoch and the lineage are BOTH 1, so every fixture that seeded
  # a replica at the object's own number produced two counters that agreed by accident. The bug —
  # `fresher?/2` comparing the replica's LOCK epoch against the object's LINEAGE — is invisible
  # until they diverge, which takes a second ownership.
  #
  # So this runs one full ownership cycle first, purely to push the lineage past 1 while
  # `release_lease` resets the lock epoch back to it. Then it seeds a follower through the REAL
  # seeding path — no `Follower.seed/7` call, the follower answers `:unknown_shard` and
  # `Session.start_seeds/3` streams it — takes a REAL flush, and compares.
  #
  # `assert is_integer(stamp)`-style preconditions on both numbers, because the failure mode here
  # is a vacuous pass: `object_position/1` answers `nil` on the graceful-drop path (see the test
  # above), and `nil` makes every comparison trivially false.
  #
  # WHAT THIS TEST IS AND IS NOT, plainly. Against the unfixed code it fails on
  # `Fathom.Shard.lineage/1` being undefined — a structural failure, not a demonstration of the
  # bug — so it is a GUARD on the new field rather than a red-green reproduction. The behavioural
  # discrimination lives in `flush_position_test.exs`, whose `fresher?/2` cases now compare
  # lineage against lineage and whose "no stated lineage is never fresher" case is new.
  #
  # A stronger version was attempted and abandoned after three fixtures: ship, flush, ship more,
  # then assert `Promote.fresher?/2` answers TRUE where it used to answer false. It cannot be
  # staged this way, and the reason is worth recording so nobody tries a fourth time — a flush
  # CHECKPOINTS, which advances the WAL generation, and `fresher?/2` orders on
  # `{lineage, wal_gen, offset}` lexicographically. A follower shipped to after a flush is a
  # generation BEHIND, so it loses on `wal_gen` regardless of how many bytes it holds, and one
  # shipped to before the flush is behind on offset. There is no window in this sequence where a
  # replica is cleanly ahead within one generation; producing one needs a primary that dies
  # between ships without flushing, which is `promote_on_open_test.exs`'s territory and needs its
  # hand-installed replicas.
  test "a real seeded replica ranks on the same counter as the object's stamp", ctx do
    %{root: root} = ctx
    set_mode!(:heartbeat)

    id = "occlin_#{System.unique_integer([:positive])}"

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 1)
    Application.put_env(:fathom, :replication_lineage_wire, true)

    on_exit(fn ->
      Shards.drain(id, 5_000)
      Application.delete_env(:fathom, :replication_lineage_wire)
      for e <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> e)
    end)

    # ONE COMPLETE OWNERSHIP FIRST. Its only job is to advance the lineage, so the second open's
    # lineage and lock epoch are different numbers and the comparison can fail.
    _ = ownership_cycle!(id)

    # TWO followers with quorum 1: `Fleet` refuses Q == N outright ("tolerates zero follower
    # failures and inherits the slowest replica"), so a single follower cannot be configured here.
    followers = start_followers!(root, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, p} <- followers, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await_connected!()

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    assert_mode!(coordinator, :heartbeat)

    lineage = Fathom.Shard.lineage(coordinator)
    {:ok, lock_epoch} = Fathom.Shard.epoch(coordinator)

    # THE PRECONDITION THAT MAKES THIS DISCRIMINATE. If these were equal the test would pass
    # against the unfixed code too, which is exactly how every existing fixture missed the bug.
    assert lineage > lock_epoch,
           "the lineage (#{lineage}) did not advance past the lock epoch (#{lock_epoch}), so the " <>
             "two counters still agree and this test cannot tell them apart — the first " <>
             "ownership cycle did not flush"

    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE IF NOT EXISTS t2 (a)", [])
    for n <- 1..20, do: {:ok, _} = Connection.query(conn, "INSERT INTO t2 VALUES (?1)", [n])

    # NOT pre-seeded: the follower answers :unknown_shard and the commit drives the real seeding
    # path, which is what carries the lineage on the wire.
    assert :ok = Session.commit(id, path <> "-wal", coordinator)
    assert await_replica!(followers, id).next_offset > 0, "nothing was shipped"

    # A REAL FLUSH — not the graceful drop, which unlinks the WAL and deliberately stamps `nil`.
    Fathom.Shard.WriteCounter.bump(id)
    :ok = Shards.flush(id)
    assert {:ok, stamp} = Storage.object_position(id)

    assert is_map(stamp),
           "the flush stamped no position, so every comparison below is vacuously false"

    # THE ASSERTION: both sides carry the SAME counter, and it is the lineage.
    replica = await_replica!(followers, id)

    assert replica.lineage == stamp.epoch,
           "the replica ranks on #{inspect(replica.lineage)} while the object's stamp carries " <>
             "#{inspect(stamp.epoch)}. Those are different counters, and that is the bug: the " <>
             "replica used to report its primary's LOCK epoch (#{lock_epoch} here, reset to 1 by " <>
             "the previous ownership's clean release) against a monotonic lineage, so " <>
             "fresher?/2 answered false for every replica however far ahead it was."

    refute replica.lineage == lock_epoch,
           "the replica is still reporting the lock epoch — the wire change did not take"

    Connection.close(conn)
    Fathom.Shard.checkin(coordinator, ref)
  end

  # THIS WAS A CHARACTERIZATION TEST, AND IT ASSERTED THE OPPOSITE OF WHAT IT DOES NOW.
  #
  # It used to say "the lease epoch RESETS on a clean release — the ordering assumption is
  # currently false", pinning a known defect (#8, then parked) so the next reader would not
  # discover it by having a replica promoted over a good object. Its comment predicted "WHEN #8
  # LANDS, THIS ASSERTION INVERTS."
  #
  # #8 landed, and the assertion did NOT invert, because the fix that shipped is not the fix that
  # comment anticipated. The chosen design SPLITS the two meanings that were conflated rather than
  # making one number serve both:
  #
  #   * the LOCK epoch stays exactly as it was — a compare-and-swap fencing token, valid only
  #     while the lock object exists, and therefore still reset by the delete-on-release. That is
  #     correct for fencing and nothing reads it as an order any more.
  #   * a separate LINEAGE counter, seeded from the store at open, fills the position stamp's
  #     `epoch` slot and only ever increases.
  #
  # So this test keeps its original assertion, now as a statement of DESIGN rather than of defect,
  # and the monotonicity it was standing in for is asserted directly by the test below it.
  test "the lock epoch still resets on a clean release — by design, it is a fencing token" do
    id = "occe_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(id, 5_000)
      for e <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> e)
    end)

    {:ok, c1, r1, _path} = Shards.checkout(id)
    first = :sys.get_state(c1).lease.epoch
    Fathom.Shard.checkin(c1, r1)

    mon = Process.monitor(c1)
    :ok = Shards.stop(id)
    assert_receive {:DOWN, ^mon, :process, ^c1, _}, 10_000

    {:ok, c2, r2, _} = Shards.checkout(id)
    second = :sys.get_state(c2).lease.epoch
    Fathom.Shard.checkin(c2, r2)

    assert second == first,
           "the lock epoch changed across a clean release/re-acquire (#{first} -> #{second}). " <>
             "It is a fencing token whose lifetime is the lock object's, and release_lease " <>
             "deletes that object — so this is expected. If it now increases, something changed " <>
             "release_lease, and the lineage test below is the one that matters."

    Shards.stop(id)
  end

  # THE REGRESSION TEST FOR #8, on the exact path the defect lived on.
  #
  # A graceful drop is where the old code was worst: it checkpoints, unlinks the WAL, and
  # `position_after_checkpoint/2` correctly answers `nil` (ambiguous) — so THERE IS NO POSITION
  # STAMP TO CARRY A NUMBER. That is precisely why the lineage is its own object metadata key
  # rather than being read back out of the stamp: a lineage derived from the stamp would find
  # nothing exactly where it is needed most, and fall back to the resetting lock epoch — the same
  # bug, one layer down.
  #
  # Pre-fix this test fails at the final assertion: both cycles stamped lock epoch 1, so the
  # lineage never moved. (Verified by reverting lib/ and re-running — it fails with 1 -> 1.)
  test "the LINEAGE rises across a clean release/re-acquire, where the lock epoch does not" do
    Application.put_env(:fathom, :replication_enabled, true)

    id = "occl_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(id, 5_000)
      for e <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> e)
    end)

    first = ownership_cycle!(id)
    second = ownership_cycle!(id)

    # PRECONDITIONS. Without these the test passes vacuously on a shard that was never flushed:
    # `object_head/1` answers `{:ok, nil}` for an absent object, and nil > nil is not a comparison.
    assert is_integer(first),
           "the first ownership wrote no lineage — nothing was flushed, so this test measured nothing"

    assert is_integer(second),
           "the second ownership wrote no lineage — nothing was flushed, so this test measured nothing"

    assert second > first,
           "the lineage did not advance across a clean release/re-acquire (#{first} -> #{second}). " <>
             "release_lease DELETES the lock, so the next acquire starts at epoch 1 again; if the " <>
             "stamped number tracks that, a replica holding a number from an earlier ownership " <>
             "outranks the object and Promote.fresher?/2 serves the replica over a good database."
  end

  # One full acquire → write → flush → drop → release, returning the lineage the object was left
  # holding. The WriteCounter bump is not decoration: flushes are write-gated, and a shard written
  # through a raw Connection (rather than ShardExecutor) is never marked dirty — so without it the
  # graceful stop skips the upload entirely and both cycles return nil.
  defp ownership_cycle!(id) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)

    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE IF NOT EXISTS t (a)", [])
    for n <- 1..10, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [n])
    Connection.close(conn)

    Fathom.Shard.WriteCounter.bump(id)
    Fathom.Shard.checkin(coordinator, ref)

    mon = Process.monitor(coordinator)
    :ok = Shards.stop(id)
    assert_receive {:DOWN, ^mon, :process, ^coordinator, _}, 10_000

    case Storage.object_head(id) do
      {:ok, %{lineage: lineage}} -> lineage
      {:ok, nil} -> nil
    end
  end
end
