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
        [{fname, _} | _] = followers
        replica = Follower.state_of(fname, id)
        assert replica, "the follower never recorded a position; nothing was actually shipped"

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

  # CHARACTERIZATION — this pins behaviour that is WRONG and is deliberately not yet fixed.
  #
  # `release_lease` DELETES the lock object, so the next `acquire_lease` takes the optimistic
  # create path and starts again at epoch 1. The epoch therefore climbs on crash-steals and RESETS
  # on every clean idle-drop, drain and rebalance handoff — while `Storage.position`'s consumers
  # treat it as the high-order component of a total order.
  #
  # That is expert review #8, which is PARKED: every candidate fix trades against something
  # load-bearing (an S3 round trip on the gated `cold_open_p50_us`, or the storage contract), and
  # the choice is a design decision rather than a bug fix. The typedoc has been corrected to state
  # the real property; this test states it too, so the next reader does not discover it by having
  # a replica promoted over a good object.
  #
  # WHEN #8 LANDS, THIS ASSERTION INVERTS. That is the point of writing it down.
  test "the lease epoch RESETS on a clean release — the ordering assumption is currently false" do
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
           "the lease epoch changed across a clean release/re-acquire (#{first} -> #{second}). " <>
             "If it now INCREASES, expert review #8 has landed and this characterization should " <>
             "become the monotonicity assertion it is standing in for. If it DECREASED, " <>
             "something else is wrong."

    Shards.stop(id)
  end
end
