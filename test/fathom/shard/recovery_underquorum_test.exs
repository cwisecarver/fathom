defmodule Fathom.Shard.RecoveryUnderquorumTest do
  @moduledoc """
  The peer-recovery responder floor (expert review 2026-10-12 #5).

  A write is acked once Q of N followers hold it, so only a read of `N - Q + 1` of them is sure to
  see it. Recovery used to decide on whatever subset answered: with two of four peers silent it
  returned `:none`, the cold open served the stored object, the first flush stamped a higher
  lineage, and the replicas holding the quorum-acked tail were reseeded away — acked writes lost
  with no client-visible signal.

  The fix (user decision Q1 = A): below the floor, refuse the open with a retryable 503 for a
  bounded per-shard window; after the window, open best-effort with an error log and a
  `[:fathom, :replication, :recovery_underquorum]` event.

  The fleet here is N = 4 configured peers with Q = 2, so 3 definite answers are needed and this
  node's own replica table is one of them. "Dead" peers are ports nothing listens on (an instant
  refused connect — silent, and never cached as unreachable); "live" peers are real `Follower`s
  that hold nothing for the shard and say so, which is a DEFINITE answer.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.PromoteOnOpen
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Recovery.UnderQuorum
  alias Fathom.Shard.Storage
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  @env [
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :replication_followers,
    :replication_recovery_peers,
    :replication_quorum,
    :replication_recovery_underquorum_window_ms,
    :replication_recovery_refusal_ttl_ms,
    :replication_lineage_wire,
    :replication_ordinal_wire
  ]

  setup do
    id = "underq_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "underq_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    prev_running = Fleet.running()

    on_exit(fn ->
      Shards.stop(id)
      Fleet.publish(prev_running)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(dir)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    Recovery.init_table()

    # The default-named follower, because that is the one a coordinator looks for.
    start_supervised!({Follower, name: Follower, port: 0, dir: dir})

    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.put_env(:fathom, :replication_recover_from_peers, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_recovery_underquorum_window_ms, 60_000)
    # These tests retry a refusal to watch each attempt reach the floor, so the checkout negative
    # cache (expert review 2026-10-13 #11, `Recovery.recent_refusal/2`) is off here; it is covered
    # in `open_crash_safety_test.exs`.
    Application.put_env(:fathom, :replication_recovery_refusal_ttl_ms, 0)
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)

    # The peer set comes from config, never from a shipper set an earlier test left published.
    Fleet.publish([])

    %{id: id}
  end

  # -- fixtures ------------------------------------------------------------------------------

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp dead(n), do: {"dead#{n}", "127.0.0.1", n}

  defp live do
    root = Path.join(System.tmp_dir!(), "underq_peer_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)
    name = :"underq_peer_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: root}, id: name)
    {:ok, port} = Follower.port(pid)
    {"live_#{name}", "127.0.0.1", port}
  end

  defp peers!(peers), do: Application.put_env(:fathom, :replication_followers, peers)

  # A real peer whose replica of `id` is TORN: seeded, then fenced for a promotion, which is the
  # same `torn: true` row a checkpoint-reset leaves behind.
  defp torn_peer(id) do
    root = Path.join(System.tmp_dir!(), "underq_torn_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)
    name = :"underq_torn_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: root}, id: name)
    {:ok, port} = Follower.port(pid)
    :ok = Follower.seed(name, id, 1, 0, 0, 100, 3)
    :ok = Follower.note_ordinal(name, id, 5)
    assert :ok = Follower.fence_for_promotion(name, id, [])
    assert %{torn: true} = Follower.state_of(name, id), "fixture: the peer is not torn"
    {"torn_#{name}", "127.0.0.1", port}
  end

  # Three rows, flushed with an ordinal so the stored object's stamp is RANKABLE — the floor only
  # applies against a stamp a replica could be promoted over. Then node loss: coordinator killed,
  # local disk gone, so the next open is a cold failover open.
  defp build_and_lose_primary(id) do
    {:ok, coordinator} = Shards.ensure(id)
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER PRIMARY KEY)"))

    for i <- 1..3,
        do: {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [i]))

    wal = Fathom.Shard.db_path(id) <> "-wal"
    assert {:ok, %{salt1: salt}} = Fathom.Shard.Replication.Wal.read(wal)
    assert Fathom.Shard.wal_ordinal(coordinator, salt) > 0
    send(coordinator, :durability_flush)
    settle(coordinator, 400)

    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 2_000

    _ =
      try do
        ShardExecutor.close(conn)
      catch
        :exit, _ -> :ok
      end

    for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)

    # Precondition: the object's stamp is rankable, or the floor is (correctly) off and every
    # assertion below would be about nothing.
    assert {:ok, %{epoch: e, wal_ordinal: n}} = Storage.object_position(id)
    assert e > 0 and n > 0, "fixture: the stored object is not rankable"
  end

  defp settle(_c, 0), do: flunk("durability flush task never settled")

  defp settle(c, tries) do
    if :sys.get_state(c).flush_task == nil do
      :ok
    else
      Process.sleep(10)
      settle(c, tries - 1)
    end
  end

  defp read_rows(id) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT a FROM t ORDER BY a"))
    :ok = ShardExecutor.close(conn)
    Enum.map(rows, fn [a] -> a end)
  end

  defp attach_telemetry(id) do
    test = self()
    handler = "underq-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :replication, :recovery_underquorum],
      fn _event, measurements, %{shard_id: ^id} = meta, _ ->
        send(test, {:underquorum, meta.outcome, measurements})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  # -- pure ----------------------------------------------------------------------------------

  describe "answers_needed/3" do
    @stamp %{epoch: 3, wal_gen: 0, wal_ordinal: 2, offset: 0}

    test "is N - Q + 1 against a rankable stamp" do
      peers = for n <- 1..4, do: dead(n)
      assert Recovery.answers_needed(peers, @stamp, quorum: 2) == 3
      assert Recovery.answers_needed(peers, @stamp, quorum: 3) == 2
    end

    test "is 0 when the stamp cannot be ranked — a new shard is never refused" do
      peers = for n <- 1..4, do: dead(n)

      for stamp <- [nil, %{epoch: 0, wal_ordinal: 2}, %{epoch: 3, wal_ordinal: 0}, %{epoch: 3}] do
        assert Recovery.answers_needed(peers, stamp, quorum: 2) == 0
      end
    end

    test "with no peers, or Q >= N, this node's own answer meets the floor" do
      assert Recovery.answers_needed([], @stamp, quorum: 2) <= 1
      assert Recovery.answers_needed([dead(1)], @stamp, quorum: 2) <= 1
      assert Recovery.answers_needed([dead(1), dead(2)], @stamp, quorum: 2) <= 1
    end
  end

  describe "window_step/3 — the window is anchored at the first refusal" do
    test "the first refusal starts the clock" do
      assert {:wait, 0, {1_000, 1_000}} = Recovery.window_step(nil, 1_000, 60_000)
    end

    test "retries do not reset it, and it expires at the window" do
      assert {:wait, 59_999, {1_000, 60_999}} =
               Recovery.window_step({1_000, 59_000}, 60_999, 60_000)

      assert {:expired, 60_000} = Recovery.window_step({1_000, 60_999}, 61_000, 60_000)
    end

    test "a row untouched for two windows is a past episode, and the clock restarts" do
      assert {:wait, 0, {500_000, 500_000}} =
               Recovery.window_step({1_000, 2_000}, 500_000, 60_000)
    end

    test "a zero window never refuses" do
      assert {:expired, 0} = Recovery.window_step(nil, 1_000, 0)
    end
  end

  describe "window_step/3 — the expiry tombstone (expert review 2026-10-13 #40)" do
    test "while the tombstone stands, a retry is expired again, not a fresh refusal" do
      assert {:expired, 10_000} = Recovery.window_step({:expired, 1_000}, 11_000, 60_000)
      assert {:expired, 60_000} = Recovery.window_step({:expired, 1_000}, 61_000, 60_000)
    end

    test "one window after the expiry it is a past episode and a new failover starts a window" do
      assert {:wait, 0, {62_001, 62_001}} =
               Recovery.window_step({:expired, 1_000}, 62_001, 60_000)
    end
  end

  describe "sweep/1 — rows are reclaimed (expert review 2026-10-13 #40)" do
    test "rows untouched for two windows go, live ones stay, tombstones follow the same rule" do
      table = Recovery.UnderQuorumWindow
      :ets.delete_all_objects(table)
      now = 10_000_000

      for n <- 1..1000, do: :ets.insert(table, {"old_#{n}", now - 500_000, now - 400_000})
      :ets.insert(table, {"old_tomb", :expired, now - 400_000})
      :ets.insert(table, {"live", now - 30_000, now - 1_000})
      :ets.insert(table, {"live_tomb", :expired, now - 30_000})

      assert Recovery.sweep(now) >= 1001
      assert :ets.info(table, :size) == 2
      assert [{"live", _, _}] = :ets.lookup(table, "live")
      assert [{"live_tomb", :expired, _}] = :ets.lookup(table, "live_tomb")
      :ets.delete_all_objects(table)
    end

    test "a refusal that inserts a new row sweeps once the table has grown" do
      table = Recovery.UnderQuorumWindow
      :ets.delete_all_objects(table)
      now = System.monotonic_time(:millisecond)
      for n <- 1..300, do: :ets.insert(table, {"stale_#{n}", now - 900_000, now - 900_000})

      peers = [dead(1), dead(2), dead(3), dead(4)]
      assert {:refuse, _} = Recovery.best_replica("sweeper", @stamp, peers: peers, quorum: 2)

      assert :ets.info(table, :size) == 1
      :ets.delete_all_objects(table)
    end

    test "a failed best-effort open is not re-refused", %{id: id} do
      peers = [dead(1), dead(2), dead(3), dead(4)]
      assert {:refuse, _} = Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)

      [{^id, first, last}] = :ets.lookup(Recovery.UnderQuorumWindow, id)
      :ets.insert(Recovery.UnderQuorumWindow, {id, first - 60_001, last})

      # The window expires and the open proceeds best-effort ...
      assert :none = Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
      # ... and suppose that open FAILED: the retry must not start a fresh 60 s refusal.
      assert :none = Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
    end
  end

  describe "previous_owner?/2" do
    # Expert review 2026-10-15 #17: the owner carries NODE_KEY, so the match is exact. The old
    # host-part-of-name@host leniency (which asserted "fathom@fathom1#.." matches "fathom1") only
    # existed to paper over the owner carrying node(); it is gone, and a lookalike must not match.
    test "matches the node key before the incarnation exactly" do
      assert Recovery.previous_owner?("fathom1#ab12", "fathom1")
      refute Recovery.previous_owner?("fathom@fathom1#ab12", "fathom1")
      refute Recovery.previous_owner?("fathom1#ab12", "fathom2")
      refute Recovery.previous_owner?("fathom10#ab12", "fathom1")
      refute Recovery.previous_owner?(nil, "fathom1")
      refute Recovery.previous_owner?("#ab12", "")
    end
  end

  # -- best_replica/3 ------------------------------------------------------------------------

  describe "best_replica/3 counts definite answers" do
    @stamp %{epoch: 3, wal_gen: 0, wal_ordinal: 2, offset: 0}

    test "two silent peers of four, Q=2, is refused rather than read as :none", %{id: id} do
      peers = [live(), dead(1), dead(2), dead(3)]

      assert {:refuse, %UnderQuorum{answered: 2, needed: 3, waited_ms: 0}} =
               Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
    end

    test "peers that definitely hold NOTHING count, so N-Q+1 answers proceed", %{id: id} do
      peers = [live(), live(), dead(1), dead(2)]
      assert :none = Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
    end

    test "a local replica ahead of the object does not skip the floor", %{id: id} do
      # Ahead of @stamp. Under the floor this node's replica may still be BEHIND a silent peer
      # that acked writes it never received, so it must not short-circuit the question.
      Follower.seed(Follower, id, 1, 0, 0, 100, 3)
      :ok = Follower.note_ordinal(Follower, id, 5)

      peers = [dead(1), dead(2), dead(3), dead(4)]

      assert {:refuse, %UnderQuorum{answered: 1, needed: 3}} =
               Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
    end

    test "an unrankable stamp is never refused", %{id: id} do
      peers = [dead(1), dead(2), dead(3), dead(4)]
      assert :none = Recovery.best_replica(id, nil, peers: peers, quorum: 2)
    end

    # SYMPTOM (expert review 2026-10-13 #5): a TORN follower answered "I hold nothing", which the
    # floor counted as DEFINITE. N=4, Q=2 needs 3 answers; this node plus two torn peers made 3, so
    # `choose` returned :none and the shard opened from the object while the torn peers held the
    # acked tail. A torn peer must count toward neither the floor nor the offers.
    test "torn peers are silent: they do not satisfy the floor", %{id: id} do
      peers = [torn_peer(id), torn_peer(id), dead(1), dead(2)]

      assert {:refuse, %UnderQuorum{answered: 1, needed: 3}} =
               Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
    end

    test "a peer holding nothing still counts, a torn one next to it does not", %{id: id} do
      peers = [live(), torn_peer(id), dead(1), dead(2)]

      assert {:refuse, %UnderQuorum{answered: 2, needed: 3}} =
               Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
    end

    # SYMPTOM (expert review 2026-10-13 #14): the node whose lease we stole is in `peers/0`, but it
    # is not one of the writer's followers. Its "nothing" must not stand in for a follower.
    test "the previous owner's answer does not count toward the floor", %{id: id} do
      {p_key, _, _} = p = live()
      peers = [p, live(), dead(1), dead(2)]

      # Control: without naming the previous owner the floor is met (own + P + A = 3).
      assert :none = Recovery.best_replica(id, @stamp, peers: peers, quorum: 2)
      Recovery.init_table()
      :ets.delete_all_objects(Recovery.UnderQuorumWindow)

      assert {:refuse, %UnderQuorum{answered: 2, needed: 3}} =
               Recovery.best_replica(id, @stamp,
                 peers: peers,
                 quorum: 2,
                 previous_owner: p_key <> "#a1b2c3d4"
               )
    end
  end

  # -- a takeover's own followers are not the dead owner's (Jepsen 2026-10-27) -----------------
  #
  # SYMPTOM: on the chaos rig each node ships to the NEXT THREE of five (a ring). fathom5 acked four
  # appends, then fathom3, fathom1 and fathom5 were killed together. fathom3 took the shard over and
  # asked ITS OWN followers (4, 5, 1): fathom5 was the dead owner (excluded), fathom1 was still
  # booting (silent), fathom4 said "I hold nothing". With itself that is two definite answers, and
  # the floor over three peers with Q=2 is two, so recovery returned `:none` and the shard opened
  # from the stale S3 object. The acked tail was on fathom5's followers (1, 2, 3); fathom2 held it
  # and was never asked. Four acked writes lost, no refusal, no error.
  #
  # The floor `N - Q + 1` only guarantees a holder is read when N is the dead owner's follower set.
  # A node cannot know that from its own config, so `:replication_recovery_peers` names every other
  # node. These tests pin the decision, not the lease: it is the same in `:legacy` and `:heartbeat`
  # liveness (nothing here touches a lease), so they are not parameterized over the two modes.
  describe "recovery peers (REPLICATION_RECOVERY_PEERS)" do
    @stamp %{epoch: 3, wal_gen: 0, wal_ordinal: 2, offset: 0}

    test "the floor is counted over the recovery peers, so a down holder refuses the open", %{
      id: id
    } do
      # The same cluster state with the holder DOWN. Over the ring's own followers [live, dead,
      # dead] two answers (this node, the live peer) meet the floor of 2 and the open proceeds from
      # the object. Over every other node it needs 3, and 2 is not enough.
      peers!([live(), dead(1), dead(2)])

      Application.put_env(:fathom, :replication_recovery_peers, [
        live(),
        dead(3),
        dead(4),
        dead(5)
      ])

      assert {:refuse, %UnderQuorum{answered: 2, needed: 3}} =
               Recovery.best_replica(id, @stamp, quorum: 2)
    end

    test "unset, recovery asks this node's own followers, as before", %{id: id} do
      peers = [live(), dead(1), dead(2)]
      peers!(peers)
      Application.delete_env(:fathom, :replication_recovery_peers)

      assert Recovery.peers() == peers
      assert :none = Recovery.best_replica(id, @stamp, quorum: 2)
    end
  end

  # -- internal errors go through the window (expert review 2026-10-13 #15) ---------------------

  describe "an internal crash does not fail open" do
    @describetag :capture_log

    test "refuses inside the window, best-effort after it, with the telemetry", %{id: id} do
      attach_telemetry(id)
      peers = [live(), live(), dead(1), dead(2)]
      # `quorum: :bad` makes the decision crash (ArithmeticError) before any answer is read.
      opts = [peers: peers, quorum: :bad]

      assert {:refuse, %UnderQuorum{shard_id: ^id}} = Recovery.best_replica(id, @stamp, opts)
      assert_receive {:underquorum, :skipped_error, _}

      [{^id, first, last}] = :ets.lookup(Recovery.UnderQuorumWindow, id)
      :ets.insert(Recovery.UnderQuorumWindow, {id, first - 60_001, last})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :none = Recovery.best_replica(id, @stamp, opts)
        end)

      assert log =~ "opening best-effort"
      assert_receive {:underquorum, :opened_best_effort, _}
    end

    test "with no peers there is no floor to apply", %{id: id} do
      assert :none = Recovery.error_window(id, :boom, peers: [], quorum: 2)
    end
  end

  describe "a failed decision HEAD does not fail open (try_promote_from_fleet)" do
    setup %{id: id} do
      prev_storage = Application.get_env(:fathom, :shard_storage)
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
      Application.put_env(:fathom, :storage_fault, :object_head)

      on_exit(fn ->
        Application.delete_env(:fathom, :storage_fault)

        if prev_storage,
          do: Application.put_env(:fathom, :shard_storage, prev_storage),
          else: Application.delete_env(:fathom, :shard_storage)
      end)

      path = Path.join(System.tmp_dir!(), "headerr_#{id}.db")
      File.write!(path, "stand-in for the cold-pulled object")
      on_exit(fn -> for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s) end)
      %{path: path}
    end

    test "refuses inside the window (and drops the cold pull), best-effort after", %{
      id: id,
      path: path
    } do
      attach_telemetry(id)
      peers!([live(), dead(1), dead(2), dead(3)])
      lease = %{owner: "me#1", epoch: 2, expires_at_ms: 0}

      call = fn ->
        PromoteOnOpen.maybe_promote_replica(id, path, lease, "etag0", 5, false, nil)
      end

      assert_raise UnderQuorum, call
      assert_receive {:underquorum, :skipped_error, _}
      refute File.exists?(path), "the cold-pulled copy must go, or the retry opens warm"

      File.write!(path, "x")
      [{^id, first, last}] = :ets.lookup(Recovery.UnderQuorumWindow, id)
      :ets.insert(Recovery.UnderQuorumWindow, {id, first - 60_001, last})

      log = ExUnit.CaptureLog.capture_log(fn -> assert "etag0" = call.() end)
      assert log =~ "opening best-effort"
      assert_receive {:underquorum, :opened_best_effort, _}
    end

    test "with no peers the HEAD error leaves the open unchanged", %{id: id, path: path} do
      peers!([])
      lease = %{owner: "me#1", epoch: 2, expires_at_ms: 0}

      assert "etag0" =
               PromoteOnOpen.maybe_promote_replica(id, path, lease, "etag0", 5, false, nil)
    end
  end

  # -- through the real open path ------------------------------------------------------------

  test "an under-quorum failover open is a retryable 503, and holds nothing", %{id: id} do
    build_and_lose_primary(id)
    attach_telemetry(id)
    peers!([live(), dead(1), dead(2), dead(3)])

    for _attempt <- 1..2 do
      assert {:error, %Filo.Error{status: 503, code: "FILO_RECOVERING"}} = ShardExecutor.open(id)
      assert_receive {:underquorum, :refused, %{answered: 2, needed: 3}}, 1_000
    end

    # Nothing is held for the window: no coordinator, no lease, no local copy. The deleted copy is
    # load-bearing — left with its provenance sidecar, the retry would open WARM, and a warm open
    # never asks the fleet, so it would skip the window and serve the object on the second try.
    assert {:ok, lease} = Storage.acquire_lease(id, "underq-probe", 30_000),
           "the refused open left the lease held"

    :ok = Storage.release_lease(id, lease)
    refute File.exists?(Fathom.Shard.db_path(id))
    refute File.exists?(Fathom.Shard.db_path(id) <> ".etag")

    # And the window kept its first anchor across the retries.
    assert [{^id, first, last}] = :ets.lookup(Recovery.UnderQuorumWindow, id)
    assert first <= last
  end

  test "once the window has elapsed the open proceeds from the object, loudly", %{id: id} do
    build_and_lose_primary(id)
    attach_telemetry(id)
    peers!([live(), dead(1), dead(2), dead(3)])

    assert {:error, %Filo.Error{status: 503}} = ShardExecutor.open(id)
    assert_receive {:underquorum, :refused, _}, 1_000

    # Age the episode past the window rather than sleep through it.
    [{^id, first, last}] = :ets.lookup(Recovery.UnderQuorumWindow, id)
    :ets.insert(Recovery.UnderQuorumWindow, {id, first - 60_001, last})

    assert read_rows(id) == [1, 2, 3]
    assert_receive {:underquorum, :opened_best_effort, %{answered: 2, needed: 3, waited_ms: w}}
    assert w >= 60_000

    # The row is now a TOMBSTONE, not gone (expert review 2026-10-13 #40). This used to assert `== []`:
    # that pinned the defect, because deleting the row before the best-effort open meant a failed
    # open's retry started a fresh 60 s refusal.
    assert [{^id, :expired, _at}] = :ets.lookup(Recovery.UnderQuorumWindow, id)
  end

  test "enough definite answers — 'nothing' included — open on the first try", %{id: id} do
    build_and_lose_primary(id)
    attach_telemetry(id)
    peers!([live(), live(), dead(1), dead(2)])

    assert read_rows(id) == [1, 2, 3]
    refute_received {:underquorum, _, _}
  end

  test "with peer recovery off, the open path is unchanged", %{id: id} do
    build_and_lose_primary(id)
    attach_telemetry(id)
    peers!([dead(1), dead(2), dead(3), dead(4)])
    Application.put_env(:fathom, :replication_recover_from_peers, false)

    assert read_rows(id) == [1, 2, 3]
    refute_received {:underquorum, _, _}
  end

  describe "the torn position reply (expert review 2026-10-13 #5)" do
    test "round-trips, and is distinct from 'nothing' and from a position" do
      torn = IO.iodata_to_binary(Protocol.encode_position("acme", :torn))
      nothing = IO.iodata_to_binary(Protocol.encode_position("acme", nil))
      assert {:ok, {:position, "acme", :torn}} = Protocol.decode(torn)
      assert {:ok, {:position, "acme", nil}} = Protocol.decode(nothing)
      assert torn != nothing
    end

    test "an old-style querier reads it as a non-position frame, which Recovery maps to silent" do
      # An old decoder has no clause for the new code: it falls to `{:error, :malformed}`, and
      # `Recovery.query/3` maps every non-position reply to :silent. Simulated with an unknown code.
      unknown = <<2::8, 99::8, 4::16, "acme">>
      assert {:error, _} = Protocol.decode(IO.iodata_to_binary(unknown))
    end

    test "old-style replies still read" do
      pos = %{epoch: 3, wal_gen: 1, salt1: 7, next_offset: 12, wal_ordinal: 4}
      frame = IO.iodata_to_binary(Protocol.encode_position("acme", pos))

      assert {:ok, {:position, "acme", %{wal_ordinal: 4, next_offset: 12}}} =
               Protocol.decode(frame)
    end
  end

  test "with no peers configured, the open path is unchanged", %{id: id} do
    build_and_lose_primary(id)
    attach_telemetry(id)
    peers!([])

    assert read_rows(id) == [1, 2, 3]
    refute_received {:underquorum, _, _}
  end
end
