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
  alias Fathom.Shard.Replication.Recovery.UnderQuorum
  alias Fathom.Shard.Storage
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  @env [
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :replication_followers,
    :replication_quorum,
    :replication_recovery_underquorum_window_ms,
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
    assert :ets.lookup(Recovery.UnderQuorumWindow, id) == []
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

  test "with no peers configured, the open path is unchanged", %{id: id} do
    build_and_lose_primary(id)
    attach_telemetry(id)
    peers!([])

    assert read_rows(id) == [1, 2, 3]
    refute_received {:underquorum, _, _}
  end
end
