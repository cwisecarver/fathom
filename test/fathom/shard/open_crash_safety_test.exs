defmodule Fathom.Shard.OpenCrashSafetyTest do
  @moduledoc """
  WP-D2: crash-safety and refusal cost of the cold open.

    * expert review 2026-10-13 #4 — a crash between the cold pull's install and the end of the peer
      ask used to leave file + sidecar == object etag, so the next open was WARM and skipped
      recovery (and the responder floor) entirely. A durable `.recovering` marker makes the reopen
      treat the pull as unvetted.
    * expert review 2026-10-13 #22 — `promote_replica` published the promoted bytes BEFORE removing
      the pre-promotion `.db`; a crash between left a stale db + old sidecar against the new object
      etag, which the next open quarantined as a `.forked` copy. The pre-promotion db is now renamed
      ASIDE before the PUT and restored if the PUT fails.
    * expert review 2026-10-13 #11 (negative-cache half) — a floor-refused shard's retries inside
      the under-quorum window re-paid the lease steal, object touch and full pull each time.

  Every test runs in both liveness modes (`acquire_gen` asserted): the open path differs between
  legacy and heartbeat leases.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.PromoteOnOpen
  alias Fathom.Shard.Replication.{Fleet, Follower, Recovery}
  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.ShardExecutor
  alias Fathom.Shards
  alias Fathom.Test.S3EtagStore

  @modes [:legacy, :heartbeat]

  @env [
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :storage_fault,
    :shard_storage,
    S3,
    :shard_idle_ms,
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_recovery_underquorum_window_ms,
    :replication_recovery_refusal_ttl_ms,
    :replication_lineage_wire,
    :replication_ordinal_wire,
    :promote_replica_crash_hook,
    :promote_replica_after_publish_hook,
    :recovery_ask_crash_hook
  ]

  setup do
    id = "opencrash_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "opencrash_#{System.unique_integer([:positive])}")
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
      for f <- Path.wildcard(Fathom.Shard.db_path(id) <> "*"), do: File.rm_rf(f)
    end)

    Recovery.init_table()
    start_supervised!({Follower, name: Follower, port: 0, dir: dir})
    %{id: id, dir: dir}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(id, mode) do
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)

    if mode == :heartbeat,
      do: assert(is_integer(:sys.get_state(pid).acquire_gen)),
      else: assert(is_nil(:sys.get_state(pid).acquire_gen))
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp rows(conn) do
    {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT a FROM t ORDER BY a"))
    Enum.map(rows, fn [a] -> a end)
  end

  defp open_and_read(id, mode) do
    {:ok, conn} = ShardExecutor.open(id)
    assert_mode!(id, mode)
    result = rows(conn)
    :ok = ShardExecutor.close(conn)
    result
  end

  defp flush!(coordinator) do
    send(coordinator, :durability_flush)
    settle(coordinator, 400)
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

  # The stored object holds rows 1..3, the shard 1..10, and a follower replica (strictly fresher
  # than the object) holds all ten. Leaves the primary dead and its disk gone: the state of a
  # survivor the LB routed a failover to.
  defp failover_fixture!(id, dir) do
    Application.put_env(:fathom, :replication_promote_on_open, true)
    {:ok, coordinator} = Shards.ensure(id)
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER PRIMARY KEY)"))

    for i <- 1..10 do
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [i]))

      if i == 3 do
        assign_ordinal!(id, coordinator)
        flush!(coordinator)
      end
    end

    assert {:ok, stamp} = Storage.object_position(id)
    refute is_nil(stamp), "no stamp, so the comparison could not run and this proves nothing"

    path = Fathom.Shard.db_path(id)
    File.cp!(path, Follower.db_path(Follower, id))

    case File.stat(path <> "-wal") do
      {:ok, %{size: size}} when size > 0 ->
        File.cp!(path <> "-wal", Follower.wal_path(Follower, id))

      _ ->
        File.write!(Follower.wal_path(Follower, id), "")
    end

    Follower.seed(Follower, id, 1, stamp.wal_gen, 0, stamp.offset + 1, stamp.epoch)
    :ok = Follower.note_ordinal(Follower, id, stamp.wal_ordinal)

    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 2_000

    _ =
      try do
        ShardExecutor.close(conn)
      catch
        :exit, _ -> :ok
      end

    for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)

    stored = Path.join(dir, "stored.db")
    assert {:ok, _} = Storage.pull(id, stored)
    {:ok, sconn} = Connection.open(stored)
    {:ok, %{rows: srows}} = Connection.query(sconn, "SELECT a FROM t ORDER BY a", [])
    Connection.close(sconn)
    assert Enum.map(srows, fn [a] -> a end) == [1, 2, 3], "precondition: the object is short"
    :ok
  end

  # The #22 tests exercise the LOCAL promote path (`:replication_recover_from_peers` off): the
  # fleet path writes the `.recovering` marker, whose reopen purge would hide a stale pulled db
  # whether or not it was moved aside, so only this path discriminates the aside fix.
  defp local_path! do
    Application.put_env(:fathom, :replication_recover_from_peers, false)
  end

  defp assign_ordinal!(id, coordinator) do
    wal = Fathom.Shard.db_path(id) <> "-wal"
    assert {:ok, %{salt1: salt}} = Fathom.Shard.Replication.Wal.read(wal)
    n = Fathom.Shard.wal_ordinal(coordinator, salt)
    assert n > 0, "fixture: no ordinal assigned, so the stamp could not be ranked"
    n
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      # Symptom (#4): a node restart during the peer ask left a MATCHING file + sidecar; the
      # reopen was warm and served the short object without recovering. The precondition asserts
      # that state exists (so the test cannot pass for a vacuous reason), and the invariant is
      # that the reopen RE-RUNS recovery: it serves the fresher replica's rows.
      test "a crash between install and the peer ask re-runs recovery on reopen (#4)", %{
        id: id,
        dir: dir
      } do
        failover_fixture!(id, dir)
        path = Fathom.Shard.db_path(id)
        test_pid = self()

        Application.put_env(:fathom, :recovery_ask_crash_hook, fn p ->
          send(test_pid, {:at_ask, File.exists?(PromoteOnOpen.recovering_path(p))})
          raise "node died during the ask"
        end)

        assert {:error, _} = ShardExecutor.open(id), "precondition: the crashed open failed"
        assert_receive {:at_ask, true}, 1_000, "the marker must be durable BEFORE the ask"

        assert File.exists?(path), "precondition: the pull is installed at the live path"
        assert File.exists?(Fathom.Shard.Provenance.sidecar_path(path))
        assert File.exists?(PromoteOnOpen.recovering_path(path))

        Application.delete_env(:fathom, :recovery_ask_crash_hook)

        assert open_and_read(id, unquote(mode)) == Enum.to_list(1..10),
               "the reopen skipped recovery: it opened the installed pull warm and served the " <>
                 "short object (expert review 2026-10-13 #4)"

        assert Path.wildcard(path <> ".forked.*") == [],
               "purging the unvetted pull must not look like a fork"

        refute File.exists?(PromoteOnOpen.recovering_path(path)), "the marker is cleared"
      end

      # Symptom (#22): a crash between the publish and the rename left the pulled db + old
      # sidecar against the new object etag -> `.forked` quarantine + alarm.
      test "a crash right after the promoted object is published leaves no .forked (#22)", %{
        id: id,
        dir: dir
      } do
        failover_fixture!(id, dir)
        local_path!()
        path = Fathom.Shard.db_path(id)
        test_pid = self()

        Application.put_env(:fathom, :promote_replica_after_publish_hook, fn p ->
          send(test_pid, {:published, File.exists?(p), File.exists?(p <> ".prepromote")})
          raise "node died after the PUT"
        end)

        assert {:error, _} = ShardExecutor.open(id), "precondition: the crashed open failed"

        assert_receive {:published, false, true},
                       1_000,
                       "at the crash the pre-promotion db must already be aside, not live"

        Application.delete_env(:fathom, :promote_replica_after_publish_hook)

        assert open_and_read(id, unquote(mode)) == Enum.to_list(1..10)

        assert Path.wildcard(path <> ".forked.*") == [],
               "the next open quarantined a fork: the pulled db was still live against the " <>
                 "promoted object's etag (expert review 2026-10-13 #22)"
      end

      test "a crash between the sidecar write and the rename leaves no .forked (#22)", %{
        id: id,
        dir: dir
      } do
        failover_fixture!(id, dir)
        local_path!()
        path = Fathom.Shard.db_path(id)

        Application.put_env(:fathom, :promote_replica_crash_hook, fn _ -> raise "node died" end)
        assert {:error, _} = ShardExecutor.open(id)
        Application.delete_env(:fathom, :promote_replica_crash_hook)

        assert open_and_read(id, unquote(mode)) == Enum.to_list(1..10)
        assert Path.wildcard(path <> ".forked.*") == []
      end

      # The fallback must still work: a failed promotion PUT serves the PULLED object, so the
      # aside copy has to come back (it is the only local copy of those bytes).
      test "a failed promotion PUT still serves the pulled object and leaves no aside (#22)", %{
        id: id,
        dir: dir
      } do
        Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
        failover_fixture!(id, dir)
        local_path!()
        path = Fathom.Shard.db_path(id)

        Application.put_env(:fathom, :storage_fault, :flush)
        # The pre-promotion snapshot is best-effort, so the fault (flush only) fails the PUT.
        assert open_and_read(id, unquote(mode)) == [1, 2, 3]
        Application.delete_env(:fathom, :storage_fault)

        refute File.exists?(path <> ".prepromote"), "the aside copy must be restored, not leaked"
        assert Path.wildcard(path <> ".forked.*") == []

        refute File.exists?(PromoteOnOpen.recovering_path(path)),
               "the decision cleared the marker"
      end
    end
  end

  test "the TempReaper sweeps a leaked .prepromote aside copy (#22)" do
    assert Storage.temp_orphan?("s.db.prepromote")
    assert Storage.temp_orphan?("s.db.prepromote-wal")
  end

  describe "recent_refusal/2 (the negative cache's decision)" do
    @table Fathom.Shard.Replication.Recovery.UnderQuorumWindow

    setup do
      Application.put_env(:fathom, :replication_recovery_underquorum_window_ms, 60_000)
      Application.put_env(:fathom, :replication_recovery_refusal_ttl_ms, 3_000)
      Recovery.init_table()
    end

    test "fresh row refuses; stale row, expired window, tombstone and no row fall through" do
      :ets.insert(@table, {"nc_shard", 1_000, 5_000})

      assert {:refuse, %Recovery.UnderQuorum{shard_id: "nc_shard"}} =
               Recovery.recent_refusal("nc_shard", 6_000)

      assert nil == Recovery.recent_refusal("nc_shard", 8_000), "last refusal older than the TTL"

      :ets.insert(@table, {"nc_shard", 1_000, 60_500})

      assert nil == Recovery.recent_refusal("nc_shard", 61_000),
             "window expired: let :expired run"

      :ets.insert(@table, {"nc_shard", :expired, 1_000})
      assert nil == Recovery.recent_refusal("nc_shard", 1_100), "a tombstone is not a refusal"

      :ets.delete(@table, "nc_shard")
      assert nil == Recovery.recent_refusal("nc_shard", 1_100)
    end
  end

  for mode <- @modes do
    describe "negative cache, #{mode} mode" do
      setup do: set_mode!(unquote(mode))

      # Symptom (#11): every 503 retry re-stole the lease, re-touched and re-pulled the object.
      test "a refused shard's retry inside the window makes zero Storage calls", %{id: id} do
        test_pid = self()
        base = build_db!(["base"])
        marker = Storage.encode_lease(Storage.released_marker(%{epoch: 3}, 5))

        store =
          start_supervised!(
            {Agent, fn -> S3EtagStore.initial(%{"#{id}.db" => base, "#{id}.lock" => marker}) end}
          )

        Agent.update(
          store,
          &put_in(&1, [:objects, "#{id}.db", :meta], %{"x-amz-meta-fathom-pos" => "2:1:100:5"})
        )

        Application.put_env(:fathom, S3,
          bucket: "b",
          region: "us-east-1",
          access_key_id: "k",
          secret_access_key: "s",
          endpoint: "https://s3.example",
          path_style: true,
          req_plug: fn conn ->
            send(test_pid, :s3_call)
            S3EtagStore.serve(conn, store)
          end
        )

        Application.put_env(:fathom, :shard_storage, S3)
        Application.put_env(:fathom, :shard_idle_ms, 600_000)
        Application.put_env(:fathom, :replication_enabled, true)
        Application.put_env(:fathom, :replication_promote_on_open, true)
        Application.put_env(:fathom, :replication_recover_from_peers, true)
        Application.put_env(:fathom, :replication_quorum, 2)
        Application.put_env(:fathom, :replication_recovery_underquorum_window_ms, 600_000)
        Application.put_env(:fathom, :replication_recovery_refusal_ttl_ms, 600_000)
        Application.put_env(:fathom, :replication_lineage_wire, true)
        Application.put_env(:fathom, :replication_ordinal_wire, true)

        Application.put_env(
          :fathom,
          :replication_followers,
          for(n <- 1..4, do: {"dead#{n}", "127.0.0.1", n})
        )

        Fleet.publish([])

        # The first checkout is a real attempt and is refused by the floor.
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, {:open_failed, %Recovery.UnderQuorum{}}} = Shards.checkout(id)
        end)

        assert_received :s3_call, "the first attempt must have reached the object store"
        flush_messages()

        # The retry is answered from the cache: no coordinator, no lock write, no pull.
        assert {:error, {:open_failed, %Recovery.UnderQuorum{shard_id: ^id}}} =
                 Shards.checkout(id)

        refute_received :s3_call

        # The error maps to the same retryable 503 the real refusal does.
        assert %{status: 503, code: "FILO_RECOVERING"} =
                 ShardExecutor.open_error(
                   {:open_failed, %Recovery.UnderQuorum{shard_id: id, answered: 0, needed: 3}}
                 )

        # A bare ensure/1 is not a request and still tries (and is refused by the floor itself).
        Application.put_env(:fathom, :replication_recovery_refusal_ttl_ms, 0)

        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, {:open_failed, %Recovery.UnderQuorum{}}} = Shards.checkout(id)
        end)

        assert_received :s3_call, "past the TTL the attempt goes through to Storage again"
      end
    end
  end

  defp flush_messages do
    receive do
      :s3_call -> flush_messages()
    after
      0 -> :ok
    end
  end

  defp build_db!(rows) do
    path = Path.join(System.tmp_dir!(), "oc_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end
end
