defmodule Fathom.Shard.RecoveryPrefetchTest do
  @moduledoc """
  The fleet recovery ask of a replicating cold open runs DURING the pull (expert review 2026-10-17
  #14 step 2).

  `PromoteOnOpen.recover_with_marker/7` used to fsync the `.recovering` marker and ask the fleet only
  after the pull was installed, though both need nothing but the post-lease head's stamp. The
  coordinator's open-head task now hands a rankable head straight to
  `PromoteOnOpen.prefetch_recovery/5`, and the decision consumes the answer.

    * **Overlap** — with a slow pull, the prefetch starts while the live path is still empty (the
      pull not yet installed). Fails against the pre-change code, which had no prefetch at all.
    * **Hygiene** — a prefetch the decision does NOT consume (the gate changed in between) still
      leaves no marker once the open serves: a marker beside a served, written file would make the
      next open purge acked writes. Fails without `finish_prefetch/2`.

  The open is a FRESH create (the previous coordinator released cleanly), the only lease kind that
  takes the open head; both liveness modes, mode asserted.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Heartbeat, PromoteOnOpen, Storage}
  alias Fathom.Shard.Replication.{Fleet, Follower, Recovery}
  alias Fathom.{ShardExecutor, Shards}

  @moduletag :capture_log
  @modes [:legacy, :heartbeat]

  @env [
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :replication_enabled,
    :storage_pull_delay_ms,
    :recovery_prefetch_hook,
    :shard_idle_ms
  ]

  setup do
    id = "prefetch_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "prefetch_#{System.unique_integer([:positive])}")
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
      Storage.Local.purge_shard(id)
    end)

    Application.put_env(:fathom, :shard_idle_ms, 600_000)
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

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  # A stamped (rankable) stored object, a replica row on this node's follower (so there is something
  # to ask about), and NO local copy and NO lock: the next open is a cold fresh create.
  defp fixture!(id) do
    Application.put_env(:fathom, :replication_enabled, false)
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, coordinator} = Shards.ensure(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER PRIMARY KEY)"))

    for i <- 1..3,
        do: {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [i]))

    wal = Fathom.Shard.db_path(id) <> "-wal"
    {:ok, %{salt1: salt}} = Fathom.Shard.Replication.Wal.read(wal)
    assert Fathom.Shard.wal_ordinal(coordinator, salt) > 0
    :ok = Shards.flush(id)
    {:ok, stamp} = Storage.object_position(id)
    assert Recovery.rankable_stamp?(stamp), "fixture: the stamp must be rankable"

    path = Fathom.Shard.db_path(id)
    File.cp!(path, Follower.db_path(Follower, id))
    File.write!(Follower.wal_path(Follower, id), "")
    Follower.seed(Follower, id, 1, stamp.wal_gen, 0, stamp.offset, stamp.epoch)

    :ok = ShardExecutor.close(conn)
    :ok = Shards.stop(id)
    refute File.exists?(path), "fixture: the clean stop must drop the local copy"

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.put_env(:fathom, :replication_recover_from_peers, true)
    path
  end

  defp open_and_read!(id, mode) do
    {:ok, conn} = ShardExecutor.open(id)
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)

    if mode == :heartbeat,
      do: assert(is_integer(:sys.get_state(pid).acquire_gen), "heartbeat mode did not take"),
      else: assert(is_nil(:sys.get_state(pid).acquire_gen), "legacy mode did not take")

    refute Map.has_key?(:sys.get_state(pid).lease, :claim), "fixture: this must be a fresh create"
    {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT a FROM t ORDER BY a"))
    :ok = ShardExecutor.close(conn)
    Enum.map(rows, fn [a] -> a end)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "the marker + ask start while the pull is still in flight", %{id: id} do
        path = fixture!(id)
        test_pid = self()
        Application.put_env(:fathom, :storage_pull_delay_ms, 300)

        Application.put_env(:fathom, :recovery_prefetch_hook, fn p ->
          send(test_pid, {:prefetch, File.exists?(p)})
        end)

        assert open_and_read!(id, unquote(mode)) == [1, 2, 3]

        assert_received {:prefetch, installed?}, "no prefetch: the ask still waits for the pull"
        refute installed?, "the ask started only after the pull was installed"
        refute File.exists?(PromoteOnOpen.recovering_path(path)), "the marker survived the open"
      end

      # The 2026-10-13 #4 crash window with a prefetch: the node dies with the pull installed and the
      # prefetched answer not yet acted on. The marker must already be on disk, so the reopen treats
      # the installed pull as unvetted and recovers again rather than opening it warm.
      test "a crash after the prefetch, pull installed: the reopen re-runs recovery", %{id: id} do
        path = fixture!(id)
        test_pid = self()

        Application.put_env(:fathom, :recovery_prefetch_hook, fn _ ->
          send(test_pid, :prefetched)
        end)

        Application.put_env(:fathom, :recovery_ask_crash_hook, fn p ->
          send(
            test_pid,
            {:at_ask, File.exists?(p), File.exists?(PromoteOnOpen.recovering_path(p))}
          )

          raise "node died with the prefetched answer outstanding"
        end)

        on_exit(fn -> Application.delete_env(:fathom, :recovery_ask_crash_hook) end)

        assert {:error, _} = ShardExecutor.open(id), "precondition: the crashed open failed"
        assert_received :prefetched, "fixture: no prefetch was made"
        assert_received {:at_ask, true, true}, "the pull was installed without a durable marker"

        Application.delete_env(:fathom, :recovery_ask_crash_hook)
        assert open_and_read!(id, unquote(mode)) == [1, 2, 3]
        assert Path.wildcard(path <> ".forked.*") == []
        refute File.exists?(PromoteOnOpen.recovering_path(path)), "the marker is cleared"
      end

      test "an unconsumed prefetch leaves no marker beside the served file", %{id: id} do
        path = fixture!(id)
        test_pid = self()

        # The gate changes between the prefetch and the decision: promote-on-open is switched off,
        # so `maybe_promote_replica` returns at its gate and never reaches the marker cleanup.
        Application.put_env(:fathom, :recovery_prefetch_hook, fn _p ->
          Application.put_env(:fathom, :replication_promote_on_open, false)
          send(test_pid, :prefetched)
        end)

        assert open_and_read!(id, unquote(mode)) == [1, 2, 3]
        assert_received :prefetched, "fixture: no prefetch was made"
        refute File.exists?(PromoteOnOpen.recovering_path(path)), "the prefetch's marker survived"
      end
    end
  end
end
