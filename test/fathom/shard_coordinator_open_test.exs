defmodule Fathom.ShardCoordinatorOpenTest do
  @moduledoc """
  Four coordinator findings from expert review 2026-10-10 (#5, #12, #18, #29), each pinning an
  invariant of the open / registration path. Every scenario runs in BOTH liveness modes
  (`acquire_gen` non-nil = heartbeat, nil = legacy) except #12, which only exists in heartbeat
  mode (legacy never subscribes to lapse broadcasts); each asserts the mode actually took.

    * **#5** — a coordinator traps exits and used to swallow the EXIT from its Registry partition,
      surviving as an unregistered orphan that could `File.rm` the live coordinator's `.db`.
    * **#12** — a heartbeat generation bump between sampling `acquire_gen` and subscribing to lapse
      broadcasts was never seen; the coordinator sat on a stale baseline until its next flush.
    * **#18** — a delete that completed between `Shards.ensure/1`'s tombstone gate and the
      coordinator's open let the open re-create a lock and a `.db` nobody reclaims.
    * **#29** — the fork-evidence HEAD wait was 60 s, so a warm-but-diverged open (wait + full pull)
      overran the checkout budget.

  Not async: shards are global and back onto real files.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Tenants.{Suspensions, Tombstones}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "coordopen_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      restore(:shard_storage, prev_storage)
      Application.delete_env(:fathom, :faulty_before)
      Application.delete_env(:fathom, :fork_evidence_timeout_ms)
      Application.delete_env(:fathom, :lapse_revalidate_jitter_ms)
      :ets.delete(Tombstones, shard)
      Suspensions.remove(shard)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(),
           "legacy mode needs the heartbeat OFF; config/test.exs default is heartbeat_server: false"
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    hb
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  defp seed!(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))
    :ok = ShardExecutor.close(conn)
  end

  # Start a coordinator the way `Shards.start/1` does, BYPASSING `Shards.ensure/1`'s gates — which is
  # exactly the position of an open whose gate check predates a concurrent delete/suspend.
  #
  # Linked to the test (trapping exits) rather than supervised, so the stop REASON is observable:
  # a monitor attached after the coordinator already stopped only ever sees `:noproc`.
  defp start_ungated(shard) do
    Process.flag(:trap_exit, true)
    GenServer.start_link(Shard, shard, name: Shard.via(shard))
  end

  for mode <- @modes do
    describe "#{mode} mode — expert review 2026-10-10 #5 (Registry partition death)" do
      test "a coordinator whose Registry partition dies stops instead of living on unregistered",
           %{shard: shard} do
        set_mode!(unquote(mode))
        seed!(shard)
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        # The via-Registry registration links the coordinator to its partition process.
        {:links, links} = Process.info(coordinator, :links)

        partition =
          Enum.find(links, fn pid ->
            case Process.info(pid, :registered_name) do
              {:registered_name, name} when is_atom(name) ->
                String.starts_with?(Atom.to_string(name), "Elixir.Fathom.ShardRegistry.")

              _ ->
                false
            end
          end)

        assert is_pid(partition), "the coordinator is not linked to a Registry partition"

        ref = Process.monitor(coordinator)

        capture_log(fn ->
          Process.exit(partition, :kill)

          # Pre-fix the EXIT was swallowed: the coordinator stayed alive with `Registry.lookup`
          # returning [] — a second coordinator then started on the same `.db`.
          assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :registry_lost}}, 5_000
        end)
      end

      test "an abnormal EXIT from an unrelated process does not stop a registered coordinator",
           %{shard: shard} do
        set_mode!(unquote(mode))
        seed!(shard)
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        send(coordinator, {:EXIT, self(), :boom})
        _ = :sys.get_state(coordinator)

        assert Process.alive?(coordinator)
        assert [{^coordinator, _}] = Registry.lookup(Fathom.ShardRegistry, shard)
      end
    end

    describe "#{mode} mode — expert review 2026-10-10 #18 (lifecycle re-check at open)" do
      test "an open racing a completed delete stops before taking a lease or creating a file",
           %{shard: shard} do
        set_mode!(unquote(mode))
        Tombstones.put(shard)

        {:ok, pid} = start_ungated(shard)
        assert_receive {:EXIT, ^pid, {:shutdown, :shard_tombstoned}}, 5_000

        # Pre-fix the open acquired the lock and created the .db, and the tombstone-skipping
        # terminate never released it: a leaked lock and file for a shard that no longer exists.
        assert Storage.lease_holder(shard) == :free
        refute File.exists?(Shard.db_path(shard))
      end

      test "an open racing a suspend stops before taking a lease or creating a file",
           %{shard: shard} do
        set_mode!(unquote(mode))
        Suspensions.put(shard)

        {:ok, pid} = start_ungated(shard)
        assert_receive {:EXIT, ^pid, {:shutdown, :shard_suspended}}, 5_000

        assert Storage.lease_holder(shard) == :free
        refute File.exists?(Shard.db_path(shard))
      end
    end

    describe "#{mode} mode — expert review 2026-10-10 #29 (fork-evidence wait)" do
      test "a hung fork-evidence HEAD is abandoned after the short bound, not 60 s",
           %{shard: shard} do
        set_mode!(unquote(mode))
        seed!(shard)
        {:ok, first} = Shards.ensure(shard)
        assert_mode!(first, unquote(mode))

        # Durable flush so the provenance sidecar exists (evidence otherwise answers
        # :no_sidecar without touching storage), then kill the coordinator, leaving the local
        # file: the next open is a warm open that runs the fork-evidence HEAD.
        assert :ok = Shards.flush(shard)
        ref = Process.monitor(first)
        Process.exit(first, :kill)
        assert_receive {:DOWN, ^ref, :process, ^first, _}, 5_000

        Application.put_env(:fathom, :fork_evidence_timeout_ms, 100)
        calls = :counters.new(1, [])

        Application.put_env(
          :fathom,
          :faulty_before,
          {:object_etag,
           fn
             ^shard ->
               if :counters.get(calls, 1) == 0 do
                 :counters.add(calls, 1, 1)
                 Process.sleep(3_000)
               end

             _other ->
               :ok
           end}
        )

        started = System.monotonic_time(:millisecond)
        {:ok, second} = Shards.ensure(shard)
        {:ok, _ref, _path} = Shard.checkout(second)
        elapsed = System.monotonic_time(:millisecond) - started

        assert :counters.get(calls, 1) == 1, "the fork-evidence HEAD hook never fired"
        assert elapsed < 2_000, "the open waited out the hung HEAD (#{elapsed} ms)"
      end
    end
  end

  describe "heartbeat mode — expert review 2026-10-10 #12 (lapse during the open window)" do
    test "a generation bump between sampling acquire_gen and subscribing is delivered after open",
         %{shard: shard} do
      hb = set_mode!(:heartbeat)
      # Keep the revalidation episode armed for the whole assertion (the default jitter is a few
      # ms, and the episode would resolve back to `false` before we could observe it).
      Application.put_env(:fathom, :lapse_revalidate_jitter_ms, 60_000)

      # The pull task runs between the acquire_gen sample and subscribe_lapse/0; bump the heartbeat
      # generation there — its broadcast fires while we are NOT yet subscribed, so it is lost.
      Application.put_env(
        :fathom,
        :faulty_before,
        {:pull,
         fn
           ^shard ->
             :sys.replace_state(hb, fn s ->
               Heartbeat.publish_status(%{s | generation: s.generation + 1})
             end)

           _other ->
             :ok
         end}
      )

      {:ok, coordinator} = Shards.ensure(shard)
      assert_mode!(coordinator, :heartbeat)

      state = :sys.get_state(coordinator)

      assert Heartbeat.generation() != state.acquire_gen,
             "the fixture did not move the generation"

      # Pre-fix: no broadcast was ever observed, so nothing armed the revalidation and the
      # coordinator kept a stale baseline until its next flush.
      assert state.lapse_revalidate_pending == true
    end
  end
end
