defmodule Fathom.ShardLeaseReleaseTest do
  @moduledoc """
  Two ways a coordinator stopped WITHOUT releasing its lease, stranding the shard
  (expert review 2026-08-01 #9 and #11).

  A leaked `.lock` names this node, and while this node's `Heartbeat` is running `owner_live?`
  reports `:live` forever — so every peer gets `{:error, {:held, us}}` indefinitely. The shard
  is unopenable by any survivor and waiting does not fix it. The rebalancer handoff breaks the
  same way: its drain lands on this path, so the target the LB was already flipped to is
  refused.

    * **#9** — `flush_then_drop/1` discarded the lease `Fence.check` returns. In legacy mode
      (no heartbeat process) that check performs a `renew_lease` PUT which ROTATES the lock's
      etag, so the conditional `DELETE … If-Match: <stale etag>` 412'd and the lock survived.

    * **#11** — a dirty shard whose local file is GONE fell out of `flush_then_drop/1` having
      done nothing at all: no upload (correct), but also no release, no log, no telemetry.
      Reachable en masse — a `WriteCounter` restart marks every open coordinator dirty at once,
      including ones that never created a file.

  Not async: shards are global and back onto real files.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Filo.Stmt

  setup do
    shard = "lease_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)

    # FaultyStorage models S3's lock-etag contract; plain Local identifies a lock by
    # {owner, epoch} alone and so cannot express #9 at all (see the backend's comment).
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  describe "#9 — the fence-refreshed lease must reach release_lease" do
    # LEGACY MODE is the precondition, and `config/test.exs` sets `heartbeat_server: false`, so
    # every coordinator in the suite already opens with `acquire_gen == nil`. `Fence.check`
    # therefore performs a `renew_lease` PUT — the thing that rotates the lock's etag and made
    # releasing with the pre-fence lease a silent no-op. In production this mode is reachable
    # from any Heartbeat restart (see finding #29).
    setup do
      refute Fathom.Shard.Heartbeat.running?(),
             "these tests assert the legacy-mode fence path; the heartbeat must be off"

      :ok
    end

    test "a dirty shard's drain leaves the lock FREE, so a peer can take it", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('dirty')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)
      :ok = Shards.drain(shard, 5_000)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000

      assert Storage.lease_holder(shard) == :free,
             "the drain stranded the lock — no peer can ever open this shard"
    end

    test "the shard is genuinely re-openable after the drain", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('one')"))
      :ok = ShardExecutor.close(conn)
      :ok = Shards.drain(shard, 5_000)

      # The end-to-end consequence of a stranded lock is this open failing.
      {:ok, conn2} = ShardExecutor.open(shard)
      assert {:ok, _} = ShardExecutor.execute(conn2, stmt("SELECT v FROM kv"))
      :ok = ShardExecutor.close(conn2)
    end
  end

  describe "#11 — dirty, but the local file is gone" do
    test "the lease is released rather than stranded", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('gone')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)

      # Delete the local copy out from under the (dirty) coordinator, and make sure it still
      # believes it is dirty. This is what a WriteCounter restart produces at fleet scale.
      path = Path.join(Shard.data_dir(), "#{shard}.db")
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
      send(coordinator, :write_counter_reset)
      _ = :sys.get_state(coordinator)
      assert Shard.dirty?(coordinator)

      ref = Process.monitor(coordinator)

      log =
        capture_log(fn ->
          :ok = Shards.drain(shard, 5_000)
          assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000
        end)

      assert Storage.lease_holder(shard) == :free,
             "a dirty shard with no local file stranded its lock"

      assert log =~ "local file is GONE",
             "the event must be logged — it was previously silent"
    end

    test "the missing-file stop emits alertable flush-failure telemetry", %{shard: shard} do
      test_pid = self()
      handler = "lease-missing-#{shard}"

      :telemetry.attach(
        handler,
        [:fathom, :shard, :flush, :failed],
        fn _e, _m, meta, _ -> send(test_pid, {:flush_failed, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)
      path = Path.join(Shard.data_dir(), "#{shard}.db")
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
      send(coordinator, :write_counter_reset)
      _ = :sys.get_state(coordinator)

      capture_log(fn -> Shards.drain(shard, 5_000) end)

      assert_receive {:flush_failed, %{reason: :local_file_missing}}, 2_000
    end
  end
end
