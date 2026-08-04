defmodule Fathom.ShardLeaseReleaseTest do
  @moduledoc """
  Four ways a coordinator stopped WITHOUT releasing its lease, stranding the shard
  (expert review 2026-08-01 #9 and #11; the drop-path pair found by `chaos.sh rollout` on
  2026-08-04 — `docs/reviews/fleet-rollout-2026-08-04.md`).

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

    * **the drop path's two "keep the local copy" branches** — a transient flush error, and a
      fence that could not CONFIRM ownership. Both correctly kept the local copy (it holds
      acked-but-unflushed writes) and both incorrectly kept the LOCK. Keeping the copy and keeping
      the lock are separable; only the copy is load-bearing for recovery.

  That last pair is the quietest of the four, which is why a 300-tenant rig rollout is what
  surfaced them: the stranded tenant **keeps serving perfectly**, because its own node reclaims a
  lock held at its own incarnation. Only a FOREIGN owner is refused — and the migrator is a foreign
  owner (`migrator@<node>@<token>`) even on the same node, so the shard becomes permanently
  unmigratable and unfailoverable with `failed: 0` and nothing logged above `[info]`. The tests
  below therefore assert the foreign-owner view; the same-node re-open assertion passes against the
  unfixed code and is labelled as the value guard it is.

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

  describe "a TRANSIENT flush failure on the drop path" do
    # The third instance of this class, found by `chaos.sh rollout` on 2026-08-04 (see
    # docs/reviews/fleet-rollout-2026-08-04.md): a 300-tenant fleet rollout stranded exactly one
    # tenant, twice, on a different shard each time. The lock read {:held, "fathom@<node>#<inc>"}
    # with NO coordinator in that node's registry.
    #
    # `flush_then_drop/1`'s `{:error, reason}` branch keeps the local copy — correct, it holds
    # acked-but-unflushed writes — but it also kept the LOCK, and a lock naming a live node is
    # never stealable. Nothing in the pre-fix code logs above `[warning]`, and the migration that
    # then cannot drain the shard snoozes silently forever: the Oban job sits in `scheduled` with
    # an EMPTY errors array, `failed: 0`, and no quarantine.
    #
    # Keeping the copy and keeping the lock are separable, and only the copy is load-bearing for
    # recovery. The next open on this node arbitrates the diverged copy through the provenance
    # sidecar (#1) and quarantines it recoverably as `.forked.<ts>` — the same path a node crash
    # with un-flushed writes already takes. Holding the lock instead buys nothing and costs the
    # tenant its ability to ever migrate or fail over.
    test "the lease is released rather than stranded", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('unflushed')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)

      assert Shard.dirty?(coordinator),
             "the fixture must be dirty or the drop takes the clean path"

      # The local file must still be present — that is what separates this from #11.
      path = Path.join(Shard.data_dir(), "#{shard}.db")
      assert File.exists?(path)

      prev_fault = Application.get_env(:fathom, :storage_fault)
      Application.put_env(:fathom, :storage_fault, :flush)

      on_exit(fn ->
        if prev_fault,
          do: Application.put_env(:fathom, :storage_fault, prev_fault),
          else: Application.delete_env(:fathom, :storage_fault)
      end)

      ref = Process.monitor(coordinator)

      log =
        capture_log(fn ->
          :ok = Shards.drain(shard, 5_000)
          assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000
        end)

      assert Storage.lease_holder(shard) == :free,
             "a transient flush failure on the drop path stranded the lock — the tenant still " <>
               "serves (same-node reclaim) but can never migrate or fail over"

      assert log =~ "keeping local copy",
             "the local copy must still be kept for recovery — only the LOCK is given up"

      # The recovery copy is the whole reason this branch does not drop_local; releasing the lock
      # must not have cost us the un-flushed writes.
      assert File.exists?(path), "the un-flushed local copy was destroyed"
    end

    # This is the assertion that actually pins the damage, and the reason the sibling test above
    # is not enough on its own: the OWNING node re-opening proves nothing, because a coordinator
    # silently reclaims a lock held by its own node at its own incarnation. On the rig the stranded
    # tenant served client reads perfectly while being permanently unmigratable — the harm is only
    # visible to a FOREIGN owner. `Fathom.Migrator.ShardMigration` acquires as
    # `migrator@<node>@<token>`, a different owner string even on the same node, which is precisely
    # why the migration could never get in.
    test "a FOREIGN owner (the migrator) can acquire after the failed drop", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('unflushed')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)

      prev_fault = Application.get_env(:fathom, :storage_fault)
      Application.put_env(:fathom, :storage_fault, :flush)

      on_exit(fn ->
        if prev_fault,
          do: Application.put_env(:fathom, :storage_fault, prev_fault),
          else: Application.delete_env(:fathom, :storage_fault)
      end)

      capture_log(fn ->
        :ok = Shards.drain(shard, 5_000)
        assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000
      end)

      assert {:ok, lease} = Storage.acquire_lease(shard, "migrator@test@#{shard}", 30_000),
             "the migrator could not take the lease — this is the stuck rollout, exactly"

      :ok = Storage.release_lease(shard, lease)
    end

    # NOT a regression test, and it must not be read as one: it passes against the UNFIXED code
    # too, because the owning node reclaims its own lock silently (see the foreign-owner test
    # above for the one that discriminates). Keep it as the value guard — the fix gives up the
    # LOCK, and this pins that it does not also give up the un-flushed WRITES.
    test "the shard is re-openable, and the kept copy still holds the un-flushed write", %{
      shard: shard
    } do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('unflushed')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)

      prev_fault = Application.get_env(:fathom, :storage_fault)
      Application.put_env(:fathom, :storage_fault, :flush)

      capture_log(fn ->
        :ok = Shards.drain(shard, 5_000)
        assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000
      end)

      # Storage is reachable again — the transient failure is over.
      if prev_fault,
        do: Application.put_env(:fathom, :storage_fault, prev_fault),
        else: Application.delete_env(:fathom, :storage_fault)

      # The end-to-end consequence of a stranded lock is this open failing.
      {:ok, conn2} = ShardExecutor.open(shard)

      assert {:ok, %{rows: [["unflushed"]]}} =
               ShardExecutor.execute(conn2, stmt("SELECT v FROM kv")),
             "the warm local copy must still serve the write that never reached storage"

      :ok = ShardExecutor.close(conn2)
    end
  end

  describe "an UNCONFIRMED-ownership fence on the drop path" do
    # The sibling of the branch above and the second half of the same fix. `Fence.check` returns
    # `:skip` when it cannot CONFIRM ownership — in legacy mode that is a failed `renew_lease`
    # PUT (config/test.exs runs the whole suite in legacy mode, so `storage_fault: :renew`
    # reproduces it directly). Same shape as the flush-failure branch: keep the local copy,
    # previously also keep the lock, strand the tenant.
    #
    # Releasing here is safe BY CONSTRUCTION rather than by argument: `release_lease` is a
    # conditional `DELETE … If-Match: <the etag we last wrote>`, so if ownership was genuinely
    # lost the delete no-ops on someone else's lock. Either we still hold it (release, correct) or
    # we do not (no-op, correct). There is no third case where this deletes a lock that is not ours.
    test "the lease is released rather than stranded", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('unconfirmed')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)
      assert Shard.dirty?(coordinator), "the fixture must be dirty to reach flush_then_drop/1"

      prev_fault = Application.get_env(:fathom, :storage_fault)
      Application.put_env(:fathom, :storage_fault, :renew)

      on_exit(fn ->
        if prev_fault,
          do: Application.put_env(:fathom, :storage_fault, prev_fault),
          else: Application.delete_env(:fathom, :storage_fault)
      end)

      ref = Process.monitor(coordinator)

      log =
        capture_log(fn ->
          :ok = Shards.drain(shard, 5_000)
          assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000
        end)

      assert log =~ "ownership unconfirmed",
             "the fixture did not reach the :skip branch — check that the suite is in legacy mode"

      assert Storage.lease_holder(shard) == :free,
             "an unconfirmed-ownership drop stranded the lock"

      assert File.exists?(Path.join(Shard.data_dir(), "#{shard}.db")),
             "the un-flushed local copy was destroyed"
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
