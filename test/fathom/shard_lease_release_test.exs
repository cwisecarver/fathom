defmodule Fathom.ShardLeaseReleaseTest do
  @moduledoc """
  Every way a coordinator has been found to stop WITHOUT releasing its lease, stranding the shard
  (expert review 2026-08-01 #9 and #11; the drop-path pair and the post-acquire exception found by
  `chaos.sh rollout` on 2026-08-04 — `docs/reviews/fleet-rollout-2026-08-04.md`).

  A leaked `.lock` names this node, and while this node's `Heartbeat` is running `owner_live?`
  reports `:live` forever — so every peer gets `{:error, {:held, us}}` indefinitely. The shard
  is unopenable by any survivor and waiting does not fix it. The rebalancer handoff breaks the
  same way: its drain lands on this path, so the target the LB was already flipped to is
  refused.

  ## Both liveness modes, on purpose

  A coordinator fixes its mode at open: `acquire_gen` is non-nil (**heartbeat** mode — the node
  heartbeat proves liveness, no per-shard renewal) or nil (**legacy** — per-shard renew PUTs).
  They are different fence, renewal and release paths.

  `config/test.exs` sets `heartbeat_server: false`, so for a long time EVERY coordinator in the
  entire suite ran legacy mode while production and the chaos rig ran heartbeat mode — and this
  file's leak scenarios only ever ran on the legacy side. That is the same shape of blind spot as
  `Storage.Local` vs the S3 lock-etag contract (see `Fathom.Test.FaultyStorage`): the environment
  silently exempts every bug that lives on the other side of it, and no amount of care inside the
  tests compensates. So the mode-agnostic scenarios below are generated for **both** modes, and the
  ones that are genuinely mode-specific say which and why.

  ## The scenarios

    * **#9** (legacy only) — `flush_then_drop/1` discarded the lease `Fence.check` returns. In
      legacy mode that check performs a `renew_lease` PUT which ROTATES the lock's etag, so the
      conditional `DELETE … If-Match: <stale etag>` 412'd and the lock survived. Heartbeat mode
      performs no renew, so the mechanism does not exist there.

    * **#11** (both) — a dirty shard whose local file is GONE fell out of `flush_then_drop/1`
      having done nothing at all: no upload (correct), but also no release, no log, no telemetry.
      Reachable en masse — a `WriteCounter` restart marks every open coordinator dirty at once,
      including ones that never created a file.

    * **a transient flush error on the drop path** (both) — kept the local copy, correctly, and
      the LOCK, incorrectly.

    * **an unconfirmed-ownership fence on the drop path** (both, via different triggers) — same
      shape. Legacy reaches `:skip` through a failed renew PUT; heartbeat reaches it when
      `Heartbeat.valid_for_write?/1` says the node's own liveness is not currently provable,
      which a Heartbeat process crash produces.

    * **an exception between `acquire_lease` and the built state** (both) — `handle_continue`
      never returns, so `terminate/2` sees the pre-open state and has no lease to release.

  ## Why the assertions look the way they do

  This class is quiet: a stranded tenant **keeps serving perfectly**, because its own node reclaims
  a lock held at its own incarnation. Only a FOREIGN owner is refused — and the migrator is a
  foreign owner (`migrator@<node>@<token>`) even on the same node, so the shard becomes permanently
  unmigratable and unfailoverable with `failed: 0` and nothing logged above `[info]`. So the tests
  assert the foreign-owner view, and the lock assertion is ordered BEFORE any log assertion so a
  pre-fix run fails on the invariant rather than on a log string.

  Not async: shards are global and back onto real files.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

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

  # Put the NEXT coordinator open into `mode`. The mode is captured at open (`acquire_gen`), so
  # this must run before the shard is first opened. `Heartbeat.terminate/2` clears its own storage
  # object on a clean stop, so `start_supervised!` is the whole cleanup.
  defp set_mode!(:legacy) do
    refute Heartbeat.running?(),
           "legacy mode needs the heartbeat OFF; config/test.exs default is heartbeat_server: false"

    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    # Long TTL so it never lapses on its own; a test that wants a lapse forces one.
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  # Assert the coordinator actually opened in the mode we asked for, rather than trusting the
  # setup. A scenario that silently ran legacy twice would look like two-mode coverage and be one.
  defp assert_mode!(coordinator, :heartbeat) do
    assert is_integer(:sys.get_state(coordinator).acquire_gen),
           "expected heartbeat mode (non-nil acquire_gen) — the mode setup did not take"
  end

  defp assert_mode!(coordinator, :legacy) do
    assert is_nil(:sys.get_state(coordinator).acquire_gen),
           "expected legacy mode (nil acquire_gen) — something started the heartbeat"
  end

  defp seed!(shard, value) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('#{value}')"))
    :ok = ShardExecutor.close(conn)
    :ok
  end

  defp put_fault!(fault) do
    prev = Application.get_env(:fathom, :storage_fault)
    Application.put_env(:fathom, :storage_fault, fault)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :storage_fault, prev),
        else: Application.delete_env(:fathom, :storage_fault)
    end)

    :ok
  end

  defp drain_and_wait!(shard, coordinator) do
    ref = Process.monitor(coordinator)

    capture_log(fn ->
      :ok = Shards.drain(shard, 5_000)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000
    end)
  end

  # The assertion that actually pins the damage. The owning node re-opening proves nothing: a
  # coordinator silently reclaims a lock held by its own node at its own incarnation, which is
  # exactly why the production symptom was invisible.
  defp assert_foreign_owner_can_acquire!(shard, context) do
    assert Storage.lease_holder(shard) == :free, context

    assert {:ok, lease} = Storage.acquire_lease(shard, "migrator@test@#{shard}", 30_000),
           "a foreign owner (the migrator) could not take the lease — this is the stuck rollout"

    :ok = Storage.release_lease(shard, lease)
  end

  for mode <- @modes do
    describe "#{mode} mode — a TRANSIENT flush failure on the drop path" do
      setup do: set_mode!(unquote(mode))

      test "releases the lease, keeps the local copy", %{shard: shard} do
        seed!(shard, "unflushed")
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        assert Shard.dirty?(coordinator),
               "the fixture must be dirty or the drop takes the clean path"

        path = Path.join(Shard.data_dir(), "#{shard}.db")
        assert File.exists?(path), "this scenario needs a local file — #11 is the one without"

        put_fault!(:flush)
        log = drain_and_wait!(shard, coordinator)

        assert_foreign_owner_can_acquire!(
          shard,
          "a transient flush failure on the drop path stranded the lock"
        )

        assert log =~ "keeping local copy",
               "the local copy must still be kept for recovery — only the LOCK is given up"

        assert File.exists?(path), "the un-flushed local copy was destroyed"
      end

      # NOT a regression test — it passes against the unfixed code too, because the owning node
      # reclaims its own lock silently. Kept as the value guard: the fix gives up the LOCK, and
      # this pins that it does not also give up the un-flushed WRITES.
      test "the kept copy still serves the write that never reached storage", %{shard: shard} do
        seed!(shard, "unflushed")
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        prev = Application.get_env(:fathom, :storage_fault)
        Application.put_env(:fathom, :storage_fault, :flush)
        drain_and_wait!(shard, coordinator)

        # Storage is reachable again — the transient failure is over.
        if prev,
          do: Application.put_env(:fathom, :storage_fault, prev),
          else: Application.delete_env(:fathom, :storage_fault)

        {:ok, conn2} = ShardExecutor.open(shard)

        assert {:ok, %{rows: [["unflushed"]]}} =
                 ShardExecutor.execute(conn2, stmt("SELECT v FROM kv")),
               "the warm local copy must still serve the write that never reached storage"

        :ok = ShardExecutor.close(conn2)
      end
    end

    describe "#{mode} mode — #11, dirty but the local file is gone" do
      setup do: set_mode!(unquote(mode))

      test "releases the lease rather than stranding it", %{shard: shard} do
        seed!(shard, "gone")
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        # Delete the local copy out from under the (dirty) coordinator and make sure it still
        # believes it is dirty. This is what a WriteCounter restart produces at fleet scale.
        path = Path.join(Shard.data_dir(), "#{shard}.db")
        for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
        send(coordinator, :write_counter_reset)
        _ = :sys.get_state(coordinator)
        assert Shard.dirty?(coordinator)

        log = drain_and_wait!(shard, coordinator)

        assert_foreign_owner_can_acquire!(
          shard,
          "a dirty shard with no local file stranded its lock"
        )

        assert log =~ "local file is GONE", "the event must be logged — it was previously silent"
      end

      test "emits alertable flush-failure telemetry", %{shard: shard} do
        test_pid = self()
        handler = "lease-missing-#{shard}-#{unquote(mode)}"

        :telemetry.attach(
          handler,
          [:fathom, :shard, :flush, :failed],
          fn _e, _m, meta, _ -> send(test_pid, {:flush_failed, meta}) end,
          nil
        )

        on_exit(fn -> :telemetry.detach(handler) end)

        seed!(shard, "x")
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        path = Path.join(Shard.data_dir(), "#{shard}.db")
        for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
        send(coordinator, :write_counter_reset)
        _ = :sys.get_state(coordinator)

        drain_and_wait!(shard, coordinator)
        assert_receive {:flush_failed, %{reason: :local_file_missing}}, 2_000
      end
    end

    describe "#{mode} mode — an EXCEPTION after the lease is acquired" do
      setup do: set_mode!(unquote(mode))

      # `handle_continue(:open, …)` holds the lock from `acquire_lease` until it RETURNS the built
      # state. Every failure the code anticipated is an `{:error, _}` TUPLE, which reaches
      # `open_with_lease/8`'s else branch and releases. An EXCEPTION does not: `handle_continue`
      # never returns, so the GenServer still holds the pre-open `%{id: shard_id}` state and
      # `terminate/2` falls to the catch-all clause that has no lease to release.
      #
      # `start_pull/2` was already rescued and review 2026-08-01 #33 rescued `fork_evidence/2`, but
      # `resolve_fork/4`, `revalidate_takeover/5` and `promote_pull/2` run storage/File calls
      # directly in the coordinator. The rig's trigger was `Req.TransportError socket closed` out of
      # a HEAD under load; this raises from `post_lease_warm_check/3`'s `object_etag/1`, the same
      # HEAD. The fixture is a WARM open (local file present), because that is the path that makes a
      # post-lease storage call at all — hence the brutal kill, which is also how the file survives
      # in production (a coordinator that dies without running terminate/2).
      test "releases the lease rather than stranding it", %{shard: shard} do
        seed!(shard, "warm")
        # Flush so a provenance sidecar exists and the next open takes the warm path.
        :ok = Shards.flush(shard)

        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))
        ref = Process.monitor(coordinator)
        Process.exit(coordinator, :kill)
        assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 5_000

        path = Path.join(Shard.data_dir(), "#{shard}.db")
        assert File.exists?(path), "the fixture needs a warm local copy or the open goes cold"

        Application.put_env(
          :fathom,
          :faulty_before,
          {:object_etag, fn -> raise Req.TransportError, reason: :closed end}
        )

        on_exit(fn -> Application.delete_env(:fathom, :faulty_before) end)

        log =
          capture_log(fn ->
            # The open must FAIL — we refuse to serve a shard we could not open. The point is what
            # it leaves behind, not that it succeeds.
            assert {:error, _} = ShardExecutor.open(shard)
          end)

        Application.delete_env(:fathom, :faulty_before)

        assert_foreign_owner_can_acquire!(
          shard,
          "an exception after acquire stranded the lock"
        )

        assert log =~ "open FAILED after the lease was acquired",
               "the abandoned open must be loud — it was previously silent"
      end
    end
  end

  describe "legacy mode — #9, the fence-refreshed lease must reach release_lease" do
    # LEGACY ONLY, and not an oversight: `Fence.check` performs a `renew_lease` PUT in legacy mode,
    # and that PUT is what rotates the lock's etag and made releasing with the pre-fence lease a
    # silent no-op. Heartbeat mode performs no per-shard renew, so this mechanism cannot occur
    # there. Reachable in production from any Heartbeat restart (finding #29).
    setup do: set_mode!(:legacy)

    test "a dirty shard's drain leaves the lock FREE, so a peer can take it", %{shard: shard} do
      seed!(shard, "dirty")
      {:ok, coordinator} = Shards.ensure(shard)
      assert_mode!(coordinator, :legacy)
      drain_and_wait!(shard, coordinator)

      assert Storage.lease_holder(shard) == :free,
             "the drain stranded the lock — no peer can ever open this shard"
    end

    test "the shard is genuinely re-openable after the drain", %{shard: shard} do
      seed!(shard, "one")
      :ok = Shards.drain(shard, 5_000)

      {:ok, conn2} = ShardExecutor.open(shard)
      assert {:ok, _} = ShardExecutor.execute(conn2, stmt("SELECT v FROM kv"))
      :ok = ShardExecutor.close(conn2)
    end
  end

  describe "legacy mode — an UNCONFIRMED-ownership fence on the drop path" do
    # `Fence.check` returns `:skip` when it cannot CONFIRM ownership. In legacy mode that is a
    # failed `renew_lease` PUT, which `storage_fault: :renew` reproduces directly. The heartbeat
    # counterpart is below and needs a different trigger entirely.
    #
    # Releasing here is safe BY CONSTRUCTION rather than by argument: `release_lease` is a
    # conditional `DELETE … If-Match: <the etag we last wrote>`, so if ownership was genuinely lost
    # the delete no-ops on someone else's lock. Either we still hold it (release, correct) or we do
    # not (no-op, correct). There is no third case where this deletes a lock that is not ours.
    setup do: set_mode!(:legacy)

    test "releases the lease rather than stranding it", %{shard: shard} do
      seed!(shard, "unconfirmed")
      {:ok, coordinator} = Shards.ensure(shard)
      assert_mode!(coordinator, :legacy)
      assert Shard.dirty?(coordinator), "the fixture must be dirty to reach flush_then_drop/1"

      put_fault!(:renew)
      log = drain_and_wait!(shard, coordinator)

      assert log =~ "ownership unconfirmed", "the fixture did not reach the :skip branch"

      assert_foreign_owner_can_acquire!(
        shard,
        "an unconfirmed-ownership drop stranded the lock"
      )

      assert File.exists?(Path.join(Shard.data_dir(), "#{shard}.db")),
             "the un-flushed local copy was destroyed"
    end
  end

  describe "heartbeat mode — an UNCONFIRMED-ownership fence on the drop path" do
    # The heartbeat counterpart of the legacy `:skip` test above, and the one that could not exist
    # while the whole suite ran legacy mode. Here `Fence.check` consults
    # `Heartbeat.valid_for_write?/1` instead of doing a renew PUT.
    #
    # Getting to `:skip` needs care, and the FIRST fixture written for this did not reach it: it
    # stopped the Heartbeat process, but `Fence.check` treats a DOWN heartbeat as "degrade to the
    # legacy per-shard renew fence", which succeeds. So that fixture proved a Heartbeat crash is
    # survivable (true, and worth knowing) while claiming to prove something else — it passed
    # against the unfixed code, which is how it was caught.
    #
    # The real `:not_valid` state is the heartbeat process ALIVE but its renewal deadline not
    # comfortably in the future (`now + margin >= deadline`). That is an ordinary production
    # moment, not a contrivance: it is exactly what the safety margin exists to catch, and it
    # happens whenever a renewal runs late. Publish a past deadline at the SAME generation — a
    # different generation would route to `:revalidate` instead.
    setup do: set_mode!(:heartbeat)

    test "releases the lease rather than stranding it", %{shard: shard} do
      seed!(shard, "hb-unconfirmed")
      {:ok, coordinator} = Shards.ensure(shard)
      assert_mode!(coordinator, :heartbeat)
      assert Shard.dirty?(coordinator), "the fixture must be dirty to reach flush_then_drop/1"

      hb = Process.whereis(Heartbeat)
      state = :sys.get_state(hb)

      Heartbeat.publish_status(%{
        state
        | mono_deadline_ms: System.monotonic_time(:millisecond) - 1
      })

      assert Heartbeat.valid_for_write?(:sys.get_state(coordinator).acquire_gen) == :not_valid,
             "the fixture did not produce :not_valid, so the drop never reaches the :skip branch"

      log = drain_and_wait!(shard, coordinator)

      assert log =~ "ownership unconfirmed",
             "the drop did not take the :skip branch this test exists for"

      assert_foreign_owner_can_acquire!(
        shard,
        "a heartbeat-mode drop whose fence could not confirm ownership stranded the lock"
      )

      assert File.exists?(Path.join(Shard.data_dir(), "#{shard}.db")),
             "the un-flushed local copy was destroyed"
    end
  end

  describe "heartbeat mode — a Heartbeat process crash mid-life" do
    # Kept from the first (wrong) fixture for the test above, because the property it accidentally
    # proved is worth pinning on its own: when the Heartbeat process is DOWN, `Fence.check`
    # degrades to the legacy per-shard renew fence rather than failing, so a coordinator that
    # opened in heartbeat mode still drops cleanly and still releases its lock.
    #
    # NOT a regression test — it passes against the unfixed code. It is labelled here so nobody
    # reads it as one, and so the next person does not "simplify" the `:not_valid` fixture above
    # back into this one.
    setup do: set_mode!(:heartbeat)

    test "a coordinator opened in heartbeat mode still releases its lock", %{shard: shard} do
      seed!(shard, "hb-crash")
      {:ok, coordinator} = Shards.ensure(shard)
      assert_mode!(coordinator, :heartbeat)

      stop_supervised!(Heartbeat)
      refute Heartbeat.running?()

      drain_and_wait!(shard, coordinator)

      assert_foreign_owner_can_acquire!(
        shard,
        "a heartbeat-mode coordinator stranded its lock after the Heartbeat process died"
      )
    end
  end
end
