defmodule Fathom.ShardAcquireLandsThenErrorsTest do
  @moduledoc """
  Expert review 2026-11-07 #26 (the create-variant of #6). The lease create is one optimistic
  `PUT If-None-Match: *` that `Req` never retries, and its reply can be lost AFTER the store applied
  it: `acquire_lease` reports `{:error, reason}` for a lock that now exists and names THIS live
  node. `open_coordinator` stopped on `{:lease_unavailable, reason}` with no lease in state, so
  nothing released it, and every peer read `{:held, us}` — a failover, migration or handoff acquire
  was refused until this node happened to open the shard again.

  The invariant: after `checkout` returns `{:error, {:lease_unavailable, _}}` for an acquire that may
  have written, no lock names this node — the shard is free for a FOREIGN owner (the migrator is one
  even on the same node, which is how this class stays quiet: our own next open silently reclaims).

  The converse is pinned too: an acquire that PROVES nothing was written (a read blip before the
  write, a live holder) must not touch the lock, so the reconcile costs a failing open nothing it
  was not already paying.

  Parameterized over both liveness modes (`acquire_gen` fixed at open; legacy renews per shard,
  heartbeat does not), and run against the `FaultyStorage` double AND the real S3 backend over
  `S3EtagStore`, because the double's `:acquire_lands_then_errors` is only as honest as the store
  contract behind it.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @modes [:legacy, :heartbeat]

  setup do
    shard = "acqland_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_s3 = Application.get_env(:fathom, S3)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      Application.delete_env(:fathom, :faulty_before)
      restore(:shard_storage, prev_storage)
      restore(:shard_idle_ms, prev_idle)

      if prev_s3,
        do: Application.put_env(:fathom, S3, prev_s3),
        else: Application.delete_env(:fathom, S3)

      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF (config/test.exs default)"
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  # The mode is captured at open, from `Heartbeat.running?/0`. The failed open leaves no coordinator
  # to inspect, so the retry that follows is the one that is checked.
  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen))

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen))

  # The foreign-owner view, the only one that sees the damage: this node reclaims its own lock.
  defp assert_foreign_owner_can_acquire!(shard) do
    assert Storage.lease_holder(shard) == :free,
           "the lock the failed acquire wrote still names this node: peers read it as held"

    assert {:ok, lease} = Storage.acquire_lease(shard, "migrator@test@#{shard}", 30_000),
           "a foreign owner (the migrator) could not take the lease — the stuck shard of #26"

    :ok = Storage.release_lease(shard, lease)
  end

  # Observe the lock at the moment the reconcile runs (before it removes anything), so the test can
  # assert its OWN fixture: the fault really did leave a lock naming this node.
  defp watch_release!(shard) do
    test_pid = self()

    Application.put_env(:fathom, :faulty_before, {
      :release,
      fn
        ^shard -> send(test_pid, {:lock_at_release, Storage.lease_holder(shard)})
        _other -> :ok
      end
    })
  end

  for mode <- @modes do
    describe "FaultyStorage, #{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a create that LANDED with its reply lost does not strand the lock", %{shard: shard} do
        watch_release!(shard)
        Application.put_env(:fathom, :storage_fault, :acquire_lands_then_errors)

        capture_log(fn ->
          assert {:error, {:lease_unavailable, :s3_unreachable}} = Shards.checkout(shard)
        end)

        # The store is healthy again (the fault would fail the foreign acquire too).
        Application.delete_env(:fathom, :storage_fault)
        assert_foreign_owner_can_acquire!(shard)

        # The fixture, asserted rather than assumed: the fault DID write a lock that names this
        # node, and the reconcile is what gave it back (a vacuous pass would see no lock at all).
        owner = Heartbeat.owner()

        assert_receive {:lock_at_release, {:held, ^owner}},
                       1_000,
                       "the fault never left a lock behind (or the reconcile never ran)"

        # And the shard still opens normally for this node, in the mode under test.
        assert {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))
      end

      test "an acquire that wrote NOTHING (a read blip) is left alone", %{shard: shard} do
        watch_release!(shard)
        Application.put_env(:fathom, :storage_fault, :acquire)

        capture_log(fn ->
          assert {:error, {:lease_unavailable, {:transient_lookup, :s3_unreachable}}} =
                   Shards.checkout(shard)
        end)

        refute_receive {:lock_at_release, _}, 100
      end

      test "a WARM open leaves the lock: a lock this node already held may be protecting a dirty copy",
           %{shard: shard} do
        # With a local copy on disk the lock the failed acquire left could just as well be an
        # ordinary lock of this node's from before (a coordinator that crashed while the node
        # stayed up), and while it names a live node it keeps peers out of a shard whose copy may
        # hold acked-but-unflushed writes. The reconcile is for a COLD open only.
        watch_release!(shard)
        File.write!(Shard.db_path(shard), "")
        Application.put_env(:fathom, :storage_fault, :acquire_lands_then_errors)

        capture_log(fn ->
          assert {:error, {:lease_unavailable, _}} = Shards.checkout(shard)
        end)

        Application.delete_env(:fathom, :storage_fault)
        refute_receive {:lock_at_release, _}, 100
        owner = Heartbeat.owner()

        assert {:held, ^owner} = Storage.lease_holder(shard),
               "fixture: the create must have landed, and a warm open must not give it back"

        assert File.exists?(Shard.db_path(shard)), "the local copy must not be touched"
      end

      test "a live holder's lock is never touched by the loser", %{shard: shard} do
        watch_release!(shard)
        # A peer holds the shard live: our acquire is refused as held, and nothing is reconciled.
        {:ok, peer} = Storage.acquire_lease(shard, "peer@node#9", 30_000)

        capture_log(fn ->
          assert {:error, {:shard_held, "peer@node#9", _}} = Shards.checkout(shard)
        end)

        refute_receive {:lock_at_release, _}, 100
        assert {:held, "peer@node#9"} = Storage.lease_holder(shard)
        :ok = Storage.release_lease(shard, peer)
      end
    end

    describe "S3 backend over S3EtagStore, #{mode} mode" do
      setup %{shard: shard} do
        # The store first, so it is stopped LAST: a heartbeat that clears its own object on a clean
        # stop would otherwise call a store that is already gone.
        store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

        Application.put_env(:fathom, S3,
          bucket: "b",
          region: "us-east-1",
          access_key_id: "k",
          secret_access_key: "s",
          endpoint: "https://s3.example",
          path_style: true,
          req_plug: fn conn -> S3EtagStore.serve(conn, store) end
        )

        Application.put_env(:fathom, :shard_storage, S3)
        set_mode!(unquote(mode))
        %{store: store, lock_key: shard <> ".lock"}
      end

      test "a lock create that LANDED with its reply lost is given back",
           %{shard: shard, store: store, lock_key: lock_key} do
        fault =
          S3EtagStore.inject(
            store,
            %{method: "PUT", key: lock_key, cond: :if_none_match},
            {:transport_error, :timeout},
            apply: true
          )

        capture_log(fn ->
          assert {:error, {:lease_unavailable, %Req.TransportError{}}} = Shards.checkout(shard)
        end)

        assert S3EtagStore.fired(store, fault) == 1, "the fault never fired"
        assert S3EtagStore.body_of(store, lock_key) == nil, "the landed lock was left behind"
        assert Storage.lease_holder(shard) == :free

        # The next open is an ordinary fresh create at epoch 1.
        assert {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        assert %{"owner" => _, "epoch" => 1} =
                 store |> S3EtagStore.body_of(lock_key) |> Jason.decode!()
      end
    end
  end
end
