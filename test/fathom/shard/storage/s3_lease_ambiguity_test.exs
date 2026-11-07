defmodule Fathom.Shard.Storage.S3LeaseAmbiguityTest do
  @moduledoc """
  Expert review 2026-11-07 #6. A steal is two writes: the lock PUT (`{us, epoch+1}`), then the
  data-object touch that makes a zombie's already-fenced `If-Match` flush 412. `Req` never retries a
  PUT, so a lock PUT whose REPLY is lost (a reset connection, a 5xx from a store that applied it)
  surfaces as an error for a lock that now exists. Pre-fix the lock was left reading `{us, epoch+1}`
  and the retry took the SAME-OWNER RECLAIM: no steal-touch, no `took_over`, no `prev_owner`. A
  stalled zombie's fenced flush could still land after our pull; promote-on-open skipped the
  takeover ask (a warm local copy skipped promotion entirely); and the previous owner's "I hold
  nothing" counted toward the recovery responder floor.

  The invariant: whatever happens to the steal PUT's reply, the retry returns the lease a COMPLETED
  steal returns — `took_over`, `prev_owner`, the touch's etags, and a rotated data object.

  The mechanism is an interrupted-steal marker (`touch_pending` + `prev_owner`) written in the SAME
  PUT as the steal, so it exists exactly when the lock does. A rollback to the dead owner at
  `epoch + 2` is the belt, and it cannot be the mechanism: it cannot close a PUT that is merely
  delayed, and it fails exactly when its own read fails in the brownout that lost the reply. Both
  halves are pinned separately here — the second test makes the rollback impossible.

  Driven against `S3EtagStore` with its apply-then-fail fault: the lock PUT LANDS and the caller is
  told it was lost, which no double could express before (`S3EtagStoreFaultsTest` pins that the
  capability means what it says). Each test asserts its fault fired.

  Both heartbeat shapes of the dead owner are covered — a stale heartbeat object (what a crashed
  node in heartbeat mode leaves) and none at all (legacy per-shard renewal, liveness from the lock's
  own TTL) — because they reach the steal through different liveness reads.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.PromoteOnOpen
  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  # The rollback-cannot-run cases log the stranded-lock error by design.
  @moduletag :capture_log

  @shard "shard_amb"
  @data_key "shard_amb.db"
  @lock_key "shard_amb.lock"
  @us "new@node#1"
  # A 403 is not retried by Req (500/503 are, with backoff), so this fails a read quickly. All the
  # tests need is that the read FAILS, as it does in the brownout that lost the lock PUT's reply.
  @read_fails {:status, 403}

  setup do
    prev = Application.get_env(:fathom, S3)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    :ok
  end

  for hb <- [:stale_heartbeat, :no_heartbeat] do
    describe "dead owner with #{hb}" do
      setup do
        {:ok, ctx} = start_store(unquote(hb))
        ctx
      end

      test "a lost reply on the steal PUT: the failed acquire does not leave a bare {us, epoch+1}",
           ctx do
        %{store: store, prev: prev} = ctx
        fault = lose_lock_reply(store)

        assert {:error, %Req.TransportError{}} = S3.acquire_lease(@shard, @us, 30_000)
        assert S3EtagStore.fired(store, fault) == 1, "the fault never fired"

        # The PUT landed (the fault applies it) and was then rolled back to the dead owner's
        # identity at a HIGHER epoch, so a peer is not left reading a live node's lock and the
        # retry re-enters the full steal.
        lock = lock_body(store)
        assert lock["owner"] == prev
        assert lock["epoch"] > 6

        assert {:ok, lease} = S3.acquire_lease(@shard, @us, 30_000)
        assert_completed_steal(lease, ctx)
      end

      test "the rollback CANNOT run (its read fails): the lock itself still says it was a steal",
           ctx do
        %{store: store, prev: prev} = ctx
        fault = lose_lock_reply(store)

        reads =
          S3EtagStore.inject(store, %{method: "GET", key: @lock_key, after: fault}, @read_fails,
            times: :infinity
          )

        assert {:error, %Req.TransportError{}} = S3.acquire_lease(@shard, @us, 30_000)
        assert S3EtagStore.fired(store, fault) == 1, "the fault never fired"
        assert S3EtagStore.fired(store, reads) > 0, "the rollback never tried to read the lock"
        S3EtagStore.clear_faults(store)

        # Stranded at our own epoch+1 — pre-fix a bare lock, which the retry reclaimed UNFENCED.
        lock = lock_body(store)
        assert lock["owner"] == @us
        assert lock["epoch"] == 6
        assert lock["touch_pending"] == true, "a stranded steal must say it is one"
        assert lock["prev_owner"] == prev

        assert {:ok, lease} = S3.acquire_lease(@shard, @us, 30_000)
        assert_completed_steal(lease, ctx)
        assert lease.epoch == 6, "the resumed steal must not move the epoch"
      end

      test "a steal PUT that did NOT land (5xx before applying): the rollback is a no-op", ctx do
        %{store: store, prev: prev, lock_before: lock_before} = ctx

        fault =
          S3EtagStore.inject(
            store,
            %{method: "PUT", key: @lock_key, cond: :if_match},
            {:status, 503}
          )

        assert {:error, {:s3_put_status, 503}} = S3.acquire_lease(@shard, @us, 30_000)
        assert S3EtagStore.fired(store, fault) == 1, "the fault never fired"

        assert lock_body(store) == Jason.decode!(lock_before),
               "nothing landed, so the dead owner's lock must be exactly as it was"

        assert {:ok, lease} = S3.acquire_lease(@shard, @us, 30_000)
        assert lease.took_over == true
        assert lease.prev_owner == prev
      end

      test "the touch fails AND the rollback cannot read: the retry still redoes the touch",
           ctx do
        %{store: store} = ctx
        # The touch's write to the DATA key (the re-nonced PUT) fails once; from then on the lock
        # cannot be read, so the rollback of the already-landed steal cannot run. Pre-fix
        # `restore_lock/4` read a failed GET as "nothing to roll back" (`_ -> :ok`) and returned.
        touch =
          S3EtagStore.inject(store, %{method: "PUT", key: @data_key}, {:status, 500})

        reads =
          S3EtagStore.inject(store, %{method: "GET", key: @lock_key, after: touch}, @read_fails,
            times: :infinity
          )

        assert {:error, {:transient_lookup, {:touch_failed, _}}} =
                 S3.acquire_lease(@shard, @us, 30_000)

        assert S3EtagStore.fired(store, touch) == 1, "the touch fault never fired"
        assert S3EtagStore.fired(store, reads) > 0, "the rollback never tried to read the lock"
        S3EtagStore.clear_faults(store)

        lock = lock_body(store)
        assert lock["owner"] == @us and lock["touch_pending"] == true

        assert {:ok, lease} = S3.acquire_lease(@shard, @us, 30_000)
        assert_completed_steal(lease, ctx)
      end

      test "a resumed steal whose touch fails again keeps the marker", ctx do
        %{store: store} = ctx
        lost = lose_lock_reply(store)

        S3EtagStore.inject(store, %{method: "GET", key: @lock_key, after: lost}, @read_fails,
          times: :infinity
        )

        assert {:error, %Req.TransportError{}} = S3.acquire_lease(@shard, @us, 30_000)
        assert S3EtagStore.fired(store, lost) == 1
        S3EtagStore.clear_faults(store)
        assert lock_body(store)["touch_pending"] == true, "fixture: the steal must be stranded"

        touch = S3EtagStore.inject(store, %{method: "PUT", key: @data_key}, {:status, 500})

        assert {:error, {:transient_lookup, {:touch_failed, _}}} =
                 S3.acquire_lease(@shard, @us, 30_000)

        assert S3EtagStore.fired(store, touch) == 1, "the touch fault never fired"

        lock = lock_body(store)
        assert lock["owner"] == @us, "a resume must not hand the lock away"
        assert lock["touch_pending"] == true, "the marker was lost while the touch was still owed"

        assert {:ok, lease} = S3.acquire_lease(@shard, @us, 30_000)
        assert_completed_steal(lease, ctx)
      end

      test "a renew clears the marker: it does not outlive the steal in the lock body", ctx do
        %{store: store} = ctx
        assert {:ok, lease} = S3.acquire_lease(@shard, @us, 30_000)

        assert lock_body(store)["touch_pending"] == true,
               "fixture: the steal's own body carries the marker until something rewrites it"

        assert {:ok, renewed} = S3.renew_lease(@shard, lease, 30_000)
        refute Map.has_key?(lock_body(store), "touch_pending")
        refute Map.has_key?(lock_body(store), "prev_owner")

        # What the coordinator keeps from the steal survives the renew (existing behaviour).
        assert renewed.touch_post_etag == lease.touch_post_etag
      end

      test "a stranded steal is given back as a CLEAN marker that still names the previous holder",
           ctx do
        %{store: store, prev: prev, data_etag: seed_etag} = ctx
        lost = lose_lock_reply(store)

        S3EtagStore.inject(store, %{method: "GET", key: @lock_key, after: lost}, @read_fails,
          times: :infinity
        )

        assert {:error, %Req.TransportError{}} = S3.acquire_lease(@shard, @us, 30_000)
        assert S3EtagStore.fired(store, lost) == 1, "the fault never fired"
        S3EtagStore.clear_faults(store)
        assert lock_body(store)["touch_pending"] == true, "fixture: the steal must be stranded"

        assert :ok = S3.release_if_owned(@shard, @us)

        # Never deleted (the next acquire would be a fresh create at epoch 1 with no touch).
        marker = lock_body(store)
        assert marker["owner"] == Storage.released_owner()
        assert marker["epoch"] == 6
        assert marker["released_by"] == prev
        refute Map.has_key?(marker, "touch_pending")
        assert S3.lease_holder(@shard) == :free

        # A peer takes the marker at once (no steal watch) and goes ABOVE the epoch, with a touch.
        assert {:ok, lease} = S3.acquire_lease(@shard, "peer@node#2", 30_000)
        assert lease.took_over == true and lease.epoch == 7
        assert Storage.floor_owner(lease) == prev
        assert lease.touch_pre_etag == seed_etag
      end

      test "an ordinary lock of ours above epoch 1 is left alone", %{store: store} do
        assert {:ok, lease} = S3.acquire_lease(@shard, @us, 30_000)
        # A renew rewrites the body without the steal marker: now it is an ordinary epoch-6 lock,
        # which may be a live predecessor's of this very node.
        assert {:ok, _} = S3.renew_lease(@shard, lease, 30_000)
        refute Map.has_key?(lock_body(store), "touch_pending"), "fixture: the marker must be gone"
        before = S3EtagStore.body_of(store, @lock_key)

        assert :ok = S3.release_if_owned(@shard, @us)
        assert S3EtagStore.body_of(store, @lock_key) == before
      end
    end
  end

  # --- helpers ---

  defp lose_lock_reply(store) do
    S3EtagStore.inject(
      store,
      %{method: "PUT", key: @lock_key, cond: :if_match},
      {:transport_error, :timeout},
      apply: true
    )
  end

  defp assert_completed_steal(lease, %{store: store, prev: prev, data_etag: seed_etag}) do
    assert lease.took_over == true, "the retry was an UNFENCED same-owner reclaim"
    assert lease.prev_owner == prev
    assert PromoteOnOpen.took_from_another_holder?(lease, @us)
    assert Storage.floor_owner(lease) == prev

    assert lease.touch_pre_etag == seed_etag
    assert is_binary(lease.touch_post_etag)
    assert lease.touch_post_etag == S3EtagStore.etag_of(store, @data_key)
    assert lease.touch_post_etag != seed_etag, "the data object's etag was never rotated"

    refute Map.has_key?(lease, :touch_pending), "the marker is lock-body state, not lease state"
  end

  defp lock_body(store), do: store |> S3EtagStore.body_of(@lock_key) |> Jason.decode!()

  defp start_store(hb) do
    prev = "dead@node#" <> Integer.to_string(System.unique_integer([:positive]))
    long_dead = Storage.now_ms() - Storage.steal_margin_ms() - 60_000

    lock = Storage.encode_lease(%{owner: prev, epoch: 5, expires_at_ms: long_dead})
    objects = %{@data_key => "shard bytes", @lock_key => lock}

    objects =
      if hb == :stale_heartbeat,
        do:
          Map.put(
            objects,
            "heartbeats/" <> URI.encode_www_form(prev),
            Storage.encode_heartbeat(%{owner: prev, expires_at_ms: long_dead})
          ),
        else: objects

    store = start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    # Assert the fixture is the shape we named, not just that we asked for it.
    case hb do
      :stale_heartbeat -> assert {:ok, %{owner: ^prev}} = S3.read_heartbeat(prev)
      :no_heartbeat -> assert :not_found = S3.read_heartbeat(prev)
    end

    assert S3.lease_holder(@shard) == :free, "the dead owner must be stealable"

    {:ok,
     %{
       store: store,
       prev: prev,
       lock_before: lock,
       data_etag: S3EtagStore.etag_of(store, @data_key)
     }}
  end
end
