defmodule Fathom.Shard.Storage.S3LockPutStatusTest do
  @moduledoc """
  Expert review 2026-11-07 #11. `put_lock/3` mapped only 412. AWS answers a conditional PUT
  (`If-Match`) against a key that no longer exists with 404 `NoSuchKey` (MinIO says 412 — which is
  why it was invisible there), and a concurrent conflicting request with 409
  `ConditionalRequestConflict`. Unmapped, both were `{:error, {:s3_put_status, _}}`, which every
  caller reads as "transient, retry".

  The damage: node A is frozen past its TTL, B steals, serves, idles out and cleanly RELEASES (the
  lock is DELETED). A thaws and revalidates through `Storage.renew_lease` — a conditional PUT on the
  etag it cached — against a lock that is gone. A 404 became `:skip` in `Fence.revalidate`, which
  retries forever: A never self-fenced, kept serving stale reads, and refused every commit once its
  write fence armed, re-PUTting the lock every ~1-11 s.

  The invariants: a conditional PUT answered 404 `NoSuchKey` is the same verdict as 412
  (`:superseded` for a renew); 409 is a retryable shape; and a 404 `NoSuchBucket` is NEITHER — a
  mis-pointed or deleted bucket must not mass-self-fence every shard that writes.

  Driven against `S3EtagStore` told to behave like AWS (`set_missing_if_match_status/2`), because the
  default double answers 412 and cannot express the bug. The fence is exercised in both liveness
  modes: legacy (`acquire_gen: nil`, a per-shard renew) and heartbeat (a lapse ⇒ `:revalidate`, which
  renews).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Fence
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @shard "shard_lockput"
  @lock_key "shard_lockput.lock"
  @owner "a@node#1"

  setup do
    prev = Application.get_env(:fathom, S3)
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

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    %{store: store}
  end

  # The deposed owner's world: it holds a lease (with the etag it cached), and the stealer has since
  # released — deleted — the lock.
  defp deposed_owner(store) do
    S3EtagStore.set_missing_if_match_status(store, 404)
    assert {:ok, lease} = S3.acquire_lease(@shard, @owner, 30_000)

    assert is_binary(lease.lock_etag),
           "fixture: the lease must carry a cached etag (the fast path)"

    Agent.update(store, fn s -> %{s | objects: Map.delete(s.objects, @lock_key)} end)
    assert S3EtagStore.body_of(store, @lock_key) == nil, "fixture: the lock must be deleted"
    lease
  end

  defp fence_ctx(lease, acquire_gen),
    do: %{id: @shard, lease: lease, ttl_ms: 30_000, acquire_gen: acquire_gen}

  # The live storage, with the heartbeat answering as a lapse-then-restart would: valid_for_write
  # says `:revalidate` because the generation moved since acquire.
  defp fence_deps do
    %{
      valid_for_write: fn _gen -> :revalidate end,
      generation: fn -> 2 end,
      check_lease: &Fathom.Shard.Storage.check_lease/2,
      renew_lease: &S3.renew_lease/3
    }
  end

  describe "a conditional PUT against a DELETED lock (AWS: 404 NoSuchKey)" do
    test "renew_lease answers :superseded, not a generic status error", %{store: store} do
      lease = deposed_owner(store)

      assert {:error, :superseded} = S3.renew_lease(@shard, lease, 30_000)
    end

    test "legacy mode: the flush fence self-fences", %{store: store} do
      lease = deposed_owner(store)

      assert Fence.check(fence_ctx(lease, nil), fence_deps()) == :superseded
    end

    test "heartbeat mode: a lapse revalidation self-fences instead of skipping forever",
         %{store: store} do
      lease = deposed_owner(store)
      ctx = fence_ctx(lease, 1)

      # Assert the mode took: this really is the revalidate branch, not the legacy renew.
      assert fence_deps().valid_for_write.(ctx.acquire_gen) == :revalidate
      assert Fence.check(ctx, fence_deps()) == :superseded
    end

    test "a release marker over a deleted lock is :ok, not an error", %{store: store} do
      lease = deposed_owner(store)

      assert :ok = S3.release_lease_keeping_claim(@shard, lease, nil)
      assert S3EtagStore.body_of(store, @lock_key) == nil, "a release must not recreate the lock"
    end
  end

  describe "a 404 that is NOT NoSuchKey" do
    test "NoSuchBucket on a lock PUT is a generic error, so a bucket outage does not self-fence",
         %{store: store} do
      lease = deposed_owner(store)

      fault =
        S3EtagStore.inject(
          store,
          %{method: "PUT", key: @lock_key, cond: :if_match},
          {:status, 404, S3EtagStore.error_body("NoSuchBucket")},
          times: :infinity
        )

      assert {:error, {:s3_put_status, 404}} = S3.renew_lease(@shard, lease, 30_000)
      assert S3EtagStore.fired(store, fault) == 1, "the fault never fired"

      assert Fence.check(fence_ctx(lease, nil), fence_deps()) == :skip
      assert Fence.check(fence_ctx(lease, 1), fence_deps()) == :skip
    end

    test "a bare 404 with no error document is not trusted to mean NoSuchKey", %{store: store} do
      lease = deposed_owner(store)

      S3EtagStore.inject(store, %{method: "PUT", key: @lock_key, cond: :if_match}, {:status, 404})

      assert {:error, {:s3_put_status, 404}} = S3.renew_lease(@shard, lease, 30_000)
    end
  end

  describe "409 ConditionalRequestConflict" do
    test "on a conditional lock PUT is retryable, and the fence skips rather than self-fences",
         %{store: store} do
      assert {:ok, lease} = S3.acquire_lease(@shard, @owner, 30_000)

      fault =
        S3EtagStore.inject(
          store,
          %{method: "PUT", key: @lock_key, cond: :if_match},
          {:status, 409, S3EtagStore.error_body("ConditionalRequestConflict")},
          times: :infinity
        )

      assert {:error, {:transient_lookup, :conditional_conflict}} =
               S3.renew_lease(@shard, lease, 30_000)

      assert S3EtagStore.fired(store, fault) == 1, "the fault never fired"
      assert Fence.check(fence_ctx(lease, nil), fence_deps()) == :skip
      assert Fence.check(fence_ctx(lease, 1), fence_deps()) == :skip
    end

    test "on the lock CREATE is a retryable shape, not a bare status", %{store: store} do
      fault =
        S3EtagStore.inject(
          store,
          %{method: "PUT", key: @lock_key, cond: :if_none_match},
          {:status, 409, S3EtagStore.error_body("ConditionalRequestConflict")}
        )

      assert {:error, {:transient_lookup, :conditional_conflict}} =
               S3.acquire_lease(@shard, @owner, 30_000)

      assert S3EtagStore.fired(store, fault) == 1, "the fault never fired"
      assert S3EtagStore.body_of(store, @lock_key) == nil, "a conflict applied nothing"
    end
  end
end
