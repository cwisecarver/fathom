defmodule Fathom.Shard.Storage.S3ReclaimIdentityTest do
  # Audit 2026-11-22 #4 part 2. A same-owner RECLAIM (`acquire_existing/4`) keeps `{owner, epoch}` and
  # only raises the CLAIM. `renew_lease` (after a 412 against our own write, and the no-etag legacy
  # path) and the no-etag releases matched on owner + epoch alone, with a comment saying "a steal
  # always bumps the epoch" that is false for a reclaim. So a coordinator's OLD lease could renew over
  # the reclaimer's lock (regressing its claim, dropping its markers) or release it. The identity is
  # now `own_lock?/2`: owner, epoch and claim, the one the release fast path already used (Jepsen
  # 2026-11-14 H6). The control tests pin that a legitimate owner (same claim) is NOT refused.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.Env
  alias Fathom.Test.S3EtagStore

  @shard "shard_reclaim"
  @lock_key "shard_reclaim.lock"
  @owner "owner@node#inc1"

  setup do
    expires = Storage.now_ms() + 60_000

    store =
      start_supervised!(
        {Agent,
         fn ->
           S3EtagStore.initial(%{
             "shard_reclaim.db" => "bytes",
             @lock_key =>
               Storage.encode_lease(%{
                 owner: @owner,
                 epoch: 1,
                 claim: 7,
                 expires_at_ms: expires
               })
           })
         end}
      )

    Env.put!(S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    %{store: store, expires: expires}
  end

  defp lock_claim(store) do
    {:ok, lock} = Storage.decode_lease(S3EtagStore.body_of(store, @lock_key))
    Map.get(lock, :claim)
  end

  defp old_lease(claim, extra \\ %{}) do
    Map.merge(%{owner: @owner, epoch: 1, claim: claim, expires_at_ms: 1}, extra)
  end

  test "renew with a stale etag over a reclaimer's lock (same owner+epoch, higher claim) is superseded",
       %{store: store} do
    assert {:error, :superseded} =
             S3.renew_lease(@shard, old_lease(5, %{lock_etag: "stale"}), 30_000)

    assert lock_claim(store) == 7, "the old lease's renew regressed the reclaimer's claim"
  end

  test "renew with NO cached etag (the by-read path) over a reclaimer's lock is superseded",
       %{store: store} do
    assert {:error, :superseded} = S3.renew_lease(@shard, old_lease(5), 30_000)
    assert lock_claim(store) == 7
  end

  test "the legacy self-race still renews: same claim, stale etag", %{store: store} do
    assert {:ok, _} = S3.renew_lease(@shard, old_lease(7, %{lock_etag: "stale"}), 30_000)
    assert lock_claim(store) == 7
  end

  test "release by read of an OLD lease leaves the reclaimer's lock alone", %{store: store} do
    assert :ok = S3.release_lease(@shard, old_lease(5))
    assert lock_claim(store) == 7, "the old lease's release deleted the reclaimer's lock"
  end

  test "release-keeping-claim by read of an OLD lease leaves the reclaimer's lock alone",
       %{store: store} do
    assert :ok = S3.release_lease_keeping_claim(@shard, old_lease(5), 5)
    assert lock_claim(store) == 7
  end

  # A failed READ of the lock is not "not ours". The no-etag release used to fold it into `:ok`, so a
  # GET blip reported the release as done while the lock still named this node; the coordinator's retry
  # ladder and its `release_failed` log ("the lock may still name this node") only run on an error,
  # which the etag fast path always returned. Both by-read releases now surface it.
  test "a failed read of the lock surfaces as an error, not as a completed release", %{
    store: store
  } do
    id =
      S3EtagStore.inject(store, %{method: "GET", key: @lock_key}, {:status, 500},
        times: :infinity
      )

    assert {:error, _} = S3.release_lease(@shard, old_lease(7))
    assert {:error, _} = S3.release_lease_keeping_claim(@shard, old_lease(7), 7)
    assert S3EtagStore.fired(store, id) >= 2, "the fault never fired: this tested the happy path"

    S3EtagStore.clear_faults(store)
    assert lock_claim(store) == 7, "the lock was touched while the read was failing"
  end

  test "release by read of the CURRENT lease still releases", %{store: store} do
    assert :ok = S3.release_lease(@shard, old_lease(7))
    assert S3EtagStore.body_of(store, @lock_key) == nil
  end

  # Audit 2026-11-22 #7. A store that ignores If-Match on DELETE (MinIO, the chaos rig under
  # `:allow_weak_conditional_delete`) answered 204 to a superseded coordinator's clean idle drop and
  # deleted the LIVE owner's lock; real S3 answers 412. The release now reads first under that flag.
  describe "under :allow_weak_conditional_delete (a store whose DELETE ignores If-Match)" do
    setup %{store: store} do
      S3EtagStore.set_weak_delete(store, true)
      Env.put!(:allow_weak_conditional_delete, true)
      :ok
    end

    test "a release whose lease no longer matches the lock (a peer owns it) leaves it alone", %{
      store: store
    } do
      peer_lock = S3EtagStore.body_of(store, @lock_key)

      stale = %{owner: "zombie@node#inc0", epoch: 1, claim: 7, lock_etag: "old", expires_at_ms: 1}
      assert :ok = S3.release_lease(@shard, stale)

      assert S3EtagStore.body_of(store, @lock_key) == peer_lock,
             "a superseded coordinator's release deleted the live owner's lock"
    end

    test "a release of our own lock still releases it", %{store: store} do
      lease = old_lease(7, %{lock_etag: "stale-but-ours"})
      assert :ok = S3.release_lease(@shard, lease)
      assert S3EtagStore.body_of(store, @lock_key) == nil
    end
  end
end
