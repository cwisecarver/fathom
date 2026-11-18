defmodule Fathom.Shard.Storage.S3StaleReleaseTest do
  # Jepsen 2026-11-14 hard-1 hunt, H6. A conditional release that comes back 412 re-reads the lock
  # to decide whether it is still OURS at a different etag (`Storage.resolve_stale_release/4`). It
  # used to match the OWNER STRING alone, so an OLDER lease of an incarnation released late, after a
  # newer lease of the same incarnation had re-acquired, and either deleted the newer lock or
  # overwrote it with a released marker carrying the older, LOWER claim: a live owner whose lock
  # vanished or regressed, so the next opener fresh-created at a lineage the live owner already
  # claims.
  #
  # The identity is owner + epoch + claim. Under replication a same-owner reclaim raises the claim,
  # which is what separates the two leases; with replication off there are no claims and this test
  # does not claim to cover it (no lineage is at stake there).
  #
  # Storage-level on purpose: the shape needs two live leases of one owner string, which the
  # per-node registry never lets two coordinators hold at once, so it cannot be built through
  # `Shards`. It runs against `S3EtagStore`, which enforces the real conditional-write contract.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.Env
  alias Fathom.Test.S3EtagStore

  @shard "shard_stale"
  @lock_key "shard_stale.lock"

  setup do
    Env.put!(:shard_storage, S3)
    Env.put!(:replication_enabled, true)
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{"shard_stale.db" => "b"}) end})

    Env.put!(S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    %{store: store}
  end

  defp stored_lock(store) do
    case S3EtagStore.plain_body_of(store, @lock_key) do
      nil -> nil
      body -> elem(Storage.decode_lease(body), 1)
    end
  end

  # Two leases of ONE owner string: the second is a same-owner reclaim of the first.
  defp two_leases! do
    {:ok, older} = S3.acquire_lease(@shard, "X#inc", 30_000)
    {:ok, newer} = S3.acquire_lease(@shard, "X#inc", 30_000)
    assert newer.claim > Map.get(older, :claim, 0), "fixture: the reclaim must raise the claim"
    refute newer.lock_etag == older.lock_etag, "fixture: the older lease's etag must be stale"
    {older, newer}
  end

  test "a plain release by an older lease of the same owner leaves the newer lock", %{
    store: store
  } do
    {older, newer} = two_leases!()

    Storage.release_lease(@shard, older)

    assert %{owner: "X#inc", claim: claim} = stored_lock(store)
    assert claim == newer.claim, "the newer lease's lock was deleted or rewritten"
  end

  test "a keeping-claim release by an older lease leaves the newer lock, not a lower-claim marker",
       %{store: store} do
    {older, newer} = two_leases!()

    Storage.release_lease(@shard, older, 1)

    assert %{owner: "X#inc", claim: claim} = lock = stored_lock(store)

    assert claim == newer.claim,
           "the newer lease's lock was replaced by a marker with the older claim: #{inspect(lock)}"
  end

  # The case `resolve_stale_release/4` exists for must keep working: our OWN legacy renew rewrites
  # the lock (new etag, same epoch and claim), so a release with the pre-renew lease still reads the
  # lock as ours and clears it instead of stranding it behind a live heartbeat.
  test "a release with the pre-renew lease still clears the lock our own renew rewrote", %{
    store: store
  } do
    {:ok, lease} = S3.acquire_lease(@shard, "X#inc", 30_000)
    assert {:ok, renewed} = S3.renew_lease(@shard, lease, 30_000)
    refute renewed.lock_etag == lease.lock_etag, "fixture: the renew must rotate the etag"
    assert %{owner: "X#inc"} = stored_lock(store)

    Storage.release_lease(@shard, lease)

    assert stored_lock(store) == nil,
           "the lock stayed behind: our own rotation was read as foreign"
  end

  # The same control with a CLAIMED lease (a takeover or reclaim under replication): the renew
  # carries the claim, so the identity check must still read the rewritten lock as ours.
  test "a release with a pre-renew CLAIMED lease still clears the lock our own renew rewrote", %{
    store: store
  } do
    {_older, claimed} = two_leases!()
    assert {:ok, renewed} = S3.renew_lease(@shard, claimed, 30_000)
    refute renewed.lock_etag == claimed.lock_etag, "fixture: the renew must rotate the etag"
    assert %{claim: claim} = stored_lock(store)
    assert claim == claimed.claim, "fixture: the renew must keep the claim"

    Storage.release_lease(@shard, claimed)

    assert stored_lock(store) == nil,
           "the lock stayed behind: our own claimed rotation was foreign"
  end
end
