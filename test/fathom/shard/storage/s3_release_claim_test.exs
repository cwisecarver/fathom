defmodule Fathom.Shard.Storage.S3ReleaseClaimTest do
  # Expert review 2026-10-12 #3, the S3 half: `release_lease_keeping_claim/3` replaces the lock with
  # the released marker under the SAME conditions `release_lease/2` deletes it (If-Match the lock we
  # last wrote; a 412 still-ours retries, a 412 someone-else's is a no-op), and the next acquire
  # STEALS the marker, claiming strictly above it. Driven against the content-etag S3 double, so the
  # If-Match semantics are the store's, not a hand-returned status.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @shard "shard_rc"
  @lock_key "shard_rc.lock"

  setup do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{"shard_rc.db" => "bytes"}) end})
    prev = Application.get_env(:fathom, S3)
    prev_repl = Application.get_env(:fathom, :replication_enabled)

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

      if is_nil(prev_repl),
        do: Application.delete_env(:fathom, :replication_enabled),
        else: Application.put_env(:fathom, :replication_enabled, prev_repl)
    end)

    %{store: store}
  end

  defp lock(store) do
    {:ok, lease} = Storage.decode_lease(S3EtagStore.body_of(store, @lock_key))
    lease
  end

  test "the release leaves the marker, and the next acquire steals strictly above its claim", %{
    store: store
  } do
    {:ok, a} = S3.acquire_lease(@shard, "a@node#1", 30_000)
    assert :ok = S3.release_lease_keeping_claim(@shard, a, 5)

    assert lock(store) == %{
             owner: Storage.released_owner(),
             epoch: a.epoch,
             expires_at_ms: 0,
             claim: 5
           }

    assert S3.lease_holder(@shard) == :free
    assert S3.check_lease(@shard, a) == {:error, :superseded}

    Application.put_env(:fathom, :replication_enabled, true)
    assert {:ok, b} = S3.acquire_lease(@shard, "b@node#2", 30_000)
    assert b.took_over
    assert b.claim > 5, "the next owner must claim above the released holder's lineage"
  end

  test "a stale cached etag on a lock still ours is retried, not reported as a release", %{
    store: store
  } do
    {:ok, a} = S3.acquire_lease(@shard, "a@node#1", 30_000)
    stale = %{a | lock_etag: "\"not-the-current-etag\""}

    assert :ok = S3.release_lease_keeping_claim(@shard, stale, 4)
    assert lock(store).owner == Storage.released_owner()
    assert lock(store).claim == 4
  end

  test "a lock that is someone else's is left alone", %{store: store} do
    {:ok, a} = S3.acquire_lease(@shard, "a@node#1", 30_000)

    Agent.update(store, fn s ->
      body =
        Storage.encode_lease(%{owner: "b@node#2", epoch: 9, expires_at_ms: Storage.now_ms() + 1})

      %{s | objects: Map.put(s.objects, @lock_key, %{body: body, form: :single, meta: %{}})}
    end)

    assert :ok = S3.release_lease_keeping_claim(@shard, a, 4)
    assert lock(store).owner == "b@node#2"
  end
end
