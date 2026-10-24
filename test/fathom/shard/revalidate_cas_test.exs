defmodule Fathom.Shard.RevalidateCasTest do
  @moduledoc """
  A heartbeat-lapse revalidation must WRITE the lock, so it serializes against a concurrent steal.

  Found by Jepsen 2026-10-24 (`jepsen/fathom --hard --target mixed`, run
  `store/fathom-mixed-hard/2026-10-24T01:48:50…`): MinIO froze for 22 s, past the lease TTL plus
  the steal margin. When it came back, fathom4 renewed its heartbeat, revalidated shard 9 with a
  READ-ONLY `check_lease` (the lock still named it), and resumed acking writes. A peer that had
  already read the lock and judged fathom4's heartbeat stale then won its `If-Match` steal, because
  a read leaves the lock's etag where it was. Both nodes acked writes to one shard: Elle reported
  G0 and incompatible list orders, two divergent histories of the same key.

  The steal is `get_lock` → heartbeat read → `put_lock If-Match <etag it read>`. If the owner's
  revalidation rewrites the lock (a fenced renew), a thief that read the lock before it 412s, and a
  thief that reads after it also reads the heartbeat after the owner renewed it, so it sees the
  owner live. Either the revalidation or the steal wins, never both.

  Runs against the real S3 backend over `S3EtagStore`: `Storage.Local` takes a lease under a
  mutex, so its steal cannot be interleaved and cannot express this race at all.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Fence, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  setup do
    shard = "revalcas_#{System.unique_integer([:positive])}"
    prev = for k <- [:shard_storage, S3], do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev,
          do:
            if(is_nil(v),
              do: Application.delete_env(:fathom, k),
              else: Application.put_env(:fathom, k, v)
            )
    end)

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
    %{shard: shard, store: store}
  end

  # The fence's view after a lapse: the heartbeat is valid again but its generation moved, so the
  # check routes to `revalidate/2`. Storage is the real S3 backend.
  defp lapse_deps do
    %{
      valid_for_write: fn _gen -> :revalidate end,
      generation: fn -> 2 end,
      check_lease: &Storage.check_lease/2,
      renew_lease: &Storage.renew_lease/3
    }
  end

  defp lock_key(shard), do: shard <> ".lock"

  # The thief's steal PUT, exactly as `acquire_existing/4` sends it: If-Match the etag it read.
  defp steal_put(store, shard, etag) do
    Req.new(plug: fn conn -> S3EtagStore.serve(conn, store) end, retry: false)
    |> Req.put!(
      url: "https://s3.example/b/#{lock_key(shard)}",
      headers: [{"if-match", etag}],
      body: ~s({"owner":"thief#1","epoch":2,"expires_at_ms":0})
    )
    |> Map.fetch!(:status)
  end

  test "a steal that read the lock before the owner's revalidation loses its If-Match",
       %{shard: shard, store: store} do
    {:ok, lease} = Storage.acquire_lease(shard, "owner#1", 10_000)

    # The thief reads the lock (and, in the real path, then reads a stale heartbeat).
    read_by_thief = S3EtagStore.etag_of(store, lock_key(shard))
    assert is_binary(read_by_thief), "fixture: the lock object must exist"

    ctx = %{id: shard, lease: lease, ttl_ms: 10_000, acquire_gen: 1}
    assert {:ok, %{lease: revalidated, acquire_gen: 2}} = Fence.check(ctx, lapse_deps())

    refute S3EtagStore.etag_of(store, lock_key(shard)) == read_by_thief,
           "the revalidation left the lock untouched, so the steal's If-Match still matches"

    assert steal_put(store, shard, read_by_thief) == 412,
           "the steal landed after the owner revalidated and resumed: two writers on one shard"

    # The owner keeps the etag it just wrote, so its later fenced lock writes still match.
    assert revalidated.lock_etag == S3EtagStore.etag_of(store, lock_key(shard))
  end

  test "a steal that landed first makes the revalidation self-fence", %{
    shard: shard,
    store: store
  } do
    {:ok, lease} = Storage.acquire_lease(shard, "owner#1", 10_000)
    read_by_thief = S3EtagStore.etag_of(store, lock_key(shard))
    assert steal_put(store, shard, read_by_thief) in 200..299

    ctx = %{id: shard, lease: lease, ttl_ms: 10_000, acquire_gen: 1}
    assert Fence.check(ctx, lapse_deps()) == :superseded
  end
end
