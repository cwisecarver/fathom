defmodule Fathom.Shard.Storage.S3StealSingleHeadTest do
  # Expert review 2026-10-17 #14 step 1: a replicating steal HEADed the data object twice, serially —
  # `takeover_claim`'s `object_head` for the lineage, then `touch_object`'s `head_sized` for the etag
  # the touch If-Matches — the same key, one lock PUT apart. One HEAD now serves both.
  #
  # "No semantic change" is the claim that needs pinning, not just the count: the touch now acts on a
  # head read BEFORE the lock PUT, so a zombie flush that lands in between must still be caught. Every
  # touch form is conditional on the etag it read, so the touch 412s and re-runs with a fresh HEAD;
  # the lease's `touch_pre_etag` must then name the ZOMBIE's object (the one actually rotated), which
  # is what the takeover revalidation fences on.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @shard "shard_x"
  @data_key "shard_x.db"
  @lock_key "shard_x.lock"

  setup do
    prev = Application.get_env(:fathom, S3)
    prev_repl = Application.get_env(:fathom, :replication_enabled)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    # `Storage.takeover_claim` reads through the Storage DISPATCH, which is `FaultyStorage` in the
    # suite: without this the pre-fix claim HEAD went to the double, not to this plug, and the count
    # could not see it (the probe against the unfixed code caught exactly that).
    Application.put_env(:fathom, :shard_storage, S3)

    on_exit(fn ->
      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)

      if is_nil(prev_repl),
        do: Application.delete_env(:fathom, :replication_enabled),
        else: Application.put_env(:fathom, :replication_enabled, prev_repl)
    end)

    :ok
  end

  defp dead_lock do
    Storage.encode_lease(%{
      owner: "dead@node#old",
      epoch: 5,
      expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
    })
  end

  # `on_lock_put` runs once, just before the store applies the steal's lock PUT.
  defp start_store!(on_lock_put \\ fn _store -> :ok end) do
    store =
      start_supervised!(
        {Agent,
         fn -> S3EtagStore.initial(%{@data_key => "shard-bytes", @lock_key => dead_lock()}) end}
      )

    test = self()
    fired = :counters.new(1, [])

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn ->
        send(test, {:s3, conn.method, conn.request_path})

        # The STEAL write is the If-Match lock PUT (the optimistic create before it is If-None-Match).
        if conn.method == "PUT" and String.ends_with?(conn.request_path, @lock_key) and
             Plug.Conn.get_req_header(conn, "if-match") != [] and :counters.get(fired, 1) == 0 do
          :counters.add(fired, 1, 1)
          on_lock_put.(store)
        end

        S3EtagStore.serve(conn, store)
      end
    )

    store
  end

  defp data_heads do
    Stream.repeatedly(fn ->
      receive do
        {:s3, m, p} -> {m, p}
      after
        0 -> nil
      end
    end)
    |> Enum.take_while(& &1)
    |> Enum.count(fn {m, p} -> m == "HEAD" and String.ends_with?(p, @data_key) end)
  end

  test "a replicating steal HEADs the data object once for the claim and the touch" do
    Application.put_env(:fathom, :replication_enabled, true)
    store = start_store!()
    before = S3EtagStore.etag_of(store, @data_key)

    assert {:ok, lease} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    assert lease.took_over == true
    assert is_integer(lease.claim), "the claim must still be computed from the shared head"
    assert lease.touch_pre_etag == before
    assert lease.touch_post_etag == S3EtagStore.etag_of(store, @data_key)
    refute lease.touch_post_etag == before, "the touch must still rotate the etag"

    assert data_heads() == 1, "the steal read the same object twice (claim HEAD + touch HEAD)"
  end

  test "not replicating: no claim, and the touch still HEADs for itself (once)" do
    Application.put_env(:fathom, :replication_enabled, false)
    _store = start_store!()

    assert {:ok, lease} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    refute Map.has_key?(lease, :claim)
    assert data_heads() == 1
  end

  test "a zombie flush landing after the shared HEAD is caught: the touch re-reads and rotates it" do
    Application.put_env(:fathom, :replication_enabled, true)

    # Between our HEAD and the touch — on the steal's lock PUT — the dead owner's stalled flush lands.
    store = start_store!(fn store -> S3EtagStore.set_body(store, @data_key, "zombie-bytes") end)

    original = S3EtagStore.etag_of(store, @data_key)
    assert {:ok, lease} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    zombie = lease.touch_pre_etag
    refute zombie == original, "the touch fenced on the stale pre-zombie head"

    # The touch rotated the ZOMBIE's object (its If-Match on the stale head 412'd, then it re-ran on
    # a fresh one), so the revalidation fences on the bytes really there.
    assert S3EtagStore.plain_body_of(store, @data_key) == "zombie-bytes"
    assert lease.touch_post_etag == S3EtagStore.etag_of(store, @data_key)
    refute lease.touch_post_etag == zombie
    assert data_heads() == 2, "the retry must take a fresh HEAD"
  end
end
