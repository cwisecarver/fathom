defmodule Fathom.Shard.Storage.S3SentinelTest do
  # Round-2 expert review #7: the steal-touch's 404 branch reasoned "nothing a
  # zombie flush could clobber that a nil-etag fence doesn't refuse" — the WRONG
  # direction. A brand-new shard's flush fence is If-None-Match:*, which succeeds
  # precisely when no object exists: shard created on A, never flushed; A stalls
  # mid-flush past the TTL; B steals (touch saw 404, no-op'd), serves, accepts
  # writes with etag nil; A unpauses and its create-only PUT LANDS; B's first flush
  # (also If-None-Match:*) then 412s → self-fence → B's acknowledged writes deleted,
  # the store durably holds the zombie's pre-steal lineage. The invariant: a steal
  # of a never-flushed shard plants a SENTINEL at the data key so the zombie's
  # create 412s, and the stealer fences its first flush with the sentinel's etag.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @shard "shard_new"
  @data_key "shard_new.db"
  @lock_key "shard_new.lock"
  @sentinel_meta "x-amz-meta-fathom-sentinel"

  defp dead_lock do
    Storage.encode_lease(%{
      owner: "dead@node#old",
      epoch: 5,
      expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
    })
  end

  defp start_store(objects) do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})
    prev = Application.get_env(:fathom, S3)

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

    store
  end

  defp tmp_db(content) do
    path = Path.join(System.tmp_dir!(), "sent_#{System.unique_integer([:positive])}.db")
    File.write!(path, content)
    on_exit(fn -> File.rm(path) end)
    path
  end

  test "stealing a never-flushed shard plants a sentinel that fences the zombie's create" do
    store = start_store(%{@lock_key => dead_lock()})

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "b@node#inc2", 30_000)

    assert S3EtagStore.meta_of(store, @data_key)[@sentinel_meta] == "1",
           "the steal must plant a sentinel at the data key (pre-fix: 404 no-op'd)"

    # The zombie (old owner A) unpauses and lands its brand-new create-only flush.
    zombie_file = tmp_db("the-zombie-lineage")

    assert {:error, :superseded} = S3.flush(@shard, zombie_file, nil),
           "the zombie's If-None-Match:* create must 412 against the sentinel " <>
             "(pre-fix it CREATED the object and the stealer later self-fenced)"

    assert S3EtagStore.meta_of(store, @data_key)[@sentinel_meta] == "1",
           "the sentinel survives the zombie's refused create"
  end

  test "the stealer's pull reads the sentinel as brand-new and fences with its etag" do
    store = start_store(%{@lock_key => dead_lock()})
    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "b@node#inc2", 30_000)

    dest = Path.join(System.tmp_dir!(), "sentpull_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> File.rm(dest) end)

    # Brand-new semantics: NO local file materializes (the sentinel is not shard bytes) —
    # but the sentinel's etag comes back as the first flush's fence.
    #
    # `{:absent, etag}`, not `{:ok, etag}` (expert review 2026-08-01 #24). This test already
    # asserted `refute File.exists?(dest)` below, i.e. it always KNEW no bytes were written —
    # while the return value said otherwise, and every pull-then-open consumer believed the
    # return value and opened the missing path, which CREATES an empty database. The fencing
    # property this test exists for is unchanged: the sentinel's etag still comes back and the
    # first real flush still replaces it via If-Match.
    assert {:absent, sentinel_etag} = S3.pull(@shard, dest)
    assert sentinel_etag == S3EtagStore.etag_of(store, @data_key)
    refute File.exists?(dest), "the sentinel placeholder must never be promoted as shard bytes"

    # The stealer's first real flush replaces the sentinel via If-Match.
    real_file = tmp_db("the-stealer-lineage")
    assert {:ok, _new_etag} = S3.flush(@shard, real_file, sentinel_etag)
    assert S3EtagStore.body_of(store, @data_key) == "the-stealer-lineage"
    assert S3EtagStore.meta_of(store, @data_key)[@sentinel_meta] == nil
  end

  test "a second steal over a sentinel refreshes it: etag rotates, sentinel semantics kept" do
    store = start_store(%{@lock_key => dead_lock()})
    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "b@node#inc2", 30_000)
    first_etag = S3EtagStore.etag_of(store, @data_key)

    # Stealer B dies before its first flush; contender C steals.
    Agent.update(store, fn s ->
      %{
        s
        | objects: Map.put(s.objects, @lock_key, %{body: dead_lock(), form: :single, meta: %{}})
      }
    end)

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "c@node#inc3", 30_000)

    assert S3EtagStore.etag_of(store, @data_key) != first_etag,
           "the refresh must rotate the etag so B's zombie If-Match 412s"

    assert S3EtagStore.meta_of(store, @data_key)[@sentinel_meta] == "1",
           "a form-rotation copy would strip the sentinel meta; the refresh must keep it"
  end

  test "the follower treats a sentinel as absent (nothing to warm)" do
    store = start_store(%{@lock_key => dead_lock()})
    assert {:ok, _} = S3.acquire_lease(@shard, "b@node#inc2", 30_000)
    assert S3EtagStore.meta_of(store, @data_key)[@sentinel_meta] == "1"

    dest = Path.join(System.tmp_dir!(), "sentwarm_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> File.rm(dest) end)

    assert {:ok, :absent} = S3.pull_if_changed(@shard, dest, nil)
    refute File.exists?(dest)
  end
end
