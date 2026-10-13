defmodule Fathom.Shard.Storage.S3StealTouchTest do
  # Expert review #3: the steal protocol never touched the DATA object, so an old
  # owner stalled inside a fenced flush it had already passed the fence for could
  # land its PUT after the new owner's pull — the If-Match fence arbitrated in
  # favor of the ZOMBIE. The invariant: a steal invalidates the data object's etag
  # before the new owner serves, so the zombie's late PUT deterministically 412s.
  #
  # Round-2 #4 hardened WHAT "invalidates" means: on an MD5-etag store (every
  # non-multipart object — exactly fathom's single-PUT uploads) a plain self-copy
  # of identical bytes produces the SAME etag, so the old test's hand-returned
  # "changed" etag proved only request shape. This suite drives the steal against
  # Fathom.Test.S3EtagStore, whose etags are content-derived like real S3, and a
  # store where nothing rotates must fail the steal closed.
  #
  # Expert review 2026-10-12 N1 hardened it again: the rotation used to ALTERNATE copy
  # forms (single <-> one-part multipart), which for identical bytes has only two etags,
  # so a second steal returned the object to the first etag and re-armed a zombie's
  # If-Match. These tests previously asserted that alternation; they now pin that each
  # touch reaches an etag the key never had (the small-object touch re-nonces the stored
  # bytes; s3_touch_aba_test.exs covers the large-object split copy and the zombie).
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @shard "shard_x"
  @data_key "shard_x.db"
  @lock_key "shard_x.lock"

  defp dead_lock do
    Storage.encode_lease(%{
      owner: "dead@node#old",
      epoch: 5,
      expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
    })
  end

  defp put_config(store) do
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
  end

  defp start_store(objects) do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})
    put_config(store)
    store
  end

  test "the double is honest: a plain self-copy of identical bytes does NOT rotate the etag" do
    store = start_store(%{@data_key => "same-bytes"})
    before = S3EtagStore.etag_of(store, @data_key)

    # A plain CopyObject self-copy (what the pre-#4 touch did exclusively).
    assert :ok = S3.retain(@shard, 1) |> then(fn _ -> :ok end)

    assert S3EtagStore.etag_of(store, @data_key) == before,
           "the double must model MD5 etags — same bytes, same single-form etag"
  end

  test "a steal rotates a small object's etag by re-noncing its stored bytes, not its content" do
    store = start_store(%{@data_key => "shard-bytes", @lock_key => dead_lock()})
    before = S3EtagStore.etag_of(store, @data_key)

    assert {:ok, lease} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    assert lease.epoch == 6, "a steal must bump the epoch"
    assert lease[:took_over] == true, "the caller must know to revalidate its pull"

    after_etag = S3EtagStore.etag_of(store, @data_key)
    assert after_etag != before, "the touch must GENUINELY rotate the etag (round-2 #4)"

    refute S3EtagStore.body_of(store, @data_key) == "shard-bytes",
           "the stored bytes carry a nonce"

    assert S3EtagStore.plain_body_of(store, @data_key) == "shard-bytes",
           "the touch changes no database content"
  end

  test "a second steal moves to a THIRD etag, never back to the first (N1)" do
    store = start_store(%{@data_key => "shard-bytes", @lock_key => dead_lock()})
    first = S3EtagStore.etag_of(store, @data_key)

    assert {:ok, _} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    second = S3EtagStore.etag_of(store, @data_key)

    # The new owner dies unrenewed; a second contender steals the same bytes.
    Agent.update(store, fn s ->
      %{
        s
        | objects: Map.put(s.objects, @lock_key, %{body: dead_lock(), form: :single, meta: %{}})
      }
    end)

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "third@node#inc3", 30_000)

    third = S3EtagStore.etag_of(store, @data_key)

    assert length(Enum.uniq([first, second, third])) == 3,
           "the second steal of identical content returned to an earlier etag (pre-N1: the first)"
  end

  # Expert review #12: the steal-time touch rewrites the object (a re-nonced PUT for a small object, a
  # multipart copy for a large one) — both of which DROP the object's user metadata unless it is re-sent. So
  # the x-amz-meta-fathom-md5 integrity metadata (#17) was stripped by exactly the steal that precedes
  # the new owner's highest-risk pull (a failover of a possibly-multi-MB shard over a degraded
  # network), leaving that pull with no content verification. The touch now re-sends the md5 on both
  # copy forms; these pin that it survives each direction.
  @md5_meta_key "x-amz-meta-fathom-md5"

  defp md5_hex(body), do: Base.encode16(:crypto.hash(:md5, body), case: :lower)

  defp seed_md5_meta(store, key, md5) do
    Agent.update(store, fn s ->
      obj = Map.get(s.objects, key)
      %{s | objects: Map.put(s.objects, key, %{obj | meta: %{@md5_meta_key => md5}})}
    end)
  end

  test "a steal preserves x-amz-meta-fathom-md5 across the touch (#12)" do
    body = "shard-bytes"
    md5 = md5_hex(body)
    store = start_store(%{@data_key => body, @lock_key => dead_lock()})
    seed_md5_meta(store, @data_key, md5)
    before = S3EtagStore.etag_of(store, @data_key)

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)

    refute S3EtagStore.etag_of(store, @data_key) == before, "the touch rotated the etag"

    assert S3EtagStore.meta_of(store, @data_key)[@md5_meta_key] == md5,
           "the integrity md5 metadata must survive the steal-time touch (pre-#12: dropped)"
  end

  test "a second steal preserves x-amz-meta-fathom-md5 across the second touch (#12)" do
    body = "shard-bytes"
    md5 = md5_hex(body)
    store = start_store(%{@data_key => body, @lock_key => dead_lock()})
    seed_md5_meta(store, @data_key, md5)

    assert {:ok, _} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    first_touch = S3EtagStore.etag_of(store, @data_key)

    assert S3EtagStore.meta_of(store, @data_key)[@md5_meta_key] == md5,
           "the metadata survives the first touch (which also re-encodes the raw seed to zstd)"

    # A second contender steals the now re-nonced object.
    Agent.update(store, fn s ->
      %{
        s
        | objects: Map.put(s.objects, @lock_key, %{body: dead_lock(), form: :single, meta: %{}})
      }
    end)

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "third@node#inc3", 30_000)
    refute S3EtagStore.etag_of(store, @data_key) == first_touch, "the second touch rotated too"

    assert S3EtagStore.meta_of(store, @data_key)[@md5_meta_key] == md5,
           "the md5 metadata must ALSO survive the second touch"
  end

  # Expert review #13: owner_live? compared this reader's LOCAL clock to the owner's stamped heartbeat
  # expiry, so a reader whose clock stepped FORWARD (VM live-migration, a bad NTP step) saw a live
  # owner's fresh heartbeat as expired and wrongfully stole — the victim then self-fenced and
  # quarantined its acked writes (a skew event → data loss). The steal now compares against S3's OWN
  # clock (its response Date), a clock both sides share. Modeled by an S3 store whose Date is BEHIND
  # this reader's local clock (i.e. the reader is skewed ahead).
  test "a forward reader-clock step does NOT wrongfully steal a live owner (uses S3's Date, #13)" do
    now = Storage.now_ms()
    owner = "liveowner"
    # Heartbeat + lock read as EXPIRED by this reader's local clock (exp 10 s in its past)...
    hb = Storage.encode_heartbeat(%{owner: owner, expires_at_ms: now - 10_000})
    lock = Storage.encode_lease(%{owner: owner, epoch: 5, expires_at_ms: now - 10_000})

    store =
      start_store(%{@data_key => "shard-bytes", @lock_key => lock, "heartbeats/#{owner}" => hb})

    # ...but S3's own clock is 20 s BEHIND the reader (the reader stepped forward), so by the SHARED
    # S3 clock the heartbeat is still fresh — the owner is live and must not be stolen.
    S3EtagStore.set_date_ms(store, now - 20_000)

    assert {:error, {:held, ^owner, _}} = S3.acquire_lease(@shard, "new@node#inc2", 30_000),
           "a forward reader-clock step must not steal a live owner (pre-#13: wrongful steal)"
  end

  test "a genuinely-dead owner is still stolen when S3's Date agrees it is stale (#13)" do
    now = Storage.now_ms()
    owner = "deadowner"
    hb = Storage.encode_heartbeat(%{owner: owner, expires_at_ms: now - 60_000})
    lock = Storage.encode_lease(%{owner: owner, epoch: 5, expires_at_ms: now - 60_000})

    store =
      start_store(%{@data_key => "shard-bytes", @lock_key => lock, "heartbeats/#{owner}" => hb})

    # S3's clock agrees the heartbeat is 60 s stale — genuinely dead, so the steal proceeds.
    S3EtagStore.set_date_ms(store, now)

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "new@node#inc2", 30_000),
           "a heartbeat stale by the shared S3 clock is genuinely dead and IS stolen"
  end
end
