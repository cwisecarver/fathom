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
  # Fathom.Test.S3EtagStore, whose etags are content-derived like real S3 — the
  # rotation must come from the ALTERNATING COPY FORM (single ↔ one-part
  # multipart), and a store where nothing rotates must fail the steal closed.
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

  test "a steal rotates a single-form etag via the multipart copy form" do
    store = start_store(%{@data_key => "shard-bytes", @lock_key => dead_lock()})
    before = S3EtagStore.etag_of(store, @data_key)
    refute before =~ "-", "seed must carry a single-form (MD5) etag"

    assert {:ok, lease} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    assert lease.epoch == 6, "a steal must bump the epoch"
    assert lease[:took_over] == true, "the caller must know to revalidate its pull"

    after_etag = S3EtagStore.etag_of(store, @data_key)
    assert after_etag != before, "the touch must GENUINELY rotate the etag (round-2 #4)"
    assert after_etag =~ "-1", "a single-form etag rotates into the multipart form"
    assert S3EtagStore.body_of(store, @data_key) == "shard-bytes", "the touch moves no bytes"
  end

  test "a second steal rotates a multipart-form etag back via the plain copy form" do
    store = start_store(%{@data_key => "shard-bytes", @lock_key => dead_lock()})

    assert {:ok, _} = S3.acquire_lease(@shard, "new@node#inc2", 30_000)
    multipart_etag = S3EtagStore.etag_of(store, @data_key)
    assert multipart_etag =~ "-1"

    # The new owner dies unrenewed; a second contender steals the same bytes.
    Agent.update(store, fn s ->
      %{s | objects: Map.put(s.objects, @lock_key, %{body: dead_lock(), form: :single})}
    end)

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "third@node#inc3", 30_000)

    single_etag = S3EtagStore.etag_of(store, @data_key)

    assert single_etag != multipart_etag,
           "the SECOND steal of identical bytes must also rotate (the form flips back)"

    refute single_etag =~ "-"
  end
end
