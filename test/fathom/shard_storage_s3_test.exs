defmodule Fathom.ShardStorageS3Test do
  @moduledoc """
  Live S3 round-trip for the `Fathom.Shard.Storage.S3` backend against an
  S3-compatible store (MinIO). Excluded from the default suite — run with:

      mix test --include s3 test/fathom/shard_storage_s3_test.exs

  Point it at a store with env vars (defaults match the MinIO container in
  `scripts/minio_test.sh`):

      FATHOM_S3_TEST_ENDPOINT   (default http://localhost:9100)
      FATHOM_S3_TEST_BUCKET     (default fathom-shards-test)
      FATHOM_S3_TEST_ACCESS_KEY (default fathomtest)
      FATHOM_S3_TEST_SECRET_KEY (default fathomtest123)

  Proves the bytes round-trip AND that the lease's fencing primitives work on the
  real store — including that the store honors the `If-None-Match` / `If-Match`
  conditional writes the lease's create/steal races depend on.
  """
  use ExUnit.Case, async: false

  @moduletag :s3

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3

  @prefix "leasetest/"

  setup_all do
    endpoint = System.get_env("FATHOM_S3_TEST_ENDPOINT", "http://localhost:9100")
    bucket = System.get_env("FATHOM_S3_TEST_BUCKET", "fathom-shards-test")
    access_key = System.get_env("FATHOM_S3_TEST_ACCESS_KEY", "fathomtest")
    secret_key = System.get_env("FATHOM_S3_TEST_SECRET_KEY", "fathomtest123")

    prev = Application.get_env(:fathom, S3)

    Application.put_env(:fathom, S3,
      bucket: bucket,
      region: "us-east-1",
      endpoint: endpoint,
      path_style: true,
      prefix: @prefix,
      access_key_id: access_key,
      secret_access_key: secret_key
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    %{
      endpoint: endpoint,
      bucket: bucket,
      access_key: access_key,
      secret_key: secret_key,
      # Scopes every shard id to THIS run — see the setup below.
      run_token: Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
    }
  end

  setup ctx do
    # The run token is NOT decoration. `System.unique_integer/1` restarts every VM, and its values
    # land in overlapping ranges across runs (measured: three fresh VMs opened at 11907, 7877 and
    # 4103, all stepping by 64). Against a bucket that PERSISTS between runs — which is every real
    # store, and `scripts/minio_test.sh --keep` — two runs can therefore mint the same shard id.
    # A stale object left by an earlier run then belongs to a later run's test, and the tests that
    # assert an object is ABSENT are the ones that break. Scoping the id to the run makes that
    # collision impossible rather than unlikely.
    shard = "s3_#{ctx.run_token}_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # purge_shard/1 rather than deleting `.db` + `.lock` by hand: the retain and snapshot tests
      # also create `@<version>` and `@snap-<id>` objects, which the old cleanup never touched, so
      # the bucket accumulated them forever. This is the same erase the tenant-delete path uses,
      # and its id-delimiter matching means purging `s3_ab12_64` can't touch `s3_ab12_640`.
      # (setup_all's on_exit restores the S3 app env only when the MODULE finishes, so the
      # backend is still configured here.)
      S3.purge_shard(shard)
    end)

    Map.put(ctx, :shard, shard)
  end

  # ── pull / flush ──

  test "pull on a missing object returns :ok and writes no file", %{shard: shard} do
    local = tmp_path(shard)
    assert {:ok, _} = S3.pull(shard, local)
    refute File.exists?(local)
  end

  test "flush uploads and pull round-trips the bytes", %{shard: shard} do
    src = tmp_path("#{shard}-src")
    dst = tmp_path("#{shard}-dst")
    File.write!(src, "the quick brown fox\n")

    assert :ok = S3.flush(shard, src)
    assert {:ok, _} = S3.pull(shard, dst)
    assert File.read!(dst) == "the quick brown fox\n"
  end

  # ── stored-object compression (expert review 2026-07-24 #38) ──

  defp with_encoding(value, fun) do
    prev = Application.get_env(:fathom, :shard_object_encoding)
    Application.put_env(:fathom, :shard_object_encoding, value)

    try do
      fun.()
    after
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_object_encoding),
        else: Application.put_env(:fathom, :shard_object_encoding, prev)
    end
  end

  # Compressible, and big enough that the ratio is unambiguous.
  defp compressible, do: String.duplicate("SQLite format 3 page of repetitive row data. ", 20_000)

  test "a zlib-encoded object round-trips and is stored smaller", %{shard: shard} = ctx do
    src = tmp_path("#{shard}-src")
    dst = tmp_path("#{shard}-dst")
    body = compressible()
    File.write!(src, body)

    with_encoding(:zlib, fn -> assert :ok = S3.flush(shard, src) end)

    # The STORED bytes are compressed — read the object raw, bypassing the backend's decode.
    %{status: 200} = raw = Req.get!(signed_req(ctx), url: object_url(ctx, shard <> ".db"))

    assert byte_size(raw.body) < byte_size(body) / 2,
           "the object wasn't actually compressed on the wire/at rest"

    assert {:ok, _etag} = S3.pull(shard, dst)
    assert File.read!(dst) == body, "the pull must inflate back to the exact database bytes"
  end

  # DECODE-ALWAYS. This is what lets a fleet roll the flag back: an object written while encoding
  # was on must stay readable by a node that has since turned it off. Without this the flag is a
  # one-way door and a rollback orphans every object written in between.
  test "an object written with encoding ON is readable by a node with it OFF", %{shard: shard} do
    src = tmp_path("#{shard}-src")
    dst = tmp_path("#{shard}-dst")
    body = compressible()
    File.write!(src, body)

    with_encoding(:zlib, fn -> assert :ok = S3.flush(shard, src) end)
    with_encoding(:none, fn -> assert {:ok, _} = S3.pull(shard, dst) end)

    assert File.read!(dst) == body
  end

  # And the other direction: turning encoding ON must not break reading the raw objects already
  # in the bucket.
  test "an unmarked (raw) object is still readable by a node with encoding ON", %{shard: shard} do
    src = tmp_path("#{shard}-src")
    dst = tmp_path("#{shard}-dst")
    File.write!(src, "plain bytes, no marker\n")

    with_encoding(:none, fn -> assert :ok = S3.flush(shard, src) end)
    with_encoding(:zlib, fn -> assert {:ok, _} = S3.pull(shard, dst) end)

    assert File.read!(dst) == "plain bytes, no marker\n"
  end

  # THE safety property. An object marked with an encoding this build cannot perform must FAIL THE
  # PULL. Writing those bytes to the local path would hand SQLite something that is not a
  # database — silent corruption of a tenant, which is far worse than a refused open.
  test "an object marked with an unknown encoding fails the pull closed", %{shard: shard} = ctx do
    dst = tmp_path("#{shard}-dst")

    # Write the object directly with a marker no build understands.
    %{status: status} =
      Req.put!(signed_req(ctx),
        url: object_url(ctx, shard <> ".db"),
        body: "definitely not a sqlite file",
        headers: [{Fathom.Shard.Storage.Codec.meta_header(), "zstd-v9"}]
      )

    assert status in 200..299

    assert {:error, {:unknown_object_encoding, "zstd-v9"}} = S3.pull(shard, dst)

    refute File.exists?(dst),
           "a pull that cannot decode the object must leave NO local file — a partially written " <>
             "or undecoded file is what SQLite would later be handed as a database"
  end

  # The integrity metadata must keep meaning "this database's hash" regardless of storage form,
  # or verify_integrity/3 would have to know about encodings.
  test "the integrity metadata is the UNCOMPRESSED hash", %{shard: shard} = ctx do
    src = tmp_path("#{shard}-src")
    body = compressible()
    File.write!(src, body)

    with_encoding(:zlib, fn -> assert :ok = S3.flush(shard, src) end)

    %{status: 200, headers: headers} =
      Req.head!(signed_req(ctx), url: object_url(ctx, shard <> ".db"))

    meta =
      case headers["x-amz-meta-fathom-md5"] do
        [v | _] -> v
        v -> v
      end

    plaintext_md5 = :crypto.hash(:md5, body) |> Base.encode16(case: :lower)

    assert meta == plaintext_md5,
           "x-amz-meta-fathom-md5 must hash the DATABASE, not the compressed body, so an " <>
             "object's identity doesn't depend on how it happened to be stored"
  end

  # ── conditional pull (warm-standby freshness) ──

  test "pull_if_changed: nil etag writes; matching etag is a 304; stale etag re-pulls",
       %{shard: shard} do
    src = tmp_path("#{shard}-src")
    dst = tmp_path("#{shard}-dst")
    File.write!(src, "v1\n")
    assert :ok = S3.flush(shard, src)

    # nil etag ⇒ unconditional GET, writes and captures the current etag.
    assert {:ok, {:written, etag1}} = S3.pull_if_changed(shard, dst, nil)
    assert is_binary(etag1)
    assert File.read!(dst) == "v1\n"

    # Same etag ⇒ If-None-Match matches ⇒ 304, no byte written.
    File.rm!(dst)
    assert {:ok, :unchanged} = S3.pull_if_changed(shard, dst, etag1)
    refute File.exists?(dst)

    # A new flush moves the etag; the stale etag re-pulls the fresh bytes.
    File.write!(src, "v2\n")
    assert :ok = S3.flush(shard, src)
    assert {:ok, {:written, etag2}} = S3.pull_if_changed(shard, dst, etag1)
    assert etag2 != etag1
    assert File.read!(dst) == "v2\n"
  end

  # This test failed ONCE in a full `--include s3` run (2026-07-25) and never reproduced across
  # ~10 further full runs and 16 seed-swept runs. It was not root-caused. The most plausible
  # mechanism was cross-run shard-id collision against a persistent bucket, which the run-token in
  # `setup` now makes impossible — but that is a removed HAZARD, not a proven fix, so the
  # assertions carry what they actually saw. If this fires again the message alone should identify
  # whether an object was really there, and whose.
  test "pull_if_changed on a missing object returns :absent", %{shard: shard} = ctx do
    dst = tmp_path("#{shard}-dst")

    first = S3.pull_if_changed(shard, dst, nil)

    assert {:ok, :absent} = first,
           "expected no object at #{@prefix}#{shard}.db, got #{inspect(first)}. " <>
             "If this is {:ok, {:written, _}} the bucket held a stale object for this id — " <>
             "check for a leaked key from an earlier run (run_token=#{ctx.run_token})."

    stale = S3.pull_if_changed(shard, dst, "\"stale\"")
    assert {:ok, :absent} = stale, "a stale etag against a missing object: got #{inspect(stale)}"

    refute File.exists?(dst), "a pull of a missing object must write no local file"
  end

  # ── lease ──

  test "acquire on a fresh shard returns epoch 1; a live lease blocks other owners",
       %{shard: shard} do
    assert {:ok, %{owner: "a@node", epoch: 1}} = S3.acquire_lease(shard, "a@node", 60_000)
    assert {:error, {:held, "a@node"}} = S3.acquire_lease(shard, "b@node", 60_000)
  end

  test "renew extends the holder's lease but is superseded after a steal",
       %{shard: shard} = ctx do
    {:ok, lease} = S3.acquire_lease(shard, "a@node", 60_000)
    assert {:ok, %{owner: "a@node", epoch: 1}} = S3.renew_lease(shard, lease, 60_000)

    put_raw_lock(ctx, shard, "b@node", 2, now_ms() + 60_000)
    assert {:error, :superseded} = S3.renew_lease(shard, lease, 60_000)
  end

  test "an expired lease is stolen and the epoch bumps", %{shard: shard} = ctx do
    # Expired by more than the STEAL MARGIN. `owner_live?/3` falls back to the lock's own TTL when
    # the owner runs no heartbeat (this test writes a raw lock and no heartbeat), and that fallback
    # is margin-aware: a peer steals only once the lock is expired by MORE than the margin, so a
    # wrongful steal needs clock skew greater than the remaining life plus the margin. This test
    # predates the margin and expired the lock by 1s, which is inside it — so the owner still read
    # as live and the steal was correctly refused.
    stale_by = Fathom.Shard.Storage.steal_margin_ms() + 5_000
    put_raw_lock(ctx, shard, "a@node", 5, now_ms() - stale_by)
    assert {:ok, %{owner: "b@node", epoch: 6}} = S3.acquire_lease(shard, "b@node", 60_000)
  end

  test "release deletes the lock so the next acquire is a fresh epoch 1", %{shard: shard} do
    {:ok, lease} = S3.acquire_lease(shard, "a@node", 60_000)
    assert :ok = S3.release_lease(shard, lease)
    assert {:ok, %{epoch: 1}} = S3.acquire_lease(shard, "b@node", 60_000)
  end

  # ── the fence depends on this: the store must enforce conditional writes ──

  test "the store enforces If-None-Match and If-Match conditional PUTs", ctx do
    req = signed_req(ctx)
    url = object_url(ctx, "cond_#{System.unique_integer([:positive])}.probe")

    # If-None-Match: * — create only if absent. First wins, second is a 412.
    assert {:ok, %{status: created}} =
             Req.put(req, url: url, body: "one", headers: [{"if-none-match", "*"}])

    assert created in 200..299

    assert {:ok, %{status: 412}} =
             Req.put(req, url: url, body: "two", headers: [{"if-none-match", "*"}])

    # If-Match: <etag> — overwrite only if the etag still matches.
    {:ok, %{status: 200, headers: headers}} = Req.get(req, url: url)
    etag = etag(headers)

    assert {:ok, %{status: 412}} =
             Req.put(req, url: url, body: "stale", headers: [{"if-match", "\"00000000\""}])

    assert {:ok, %{status: ok}} =
             Req.put(req, url: url, body: "fresh", headers: [{"if-match", etag}])

    assert ok in 200..299

    Req.delete(req, url: url)
  end

  # ── versioned copies (blue/green) ──

  test "retain + restore round-trip an old version via server-side copy", %{shard: shard} do
    v1 = tmp_path("#{shard}-v1")
    File.write!(v1, "v1")
    assert :ok = S3.flush(shard, v1)
    assert :ok = S3.retain(shard, 1)

    v2 = tmp_path("#{shard}-v2")
    File.write!(v2, "v2")
    assert :ok = S3.flush(shard, v2)

    assert :ok = S3.restore(shard, 1)

    dst = tmp_path("#{shard}-dst")
    assert {:ok, _} = S3.pull(shard, dst)
    assert File.read!(dst) == "v1"
  end

  # Point-in-time snapshots (expert review 2026-07-14 #12): server-side copy to/from
  # `<shard>@snap-<id>`, listed via ListObjectsV2 over the snapshot prefix.
  test "snapshot + restore_snapshot round-trip; list and drop", %{shard: shard} do
    v1 = tmp_path("#{shard}-v1")
    File.write!(v1, "v1")
    assert :ok = S3.flush(shard, v1)
    assert :ok = S3.snapshot(shard, "test1")

    v2 = tmp_path("#{shard}-v2")
    File.write!(v2, "v2")
    assert :ok = S3.flush(shard, v2)

    assert {:ok, snaps} = S3.list_snapshots(shard)
    assert Enum.any?(snaps, &(&1.id == "test1" and &1.bytes == 2))

    assert :ok = S3.restore_snapshot(shard, "test1")
    dst = tmp_path("#{shard}-dst")
    assert {:ok, _} = S3.pull(shard, dst)
    assert File.read!(dst) == "v1"

    assert :ok = S3.drop_snapshot(shard, "test1")
    assert {:ok, after_drop} = S3.list_snapshots(shard)
    refute Enum.any?(after_drop, &(&1.id == "test1"))
  end

  # Expert review 2026-07-14 #4: the revert's restore must be If-Match-fenced on the live etag
  # (mirroring the forward flush/3) so a steal landing between the migrator's fence and the
  # copy-back is caught instead of clobbering the new owner's live object.
  test "restore/3 is If-Match-fenced: a stale etag is superseded, the live etag restores",
       %{shard: shard} do
    v1 = tmp_path("#{shard}-v1")
    File.write!(v1, "v1")
    assert :ok = S3.flush(shard, v1)
    assert :ok = S3.retain(shard, 1)

    v2 = tmp_path("#{shard}-v2")
    File.write!(v2, "v2")
    assert :ok = S3.flush(shard, v2)

    # A stale etag (the pre-steal snapshot) no longer matches live → superseded, no clobber.
    assert {:error, :superseded} = S3.restore(shard, 1, ~s("stale-etag"))

    still = tmp_path("#{shard}-still-v2")
    assert {:ok, _} = S3.pull(shard, still)
    assert File.read!(still) == "v2"

    # The live object's current etag restores v1 over live.
    {:ok, live_etag} = S3.object_etag(shard)
    assert :ok = S3.restore(shard, 1, live_etag)

    back = tmp_path("#{shard}-back-to-v1")
    assert {:ok, _} = S3.pull(shard, back)
    assert File.read!(back) == "v1"
  end

  test "drop_version removes the versioned object (idempotent)", %{shard: shard} do
    src = tmp_path("#{shard}-d")
    File.write!(src, "v1")
    assert :ok = S3.flush(shard, src)
    assert :ok = S3.retain(shard, 1)

    assert :ok = S3.drop_version(shard, 1)
    assert :ok = S3.drop_version(shard, 1)

    # The versioned object is gone, so restoring it now fails.
    assert {:error, _} = S3.restore(shard, 1)
  end

  # ── helpers ──

  defp now_ms, do: System.system_time(:millisecond)
  defp tmp_path(name), do: Path.join(System.tmp_dir!(), "fathom_s3_test_#{name}.db")

  defp object_url(%{bucket: bucket}, key), do: "/#{bucket}/#{@prefix}#{key}"

  defp signed_req(ctx) do
    Req.new(
      base_url: ctx.endpoint,
      aws_sigv4: [
        access_key_id: ctx.access_key,
        secret_access_key: ctx.secret_key,
        service: :s3,
        region: "us-east-1"
      ]
    )
  end

  # Write a lock object directly (unconditional) to simulate another node's lease.
  defp put_raw_lock(ctx, shard, owner, epoch, expires_at_ms) do
    body = Storage.encode_lease(%{owner: owner, epoch: epoch, expires_at_ms: expires_at_ms})

    {:ok, %{status: status}} =
      Req.put(signed_req(ctx), url: object_url(ctx, "#{shard}.lock"), body: body)

    assert status in 200..299
  end

  defp etag(headers) do
    case headers["etag"] do
      [value | _] -> value
      value when is_binary(value) -> value
    end
  end
end
