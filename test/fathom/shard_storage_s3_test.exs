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

    %{endpoint: endpoint, bucket: bucket, access_key: access_key, secret_key: secret_key}
  end

  setup ctx do
    shard = "s3_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # Best-effort cleanup of the shard's objects.
      req = signed_req(ctx)
      for suffix <- [".db", ".lock"], do: Req.delete(req, url: object_url(ctx, shard <> suffix))
    end)

    Map.put(ctx, :shard, shard)
  end

  # ── pull / flush ──

  test "pull on a missing object returns :ok and writes no file", %{shard: shard} do
    local = tmp_path(shard)
    assert :ok = S3.pull(shard, local)
    refute File.exists?(local)
  end

  test "flush uploads and pull round-trips the bytes", %{shard: shard} do
    src = tmp_path("#{shard}-src")
    dst = tmp_path("#{shard}-dst")
    File.write!(src, "the quick brown fox\n")

    assert :ok = S3.flush(shard, src)
    assert :ok = S3.pull(shard, dst)
    assert File.read!(dst) == "the quick brown fox\n"
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

  test "pull_if_changed on a missing object returns :absent", %{shard: shard} do
    dst = tmp_path("#{shard}-dst")
    assert {:ok, :absent} = S3.pull_if_changed(shard, dst, nil)
    assert {:ok, :absent} = S3.pull_if_changed(shard, dst, "\"stale\"")
    refute File.exists?(dst)
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
    put_raw_lock(ctx, shard, "a@node", 5, now_ms() - 1_000)
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
    assert :ok = S3.pull(shard, dst)
    assert File.read!(dst) == "v1"
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
