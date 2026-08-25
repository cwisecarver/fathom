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

  # This asserted `{:ok, _}` until 2026-08-12. Expert review 2026-08-01 #24 deliberately split
  # `{:absent, etag}` out of `{:ok, etag}`, because collapsing them told every caller "bytes are at
  # local_path" when none were, and the pull-then-open consumers duly opened the missing path —
  # which CREATES an empty database. The test kept the old contract for eleven days because the
  # `:s3` suite is excluded from `mix test` AND from CI, so nothing ever ran it.
  test "pull on a missing object returns :absent and writes no file", %{shard: shard} do
    local = tmp_path(shard)
    assert {:absent, nil} = S3.pull(shard, local)
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

  # THE `nil`-LINEAGE ASYMMETRY, ASSERTED AGAINST THE BACKEND THAT CAN ACTUALLY BREAK IT
  # (expert review 2026-08-24 #11, found independently by two panels).
  #
  # `test/fathom/shard/lineage_test.exs` already pins "nil LEAVES the previous lineage in place —
  # the asymmetry with position", but only against `Storage.Local`, where the lineage is a
  # separate sidecar and `write_lineage(_, nil)` is a no-op. It cannot fail there. S3 stores the
  # lineage as user metadata ON the object, and a PUT replaces the object wholesale, so `nil` DID
  # erase it — the two backends implemented opposite behaviours for the same argument and the
  # contract was enforced only where it could not fail. `Fathom.Test.FaultyStorage` delegates to
  # `Local` and inherits the same blind spot, so this assertion has no home outside this file.
  #
  # Note this suite is excluded from `mix test` AND from CI (it needs MinIO), which is exactly how
  # the `{:absent, _}` contract above drifted for eleven days. Run it with `scripts/minio_test.sh`
  # when touching object metadata.
  test "a flush with nil lineage PRESERVES the stored lineage; an integer overwrites it",
       %{shard: shard} do
    src = tmp_path("#{shard}-src")
    File.write!(src, "v1\n")

    # Claim lineage 7, then confirm the store really holds it.
    assert {:ok, etag1} = S3.flush(shard, src, nil, nil, 7)
    assert {:ok, %{lineage: 7}} = S3.object_head(shard)

    # A flush with NOTHING to claim must leave it at 7. Pre-fix this dropped the header and the
    # object came back with lineage nil, so `Storage.next_lineage/1` restarted the counter — and a
    # peer replica stamped higher then outranked the object at the next failover.
    File.write!(src, "v2\n")
    assert {:ok, etag2} = S3.flush(shard, src, etag1, nil, nil)

    assert {:ok, %{lineage: 7}} = S3.object_head(shard),
           "a nil-lineage flush ERASED the shard's lineage; the counter has been reset and a " <>
             "stale replica can now outrank this object"

    # …and an integer still overwrites, so preserving is not the same as freezing.
    File.write!(src, "v3\n")
    assert {:ok, _} = S3.flush(shard, src, etag2, nil, 9)
    assert {:ok, %{lineage: 9}} = S3.object_head(shard)
  end

  test "a first flush of a brand-new object carries no lineage and pays no HEAD", %{shard: shard} do
    src = tmp_path("#{shard}-src")
    File.write!(src, "new\n")

    # `If-None-Match: *` — there is no object to inherit metadata from, so `nil` means nil.
    assert {:ok, _} = S3.flush(shard, src, nil, nil, nil)
    assert {:ok, %{lineage: nil}} = S3.object_head(shard)
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

  # Expert review 2026-08-01 #12. `owner_live?/3` was asymmetric: NO heartbeat object fell back
  # to the lock's own TTL (correct for a legacy-mode owner renewing per-shard), but a
  # PRESENT-BUT-EXPIRED one returned :dead outright and never consulted the lock at all.
  #
  # That is reachable by design, not by misconfiguration: when the Heartbeat GenServer dies
  # abnormally it deliberately LEAVES its object behind and coordinators degrade to the legacy
  # per-shard renew fence. So a node that is healthy, serving, and renewing every lock it holds
  # had its ENTIRE keyspace slice become instantly stealable — per-shard loss up to a flush
  # interval and a `.fenced.<ts>` quarantine each, while it kept serving. A stale-but-present
  # heartbeat was strictly WORSE than an absent one.
  test "a stale heartbeat does NOT make a still-renewed lock stealable", %{shard: shard} = ctx do
    margin = Fathom.Shard.Storage.steal_margin_ms()

    # The owner's heartbeat lapsed well past the margin (its Heartbeat process died) ...
    put_raw_heartbeat(ctx, "a@node", now_ms() - (margin + 30_000))
    # ... but its lock is fresh, because legacy mode renews per-shard.
    put_raw_lock(ctx, shard, "a@node", 3, now_ms() + 60_000)

    assert {:error, {:held, "a@node"}} = S3.acquire_lease(shard, "b@node", 60_000),
           "a healthy node that is still renewing its locks was stolen from"
  end

  test "an owner is stealable once BOTH its heartbeat and its lock have lapsed",
       %{shard: shard} = ctx do
    margin = Fathom.Shard.Storage.steal_margin_ms()
    stale = margin + 30_000

    put_raw_heartbeat(ctx, "a@node", now_ms() - stale)
    put_raw_lock(ctx, shard, "a@node", 3, now_ms() - stale)

    assert {:ok, %{owner: "b@node", epoch: 4}} = S3.acquire_lease(shard, "b@node", 60_000),
           "a genuinely dead owner must still be stealable — the fix must not wedge failover"
  end

  # Expert review 2026-08-01 #10. When a steal's lock PUT landed but the follow-up
  # `touch_object/1` failed, the code wrote the DEAD OWNER'S ORIGINAL LOCK CONTENT back — moving
  # the fencing epoch N → N+1 → N. The epoch's monotonicity is the one invariant the whole
  # single-writer design rests on, and rolling it back let the zombie's `check_lease/2` start
  # matching again, i.e. conclude it was still the owner.
  #
  # The lock is now DELETED instead (conditional on our own write), so the next acquire is a
  # fresh conditional create and a zombie reads `:not_found ⇒ :superseded`.
  #
  # THESE TWO DO NOT DISCRIMINATE and pass against the unfixed code: reaching the rollback
  # requires `touch_object/1` to fail AFTER its lock PUT has landed, which needs fault injection
  # this backend has no seam for. They are invariant guards — the epoch never regresses, a
  # superseded owner never re-validates — so a future change that reintroduces a rollback on any
  # reachable path is caught. The fix itself rests on the monotonicity argument. (#12's two
  # tests above DO discriminate: the first fails on the parent commit.)
  test "a failed takeover never leaves a lower epoch than was already visible",
       %{shard: shard} = ctx do
    margin = Fathom.Shard.Storage.steal_margin_ms()
    put_raw_lock(ctx, shard, "a@node", 7, now_ms() - (margin + 30_000))

    # A successful steal publishes epoch 8.
    assert {:ok, %{owner: "b@node", epoch: 8}} = S3.acquire_lease(shard, "b@node", 60_000)

    # Whatever happens next, no observer may ever see the epoch go backwards.
    assert {:ok, %{epoch: epoch}} = read_lock_or_free(ctx, shard)
    assert epoch >= 8, "the fencing epoch moved backward to #{epoch}"
  end

  test "after a takeover the superseded owner's lease no longer validates",
       %{shard: shard} = ctx do
    margin = Fathom.Shard.Storage.steal_margin_ms()
    put_raw_lock(ctx, shard, "a@node", 7, now_ms() - (margin + 30_000))
    stale_lease = %{owner: "a@node", epoch: 7, expires_at_ms: now_ms() + 60_000}

    assert {:ok, _} = S3.acquire_lease(shard, "b@node", 60_000)

    assert {:error, :superseded} = S3.check_lease(shard, stale_lease),
           "the superseded owner must never re-validate as the holder"
  end

  # ── the position stamp must survive a REAL takeover touch ──

  # THE bug found on the rig 2026-08-12, and the only place it can be pinned.
  #
  # A steal touches the data object (server-side self-copy) to rotate its etag, which fences the
  # deposed node's `If-Match` flush. S3 requires the REPLACE metadata directive for that copy, and
  # **REPLACE drops every user metadata header unless the copy re-sends it.** The touch re-sent one
  # key (the integrity md5); `x-amz-meta-fathom-pos` was added later for A2 and never added to the
  # list, so every takeover silently ERASED the position stamp — and an unstamped object is never
  # overridable by design. promote-on-open and survivor selection therefore went inert at exactly
  # the moment they exist for, with no error and no log line.
  #
  # `shard_storage_touch_meta_test.exs` pins the LIST `carry_meta/1` returns. It cannot pin the
  # BEHAVIOUR, because "REPLACE drops user metadata" is a property only a real store has:
  # `Storage.Local` and `Fathom.Test.FaultyStorage` both keep their metadata map across a touch,
  # which is why the default suite could never have seen this. That is what this test is for.
  #
  # It drives the PRODUCTION path — `acquire_lease` on an expired lock — not `touch_object` directly,
  # so it also proves the steal actually reaches the touch.
  test "the position stamp survives a takeover touch, in BOTH etag-rotation forms",
       %{shard: shard} = ctx do
    src = tmp_path("#{shard}-src")
    File.write!(src, "the tenant's bytes\n")

    position = %{epoch: 3, wal_gen: 1, offset: 8272}
    assert {:ok, etag0} = S3.flush(shard, src, nil, position)

    # Precondition: the stamp is really on the object before any steal. Without this the whole
    # test could pass by never having written a stamp at all.
    assert {:ok, ^position} = S3.object_position(shard),
           "the fixture never stamped the object, so it can prove nothing about the touch"

    refute multipart_form?(etag0), "a plain PUT should yield a single-form (MD5) etag"

    # ── steal 1: single-form etag ⇒ rotate_etag takes the MULTIPART copy branch.
    md5_before = meta_header(ctx, shard, "x-amz-meta-fathom-md5")
    assert {:ok, %{owner: "b@node", epoch: 6}} = steal(ctx, shard, "a@node", 5, "b@node")

    {:ok, etag1} = S3.object_etag(shard)
    assert etag1 != etag0, "the touch did not rotate the etag — the steal never reached it"
    assert multipart_form?(etag1), "expected the multipart-copy branch to have run"

    assert {:ok, ^position} = S3.object_position(shard),
           "the takeover ERASED the position stamp — every A2 recovery path is inert"

    assert meta_header(ctx, shard, "x-amz-meta-fathom-md5") == md5_before,
           "the touch dropped the integrity md5 as well"

    # ── steal 2: the object now carries a multipart-form etag ⇒ the SINGLE-copy branch, which is
    # a separate REPLACE call with its own headers list and can regress independently.
    assert {:ok, %{owner: "c@node", epoch: 8}} = steal(ctx, shard, "b@node", 7, "c@node")

    {:ok, etag2} = S3.object_etag(shard)
    assert etag2 != etag1, "the second touch did not rotate the etag"
    refute multipart_form?(etag2), "expected the single-copy branch to have run"

    assert {:ok, ^position} = S3.object_position(shard),
           "the single-copy touch ERASED the position stamp"

    assert meta_header(ctx, shard, "x-amz-meta-fathom-md5") == md5_before

    # A touch is a self-copy: the tenant's bytes must be untouched by all of it.
    dst = tmp_path("#{shard}-dst")
    assert {:ok, _} = S3.pull(shard, dst)
    assert File.read!(dst) == "the tenant's bytes\n"
  end

  # An object flushed before A2 shipped carries no stamp, and a touch must NOT fabricate one:
  # a stamp claims an ordering, and inventing "0:0:0" would let any replica claim to be ahead of
  # an object nobody ever positioned.
  test "a takeover touch does not invent a position stamp on an unstamped object",
       %{shard: shard} = ctx do
    src = tmp_path("#{shard}-src")
    File.write!(src, "legacy object, no stamp\n")

    assert :ok = S3.flush(shard, src)
    assert {:ok, nil} = S3.object_position(shard)

    assert {:ok, %{owner: "b@node"}} = steal(ctx, shard, "a@node", 2, "b@node")

    assert {:ok, nil} = S3.object_position(shard),
           "the touch fabricated a position stamp for an object that never had one"
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

  # Write a heartbeat object directly, to stage an owner whose Heartbeat process died and left
  # its (now stale) object behind — the #12 scenario.
  defp put_raw_heartbeat(ctx, owner, expires_at_ms) do
    body = Storage.encode_heartbeat(%{owner: owner, expires_at_ms: expires_at_ms})
    key = "heartbeats/" <> URI.encode_www_form(owner)

    {:ok, %{status: status}} = Req.put(signed_req(ctx), url: object_url(ctx, key), body: body)

    assert status in 200..299
  end

  # The lock as an observer sees it: `{:ok, lease}`, or `:free` when no lock object exists.
  defp read_lock_or_free(ctx, shard) do
    case Req.get(signed_req(ctx), url: object_url(ctx, "#{shard}.lock")) do
      {:ok, %{status: 200, body: body}} -> Storage.decode_lease(body)
      {:ok, %{status: 404}} -> :free
      other -> flunk("unexpected lock read: #{inspect(other)}")
    end
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

  # Stage `dead_owner`'s lock as expired past the steal margin, then take it as `new_owner` —
  # the production takeover path, whose steal branch is what invokes the object touch.
  defp steal(ctx, shard, dead_owner, epoch, new_owner) do
    stale_by = Fathom.Shard.Storage.steal_margin_ms() + 5_000
    put_raw_lock(ctx, shard, dead_owner, epoch, now_ms() - stale_by)
    S3.acquire_lease(shard, new_owner, 60_000)
  end

  # `rotate_etag/3` branches on this: a multipart-form etag (`<hash>-<n>`) takes the single-copy
  # touch, a single-form one takes the multipart-copy touch. Reading it here is how the test knows
  # WHICH branch it just exercised, rather than assuming.
  defp multipart_form?(etag), do: etag |> String.trim(~s(")) |> String.contains?("-")

  # One user metadata header off a live HEAD of the shard's data object.
  defp meta_header(ctx, shard, name) do
    %{status: 200, headers: headers} =
      Req.head!(signed_req(ctx), url: object_url(ctx, shard <> ".db"))

    case headers[name] do
      [v | _] -> v
      v -> v
    end
  end
end
