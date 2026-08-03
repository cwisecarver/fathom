defmodule Fathom.Shard.Storage.S3SentinelCopyTest do
  @moduledoc """
  A steal sentinel must never propagate through a server-side copy (expert review 2026-08-01 #25).

  ## Why this is the same bug as #24, not a new one

  `touch_object/2` plants a sentinel **at the data key** on a steal of a never-flushed shard
  (round-2 #7 — the zombie's stalled create-only `PUT If-None-Match:*` has to 412, and it only
  does if that key is occupied). CopyObject's default metadata directive is `COPY`, so
  `retain`/`snapshot`/`fork_shard`/`fork_from`/`restore_snapshot` all duplicated the placeholder
  verbatim and reported `:ok`.

  #24 was the same root cause — a sentinel read as real bytes — on the *pull* consumers. The panel
  rated both "Low likelihood (needs a steal of a never-flushed shard)" and both were deferred on
  that basis. The chaos rig then hit exactly that state **three times in one 180s soak**, so the
  shared likelihood rationale is measured-false, which is why #25 was re-promoted.

  ## The two directions, and which one is worst

  * **Sentinel source** — the copy succeeds and stores a placeholder. The sharpest case is
    `retain/2`, which is the migration's PRE-MIGRATION BACKUP (`Migrator.ShardMigration`): the
    backup was silently never taken and the migration proceeded believing it had a rollback.
    `#24`'s fix makes the eventual `restore/3` fail honestly (`:version_absent`) — but only at
    revert time, long after the window to do anything about it.
  * **Sentinel destination** — `fork_shard/2` HEADed the dst with `head_etag/1`, which returns a
    plain `{:ok, etag}` for ANY 200, so a dst holding only a sentinel read as "destination taken"
    and returned `:dst_exists` **forever**. `Tenants.fork/3` can inflict this on itself:
    `fork_into_leased_dst/2` acquires the dst lease first, and a stale dst lock routes through the
    steal path whose `touch_object` plants the sentinel — so the very next `fork_shard/2` call in
    the same function failed on a placeholder it had just created.

  ## Fixture realism

  Every sentinel here is planted by driving a **real steal** (`acquire_lease` over an expired
  lock), not by hand-writing metadata into the fake store. A fabricated sentinel would prove the
  guard matches a header; a real one proves it matches what `touch_object/2` actually writes.

  Note `Fathom.Shard.Storage.Local` has no sentinel concept, so this whole bug class is
  structurally unreachable through it — that is finding #30 item (7), still open.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

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

  # Plant a genuine sentinel at `<shard>.db` by stealing a never-flushed shard.
  defp steal_to_plant_sentinel(store, shard) do
    assert {:ok, %{took_over: true}} = S3.acquire_lease(shard, "b@node#inc2", 30_000)

    assert S3EtagStore.meta_of(store, "#{shard}.db")[@sentinel_meta] == "1",
           "the fixture did not actually plant a sentinel — the rest of this test proves nothing"

    :ok
  end

  describe "a sentinel SOURCE is refused, not copied" do
    test "retain/2 refuses — the migration's pre-migration backup must not silently be a placeholder" do
      store = start_store(%{"acme.lock" => dead_lock()})
      :ok = steal_to_plant_sentinel(store, "acme")

      assert {:error, :no_source} = S3.retain("acme", 7),
             "retain reported :ok having copied a placeholder; the migration then ran believing " <>
               "it had a rollback"

      refute S3EtagStore.meta_of(store, "acme@7.db"),
             "no version object should exist at all"
    end

    test "snapshot/2 refuses" do
      store = start_store(%{"acme.lock" => dead_lock()})
      :ok = steal_to_plant_sentinel(store, "acme")

      assert {:error, :no_source} = S3.snapshot("acme", "snap1")
      refute S3EtagStore.meta_of(store, "acme@snap-snap1.db")
    end

    test "fork_shard/2 refuses a sentinel src instead of forking an empty tenant" do
      store = start_store(%{"src.lock" => dead_lock()})
      :ok = steal_to_plant_sentinel(store, "src")

      assert {:error, :no_source} = S3.fork_shard("src", "dst")

      refute S3EtagStore.meta_of(store, "dst.db"),
             "the fork must not have created dst — Tenants.fork would then register it at the " <>
               "SOURCE's schema_version, so the laggard sweep never migrates the empty tenant"
    end

    test "fork_from/3 refuses a sentinel version, and it classifies as :no_template_snapshot" do
      # A version key can only hold a sentinel if a pre-fix retain/2 put one there. That state is
      # already durable in any bucket this ran against, so the guard has to cover it.
      store = start_store(%{"tmpl.lock" => dead_lock()})
      :ok = steal_to_plant_sentinel(store, "tmpl")
      :ok = S3EtagStore.copy(store, "tmpl.db", "tmpl@3.db")

      assert {:error, :no_source} = S3.fork_from("tmpl", 3, "newborn")
      refute S3EtagStore.meta_of(store, "newborn.db")
    end

    test "restore_snapshot/2 refuses a sentinel snapshot rather than clobbering live with it" do
      store = start_store(%{"acme.lock" => dead_lock()})
      :ok = steal_to_plant_sentinel(store, "acme")
      :ok = S3EtagStore.copy(store, "acme.db", "acme@snap-poisoned.db")

      assert {:error, :no_source} = S3.restore_snapshot("acme", "poisoned")
    end
  end

  describe "a sentinel DESTINATION is absent, not taken" do
    test "fork_shard/2 forks into a sentinel dst instead of wedging it at :dst_exists forever" do
      store = start_store(%{"src.db" => "real-shard-bytes", "dst.lock" => dead_lock()})
      :ok = steal_to_plant_sentinel(store, "dst")

      assert :ok = S3.fork_shard("src", "dst"),
             "a dst holding only a placeholder read as 'destination taken', permanently " <>
               "poisoning that tenant id"

      assert S3EtagStore.meta_of(store, "dst.db")[@sentinel_meta] == nil,
             "the sentinel must be replaced by the forked bytes, not preserved by the copy"
    end

    test "a REAL dst object still refuses — never clobber a tenant" do
      # The guard must not have widened :dst_exists into "overwrite anything".
      start_store(%{"src.db" => "real-src", "dst.db" => "real-dst"})

      assert {:error, :dst_exists} = S3.fork_shard("src", "dst")
    end
  end

  describe "the guard does not fire on healthy objects" do
    test "retain, snapshot and fork all still succeed against real bytes" do
      store = start_store(%{"acme.db" => "real-shard-bytes"})

      assert :ok = S3.retain("acme", 4)
      assert :ok = S3.snapshot("acme", "snap1")
      assert :ok = S3.fork_shard("acme", "clone")
      assert :ok = S3.fork_from("acme", 4, "newborn")

      assert S3EtagStore.meta_of(store, "clone.db") == %{}
      assert S3EtagStore.meta_of(store, "newborn.db") == %{}
    end

    test "a MISSING source keeps its existing error shape, which callers match on" do
      # Not widened to :no_source: ShardMigration.classify_fork_error/1 maps
      # {:s3_copy_status, 404} to :no_template_snapshot, and fork_shard/2's absent-src contract
      # is :no_source. Both must survive the sentinel guard.
      start_store(%{})

      assert {:error, {:s3_copy_status, 404}} = S3.fork_from("tmpl", 9, "newborn")
      assert {:error, :no_source} = S3.fork_shard("nothing", "dst")
    end
  end
end
