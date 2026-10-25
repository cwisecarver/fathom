defmodule Fathom.Shard.Storage.StealWatchTest do
  # Jepsen finding 3 (2026-10-24, docs/reviews/jepsen-2026-10-24.md): a steal decided "is the owner's
  # heartbeat expired?" by comparing S3's Date with an expiry the OWNER stamped from its own wall
  # clock. A 35 s clock step on the rig put every live owner's heartbeat 35 s "in the past" by the
  # store's clock, two nodes stole a shard whose owner was still renewing and acking, and acked
  # writes were lost.
  #
  # The invariant pinned here: a stealer takes a foreign hold only after it has ITSELF seen the
  # owner's lock and heartbeat unchanged for a whole watch window on its own monotonic clock. A live
  # owner renews (changing the heartbeat) far more often than that, so no clock disagreement can make
  # it stealable. Pre-fix, the first test steals on the first call.
  #
  # The watch is off in config/test.exs (`:steal_watch_ms` 0) so the many steal tests that seed an
  # already-expired lock keep their timing; every test here turns it on and asserts it took.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @shard "shard_watch"
  @data_key "shard_watch.db"
  @lock_key "shard_watch.lock"
  # No `@`/`#`: the heartbeat key is URL-encoded, and this fixture writes it by its raw name.
  @owner "liveowner"
  @hb_key "heartbeats/liveowner"
  @window 60_000
  @watch_table Fathom.Shard.Storage.StealWatch

  setup do
    prev = Application.get_env(:fathom, S3)
    prev_watch = Application.get_env(:fathom, :steal_watch_ms)
    prev_margin = Application.get_env(:fathom, :steal_margin_ms)

    Application.put_env(:fathom, :steal_watch_ms, @window)
    # Margin 0 also turns the heartbeat read memo off (its TTL is margin / 5), so every acquire
    # below reads the heartbeat the test just wrote rather than a cached one.
    Application.put_env(:fathom, :steal_margin_ms, 0)
    Storage.reset_steal_watch()

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)

      restore(:steal_watch_ms, prev_watch)
      restore(:steal_margin_ms, prev_margin)
      Storage.reset_steal_watch()
    end)

    assert :ets.whereis(@watch_table) != :undefined, "the watch table is not up"
    assert Storage.steal_watch_window_ms() == @window, "the watch is not on"
    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp start_store(store_now, hb_exp, lock_exp) do
    objects = %{
      @data_key => "shard-bytes",
      @lock_key => Storage.encode_lease(%{owner: @owner, epoch: 5, expires_at_ms: lock_exp}),
      @hb_key => Storage.encode_heartbeat(%{owner: @owner, expires_at_ms: hb_exp})
    }

    store = start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})
    S3EtagStore.set_date_ms(store, store_now)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    store
  end

  defp put_object(store, key, body) do
    Agent.update(store, fn s ->
      %{s | objects: Map.put(s.objects, key, %{body: body, form: :single, meta: %{}})}
    end)
  end

  # Make this node's current observation of the hold `ms` older, as if that much monotonic time had
  # passed with nothing changing. Deterministic, unlike sleeping through a window.
  defp age_watch(ms) do
    [{@shard, fingerprint, since}] = :ets.lookup(@watch_table, @shard)
    :ets.insert(@watch_table, {@shard, fingerprint, since - ms})
  end

  test "precondition: with the watch off, the lagging-clock fixture IS stolen (the old rule)" do
    Application.put_env(:fathom, :steal_watch_ms, 0)
    owner_now = Storage.now_ms()
    store = start_store(owner_now + 35_000, owner_now + 30_000, owner_now + 30_000)

    assert {:ok, %{took_over: true}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000),
           "the fixture does not make a live owner look dead to the store-clock rule"

    _ = store
  end

  test "a live owner whose clock lags the store is not stolen while it keeps renewing" do
    owner_now = Storage.now_ms()
    # The owner stamped a fresh 30 s expiry from ITS clock, but the store's clock is 35 s ahead
    # (the rig's clock-jump), so by the store-clock rule the owner has been dead for 5 s.
    store = start_store(owner_now + 35_000, owner_now + 30_000, owner_now + 30_000)

    assert {:error, {:held, @owner, _}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000),
           "stolen on the first look from an owner that only LOOKS dead by the store's clock"

    # The owner renews (its clock still lags, so the new expiry still reads as past to the store)...
    age_watch(@window - 1)

    put_object(
      store,
      @hb_key,
      Storage.encode_heartbeat(%{owner: @owner, expires_at_ms: owner_now + 30_001})
    )

    # ...which restarts the watch, so a full window after the FIRST look is not enough.
    age_watch(1)

    assert {:error, {:held, @owner, _}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000),
           "a renewal inside the window must restart the watch"

    assert S3EtagStore.body_of(store, @lock_key) =~ @owner, "the lock was not taken"
  end

  test "an owner seen unchanged for the whole window is stolen" do
    owner_now = Storage.now_ms()
    store = start_store(owner_now + 35_000, owner_now + 30_000, owner_now + 30_000)

    assert {:error, {:held, @owner, at}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000)

    # The steal instant a caller is handed accounts for the watch, so a checkout holding for it
    # aims at the real steal, not at the store-clock instant that has already passed.
    assert is_integer(at) and at >= Storage.now_ms() + @window - 1_000,
           "the reported steal instant ignores the watch"

    age_watch(@window)

    assert {:ok, %{took_over: true, epoch: 6}} =
             S3.acquire_lease(@shard, "thief@node#inc2", 30_000),
           "a hold unchanged for the whole window is dead and is stolen"

    _ = store
  end

  test "a lock renewal (legacy mode, no heartbeat) restarts the watch" do
    owner_now = Storage.now_ms()
    store = start_store(owner_now + 35_000, owner_now + 30_000, owner_now + 30_000)
    Agent.update(store, fn s -> %{s | objects: Map.delete(s.objects, @hb_key)} end)

    assert {:error, {:held, @owner, _}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000)
    age_watch(@window - 1)

    put_object(
      store,
      @lock_key,
      Storage.encode_lease(%{owner: @owner, epoch: 5, expires_at_ms: owner_now + 30_001})
    )

    age_watch(1)

    assert {:error, {:held, @owner, _}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000),
           "a renewed lock must restart the watch"
  end

  test "the store-clock rule still applies: a finished watch does not steal a fresh-looking owner" do
    owner_now = Storage.now_ms()
    # Owner clock AHEAD of the store: by the store clock the heartbeat has 40 s left.
    _store = start_store(owner_now - 10_000, owner_now + 30_000, owner_now + 30_000)

    assert {:error, {:held, @owner, _}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000)
    age_watch(@window)

    assert {:error, {:held, @owner, _}} = S3.acquire_lease(@shard, "thief@node#inc2", 30_000),
           "the watch is an AND with the store-clock rule, never a replacement"
  end

  # `./chaos.sh rpo`, 2026-10-25: the first version of the watch also gated a RELEASED marker, so a
  # shard its holder had cleanly let go of answered "temporarily owned by another node" to every
  # other node for a whole window — every rebalance and idle handoff, 35 s in prod. A release is the
  # holder's own statement, not a clock reading, so it is taken on the first look.
  test "a released marker is taken on the first look, with no watch" do
    owner_now = Storage.now_ms()
    store = start_store(owner_now, owner_now + 30_000, owner_now + 30_000)

    put_object(
      store,
      @lock_key,
      Storage.encode_lease(Storage.released_marker(%{epoch: 5}, nil))
    )

    assert {:ok, %{took_over: true, epoch: 6}} =
             S3.acquire_lease(@shard, "next@node#inc2", 30_000),
           "a cleanly released shard waited out the steal watch"
  end

  test "the default window is the lease TTL plus the steal margin" do
    Application.delete_env(:fathom, :steal_watch_ms)
    Application.put_env(:fathom, :steal_margin_ms, 5_000)
    ttl = Application.get_env(:fathom, :shard_lease_ttl_ms, 30_000)
    assert Storage.steal_watch_window_ms() == ttl + 5_000
  end

  describe "watch_since/3" do
    test "an unchanged fingerprint keeps its start" do
      assert Storage.watch_since({:fp, 100}, :fp, 900) == {100, false}
    end

    test "a changed fingerprint, or none yet, starts now" do
      assert Storage.watch_since({:old, 100}, :new, 900) == {900, true}
      assert Storage.watch_since(nil, :new, 900) == {900, true}
    end
  end
end
