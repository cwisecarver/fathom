defmodule Fathom.Shard.WarmFollowerTest do
  @moduledoc """
  The warm-standby follower (Phase 2, A1-H1): pre-pulls the fleet's recently-active
  shards it doesn't own into a cache dir, so a failover finds them warm. Holds no
  lease and never serves — a pure read cache. Not async (global registry + shared
  storage dir); DataCase shared sandbox lets the follower read the directory.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.Shard.{Connection, Storage, WarmFollower}

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")
  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")

  setup do
    cache_dir =
      Path.join(System.tmp_dir!(), "fathom_warm_test_#{System.unique_integer([:positive])}")

    prev = Application.get_env(:fathom, :warm_cache_dir)
    Application.put_env(:fathom, :warm_cache_dir, cache_dir)

    on_exit(fn ->
      File.rm_rf!(cache_dir)

      if prev,
        do: Application.put_env(:fathom, :warm_cache_dir, prev),
        else: Application.delete_env(:fathom, :warm_cache_dir)
    end)

    :ok
  end

  defp uniq, do: System.unique_integer([:positive])

  # Register a shard in the directory (active, last_active_at now) and seed its S3 object.
  defp seed_shard(id, opts \\ []) do
    {:ok, _} = Directory.resolve(id)

    if Keyword.get(opts, :object, true) do
      seed = Path.join(System.tmp_dir!(), "warmseed_#{id}.db")
      {:ok, c} = Connection.open(seed)
      :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
      :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{id}')")
      :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(c)
      :ok = Storage.flush(id, seed)
      for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)
    end

    on_exit(fn ->
      for dir <- [@remote_dir, @local_dir],
          s <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, id <> s))
    end)

    id
  end

  # Drive one refresh cycle synchronously and return the cached set.
  defp refresh(pid), do: GenServer.call(pid, :refresh)

  test "pre-pulls recently-active shards it doesn't own" do
    a = seed_shard("warm_a_#{uniq()}")
    b = seed_shard("warm_b_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    cached = refresh(pid)

    assert a in cached and b in cached
    assert WarmFollower.cached?(a)
    assert File.exists?(WarmFollower.cache_path(b))
  end

  test "records an etag sidecar per cached shard so a failover can validate freshness" do
    a = seed_shard("warm_etag_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    assert a in refresh(pid)

    # The etag the coordinator presents to Storage.pull_if_changed/3 before promoting.
    assert is_binary(WarmFollower.cached_etag(a))
    assert File.exists?(WarmFollower.cache_path(a) <> ".etag")

    # A revalidation cycle with an unchanged object keeps the same etag (a 304, no churn).
    etag = WarmFollower.cached_etag(a)
    assert a in refresh(pid)
    assert WarmFollower.cached_etag(a) == etag
  end

  test "eviction drops the etag sidecar with the cached file" do
    a = seed_shard("warm_etagevict_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    assert a in refresh(pid)
    assert File.exists?(WarmFollower.cache_path(a) <> ".etag")

    {:ok, _} = Directory.retire(a, DateTime.add(DateTime.utc_now(), 3600))
    refute a in refresh(pid)

    refute WarmFollower.cached?(a)

    refute File.exists?(WarmFollower.cache_path(a) <> ".etag"),
           "eviction must drop the sidecar, not leave a dangling etag"

    assert WarmFollower.cached_etag(a) == nil
  end

  test "skips shards this node already owns" do
    a = seed_shard("warm_owned_#{uniq()}")
    b = seed_shard("warm_other_#{uniq()}")

    # Own `a`: a process registered under it in the shard registry is exactly what
    # `owned_shards` sees for a live coordinator — no full coordinator (lease/pull/
    # timers), so the test stays fully contained.
    test = self()

    owner =
      spawn(fn ->
        {:ok, _} = Registry.register(Fathom.ShardRegistry, a, nil)
        send(test, :owned)
        receive do: (:stop -> :ok)
      end)

    assert_receive :owned

    follower = start_supervised!(WarmFollower)
    cached = refresh(follower)

    refute a in cached, "must not warm a shard this node owns"
    assert b in cached
    refute WarmFollower.cached?(a)

    send(owner, :stop)
  end

  test "evicts shards that leave the active set" do
    a = seed_shard("warm_evict_#{uniq()}")
    b = seed_shard("warm_keep_#{uniq()}")

    pid = start_supervised!(WarmFollower)
    assert a in refresh(pid)
    assert WarmFollower.cached?(a)

    # `a` is retired ⇒ no longer active ⇒ drops out of the target ⇒ evicted next refresh.
    {:ok, _} = Directory.retire(a, DateTime.add(DateTime.utc_now(), 3600))
    cached = refresh(pid)

    refute a in cached
    refute WarmFollower.cached?(a), "a shard that left the active set is evicted from the cache"
    assert b in cached
  end

  test "skips a directory entry that has no S3 object yet" do
    # Active in the directory but never flushed — nothing to warm.
    d = seed_shard("warm_noobj_#{uniq()}", object: false)

    pid = start_supervised!(WarmFollower)
    cached = refresh(pid)

    refute d in cached
    refute WarmFollower.cached?(d)
  end

  # Finding #23: pull_all used Task.async_stream's default on_timeout: :exit, so one pull
  # past the timeout (a congested >timeout S3 fetch — exactly the failover moment) exited the
  # follower mid-cycle. With on_timeout: :kill_task the slow pull is killed and the shard
  # skipped, and the follower survives. Drive it with a pull slower than a short timeout.
  test "a pull past the timeout is killed and skipped, not fatal to the follower (finding #23)" do
    a = seed_shard("warm_slow_#{uniq()}")

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :storage_pull_delay_ms, 400)
    Application.put_env(:fathom, :warm_pull_timeout_ms, 40)

    on_exit(fn ->
      Application.delete_env(:fathom, :shard_storage)
      Application.delete_env(:fathom, :storage_pull_delay_ms)
      Application.delete_env(:fathom, :warm_pull_timeout_ms)
    end)

    pid = start_supervised!(WarmFollower)
    ref = Process.monitor(pid)

    # refresh/1 returning at all is the proof: with on_timeout: :exit this GenServer.call would
    # exit when the pull timed out. The slow shard must not be recorded as warm.
    cached = refresh(pid)

    refute a in cached
    assert Process.alive?(pid)
    refute_received {:DOWN, ^ref, :process, ^pid, _}
  end
end
