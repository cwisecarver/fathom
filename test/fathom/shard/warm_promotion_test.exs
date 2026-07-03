defmodule Fathom.Shard.WarmPromotionTest do
  @moduledoc """
  Phase 2 A1-H2: freshness-validated promotion of a warm-follower cache at cold-open.

  The warm follower keeps recently-active shards cached on a standby node so a failover
  skips the cold pull from S3. But a cached copy may lag the owner's latest flush, so the
  coordinator must NEVER serve it as-is — it validates the cache's etag against storage
  first (`Storage.pull_if_changed/3`) and promotes it only when it equals the current
  object, otherwise it re-pulls the fresh bytes.

  These tests drive a real coordinator through `Fathom.ShardExecutor` and assert on the
  bytes it serves. The load-bearing one is "a stale cache is re-pulled, not served" — the
  pinned "never serve stale" invariant; it fails if the coordinator blindly promotes the
  cache. Not async: shards + storage dirs are global.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shards, ShardExecutor}
  alias Fathom.Shard.{Connection, Storage, WarmFollower}
  alias Filo.{Stmt, StmtResult}

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    prev_cache = Application.get_env(:fathom, :warm_cache_dir)

    cache_dir =
      Path.join(System.tmp_dir!(), "fathom_warmpromo_#{System.unique_integer([:positive])}")

    Application.put_env(:fathom, :shard_idle_ms, 50)
    # Disable the periodic durability flush so it can't re-upload mid-test.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :warm_cache_dir, cache_dir)

    shard = "warmpromo_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      restore(:shard_idle_ms, prev_idle)
      restore(:shard_flush_interval_ms, prev_flush)
      restore(:warm_cache_dir, prev_cache)
      File.rm_rf!(cache_dir)

      for base <- [Path.join(@local_dir, "#{shard}.db"), Path.join(@remote_dir, "#{shard}.db")],
          suffix <- ["", "-wal", "-shm"],
          do: File.rm(base <> suffix)

      File.rm(Path.join(@remote_dir, "#{shard}.lock"))
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp uniq, do: System.unique_integer([:positive])
  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # Write a valid one-row db and flush it as the shard's storage object.
  defp seed_remote(shard, value) do
    src = Path.join(System.tmp_dir!(), "wpseed_#{shard}_#{uniq()}.db")
    {:ok, c} = Connection.open(src)
    :ok = Connection.exec(c, "CREATE TABLE IF NOT EXISTS kv (v TEXT)")
    :ok = Connection.exec(c, "DELETE FROM kv")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{value}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, src)
    for s <- ["", "-wal", "-shm"], do: File.rm(src <> s)
    :ok
  end

  # Populate the warm cache from storage's *current* object, exactly as the follower
  # does: capture the object's etag into the sidecar. After this the cache == storage.
  defp warm_cache_from_remote(shard) do
    path = WarmFollower.cache_path(shard)
    File.mkdir_p!(Path.dirname(path))
    {:ok, {:written, etag}} = Storage.pull_if_changed(shard, path, nil)
    File.write!(path <> ".etag", etag)
    etag
  end

  # Seed a valid db directly in the LIVE data dir (stands in for this node's own
  # un-flushed writes from a prior boot — a warm *restart*, authoritative).
  defp seed_live(shard, value) do
    path = Path.join(@local_dir, "#{shard}.db")
    File.mkdir_p!(Path.dirname(path))
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE IF NOT EXISTS kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{value}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok
  end

  defp attach_promoted_telemetry(shard) do
    test = self()
    id = "warmpromo-#{shard}-#{uniq()}"

    :telemetry.attach(
      id,
      [:fathom, :shard, :warm, :promoted],
      fn _event, _measure, meta, _ ->
        if meta.shard_id == shard, do: send(test, {:warm_promoted, meta.result})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  # Open the shard through the executor, read the single kv value, then let it idle-stop
  # so the local copy is dropped before the next assertion. Returns the served value.
  defp serve_value(shard) do
    {:ok, coordinator} = Shards.ensure(shard)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, %StmtResult{rows: [[v]]}} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))

    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
    v
  end

  test "a current warm cache is promoted (304) without a full re-pull", %{shard: shard} do
    seed_remote(shard, "v1")
    warm_cache_from_remote(shard)
    attach_promoted_telemetry(shard)

    assert serve_value(shard) == "v1"
    assert_receive {:warm_promoted, :hit}, 2_000
  end

  test "a STALE warm cache is re-pulled fresh, never served", %{shard: shard} do
    # Cache captured at v1 ...
    seed_remote(shard, "v1")
    warm_cache_from_remote(shard)
    # ... then the owner flushes v2. The cache now lags storage.
    seed_remote(shard, "v2")
    attach_promoted_telemetry(shard)

    # The invariant: never serve the stale cached bytes (v1) — re-pull the current v2.
    assert serve_value(shard) == "v2",
           "a warm cache behind storage's latest flush must be re-pulled, not served stale"

    assert_receive {:warm_promoted, :stale}, 2_000
  end

  test "with no warm cache the cold-open path is unchanged", %{shard: shard} do
    seed_remote(shard, "cold")
    attach_promoted_telemetry(shard)

    assert serve_value(shard) == "cold"
    refute_receive {:warm_promoted, _}, 300
  end

  test "a live-dir warm restart wins over the follower cache (own writes authoritative)",
       %{shard: shard} do
    # This node's own un-flushed local copy says "restart"; the follower cache and the
    # storage object both say something else. The restart copy must win untouched — we
    # must not freshness-check (and possibly clobber) our own un-flushed writes.
    seed_live(shard, "restart")
    seed_remote(shard, "remote")
    warm_cache_from_remote(shard)
    attach_promoted_telemetry(shard)

    assert serve_value(shard) == "restart"
    refute_receive {:warm_promoted, _}, 300
  end
end
