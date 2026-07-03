defmodule Fathom.Cluster.PartitionTest do
  # Cluster phase (S6): node <-> lease-store (S3) partition behaviour — the lease-store-down
  # runbook scenario as an executable invariant. The lease deliberately FAILS CLOSED: a node that
  # can't reach S3 must not open/serve a shard (better unavailable than split-brain). But a
  # TRANSIENT renew blip is not loss of ownership, so a shard already serving keeps serving.
  # Fault injected via Fathom.Test.FaultyStorage. Helpers + setup from Fathom.ClusterShardCase.
  use Fathom.ClusterShardCase

  alias Fathom.Test.FaultyStorage

  setup do
    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, FaultyStorage)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    :ok
  end

  test "partitioned from the lease store, a checkout FAILS CLOSED (no serve without a lease)",
       %{shard: shard} do
    Application.put_env(:fathom, :storage_fault, :acquire)

    capture_log(fn ->
      assert {:error, {:lease_unavailable, _}} = Shards.checkout(shard)
    end)

    refute File.exists?(local_db(shard)), "a fail-closed start leaves no local copy"
    refute File.exists?(remote_db(shard)), "and writes nothing to storage"
  end

  test "a TRANSIENT renew failure does not fence a serving shard (a blip is not loss of ownership)",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    # Short TTL so renewal fires several times inside the window below.
    Application.put_env(:fathom, :shard_lease_ttl_ms, 60)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)

    capture_log(fn ->
      # The store goes flaky on renew. Several renewal ticks fail transiently; the coordinator
      # must keep serving (retry), NOT self-fence — a blip is not loss of ownership.
      Application.put_env(:fathom, :storage_fault, :renew)
      refute_receive {:DOWN, ^ref, :process, ^coordinator, _}, 200
      # Store recovers; let the shard idle-stop cleanly.
      Application.delete_env(:fathom, :storage_fault)
      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
    end)
  end
end
