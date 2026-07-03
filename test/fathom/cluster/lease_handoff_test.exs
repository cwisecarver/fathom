defmodule Fathom.Cluster.LeaseHandoffTest do
  # Cluster phase (S3): the cross-node OWNERSHIP HANDOFF the LB-keyspace-partition design rests
  # on. When the load balancer remaps a subdomain to a different node (node death or a scale
  # change), the new node must (a) take ownership safely via the S3 lease and (b) serve the
  # shard's data from storage. `shard_lease_test.exs` covers the *negative* halves (a node
  # refuses to start / self-fences against a foreign lease); this covers the *positive* handoff
  # and pins the data-loss boundary. Helpers + setup come from Fathom.ClusterShardCase.
  use Fathom.ClusterShardCase

  test "a clean handoff: a new owner cold-opens the prior owner's FLUSHED data from storage",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # Node A serves, writes, and on idle flushes to storage + releases the lease + drops local.
    serve(shard, fn conn ->
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('alice')"))
    end)

    assert File.exists?(remote_db(shard)), "A flushed the shard to storage"
    refute File.exists?(local_db(shard)), "A dropped its local copy on idle"
    refute File.exists?(lock_file(shard)), "a clean flush releases the lease"

    # Node B takes over (LB remapped the subdomain here). The local copy is gone, so B can only
    # be serving data it cold-pulled from storage — that is the handoff.
    got = serve(shard, &select_v/1)
    assert got == ["alice"]
  end

  test "steal-on-lapse: a checkout steals a crashed node's EXPIRED lease and serves the data",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # Get durable data into storage, then make it look like the owning node CRASHED without
    # releasing: an expired `.lock` left behind by "dead@node".
    serve(shard, fn conn ->
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('survived')"))
    end)

    # Expired well past the steal margin (no heartbeat object in test ⇒ liveness falls back to
    # the lock's TTL — finding #11), so the crashed owner is genuinely dead, not just skewed.
    put_raw_lock(shard, "dead@node", 5, now_ms() - 60_000)

    # The new owner's checkout SUCCEEDS here (unlike against a LIVE foreign lease, which is
    # refused): it steals the expired lease and serves the flushed data.
    got = serve(shard, &select_v/1)
    assert got == ["survived"]
    refute File.exists?(lock_file(shard)), "the new owner released the lease on its clean flush"
  end

  test "steal-on-lapse bumps the fencing epoch", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # A crashed prior owner left an expired epoch-5 lease and NO running coordinator. A fresh
    # checkout starts a coordinator that steals it, bumping the fencing token 5 -> 6.
    # Expired well past the steal margin (no heartbeat object in test ⇒ liveness falls back to
    # the lock's TTL — finding #11), so the crashed owner is genuinely dead, not just skewed.
    put_raw_lock(shard, "dead@node", 5, now_ms() - 60_000)

    {:ok, conn} = ShardExecutor.open(shard)
    # Read while the connection is open (lease live, not yet released by an idle flush).
    assert read_lock_epoch(shard) == 6, "stealing an epoch-5 lease bumps the fencing token to 6"

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
  end

  test "data boundary on a steal: the new owner sees FLUSHED writes, not committed-but-unflushed",
       %{shard: shard} do
    # Pins the design's RPO / at-least-once contract: a write committed but not yet flushed when
    # ownership is lost is dropped (the loser self-fences WITHOUT flushing, so it can't clobber
    # the new owner). The new owner sees only the last flushed state.
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # 'durable' is written and flushed to storage, so it survives the handoff.
    serve(shard, fn conn ->
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('durable')"))
    end)

    assert File.exists?(remote_db(shard))

    # Re-open (cold-pull 'durable'), write 'unflushed' (local only), then a NEW node steals the
    # lease out from under us before we flush.
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('unflushed')"))
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      # Closing the last connection lets the shard idle -> flush. The flush re-checks ownership,
      # sees the steal, and drops local WITHOUT flushing 'unflushed'.
      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
    end)

    # The thief is a simulated lock; clear it so the real new owner can take over and read.
    File.rm(lock_file(shard))

    got = serve(shard, &select_v/1)
    assert got == ["durable"], "the flushed write survives the handoff"

    refute "unflushed" in got,
           "the committed-but-unflushed write is dropped on the steal (RPO boundary)"
  end
end
