defmodule Fathom.Cluster.CrashContractTest do
  # Cluster phase (S4): the CLIENT-FACING crash contract (design finding F3). When the owning
  # node dies mid-stream, the LB reroutes the subdomain and the client reconnects to the new
  # node. This pins what the client can rely on across that handoff:
  #
  #   - an UNCOMMITTED transaction leaves no partial data (SQLite atomicity, preserved across
  #     the handoff), so a retry is safe;
  #   - a COMMITTED write whose ack was lost is AT-LEAST-ONCE, not exactly-once — a blind retry
  #     double-applies. Clients needing exactly-once must use idempotency keys.
  #
  # The transport-level "client sees a retryable error on node death" half is the load
  # balancer's job (a dropped TCP connection is inherently retryable; proven by the S1 spike's
  # in-request failover), so it is not re-tested here. S3 covered the storage/RPO side; this is
  # the transaction-semantics side. Helpers + setup come from Fathom.ClusterShardCase.
  use Fathom.ClusterShardCase

  test "commit-ack-lost is at-least-once: a blind retry double-applies a non-idempotent write",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # The owner commits INSERT 'x' and it reaches durable storage (the idle flush stands in for
    # "the commit was durable"), but the ack never reaches the client — the node died first.
    serve(shard, fn conn ->
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))
    end)

    # Not knowing whether it committed, the client reconnects (LB reroutes to a new node) and
    # BLINDLY RETRIES the same INSERT. Hrana/libSQL is at-least-once across a node death.
    got =
      serve(shard, fn conn ->
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))
        select_v(conn)
      end)

    assert got == ["x", "x"],
           "a blind retry of an already-committed write applies twice (at-least-once, not exactly-once)"
  end

  test "an interrupted uncommitted transaction leaves no partial data after the handoff",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 50)

    # 'base' is committed and flushed — it must survive.
    serve(shard, fn conn ->
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('base')"))
    end)

    # The client is mid-transaction (BEGIN, INSERT, no COMMIT) when its node is lost: a new node
    # steals the lease and the old owner self-fences on close without flushing.
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('pending')"))
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
    end)

    File.rm(lock_file(shard))

    # The new owner sees only the committed-and-flushed 'base'; the uncommitted 'pending' left no
    # trace. A client that hit this mid-transaction simply retries the whole transaction.
    got = serve(shard, &select_v/1)
    assert got == ["base"]
    refute "pending" in got
  end
end
