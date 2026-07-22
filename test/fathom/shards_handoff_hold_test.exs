defmodule Fathom.ShardsHandoffHoldTest do
  # Expert review #20: a rebalancer handoff flips the LB to the target BEFORE the source drains, so
  # every request landing on the target hits `acquire_lease` -> `{:shard_held, source}` for the
  # drain window. `retry_checkout?` excludes `:held`, and tenant drivers don't retry a mid-request
  # 503 — so a handoff became a burst of client errors on the HOTTEST shard. The fix: when THIS node
  # is the pinned handoff target, Fathom.Shards.checkout holds + retries the acquire up to a bounded
  # budget, so the first post-flip requests QUEUE instead of erroring.
  #
  # Uses DataCase (the Overrides pin is Postgres-backed, and handoff_pin_here? reads it) plus a
  # foreign `.lock` to stand in for the source still holding the lease. Not async — shards + lock
  # files are global.
  use Fathom.DataCase, async: false

  alias Fathom.{Shards, ShardExecutor}
  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.Overrides
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "handoff_#{System.unique_integer([:positive])}"
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_budget = Application.get_env(:fathom, :handoff_held_retry_budget_ms)
    Application.put_env(:fathom, :shard_idle_ms, 50)

    on_exit(fn ->
      restore(:shard_idle_ms, prev_idle)
      restore(:handoff_held_retry_budget_ms, prev_budget)

      for dir <- [@local_dir, @remote_dir],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard, node: Rebalancer.node_key()}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp lock_file(shard), do: Path.join(@remote_dir, "#{shard}.lock")
  defp now_ms, do: System.system_time(:millisecond)

  defp put_live_foreign_lock(shard, owner) do
    File.mkdir_p!(@remote_dir)

    File.write!(
      lock_file(shard),
      Jason.encode!(%{"owner" => owner, "epoch" => 7, "expires_at_ms" => now_ms() + 60_000})
    )
  end

  # Seed a durable stored object (so a takeover cold-open can serve it) and release the lease.
  defp seed_durable(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('served')"))
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 3_000
  end

  defp drain_and_wait(shard) do
    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)
    _ = Shards.drain(shard)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      5_000 -> :ok
    end
  end

  test "a checkout at a pinned handoff target holds + retries, then serves once the source releases",
       %{shard: shard, node: node} do
    seed_durable(shard)

    # Handoff in flight: the LB flipped to THIS node (pin), but the source still holds the lease.
    {:ok, _} = Overrides.pin(shard, node, reason: "test")
    put_live_foreign_lock(shard, "source@node")

    # The source drains/releases shortly after — a helper process removes the lock (no Postgres).
    spawn(fn ->
      Process.sleep(250)
      File.rm(lock_file(shard))
    end)

    # Pre-#20 this returned {:error, {:shard_held, _}} immediately. Now it HOLDS + retries and
    # serves once the lease frees.
    assert {:ok, pid, ref, _path} = Shards.checkout(shard),
           "a pinned handoff target must queue for the drain window, not error"

    Fathom.Shard.checkin(pid, ref)
    drain_and_wait(shard)
  end

  test "a held lease that is NOT a handoff to this node errors immediately (no wasted hold)",
       %{shard: shard} do
    seed_durable(shard)

    # Pinned to ANOTHER node (or not at all): a {:held} here is a genuine foreign lease, not our
    # in-flight handoff — surface it right away rather than stalling the request for the budget.
    {:ok, _} = Overrides.pin(shard, "some_other_node", reason: "test")
    put_live_foreign_lock(shard, "source@node")

    {us, result} = :timer.tc(fn -> Shards.checkout(shard) end)

    assert {:error, {:shard_held, "source@node"}} = result
    assert us < 2_000_000, "a non-handoff held error must not enter the retry budget (#{us} us)"
  end

  test "a pinned handoff whose source never releases falls back to the error at budget exhaustion",
       %{shard: shard, node: node} do
    # Bounded: the hold is not indefinite. A short budget so the test is fast.
    Application.put_env(:fathom, :handoff_held_retry_budget_ms, 300)
    seed_durable(shard)

    {:ok, _} = Overrides.pin(shard, node, reason: "test")
    put_live_foreign_lock(shard, "source@node")

    {us, result} = :timer.tc(fn -> Shards.checkout(shard) end)

    assert {:error, {:shard_held, "source@node"}} = result
    assert us >= 250_000, "the checkout must have held ~the budget before falling back (#{us} us)"
    assert us < 3_000_000, "the hold must be bounded by the budget, not indefinite (#{us} us)"
  end
end
