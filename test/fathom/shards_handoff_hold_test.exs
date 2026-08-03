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
  alias Fathom.Shard.Storage
  alias Filo.Stmt

  setup do
    shard = "handoff_#{System.unique_integer([:positive])}"
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_budget = Application.get_env(:fathom, :handoff_held_retry_budget_ms)
    prev_crash = Application.get_env(:fathom, :crash_failover_hold_ms)
    prev_margin = Application.get_env(:fathom, :steal_margin_ms)
    Application.put_env(:fathom, :shard_idle_ms, 50)
    # Off by default here so the #20 (handoff) tests exercise only the pin path; the #21 tests
    # opt it back in.
    Application.put_env(:fathom, :crash_failover_hold_ms, 0)

    on_exit(fn ->
      restore(:shard_idle_ms, prev_idle)
      restore(:handoff_held_retry_budget_ms, prev_budget)
      restore(:crash_failover_hold_ms, prev_crash)
      restore(:steal_margin_ms, prev_margin)
      Storage.clear_heartbeat("dead@node#1")
      Storage.clear_heartbeat("live@node#1")

      for dir <- [local_dir(), remote_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard, node: Rebalancer.node_key()}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp lock_file(shard), do: Path.join(remote_dir(), "#{shard}.lock")
  defp now_ms, do: System.system_time(:millisecond)

  defp put_live_foreign_lock(shard, owner), do: put_foreign_lock(shard, owner, 60_000)

  defp put_foreign_lock(shard, owner, expires_in_ms) do
    File.mkdir_p!(remote_dir())

    File.write!(
      lock_file(shard),
      Jason.encode!(%{
        "owner" => owner,
        "epoch" => 7,
        "expires_at_ms" => now_ms() + expires_in_ms
      })
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

  # --- #21: hard-crash failover tail ---------------------------------------------------------

  test "a checkout at the tail of a crashed owner's TTL window holds + retries, then steals and serves",
       %{shard: shard} do
    Application.put_env(:fathom, :crash_failover_hold_ms, 5_000)
    Application.put_env(:fathom, :steal_margin_ms, 100)
    seed_durable(shard)

    # A hard-crashed owner: its heartbeat OBJECT survives (not cleared) but is frozen, expiring in
    # ~200ms — so the steal becomes possible ~300ms out (exp + margin), inside the 5s budget.
    #
    # Its LOCK expires on the same horizon, because an owner is dead only when BOTH have lapsed
    # (expert review 2026-08-01 #12). This used to write a 60s lock and still expect the steal,
    # which only worked because #12 was unfixed in the Local backend. 60s was never realistic
    # anyway: in heartbeat mode coordinators do NO per-shard renewal, so a crashed owner's lock
    # simply runs out `shard_lease_ttl_ms` after ITS acquire — for any shard open longer than the
    # TTL the lock has already lapsed and the heartbeat is the binding signal, which is the case
    # this test is about.
    put_foreign_lock(shard, "dead@node#1", 200)
    Storage.renew_heartbeat("dead@node#1", 200)

    # Pre-#21 this errored immediately (retry_checkout? excludes :held). Now it HOLDS + retries and
    # serves once the frozen heartbeat ages out and the acquire steals.
    assert {:ok, pid, ref, _path} = Shards.checkout(shard),
           "the crash-window tail must queue for the imminent steal, not error"

    Fathom.Shard.checkin(pid, ref)
    drain_and_wait(shard)
  end

  test "a held lease whose owner is LIVE (heartbeat far from expiry) errors immediately, never held",
       %{shard: shard} do
    Application.put_env(:fathom, :crash_failover_hold_ms, 5_000)
    seed_durable(shard)

    # A genuinely-live foreign owner keeps its heartbeat ~ttl ahead of now; holder_stealable_soon?
    # is false, so we must NOT hold a request for it (that would be worse than the immediate error).
    put_live_foreign_lock(shard, "live@node#1")
    Storage.renew_heartbeat("live@node#1", 30_000)

    {us, result} = :timer.tc(fn -> Shards.checkout(shard) end)

    assert {:error, {:shard_held, "live@node#1"}} = result
    assert us < 2_000_000, "a live holder must not enter the crash-hold budget (#{us} us)"
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
