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

    # Pre-#20 this returned {:error, {:shard_held, _, _}} immediately. Now it HOLDS + retries and
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

    assert {:error, {:shard_held, "source@node", _}} = result
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

    assert {:error, {:shard_held, "source@node", _}} = result
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

  # Expert review 2026-08-26 #23. `holder_stealable_soon?` asked the backend for the exact instant
  # the hold becomes stealable, reduced it to a boolean, and threw the instant away — and then
  # `backoff_held` rediscovered that same instant by POLLING: 50, 100, 200, 400, 800, 1 000, 1 000,
  # 1 000 ms, roughly eight retries inside the 5 s budget. Each retry re-enters `do_checkout/3`,
  # which starts a new coordinator and re-pays a create_lock PUT plus a get_lock GET, so a single
  # held-and-retried checkout cost ~17 S3 requests. On a hard-crash failover the survivor pays that
  # per shard, across the dead node's whole keyspace slice, on its already-contended pool.
  #
  # The invariant is the retry SHAPE, not latency: with the steal instant known, the wait is aimed
  # at it once rather than stepped up to it. Asserted on the `:held_retry` event's count and its
  # `aimed:` flag, which is deterministic — a wall-clock assertion here would be exactly the
  # timing-fixture flake AGENTS.md warns about.
  #
  # PROBED by restoring the old wait computation (`min(backoff, remaining)`) while keeping the
  # telemetry: it emitted 5 un-aimed retries before the steal instead of 1 aimed one.
  test "a known steal instant is slept TO once, not rediscovered by polling", %{shard: shard} do
    Application.put_env(:fathom, :crash_failover_hold_ms, 5_000)
    Application.put_env(:fathom, :steal_margin_ms, 100)
    seed_durable(shard)

    test_pid = self()
    handler = "heldretry-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :held_retry],
      fn _e, m, meta, _ ->
        if meta.shard_id == shard, do: send(test_pid, {:held_retry, m.wait_ms, meta.aimed})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # A crashed owner whose steal instant is ~1.3 s out (heartbeat + lock both expiring in 1.2 s,
    # plus the 100 ms steal margin) — comfortably inside the 5 s budget, and far enough past the
    # early backoff steps that polling to it takes several attempts.
    put_foreign_lock(shard, "dead@node#1", 1_200)
    Storage.renew_heartbeat("dead@node#1", 1_200)

    assert {:ok, pid, ref, _path} = Shards.checkout(shard),
           "the crash-window tail must queue for the imminent steal, not error"

    Fathom.Shard.checkin(pid, ref)

    waits = drain_held_retries([])

    assert length(waits) <= 2,
           "#{length(waits)} retries to reach a steal instant the backend had already computed " <>
             "(waits: #{inspect(waits)})"

    assert [{first_wait, true} | _] = waits,
           "the first wait must be AIMED at the known steal instant, not a backoff step " <>
             "(waits: #{inspect(waits)})"

    # ~1.3 s to the instant, plus at most @steal_wait_jitter_ms of spread. The bound is
    # order-of-magnitude, not exact: it only has to exclude the 50 ms first backoff step.
    assert first_wait > 500 and first_wait <= 1_500,
           "the aimed wait was #{first_wait}ms; it should land near the ~1.3s steal instant"

    drain_and_wait(shard)
  end

  defp drain_held_retries(acc) do
    receive do
      {:held_retry, wait, aimed} -> drain_held_retries([{wait, aimed} | acc])
    after
      0 -> Enum.reverse(acc)
    end
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

    assert {:error, {:shard_held, "live@node#1", _}} = result
    assert us < 2_000_000, "a live holder must not enter the crash-hold budget (#{us} us)"
  end

  # The crash-hold prediction must agree with what acquire_lease will actually DO.
  #
  # `holder_stealable_soon?` used to read the heartbeat by itself and compare
  # `hb_exp + steal_margin` against the budget. #12 then made a steal require BOTH the heartbeat
  # and the lock TTL to have lapsed, and nothing updated the predictor — so for a holder with a
  # lapsing heartbeat but a still-fresh lock, the two disagreed: the checkout held and retried for
  # the WHOLE budget waiting for a steal that could not happen, then returned the same error it
  # would have returned immediately. Bounded, never unsafe, and pure latency on a failing request.
  #
  # The discriminator is the `:crash_wait` telemetry event, which `start_held_retry/5` emits only
  # when it decides to hold — cleaner than timing, and it says WHY the wait happened.
  test "a holder whose heartbeat lapses but whose lock TTL does not is NOT waited for",
       %{shard: shard} do
    Application.put_env(:fathom, :crash_failover_hold_ms, 5_000)
    Application.put_env(:fathom, :steal_margin_ms, 100)
    seed_durable(shard)

    test_pid = self()
    handler = "crashwait-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :crash_wait],
      fn _e, _m, meta, _ -> send(test_pid, {:crash_wait, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # Heartbeat expiring in ~200ms — well inside the 5s budget, so the OLD predictor says
    # "stealable soon". The LOCK runs 60s out, so under #12 the owner is not dead until then and
    # acquire_lease will refuse for the whole window.
    put_foreign_lock(shard, "halfdead@node#1", 60_000)
    Storage.renew_heartbeat("halfdead@node#1", 200)

    {us, result} = :timer.tc(fn -> Shards.checkout(shard) end)

    assert {:error, {:shard_held, "halfdead@node#1", _}} = result

    refute_receive {:crash_wait, %{shard_id: ^shard}}, 100

    assert us < 2_000_000,
           "held for #{div(us, 1000)}ms waiting on a steal the lock TTL forbids for another 60s"
  end

  test "a holder with BOTH signals lapsing soon is still waited for", %{shard: shard} do
    # The other side: the fix must not have turned the crash-hold off entirely. This is the same
    # shape as the #21 test above, asserted through the telemetry event so the two are directly
    # comparable.
    Application.put_env(:fathom, :crash_failover_hold_ms, 5_000)
    Application.put_env(:fathom, :steal_margin_ms, 100)
    seed_durable(shard)

    test_pid = self()
    handler = "crashwait2-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :crash_wait],
      fn _e, _m, meta, _ -> send(test_pid, {:crash_wait, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    put_foreign_lock(shard, "dead@node#2", 200)
    Storage.renew_heartbeat("dead@node#2", 200)

    assert {:ok, pid, ref, _path} = Shards.checkout(shard)
    assert_received {:crash_wait, %{shard_id: ^shard}}

    Fathom.Shard.checkin(pid, ref)
    drain_and_wait(shard)
  end

  # THE PARKED TIER OF #23, now shipped. `acquire_lease` decides `:live` by reading the lock and the
  # holder's heartbeat — everything `stealable_at` needs — and used to discard the answer, so
  # `holder_stealable_at/4` paid for a SECOND `lease_stealable_at/1` round trip to rediscover it on
  # every first held error. The backend now hands the instant back through
  # `{:error, {:held, owner, at}}` and the read is gone.
  #
  # Counted rather than timed: a latency assertion on one avoided round trip against a local file
  # backend would be noise. `Fathom.Test.FaultyStorage` counts the call in the CALLER's process
  # dictionary, which is where `held_retry/4` runs, and otherwise delegates to `Local` — so
  # behaviour is identical and only the observability differs.
  test "the crash-hold uses the instant the acquire already computed, with no second read",
       %{shard: shard} do
    Application.put_env(:fathom, :crash_failover_hold_ms, 5_000)
    Application.put_env(:fathom, :steal_margin_ms, 100)
    prev_backend = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    on_exit(fn -> restore(:shard_storage, prev_backend) end)

    seed_durable(shard)

    test_pid = self()
    handler = "noreread-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :crash_wait],
      fn _e, _m, meta, _ -> send(test_pid, {:crash_wait, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    Process.put(:faulty_stealable_at_reads, 0)

    put_foreign_lock(shard, "dead@node#3", 200)
    Storage.renew_heartbeat("dead@node#3", 200)

    assert {:ok, pid, ref, _path} = Shards.checkout(shard)

    # PRECONDITION: the hold path actually ran. Without it a checkout that never reached
    # `holder_stealable_at/4` at all would satisfy the read count for the wrong reason.
    assert_received {:crash_wait, %{shard_id: ^shard}},
                    "the crash-hold never fired, so the read count below proves nothing"

    assert Process.get(:faulty_stealable_at_reads) == 0,
           "holder_stealable_at/4 re-read lease_stealable_at even though acquire_lease handed " <>
             "back the instant — the round trip #23 removes is back"

    Fathom.Shard.checkin(pid, ref)
    drain_and_wait(shard)
  end

  # The other half of the same dispatch: two paths cannot compute an instant (S3's lost-create-race
  # `:exists`, where the winner's lock has not been read, and a `put_lock` held error), and there
  # the re-read is the honest answer rather than a fabricated deadline. Structural, because
  # producing an S3 lost-create-race against the Local backend is not something a unit test can do.
  test "a held error with no instant still falls back to asking the backend" do
    source = File.read!("lib/fathom/shards.ex")

    assert source =~ "defp holder_stealable_at(_shard_id, _owner, budget, at) when is_integer(at)",
           "the integer clause is gone; the instant the acquire supplies is being ignored again"

    assert source =~ "defp holder_stealable_at(shard_id, owner, budget, _nil) do",
           "the nil fallback is gone. Two backend paths cannot state an instant, and without " <>
             "this clause they would silently lose the crash-failover hold entirely."
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
