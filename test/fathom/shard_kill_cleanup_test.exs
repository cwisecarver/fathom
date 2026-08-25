defmodule Fathom.ShardKillCleanupTest do
  @moduledoc """
  WHAT A COORDINATOR THAT SKIPS `terminate/2` LEAVES BEHIND (expert review 2026-08-20 #39).

  `shard_lease_release_test.exs` does this thoroughly for the **lease**, across both liveness
  modes. But four other NODE-GLOBAL resources are released only from `Fathom.Shard.terminate/2` —
  `WriteFence.forget`, `FlushGate.release` via `release_flush_slot`, `Lru.forget`, and
  `ShardLoad`/`ShardLatency.forget` — and nothing tested the path where terminate does not run.

  That is why #6, #15 and #29 all survived. Each of them is "acquired by a coordinator, released
  only from terminate", and each has a different blast radius:

    * `WriteFence` — a PERMANENT per-tenant write outage (503 on every write), recoverable only by
      restarting the node.
    * `FlushGate` — a NODE-WIDE permanent flush stall: the cap is single digits, so a handful of
      leaked slots means every dirty shard on the node stops flushing.
    * `Lru` — a node-wide capacity degradation: a ghost row spends one of 16 eviction probe slots
      forever, so ~16 of them turn the soft cap into a hard cap.

  `Process.exit(coordinator, :kill)` appears in seven other test files, all asserting on
  lease/durability/promote outcomes and **none on node-global cleanup**. `heartbeat_fence_test.exs`
  proves fence→unfence within one coordinator's lifetime; it never kills the coordinator and never
  re-opens the shard, so #6 was structurally invisible to `mix test`.

  ## The three acceptable outcomes, and why each counts

  A kill is not a graceful stop: nothing runs on the way out, by definition. So the invariant is
  not "the resource is released at death" — it is **"the node does not degrade"**. Either the
  resource is not held past death, a successor's open cleans it up, or the leak is bounded and
  measured and we have decided to accept it. Each test below says which of the three it asserts.

  The third outcome is `ShardLoad`/`ShardLatency`, and it was missing here (expert review
  2026-08-24 #28): this moduledoc named four resources and tested three, which reads as coverage
  and was not. That fourth one has no cleanup mechanism at all — no sweep, no successor clear, no
  liveness check on read — and the last test in this file now pins that, along with the reasoning
  for accepting it rather than leaving the gap to be re-found.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.FlushGate
  alias Fathom.Shard.WriteFence
  alias Fathom.Shards
  alias Fathom.Shards.Lru

  defp uniq, do: "killclean_#{System.unique_integer([:positive])}"

  # Open a coordinator and hard-kill it, returning once it is really gone. `:kill` cannot be
  # trapped, so `terminate/2` provably does not run — which is the whole point.
  defp open_and_kill!(id) do
    {:ok, pid, ref, path} = Shards.checkout(id)
    Fathom.Shard.checkin(pid, ref)
    _ = :sys.get_state(pid)

    mon = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^mon, :process, ^pid, :killed}, 2_000

    # AND WAIT FOR THE REGISTRY TO NOTICE. `Registry` unregisters by monitoring, so our DOWN and
    # its DOWN are two independent messages — the entry can still resolve for a moment after the
    # process is gone. Idle, that window is microseconds; under a full-suite load it is long
    # enough that `Lru`'s liveness check reads the shard as still open and the ghost assertion
    # below fails. (Went red on CI at OTP 28 while 27 and 29 passed, which is the signature of a
    # timing assumption rather than a real difference.)
    #
    # Harmless in production for the same reason it is invisible here: the walk simply spends one
    # probe slot and the next walk cleans the row.
    deadline = System.monotonic_time(:millisecond) + 2_000

    Stream.repeatedly(fn ->
      if Registry.lookup(Fathom.ShardRegistry, id) == [],
        do: :gone,
        else: Process.sleep(5)
    end)
    |> Enum.find(fn
      :gone -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :gone -> :ok
      _ -> flunk("the registry still resolves #{id} after its coordinator died")
    end

    path
  end

  setup do
    id = uniq()

    on_exit(fn ->
      Shards.drain(id, 2_000)
      WriteFence.unfence(id)
      for e <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> e)
    end)

    %{id: id}
  end

  # #6. A published WriteFence row outlives its coordinator and can never be cleared: `unfence/1`
  # is called only from `terminate/2`, and a killed coordinator never runs it. Every subsequent
  # write to that tenant 503s, forever, on a node that is otherwise healthy.
  #
  # CLEANED BY THE SUCCESSOR, not at death — `open_with_lease/8` lifts the fence unconditionally on
  # open, because the new coordinator has just proven ownership and a fence describes a doubt the
  # PREVIOUS one had.
  test "a fenced shard whose coordinator is killed is writable again after the next open", %{
    id: id
  } do
    _ = open_and_kill!(id)

    WriteFence.fence(id)
    assert WriteFence.fenced?(id), "the fixture did not actually fence the shard"

    {:ok, pid, ref, _path} = Shards.checkout(id)

    refute WriteFence.fenced?(id),
           "the write fence survived its coordinator's death and the next open did not lift it. " <>
             "Nothing else clears it, so this tenant is 503 on every write until the node " <>
             "restarts — a permanent per-tenant outage with no operator remedy."

    Fathom.Shard.checkin(pid, ref)
  end

  # #29. An Lru row is dropped only in `terminate/2`. A ghost's stamp still matches and it is not
  # busy, so the bounded eviction walk returned it as a candidate and spent one of its 16 probe
  # slots on it — permanently, because a ghost's stamp never advances and it sits at the cold end
  # where the walk starts.
  #
  # NOT HELD PAST DEATH: the walk itself now validates liveness and forgets the row.
  test "an LRU row for a killed coordinator does not survive the next eviction walk", %{id: id} do
    prev_cap = Application.get_env(:fathom, :max_open_shards)
    on_exit(fn -> restore(:max_open_shards, prev_cap) end)
    Application.put_env(:fathom, :max_open_shards, 10_000)

    Lru.reset()
    _ = open_and_kill!(id)

    # PRECONDITION: the row really is there. Asserted rather than assumed, because a fixture that
    # never recorded it would make the assertion below vacuous.
    assert id in Lru.lru_order(100, fn _ -> true end),
           "the fixture never stamped an Lru row for this shard"

    # Asserted on THIS SHARD, not on the list being empty. A coordinator another test left alive is
    # a legitimate eviction candidate and has nothing to do with this invariant — the empty-list
    # form went red on CI (OTP 28, 2026-08-22) for exactly that reason while OTP 27 and 29 passed,
    # which is the signature of a shared-state assumption rather than a real difference.
    refute id in Lru.lru_order(100),
           "a dead coordinator's Lru row was returned as an eviction candidate. It costs one of " <>
             "16 probe slots forever, so ~16 of them make a full node refuse every novel open " <>
             "while idle, evictable shards sit just past the probe window."
  end

  # #15. A flush slot is reserved before the state that owns its release exists, and `release/0` is
  # reached only through the coordinator. The cap is single digits, so a handful of leaked slots
  # answers `:full` forever and every dirty shard on the node stops flushing.
  #
  # CLEANED BY THE SWEEP: `FlushGate.sweep/0` reclaims a slot whose holder is dead. Asserted by
  # calling it directly rather than waiting out the sweep interval — the timer firing is not the
  # property under test, reclaiming from a dead holder is.
  test "a flush slot held by a dead process is reclaimed", %{id: _id} do
    prev = Application.get_env(:fathom, :shard_flush_max_concurrency)
    on_exit(fn -> restore(:shard_flush_max_concurrency, prev) end)

    FlushGate.reset()
    Application.put_env(:fathom, :shard_flush_max_concurrency, 1)

    # A holder that dies without releasing — exactly what a killed coordinator is. The acquire has
    # to happen INSIDE the doomed process, because `try_acquire/0` records the CALLER as the
    # holder; the handshake keeps it synchronous so nothing here waits on a timer.
    parent = self()

    holder =
      spawn(fn ->
        send(parent, {:acquired, FlushGate.try_acquire()})
        receive do: (:never -> :ok)
      end)

    assert_receive {:acquired, :ok}, 2_000
    mon = Process.monitor(holder)
    Process.exit(holder, :kill)
    assert_receive {:DOWN, ^mon, :process, ^holder, :killed}, 2_000

    assert FlushGate.in_flight() >= 1, "the fixture did not leave a slot held"

    FlushGate.sweep()

    assert FlushGate.in_flight() == 0,
           "a flush slot held by a dead process was never reclaimed. The cap is single digits, " <>
             "so a handful of these answers :full forever and every dirty shard on the node " <>
             "stops flushing — a node-wide, permanent, silent durability stall."

    assert :ok = FlushGate.try_acquire()
    FlushGate.release()
  end

  # THE FOURTH RESOURCE, AND THE ONE THAT IS GENUINELY NOT CLEANED (expert review 2026-08-24 #28).
  #
  # The moduledoc above named four node-global resources released only from `terminate/2` and then
  # tested three. This is the fourth, and unlike the others it has no cleanup mechanism at all:
  # `WriteFence` is lifted by the successor's `open_with_lease/8`, `FlushGate` has `sweep/0`, `Lru`
  # has the `ghost?/2` liveness check in its walk — `ShardLoad`/`ShardLatency` have `forget/1`
  # called from four sites, all inside `terminate/2`, with no sweep, no successor clear, and no
  # liveness validation on read.
  #
  # THIS TEST PINS THE STATUS QUO, DELIBERATELY. The row survives the kill, and a later graceful
  # stop of the same shard clears it. That is accepted rather than fixed, and the reasoning is
  # recorded here so it stays a decision instead of drifting back into an oversight:
  #
  #   * The blast radius is genuinely small. A stale row is a 5-tuple in `ShardLoad` and a
  #     15-bucket histogram row in `ShardLatency`, bounded by the shards this node has ever opened.
  #   * The consumer that matters is unaffected. `Rebalancer.Reporter` DIFFS two snapshots, so a
  #     frozen row contributes a rate of zero. What degrades is `snapshot/0` / `top/2` — the admin
  #     "which shards are hot" page listing a dead shard — and cumulative counters resuming from a
  #     previous incarnation when the shard re-opens.
  #   * Both available fixes cost more than the defect. A liveness filter on read would put a
  #     `Registry.lookup` per row into `snapshot_tuples/0`, which the Reporter calls per window at
  #     up to 30k shards a node. Clearing on OPEN (the `WriteFence` pattern) would fix the stale
  #     counters too, but it resets the Reporter's baseline on every idle re-open, which is a
  #     change to what the rebalancer measures — not something to do for a cosmetic admin row.
  #   * `:shard_load` is off by default, so on a default fleet none of this is even recorded.
  #
  # If that trade stops holding — the admin page becomes load-bearing, or the counters get a
  # non-rate consumer — clearing on open is the fix, and it belongs next to the `WriteFence.unfence`
  # call in `open_with_lease/8`.
  test "a killed coordinator's ShardLoad row survives, and is cleared by the next graceful stop",
       %{id: id} do
    prev = Application.get_env(:fathom, :shard_load)
    on_exit(fn -> restore(:shard_load, prev) end)
    Application.put_env(:fathom, :shard_load, true)

    Fathom.ShardLoad.reset()
    _ = open_and_kill!(id)

    # PRECONDITION: the counters were actually recorded, or everything below is vacuous.
    assert Fathom.ShardLoad.get(id),
           "the fixture never recorded a ShardLoad row for this shard"

    # The documented status quo: nothing reclaims it at death.
    assert Fathom.ShardLoad.get(id),
           "a ShardLoad row for a dead coordinator was reclaimed — if something now cleans this " <>
             "up, the trade recorded above no longer applies and this test should assert THAT"

    # …and a graceful stop of the same shard does clear it, which is what bounds the leak.
    {:ok, pid, ref, _path} = Shards.checkout(id)
    Fathom.Shard.checkin(pid, ref)
    :ok = Shards.drain(id, 5_000)

    refute Fathom.ShardLoad.get(id),
           "a graceful stop must clear the row — it is the only thing that bounds the leak"
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)
end
