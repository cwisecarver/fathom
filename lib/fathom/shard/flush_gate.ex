defmodule Fathom.Shard.FlushGate do
  @moduledoc """
  Node-level cap on **concurrent durability-flush tasks** (expert review #17).

  Each dirty coordinator's periodic flush spawns a task — `VACUUM INTO` snapshot + full-object
  PUT — through the one shared Finch pool. A coordinator's flush timer is staggered only by its
  open time, so after a failover/LB flip re-homes a *burst* of shards within one interval their
  timers phase-align: every interval thereafter N snapshots + PUTs fire in lockstep, competing
  with cold-open pulls for the same pool on exactly the survivor that's absorbing traffic. There
  is at most one task *per shard* but nothing bounds tasks *per node*.

  This is a node-global in-flight counter checked before a coordinator spawns its flush task. Over
  the cap, the coordinator reschedules with a short backoff and the shard stays dirty — the safe
  direction, the flush just waits — so the node's concurrent snapshot/upload load is bounded no
  matter how many shards phase-align. It bounds concurrency; `schedule_flush`'s jitter separately
  decorrelates the timers so shards don't pile onto the gate in lockstep in the first place.

  A public ETS counter bumped lock-free (`:ets.update_counter`), no GenServer hop on the flush
  path. Gated by `:shard_flush_max_concurrency` (nil ⇒ unbounded, the default): when unset a
  coordinator never touches this table (`try_acquire/0` returns `:disabled`), so it is zero-cost
  off. The GenServer only owns the table; it is never called on the flush path.
  """
  use GenServer

  require Logger

  @table __MODULE__
  @counter :in_flight

  # Monotonic count of `:full` answers (expert review 2026-08-26 #16). The refusal rate was
  # invisible to EVERY existing metric: `[:fathom, :shard, :flush, :failed]` "only fires for a
  # flush that actually RAN" (see `sweep/0` below), and the `[:fathom, :shard, :flush_gate]` gauge
  # reports `in_flight`/`cap`, which say the gate is busy but not that anyone was turned away or
  # how often. One extra `update_counter` on a path that already does two, read by the periodic
  # gauge rather than emitted per refusal — at the density this finding is about, one telemetry
  # event per refusal would itself be the problem.
  @refusals :refusals

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The node-wide concurrent-flush cap. `nil` only if explicitly configured off.

  SHIPS BOUNDED (expert review 2026-08-01 #16). This used to default to `nil` — unbounded —
  so the cascade the moduledoc above describes had no active mitigation at all beyond the
  ±25% timer jitter, which decorrelates phase but not sustained rate. At the measured density
  and the default 5s interval that is thousands of full-object PUTs per second per node
  through one 200-connection Finch pool that also carries cold-open pulls, lease and heartbeat
  ops, and warm-follower revalidation — on exactly the survivor absorbing a failover.
  #
  The default is derived from the pool rather than fixed, so raising `pool_size` raises the
  cap with it: a quarter of the pool for bulk background writes, floored at the dirty-IO
  scheduler count (each flush's `VACUUM INTO` occupies one) and never below 4. Set
  `:shard_flush_max_concurrency` to an integer to pin it, or to `0`/`false` to restore the
  old unbounded behaviour.
  """
  def cap do
    case Application.get_env(:fathom, :shard_flush_max_concurrency, :default) do
      :default -> default_cap()
      n when is_integer(n) and n > 0 -> n
      _ -> nil
    end
  end

  defp default_cap do
    pool_size = get_in(Application.get_env(:fathom, Fathom.Shard.Storage.S3, []), [:pool_size])
    default_cap(pool_size || 200, System.schedulers_online())
  end

  @doc """
  The pure derivation behind `cap/0`: a quarter of the Finch pool, capped at the scheduler
  count (each in-flight flush's `VACUUM INTO` occupies one) and never below 4.

  Public and pure ONLY so it is testable on any machine. Driving it through
  `System.schedulers_online/0` is not: on a small-core box `schedulers` is the binding term
  at *every* pool size, so the cap is a flat 4 and "raising the pool raises the cap" is
  unobservable — not because the derivation is wrong, but because that machine has no band
  in which the pool is the binding term. That is exactly how `flush_storm_test`'s
  `assert big > small` passed on an 18-scheduler dev box and failed on every CI runner from
  `dc3d2a3` until this commit. Assert the derivation here, not through the live scheduler count.
  """
  @spec default_cap(pos_integer(), pos_integer()) :: pos_integer()
  def default_cap(pool_size, schedulers) do
    max(min(div(pool_size, 4), schedulers), 4)
  end

  @doc """
  Reserve a flush slot. Returns `:ok` (slot reserved — the caller MUST `release/0` when the flush
  settles), `:full` (at/over the cap — nothing reserved, back off and stay dirty), or `:disabled`
  (no cap configured — unbounded, nothing reserved, no release needed). Lock-free.
  """
  @spec try_acquire() :: :ok | :full | :disabled
  def try_acquire do
    case cap() do
      cap when is_integer(cap) and cap > 0 ->
        n = :ets.update_counter(@table, @counter, {2, 1}, {@counter, 0})

        if n <= cap do
          # Record WHO holds it, so a slot can be reclaimed from a process that died without
          # releasing (expert review 2026-08-20 #15). Still lock-free and still no GenServer hop:
          # one extra ETS insert on a `write_concurrency` table. See `sweep/0`.
          :ets.insert(@table, {{:holder, self()}, System.monotonic_time(:millisecond)})
          :ok
        else
          # Roll back our own increment (clamped at 0) — we didn't get a slot.
          :ets.update_counter(@table, @counter, {2, -1, 0, 0}, {@counter, 0})
          :ets.update_counter(@table, @refusals, {2, 1}, {@refusals, 0})
          :full
        end

      _ ->
        :disabled
    end
  rescue
    # The table only exists once this GenServer has started; a flush during boot (or a test that
    # didn't start it) must fail OPEN (unbounded) rather than crash the flush path.
    ArgumentError -> :disabled
  end

  @doc """
  Monotonic count of `:full` answers since this node booted (expert review 2026-08-26 #16).

  Read by the periodic gauge, not emitted per refusal — at the density the gate exists for
  (a failover re-homing a burst of shards) the refusal rate is the thing being measured, so
  emitting an event per refusal would be the same mistake in a different colour.
  """
  @spec refusals() :: non_neg_integer()
  def refusals do
    case :ets.lookup(@table, @refusals) do
      [{@refusals, n}] -> n
      [] -> 0
    end
  rescue
    ArgumentError -> 0
  end

  @doc """
  How long a refused shard should wait before probing again — derived from the POPULATION that
  could be probing, not a fixed 250 ms (expert review 2026-08-26 #16).

  The fixed backoff turned a node-wide flush backlog into a node-wide busy-poll at exactly the
  moment the node was I/O-saturated absorbing a failover. D dirty shards against cap C is a
  service rate of C but a PROBE rate of `D / 0.25 s`: at 30 000 shards that is ~120 000 gate probes
  per second, each two `:ets.update_counter` ops on ONE key, plus 120 000 timer insertions and
  120 000 process wakeups. And it is reachable without a failover — `WriteCounter.init/1` marks
  every open coordinator dirty at once on an ETS-owner restart, which is a documented, expected
  event.

  Same shape as `Fathom.Shard.lapse_spread_ms/1` and for the same reason: the spread has to come
  from the size of the herd. `open_shards / target` seconds, floored at the fixed backoff (so
  small fleets are completely unchanged) and capped at one flush interval — which is the audit's
  own "demote the timer to a safety net at a much longer period". At 30 000 shards the cap binds
  and the achieved probe rate is `30 000 / 5 s` = 6 000/s, a 20x reduction.

  PURE in its inputs so it is testable without racing the registry.
  """
  @spec backoff_ms(non_neg_integer(), pos_integer(), pos_integer(), pos_integer()) ::
          pos_integer()
  def backoff_ms(open_shards, target_probes_per_sec, base_ms, flush_interval_ms) do
    (open_shards * 1000 / target_probes_per_sec)
    |> round()
    |> max(base_ms)
    |> min(max(flush_interval_ms, base_ms))
  end

  @doc "Release a slot reserved by `try_acquire/0`. Clamped at 0 so a stray release can't underflow."
  @spec release() :: :ok
  def release do
    :ets.delete(@table, {:holder, self()})
    :ets.update_counter(@table, @counter, {2, -1, 0, 0}, {@counter, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Reclaim slots held by processes that are no longer alive. Returns how many it freed.

  THE LEAK THIS EXISTS FOR (expert review 2026-08-20 #15). The counter is node-global and outlives
  any coordinator, while `release/0` was reachable only from coordinator callbacks — so a
  coordinator brutally killed mid-flush (shutdown-budget expiry, `DynamicSupervisor.terminate_child`
  from `Shards.stop/1`), or one that raised between `try_acquire/0` and recording
  `flush_slot_held:` in its state, leaked a slot permanently.

  The default cap is `max(min(pool_size / 4, schedulers), 4)` — 8 on an 8-core box, 4 at the floor.
  Leaking that few makes `try_acquire/0` answer `:full` FOREVER, so every dirty shard on the node
  reschedules at the 250 ms backoff and never flushes again. The RPO goes unbounded with no
  telemetry: `[:fathom, :shard, :flush, :failed]` only fires for a flush that actually RAN.

  Runs on a timer rather than via monitors so the acquire path keeps its "no GenServer hop"
  property — a leak is rare, and correcting it within one sweep interval is enough.
  """
  @spec sweep() :: non_neg_integer()
  def sweep do
    @table
    |> :ets.match({{:holder, :"$1"}, :_})
    |> List.flatten()
    |> Enum.reject(&Process.alive?/1)
    |> Enum.reduce(0, fn dead, freed ->
      # `:ets.take/2` so two concurrent sweeps cannot both free the same slot.
      case :ets.take(@table, {:holder, dead}) do
        [] ->
          freed

        [_ | _] ->
          :ets.update_counter(@table, @counter, {2, -1, 0, 0}, {@counter, 0})
          freed + 1
      end
    end)
  rescue
    ArgumentError -> 0
  end

  @doc "Current in-flight flush count (for tests / observability)."
  @spec in_flight() :: non_neg_integer()
  def in_flight do
    case :ets.lookup(@table, @counter) do
      [{@counter, n}] -> n
      _ -> 0
    end
  rescue
    ArgumentError -> 0
  end

  @doc false
  def reset do
    :ets.match_delete(@table, {{:holder, :_}, :_})
    :ets.insert(@table, {@counter, 0})
  end

  @sweep_interval_ms 30_000

  @impl true
  def handle_info(:sweep, state) do
    case sweep() do
      0 ->
        :ok

      freed ->
        Logger.warning(
          "flush gate reclaimed #{freed} slot(s) from dead holders — a coordinator was killed " <>
            "mid-flush. Left unreclaimed these accumulate and eventually refuse EVERY flush on " <>
            "this node, which makes the RPO unbounded with no other signal."
        )

        :telemetry.execute([:fathom, :shard, :flush_gate, :reclaimed], %{count: freed}, %{})
    end

    schedule_sweep()
    {:noreply, state}
  end

  defp schedule_sweep, do: Process.send_after(self(), :sweep, @sweep_interval_ms)

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    :ets.insert(@table, {@counter, 0})
    schedule_sweep()
    {:ok, %{}}
  end
end
