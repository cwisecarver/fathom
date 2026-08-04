defmodule Fathom.Shard.WriteCounter do
  @moduledoc """
  A monotonic per-shard **write counter** in public ETS — the dirty-flag signal moved off the
  coordinator mailbox (finding #27).

  ## Why ETS, not a cast per write

  `Fathom.ShardExecutor` runs in the Hrana **stream** process. Marking the shard dirty by sending
  the `Fathom.Shard` coordinator a message **per write statement** turns the coordinator into a
  mailbox bottleneck — the exact trap `Fathom.ShardLoad`'s moduledoc warns against. Instead each
  write does a **lock-free `:ets.update_counter`** from the executing process (`write_concurrency`,
  different shards hit different key locks — no process hop, no cross-shard contention).

  ## Dirtiness = a watermark comparison, not a boolean

  The coordinator keeps a private `flushed_through` high-water mark; the shard is dirty iff
  `count(id) > flushed_through`. A successful flush captures the count **before** snapshotting and
  advances the watermark to it; a write landing during the (blocking) snapshot/upload bumps the
  counter past that value, so it stays dirty and re-flushes next interval. That makes "clear on a
  successful flush, keep dirty on a mid-flush write or a failed upload" fall out for free —
  race-free, with **no ETS clear and no CAS** (findings #1/#2). The counter is append-only; the
  only mutation besides `bump/1` is `forget/1` on coordinator terminate (so a stopped shard's row
  doesn't leak — mirrors `ShardLoad`).

  ## Always on

  Unlike `ShardLoad` (optional telemetry, gated off), this is a **data-loss-prevention invariant**,
  so the table is unconditional. An idle owner + empty table is free. Its owner is supervised
  **before** the shard `DynamicSupervisor` so the table is up before any coordinator seeds or bumps.
  """
  use GenServer

  @table __MODULE__
  # Row layout: {shard_id, count}. Position 1 is the key; 2 is the counter.
  @pos_count 2
  @empty {nil, 0}

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Bumps `shard_id`'s write counter. Lock-free ETS increment from the executing process; a no-op
  before the table is up (boot/teardown — a dropped bump can't lose data, the next write re-dirties).
  """
  @spec bump(String.t()) :: :ok
  def bump(shard_id) do
    :ets.update_counter(@table, shard_id, {@pos_count, 1}, default_row(shard_id))
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Like `bump/1` but RAISES when the table is absent, instead of rescuing to `:ok`.

  `bump/1`'s rescue is justified by its moduledoc — "a dropped bump can't lose data, the next
  write re-dirties" — and that is true for a per-write bump. It is **false for the dirtiness
  SEED** (expert review 2026-08-01 #39): `Fathom.Shard.init_flushed_through/2` bumps a warm
  shard one past its watermark to mark it dirty, and there is no "next write" to re-dirty it.
  A rescued no-op there leaves a warm shard with un-flushed local writes reading CLEAN, so idle
  or shutdown takes `drop_clean/1` and deletes them without uploading.

  The window is real, not hypothetical: `WriteCounter.init/1` bumps the persistent_term
  generation BEFORE creating the table (deliberately, per audit 2026-07-18 #7), so a coordinator
  opening a warm shard in that gap reads the new generation, `count/1` → 0 rescued, `bump/1` →
  no-op rescued. Callers that need "unknown ⇒ dirty" must use this and fail closed.
  """
  @spec bump_strict(String.t()) :: :ok
  def bump_strict(shard_id) do
    :ets.update_counter(@table, shard_id, {@pos_count, 1}, default_row(shard_id))
    :ok
  end

  @doc "The current write count for `shard_id` (0 if it has none / the table isn't up)."
  @spec count(String.t()) :: non_neg_integer()
  def count(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{_, n}] -> n
      [] -> 0
    end
  rescue
    ArgumentError -> 0
  end

  @doc """
  The counter-table generation: bumped every time the owner (re)starts with a fresh empty
  table. Coordinators anchor their `flushed_through` watermark to the generation it was
  captured under (expert review #11); a mismatch means the watermark refers to a dead
  table whose counts are gone — unknown state, so the shard must read dirty. `-1` before
  the first boot orders below any real generation.
  """
  @spec generation() :: integer()
  def generation, do: :persistent_term.get({__MODULE__, :generation}, -1)

  @doc "Drops `shard_id`'s row — called from `Fathom.Shard.terminate` so stopped shards don't leak."
  @spec forget(String.t()) :: :ok
  def forget(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Clears all counters (test/ops helper)."
  @spec reset() :: :ok
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp default_row(shard_id), do: put_elem(@empty, 0, shard_id)

  @impl true
  def init(_opts) do
    # Bump the generation BEFORE creating the table (expert review 2026-07-18 #7). Expert review #14
    # (below) closes the case where a restart hands every open coordinator a FRESH EMPTY table —
    # count(id) = 0 for all shards — while each keeps its old flushed_through watermark, so
    # `count > flushed_through` classified every dirty shard as clean and drop_clean deleted
    # un-flushed writes. The generation guard (unflushed?/1) detects that reset — but only if the
    # generation is already bumped when the fresh table becomes observable. Creating the table first
    # left a window where the empty table exists under the OLD generation, so the guard passed and a
    # dirty shard still read clean. Bumping first means any observation of a fresh/absent table also
    # sees the new generation ⇒ treated dirty (the safe direction). (persistent_term generation, the
    # Heartbeat #8 pattern; force_dirty_open_shards proactively notifies registered coordinators.)
    gen = :persistent_term.get({__MODULE__, :generation}, -1) + 1
    :persistent_term.put({__MODULE__, :generation}, gen)

    # public + write_concurrency: many stream processes bump concurrently, each on its own shard
    # key; the coordinator reads its own shard's count off the hot path. decentralized_counters
    # keeps concurrent update_counter writes off a shared counter cache line.
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      decentralized_counters: true,
      read_concurrency: true
    ])

    if gen > 0, do: force_dirty_open_shards()

    {:ok, %{}}
  end

  # Best-effort: if the registry itself is down (a whole-plane restart, not just this
  # process), the coordinators are dead too and there is nobody to notify.
  defp force_dirty_open_shards do
    require Logger

    Logger.warning(
      "WriteCounter restarted with a fresh table; forcing open coordinators dirty (unknown ⇒ flush)"
    )

    Fathom.ShardRegistry
    |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(&send(&1, :write_counter_reset))
  rescue
    ArgumentError -> :ok
  end
end
