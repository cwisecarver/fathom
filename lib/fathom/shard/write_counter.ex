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

    {:ok, %{}}
  end
end
