defmodule Fathom.Admin.FlushWatermark do
  @moduledoc """
  Node-local publication of each open shard's flush watermark, so an observer (the admin
  dashboard's `Fathom.Admin.MetricsCollector`, or a periodic RPO gauge) can compute
  per-shard dirtiness and the live **RPO** (recovery-point) age *without* a `GenServer` call
  into every coordinator.

  ## Why publish the watermark, not a `dirty_since`

  The `Fathom.Shard` coordinator can't observe a write — writes bump `Fathom.Shard.WriteCounter`
  lock-free from the Hrana stream process. Dirtiness is a watermark comparison the coordinator
  evaluates on flush ticks (`Shard.unflushed?/1`): `counter_gen != WriteCounter.generation() or
  WriteCounter.count(id) > flushed_through`. Stamping a `dirty_since` and deleting it on flush
  would (1) race a write landing *during* the snapshot/upload (which bumps the counter past the
  flushed watermark, so the shard is still dirty) and (2) miss the `WriteCounter`-table-reset
  case. So the coordinator instead publishes its watermark here — at open and after every
  successful flush — and readers derive dirtiness with the *same* expression `unflushed?/1` uses,
  race-free, for free.

  ## Row + reader math

  Row: `{shard_id, flushed_through, counter_gen, flushed_at_mono_ms}`. A reader computes:

    * `dirty? = counter_gen != WriteCounter.generation() or WriteCounter.count(id) > flushed_through`
    * `rpo_age_ms = now_mono_ms - flushed_at_mono_ms` — an **upper bound** on the oldest unflushed
      write's age (any unflushed write happened after the last successful flush). A write landing
      during a flush resets `flushed_at` on that flush's success, so its residual age is
      under-estimated by at most one flush interval — the same window the durability design already
      accepts as its loss bound.

  Like `Fathom.ShardLoad` / `Fathom.Shard.WriteCounter`: a public ETS table with an
  always-supervised owner (so reads never crash), writes gated by `Fathom.Admin.enabled?/0`
  (the coordinator pays nothing when the dashboard/metrics layer is off) and a no-op before the
  table is up (rescue). `forget/1` on coordinator terminate keeps a stopped shard from leaking a row.
  """
  use GenServer

  @table __MODULE__

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Publishes `shard_id`'s flush watermark (called at open and after each successful flush).
  A no-op when the metrics layer is disabled or before the table is up.
  """
  @spec record(String.t(), non_neg_integer(), integer()) :: :ok
  def record(shard_id, flushed_through, counter_gen) do
    if Fathom.Admin.enabled?() do
      :ets.insert(
        @table,
        {shard_id, flushed_through, counter_gen, System.monotonic_time(:millisecond)}
      )
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Drops `shard_id`'s row — called from `Fathom.Shard.terminate` so stopped shards don't leak."
  @spec forget(String.t()) :: :ok
  def forget(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Every published watermark on this node (unordered)."
  @spec snapshot() :: [{String.t(), non_neg_integer(), integer(), integer()}]
  def snapshot do
    :ets.tab2list(@table)
  rescue
    ArgumentError -> []
  end

  @doc "Clears all rows (test/ops helper)."
  @spec reset() :: :ok
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    # Public so the coordinator writes its own row and the collector reads them all off the
    # coordinator mailbox; single-writer-per-key (each coordinator owns its shard's row).
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    {:ok, %{}}
  end
end
