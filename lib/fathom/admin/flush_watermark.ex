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
    # Bumped before the table exists, so a restart is detectable even though the table itself
    # cannot survive it (see `republish_open_shards/0`).
    gen = :persistent_term.get({__MODULE__, :generation}, 0) + 1
    :persistent_term.put({__MODULE__, :generation}, gen)

    # Public so the coordinator writes its own row and the collector reads them all off the
    # coordinator mailbox; single-writer-per-key (each coordinator owns its shard's row).
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    if gen > 1, do: republish_open_shards()

    {:ok, %{}}
  end

  # A RESTART USED TO REPORT THE FLEET AS FULLY FLUSHED (expert review 2026-08-26 #17).
  #
  # This table is owned by this GenServer and has no `heir`, so a crash + supervisor restart makes
  # ETS destroy it and `init/1` create a fresh empty one. `Measurements.do_durability/0` REDUCES
  # over `snapshot/0`, so an absent row contributes nothing — it emitted `dirty_shards: 0,
  # oldest_age_ms: 0`, i.e. "everything is flushed". And coordinators re-published only at open and
  # after a SUCCESSFUL flush, so a shard whose flushes are failing — the exact condition the gauge
  # exists to surface — never re-published and stayed invisible indefinitely.
  #
  # `FathomUnflushedAgeHigh` and `FathomManyDirtyShards` read that gauge, so one owner restart
  # silenced both precisely while a persistent storage failure grew the RPO unbounded.
  #
  # Fixed by mirroring `Fathom.Shard.WriteCounter`, which already faced this and solved it the same
  # way: detect the restart via a persistent_term generation and tell every open coordinator to
  # re-assert its row. Chosen over an ETS `heir` deliberately — an heir needs a cooperating owner
  # process to hold the table and hand it back through `{:"ETS-TRANSFER", ...}`, which is real
  # machinery for a table whose contents can simply be re-published by their owners in
  # milliseconds. The sibling module's precedent also keeps the two readable together.
  defp republish_open_shards do
    require Logger

    Logger.warning(
      "FlushWatermark restarted with a fresh table; asking open coordinators to re-publish " <>
        "their watermarks (until they do, the RPO gauge under-reports)"
    )

    Fathom.ShardRegistry
    |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(&send(&1, :republish_flush_watermark))
  rescue
    # The registry itself is down (a whole-plane restart, not just this process) — the
    # coordinators are dead too and there is nobody to notify.
    ArgumentError -> :ok
  end
end
