defmodule Fathom.Admin.Measurements do
  @moduledoc """
  Periodic gauge measurements for the metrics layer, driven by the `Fathom.Telemetry` poller.
  Each emits a `:telemetry` event the Prometheus reporter turns into a gauge (and the dashboard's
  `Fathom.Admin.MetricsCollector` reads the same values). Cheap, node-local, off the hot path.

  Storage footprint (`Fathom.Shard.Storage.stored_usage/0`) is deliberately **not** here — it can
  be a full S3 LIST, so the collector polls it on a slow, separate cadence and caches it.
  """
  alias Fathom.Admin.FlushWatermark
  alias Fathom.Shard.WriteCounter

  @doc "BEAM memory gauge for this node."
  @spec node_memory() :: :ok
  def node_memory do
    :telemetry.execute([:fathom, :node, :memory], %{total: :erlang.memory(:total)}, %{})
  end

  @doc """
  Durability / RPO gauge: how many open shards hold un-flushed writes, and the oldest such
  shard's RPO age (ms). Derives dirtiness from the published flush watermark exactly as
  `Fathom.Shard.unflushed?/1` does — a `WriteCounter` generation mismatch or a write count past
  the flushed watermark — so it never disagrees with the coordinator's own dirty decision.
  """
  @spec durability() :: :ok
  def durability do
    now = System.monotonic_time(:millisecond)
    gen = WriteCounter.generation()

    {dirty, oldest} =
      Enum.reduce(FlushWatermark.snapshot(), {0, 0}, fn
        {id, flushed_through, counter_gen, flushed_at}, {dirty, oldest} ->
          if counter_gen != gen or WriteCounter.count(id) > flushed_through do
            {dirty + 1, max(oldest, now - flushed_at)}
          else
            {dirty, oldest}
          end
      end)

    :telemetry.execute(
      [:fathom, :durability, :rpo],
      %{dirty_shards: dirty, oldest_age_ms: oldest},
      %{}
    )
  end
end
