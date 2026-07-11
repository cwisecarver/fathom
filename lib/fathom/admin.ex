defmodule Fathom.Admin do
  @moduledoc """
  Shared gate for the admin observability layer (Prometheus metrics + the realtime
  dashboard's `Fathom.Admin.MetricsCollector`).

  `enabled?/0` governs the node-local instrumentation only the dashboard / metrics layer
  consumes — the per-shard flush-watermark publication (`Fathom.Admin.FlushWatermark`), the
  Prometheus Core reporter, and the collector itself. Mirrors `Fathom.ShardLoad.enabled?/0`:
  the config is `:metrics_collector`, **default on**, and off in test (so the suite pays
  nothing and doesn't stand up a metrics singleton). The hot-path `:telemetry` events
  (`[:fathom, :shard, :query]`, `[:fathom, :s3, :op]`) are emitted regardless — with no
  reporter attached a `:telemetry.execute` with zero handlers is effectively free.
  """
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :metrics_collector, true) != false
end
