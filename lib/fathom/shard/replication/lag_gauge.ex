defmodule Fathom.Shard.Replication.LagGauge do
  @moduledoc """
  The `catching_up_shards` gauge for the 10 s telemetry poller (admin observability 2026-10-15 P2).

  Lives in its own module (not `Fathom.Admin.Measurements`) and only fans out when the Session
  registry exists, i.e. when replication is running. Uses `Fleet.lag_report/1` with a short
  per-session timeout, so a Session busy in a commit is skipped rather than stalling the poller.
  """
  alias Fathom.Shard.Replication.Fleet

  @doc "Poller entry point: emits `[:fathom, :replication, :lag]` with `catching_up_shards`."
  @spec measure() :: :ok
  def measure do
    if Fleet.replicating?() do
      %{catching_up: n} = Fleet.lag_report(timeout: 250)
      :telemetry.execute([:fathom, :replication, :lag], %{catching_up_shards: n}, %{})
    end

    :ok
  end
end
