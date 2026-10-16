defmodule Fathom.Admin.LeaseHealth do
  @moduledoc """
  Node lease-health gauges for the metrics layer and the admin Cluster page (expert review
  2026-10-15 #9). Everything here is a lock-free read of the heartbeat status row and the write-fence
  ETS table: no GenServer call, no per-shard walk, safe to call from a render or a poller.

  `snapshot/0` is the page's view; `measure/0` is the `Fathom.Telemetry` poller entry that turns the
  same values into node-level gauges (`fathom.node.lease.*`).
  """
  alias Fathom.Shard.{Heartbeat, WriteFence}

  @type snapshot :: %{
          mode: :heartbeat | :legacy,
          generation: non_neg_integer() | nil,
          ms_until_deadline: integer() | nil,
          margin_ms: pos_integer(),
          skew_ms: non_neg_integer(),
          lapsed: boolean() | nil,
          write_fenced_shards: non_neg_integer()
        }

  @doc "This node's lease health: the heartbeat status row plus the count of write-fenced shards."
  @spec snapshot() :: snapshot()
  def snapshot do
    Map.put(Heartbeat.status_snapshot(), :write_fenced_shards, WriteFence.count())
  end

  @doc "Poller entry: emits `[:fathom, :node, :lease]` (heartbeat row) and `[:fathom, :node, :write_fenced]`."
  @spec measure() :: :ok
  def measure do
    s = snapshot()

    :telemetry.execute(
      [:fathom, :node, :lease],
      %{
        heartbeat_running: if(s.mode == :heartbeat, do: 1, else: 0),
        lapsed: if(s.lapsed, do: 1, else: 0),
        margin_ms: s.margin_ms,
        skew_ms: s.skew_ms,
        generation: s.generation || 0
      }
      |> maybe_put(:ms_until_deadline, s.ms_until_deadline),
      %{}
    )

    :telemetry.execute([:fathom, :node, :write_fenced], %{shards: s.write_fenced_shards}, %{})
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
