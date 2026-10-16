defmodule Fathom.Rebalancer.NodeHealth do
  @moduledoc """
  The per-node health snapshot the reporter publishes on its beat (`rebalancer_nodes.health`,
  jsonb) so the admin Nodes table can be the one fleet triage table (expert review 2026-10-15 #7).

  Every value is read from a source that already exists and is already cheap — the collector's
  last computed `current`, the Postgres-free replication connection status, a persistent_term, the
  heartbeat's status row — so publishing adds no per-request work. A source that is down yields
  `nil` for its field, never a crash, and never costs the beat.

  Keys are **strings** (the map round-trips through jsonb). `nil` means "unknown / not applicable":
  `replication_slack` is nil when replication is off, `disk_used_ratio` before the first poll.
  A whole-`nil` `health` is a node on an older release; readers treat it as unknown.
  """

  alias Fathom.Admin.MetricsCollector

  @doc "This node's current health snapshot (string-keyed, JSON-safe)."
  @spec snapshot() :: map()
  def snapshot do
    cur = current()
    adm = Map.get(cur, :admission) || %{}
    disks = get_in(cur, [:node_health, :disks]) || %{}

    %{
      "dirty_shards" => Map.get(cur, :dirty_shards),
      "oldest_unflushed_ms" => Map.get(cur, :oldest_rpo_ms),
      "open_shards" => Map.get(cur, :open_shards),
      "max_open_shards" => Map.get(adm, :cap),
      "disk_used_ratio" => get_in(disks, ["data", :used_ratio]),
      "replication_slack" => replication_slack(),
      "query_p99_ms" => Map.get(cur, :query_p99_ms),
      "draining" => Fathom.HealthPlug.draining?(),
      "skew_ms" => skew_ms()
    }
    |> Map.new(fn {k, v} -> {k, jsonable(v)} end)
  end

  @doc "Reads one field from a (possibly nil / old-release) published snapshot; nil = unknown."
  @spec get(map() | nil, String.t()) :: term()
  def get(%{} = health, key), do: Map.get(health, key)
  def get(_, _), do: nil

  defp current do
    case MetricsCollector.snapshot() do
      %{current: %{} = c} -> c
      _ -> %{}
    end
  catch
    _, _ -> %{}
  end

  # connected - quorum, exactly `Measurements.replication/0`'s figure; nil when A2 is off.
  defp replication_slack do
    if Fathom.Shard.Replication.Session.enabled?() do
      status = Fathom.Shard.Replication.Fleet.connection_status()
      quorum = Application.get_env(:fathom, :replication_quorum, 2)
      Enum.count(status, fn {_key, up?} -> up? end) - quorum
    end
  catch
    _, _ -> nil
  end

  defp skew_ms do
    Fathom.Shard.Heartbeat.measured_skew_ms()
  catch
    _, _ -> nil
  end

  # A non-JSON value would fail the whole beat insert; degrade it to unknown instead.
  defp jsonable(v) when is_number(v) or is_boolean(v) or is_nil(v), do: v
  defp jsonable(_), do: nil
end
