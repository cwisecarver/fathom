defmodule Fathom.Rebalancer do
  @moduledoc """
  Phase-2 B1 — dynamic rebalancing (moving a persistently-hot shard off an overloaded
  node by layering a per-subdomain exception on the LB's consistent hash).

  This module holds the shared node-identity + LB-topology config the rebalancer keys on;
  the moving parts live in submodules: `Reporter`/`LoadSamples` (per-node load →
  Postgres), `Policy` (hot detection + target selection), `Overrides`/`LbMap` (the
  exception table + nginx render), `CommandPoller` (per-node warm/drain), and the Oban
  `RebalanceJob`/`HandoffJob` orchestration.

  See `docs/phase2-scoping.md` §B1.
  """

  @doc """
  This node's stable rebalancer key — the LB backend it is addressed as. Unlike the S3
  lease owner (`Fathom.Shard.Heartbeat.owner/0`, which carries a per-boot nonce), this is
  stable across restarts so the exception table and the LB backend set can reference it.
  `:node_key` / env `NODE_KEY`, default `node()`.
  """
  @spec node_key() :: String.t()
  def node_key, do: to_string(Application.get_env(:fathom, :node_key) || node())

  @doc """
  The LB backend set as `%{node_key => upstream_address}` (e.g. `%{"fathom1" =>
  "fathom1:8080"}`) — how the LB names each node. `:lb_backends`, default empty. The
  policy picks targets from its keys; the map renderer emits a pin-upstream per entry.
  """
  @spec lb_backends() :: %{optional(String.t()) => String.t()}
  def lb_backends, do: Application.get_env(:fathom, :lb_backends, %{})

  @doc """
  Parses `REBALANCE_HOT_QPS_FLOOR` into a positive float (finding #16). Accepts an integer
  or float string (`"500"` or `"500.0"` — the old `String.to_float/1` boot-crashed on the
  integer form) and **raises at boot** on an unusable value (non-numeric, ≤ 0, or trailing
  junk) rather than silently degrading to the p99-relative path — a mis-set floor was a
  silent no-op otherwise. Used by `config/runtime.exs`.
  """
  @spec parse_hot_qps_floor!(String.t()) :: float()
  def parse_hot_qps_floor!(raw) do
    case raw |> to_string() |> String.trim() |> Float.parse() do
      {floor, ""} when floor > 0 ->
        floor

      _ ->
        raise ArgumentError,
              "REBALANCE_HOT_QPS_FLOOR must be a positive number (e.g. 500 or 500.0), got: " <>
                inspect(raw)
    end
  end
end
