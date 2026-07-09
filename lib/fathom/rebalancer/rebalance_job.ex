defmodule Fathom.Rebalancer.RebalanceJob do
  @moduledoc """
  The control loop — an Oban **cron** job that reads the merged fleet load, decides moves
  via `Fathom.Rebalancer.Policy`, and enqueues a `HandoffJob` per move. Singleton across
  the fleet via Oban's Postgres peer leadership (only one node inserts the cron job per
  tick), which is exactly right for the LB-partition model: no BEAM cluster, coordination
  through Postgres.

  Gated by `:rebalancer_enabled` (default off) — the cron can be scheduled everywhere and
  stays inert (a config read, no DB) until a deployment turns it on. When on, it reads
  `LoadSamples.since/1` + `Overrides.all/0` + `Rebalancer.lb_backends/0`, runs the policy,
  and enqueues unique-per-shard handoffs (a shard already mid-handoff is a no-op).
  """
  use Oban.Worker, queue: :rebalance, max_attempts: 1

  require Logger

  alias Fathom.Rebalancer
  alias Fathom.Shard.Storage

  alias Fathom.Rebalancer.{
    Commands,
    HandoffJob,
    LbApply,
    LoadSamples,
    Nodes,
    Overrides,
    Policy,
    WarmLocations
  }

  @sample_horizon_ms 120_000
  # Command retention (finding #12): terminal rows deleted after this; pending rows older
  # than the stale window (far past a handoff's warm+drain+retry lifetime) are expired so a
  # command for an absent node doesn't stay pending forever.
  @command_retention_ms 3_600_000
  @command_stale_ms 900_000
  # A node_key not seen (beaten) within this window is treated as dead by the reconciler
  # (finding #1b) — generous vs the ~10s reporter tick so a briefly-slow node keeps its pins.
  @node_stale_ms 60_000
  # Minimum fleet-wide sample count before the fleet p99 is trusted as the hot bar (finding
  # #2); below it, the p99 is noise, so the policy falls back to the floor / legacy path.
  @min_p99_samples 50

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    if enabled?() do
      run()
    else
      :ok
    end
  end

  defp run do
    # Bound the command channel each tick (finding #12): commands only exist when the
    # rebalancer is enabled, so the enabled cron is the right home for the sweep.
    Commands.prune_terminal(command_retention_ms())
    Commands.expire_stale_pending(command_stale_ms())

    # The live fleet (nodes beating within the staleness window) — the reconciler prefilter
    # AND the policy's viable-target set. Computed once (review 2026-07-09 #1).
    alive = Nodes.alive(node_stale_ms())

    # Unpin any shard pinned to a dead node (finding #1b) BEFORE the re-render, so the shard
    # returns to the hash pool (re-homes to a survivor — #1a already routes it there) instead
    # of staying pinned to a node that isn't coming back.
    reconcile_dead_pins(alive)

    # Re-render the full override table each tick (finding #10): the leader re-applies the
    # LB map (a byte-identical render is a cheap no-op) so any drift between the DB table and
    # the on-disk map — a raced apply!, a flip whose reload failed (#11), or a just-reconciled
    # dead-node unpin — self-heals within one tick without waiting for the next handoff.
    LbApply.apply!()

    samples = LoadSamples.since(horizon_ms()) |> Enum.map(&Map.from_struct/1)
    overrides = Overrides.all()
    backends = Rebalancer.lb_backends()
    # The fleet-relative hot bar (finding #2): count-weighted mean of loaded nodes'
    # full-distribution p99s, or nil (untrusted → policy uses the floor / legacy path) below
    # the sample floor.
    fleet_p99 = Nodes.fleet_p99(node_stale_ms(), min_p99_samples())
    # Warm-location map for affinity-aware target selection (Phase 2 C): which nodes have each
    # hot shard warm-cached, so a handoff prefers a warm target. Same freshness as liveness.
    warm_locations = WarmLocations.warm_nodes(node_stale_ms())

    case Policy.propose(samples, overrides, backends,
           fleet_p99: fleet_p99,
           warm_locations: warm_locations,
           alive_nodes: alive
         ) do
      [] ->
        :ok

      moves ->
        Enum.each(moves, &enqueue(&1, warm_locations))
        Logger.info("rebalance: enqueued #{length(moves)} handoff(s): #{summarize(moves)}")
        :ok
    end
  end

  # Unpin overrides whose pinned_node isn't in the live fleet (finding #1b). FAIL OPEN: if
  # nothing is beating (fresh deploy, or the beat mechanism is down), the alive set is empty
  # and we unpin NOTHING — we only reconcile when we have positive evidence the fleet is
  # beating. Already-failed rows are cooldown records that don't route, so they're left alone.
  defp reconcile_dead_pins(alive) do
    if MapSet.size(alive) > 0 do
      Overrides.all()
      |> Enum.reject(& &1.failed_at)
      |> Enum.reject(&MapSet.member?(alive, &1.pinned_node))
      |> Enum.each(&reconcile_candidate/1)
    end
  end

  # `pinned_node` isn't beating (the cheap reporter-beat prefilter). But the reporter beat is
  # NOT the data-plane liveness the lease respects — a node whose Reporter died while its data
  # plane keeps serving still holds the shard's S3 lease (review 2026-07-09 #1). Confirm
  # against the AUTHORITATIVE per-shard signal before unpinning: unpin ONLY when the lease is
  # free; on `{:held,_}` (the node is alive after all) or a store error, KEEP the pin
  # (fail-safe — unpinning a live-owned shard would route it to a node that can't steal the
  # held lease → unavailable) and flag the reporter-vs-data-plane divergence.
  defp reconcile_candidate(o) do
    case Storage.lease_holder(o.shard_id) do
      :free ->
        Overrides.unpin(o.shard_id)

        :telemetry.execute([:fathom, :rebalancer, :reconcile, :unpinned], %{count: 1}, %{
          shard_id: o.shard_id,
          node: o.pinned_node
        })

        Logger.warning(
          "rebalance: unpinned #{o.shard_id} — #{o.pinned_node} not live (S3 lease free); re-homing"
        )

      {:held, owner} ->
        divergence(o, "S3 lease held by #{owner}")

      {:error, reason} ->
        divergence(o, "S3 lease check failed (#{inspect(reason)})")
    end
  end

  defp divergence(o, why) do
    :telemetry.execute([:fathom, :rebalancer, :reconcile, :divergence], %{count: 1}, %{
      shard_id: o.shard_id,
      node: o.pinned_node
    })

    Logger.warning(
      "rebalance: kept pin #{o.shard_id} — #{o.pinned_node} isn't beating but #{why}; " <>
        "reporter/data-plane divergence, not unpinning (fail-safe)"
    )
  end

  defp enqueue(move, warm_locations) do
    # Observability (rebalancer telemetry): the move, and whether it landed on a warm target
    # (affinity hit, #C) or a cold one — the warm-hit rate shows the affinity signal's payoff.
    affinity = if warm_target?(move, warm_locations), do: :hit, else: :miss

    :telemetry.execute([:fathom, :rebalancer, :move, :proposed], %{count: 1}, %{
      shard_id: move.shard_id,
      from_node: move.from_node,
      to_node: move.to_node
    })

    :telemetry.execute([:fathom, :rebalancer, :affinity], %{count: 1}, %{
      outcome: affinity,
      shard_id: move.shard_id,
      to_node: move.to_node
    })

    %{
      "shard_id" => move.shard_id,
      "from_node" => move.from_node,
      "to_node" => move.to_node,
      "q_per_s" => move.q_per_s
    }
    |> HandoffJob.new()
    |> Oban.insert()
  end

  defp warm_target?(move, warm_locations) do
    warm_locations |> Map.get(move.shard_id, MapSet.new()) |> MapSet.member?(move.to_node)
  end

  defp summarize(moves) do
    Enum.map_join(moves, ", ", fn m -> "#{m.shard_id} #{m.from_node}->#{m.to_node}" end)
  end

  @doc "Whether the rebalancer acts (`:rebalancer_enabled`, default off)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :rebalancer_enabled, false) == true

  defp horizon_ms,
    do: Application.get_env(:fathom, :rebalance_sample_horizon_ms, @sample_horizon_ms)

  defp command_retention_ms,
    do: Application.get_env(:fathom, :rebalance_command_retention_ms, @command_retention_ms)

  defp command_stale_ms,
    do: Application.get_env(:fathom, :rebalance_command_stale_ms, @command_stale_ms)

  defp node_stale_ms,
    do: Application.get_env(:fathom, :rebalance_node_stale_ms, @node_stale_ms)

  defp min_p99_samples,
    do: Application.get_env(:fathom, :rebalance_min_p99_samples, @min_p99_samples)
end
