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
  alias Fathom.Rebalancer.{Commands, HandoffJob, LbApply, LoadSamples, Nodes, Overrides, Policy}

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

    # Unpin any shard pinned to a dead node (finding #1b) BEFORE the re-render, so the shard
    # returns to the hash pool (re-homes to a survivor — #1a already routes it there) instead
    # of staying pinned to a node that isn't coming back.
    reconcile_dead_pins()

    # Re-render the full override table each tick (finding #10): the leader re-applies the
    # LB map (a byte-identical render is a cheap no-op) so any drift between the DB table and
    # the on-disk map — a raced apply!, a flip whose reload failed (#11), or a just-reconciled
    # dead-node unpin — self-heals within one tick without waiting for the next handoff.
    LbApply.apply!()

    samples = LoadSamples.since(horizon_ms()) |> Enum.map(&Map.from_struct/1)
    overrides = Overrides.all()
    backends = Rebalancer.lb_backends()
    # The fleet-relative hot bar (finding #2): median of live nodes' full-distribution p99s,
    # or nil (untrusted → policy uses the floor / legacy path) below the sample floor.
    fleet_p99 = Nodes.fleet_p99(node_stale_ms(), min_p99_samples())

    case Policy.propose(samples, overrides, backends, fleet_p99: fleet_p99) do
      [] ->
        :ok

      moves ->
        Enum.each(moves, &enqueue/1)
        Logger.info("rebalance: enqueued #{length(moves)} handoff(s): #{summarize(moves)}")
        :ok
    end
  end

  # Unpin overrides whose pinned_node isn't in the live fleet (finding #1b). FAIL OPEN: if
  # nothing is beating (fresh deploy, or the beat mechanism is down), the alive set is empty
  # and we unpin NOTHING — we only yank a pin when we have positive evidence the fleet is
  # beating AND this node isn't. Already-failed rows are cooldown records that don't route,
  # so they're left alone.
  defp reconcile_dead_pins do
    alive = Nodes.alive(node_stale_ms())

    if MapSet.size(alive) > 0 do
      Overrides.all()
      |> Enum.reject(& &1.failed_at)
      |> Enum.reject(&MapSet.member?(alive, &1.pinned_node))
      |> Enum.each(fn o ->
        Overrides.unpin(o.shard_id)

        Logger.warning(
          "rebalance: unpinned #{o.shard_id} — node #{o.pinned_node} not live (re-homing)"
        )
      end)
    end
  end

  defp enqueue(move) do
    %{
      "shard_id" => move.shard_id,
      "from_node" => move.from_node,
      "to_node" => move.to_node,
      "q_per_s" => move.q_per_s
    }
    |> HandoffJob.new()
    |> Oban.insert()
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
