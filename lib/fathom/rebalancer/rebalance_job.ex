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
  alias Fathom.Rebalancer.{Commands, HandoffJob, LoadSamples, Overrides, Policy}

  @sample_horizon_ms 120_000
  # Command retention (finding #12): terminal rows deleted after this; pending rows older
  # than the stale window (far past a handoff's warm+drain+retry lifetime) are expired so a
  # command for an absent node doesn't stay pending forever.
  @command_retention_ms 3_600_000
  @command_stale_ms 900_000

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

    samples = LoadSamples.since(horizon_ms()) |> Enum.map(&Map.from_struct/1)
    overrides = Overrides.all()
    backends = Rebalancer.lb_backends()

    case Policy.propose(samples, overrides, backends) do
      [] ->
        :ok

      moves ->
        Enum.each(moves, &enqueue/1)
        Logger.info("rebalance: enqueued #{length(moves)} handoff(s): #{summarize(moves)}")
        :ok
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
end
