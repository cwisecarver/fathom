defmodule Fathom.Rebalancer.HandoffJob do
  @moduledoc """
  Executes one shard handoff — moving a hot shard from its current node to a target,
  safely, via the cross-node command channel. Unique per shard (Oban `unique`), so a
  shard has at most one handoff in flight.

  ## The sequence (ordering is the safety)

  1. **Warm the target** (best-effort) — pre-pull the shard onto the target so its open is
     a 304, not a full body transfer.
  2. **Pin + flip the LB** — write the override (shard → target) and apply the LB map. New
     requests for the shard now route to the target. The target can't serve yet (the
     source still holds the lease → `acquire_lease` returns `{:held}`), so clients briefly
     retry; existing connections on the source finish.
  3. **Drain the source** — with new traffic now going to the target, the source's
     in-flight connections finish and it flushes + releases the lease.
  4. The target's next request (already routed there) acquires the freed lease and serves
     from the warm cache.

  **Why this order.** Draining first would leave the shard routed to the source (old hash
  home), so a client would immediately re-open it there — a race. Flipping first stops the
  inflow to the source, which is what makes draining a *hot* shard finish quickly. The
  `{owner, epoch}` lease guarantees no double-write across every interleaving regardless.

  **On drain failure** (connections don't drain in time): retry (the source usually drains
  on the next attempt now that traffic moved). If it still fails on the last attempt,
  **revert the pin** so traffic returns to the source (which still owns + serves) — a
  stuck shard is restored, not left pinned-and-unavailable.
  """
  use Oban.Worker,
    queue: :rebalance,
    max_attempts: 3,
    unique: [
      keys: [:shard_id],
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  require Logger

  alias Fathom.Rebalancer.{Commands, LbApply, Overrides}

  @impl Oban.Worker
  def perform(%Oban.Job{args: args, attempt: attempt, max_attempts: max}) do
    %{"shard_id" => shard, "from_node" => from, "to_node" => to} = args
    q = args["q_per_s"]

    warm(shard, to)
    pin_and_flip(shard, to, from, q)

    case drain(shard, from) do
      :ok ->
        Logger.info("rebalance: handoff #{shard} #{from} -> #{to} complete")
        :ok

      {:error, reason} when attempt >= max ->
        # Give up safely: return the shard to its source so it's served, not stranded.
        Overrides.unpin(shard)
        LbApply.apply!()
        Logger.warning("rebalance: handoff #{shard} drain failed (#{inspect(reason)}); reverted")
        {:cancel, "drain failed after #{max} attempts; reverted to #{from}"}

      {:error, reason} ->
        # Source's connections haven't finished; retry — it usually drains next attempt.
        {:error, reason}
    end
  end

  # Best-effort: a warm failure isn't fatal (the target cold-opens correctly), so proceed.
  defp warm(shard, to) do
    {:ok, cmd} = Commands.issue(shard, to, "warm")

    case Commands.await(cmd.id, timeout_ms: warm_timeout()) do
      {:ok, _} -> :ok
      other -> Logger.info("rebalance: warm #{shard} on #{to} not confirmed (#{inspect(other)})")
    end
  end

  defp pin_and_flip(shard, to, from, q) do
    {:ok, _} = Overrides.pin(shard, to, reason: "rebalance", q_per_s_at_pin: q, from_node: from)
    LbApply.apply!()
  end

  defp drain(shard, from) do
    {:ok, cmd} = Commands.issue(shard, from, "drain")

    case Commands.await(cmd.id, timeout_ms: drain_timeout()) do
      {:ok, _} -> :ok
      {:error, {:command_failed, detail}} -> {:error, detail}
      {:error, :timeout} -> {:error, :drain_timeout}
    end
  end

  defp warm_timeout, do: Application.get_env(:fathom, :handoff_warm_timeout_ms, 30_000)
  defp drain_timeout, do: Application.get_env(:fathom, :handoff_drain_timeout_ms, 30_000)
end
