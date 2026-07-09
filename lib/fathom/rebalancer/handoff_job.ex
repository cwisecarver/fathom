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

  **On failure** — either the flip couldn't be applied (finding #11: `LbApply.apply!`
  returns `{:error, _}`, so the drain is *skipped* rather than stranding the source) or the
  drain didn't finish in time: retry (both usually resolve on the next attempt now that
  traffic moved). If it still fails on the last attempt, **revert the pin** so traffic
  returns to the source (which still owns + serves) — a stuck shard is restored, not left
  pinned-and-unavailable.
  """
  use Oban.Worker,
    queue: :rebalance,
    max_attempts: 3,
    # period: :infinity — a handoff routinely outlives Oban's default 60s unique window
    # (warm await + drain await + retry backoff across 3 attempts). With a 60s period, once
    # the first job's inserted_at ages past 60s a second HandoffJob for the same shard is no
    # longer deduped even while the first is still executing/retryable — two handoffs then
    # pin + drain the same shard. With :infinity + the live states below, uniqueness is
    # exactly "one handoff per shard until it reaches a terminal state" (finding #5).
    unique: [
      keys: [:shard_id],
      period: :infinity,
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  require Logger

  alias Fathom.Rebalancer.{Commands, LbApply, Overrides}

  @impl Oban.Worker
  def perform(%Oban.Job{args: args, attempt: attempt, max_attempts: max}) do
    %{"shard_id" => shard, "from_node" => from, "to_node" => to} = args
    q = args["q_per_s"]

    warm(shard, to)

    # Gate the drain on a confirmed live flip (finding #11): draining the source while the
    # LB still routes to it (flip not applied) would strand the shard. pin_and_flip fails
    # when the map/reload couldn't be applied, and then drain is skipped.
    with :ok <- pin_and_flip(shard, to, from, q),
         :ok <- drain(shard, from) do
      emit(:stop, %{outcome: :completed, shard_id: shard, from_node: from, to_node: to})
      Logger.info("rebalance: handoff #{shard} #{from} -> #{to} complete")
      :ok
    else
      {:error, reason} when attempt >= max ->
        result = revert(shard, from, reason, max)
        emit(:stop, %{outcome: :reverted, shard_id: shard, from_node: from, to_node: to})
        result

      {:error, reason} ->
        # Flip not applied yet, or source's connections haven't finished — retry (both
        # usually resolve on the next attempt). Emit the retry: a rising rate flags a
        # slow/wedged drain before it fully reverts (the #4 thrash precursor).
        emit(:retry, %{shard_id: shard, from_node: from, to_node: to})
        {:error, reason}
    end
  end

  defp emit(event, meta) do
    :telemetry.execute([:fathom, :rebalancer, :handoff, event], %{count: 1}, meta)
  end

  # Give up safely: return the shard to its source so it's served, not stranded. Mark (not
  # delete) the override so it's retained as a cooldown record — the renderer skips it
  # (traffic returns to source) but its fresh updated_at keeps the shard in the Policy
  # cooldown, so a wedged hot shard backs off instead of re-proposing every tick (#4). Cancel
  # any drain whose await timed out (row still pending) so the source poller can't fire it
  # after traffic was restored (#7).
  defp revert(shard, from, reason, max) do
    Overrides.mark_failed(shard)
    Commands.cancel_pending_drains(shard)
    LbApply.apply!()
    Logger.warning("rebalance: handoff #{shard} failed (#{inspect(reason)}); reverted to #{from}")
    {:cancel, "handoff failed after #{max} attempts (#{inspect(reason)}); reverted to #{from}"}
  end

  # Best-effort: a warm failure isn't fatal (the target cold-opens correctly), so proceed.
  defp warm(shard, to) do
    {:ok, cmd} = Commands.issue(shard, to, "warm")

    case Commands.await(cmd.id, timeout_ms: warm_timeout()) do
      {:ok, _} -> :ok
      other -> Logger.info("rebalance: warm #{shard} on #{to} not confirmed (#{inspect(other)})")
    end
  end

  # Pin the DB override, then apply the LB map. Returns apply!'s result: :ok when the flip
  # is live-or-out-of-band, {:error, reason} when it's known not live (so drain is skipped).
  # A rejected pin (e.g. an invalid shard_id — finding #14) returns {:error, _} instead of
  # MatchError-crashing the job; the with-chain reverts it rather than retrying forever.
  defp pin_and_flip(shard, to, from, q) do
    case Overrides.pin(shard, to, reason: "rebalance", q_per_s_at_pin: q, from_node: from) do
      {:ok, _} -> LbApply.apply!()
      {:error, changeset} -> {:error, {:invalid_pin, changeset.errors}}
    end
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

  # The drain await must exceed the poller's WORST-CASE drain — its `command_drain_ms` budget
  # plus `Fathom.Shards.drain`'s coordinator-shutdown safety net (+30s) — so a legitimately
  # slow-but-succeeding drain isn't mislabeled a timeout, which would trigger a premature
  # Oban retry (duplicate drain, #5/#7) or a spurious revert (#4). Ordering (finding #8):
  # handoff_drain_timeout_ms ≥ command_drain_ms + shutdown grace. Derived from
  # command_drain_ms so the invariant holds if an operator tunes it; an explicit
  # :handoff_drain_timeout_ms wins (operator owns the ordering then).
  @shutdown_grace_ms 35_000
  @doc false
  def drain_timeout do
    Application.get_env(:fathom, :handoff_drain_timeout_ms) ||
      Application.get_env(:fathom, :command_drain_ms, 10_000) + @shutdown_grace_ms
  end
end
