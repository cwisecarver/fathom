defmodule Fathom.Rebalancer.HandoffJob do
  @moduledoc """
  Executes one shard handoff — moving a hot shard from its current node to a target,
  safely, via the cross-node command channel. Unique per shard (Oban `unique`), so a
  shard has at most one handoff in flight.

  ## The sequence (ordering is the safety)

  1. **Warm the target** (best-effort) — pre-pull the shard onto the target so its open is
     a 304, not a full body transfer.
  2. **Pin + flip the LB** — write the override (shard → target) and apply the LB map. New
     requests for the shard now route to the target. The target can't serve yet (the source
     still holds the lease → `acquire_lease` returns `{:held}`), but `Fathom.Shards.checkout`
     sees this node is the pinned handoff target and **holds + retries the acquire** up to the
     drain window (finding #20) — so the first post-flip requests QUEUE for a few seconds
     rather than erroring; existing connections on the source finish.
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
        # ASK WHO ACTUALLY HOLDS THE LEASE before reverting (expert review 2026-08-26 #20).
        # `{:error, :drain_timeout}` is a statement about the COMMAND CHANNEL, not about the shard.
        settle_or_revert(shard, from, to, reason, max)

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
  # The last attempt failed. Before assuming the handoff did not happen, ask the data plane
  # (expert review 2026-08-26 #20).
  #
  # `drain/2` returns `{:error, :drain_timeout}` when `Commands.await/2` expires — which says the
  # command channel did not answer in time, NOT that the shard failed to move. The source may have
  # drained, flushed and released its lease, and the target may already hold it.
  #
  # Reverting in that state is the worst available outcome. `revert/4` marks the override failed and
  # re-renders the LB map, sending traffic back to the source; the source's `acquire_lease` then
  # finds a LIVE lease held by the target, and neither of `Fathom.Shards`' hold paths applies —
  # `handoff_pin_here?/1` is now false (the override carries `failed_at`) and the crash-failover
  # hold is false because the target's heartbeat keeps advancing. So `do_checkout/3` returns
  # `{:error, {:shard_held, target}}` immediately and the tenant 503s on EVERY request until the
  # target's coordinator idles out — `:shard_idle_ms`, 60 s at the default — on the shard that, by
  # construction, is the least likely in the fleet to go idle. The revert exists so that "a stuck
  # shard is restored, not left pinned-and-unavailable" (docs/rebalancing.md §3); on this path it
  # produced exactly the unavailability it was written to prevent.
  #
  # `lease_holder/1` is the callback that exists for this — `Fathom.Shard.Storage` calls it "the
  # authoritative data-plane liveness check before unpinning".
  #
  # FAILS CLOSED. Only a positive identification of the TARGET keeps the pin; `:free`, the source,
  # an unrecognised holder, a store error, or an exit all revert exactly as before. A revert is
  # always safe (the shard is served by the source); keeping the pin on a bad reading is not.
  defp settle_or_revert(shard, from, to, reason, max) do
    # `to != from` matters: when they are equal the handoff is a no-op and the probe is
    # DEGENERATE — "the target holds the lease" is trivially true of the source too, so it
    # distinguishes nothing and a drain timeout means only that the command channel is broken.
    # Learning nothing is a reason to take the conservative branch, not to skip it.
    if to != from and lease_held_by?(shard, to) do
      Logger.info(
        "rebalance: handoff #{shard} #{from} -> #{to} reported #{inspect(reason)}, but #{to} holds " <>
          "the lease — the handoff succeeded and the command channel lagged. Keeping the pin."
      )

      # The drain row is still pending; cancel it for the same reason `revert/4` does, so the
      # source poller cannot fire it later against a shard that has already moved (#7).
      Commands.cancel_pending_drains(shard)
      emit(:stop, %{outcome: :completed, shard_id: shard, from_node: from, to_node: to})
      :ok
    else
      result = revert(shard, from, reason, max)
      emit(:stop, %{outcome: :reverted, shard_id: shard, from_node: from, to_node: to})
      result
    end
  end

  # Read-only, and deliberately strict: anything that is not "the target holds it" is false.
  defp lease_held_by?(shard, node_key) do
    case Fathom.Shard.Storage.lease_holder(shard) do
      {:held, owner} -> owner_is_node?(owner, node_key)
      _ -> false
    end
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  # A lease owner is `Heartbeat.owner/0` — `"#{node()}##{incarnation}"` — so this compares the node
  # PREFIX rather than the whole string, anchored at the separator so `"node1"` cannot match
  # `"node10#abc"`.
  #
  # KNOWN LIMIT, and it fails in the safe direction: the handoff's `to`/`from` are
  # `Rebalancer.node_key/0`, which is `:node_key || node()`, while the lease owner is built from
  # `node()` alone. On a deployment that SETS `:node_key` to something other than the node name the
  # two never match, this returns false, and the job reverts exactly as it did before — no new
  # hazard, just no benefit there. (`docs/configuration.md` describes `NODE_KEY` as covering the
  # "heartbeat object", which `Heartbeat.owner/0` does not currently honour; that drift is worth a
  # look on its own and is deliberately not chased here.)
  defp owner_is_node?(owner, node_key) when is_binary(owner) and is_binary(node_key),
    do: owner == node_key or String.starts_with?(owner, node_key <> "#")

  defp owner_is_node?(_, _), do: false

  defp revert(shard, from, reason, max) do
    Overrides.mark_failed(shard)
    Commands.cancel_pending_drains(shard)
    LbApply.apply!()
    Logger.warning("rebalance: handoff #{shard} failed (#{inspect(reason)}); reverted to #{from}")
    {:cancel, "handoff failed after #{max} attempts (#{inspect(reason)}); reverted to #{from}"}
  end

  # Best-effort: a warm failure isn't fatal (the target cold-opens correctly), so proceed.
  # A rejected issue (e.g. an invalid shard_id now gated at Command.changeset — #6) is logged
  # and skipped, NOT hard-matched — so an invalid shard reverts at pin_and_flip (#14) instead
  # of MatchError-crashing here before the with-chain can handle it.
  defp warm(shard, to) do
    case Commands.issue(shard, to, "warm") do
      {:ok, cmd} ->
        case Commands.await(cmd.id, timeout_ms: warm_timeout()) do
          {:ok, _} ->
            :ok

          other ->
            Logger.info("rebalance: warm #{shard} on #{to} not confirmed (#{inspect(other)})")
        end

      {:error, changeset} ->
        Logger.info("rebalance: warm #{shard} not issued (#{inspect(changeset.errors)})")
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
    # Supersede any drain a prior attempt left pending (its await timed out with the poller
    # lagging) BEFORE issuing this attempt's — so the poller never runs two same-shard drains
    # concurrently, where the second hits the coordinator's "draining" busy-guard and could
    # mark THIS attempt failed even though the shard drained (review 2026-07-09 #7). One pending
    # drain per shard; the {owner,epoch} lease prevents a double-write regardless.
    Commands.cancel_pending_drains(shard)
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
  @spec drain_timeout() :: pos_integer()
  def drain_timeout do
    Application.get_env(:fathom, :handoff_drain_timeout_ms) ||
      Application.get_env(:fathom, :command_drain_ms, 10_000) + @shutdown_grace_ms
  end
end
