defmodule Fathom.Shard.PromoteOnOpen do
  @moduledoc """
  A2 promote-on-open — extracted from `Fathom.Shard` (2026-09-13, Phase 5, the final one).

  A survivor may hold a REPLICA of this shard that is newer than the stored object — that is the
  entire point of A2, and until this runs the replica sits on disk while the open serves the last
  flush. This decides WHICH BYTES ARE SERVED, so the coordinator calls it AFTER the lease and the
  fork verdict are settled (`revalidate_takeover/5`), passing the shard id, live path, lease, the
  fence etag so far, and the lineage.

  `maybe_promote_replica/5` is the only entry point. Every branch that is not a proven win returns
  the caller's `etag` unchanged, so the ordinary open path is bit-for-bit what it was — including
  every error, because a failed promotion must never fail an open (the stored-object path is still
  correct, it just recovers less). `Promote.fresher?/2` is what makes it safe: a replica is promoted
  only when STRICTLY ahead of what the object claims, the object's claim is an over-claim by
  construction (`Position.flush_position/1`), and an unstamped object is never overridable — so an
  object written before stamping existed, or by a node not yet upgraded, is left alone.

  Holds no coordinator state; every dependency is a sibling module (`Follower`, `Recovery`,
  `Promote`, `Storage`, `Position`, `Provenance`, `Fathom.Snapshots`).
  """

  require Logger

  alias Fathom.Shard.Position
  alias Fathom.Shard.Provenance
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Storage

  @spec maybe_promote_replica(String.t(), Path.t(), map(), String.t() | nil, term()) ::
          String.t() | nil | {:error, term()}
  def maybe_promote_replica(shard_id, path, lease, etag, lineage) do
    # The gate is checked FIRST and returns the caller's own binding, so a node that has not
    # enabled this allocates nothing at all on its open path. See the note at the call site.
    #
    # `follower_running?/0` is hoisted to sit beside the gate rather than living inside
    # `nothing_to_promote?/1`, and that is a DEFAULT-ON decision: the gate is now on everywhere, so
    # this `if` is the open path for every node that is not part of a replicating fleet, and it
    # should cost one `Process.whereis` rather than a walk through two more predicates. No follower
    # means no replica table to read AND nowhere for a pulled replica to install, so both branches
    # below would have declined anyway — `Recovery.search/5` says so in as many words.
    if promote_on_open?() and follower_running?() do
      try_promote(shard_id, path, lease, etag, lineage)
    else
      etag
    end
  end

  defp follower_running?, do: Process.whereis(Follower) != nil

  # TWO PATHS, and the split is about what a cold open is allowed to pay for.
  #
  # The local-only path checks ETS first and reaches the object store ONLY when this node actually
  # holds a replica — so a node with promote-on-open enabled pays nothing extra on the vast
  # majority of opens, which is why it was written that way and why it is kept bit-for-bit.
  #
  # The fleet path cannot be that lazy: deciding whether a PEER is worth asking requires the
  # object's position first, so it costs one stamp read on every promote-eligible open plus a
  # concurrent round trip to each peer. That is the price of closing the RPO gap on a failover the
  # LB routed to a node holding no replica, and it is why `Recovery` is a separate gate rather than
  # part of `:replication_promote_on_open`.
  # The middle branch is a COST check that became load-bearing when the gate started defaulting ON
  # (2026-08-25): `try_promote_from_fleet/5` opens with `Storage.object_head/1`, so without it every
  # cold open on every node — including nodes that follow nothing and have no peers — would pay an
  # object-store round trip to reach a decision that was already determined.
  #
  # BOTH halves of `nothing_to_promote?/1` are required, and dropping either is a real regression
  # rather than a tidier condition. An earlier draft short-circuited on `fleet_reachable?/1` alone
  # and fell through to the LOCAL path, which `promote_on_open_test.exs:435` immediately caught: a
  # node with a local replica and no peers still runs the fleet DECISION (`best_replica/3`
  # short-circuits on its own replica before opening a socket), and only that path performs the
  # mid-flight `recheck_object/4`. Routing it to the local path silently dropped the re-read and
  # promoted against a stamp that had moved.
  defp try_promote(shard_id, path, lease, etag, lineage) do
    cond do
      not Recovery.enabled?() -> try_promote_local(shard_id, path, lease, etag, lineage)
      nothing_to_promote?(shard_id) -> etag
      true -> try_promote_from_fleet(shard_id, path, lease, etag, lineage)
    end
  end

  defp nothing_to_promote?(shard_id) do
    replica_state(shard_id) == nil and not Recovery.fleet_reachable?()
  end

  defp try_promote_local(shard_id, path, lease, etag, lineage) do
    with replica when replica != nil <- replica_state(shard_id),
         {:ok, stamp} <- Storage.object_position(shard_id),
         true <- Promote.fresher?(replica, stamp) do
      promote_replica(shard_id, path, lease, etag, replica, stamp, lineage)
    else
      _ -> etag
    end
  end

  # `Recovery.best_replica/3` re-checks this node's own replica and short-circuits the network when
  # it already wins, so there is one call here rather than a local branch and a fleet branch that
  # could drift apart on what "fresher" means.
  #
  # `fresher?` is asserted again afterwards even though `Recovery` only ever returns a replica that
  # passed it. It is one comparison on a path that ends in overwriting a tenant's stored database,
  # and the alternative is trusting a promise made by a different module about bytes that arrived
  # over an unauthenticated socket.
  #
  # `object_head/1` rather than `object_position/1` because the decision needs the object's stamp
  # AND the etag it will be fenced with to describe the SAME version — see the callback. The head
  # is then RE-READ after the transfer and compared (`Recovery.recheck/3`): everything between the
  # two reads is network, bounded in seconds by the peer query and in database-size by the pull,
  # and a flush landing in there left the promote decision resting on a version that no longer
  # exists. That was never unsafe — the fenced publish 412s — but the cost was a whole transfer, a
  # snapshot, and a log line claiming the object was behind when by then it was not.
  defp try_promote_from_fleet(shard_id, path, lease, etag, lineage) do
    started = System.monotonic_time(:millisecond)

    with {:ok, head} <- Storage.object_head(shard_id),
         {:ok, replica} <- Recovery.best_replica(shard_id, position_of(head)),
         true <- Promote.fresher?(replica, position_of(head)),
         :ok <- recheck_object(shard_id, head, replica, started) do
      promote_replica(shard_id, path, lease, etag, replica, position_of(head), lineage)
    else
      _ -> etag
    end
  end

  defp position_of(nil), do: nil
  defp position_of(%{position: position}), do: position

  # The re-read. A failure to READ is treated as "moved" — declining is the conservative answer and
  # this whole path is an optimization over opening from the stored object, so an unreadable store
  # is a reason to take the ordinary path rather than to promote on a comparison we can no longer
  # confirm.
  defp recheck_object(shard_id, before, replica, started) do
    now =
      case Storage.object_head(shard_id) do
        {:ok, head} -> head
        {:error, reason} -> {:unreadable, reason}
      end

    case now do
      {:unreadable, reason} ->
        decline_promotion(shard_id, {:object_head_unreadable, reason}, started)

      head ->
        case Recovery.recheck(before, head, replica) do
          :ok -> :ok
          {:error, reason} -> decline_promotion(shard_id, reason, started)
        end
    end
  end

  # Counted, not just logged. A promotion abandoned here means a database was transferred across
  # the network for nothing, and that is invisible in the success telemetry by construction —
  # `recovered_from_peer` fires on the pull, which DID happen.
  defp decline_promotion(shard_id, reason, started) do
    elapsed = System.monotonic_time(:millisecond) - started

    Logger.warning(
      "shard #{shard_id}: abandoning replica promotion after #{elapsed}ms — the stored object " <>
        "changed while we were recovering (#{inspect(reason)}); opening from the stored object"
    )

    :telemetry.execute(
      [:fathom, :replication, :promotion_raced],
      %{count: 1, duration_ms: elapsed},
      %{shard_id: shard_id, reason: elem_reason(reason)}
    )

    {:error, reason}
  end

  # Every reason that reaches here today IS a tagged tuple (`Recovery.recheck/3` returns
  # `{:object_moved, _, _}` / `{:object_advanced, _}`, and the other caller passes
  # `{:object_head_unreadable, _}`), which dialyzer proves — a second `defp` clause for the bare
  # case was reported as unreachable. It is written as ONE total clause rather than deleted,
  # because this runs inside `decline_promotion/3` on the shard-open path: a future reason that is
  # a plain atom would otherwise raise FunctionClauseError from a telemetry label and fail an open
  # that the ordinary stored-object path would have served fine.
  defp elem_reason(reason) do
    if is_tuple(reason) and tuple_size(reason) > 0, do: elem(reason, 0), else: reason
  end

  # Default ON since 2026-08-25 (`REPLICATION_PROMOTE_ON_OPEN=false` turns it off). Free on a node
  # that holds no replicas: `try_promote_local/5` checks ETS before it touches the object store, so
  # a node outside a replicating fleet pays one ETS miss per cold open and nothing else.
  defp promote_on_open?, do: Application.get_env(:fathom, :replication_promote_on_open, true)

  # THE `Process.whereis` IS NOT REDUNDANT WITH THE RESCUE, it is the whole cost of this function.
  # `Follower.state_of/2` is an `:ets.lookup` on a table the follower owns, so on a node running no
  # follower it RAISED and this clause rescued — raising is control flow here, and building an
  # `ArgumentError` plus its stacktrace is orders of magnitude more expensive, and more garbage,
  # than the lookup it replaces. That was survivable while `:replication_promote_on_open` was
  # opt-in. Defaulting it on (2026-08-25) put it on every cold open of every node, and the bench
  # gate caught it immediately: `fanout_kb_per_shard` +52.7% while its GC'd twin moved +1.1% and
  # `served_kb_per_shard` -0.1% — i.e. per-open garbage, exactly the shape an exception makes.
  #
  # No follower process ⇒ no replica table ⇒ no replica, so the early return is also the truthful
  # answer rather than a shortcut. The rescue stays as a backstop for the race where the follower
  # dies between the two calls.
  defp replica_state(shard_id) do
    if Process.whereis(Follower), do: Follower.state_of(Follower, shard_id)
  rescue
    ArgumentError -> nil
  end

  # Ordering here is the whole risk, so it is worth stating:
  #
  #   1. SNAPSHOT the stored object first. This is the least reversible thing A2 does — it declares
  #      one of two lineages the winner and overwrites the other, on the strength of a comparison
  #      that is new code. A server-side copy costs no body transfer and is the difference between
  #      a bad hour and permanent loss if the comparison is ever wrong.
  #   2. STAGE into a temp and verify it there. Nothing touches the live path until the replica has
  #      been checkpointed into a standalone database and passed `quick_check`.
  #   3. PUBLISH from the temp, FENCED with the etag we hold. A 412 means someone wrote the object
  #      since our pull, so the replica is no longer provably newer — abandon and serve the
  #      ordinary path.
  #   4. Only then move the temp onto the live path and stamp the new etag as its provenance.
  #
  # A failure at 1–3 leaves the shard exactly as the ordinary open left it. A failure at 4 is the
  # one that cannot be shrugged off — the object is now the replica while the local file is not —
  # so it fails the open, which releases the lease and lets a clean open pull the bytes we just
  # published.
  defp promote_replica(shard_id, path, lease, etag, replica, stamp, lineage) do
    temp = "#{path}.promote.#{System.unique_integer([:positive])}"
    follower = Follower

    try do
      with :ok <- snapshot_before_promotion(shard_id),
           :ok <- Promote.stage(follower, shard_id, temp),
           {:ok, new_etag, _carried} <-
             Storage.flush(
               shard_id,
               temp,
               etag,
               Position.flush_position(%{lease: lease, path: temp, lineage: lineage}),
               Position.lineage_to_store(lineage)
             ) do
        case File.rename(temp, path) do
          :ok ->
            Enum.each(["-wal", "-shm"], &File.rm(path <> &1))
            Provenance.write(path, new_etag)
            Follower.forget(follower, shard_id)

            Logger.warning(
              "shard #{shard_id}: PROMOTED a local replica over the stored object " <>
                "(replica #{inspect(replica)} > object #{inspect(stamp)}); " <>
                "pre-promotion state snapshotted"
            )

            :telemetry.execute(
              [:fathom, :shard, :replica_promoted],
              %{count: 1},
              %{shard_id: shard_id, epoch: lease.epoch}
            )

            new_etag

          {:error, reason} ->
            # The ONE failure here that cannot be shrugged off: the object is now the replica while
            # the live path is not, so serving on would serve a lineage the store disagrees with.
            # Raising routes to `abandon_open/5`, which releases the lease and stops the
            # coordinator — a clean re-open then pulls exactly what was just published.
            raise "shard #{shard_id}: promoted object published but the local rename failed " <>
                    "(#{inspect(reason)}); refusing to serve a diverged local copy"
        end
      else
        {:error, reason} ->
          Logger.warning(
            "shard #{shard_id}: replica promotion declined (#{inspect(reason)}); " <>
              "opening from the stored object"
          )

          etag
      end
    after
      Enum.each(["", "-wal", "-shm"], &File.rm(temp <> &1))
    end
  end

  # Best-effort, and deliberately not fatal: a shard with no stored object yet has nothing to
  # snapshot, and a snapshot backend hiccup should not block a recovery that is otherwise sound.
  # It is attempted first precisely because it is the cheap insurance on the irreversible step.
  defp snapshot_before_promotion(shard_id) do
    case Fathom.Snapshots.create(shard_id, label: "pre-promotion") do
      {:ok, _snapshot_id} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "shard #{shard_id}: could not snapshot before promoting a replica " <>
            "(#{inspect(reason)}); proceeding"
        )

        :ok
    end
  end
end
