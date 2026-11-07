defmodule Fathom.Shard.PromoteOnOpen do
  @moduledoc """
  A2 promote-on-open — extracted from `Fathom.Shard` (2026-09-13, Phase 5, the final one).

  A survivor may hold a REPLICA of this shard that is newer than the stored object — that is the
  entire point of A2, and until this runs the replica sits on disk while the open serves the last
  flush. This decides WHICH BYTES ARE SERVED, so the coordinator calls it AFTER the lease and the
  fork verdict are settled (`revalidate_takeover/5`), passing the shard id, live path, lease, the
  fence etag so far, and the lineage.

  `maybe_promote_replica/8` is the only decision entry point (the coordinator passes `warm?`, and a warm open
  never promotes — see the head of that function). Every branch that decides "nothing fresher"
  returns the caller's `etag` unchanged, so the ordinary open path is bit-for-bit what it was. An
  ERROR is different: an unreadable head, too few peer answers, or a promotion that fails after a
  fresher replica was chosen leaves the decision unconfirmed, and serving the stored object then
  drops quorum-acked writes. Those refuse the open for the under-quorum window
  (`Recovery.error_window/3`) and fall back to the object only after it (Jepsen 2026-10-24: a
  promotion failed on a MinIO pause, the open served the object, and the acked tail was lost for
  good). `Promote.fresher?/2` is what makes it safe: a replica is promoted
  only when STRICTLY ahead of what the object claims, the object's claim never exceeds what it holds
  (a live flush stamps the position as of BEFORE its snapshot, `Position.snapshot_position/2`, so
  a replica holding acked writes the object lacks always wins — expert review 2026-10-15 #5; the
  occasional extra promotion this buys can drop only never-acked writes), and an unstamped object
  is never overridable — so an object written before stamping existed is left alone.

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

  @doc """
  Decide which bytes a cold open serves: the stored object, or a fresher A2 replica.

  The last argument is the object head the coordinator already read for this open's lineage
  (`{:ok, head}` / `{:error, _}`), or `nil` when it read none. When present it is the fleet path's
  decision read, so a replicating cold open pays one HEAD here rather than two back to back
  (expert review 2026-10-01 perf #3).

  `prefetch` is the ask `prefetch_recovery/5` made while the pull ran (`nil`: ask inline, as before).
  The caller follows this with `finish_prefetch/2`.
  """
  @spec maybe_promote_replica(
          String.t(),
          Path.t(),
          map(),
          String.t() | nil,
          term(),
          boolean(),
          {:ok, term()} | {:error, term()} | nil,
          prefetch()
        ) ::
          String.t() | nil | {:error, term()} | {:promoted, String.t(), pos_integer()}
  def maybe_promote_replica(shard_id, path, lease, etag, lineage, warm?, head, prefetch \\ nil) do
    cond do
      # A WARM open means the coordinator has ALREADY decided this node's local file is the bytes to
      # serve, and promote must not then clobber them (expert review 2026-09-18 #1 — a data-loss
      # bug). `warm? = file present AND NOT Fork.resolve(...)`, so a warm local is a non-forked,
      # SAME-LINEAGE copy — this node's own former-primary file. The primary writes locally BEFORE it
      # ships a frame, so no same-lineage peer replica (nor the stored object, which the primary's
      # own flush produced) can be strictly ahead of it; a cross-lineage stale local is a FORK, which
      # makes `warm?` false and routes here as a cold open instead. So promoting a peer over a warm
      # local always File.renames away acked-but-unflushed writes the local uniquely holds. Skipping
      # on `warm?` keeps ONE authority for "is the local good?" — the coordinator cannot both serve
      # the warm local and overwrite it. The node-loss RPO promote-on-open exists for is the COLD
      # failover path (a survivor the LB routed to that holds no local, so `warm?` is false), which
      # is untouched.
      #
      # DIVERGENCE from the review's suggested fix (recorded 2026-09-18): the review preferred a
      # position comparison — promote only when the candidate is strictly ahead of BOTH the warm
      # local file AND the object. That is more code (it must recompute the warm local's
      # lineage+ordinal, the subtle part) buying defense-in-depth against a Fork.resolve gap — but if
      # Fork.resolve were wrong the SERVE path would already be serving a stale local, so a promote
      # path that re-derived freshness independently would only let the two disagree. Revisit only if
      # a Fork.resolve gap is ever actually found.
      warm? ->
        etag

      # The gate is checked before the fleet machinery, so a node that has not enabled this allocates
      # nothing at all on its (cold) open path. See the note at the call site.
      #
      # `follower_running?/0` is hoisted to sit beside the gate rather than living inside
      # `nothing_to_promote?/1`, and that is a DEFAULT-ON decision: the gate is now on everywhere, so
      # this branch is the open path for every node that is not part of a replicating fleet, and it
      # should cost one `Process.whereis` rather than a walk through two more predicates. No follower
      # means no replica table to read AND nowhere for a pulled replica to install, so both branches
      # below would have declined anyway — `Recovery.search/5` says so in as many words.
      promote_on_open?() and follower_running?() ->
        try_promote(shard_id, path, lease, etag, lineage, head, prefetch)

      true ->
        etag
    end
  end

  defp follower_running?, do: Process.whereis(Follower) != nil

  @doc """
  Would a cold open promote a peer/local replica over the stored object right now? A READ-ONLY
  probe over the same predicate as `maybe_promote_replica/7` (expert review 2026-10-15 #2): same
  gates, same `Recovery.best_replica/3` decision, same `Promote.fresher?/2` test — but it installs
  nothing and touches no marker, so the migrator can ask "is a replica ahead of the object?"
  without becoming a second A2 writer.

    * `:no` — nothing fresher than the object (or A2 promotion is gated off / not applicable),
      so reading the object is as good as a cold open would be.
    * `:ahead` — a replica strictly ahead of the object's stamp exists; the object is STALE and
      anything that reads-modifies-flushes it would permanently drop the acked tail.
    * `{:unknown, reason}` — the decision could not be made (object HEAD failed, or too few peers
      answered inside the under-quorum window). Callers that would overwrite the object treat it
      like `:ahead`: retry later.

  `prev_owner` is the lease owner string that was stolen, if any (its answer is excluded from the
  responder floor, as on the open path).
  """
  @spec replica_ahead?(String.t(), String.t() | nil) :: :no | :ahead | {:unknown, term()}
  def replica_ahead?(shard_id, prev_owner) do
    if promote_on_open?() and follower_running?() and not nothing_to_promote?(shard_id) do
      probe_replica_ahead(shard_id, prev_owner)
    else
      :no
    end
  end

  defp probe_replica_ahead(shard_id, prev_owner) do
    case Storage.object_head(shard_id) do
      {:ok, nil} ->
        :no

      {:ok, head} ->
        stamp = position_of(head)

        cond do
          Recovery.enabled?() ->
            case Recovery.best_replica(shard_id, stamp, previous_owner: prev_owner) do
              {:ok, replica} -> if Promote.fresher?(replica, stamp), do: :ahead, else: :no
              {:refuse, refusal} -> {:unknown, refusal}
              _ -> :no
            end

          true ->
            if Promote.fresher?(replica_state(shard_id), stamp), do: :ahead, else: :no
        end

      {:error, reason} ->
        {:unknown, {:object_head, reason}}
    end
  end

  @typedoc """
  A fleet recovery ask made while the cold pull ran (`prefetch_recovery/5`): `nil`, or the answer
  `Recovery.best_replica/3` gave, with the `.recovering` marker already durable.
  """
  @type prefetch :: nil | {:asked, term()}

  @doc """
  Start the fleet recovery decision EARLY: write the `.recovering` marker and ask the fleet as soon
  as the open's post-lease head is known, instead of after the pull is installed (expert review
  2026-10-17 #14 step 2). Called from the coordinator's open-head task, so the marker fsync and the
  peer round trip run alongside the pull. Returns `nil` (nothing done) or `{:asked, answer}`.

  Only for a cold open with NO local file (`cold?`): a warm or fork-resolved open decides later and
  takes the inline path, and a marker beside a warm file would make the next open purge it. The
  remaining gate is the one the inline path reaches the ask through — promote on, a follower running,
  `Recovery.enabled?/0`, something to promote, a rankable stamp — so a prefetch is never made where
  the inline path would not ask. Should the gate still change before consumption, the coordinator's
  `finish_prefetch/2` removes the marker before anything is served.

  The marker is durable before the ask is sent (the 2026-10-13 #4 invariant). It is NOT ordered
  against the pull's install: when the pull lands first, the installed-but-unmarked window is the
  one the inline path always had (install, then the head await, then the marker) — no wider, since
  the head task starts the marker the moment its HEAD returns. Ordering it strictly before the install
  would make the open await the head before the pull, giving up a failed pull's prompt abort
  (`ReplicatingOpenRttTest`).
  """
  @spec prefetch_recovery(String.t(), Path.t(), map(), term(), boolean()) :: prefetch()
  def prefetch_recovery(shard_id, path, lease, {:ok, %{} = head}, true = _cold?) do
    stamp = position_of(head)

    if promote_on_open?() and follower_running?() and Recovery.enabled?() and
         not nothing_to_promote?(shard_id) and Recovery.rankable_stamp?(stamp) do
      prefetch_hook(path)
      mark_recovering(path)

      {:asked, Recovery.best_replica(shard_id, stamp, previous_owner: Storage.floor_owner(lease))}
    end
  end

  def prefetch_recovery(_shard_id, _path, _lease, _head, _cold?), do: nil

  @doc """
  Remove a prefetch's marker once the open's promote decision is made (expert review 2026-10-17 #14
  step 2). Idempotent: the consuming path (`recover_with_marker/7`) already removed it. Only reached
  on a decided open — an open that FAILS keeps the marker, so the next open purges the unvetted pull.
  """
  @spec finish_prefetch(prefetch(), Path.t()) :: :ok
  def finish_prefetch(nil, _path), do: :ok

  def finish_prefetch({:asked, _}, path) do
    _ = Fathom.FS.rm(recovering_path(path))
    :ok
  end

  # Test seam: runs in the open-head task right before the prefetch's marker (expert review
  # 2026-10-17 #14 step 2), so a test can see WHEN the ask starts relative to the pull. Unset in
  # every real config.
  defp prefetch_hook(path) do
    case Application.get_env(:fathom, :recovery_prefetch_hook) do
      nil -> :ok
      hook -> hook.(path)
    end
  end

  @doc """
  The etag to serve from, given what `maybe_promote_replica/8` returned.

  A promotion that stated an ordinal answers `{:promoted, etag, seed}`; every other outcome is the
  etag itself. Unwrapped with two pattern-matching accessors rather than a tuple on every open, so
  the (overwhelmingly common) no-promotion path allocates nothing new — see the note at the call
  site in `Fathom.Shard` about per-open allocations staying resident.
  """
  @spec etag_of(term()) :: term()
  def etag_of({:promoted, etag, _seed}), do: etag
  def etag_of(etag), do: etag

  @doc """
  The WAL ordinal a coordinator must start its counter from after a promotion, or 0.

  See `promote_replica/7`: the promoted object is stamped at the replica's ordinal + 1, so this
  coordinator's first WAL must number ABOVE it — or its own quorum-acked frames rank below the
  object it just published and can never be promoted if it dies before its first flush.
  """
  @spec seed_ordinal_of(term()) :: non_neg_integer()
  def seed_ordinal_of({:promoted, _etag, seed}), do: seed
  def seed_ordinal_of(_etag), do: 0

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
  # mid-flight `recheck_object/5`. Routing it to the local path silently dropped the re-read and
  # promoted against a stamp that had moved.
  #
  # Do NOT skip the fleet ask for a claim-less lease (expert review 2026-10-17 #7, decided: keep it).
  # "No :claim ⇒ the previous owner flushed and stamped everything, so no peer can be fresher" is
  # false on the conns>0 terminate path (rolling deploy / SIGTERM): WriteFence goes up and the drop
  # snapshot is taken, but a statement that passed the fence check just before can commit after the
  # snapshot's position stamp was read and be quorum-acked to followers. The drop flush still
  # succeeds, so `release_plain` deletes the lock with no claim, and the object lacks a write a peer
  # replica holds. This ask is what recovers it.
  defp try_promote(shard_id, path, lease, etag, lineage, head, prefetch) do
    cond do
      not Recovery.enabled?() -> try_promote_local(shard_id, path, lease, etag, lineage)
      nothing_to_promote?(shard_id) -> etag
      true -> try_promote_from_fleet(shard_id, path, lease, etag, lineage, head, prefetch)
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
  #
  # The decision read reuses the head the coordinator took for this open's lineage when it has one:
  # both read the same object under the same lease with nothing served in between, so a second HEAD
  # only added a serial round trip (expert review 2026-10-01 perf #3). The RE-read below is a
  # different read on purpose and is never shared.
  defp try_promote_from_fleet(shard_id, path, lease, etag, lineage, {:ok, head} = prefetched, ask) do
    # perf review 2026-10-14 #8(b): an object whose stamp cannot be ranked (unstamped, lineage 0)
    # can never be overridden, so `Recovery.best_replica/3` answers `:none` without a socket and
    # nothing is "pending". Skip the marker's open + fsync + close + rm too — the marker exists to
    # make the ASK crash-safe, and there is no ask. Only decidable when the coordinator already
    # holds the decision head; otherwise the marker is written before the HEAD exactly as before.
    # A takeover that found NO object is the exception (`absent_takeover?/3`): it asks.
    if Recovery.rankable_stamp?(position_of(head)) or
         absent_takeover?(position_of(head), path, lease) do
      recover_with_marker(shard_id, path, lease, etag, lineage, prefetched, ask)
    else
      etag
    end
  end

  # A prefetch exists only with an `{:ok, head}` (`prefetch_recovery/5`), so this clause has none.
  defp try_promote_from_fleet(shard_id, path, lease, etag, lineage, prefetched, _no_ask),
    do: recover_with_marker(shard_id, path, lease, etag, lineage, prefetched, nil)

  defp recover_with_marker(shard_id, path, lease, etag, lineage, prefetched, ask) do
    # RECOVERY PENDING, DURABLY, BEFORE THE ASK (expert review 2026-10-13 #4). By here the cold pull
    # has installed the object's bytes at `path` with a MATCHING sidecar, and the cleanup that makes
    # a retry re-run recovery used to exist only on the refusal path. A node dying during the up-to-2 s
    # peer ask left file + sidecar == object etag: the next open read `:match`, opened WARM, and a
    # warm open never recovers — the floor was bypassed behind a restart. The marker is fsynced
    # before `Recovery.best_replica` and removed after the decision (accept, decline or promote);
    # `purge_if_recovering/1` (the open path) treats a surviving marker as cold. NOT "delete the
    # sidecar early": a crash would then read `:no_sidecar`, which quarantines a shard-sized file
    # and raises the fork alarm. A raise (refusal, failed rename) leaves the marker or removes it
    # with the files — either way the next open recovers again.
    #
    # A PREFETCHED ask (`prefetch_recovery/5`, expert review 2026-10-17 #14 step 2) already wrote the
    # marker -- durably, before it asked -- and asked the fleet while the pull ran; only its answer
    # is consumed here. `before_recovery_ask/1` stays at this point either way: it is where the node
    # can die with the pull installed and the answer not yet acted on.
    if ask == nil, do: mark_recovering(path)
    before_recovery_ask(path)
    result = decide_from_fleet(shard_id, path, lease, etag, lineage, prefetched, ask)
    Fathom.FS.rm(recovering_path(path))
    result
  end

  defp decide_from_fleet(shard_id, path, lease, etag, lineage, prefetched, ask) do
    started = System.monotonic_time(:millisecond)

    with {:head, {:ok, head}} <- {:head, decision_head(shard_id, prefetched)},
         {:stamp, {:ok, stamp}} <- {:stamp, decision_stamp(shard_id, path, lease, head)},
         {:ok, replica} <- ask_answer(ask, shard_id, path, stamp, Storage.floor_owner(lease)),
         true <- Promote.fresher?(replica, stamp),
         :ok <- recheck_object(shard_id, head, replica, stamp, started) do
      promote_replica(shard_id, path, lease, etag, replica, stamp, lineage)
    else
      # The resurrection guard could not be read, so whether a peer's copy may be promoted over
      # NO object is undecided: the same window as an unreadable decision head.
      {:stamp, {:error, reason}} ->
        refuse_or_etag(Recovery.error_window(shard_id, {:tombstone_check, reason}), path, etag)

      # THE RE-READ FAILED (expert review 2026-10-13 N3). By here a peer replica was already
      # chosen as fresher, and used to be DECLINED on this error: the open then proceeded from the
      # OLDER stored object, with no floor, on exactly the S3 brownout that coincides with the
      # partition. A re-read error means the decision can no longer be confirmed either way, which
      # is the same situation as a failed decision HEAD, so it takes the same under-quorum window
      # (refuse inside it, best-effort after, `:none` with no peers). Declining remains right for a
      # re-read that SUCCEEDS and shows the object moved.
      {:head_error, reason} ->
        refuse_or_etag(Recovery.error_window(shard_id, {:object_head, reason}), path, etag)

      # THE DECISION READ FAILED (expert review 2026-10-13 #15). Without the object's stamp the
      # floor cannot be evaluated at all, and this used to fall through to the plain etag: open
      # from the object with no floor, on a HEAD error that coincides with the very S3 brownouts
      # that accompany partitions. Now it goes through the SAME under-quorum window as too few
      # answers: refuse inside it, best-effort (error-logged) after it. With no peers configured
      # (a local replica only) `error_window/3` returns :none and the open proceeds as before.
      {:head, {:error, reason}} ->
        refuse_or_etag(Recovery.error_window(shard_id, {:object_head, reason}), path, etag)

      _ ->
        etag
    end
  end

  # The prefetched answer was computed against this same head (the coordinator's shared open head),
  # so it is the answer `best_replica_or_refuse/4` would give here -- including a refusal, which is
  # raised now, with the pull installed, exactly as an inline one is.
  defp ask_answer({:asked, {:refuse, refusal}}, _shard_id, path, _stamp, _prev),
    do: refuse_open(path, refusal)

  defp ask_answer({:asked, answer}, _shard_id, _path, _stamp, _prev), do: answer

  defp ask_answer(nil, shard_id, path, stamp, prev),
    do: best_replica_or_refuse(shard_id, path, stamp, prev)

  defp refuse_or_etag({:refuse, refusal}, path, _etag), do: refuse_open(path, refusal)
  defp refuse_or_etag(_none, _path, etag), do: etag

  # TOO FEW PEERS ANSWERED (expert review 2026-10-12 #5): refuse the open, so the client retries
  # while the partition heals, rather than serve — and at the first flush re-stamp over — a view
  # that may be missing quorum-acked writes. The raise lands in the coordinator's lease-holding
  # try (`abandon_open/5`): the lease is released and the coordinator stops, so nothing is held for
  # the window and the checkout gets a retryable `{:open_failed, %UnderQuorum{}}` (503).
  #
  # THE COLD-PULLED LOCAL COPY IS DELETED FIRST, and that is load-bearing: left on disk with its
  # provenance sidecar, the retry's fork check reads `:match`, opens WARM, and a warm open never
  # promotes (`maybe_promote_replica/7`) — so the second attempt would skip recovery and the window
  # entirely and serve the object. Deleting is safe: this is a cold open, so the file is a fresh
  # pull of the stored object (or its re-pull after revalidation) that nothing has served yet.
  #
  # `prev_owner` is the owner string of the lease we stole (expert review 2026-10-13 #14): that node
  # is not one of the writer's followers, so its answer is excluded from the responder floor.
  defp best_replica_or_refuse(shard_id, path, stamp, prev_owner) do
    case Recovery.best_replica(shard_id, stamp, previous_owner: prev_owner) do
      {:refuse, refusal} -> refuse_open(path, refusal)
      other -> other
    end
  end

  defp refuse_open(path, refusal) do
    Enum.each(["", "-wal", "-shm"], &Fathom.FS.rm(path <> &1))
    Fathom.FS.rm(Provenance.sidecar_path(path))
    Fathom.FS.rm(recovering_path(path))
    raise refusal
  end

  # Test seam for the crash window between the marker and the peer ask (expert review 2026-10-13
  # #4). Unset in every real config.
  defp before_recovery_ask(path) do
    case Application.get_env(:fathom, :recovery_ask_crash_hook) do
      nil -> :ok
      hook -> hook.(path)
    end
  end

  @doc "The recovery-pending marker beside a shard's live path (expert review 2026-10-13 #4)."
  @spec recovering_path(Path.t()) :: Path.t()
  def recovering_path(path), do: path <> ".recovering"

  # fsynced (a marker lost to a power cut is the same hole as no marker). An empty file: its
  # existence is the whole signal. A failure to write it is logged and the open continues — the
  # floor still runs for THIS open, only the crash-safety of the ask is lost.
  defp mark_recovering(path) do
    marker = recovering_path(path)

    with {:ok, fd} <- :file.open(marker, [:write, :raw, :binary]),
         :ok <- :file.sync(fd),
         :ok <- :file.close(fd) do
      :ok
    else
      other -> Logger.warning("could not write recovery marker #{marker}: #{inspect(other)}")
    end
  end

  @doc """
  Open-path check, called before the warm/cold decision: a surviving `.recovering` marker means a
  previous open died between installing the pulled object and finishing the peer ask (expert review
  2026-10-13 #4), so the local file is an unvetted pull and must not be served warm. Removes the db,
  its `-wal`/`-shm` and the sidecar, then the marker LAST (a crash part-way re-enters here). Returns
  true when a marker was found. The caller then opens cold, pulling and recovering again.
  """
  @spec purge_if_recovering(Path.t()) :: boolean()
  def purge_if_recovering(path) do
    marker = recovering_path(path)

    if Fathom.FS.exists?(marker) do
      Logger.warning(
        "shard open found #{marker}: a previous open died before peer recovery finished; " <>
          "discarding the unvetted pull and recovering again"
      )

      Enum.each(["-wal", "-shm", ""], &Fathom.FS.rm(path <> &1))
      Fathom.FS.rm(Provenance.sidecar_path(path))
      Fathom.FS.rm(marker)
      true
    else
      false
    end
  end

  defp decision_head(_shard_id, {:ok, _} = head), do: head
  defp decision_head(shard_id, _), do: Storage.object_head(shard_id)

  # A TAKEOVER THAT FOUND NO OBJECT DECIDES AGAINST `Recovery.absent_stamp/0` (Jepsen round-8 herd
  # run, 2026-10-23; the user chose option A). The owner of a brand-new shard can be killed after
  # quorum-acking writes and before its first object lands (`Fathom.Shard`'s `stamp_first_write/1`
  # shrank that window to one object-store round trip; it did not close it). The survivor then found
  # no object, or only a steal sentinel, and an unrankable `nil` stamp skipped the fleet ask, so it
  # served the shard EMPTY while peers held the writes. Ranking "nothing" as `{0, 0, 0}` lets any
  # replica with a stated lineage and ordinal win, under the ordinary `N - Q + 1` floor.
  #
  # All three conditions are load-bearing:
  #   * NOT RANKABLE: a stamped object is decided on its own stamp as before;
  #   * TAKEN FROM ANOTHER HOLDER (`took_from_another_holder?/2`): only then can someone else have
  #     acked writes this node did not see. A fresh create or a reclaim of this node's own lock
  #     keeps today's path, so a read-only probe of a never-seen id never asks the fleet;
  #   * NO LOCAL FILE: the open pulled no bytes. An unstamped REAL object pulls bytes, and must stay
  #     unoverridable (`Promote.fresher?(_, nil)`), so "no bytes", not "no stamp", is the test. It
  #     is also backend-neutral: S3 leaves a steal sentinel with no position, Local leaves nothing.
  defp absent_takeover?(stamp, path, lease) do
    # `presence/1 == :absent`: an indeterminate stat is not "no local file" (expert review
    # 2026-11-07 #21) — promoting a peer's replica over a local copy that may hold acked writes is
    # the clobber the third condition exists to prevent.
    not Recovery.rankable_stamp?(stamp) and took_from_another_holder?(lease, lease.owner) and
      Fathom.FS.presence(path) == :absent
  end

  # THE RESURRECTION GUARD for `absent_takeover?/3`. A purged tenant has no object either, and a
  # peer that missed the delete notification may still hold its replica, so promoting it would bring
  # a deleted tenant back. The coordinator already refused ids in this node's tombstone ETS at init;
  # this reads the DURABLE marker (`Storage.tombstone_present?/1`), which `Tenants.delete/1` writes
  # before the purge job is enqueued and `Tenants.purge/1` writes again before its final sweep, so it
  # covers a node whose ETS missed the notification. One HEAD, only on an absent takeover.
  defp decision_stamp(shard_id, path, lease, head) do
    stamp = position_of(head)

    if absent_takeover?(stamp, path, lease) do
      case Storage.tombstone_present?(shard_id) do
        {:ok, false} -> {:ok, Recovery.absent_stamp()}
        {:ok, true} -> {:ok, stamp}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, stamp}
    end
  end

  @doc """
  Did this lease come from ANOTHER holder, so that someone else may have acked writes this node never
  saw? True when it was stolen from another owner, or taken from a released marker that names
  another NODE.

  A WARM LOCAL IS NOT THE NEWEST HISTORY WHEN THE LEASE CAME FROM ANOTHER HOLDER (Jepsen
  2026-10-24, `WarmTakeoverPromoteTest`). Promote-on-open skips a warm local (2026-09-18 #1) on
  the argument that no replica can be ahead of this node's own former-primary file. That holds only
  if nobody else owned the shard since. Jepsen: fathom4's flush landed and it released; fathom3
  took over, promoted exactly those bytes, and acked 5,044 writes it never managed to flush (MinIO
  paused); fathom4 stole the shard back, found its local copy byte-identical to the object (so
  `Fork.resolve` correctly saw no fork), opened WARM, skipped the promotion, and served without
  fathom3's quorum-acked writes, which its own replica table was holding. Whenever the lease was
  stolen from another NODE (`:prev_owner` names one), the promotion decision runs. It only
  promotes a replica strictly ahead of the object, so a local that equals the object loses
  nothing, and a reclaim of this node's own lock (no `:prev_owner`) is unchanged.

  A RELEASED MARKER counts when the holder it names (`:released_by`, carried through clean
  visitors by `Storage.floor_owner/1`) is another NODE (Jepsen 2026-11-01, shard 4). fathom4 kept
  its local copy and released; fathom5 took over, promoted those same bytes, acked writes to
  fathom2 and lost power before flushing; fathom1 stole fathom5's lock, refused its open under
  quorum and released clean, so the marker named fathom5; fathom4 took that marker, adopted the
  unchanged object as its own flush, opened WARM, never asked fathom2, and wrote over fathom5's
  acked writes. Only a marker naming this node (its own release, or clean visits since) or naming
  nobody keeps the warm skip: there the warm local IS the newest history, and running the decision
  would refuse a safe open whenever peers are short (`RefusedOpenUnderQuorumTest` pins serving it).
  Compared by node key, not owner string, so this node's own release from an earlier incarnation
  is still its own. Pure.
  """
  @spec took_from_another_holder?(Storage.lease(), String.t()) :: boolean()
  def took_from_another_holder?(%{prev_owner: prev} = lease, owner) when is_binary(prev) do
    if prev == Storage.released_owner(),
      do: released_by_another_node?(Storage.floor_owner(lease), owner),
      else: true
  end

  def took_from_another_holder?(_lease, _owner), do: false

  defp released_by_another_node?(by, owner) when is_binary(by),
    do: Fathom.Rebalancer.owner_node_key(by) != Fathom.Rebalancer.owner_node_key(owner)

  defp released_by_another_node?(_nobody, _owner), do: false

  defp position_of(nil), do: nil
  defp position_of(%{position: position}), do: position

  # The re-read. A re-read that SUCCEEDS and shows the object moved declines the promotion (the
  # ordinary path then serves the newer object). A re-read that FAILS is `{:head_error, reason}`,
  # routed by the caller through the recovery error window — NOT treated as "moved" (expert review
  # 2026-10-13 N3: that opened from the older object after the replica was already pulled).
  defp recheck_object(shard_id, before, replica, stamp, started) do
    case Storage.object_head(shard_id) do
      {:ok, head} ->
        case Recovery.recheck(before, head, replica, absent_or_nil(stamp)) do
          :ok -> :ok
          {:error, reason} -> decline_promotion(shard_id, reason, started)
        end

      {:error, reason} ->
        {:head_error, reason}
    end
  end

  defp absent_or_nil(:absent), do: :absent
  defp absent_or_nil(_stamp), do: nil

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
  # PROMOTE STRICTLY ABOVE THE REPLICA (expert review 2026-09-29 #3, the promote tier). The promoted
  # object is stamped with THIS owner's lineage; a replica already holding that lineage or a higher
  # one came from a history this owner does not continue, and stamping over it would give two
  # histories one label. With the takeover claim in the lock (`Storage.takeover_claim/2`) the
  # canonical failover always opens ABOVE the dead owner's replica, so this should never fire —
  # it is the guard for anything that reaches here otherwise, and it declines rather than stamping:
  # serving the stored object is always correct, merely older.
  defp promote_replica(shard_id, _path, _lease, etag, %{lineage: rl}, _stamp, lineage)
       when is_integer(rl) and is_integer(lineage) and rl >= lineage do
    Logger.warning(
      "shard #{shard_id}: not promoting — the replica's lineage #{rl} is not below this owner's " <>
        "#{lineage}, so stamping it would reuse a lineage; serving the stored object"
    )

    etag
  end

  defp promote_replica(shard_id, path, lease, etag, replica, stamp, lineage) do
    temp = "#{path}.promote.#{System.unique_integer([:positive])}"
    follower = Follower

    try do
      with :ok <- snapshot_before_promotion(shard_id),
           # Copied under the replica's lock and fenced at THIS owner's lineage before the lock is
           # released (expert review 2026-10-08 #13), and only if the replica is still the one
           # ranked above (`expect:`) — see `Promote.stage/4`.
           {:ok, fence} <-
             Promote.stage(follower, shard_id, temp, lineage: lineage, expect: replica),
           {:ok, new_etag, _carried} <-
             publish_or_unfence(follower, shard_id, fence, fn ->
               publish_promoted(
                 shard_id,
                 path,
                 temp,
                 etag,
                 Position.flush_position(promoted_stamp_state(lease, temp, lineage, replica)),
                 Position.lineage_to_store(lineage)
               )
             end) do
        # SIDECAR FIRST, like `Materializer.promote_pull/3` (expert review 2026-10-12 #22). Writing the
        # sidecar AFTER the rename left a crash window: the live `.db` is the promoted bytes while the
        # sidecar still names the pre-promotion etag, which the next open reads as a divergence
        # (`Fork.resolve` → `:fork`), quarantining a shard-sized copy that equals the object and
        # re-pulling it. Sidecar-first is safe only if the crash leaves "a sidecar and NO `.db`", and
        # the ordinary pull that preceded this promotion left the pre-promotion `.db` at `path` — so
        # that goes first (with its `-wal`/`-shm`, which must never be paired with the promoted
        # bytes). No connection has opened it: this runs inside the open. Removing it loses nothing:
        # the promoted object is already published and a cold open pulls exactly those bytes.
        # (The pre-promotion `.db` is already ASIDE — `publish_promoted/6` — so there is nothing
        # live to remove here; expert review 2026-10-13 #22.)
        #
        # The write's result gates the rename (expert review 2026-11-07 #28): landing the promoted
        # bytes beside a stale or absent sidecar is the false fork this ordering exists to prevent.
        # Raising routes to `abandon_open/5` exactly like the failed rename below.
        case Provenance.write(path, new_etag) do
          :ok ->
            :ok

          {:error, reason} ->
            raise "shard #{shard_id}: promoted object published but its provenance sidecar could " <>
                    "not be written (#{inspect(reason)}); refusing to land it beside a stale sidecar"
        end

        before_promote_rename(path)

        case Fathom.FS.rename(temp, path) do
          :ok ->
            Enum.each(["-wal", "-shm", ""], &Fathom.FS.rm(prepromote(path) <> &1))
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

            case replica_ordinal(replica) do
              0 -> new_etag
              n -> {:promoted, new_etag, n + 1}
            end

          {:error, reason} ->
            # The ONE failure here that cannot be shrugged off: the object is now the replica while
            # the live path is not, so serving on would serve a lineage the store disagrees with.
            # Raising routes to `abandon_open/5`, which releases the lease and stops the
            # coordinator — a clean re-open then pulls exactly what was just published.
            raise "shard #{shard_id}: promoted object published but the local rename failed " <>
                    "(#{inspect(reason)}); refusing to serve a diverged local copy"
        end
      else
        # A FAILED PROMOTION IS AN UNCONFIRMED DECISION, NOT A "NO" (Jepsen 2026-10-24,
        # `PromoteOnOpenTest` "a promotion that fails on a store error refuses the open"). This
        # used to log and serve the stored object, on the moduledoc's argument that the
        # stored-object path "is still correct, it just recovers less". By here a replica was
        # chosen as strictly ahead of the object, so "less" is quorum-acked writes, and this owner
        # then writes over them at a higher lineage that outranks every replica still holding them.
        # Jepsen hit it on a MinIO pause: the pre-promotion snapshot or the publish timed out, the
        # takeover served the object, and the acked tail was gone for good.
        #
        # So it takes the same window as an unreadable decision head (expert review 2026-10-13 #15):
        # refuse the open inside the under-quorum window, so the client retries and the retry runs
        # recovery again, and fall back to the object, logged as an error, only after it. With no
        # peers configured `error_window/3` answers `:none` and the open proceeds as before.
        {:error, reason} ->
          Logger.warning(
            "shard #{shard_id}: replica promotion failed (#{inspect(reason)}); refusing the " <>
              "open for the under-quorum window rather than serving the older stored object"
          )

          refuse_or_etag(Recovery.error_window(shard_id, {:promotion_failed, reason}), path, etag)
      end
    after
      Enum.each(["", "-wal", "-shm"], &Fathom.FS.rm(temp <> &1))
    end
  end

  # PUBLISH WITH THE PRE-PROMOTION `.db` ASIDE (expert review 2026-10-13 #22). `Storage.flush`
  # publishes the promoted bytes, and the pulled `.db` used to be removed only AFTER it: a crash in
  # between left that `.db` with the OLD sidecar against the NEW object etag, which the next open
  # reads as `:diverged` and quarantines as a fork (a shard-sized `.forked` plus the alarm) though
  # nothing was lost. Now the file (and its -wal/-shm) is renamed to `<path>.prepromote` before the
  # PUT, so a crash leaves no live `.db` and the next open is a plain cold pull of the published
  # object; `TempReaper` sweeps the aside copy. It is renamed BACK when the PUT fails or raises,
  # because the fallback serves the pulled object. Two other prescriptions are wrong: "delete before
  # the flush" breaks that fallback, and `pre_put: flush_intent_opts(path)` would record the intent
  # of the WRONG bytes (the pulled file, not the temp being published). The aside is deleted by the
  # caller once the sidecar and the rename have landed.
  defp publish_promoted(shard_id, path, temp, etag, position, lineage_store) do
    with :ok <- aside_pre_promotion(path) do
      result =
        try do
          Storage.flush(shard_id, temp, etag, position, lineage_store)
        catch
          kind, reason ->
            restore_pre_promotion(path)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      case result do
        {:ok, _new_etag, _carried} ->
          after_promote_publish(path)
          result

        other ->
          restore_pre_promotion(path)
          other
      end
    end
  end

  # A PUBLISH THAT FAILS UN-FENCES THE REPLICA (expert review 2026-11-07 #3). `Promote.stage/4` fenced
  # it (torn, lineage raised to this owner's) so the deposed owner's pushes could not land under the
  # copy, and nothing ever lifted that fence when the publish PUT then failed (a store pause, a 412
  # because the object moved, ENOSPC): the refused open's retry met a replica torn at the NEW lineage
  # over the OLD bytes, ranked it nowhere and, with the peer's L1 seed refused as "below the replica's
  # lineage", could not refill it either — the Jepsen 2026-10-24 refuse-and-retry converted "serve
  # stale now" into "serve stale after one window". The PRIOR row is restored, not a torn-cleared row
  # at the raised lineage: that would claim the new lineage over old bytes and out-rank a peer that
  # holds more of the old lineage (`Follower.unfence/3`). A raise inside the publish restores too.
  # Only a failure BEFORE the object changed restores: past a successful publish the replica's tail
  # is the object, and the local-rename failure (which raises, abandoning the open) leaves the fence.
  defp publish_or_unfence(follower, shard_id, fence, publish) do
    case publish.() do
      {:ok, _new_etag, _carried} = ok ->
        ok

      other ->
        unfence_after_failed_publish(follower, shard_id, fence)
        other
    end
  catch
    kind, reason ->
      unfence_after_failed_publish(follower, shard_id, fence)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp unfence_after_failed_publish(follower, shard_id, fence) do
    case Follower.unfence(follower, shard_id, fence) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "shard #{shard_id}: could not restore the replica after a failed promotion publish " <>
            "(#{inspect(reason)}); it stays fenced and torn until a peer pull or a re-seed"
        )
    end
  end

  defp prepromote(path), do: path <> ".prepromote"

  # -wal/-shm first, the db last: a crash part-way leaves a db (still matching its sidecar) rather
  # than a wal with no db. A missing db (nothing was pulled) is fine; any other failure declines the
  # promotion before anything is published.
  defp aside_pre_promotion(path) do
    Enum.each(["-wal", "-shm"], &Fathom.FS.rename(path <> &1, prepromote(path) <> &1))

    case Fathom.FS.rename(path, prepromote(path)) do
      :ok ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        restore_pre_promotion(path)
        {:error, {:aside_failed, reason}}
    end
  end

  defp restore_pre_promotion(path) do
    for suffix <- ["", "-wal", "-shm"] do
      Fathom.FS.rename(prepromote(path) <> suffix, path <> suffix)
    end

    :ok
  end

  # Test seam for the crash window between the publish and the sidecar write (expert review
  # 2026-10-13 #22). Unset in every real config.
  defp after_promote_publish(path) do
    case Application.get_env(:fathom, :promote_replica_after_publish_hook) do
      nil -> :ok
      hook -> hook.(path)
    end
  end

  # Test seam for the crash window between the sidecar write and the rename (expert review
  # 2026-10-12 #22), mirroring `:promote_pull_crash_hook` in `Materializer`. Unset in every real
  # config, so this is one env read per promotion.
  defp before_promote_rename(path) do
    case Application.get_env(:fathom, :promote_replica_crash_hook) do
      nil -> :ok
      hook -> hook.(path)
    end
  end

  # THE PROMOTED OBJECT GETS A POSITION STAMP (expert review 2026-09-29 #9). The temp has been
  # checkpointed and closed, so its WAL reads `:empty`, and a synthetic state with no `:wal_ordinal`
  # fell through `Position.position_after_checkpoint/3` to `nil` — every promotion published an
  # object `Promote.fresher?/2` cannot rank against, so a SECOND failover before this coordinator's
  # first dirty flush could not promote anything, and this owner's quorum-acked writes were lost with
  # it. Carrying the replica's own ordinal takes the seed-on-known-short clause: the object is stamped
  # `{lineage, ordinal + 1, 0}`, strictly above every replica still at that ordinal (the object holds
  # all of it — it IS that replica, folded). `seed_ordinal_of/1` then starts this coordinator's
  # counter at `ordinal + 1`, so its first WAL numbers `ordinal + 2` and its own frames rank above
  # the stamp — which matters because in the canonical failover the new owner's lineage EQUALS the
  # replica's (expert review 2026-09-29 #3), so the ordinal is the only component that separates them.
  defp promoted_stamp_state(lease, temp, lineage, replica) do
    case replica_ordinal(replica) do
      0 -> %{lease: lease, path: temp, lineage: lineage}
      n -> %{lease: lease, path: temp, lineage: lineage, wal_ordinal: n}
    end
  end

  defp replica_ordinal(%{wal_ordinal: n}) when is_integer(n) and n > 0, do: n
  defp replica_ordinal(_replica), do: 0

  # Best-effort, and deliberately not fatal: a shard with no stored object yet has nothing to
  # snapshot, and a snapshot backend hiccup should not block a recovery that is otherwise sound.
  # It is attempted first precisely because it is the cheap insurance on the irreversible step.
  defp snapshot_before_promotion(shard_id) do
    case Fathom.Snapshots.create(shard_id, label: "pre-promotion", pre_promotion: true) do
      {:ok, _snapshot_id} ->
        stamp_promotion_snapshot(shard_id)
        :ok

      {:error, reason} ->
        Logger.warning(
          "shard #{shard_id}: could not snapshot before promoting a replica " <>
            "(#{inspect(reason)}); proceeding"
        )

        :ok
    end
  end

  # Expert review 2026-10-12 #8: record the promotion snapshot in the directory so the always-on
  # ReconcileJob can expire it. Async and best-effort — a Postgres write must never sit on the open
  # path, and a lost stamp only leaves the snapshot for an operator (as before).
  defp stamp_promotion_snapshot(shard_id) do
    Task.Supervisor.start_child(Fathom.TaskSupervisor, fn ->
      Fathom.Directory.stamp_promotion_snapshot(shard_id)
    end)
  catch
    :exit, _ -> :ok
  end
end
