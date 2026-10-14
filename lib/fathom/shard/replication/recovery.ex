defmodule Fathom.Shard.Replication.Recovery do
  @moduledoc """
  Survivor selection — finding the node that holds the freshest replica of a shard, and pulling it.
  Phase 2 A2. See `docs/a2-quorum-replication.md`.

  ## The gap this closes

  A2 replicates every commit to a quorum before acking, so at any moment several nodes hold writes
  the stored S3 object does not. `Fathom.Shard`'s promote-on-open then serves those writes — **if
  they happen to be on the node that took the shard over.** Nothing connected "which node holds a
  current replica" to "which node the LB fails over to", and the LB picks by consistent hash on the
  Host subdomain, which knows nothing about replication.

  So the headline claim held only by luck. Measured on the chaos rig 2026-08-11: an acked,
  quorum-replicated write was **lost** while three other nodes held it, because the survivor held
  none and cold-opened from S3. The replication was working perfectly; the recovery was reading the
  wrong copy.

  This is the piece Waterpark describes as "the replacement asks the followers and adopts the state
  of the best reader", and fathom can do it without the mailroom that was rejected in scoping — that
  rejection was about moving *streams* (Hrana batons are entry-node-local), and A2 already carries
  its own socket for exactly this kind of traffic.

  ## Four steps, three of which already existed

  | step | what runs it |
  |---|---|
  | **Ask** every peer where its replica sits | `Protocol.encode_position_query/1` (new) |
  | **Choose** the best copy | `choose/3` here, over `Promote.fresher?/2` (existing) |
  | **Pull** it | `seed_begin`/`seed_chunk`/`seed_end` **in reverse** (existing frames) |
  | **Publish** it | `Fathom.Shard`'s promote path, unchanged |

  The pulled bytes land through `Follower`'s own seed sink, into that node's replica directory and
  ETS row, so they are **indistinguishable from a replica this node had been following all along.**
  That is the reason the promote path needs no new case and no new provenance story: it already
  knows how to turn a local replica into a served database, snapshot first, verify with
  `quick_check` and publish under the lease fence.

  ## Safer than Waterpark's version, in the one direction that matters

  Waterpark is RAM-only: the best reader is the *only* copy, so it is adopted whatever it says.
  Fathom has the stored object underneath, so a peer's replica is adopted **only when it is
  provably ahead of that object** — the same `Promote.fresher?/2` test the local path uses, with
  the same rules:

    * an object with **no position stamp is never overridable**, so this is inert for a shard until
      its next flush after upgrading;
    * `>` and not `>=`, so an equal position leaves the object (which has provenance) in charge;
    * every uncertain answer is `false`.

  When no peer is provably ahead, `choose/3` returns `:none` and the open degrades to exactly
  today's behaviour. **There is no state in which this serves older bytes than the code without
  it.**

  ## Cost, and why it is gated separately

  Asking runs on the **cold-open path**, which is a measured hot path (`cold_open_p50_us`), and
  costs one concurrent round trip to each peer plus — only when a peer wins — a whole database
  transfer. That is the right trade during a failover and the wrong one on an ordinary open, which
  is why `:replication_recover_from_peers` is its own gate on top of
  `:replication_promote_on_open`, and why the query round is bounded by
  `:replication_recovery_timeout_ms` rather than by TCP.

  It defaults **on** (2026-08-25) despite that cost, because the cost is not paid by a node that
  cannot use it: the gate below is `and` -ed with `:replication_listen`, which is off by default, so
  a node outside a replicating fleet never opens a socket here. What defaulting on buys is that a
  fleet which turns listening and shipping on gets the whole RPO story instead of the
  "~0-if-lucky" half a forgotten fourth flag leaves behind.

  **The local replica is checked first and short-circuits the network entirely** — a node that
  already holds the freshest copy asks nobody.

  ## This node must be listening

  The pull installs through the local `Follower`'s sink, so a node with `:replication_listen` off
  has no replica directory and no ETS table to install into, and recovery declines with a log line
  rather than inventing a second install path. That matches the documented rollout order (listening
  on fleet-wide first) and is not a limitation worth engineering around: a node that cannot hold a
  replica cannot be a useful survivor for anyone else either.
  """

  require Logger

  alias Fathom.Shard.Replication.{Fleet, Follower, FollowerLog, Promote, Protocol}
  alias Fathom.Shard.Replication.Recovery.UnderQuorum

  @connect_timeout 2_000
  @query_timeout_ms 2_000
  @pull_timeout_ms 60_000

  @typedoc "A peer to ask: `{node_key, host, port}`, the shape `Fleet` publishes."
  @type endpoint :: {String.t(), String.t() | charlist(), :inet.port_number()}

  @typedoc """
  What a peer says about its replica — `FollowerLog.t()` itself, not a restatement of it.

  It used to spell out the four positional fields under a comment claiming "the same shape as
  `FollowerLog.t()`", and then drifted: the 2026-08-12 torn-replica fix added `torn` to
  `FollowerLog.t()` and never added it here. So the type asserted equivalence while omitting the
  one field that decides whether a replica may be promoted AT ALL — a torn replica is never
  fresher than anything (`Promote.fresher?/2`), which is exactly the rule that stopped a tenant
  being served an empty database. Aliasing rather than copying makes that drift impossible.
  """
  @type position :: FollowerLog.t()

  @doc """
  Is peer recovery switched on? (`:replication_recover_from_peers`, default `true` since
  2026-08-25.)
  """
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :replication_recover_from_peers, true) == true

  @doc """
  Is there anything to ask — does this node run a `Follower` (so a pulled replica has somewhere to
  install) and does the fleet publish at least one peer?

  **A COST gate, not a correctness one**, and it is what makes defaulting `enabled?/0` on affordable.
  The fleet path's first act in `Fathom.Shard` is `Storage.object_head/1` — an object-store round
  trip on the cold-open path — while the two conditions that make this whole search return `:none`
  are a `Process.whereis` and a `:persistent_term` read. Without this check, the flipped default
  would have put one HEAD on **every** cold open of **every** node, including nodes with no peers
  and no listener where recovery can never do anything.

  **False here does NOT mean "take the local path"**, and that distinction cost a test failure
  before it was written down. `best_replica/3` short-circuits on this node's own replica *before*
  opening a socket, so a node with a local replica and no peers still has a real fleet decision to
  make — and only that path performs the mid-flight object re-check. The caller
  (`Fathom.Shard.nothing_to_promote?/1`) therefore pairs this with "no local replica either", and
  skips the whole thing only when both are true, which is precisely when `best_replica/3` would
  have returned `:none`.
  """
  @spec fleet_reachable?(atom() | pid()) :: boolean()
  def fleet_reachable?(follower \\ Follower), do: listening?(follower) and peers() != []

  @doc """
  Which copy of this shard should be served: ours, a peer's, or the stored object's.

  **Pure**, and that is deliberate for the same reason `Promote.fresher?/2` and
  `FollowerLog.decide/2` are: this picks one lineage of a tenant's database and discards the
  others. Getting it wrong loses acknowledged writes, and it must be reachable from a plain unit
  test rather than only from a failover.

    * `:none` — nothing is provably ahead of the stored object. Open normally.
    * `:local` — this node's own replica is the best copy. The existing promote path handles it,
      with no network at all.
    * `{:pull, endpoint, position}` — a peer holds the best copy.

  `offers` is `[{endpoint, position | nil}]`; a peer that holds nothing, could not be reached, or
  speaks an older protocol simply contributes `nil` and is skipped. **Silence is never taken as
  agreement**: an unreachable fleet yields `:none`, which is the stored object, which is the
  pre-A2 behaviour.

  Ties go to the local replica (no transfer for no gain) and are then broken by `node_key`, so two
  nodes recovering the same shard make the same choice rather than each picking a different peer.
  """
  @spec choose(position() | nil, map() | nil, [{endpoint(), position() | nil}]) ::
          :none | :local | {:pull, endpoint(), position()}
  def choose(local, object_stamp, offers) do
    ahead = Enum.filter(offers, fn {_endpoint, pos} -> Promote.fresher?(pos, object_stamp) end)

    case ahead do
      [] ->
        if Promote.fresher?(local, object_stamp), do: :local, else: :none

      _ ->
        {endpoint, pos} = Enum.max_by(ahead, fn {{key, _h, _p}, pos} -> {rank(pos), key} end)

        # `>=`, so an equal peer does not cost a database transfer. Transitivity does the rest: the
        # winner is already strictly ahead of the object, so a local copy at least equal to it is
        # too, and `:local` is safe without re-testing.
        if local && rank(local) >= rank(pos), do: :local, else: {:pull, endpoint, pos}
    end
  end

  # RANKS ON THE LINEAGE, the same key `Promote.fresher?/2` uses (expert review 2026-08-24 #12).
  # This used to read `epoch`, which on a replica is the primary's LOCK epoch — reset to 1 on every
  # clean release — so peers were ordered against each other by a counter that carries no ordering.
  # `choose/3` has already filtered these by `fresher?/2` against the object, so ranking them by a
  # different key than the filter used was incoherent as well as wrong.
  #
  # Falls back to `epoch` for a map that has no lineage: a `local` read from an older follower
  # state, or a peer one deploy behind whose offer decodes with both keys set from the one wire
  # field. Same value in both cases, so the fallback changes nothing where it fires.
  #
  # THE SECOND COMPONENT IS THE ORDINAL (expert review 2026-08-26 #2, step 3b), for the same reason
  # `fresher?/2` swapped: `wal_gen` restarts at 0 on a WAL recreate, so ranking peers by it could
  # order two unrelated WALs. Ranking by a different key than the filter used was already called
  # out as incoherent above; it still applies, so this moves with it.
  #
  # A peer with no ordinal ranks at 0, which loses to any peer that has one — and cannot be reached
  # in practice anyway, because `choose/3` filtered these through `fresher?/2`, which refuses a
  # replica whose ordinal is unstated.
  defp rank(%{lineage: l, next_offset: o} = pos) when is_integer(l), do: {l, ordinal(pos), o}
  defp rank(%{epoch: e, next_offset: o} = pos), do: {e, ordinal(pos), o}

  defp ordinal(%{wal_ordinal: n}) when is_integer(n), do: n
  defp ordinal(_), do: 0

  @typedoc "One read of the stored object: what it is, and what it claims. `nil` means no object."
  @type head :: %{etag: String.t() | nil, position: map() | nil} | nil

  @doc """
  Is the promotion decision still true, now that the transfer is finished?

  `choose/3` is answered against a head read BEFORE the peer query and the pull, and those two
  phases are bounded by `:replication_recovery_timeout_ms` and
  `:replication_recovery_pull_timeout_ms` — seconds, and on a large shard the pull is however long
  the database takes to cross the network. Any flush landing in that window makes the comparison a
  statement about a version of the object that no longer exists.

  Nothing was ever UNSAFE about that: the publish is `If-Match`-fenced on the etag we hold, so a
  moved object turns into a 412 and the promotion is abandoned. What it cost was everything before
  the 412 — a whole-database transfer, a pre-promotion snapshot, and a log line reading "pulled a
  replica … the stored object was behind it" about an object that had since moved ahead of it.

  **Pure**, like `choose/3` and `Promote.fresher?/2`, and for the same reason: it decides whether a
  tenant's stored database gets overwritten, so it has to be reachable from a plain unit test
  rather than only from a failover with a concurrent writer.

  Two distinct failures, because they are two different facts going stale:

    * `{:error, {:object_moved, was, now}}` — the object is not the version we compared against, so
      the etag we would fence with is stale and the publish could not land anyway.
    * `{:error, {:object_advanced, position}}` — the object is the same version by etag but its
      stamp is no longer behind the replica. **This is reachable, not a paranoid branch:** S3's
      etag hashes the BODY and the stamp is user metadata, so a re-flush of byte-identical bytes
      carrying an advanced position keeps the etag. Etag equality alone would call that unchanged
      and promote over a newer claim.
  """
  @spec recheck(head(), head(), position() | nil) :: :ok | {:error, term()}
  def recheck(before, now, replica) do
    cond do
      etag_of(now) != etag_of(before) ->
        {:error, {:object_moved, etag_of(before), etag_of(now)}}

      not Promote.fresher?(replica, position_of(now)) ->
        {:error, {:object_advanced, position_of(now)}}

      true ->
        :ok
    end
  end

  defp etag_of(nil), do: nil
  defp etag_of(%{etag: etag}), do: etag

  defp position_of(nil), do: nil
  defp position_of(%{position: position}), do: position

  @doc """
  Find and install the freshest replica of `shard_id` available anywhere in the fleet.

  Returns `{:ok, replica_state}` when this node now holds a replica that is strictly ahead of the
  stored object — whether it already did, or just pulled one — and `:none` otherwise. The caller
  (`Fathom.Shard`'s cold open) treats `:none` as "open from the stored object", which is what it
  did before this existed.

  `{:refuse, %UnderQuorum{}}` when too few peers answered to rule out a fresher replica and the
  shard's under-quorum window is still open — see "The responder floor" below. The caller refuses
  the open with a retryable error.

  Options:

    * `:follower` — the local `Follower` instance (default `Follower`).
    * `:peers` — override the peer set, for tests.
    * `:quorum` — override `:replication_quorum`, for tests.
    * `:query_timeout_ms` / `:pull_timeout_ms` — bounds for the two network phases.

  **Never raises.** This runs inside a shard's open, and a failure to improve on the stored object
  must not fail the open — the ordinary path is still correct, it just recovers less.

  ## The responder floor (expert review 2026-10-12 #5)

  A write is acked once Q of the primary's N followers hold it, so only a read of at least
  `N - Q + 1` of those N is guaranteed to include one of them. This used to accept whatever subset
  answered within the query timeout: under a partial partition, two silent peers out of four with
  Q=2 was enough to return `:none`, open from the object, stamp a higher lineage at the first
  flush, and reseed the replicas holding the acked tail away. Acked writes lost, with no signal.

  So recovery counts DEFINITE answers — "I have this position" and "I have nothing" both count; a
  timeout, refused connect, skipped-unreachable peer or malformed reply does not — plus this node's
  own replica table (it is listening, so its answer is definite too). Below the floor the open is
  refused for a bounded per-shard window (`:replication_recovery_underquorum_window_ms`, default
  60 s) so the client retries while the partition heals. After it, the open proceeds best-effort
  over the answers it has (never older than the stored object), with an error log and a
  `[:fathom, :replication, :recovery_underquorum]` event. This is the user's decision on the
  2026-10-12 review (Q1 = A): availability is traded for at most one window, never indefinitely.

  N is this node's peer set and Q this node's `:replication_quorum` — the symmetric-fleet
  assumption `peers/0` already makes (every node follows every other, one fleet-wide quorum). The
  floor applies only when the object's stamp is RANKABLE: against an unstamped object nothing can
  ever be promoted, so no answer could change the outcome, and a brand-new shard is never refused.
  With no peers, or with `Q >= N` (a misconfiguration the shipping side refuses at boot), the floor
  is at most 1, which this node's own answer meets, so those cases are unchanged.
  """
  @spec best_replica(String.t(), map() | nil, keyword()) ::
          {:ok, position()} | :none | {:refuse, UnderQuorum.t()}
  def best_replica(shard_id, object_stamp, opts \\ []) do
    follower = Keyword.get(opts, :follower, Follower)
    local = local_state(follower, shard_id)
    peers = Keyword.get(opts, :peers) || peers()
    needed = answers_needed(peers, object_stamp, opts)

    # Short-circuit before any socket: a node that already holds a copy ahead of the object has
    # nothing to gain from the fleet — but only when this node's answer alone meets the responder
    # floor. Above it, a local replica ahead of the object can still be BEHIND a peer that acked
    # writes this node never received, and adopting it loses them exactly as the object would.
    if needed <= 1 and Promote.fresher?(local, object_stamp) do
      {:ok, local}
    else
      search(shard_id, object_stamp, local, follower, peers, needed, opts)
    end
  rescue
    e -> error_window(shard_id, {:crashed, Exception.message(e)}, opts)
  catch
    kind, reason -> error_window(shard_id, {:exited, {kind, reason}}, opts)
  end

  @doc """
  An INTERNAL error during the recovery decision (a crash in `best_replica/3`, or the decision-time
  object HEAD failing), routed through the same under-quorum window as too few answers (expert
  review 2026-10-13 #15, refining 2026-10-12 Q1 = A).

  These used to degrade to `:none`, i.e. open from the stored object with no floor and a warning.
  Internal errors coincide with the S3 brownouts that accompany partitions, so the floor failed open
  exactly when it mattered. Now: while peers exist, refuse inside the window (`{:refuse, _}`, 503),
  and after it open best-effort (`:none`) with an error-level log. No peers means no floor to apply
  and `:none`. Never raises. The event's `outcome` is `:skipped_error` inside the window.
  """
  @spec error_window(String.t(), term(), keyword()) :: :none | {:refuse, UnderQuorum.t()}
  def error_window(shard_id, reason, opts \\ []) do
    peers = Keyword.get(opts, :peers) || peers()

    if peers == [] do
      Logger.warning("replication recovery for #{shard_id} failed: #{inspect(reason)}")
      :none
    else
      # Informational only (the struct and the event), so a bad quorum must not take the error
      # path down with the crash it is handling.
      needed =
        try do
          max(length(peers) - Keyword.get(opts, :quorum, quorum()) + 1, 1)
        rescue
          _ -> length(peers)
        end

      case underquorum_step(shard_id) do
        {:wait, waited} ->
          refusal = %UnderQuorum{
            shard_id: shard_id,
            answered: 0,
            needed: needed,
            waited_ms: waited
          }

          Logger.error(
            "shard #{shard_id}: replication recovery failed (#{inspect(reason)}); refusing the " <>
              "open for the under-quorum window so the floor is not skipped on an error"
          )

          emit_underquorum(refusal, :skipped_error)
          {:refuse, refusal}

        {:expired, waited} ->
          Logger.error(
            "shard #{shard_id}: replication recovery still failing (#{inspect(reason)}) after " <>
              "#{waited} ms; opening best-effort from the stored object. Quorum-acked writes " <>
              "held only by peers may be lost."
          )

          emit_underquorum(
            %UnderQuorum{shard_id: shard_id, answered: 0, needed: needed, waited_ms: waited},
            :opened_best_effort
          )

          :none
      end
    end
  rescue
    e ->
      # The error path itself failed. Nothing left to protect with; stay open.
      Logger.error("replication recovery error handling for #{shard_id} crashed: #{inspect(e)}")
      :none
  end

  @doc """
  How many definite answers (this node's own included) recovery needs before it may decide:
  `N - Q + 1`, or 0 when the stored object's stamp cannot be ranked (nothing could be promoted over
  it anyway). Pure, for the test.
  """
  @spec answers_needed([endpoint()], map() | nil, keyword()) :: number()
  def answers_needed(peers, object_stamp, opts \\ []) do
    if rankable_stamp?(object_stamp) do
      length(peers) - Keyword.get(opts, :quorum, quorum()) + 1
    else
      0
    end
  end

  # Mirrors the clauses of `Promote.fresher?/2` that refuse on the STAMP side: nil, lineage 0 and an
  # unstated ordinal answer false for every replica, so no peer's answer could matter.
  defp rankable_stamp?(%{epoch: e, wal_ordinal: n})
       when is_integer(e) and e > 0 and is_integer(n) and n > 0,
       do: true

  defp rankable_stamp?(_stamp), do: false

  defp quorum, do: Application.get_env(:fathom, :replication_quorum, 2)

  defp search(shard_id, object_stamp, local, follower, peers, needed, opts) do
    cond do
      peers == [] ->
        :none

      not listening?(follower) ->
        Logger.warning(
          "shard #{shard_id}: a peer may hold a fresher replica, but this node is not running a " <>
            "replication follower (:replication_listen) and has nowhere to install one"
        )

        :none

      true ->
        answers = ask(peers, shard_id, query_timeout_ms(opts), true)

        if answered(answers, opts) >= needed do
          settle(shard_id, object_stamp, local, follower, answers, needed, opts)
        else
          under_quorum(shard_id, object_stamp, local, follower, peers, needed, answers, opts)
        end
    end
  end

  # Enough answers: any under-quorum episode is over, and the choice is the ordinary one.
  defp settle(shard_id, object_stamp, local, follower, answers, needed, opts) do
    if needed > 1, do: clear_underquorum(shard_id)
    act(shard_id, choose(local, object_stamp, offers(answers)), local, follower, opts)
  end

  defp under_quorum(shard_id, object_stamp, local, follower, peers, needed, answers, opts) do
    case underquorum_step(shard_id) do
      {:wait, waited} ->
        refusal = %UnderQuorum{
          shard_id: shard_id,
          answered: answered(answers, opts),
          needed: needed,
          waited_ms: waited
        }

        Logger.warning(Exception.message(refusal))
        emit_underquorum(refusal, :refused)
        {:refuse, refusal}

      {:expired, waited} ->
        # The last chance before deciding on a partial view: ask again WITHOUT the unreachable
        # cache. Inside the window the cache keeps each refusal fast (a blackholed peer would cost
        # every retry its full connect timeout) and its 10 s TTL re-dials a recovered peer well
        # within the window; here, one connect timeout is cheap next to losing an acked tail.
        answers = ask(peers, shard_id, query_timeout_ms(opts), false)

        if answered(answers, opts) >= needed do
          settle(shard_id, object_stamp, local, follower, answers, needed, opts)
        else
          giving_up = %UnderQuorum{
            shard_id: shard_id,
            answered: answered(answers, opts),
            needed: needed,
            waited_ms: waited
          }

          Logger.error(
            "shard #{shard_id}: peer recovery still has only #{giving_up.answered} of the " <>
              "#{needed} definite answers it needs after #{waited} ms; opening best-effort from " <>
              "what answered (the stored object unless a peer is provably ahead of it). " <>
              "Quorum-acked writes held only by the silent peers may be lost."
          )

          emit_underquorum(giving_up, :opened_best_effort)
          act(shard_id, choose(local, object_stamp, offers(answers)), local, follower, opts)
        end
    end
  end

  defp act(_shard_id, :none, _local, _follower, _opts), do: :none
  defp act(_shard_id, :local, local, _follower, _opts), do: {:ok, local}

  defp act(shard_id, {:pull, endpoint, pos}, _local, follower, opts),
    do: pull(shard_id, endpoint, pos, follower, opts)

  # `outcome` is a bounded set (`:refused` | `:opened_best_effort` | `:skipped_error`), so it is safe as a metric tag.
  defp emit_underquorum(%UnderQuorum{} = e, outcome) do
    :telemetry.execute(
      [:fathom, :replication, :recovery_underquorum],
      %{count: 1, answered: e.answered, needed: e.needed, waited_ms: e.waited_ms},
      %{shard_id: e.shard_id, outcome: outcome}
    )
  end

  # This node's own replica table is one definite answer: `search/7` only asks when it is
  # listening, so a nil there means "I have nothing", not "I could not tell".
  #
  # THE PREVIOUS OWNER DOES NOT COUNT (expert review 2026-10-13 #14). `peers/0` is every other node,
  # so it includes the node whose lease we just stole, but that node is not one of the followers the
  # writer's quorum was taken over: a lapsed-but-answering P saying "nothing" would otherwise stand
  # in for a follower and shrink the read to N-Q real followers. Its OFFER (if it holds a position)
  # is still honoured by `offers/1`; it only stops counting toward the floor. `needed` is unchanged.
  defp answered(answers, opts) do
    prev = Keyword.get(opts, :previous_owner)

    1 +
      Enum.count(answers, fn
        {{key, _h, _p}, {:answered, _}} -> not previous_owner?(prev, key)
        _ -> false
      end)
  end

  @doc """
  Is `node_key` the node a lease `owner` string (`"<node name>#<incarnation>"`) names? Pure.

  The codebase's convention for mapping an owner to a fleet node (`RemoteFlush`, `ShardMigration`)
  is the name before the `#`. A release's `node()` is `name@host`, so the host part is accepted too:
  with `NODE_KEY` equal to the container hostname the two coincide. A deployment where neither
  matches simply gets no exclusion, i.e. the pre-fix behaviour.
  """
  @spec previous_owner?(String.t() | nil, String.t()) :: boolean()
  def previous_owner?(owner, key) when is_binary(owner) do
    [name | _] = String.split(owner, "#", parts: 2)
    name != "" and (name == key or List.last(String.split(name, "@", parts: 2)) == key)
  end

  def previous_owner?(_owner, _key), do: false

  defp offers(answers) do
    Enum.map(answers, fn
      {endpoint, {:answered, pos}} -> {endpoint, pos}
      {endpoint, :silent} -> {endpoint, nil}
    end)
  end

  # -- the under-quorum window -----------------------------------------------------------------
  #
  # PER SHARD, PER NODE, and anchored at the FIRST refusal so retries do not extend it: each retry
  # is a fresh coordinator that asks again and fails fast, and this row is the only state that spans
  # them. Per node is enough because the LB hashes a shard to one node; a shard that moves starts a
  # window there, which can only add one more bounded wait.
  #
  # A row nobody has touched for two windows is a PAST episode, not this one — otherwise a shard
  # refused once and then left alone for an hour would skip the wait entirely at its next failover.
  @underquorum_table __MODULE__.UnderQuorumWindow

  @doc """
  One step of a shard's under-quorum window. Pure, for the test.

  `entry` is `{first_ms, last_ms}` from the previous refusal, `{:expired, at_ms}` (the tombstone
  left when the window expired), or nil; `now` a monotonic millisecond reading. Returns
  `{:wait, waited_ms, entry}` (refuse, and store `entry`) or `{:expired, waited_ms}` (stop refusing).

  THE EXPIRY LEAVES A TOMBSTONE for one window (expert review 2026-10-13 #40). The `:expired` branch
  used to delete the row before the best-effort open ran, so if that open failed (a transient S3
  error, a lost race) the retry found no row and started a FRESH 60 s refusal: the window was
  re-armed by exactly the failure it was meant to bound. While the tombstone stands a retry is
  `:expired` again; one window after the expiry it is a past episode, and a new failover starts a
  new window.
  """
  @spec window_step(
          {integer(), integer()} | {:expired, integer()} | nil,
          integer(),
          integer()
        ) ::
          {:wait, integer(), {integer(), integer()}} | {:expired, integer()}
  def window_step(nil, now, window_ms), do: window_start(now, window_ms)

  def window_step({:expired, at}, now, window_ms) when now - at <= window_ms,
    do: {:expired, now - at}

  def window_step({:expired, _at}, now, window_ms), do: window_start(now, window_ms)

  def window_step({_first, last}, now, window_ms) when now - last > 2 * window_ms,
    do: window_start(now, window_ms)

  def window_step({first, _last}, now, window_ms) when now - first >= window_ms,
    do: {:expired, now - first}

  def window_step({first, _last}, now, _window_ms), do: {:wait, now - first, {first, now}}

  defp window_start(_now, window_ms) when window_ms <= 0, do: {:expired, 0}
  defp window_start(now, _window_ms), do: {:wait, 0, {now, now}}

  # A MISSING TABLE FAILS OPEN (best-effort, still logged at error by the caller). The table is
  # owned by `Fleet`, which also supervises the `Follower` this path requires, so it is absent only
  # in a harness that started a follower by hand; refusing forever there is the worse failure.
  defp underquorum_step(shard_id) do
    now = System.monotonic_time(:millisecond)

    entry =
      case :ets.lookup(@underquorum_table, shard_id) do
        [{_, first, last}] -> {first, last}
        [] -> nil
      end

    case window_step(entry, now, underquorum_window_ms()) do
      {:wait, waited, {first, last}} ->
        if entry == nil, do: maybe_sweep(now)
        :ets.insert(@underquorum_table, {shard_id, first, last})
        {:wait, waited}

      {:expired, waited} ->
        # Tombstone, not delete (see `window_step/3`). An already-tombstoned row keeps its original
        # instant, so retries do not extend it; window <= 0 means no window, so nothing to remember.
        if not match?({:expired, _}, entry) and underquorum_window_ms() > 0 do
          :ets.insert(@underquorum_table, {shard_id, :expired, now})
        end

        {:expired, waited}
    end
  rescue
    ArgumentError -> {:expired, 0}
  end

  # RECLAIMING ROWS (expert review 2026-10-13 #40). A row was deleted only on settle or expiry, so a
  # shard refused once and never retried (client gave up, shard deleted) left its row forever; the
  # `Unreachable` marks leaked the same way. The table is owned by the `Fleet` SUPERVISOR, which has
  # no mailbox to hang a timer on, and a new process for this is not worth it: rows are only ever
  # ADDED by a refusal, so the sweep runs there, when a new row is inserted and the table has grown
  # past a threshold. Growth is therefore bounded by the threshold plus the rows of one live burst.
  @sweep_threshold 256

  defp maybe_sweep(now) do
    if :ets.info(@underquorum_table, :size) >= @sweep_threshold, do: sweep(now)
    :ok
  end

  @doc false
  # Drop under-quorum rows (and tombstones) untouched for two windows, and expired unreachable
  # marks. Returns how many rows went. Public for the test.
  @spec sweep(integer()) :: non_neg_integer()
  def sweep(now \\ System.monotonic_time(:millisecond)) do
    cutoff = now - 2 * underquorum_window_ms()

    :ets.select_delete(@underquorum_table, [
      {{:_, :"$1", :"$2"}, [{:is_integer, :"$1"}, {:<, :"$2", cutoff}], [true]},
      {{:_, :expired, :"$1"}, [{:<, :"$1", cutoff}], [true]}
    ]) + sweep_unreachable(now)
  rescue
    ArgumentError -> 0
  end

  defp clear_underquorum(shard_id) do
    :ets.delete(@underquorum_table, shard_id)
  rescue
    ArgumentError -> true
  end

  defp underquorum_window_ms,
    do: Application.get_env(:fathom, :replication_recovery_underquorum_window_ms, 60_000)

  defp query_timeout_ms(opts), do: Keyword.get(opts, :query_timeout_ms, query_timeout_ms())

  @doc """
  The nodes to ask: the live shipper set when this node is replicating, else the configured list.

  Deliberately the membership set rather than a new discovery mechanism. In a symmetric fleet every
  node follows every other, so the nodes this node would ship to are exactly the nodes that were
  following the one that died. It is also the only peer set that is already maintained, validated
  and swapped safely (`Membership`); inventing a second one would give recovery a different view of
  the fleet from replication, and the two disagreeing is worse than either being slightly stale.
  """
  @spec peers() :: [endpoint()]
  def peers do
    case Fleet.running() do
      [] -> Fleet.endpoints()
      running -> Enum.map(running, fn {key, host, port, _name} -> {key, host, port} end)
    end
  end

  # -- ask -------------------------------------------------------------------------------------

  # Every peer at once, bounded by one shared timeout. Sequentially this would be N round trips on
  # the cold-open path; concurrently it is one, which is what makes asking affordable enough to do
  # on every promote-eligible open rather than only when an operator suspects a failover.
  #
  # Each peer answers `{:answered, position | nil}` — a definite reply, "nothing" included — or
  # `:silent`, which the responder floor does not count (expert review 2026-10-12 #5). A peer that
  # timed out, refused, crashed or was skipped as unreachable is silent.
  defp ask(peers, shard_id, timeout_ms, honor_unreachable?) do
    peers
    |> Task.async_stream(&ask_one(&1, shard_id, timeout_ms, honor_unreachable?),
      timeout: timeout_ms + @connect_timeout,
      on_timeout: :kill_task,
      max_concurrency: max(length(peers), 1),
      ordered: false
    )
    |> Enum.flat_map(fn
      {:ok, {endpoint, answer}} -> [{endpoint, answer}]
      # A peer that timed out, refused, or crashed contributes nothing. NOT an error: the whole
      # point of a quorum is that some nodes are gone, and a failover is when that is most likely.
      # It is also not an answer, which is what the responder floor counts.
      _ -> []
    end)
  end

  defp ask_one({_key, host, port} = endpoint, shard_id, timeout_ms, honor_unreachable?) do
    if honor_unreachable? and unreachable?(endpoint) do
      {endpoint, :silent}
    else
      case connect(host, port) do
        {:ok, sock} ->
          reachable(endpoint)

          try do
            {endpoint, query(sock, shard_id, timeout_ms)}
          after
            :gen_tcp.close(sock)
          end

        {:error, reason} ->
          note_unreachable(endpoint, reason)
          {endpoint, :silent}
      end
    end
  end

  # -- unreachable peers -----------------------------------------------------------------------
  #
  # A BLACKHOLED PEER COST EVERY COLD OPEN ITS FULL CONNECT TIMEOUT (expert review 2026-10-01 #3c).
  # `ask/3` dials every peer on every promote-eligible open, and a peer that drops SYNs (paused,
  # partitioned, its host gone) answers nothing until `@connect_timeout` — so during a failover,
  # exactly when the opens come in a burst, each one waited ~2 s on the same dead node.
  #
  # So a connect that TIMED OUT marks the endpoint unreachable for `unreachable_ms/0` (10 s), and
  # opens in that window skip it. Deliberately ONLY a timeout:
  #
  #   * A refused or reset connect already fails in microseconds; skipping it saves nothing, and
  #     it is what a peer mid-restart returns — the peer most likely to come back holding the
  #     freshest replica a moment later.
  #   * Skipping trades RPO for latency: a skipped peer may hold the freshest copy, and the open then
  #     falls back to the stored object (correct, recovers less). That is why it is not keyed off the
  #     shipper's connection status, which goes `:disconnected` on any blip (option A in the review);
  #     a peer has to have actually swallowed a full connect timeout to be skipped, and only for
  #     seconds. A successful connect clears the mark.
  @unreachable_table __MODULE__.Unreachable

  @doc false
  # Owned by `Fleet`'s supervisor, created before any shard can open. Idempotent for tests.
  @spec init_table() :: :ok
  # Also creates the per-shard under-quorum window table (expert review 2026-10-12 #5), each
  # independently so an existing one does not stop the other being created.
  def init_table do
    for table <- [@unreachable_table, @underquorum_table] do
      try do
        :ets.new(table, [:named_table, :public, :set, read_concurrency: true])
      rescue
        ArgumentError -> table
      end
    end

    :ok
  end

  @doc false
  # Which connect failures mark a peer unreachable. Pure, for the test.
  @spec mark_unreachable?(term()) :: boolean()
  def mark_unreachable?(:timeout), do: true
  def mark_unreachable?(_reason), do: false

  @doc false
  @spec unreachable?(endpoint()) :: boolean()
  def unreachable?(endpoint) do
    case :ets.lookup(@unreachable_table, endpoint) do
      [{_, until}] -> System.monotonic_time(:millisecond) < until
      [] -> false
    end
  rescue
    ArgumentError -> false
  end

  @doc false
  @spec note_unreachable(endpoint(), term()) :: :ok
  def note_unreachable(endpoint, reason) do
    if mark_unreachable?(reason) do
      :ets.insert(
        @unreachable_table,
        {endpoint, System.monotonic_time(:millisecond) + unreachable_ms()}
      )
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp sweep_unreachable(now) do
    :ets.select_delete(@unreachable_table, [{{:_, :"$1"}, [{:<, :"$1", now}], [true]}])
  end

  defp reachable(endpoint) do
    :ets.delete(@unreachable_table, endpoint)
  rescue
    ArgumentError -> true
  end

  defp unreachable_ms,
    do: Application.get_env(:fathom, :replication_recovery_unreachable_ms, 10_000)

  defp query(sock, shard_id, timeout_ms) do
    with :ok <- :gen_tcp.send(sock, Protocol.encode_position_query(shard_id)),
         {:ok, bytes} <- :gen_tcp.recv(sock, 0, timeout_ms),
         {:ok, {:position, ^shard_id, pos}} when pos != :torn <- Protocol.decode(bytes) do
      {:answered, pos}
    else
      # A TORN replica answers `{:position, id, :torn}` (expert review 2026-10-13 #5): it may hold
      # the acked tail while hiding it, so it is neither a definite "nothing" nor an offer. Silent
      # is exactly that: counted toward neither `answered` nor `offers`.
      # Includes a peer one deploy behind, which answers `{:error, :malformed}` and closes: see the
      # note in `Protocol` on why these frames were added without bumping the version. It also
      # includes a reply naming a DIFFERENT shard, which is refused rather than trusted.
      _ -> :silent
    end
  end

  # -- pull ------------------------------------------------------------------------------------

  defp pull(shard_id, {key, host, port} = endpoint, promised, follower, opts) do
    timeout = Keyword.get(opts, :pull_timeout_ms, pull_timeout_ms())
    started = System.monotonic_time(:millisecond)

    case connect(host, port) do
      {:ok, sock} ->
        try do
          do_pull(sock, shard_id, endpoint, promised, follower, timeout, started)
        after
          :gen_tcp.close(sock)
        end

      {:error, reason} ->
        Logger.warning(
          "shard #{shard_id}: could not reach #{key} to pull a replica: #{inspect(reason)}"
        )

        :none
    end
  end

  defp do_pull(sock, shard_id, {key, _h, _p}, promised, follower, timeout, started) do
    deadline = started + timeout
    ordinal = Map.get(promised, :wal_ordinal, 0)

    with :ok <- :gen_tcp.send(sock, Protocol.encode_replica_request(shard_id)),
         {:ok, offset} <- receive_seed(sock, follower, shard_id, deadline, %{}, ordinal) do
      # THE PULLED REPLICA HAS NO ORDINAL OF ITS OWN (expert review 2026-08-26 #2, step 3b): a seed
      # installs `FollowerLog.seeded/5`'s state and `SeedBegin` carries none. Without this the
      # survivor would hold a replica `Promote.fresher?/2` can never rank, and recovery would end
      # exactly where it began — cold-opening the stale object. The offer this peer was CHOSEN on
      # is the only statement about these bytes we have, and taking it can only under-claim.
      # Stamped by the install itself, under the shard's lock (expert review 2026-10-10 #22):
      # `Follower.stamp_seed_ordinal/3` above, not a read-modify-write after the lock was released.
      installed = local_state(follower, shard_id)
      elapsed = System.monotonic_time(:millisecond) - started

      Logger.warning(
        "shard #{shard_id}: pulled a replica from #{key} at #{inspect(installed)} " <>
          "(promised #{inspect(promised)}, #{offset}B of WAL, #{elapsed}ms) — the stored object " <>
          "was behind it"
      )

      :telemetry.execute(
        [:fathom, :replication, :recovered_from_peer],
        %{count: 1, duration_ms: elapsed, wal_offset: offset},
        %{shard_id: shard_id, source: key}
      )

      {:ok, installed}
    else
      other ->
        Logger.warning("shard #{shard_id}: replica pull from #{key} failed: #{inspect(other)}")
        :none
    end
  end

  # Drives `Follower`'s own seed sink frame by frame. The sink is reused rather than reimplemented
  # because it is where a truncated or transposed stream becomes a corrupt tenant database, and a
  # second copy of that logic for the pull direction is the kind of duplicate that drifts silently.
  #
  # EVERY FRAME'S SHARD ID IS CHECKED against the one we asked for. A peer answering with another
  # tenant's bytes would otherwise install them under this shard's name — a cross-tenant leak, which
  # AGENTS.md treats as a release blocker rather than a finding. The port is unauthenticated, so
  # "our own peer would not do that" is not a property we get to assume.
  defp receive_seed(sock, follower, shard_id, deadline, seeds, ordinal) do
    case remaining(deadline) do
      0 ->
        Follower.discard_seeds(seeds)
        {:error, :pull_timeout}

      left ->
        case :gen_tcp.recv(sock, 0, left) do
          {:ok, bytes} -> handle_frame(sock, follower, shard_id, deadline, seeds, ordinal, bytes)
          {:error, reason} -> abort(seeds, {:error, reason})
        end
    end
  end

  defp handle_frame(sock, follower, shard_id, deadline, seeds, ordinal, bytes) do
    case Protocol.decode(bytes) do
      {:ok, %Protocol.SeedBegin{shard_id: ^shard_id} = begin} ->
        seeds = Follower.begin_seed(follower, seeds, begin)
        # The ordinal rides in the seed's own state so the install stamps it under the shard lock
        # (expert review 2026-10-10 #22) instead of a read-modify-write after the lock is released.
        seeds = Follower.stamp_seed_ordinal(seeds, shard_id, ordinal)
        receive_seed(sock, follower, shard_id, deadline, seeds, ordinal)

      {:ok, {:seed_chunk, ^shard_id, part, seq, chunk}} ->
        seeds = Follower.write_chunk(seeds, shard_id, part, seq, chunk)
        receive_seed(sock, follower, shard_id, deadline, seeds, ordinal)

      {:ok, {:seed_end, ^shard_id}} ->
        {result, _seeds} = Follower.finish_seed(follower, seeds, shard_id)
        result

      {:ok, {:seed_abort, ^shard_id}} ->
        # The source found its `.db` and `-wal` no longer belong together. Correct behaviour on its
        # part; nothing to install here.
        abort(seeds, {:error, :aborted_by_source})

      {:ok, {:reject, ^shard_id, reason, _}} ->
        abort(seeds, {:error, reason})

      other ->
        abort(seeds, {:error, {:unexpected_frame, other}})
    end
  end

  defp abort(seeds, result) do
    Follower.discard_seeds(seeds)
    result
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  # -- helpers ---------------------------------------------------------------------------------

  # Connect, then run the #17 handshake before any frame is exchanged (a no-op with
  # `REPLICATION_CONN_NONCE` off). Each query/pull runs in its own process and dials its own
  # connection, so the binding the handshake leaves in the process dictionary is exactly this one.
  #
  # A per-peer reused query connection was considered and NOT built (perf review 2026-10-01
  # #3(d)). Measured 2026-10-05 on loopback with the nonce handshake: fresh dial + handshake +
  # position query p50 227 µs / p99 431 µs vs 56 / 136 µs on a held socket, i.e. ~0.17 ms of local
  # work plus ~2 peer RTTs (connect, the follower's hello) per replicating cold open, with peers
  # queried in parallel. In-region that is well under the open's S3 round trips (tens of ms), and
  # a pooled socket breaks the one-connection-per-process assumption the handshake binding relies
  # on. The large cost here was a blackholed peer, which the unreachable cache handles (#3(c)).
  defp connect(host, port) do
    with {:ok, sock} <- dial(host, port) do
      case Protocol.handshake_connect(sock, Protocol.handshake_timeout_ms()) do
        :ok ->
          {:ok, sock}

        {:error, _} = error ->
          :gen_tcp.close(sock)
          error
      end
    end
  end

  defp dial(host, port) do
    charlist = if is_binary(host), do: String.to_charlist(host), else: host

    :gen_tcp.connect(
      charlist,
      port,
      [
        :binary,
        packet: 4,
        # A peer answering a position query during a failover is not automatically trustworthy;
        # bound what its declared length can make us allocate. See Protocol.max_frame_bytes/0.
        packet_size: Fathom.Shard.Replication.Protocol.max_frame_bytes(),
        active: false,
        nodelay: true
      ],
      @connect_timeout
    )
  end

  defp local_state(follower, shard_id) do
    Follower.state_of(follower, shard_id)
  rescue
    # No follower running: no table, so no replica. Not an error.
    ArgumentError -> nil
  end

  defp listening?(follower) do
    is_pid(follower) or Process.whereis(follower) != nil
  end

  defp query_timeout_ms,
    do: Application.get_env(:fathom, :replication_recovery_timeout_ms, @query_timeout_ms)

  defp pull_timeout_ms,
    do: Application.get_env(:fathom, :replication_recovery_pull_timeout_ms, @pull_timeout_ms)
end
