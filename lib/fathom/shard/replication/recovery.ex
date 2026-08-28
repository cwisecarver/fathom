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

  Options:

    * `:follower` — the local `Follower` instance (default `Follower`).
    * `:peers` — override the peer set, for tests.
    * `:query_timeout_ms` / `:pull_timeout_ms` — bounds for the two network phases.

  **Never raises.** This runs inside a shard's open, and a failure to improve on the stored object
  must not fail the open — the ordinary path is still correct, it just recovers less.
  """
  @spec best_replica(String.t(), map() | nil, keyword()) :: {:ok, position()} | :none
  def best_replica(shard_id, object_stamp, opts \\ []) do
    follower = Keyword.get(opts, :follower, Follower)
    local = local_state(follower, shard_id)

    # Short-circuit before any socket: a node that already holds a copy ahead of the object has
    # nothing to gain from the fleet, and this is the common case on a warm restart.
    if Promote.fresher?(local, object_stamp) do
      {:ok, local}
    else
      search(shard_id, object_stamp, local, follower, opts)
    end
  rescue
    e ->
      Logger.warning("replication recovery for #{shard_id} crashed: #{Exception.message(e)}")
      :none
  catch
    kind, reason ->
      Logger.warning("replication recovery for #{shard_id} exited: #{inspect({kind, reason})}")
      :none
  end

  defp search(shard_id, object_stamp, local, follower, opts) do
    peers = Keyword.get(opts, :peers) || peers()

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
        offers = ask(peers, shard_id, Keyword.get(opts, :query_timeout_ms, query_timeout_ms()))

        case choose(local, object_stamp, offers) do
          :none -> :none
          :local -> {:ok, local}
          {:pull, endpoint, pos} -> pull(shard_id, endpoint, pos, follower, opts)
        end
    end
  end

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
  defp ask(peers, shard_id, timeout_ms) do
    peers
    |> Task.async_stream(&ask_one(&1, shard_id, timeout_ms),
      timeout: timeout_ms + @connect_timeout,
      on_timeout: :kill_task,
      max_concurrency: max(length(peers), 1),
      ordered: false
    )
    |> Enum.flat_map(fn
      {:ok, {endpoint, pos}} -> [{endpoint, pos}]
      # A peer that timed out, refused, or crashed contributes nothing. NOT an error: the whole
      # point of a quorum is that some nodes are gone, and a failover is when that is most likely.
      _ -> []
    end)
  end

  defp ask_one({_key, host, port} = endpoint, shard_id, timeout_ms) do
    case connect(host, port) do
      {:ok, sock} ->
        try do
          {endpoint, query(sock, shard_id, timeout_ms)}
        after
          :gen_tcp.close(sock)
        end

      {:error, _reason} ->
        {endpoint, nil}
    end
  end

  defp query(sock, shard_id, timeout_ms) do
    with :ok <- :gen_tcp.send(sock, Protocol.encode_position_query(shard_id)),
         {:ok, bytes} <- :gen_tcp.recv(sock, 0, timeout_ms),
         {:ok, {:position, ^shard_id, pos}} <- Protocol.decode(bytes) do
      pos
    else
      # Includes a peer one deploy behind, which answers `{:error, :malformed}` and closes: see the
      # note in `Protocol` on why these frames were added without bumping the version. It also
      # includes a reply naming a DIFFERENT shard, which is refused rather than trusted.
      _ -> nil
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

    with :ok <- :gen_tcp.send(sock, Protocol.encode_replica_request(shard_id)),
         {:ok, offset} <- receive_seed(sock, follower, shard_id, deadline, %{}) do
      # THE PULLED REPLICA HAS NO ORDINAL OF ITS OWN (expert review 2026-08-26 #2, step 3b): a seed
      # installs `FollowerLog.seeded/5`'s state and `SeedBegin` carries none. Without this the
      # survivor would hold a replica `Promote.fresher?/2` can never rank, and recovery would end
      # exactly where it began — cold-opening the stale object. The offer this peer was CHOSEN on
      # is the only statement about these bytes we have, and taking it can only under-claim.
      Follower.note_ordinal(follower, shard_id, Map.get(promised, :wal_ordinal, 0))

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
  defp receive_seed(sock, follower, shard_id, deadline, seeds) do
    case remaining(deadline) do
      0 ->
        Follower.discard_seeds(seeds)
        {:error, :pull_timeout}

      left ->
        case :gen_tcp.recv(sock, 0, left) do
          {:ok, bytes} -> handle_frame(sock, follower, shard_id, deadline, seeds, bytes)
          {:error, reason} -> abort(seeds, {:error, reason})
        end
    end
  end

  defp handle_frame(sock, follower, shard_id, deadline, seeds, bytes) do
    case Protocol.decode(bytes) do
      {:ok, %Protocol.SeedBegin{shard_id: ^shard_id} = begin} ->
        seeds = Follower.begin_seed(follower, seeds, begin)
        receive_seed(sock, follower, shard_id, deadline, seeds)

      {:ok, {:seed_chunk, ^shard_id, part, seq, chunk}} ->
        seeds = Follower.write_chunk(seeds, shard_id, part, seq, chunk)
        receive_seed(sock, follower, shard_id, deadline, seeds)

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

  defp connect(host, port) do
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
