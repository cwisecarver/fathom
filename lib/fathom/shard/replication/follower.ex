defmodule Fathom.Shard.Replication.Follower do
  @moduledoc """
  The receiving end of A2 replication — see `docs/a2-quorum-replication.md`.

  A thin shell over `Fathom.Shard.Replication.FollowerLog`: this module owns a listener, a socket
  and a file handle, and every accept/reject decision is delegated. That split is the point — the
  decisions are where a tenant database gets corrupted, and they are unit-tested without a network.

  **One connection serves every shard a primary replicates here.** Each push names its shard and
  each ack names it back, so a primary holds one socket per follower *node*, not per shard. At
  millions of shards the alternative is millions of sockets.

  ## Per-shard state, and why it lives in ETS

  A follower must remember `{epoch, wal_gen, next_offset}` per shard **across connections**: a
  primary that reconnects after a blip has not rewound, and losing the offset would force a full
  re-seed from S3 for what was a two-second network event. The table is owned by this GenServer, so
  a crash here drops the state and the next push is refused with `:unknown_shard` — the correct
  direction. Fabricating a resume point after losing it is how a follower ends up appending into
  the wrong position.

  ## Followers do not open the database

  Per the gate-1 finding, a clean close **checkpoints**: one open-and-close moved a follower's
  `.db` from 4096 to 8192 bytes and deleted its `-wal`, desynchronizing it from the primary's byte
  offsets. So this module only ever appends to the `-wal` file with a raw handle and never opens
  the shard through SQLite. Promotion is the first legitimate open, and it is not this module's job.

  ## Acking from RAM by default

  `:replication_fsync` (default **false**) decides whether a follower `fdatasync`s before acking.
  Off matches Waterpark, which acks from RAM and takes durability from replica count rather than
  disk. Measured cost of turning it on: ~300 µs against a ~96 µs floor, i.e. **2.4× fathom's whole
  current request round trip** (gate 2). Off is also never *worse* than today: if every replica
  holding an un-synced frame dies at once, the shard falls back to its S3 object, which is exactly
  the pre-A2 behaviour.
  """
  use GenServer

  require Logger

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Replication.FollowerLog
  alias Fathom.Shard.Replication.Priority
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Shard.Replication.Protocol

  # ------------------------------------------------------------------------------------------
  # api
  # ------------------------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  The ETS table backing `name`'s per-shard state.

  Derived from the process name rather than a single module-wide constant, so more than one
  follower can run in one VM. Production has exactly one per node and would not care — but a
  singleton table also made it impossible to stand four real followers up in a test, and an
  environment that cannot express the topology cannot catch bugs in it (AGENTS.md).
  """
  @spec table(atom()) :: atom()
  # The default (production) name resolves at compile time; `Module.concat/2` interned an atom and
  # allocated a binary on EVERY call, 17 sites on the per-frame path (expert review 2026-10-10 #P4).
  # Other names (tests standing up several followers) keep the concat.
  @default_table Module.concat(__MODULE__, Shards)
  @default_locks Module.concat(__MODULE__, ShardLocks)
  @default_touches Module.concat(__MODULE__, ShardTouches)

  # A WAL frame header: pgno, nTruncate, two salts, two checksums; and the two header magics (`Wal`).
  @wal_frame_header_bytes 24
  @wal_magics [0x377F0682, 0x377F0683]

  def table(name \\ __MODULE__)
  def table(__MODULE__), do: @default_table
  def table(name), do: Module.concat(name, Shards)

  @doc false
  # The per-shard mutex table (expert review 2026-10-08 #5). Separate from `table/1` so the lock
  # rows can never be mistaken for state rows by `replica_shard_ids/1`'s `tab2list`.
  @spec locks(atom()) :: atom()
  def locks(name \\ __MODULE__)
  def locks(__MODULE__), do: @default_locks
  def locks(name), do: Module.concat(name, ShardLocks)

  @doc false
  # When each replica row was last written, `{shard_id, monotonic_s}` (expert review
  # 2026-10-20-2027 #11, see "SWEEPING IDLE REPLICA ROWS"). Separate from `table/1` for the same
  # reason as `locks/1`.
  @spec touches(atom()) :: atom()
  def touches(name \\ __MODULE__)
  def touches(__MODULE__), do: @default_touches
  def touches(name), do: Module.concat(name, ShardTouches)

  # ------------------------------------------------------------------------------------------
  # the per-shard lock
  # ------------------------------------------------------------------------------------------
  #
  # ONE SHARD'S REPLICA IS MUTATED BY ONE PROCESS AT A TIME, NODE-WIDE (expert review 2026-10-08
  # #5 tiers b+c, and #13).
  #
  # Per-shard workers (c61cbe3) keep one shard's frames in order WITHIN a connection, and that was
  # the only serialization there was. A primary that reconnects gets a NEW connection with new
  # workers while the old connection's workers may still be executing a frame, so two processes
  # could each read the state, decide, write the WAL and put the state for the same shard — the
  # old one truncating a WAL the new one had just appended to and acked, or rolling `next_offset`
  # back. Seeds made it worse: a seed spans many frames, and a second connection's `seed_begin`
  # truncated the first's temp file in place. `Promote.stage/4` copied the `.db` and the `-wal` with
  # no coordination at all, so a deposed owner's reset push could land between the two copies.
  #
  # A row `{shard_id, holder_pid, acquired_at_ms}` in a `:public` ETS table owned by the Follower,
  # taken with `:ets.insert_new/2` and released with `:ets.delete_object/2` of the exact row — one
  # ETS op pair per push on the uncontended path, which is every push of single-connection
  # operation. Held around:
  #
  #   * one push — decide, absorb, write, put_state (`handle_push/2`);
  #   * one whole seed — `seed_begin` until its install or discard, across many frames (the seed's
  #     state carries the token);
  #   * a promotion's fence-and-copy (`with_shard_lock/4` from `Promote.stage/4`).
  #
  # RE-ENTRANT for the holder: a worker that holds a shard's seed lock and gets a push for the same
  # shard on the same connection proceeds exactly as it did before this lock existed.
  #
  # A HOLDER CANNOT LEAK IT. A contender that finds the row held by a DEAD process (a worker killed
  # by `close_connection/1`'s `@worker_stop_ms`, a crashed Recovery pull) or one older than
  # `:replication_shard_lock_max_ms` (a seed on a half-open connection whose reader never sees the
  # close) deletes that exact row and takes the lock. The age takeover is what makes unique seed
  # temps (tier c) necessary rather than tidy: the overtaken holder may still be alive, so its
  # files must be its own, and its install re-checks ownership before renaming anything.
  #
  # CONTENTION IS A BOUNDED WAIT, THEN A REFUSAL. A push waits `:replication_shard_lock_wait_ms`
  # and is then refused `:internal` — a reject the primary already settles and re-plans from on its
  # next commit (`Session`'s `@settled_rejects`), so no wire change and no new reason. Kept short
  # because the waiting worker also serves every other shard hashed to it.
  @default_lock_wait_ms 50
  @default_lock_max_ms 180_000

  @typedoc "Proof of holding a shard's lock, handed back to `unlock_shard/2`."
  @type lock_token :: {:held, {String.t(), pid(), integer()}} | :reentrant

  @doc false
  @spec lock_shard(atom(), String.t(), non_neg_integer()) ::
          {:ok, lock_token()} | {:error, :busy | :follower_stopped}
  def lock_shard(name, shard_id, wait_ms) do
    acquire_lock(locks(name), shard_id, System.monotonic_time(:millisecond) + wait_ms)
  rescue
    ArgumentError -> {:error, :follower_stopped}
  end

  defp acquire_lock(tab, shard_id, deadline) do
    me = self()
    now = System.monotonic_time(:millisecond)
    row = {shard_id, me, now}

    if :ets.insert_new(tab, row) do
      {:ok, {:held, row}}
    else
      case :ets.lookup(tab, shard_id) do
        [{_, ^me, _}] ->
          {:ok, :reentrant}

        # Released between the two calls.
        [] ->
          acquire_lock(tab, shard_id, deadline)

        [{_, holder, at} = held] ->
          cond do
            abandoned_lock?(holder, at, now) ->
              Logger.warning(
                "replication: taking over #{shard_id}'s replica lock from " <>
                  "#{inspect(holder)} (#{if Process.alive?(holder), do: "held #{now - at}ms", else: "holder is dead"})"
              )

              :ets.delete_object(tab, held)
              acquire_lock(tab, shard_id, deadline)

            now >= deadline ->
              {:error, :busy}

            true ->
              receive do
              after
                1 -> :ok
              end

              acquire_lock(tab, shard_id, deadline)
          end
      end
    end
  end

  defp abandoned_lock?(holder, at, now),
    do: not Process.alive?(holder) or now - at > lock_max_ms()

  @doc false
  @spec unlock_shard(atom(), lock_token() | nil) :: :ok
  def unlock_shard(name, {:held, row}) do
    :ets.delete_object(locks(name), row)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def unlock_shard(_name, _reentrant_or_none), do: :ok

  # Does `token` still hold the lock? False once a contender took it over by age.
  defp owns_lock?(name, shard_id, {:held, row}) do
    :ets.lookup(locks(name), shard_id) == [row]
  rescue
    ArgumentError -> false
  end

  defp owns_lock?(name, shard_id, :reentrant) do
    me = self()
    match?([{_, ^me, _}], :ets.lookup(locks(name), shard_id))
  rescue
    ArgumentError -> false
  end

  defp owns_lock?(_name, _shard_id, _none), do: false

  @doc """
  Run `fun` holding `shard_id`'s replica lock (see the lock section above), waiting at most
  `wait_ms` for it. Returns `fun`'s result, or `{:error, :replica_busy}` when another holder kept
  it for the whole wait, or `{:error, :no_replica}` when this follower is not running.

  For callers outside the follower's own workers that must see a replica's `.db` and `-wal` as one
  consistent pair — `Fathom.Shard.Replication.Promote.stage/4`.
  """
  @spec with_shard_lock(atom(), String.t(), non_neg_integer(), (-> result)) ::
          result | {:error, :replica_busy | :no_replica}
        when result: term()
  def with_shard_lock(name, shard_id, wait_ms, fun) do
    case lock_shard(name, shard_id, wait_ms) do
      {:ok, token} ->
        try do
          fun.()
        after
          unlock_shard(name, token)
        end

      {:error, :busy} ->
        {:error, :replica_busy}

      {:error, :follower_stopped} ->
        {:error, :no_replica}
    end
  end

  defp lock_wait_ms,
    do: Application.get_env(:fathom, :replication_shard_lock_wait_ms, @default_lock_wait_ms)

  defp lock_max_ms,
    do: Application.get_env(:fathom, :replication_shard_lock_max_ms, @default_lock_max_ms)

  @doc """
  Fence this follower's replica of `shard_id` for a promotion (expert review 2026-10-08 #13).
  **Call it holding the shard's lock** (`with_shard_lock/4`) and only after the copy succeeded.

  Until `forget/2` runs after a promotion publishes, the replica row still accepts the DEPOSED
  owner's pushes — an append it acks after the copy is a write the promoted database does not hold,
  and its reset would absorb-and-truncate under a later reader. This advances the row so those
  pushes are refused by `FollowerLog.decide/2`'s existing fences:

    * `:lineage` — the promoting owner's lineage; a deposed owner states a lower one and is refused
      `:stale_epoch` by the lineage fence. Raised, never lowered.
    * `:epoch` — the promoting lease's epoch, for the operator path (`Promote.promote/2`) that has
      no lineage to claim. Raised, never lowered.

  And always `torn: true`, durably: from here the row no longer describes a replica of anyone's
  history (its lineage is the new owner's, its bytes the old owner's), so it must never be ranked
  by `Promote.fresher?/2` or offered to a peer — if the promotion goes on to fail, the replica
  waits for a re-seed rather than being mis-ranked against the new owner's own stamps.

  `:expect` — the replica state the caller decided to promote on. Refused `{:error,
  :replica_changed}` if the replica has since gone torn, changed lineage or moved to a lower
  ordinal (a re-seed), because the staged copy would no longer be the replica that decision ranked.
  """
  @spec fence_for_promotion(atom(), String.t(), keyword()) ::
          :ok | {:error, :no_replica | :replica_changed}
  def fence_for_promotion(name, shard_id, opts) do
    case state_of(name, shard_id) do
      nil ->
        {:error, :no_replica}

      state ->
        if changed_since?(state, Keyword.get(opts, :expect)) do
          {:error, :replica_changed}
        else
          put_state(name, shard_id, %{
            state
            | torn: true,
              lineage: raise_to(state.lineage, Keyword.get(opts, :lineage)),
              epoch: raise_to(state.epoch, Keyword.get(opts, :epoch))
          })

          :ok
        end
    end
  rescue
    ArgumentError -> {:error, :no_replica}
  end

  # Only a real counter moves the fence, and only upward: the caller's lineage can be `:disabled`
  # (`Position.lineage_to_store/1`) or nil, and neither may land in the replica's state.
  defp raise_to(current, value) when is_integer(value) and value > current, do: value
  defp raise_to(current, _value), do: current

  defp changed_since?(_state, nil), do: false

  defp changed_since?(state, expect) do
    state.torn or state.lineage != Map.get(expect, :lineage, 0) or
      Map.get(state, :wal_ordinal, 0) < Map.get(expect, :wal_ordinal, 0)
  end

  @doc "The port this follower is listening on (0 in config means 'pick one')."
  @spec port(GenServer.server()) :: {:ok, :inet.port_number()}
  def port(server \\ __MODULE__), do: GenServer.call(server, :port)

  @doc """
  Register a shard this node is following, with the state its seed left it in.

  Must be called before any push for that shard, or the push is refused — see `FollowerLog.decide/2`
  on why "never seen" is deliberately distinct from "at offset 0".
  """
  @spec seed(
          atom(),
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def seed(name \\ __MODULE__, shard_id, epoch, wal_gen, salt1, wal_bytes, lineage \\ 0) do
    put_state(name, shard_id, FollowerLog.seeded(epoch, wal_gen, salt1, wal_bytes, lineage))
    :ok
  end

  @doc """
  Stop following a shard: drop its replication state and its local files.

  Used by promotion, once the replica has become this node's primary. Keeping the row would leave
  the node believing it is still a replica of a shard it now serves, so the next push from a
  *deposed* primary would be measured against a position that no longer describes anything.

  Dropping the state is the safe direction by construction — the next push for this shard is
  refused `:unknown_shard`, which asks for a re-seed rather than guessing a resume point.
  """
  @spec forget(atom(), String.t()) :: :ok
  def forget(name \\ __MODULE__, shard_id) do
    # Under the shard's lock (expert review 2026-10-10 #22): the row delete and the file removals
    # must not interleave with a push or seed install, which could re-create the row over files
    # that are then removed (or the reverse). If the lock stays busy for the whole wait we forget
    # anyway — erasing a deleted tenant's replica (Tombstones) must not be blockable by a stuck
    # holder, and that is exactly what this did before the lock existed.
    case with_shard_lock(name, shard_id, forget_lock_wait_ms(), fn ->
           do_forget(name, shard_id)
         end) do
      :ok -> :ok
      {:error, _busy_or_stopped} -> do_forget(name, shard_id)
    end
  end

  defp forget_lock_wait_ms,
    do: Application.get_env(:fathom, :replication_forget_lock_wait_ms, 2_000)

  defp do_forget(name, shard_id) do
    :ets.delete(table(name), shard_id)
    :ets.delete(touches(name), shard_id)
    Fathom.FS.rm(db_path(name, shard_id))
    Fathom.FS.rm(wal_path(name, shard_id))
    # And the torn marker (expert review 2026-08-20 #11b), or a promoted-then-re-followed shard
    # inherits a quarantine flag from a replica that no longer exists.
    Fathom.FS.rm(torn_path(name, shard_id))
    Fathom.FS.rm(lineage_path(name, shard_id))
    Fathom.FS.rm(ordinal_path(name, shard_id))
    Fathom.FS.rm(db_position_path(name, shard_id))
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Current replication state for a shard, or nil if it was never seeded.

  A row that is not resident — never derived since this follower started (rows are derived on
  first use, not at boot), or dropped by the idle sweep (`sweep_idle_rows/1`) — is derived from
  the replica's files here, so no caller can tell it from a resident one.
  """
  @spec state_of(atom(), String.t()) :: FollowerLog.t() | nil
  def state_of(name \\ __MODULE__, shard_id) do
    case :ets.lookup(table(name), shard_id) do
      [{^shard_id, s}] -> s
      [] -> rederive(name, shard_id)
    end
  end

  @doc """
  The replica we are willing to OFFER a peer — `state_of/2`, or `nil` when it is torn.

  Separate from `state_of/2` on purpose. `state_of/2` answers "what is our replication state",
  which the shipper and the local promote path both need to see truthfully including the torn
  flag; this answers the narrower "do we hold a copy worth pulling", where torn and never-seeded
  are the same answer. Collapsing the two would mean either lying to the shipper or offering a
  peer an incoherent `.db`/`-wal` pair.
  """
  @spec offerable(atom(), String.t()) :: FollowerLog.t() | nil
  def offerable(name \\ __MODULE__, shard_id) do
    case state_of(name, shard_id) do
      %{torn: true} -> nil
      other -> other
    end
  end

  @doc """
  The shard ids this node holds a coherent (non-torn) A2 replica of — the "warm" set for
  affinity-aware placement (`Fathom.Rebalancer.WarmLocations`).

  This became the affinity signal when the `Fathom.Shard.WarmFollower` read cache was retired
  (2026-09-14): a node holding an A2 replica IS "warm" for that shard — it already has recent bytes,
  so a handoff there is a peer-pull/304 rather than a full cold S3 pull. Torn replicas are excluded,
  the same `offerable/2` notion, because an affinity target must actually hold bytes worth pulling.
  A node not running a `Follower` (replication off) has no table and answers `[]`, exactly as the
  old `WarmFollower.cached_shard_ids/0` returned an empty set on a node with no cache.
  """
  @spec replica_shard_ids(atom()) :: [String.t()]
  def replica_shard_ids(name \\ __MODULE__) do
    if Process.whereis(name) do
      table(name)
      |> :ets.tab2list()
      |> Enum.reject(fn {_id, s} -> match?(%{torn: true}, s) end)
      |> Enum.map(fn {id, _s} -> id end)
    else
      []
    end
  rescue
    # The follower died between the whereis and the read — no table, no replicas to advertise.
    ArgumentError -> []
  end

  @doc """
  The subset of `ids` this node holds a coherent (non-torn) A2 replica of — `replica_shard_ids/1`
  restricted to a caller-supplied set, via one `:ets.lookup/2` per id instead of copying the whole
  replica table (expert review 2026-10-17 #14: the Reporter intersected ~50 hot ids against a
  `tab2list` of every replica, every beat). A node with no follower answers `[]`.
  """
  @spec replica_shard_ids_among([String.t()], atom()) :: [String.t()]
  def replica_shard_ids_among(ids, name \\ __MODULE__) when is_list(ids) do
    if Process.whereis(name) do
      tab = table(name)

      Enum.filter(ids, fn id ->
        case :ets.lookup(tab, id) do
          [{^id, %{torn: true}}] -> false
          [{^id, _}] -> true
          [] -> false
        end
      end)
    else
      []
    end
  rescue
    ArgumentError -> []
  end

  @doc """
  Where this follower keeps the WAL files it receives.

  Per-instance, not global. Four followers sharing one directory would all write the SAME file for
  a given shard, so a test standing up four of them would pass while proving nothing about
  independent replicas.
  """
  @spec dir(atom()) :: Path.t()
  def dir(name \\ __MODULE__) do
    case :ets.lookup(table(name), :__dir__) do
      [{:__dir__, d}] -> d
      [] -> default_dir()
    end
  end

  @doc false
  def default_dir do
    Application.get_env(:fathom, :replication_dir) ||
      Path.join(System.tmp_dir!(), "fathom_replication")
  end

  # The bound interface, for the boot log only. "0.0.0.0" is spelled out rather than left blank
  # because "listening on :9100" reads like a local detail while "listening on 0.0.0.0:9100" reads
  # like the exposure it is — and this port is unauthenticated.
  defp bind_label(opts) do
    case Keyword.get(opts, :ip) do
      nil -> "0.0.0.0 (ALL interfaces — set REPLICATION_BIND_IP)"
      ip -> ip |> :inet.ntoa() |> to_string()
    end
  end

  # Path-traversal / isolation gate (expert review 2026-08-20 #1), the same fail-closed assertion
  # every shard-id-to-path boundary carries and for the same reason: shard_id becomes a
  # FILE NAME here, so a `..` or `/` id escapes dir/1. `Path.join/2` neutralizes a leading `/` but
  # NOT `..` — `Path.join("/a/b", "../c")` is `/a/b/../c`, which the OS resolves.
  #
  # The real gate is `decode_validated/1` at the frame boundary, which refuses the id before any
  # handler sees it. This raise is the backstop for a caller that bypasses it, and should never
  # fire on a validated id.
  @doc false
  def wal_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db-wal")
  end

  @doc false
  def db_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db")
  end

  @doc false
  def torn_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db.torn")
  end

  @doc false
  def lineage_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db.lineage")
  end

  @doc false
  def ordinal_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db.ordinal")
  end

  @doc false
  # What the `.db` ALONE holds, for a replica whose `-wal` is empty or absent (expert review
  # 2026-10-15 #4 follow-up). See `record_db_position/5`.
  def db_position_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db.position")
  end

  defp assert_valid_shard_id!(shard_id) do
    Fathom.ShardId.valid?(shard_id) ||
      raise(ArgumentError, "invalid shard id: #{inspect(shard_id)}")
  end

  # ------------------------------------------------------------------------------------------
  # server
  # ------------------------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Without this, an exit signal from the linked accept loop kills this GenServer OUTRIGHT and
    # `terminate/2` — which closes the listen socket — never runs (expert review 2026-08-20 #21).
    Process.flag(:trap_exit, true)

    name = Keyword.get(opts, :name, __MODULE__)
    dir = Keyword.get(opts, :dir) || default_dir()
    File.mkdir_p!(dir)

    # `write_concurrency` because every connection handler writes its own shards' rows; the
    # GenServer owns the table only so the state dies with it.
    tab =
      :ets.new(table(name), [
        :named_table,
        :public,
        :set,
        write_concurrency: true,
        read_concurrency: true
      ])

    :ets.insert(tab, {:__dir__, dir})

    # Owned here for the same reason as the state table: it dies with this process, and a holder
    # whose release then raises is rescued (`unlock_shard/2`). See the lock section above.
    :ets.new(locks(name), [:named_table, :public, :set, write_concurrency: true])
    :ets.new(touches(name), [:named_table, :public, :set, write_concurrency: true])

    # Which registry says "this node coordinates that shard" (`coordinated_here?/2`). `Fleet` passes
    # `Fathom.ShardRegistry`, so production always has it. The DEFAULT is off (`nil`) because a
    # loopback test runs the PRIMARY and the follower in one VM: the primary's coordinator is then
    # registered in the same registry and would be mistaken for the follower's own node serving the
    # shard, refusing the very pushes the test exists to drive.
    :persistent_term.put(
      {__MODULE__, :coordinator_registry, name},
      Keyword.get(opts, :coordinator_registry)
    )

    # NO EAGER RECOVERY HERE (expert review 2026-10-21 #5). `init/1` used to derive a row for every
    # replica on disk before returning, ~150-300 µs each (8,000 replicas = 1.2-2.1 s), and since
    # `Fleet` sits in the data plane ahead of `Edge`, that held up this node's Hrana and /health
    # listeners as well as this one. Rows are now derived on first use (`state_of/2` ->
    # `rederive/2`, the same `derive_state/3` the eager pass ran), and the two housekeeping passes
    # that need a directory listing (stale seed temps, tombstoned replicas) run in
    # `handle_continue(:boot_housekeeping)`, after the listen socket exists. See "ROWS ARE DERIVED
    # ON FIRST USE" above `recover_shard/3` for why that is equivalent.
    schedule_seed_temp_reap()
    schedule_row_sweep()

    # `ip:` is a security control, not tuning. This socket takes raw WAL frames for a tenant's
    # database and the protocol has NO authentication — whoever reaches the port can write into
    # any shard this node follows. Without `ip:` `:gen_tcp` binds every interface, which on a
    # cloud host means the public one. Same trust posture as `hrana_auth: :disabled` (the network
    # IS the boundary), so it gets the same knob: `REPLICATION_BIND_IP`.
    listen_opts =
      [
        :binary,
        packet: 4,
        # Bounds the pre-body allocation `packet: 4` would otherwise make from an attacker-
        # declared length. See Protocol.max_frame_bytes/0 — this is a security control.
        packet_size: Protocol.max_frame_bytes(),
        active: false,
        reuseaddr: true,
        nodelay: true,
        backlog: 128
      ]
      |> then(fn base ->
        case Keyword.get(opts, :ip) do
          nil -> base
          ip -> [{:ip, ip} | base]
        end
      end)

    # Accepted sockets inherit the keepalive options. If the OS rejects the tuned raw options the
    # listener still starts, on plain keepalive or none (expert review 2026-10-10 #11).
    {:ok, lsock} =
      Fathom.Shard.Replication.Keepalive.with_fallback(listen_opts, fn o ->
        :gen_tcp.listen(Keyword.get(opts, :port, 0), o)
      end)

    {:ok, port} = :inet.port(lsock)
    Logger.info("replication follower listening on #{bind_label(opts)}:#{port} (dir #{dir})")

    {:ok, %{lsock: lsock, port: port, name: name}, {:continue, :accept}}
  end

  @impl true
  def handle_continue(:accept, state) do
    spawn_link(fn -> accept_loop(state.lsock, state.name) end)
    {:noreply, state, {:continue, :boot_housekeeping}}
  end

  # After the listener: a directory listing, no per-replica derive. Runs in this GenServer, which
  # nothing on the push/ask/promote path waits on (they use the ETS tables directly), so it delays
  # only `port/1` and the timers. Tests sync on it with `:sys.get_state/1`.
  def handle_continue(:boot_housekeeping, state) do
    dir = dir(state.name)
    entries = list_dir(dir)
    reap_stale_seed_temps(dir, entries)
    erase_tombstoned_replicas(state.name, entries)
    {:noreply, state}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, {:ok, state.port}, state}

  # With `trap_exit` set, the linked accept loop's exit arrives here instead of killing us. A
  # normal exit is the listen socket closing during shutdown; anything else means the loop died and
  # nothing is accepting, so stop and let the supervisor start a fresh listener rather than sit
  # there with an open port and no acceptor (expert review 2026-08-20 #21).
  @impl true
  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.error(
      "replication accept loop exited (#{inspect(reason)}); stopping the listener so it is " <>
        "restarted with a fresh socket rather than left bound with nothing accepting"
    )

    {:stop, {:accept_loop_exited, reason}, state}
  end

  # PERIODIC SEED-TEMP SWEEP (expert review 2026-10-10 #19). Since 8162cf3 seed temps are unique per
  # seed, so a worker killed in `close_connection/1` leaves files nothing replaces; the boot reaper
  # alone let them accumulate for the life of the node. Same age gate, run on a timer.
  def handle_info(:reap_seed_temps, state) do
    dir = dir(state.name)
    reap_stale_seed_temps(dir, list_dir(dir))
    schedule_seed_temp_reap()
    {:noreply, state}
  end

  def handle_info(:sweep_rows, state) do
    _ = sweep_idle_rows(state.name)
    schedule_row_sweep()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule_row_sweep do
    Process.send_after(self(), :sweep_rows, row_sweep_ms())
  end

  defp row_sweep_ms, do: Application.get_env(:fathom, :replication_row_sweep_ms, 60_000)

  defp schedule_seed_temp_reap do
    Process.send_after(self(), :reap_seed_temps, seed_temp_reap_ms())
  end

  defp seed_temp_reap_ms,
    do: Application.get_env(:fathom, :replication_seed_temp_reap_ms, 5 * 60_000)

  @impl true
  def terminate(_reason, %{lsock: lsock}), do: :gen_tcp.close(lsock)

  # NO `fullsweep_after: 0` HERE, and that is a measured decision rather than an oversight.
  #
  # `Fathom.Shard.Replication.Shipper` carries it because a GenServer holding WAL-frame payloads in
  # its mailbox retained 7-18 GiB per node (expert review 2026-07-24 #9, then `e0fda94`), and the
  # obvious next question was whether the RECEIVE side does the same. It does not, and the reason is
  # structural: `serve/3` below is a tail-recursive `:gen_tcp.recv` loop, so it holds ONE frame at a
  # time, decodes it, writes it straight to disk (seed chunks stream to an fd — see `accept_chunk`,
  # nothing is buffered) and drops it on the tail call. There is no mailbox to accumulate in, and
  # one process per PEER rather than per shard.
  #
  # MEASURED on the rig 2026-08-19, 1024 replicating tenants, sampled twice during the run with
  # `deploy/chaos/follower_mem.sh` (which classifies binary holders by role):
  #
  #     shipper        1,031 MB / 1,078 MB across 4 procs
  #     follower_task      3 MB /     2 MB across 5 procs
  #
  # The receive side held ~0.2% of the send side's binary memory and stayed flat (1 -> 3 MB) while
  # the shippers went 0 -> 1,031 MB. Adding the flag here would be optimizing from analogy against a
  # measurement that says there is nothing to collect.
  #
  # UPDATE 2026-10-01 #7: that measurement predates the per-shard workers below. The READER still
  # holds one frame at a time, but workers now wait in `receive` with frames in their mailboxes
  # (bounded by `@max_outstanding_bytes`), so the WORKERS are spawned with `fullsweep_after: 0`.
  defp accept_loop(lsock, name), do: accept_loop(lsock, name, [])

  # `handlers` is the live connection handlers this loop has started. CAPPED (expert review
  # 2026-10-13 #31): an unauthenticated peer could otherwise open connections without limit and
  # exhaust the node's file descriptors. Dead handlers are pruned on every accept (pids are local
  # and accepts are rare), which is robust to a handler that is killed rather than returning, where
  # a counter decremented by the handler itself would leak.
  defp accept_loop(lsock, name, handlers) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        handlers = Enum.filter(handlers, &Process.alive?/1)

        if length(handlers) >= max_connections() do
          Logger.debug("replication accept: connection cap reached; refusing a connection")
          :gen_tcp.close(sock)
          accept_loop(lsock, name, handlers)
        else
          accept_loop(lsock, name, [hand_off(sock, name) | handlers])
        end

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Logger.warning("replication accept failed: #{inspect(reason)}")
        accept_loop(lsock, name, handlers)
    end
  end

  defp max_connections, do: Application.get_env(:fathom, :replication_max_connections, 256)

  # How long a connection may stay open without delivering ONE frame that decodes (and, with frame
  # auth on, verifies). Long by default on purpose: a legitimate Shipper connects at boot and sends
  # nothing until the shard's next replicated write, which on a quiet fleet can be hours. A short
  # deadline closes that healthy idle link, the Shipper redials after its 500 ms backoff, and a push
  # landing in that gap is rejected `:disconnected` (two followers in the gap at once fails the
  # commit). The connection CAP above is what bounds fd exhaustion; this stops one silent connection
  # holding a slot forever. Lower it only together with a Shipper-side keepalive.
  defp first_frame_timeout_ms,
    do: Application.get_env(:fathom, :replication_first_frame_timeout_ms, 600_000)

  defp hand_off(sock, name) do
    # NEITHER OF THESE MAY BE A BARE `=` MATCH (expert review 2026-08-20 #21).
    #
    # `:gen_tcp.controlling_process/2` returns `{:error, :closed}` when the accepted socket has
    # already been reset by the peer — an ORDINARY event, no attacker required. As a MatchError
    # inside this `spawn_link`ed loop, whose owner does not trap exits, it killed the entire
    # listener GenServer and took its ETS table with it: every follower connection dropped and
    # every replicated shard on the node needed a full DATABASE re-seed. Four peer resets in
    # five seconds then exhausted the supervisor's restart budget and terminated `Fleet` itself.
    #
    # Unlinked handler, deliberately: one primary dropping its connection must not take down
    # the listener, and a malformed frame from one peer must not affect another's shards.
    {:ok, pid} = Task.start(fn -> serve_connection(sock, name) end)

    case :gen_tcp.controlling_process(sock, pid) do
      :ok ->
        :ok

      {:error, reason} ->
        # The peer is already gone. Close our end and let the handler find a dead socket.
        Logger.debug("replication accept: handing off socket failed (#{inspect(reason)})")
        :gen_tcp.close(sock)
    end

    pid
  end

  # The connection's first exchange (expert review 2026-09-29 #17): with `REPLICATION_CONN_NONCE`
  # on, bind this handler's frames to a per-connection nonce pair before reading anything, so a
  # frame recorded on another connection cannot be replayed here. A peer that does not complete the
  # handshake is closed without a single frame being interpreted.
  defp serve_connection(sock, name) do
    case Protocol.handshake_accept(sock, Protocol.handshake_timeout_ms()) do
      :ok ->
        serve(sock, name)

      # A peer that connects and hangs up before its hello is ordinary — a reachability probe (the
      # chaos rig's `replication` step 1 does exactly this) or a load balancer health check — so it
      # is not worth a warning. Anything else (a bad hello, a timeout) is.
      {:error, :closed} ->
        Logger.debug("replication follower: peer closed before the handshake")
        :gen_tcp.close(sock)

      {:error, reason} ->
        Logger.warning("replication follower: handshake failed (#{inspect(reason)}); closing")
        :gen_tcp.close(sock)
    end
  end

  # ONE READER, PER-SHARD WORKERS (expert review 2026-10-01 #7).
  #
  # This was one serial loop per primary connection: recv -> decide -> write -> ack, strictly in
  # order, for EVERY shard that primary replicates here. So one shard's slow step held every other
  # shard on the link — and the slow steps are routine, not exotic: every primary WAL reset absorbs
  # the old WAL (`checkpoint_into_db/1`: a SQLite open, a TRUNCATE checkpoint, fsyncs), which happens
  # on every flush; seed chunks and installs ran in the same loop too. The rig (2026-10-03, 1024
  # tenants) measured this socket's Recv-Q at a 2-3 MB median and ~6 MB (the buffer max) at p90 on
  # every node, before and after the #17/#6 changes: the follower was not reading fast enough, and
  # the primary's sends were blocking behind it.
  #
  # Now the reader only decodes and dispatches. Each shard's frames go to one worker, chosen by a
  # hash of the shard id, so ONE SHARD'S FRAMES STAY IN ORDER (the follower log, seeds and absorbs
  # all rely on that) while different shards proceed in parallel. Workers send their own replies:
  # the primary matches every reply by shard id and holds at most one push in flight per shard
  # (`Shipper.reply_to/3`), so acks for different shards may arrive in any order. No wire change.
  #
  # BOUNDED. The old loop held one frame at a time. Workers have mailboxes, so the reader counts
  # the bytes it has handed out and not yet seen finished, and stops reading at
  # `@max_outstanding_bytes` — the socket then fills and the primary blocks, exactly as before, just
  # later. `:replication_follower_workers` = 1 restores the serial behaviour.
  #
  # `seeds` (partial seeds in flight on THIS connection) now lives in the worker that owns those
  # shards. Still connection-scoped: when the connection drops, every worker discards its partial
  # seeds on the way out, so a reconnect never inherits them.
  @max_outstanding_bytes 16 * 1024 * 1024
  @worker_stop_ms 5_000

  defp serve(sock, name) do
    # The per-connection reader is a per-link hub (expert review 2026-10-21 r7 #12, `Priority`).
    Priority.adopt(:hub)

    conn = %{
      sock: sock,
      name: name,
      binding: Protocol.binding(),
      nworkers: max(Application.get_env(:fathom, :replication_follower_workers, 8), 1),
      workers: %{},
      outstanding: 0,
      # True once ONE frame has decoded (and, with frame auth on, verified): until then the read has
      # a deadline (expert review 2026-10-13 #31).
      authed: false,
      # Set by `close_connection/1` before it stops the workers; each worker checks it before
      # taking a frame. See `worker_loop/6`.
      closed: :atomics.new(1, [])
    }

    read_loop(conn)
  end

  defp read_loop(conn) do
    conn = reap_done(conn)

    timeout = if conn.authed, do: :infinity, else: first_frame_timeout_ms()

    case :gen_tcp.recv(conn.sock, 0, timeout) do
      {:ok, bytes} ->
        case decode_validated(bytes) do
          {:ok, frame} ->
            conn = %{conn | authed: true}

            case frame_shard_id(frame) do
              nil ->
                # A follower receiving an ack means someone pointed a primary at a primary.
                Logger.warning("replication follower got a non-push message: #{inspect(frame)}")
                read_loop(conn)

              shard_id ->
                read_loop(dispatch(conn, shard_id, frame, held_bytes(frame, byte_size(bytes))))
            end

          {:error, reason} ->
            # Do not keep reading a stream we cannot parse — the framing may be out of sync, and
            # every further read would be garbage interpreted as a push.
            Logger.error("replication follower closing connection: #{inspect(reason)}")
            close_connection(conn)
        end

      {:error, :timeout} ->
        Logger.warning(
          "replication follower closing a connection that sent no valid frame within " <>
            "#{timeout} ms"
        )

        close_connection(conn)

      {:error, _} ->
        close_connection(conn)
    end
  end

  defp dispatch(conn, shard_id, frame, size) do
    {worker, conn} = worker_for(conn, :erlang.phash2(shard_id, conn.nworkers))
    send(worker, {:frame, frame, size})
    conn = %{conn | outstanding: conn.outstanding + size}

    if conn.outstanding >= @max_outstanding_bytes, do: await_done(conn), else: conn
  end

  defp worker_for(conn, index) do
    case conn.workers do
      %{^index => pid} ->
        {pid, conn}

      _ ->
        %{sock: sock, name: name, binding: binding, closed: closed} = conn
        pid = spawn_worker(sock, name, binding, self(), closed)
        {pid, %{conn | workers: Map.put(conn.workers, index, pid)}}
    end
  end

  # LINKED: a worker that crashes takes the connection down (the primary reconnects and its waiters
  # are failed `:disconnected`), rather than leaving its shards silently unanswered.
  # `fullsweep_after: 0` because a worker, unlike the old loop, sits in `receive` holding references
  # to frame binaries — the Shipper's measured retention problem in miniature.
  @doc false
  def spawn_worker(sock, name, binding, reader, closed) do
    :erlang.spawn_opt(
      fn -> worker_init(sock, name, binding, reader, closed) end,
      [:link, fullsweep_after: 0]
    )
  end

  # Block until enough dispatched work has finished to read again.
  defp await_done(conn) do
    receive do
      {:frame_done, size} ->
        conn = %{conn | outstanding: conn.outstanding - size}
        if conn.outstanding >= @max_outstanding_bytes, do: await_done(conn), else: conn
    end
  end

  defp reap_done(conn) do
    receive do
      {:frame_done, size} -> reap_done(%{conn | outstanding: conn.outstanding - size})
    after
      0 -> conn
    end
  end

  # Every worker discards its partial seeds and exits. Bounded: a worker stuck in a long step is
  # killed after `@worker_stop_ms`. A killed worker releases nothing: its shard locks are taken over
  # by the next contender because the holder is dead (`abandoned_lock?/3`), and its uniquely-named
  # seed temps are removed by the boot reaper (`reap_stale_seed_temps/1`) — expert review
  # 2026-10-08 #5.
  defp close_connection(conn) do
    # Before the :stop messages, so a worker with frames still queued stops at its next one rather
    # than applying them all first (see `worker_loop/6`).
    :atomics.put(conn.closed, 1, 1)
    :gen_tcp.close(conn.sock)

    refs =
      for {_i, pid} <- conn.workers do
        ref = Process.monitor(pid)
        send(pid, :stop)
        {pid, ref}
      end

    deadline = System.monotonic_time(:millisecond) + @worker_stop_ms

    Enum.each(refs, fn {pid, ref} ->
      wait = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        wait ->
          Process.unlink(pid)
          Process.exit(pid, :kill)
      end
    end)
  end

  defp worker_init(sock, name, binding, reader, closed) do
    # Replies are sealed with this connection's binding, which lives in the process dictionary of
    # whoever encodes them (see `Protocol.seal/1`).
    # `spawn_opt(fn -> ...)` makes initial_call `{:erlang, :apply, 2}`, invisible to role attribution;
    # the label is what `chaos.sh repl-cpu` reads (expert review 2026-10-20 #26).
    Process.set_label(:replication_follower_worker)
    # A per-link hub (expert review 2026-10-21 r7 #12, `Priority`). Its offset-0 absorb push tasks
    # are spawned processes and stay `normal`: they hold a dirty thread for whole checkpoints.
    Priority.adopt(:hub)
    Protocol.adopt_binding(binding)
    worker_loop(sock, name, reader, closed, %{}, %{})
  end

  # A CLOSED CONNECTION'S QUEUED FRAMES ARE DROPPED, NOT APPLIED (expert review 2026-10-08 #5).
  # `:stop` sits BEHIND every frame already in the mailbox — up to `@max_outstanding_bytes` of them —
  # so a worker used to apply them all before it saw it, for up to `@worker_stop_ms`, while the
  # primary had already reconnected and was re-sending the same shards to a NEW connection's worker.
  # Nothing serializes one shard across two connections, so the old worker could truncate a WAL the
  # new one had just appended to and acked, or roll `next_offset` back. Dropping is safe: those
  # frames' replies could never be sent (the socket is closed), so the primary counts none of them
  # and re-sends from its un-advanced position.
  #
  # This narrows the overlap to the ONE frame a worker may be in the middle of — what it was before
  # per-shard workers (c61cbe3). The shard lock (`lock_shard/3`, tiers b+c of the same finding)
  # closes the rest: that one frame and the new connection's frame for the same shard can no longer
  # interleave, they run one after the other.
  #
  # OFFSET-0 PUSHES RUN IN A PER-SHARD TASK (expert review 2026-10-17 #5). A push at offset 0 is the
  # only kind that can reset a WAL generation, and a reset absorbs the outgoing WAL into the `.db`
  # with `synchronous=FULL` (two fsync-bearing checkpoints) inline before its ack — which used to
  # head-of-line block every OTHER shard hashed to this worker (1/8 of the shards) for the whole
  # absorb. Now the worker hands that frame to a task and keeps serving other shards; every later
  # frame for the SAME shard queues behind it in `inflight` and is replayed in arrival order when
  # the task reports done, so per-shard order — which `FollowerLog.decide/2` depends on — is
  # unchanged. Durability is untouched: the task replies only after the same apply path returns.
  # `inflight` is `%{shard_id => {task_pid, queued_frames_reversed}}`.
  defp worker_loop(sock, name, reader, closed, seeds, inflight) do
    receive do
      {:frame, frame, size} ->
        if :atomics.get(closed, 1) == 1 do
          discard_seeds(seeds)
          await_tasks(inflight)
        else
          {seeds, inflight} = run_frame(sock, name, reader, seeds, inflight, frame, size)
          worker_loop(sock, name, reader, closed, seeds, inflight)
        end

      {:task_done, shard_id} ->
        {queued, inflight} = pop_inflight(inflight, shard_id)

        if :atomics.get(closed, 1) == 1 do
          # The queued frames' replies could never be sent; the primary re-sends them.
          Enum.each(queued, fn {_frame, size} -> send(reader, {:frame_done, size}) end)
          discard_seeds(seeds)
          await_tasks(inflight)
        else
          {seeds, inflight} =
            Enum.reduce(queued, {seeds, inflight}, fn {frame, size}, {seeds, inflight} ->
              run_frame(sock, name, reader, seeds, inflight, frame, size)
            end)

          worker_loop(sock, name, reader, closed, seeds, inflight)
        end

      :stop ->
        discard_seeds(seeds)
        await_tasks(inflight)
    end
  end

  # Frames for a shard with a task in flight queue behind it (arrival order kept, reversed).
  defp run_frame(sock, name, reader, seeds, inflight, frame, size) do
    shard_id = frame_shard_id(frame)

    case inflight do
      %{^shard_id => {task, queued}} ->
        {seeds, %{inflight | shard_id => {task, [{frame, size} | queued]}}}

      _ ->
        case frame do
          # NOT while this connection has a seed in flight for the shard: that seed holds the shard
          # lock as THIS process, and a push "during its own seed" must run here, reentrant on it
          # (`follower_shard_lock_test`), not in a task that would be refused as a second holder.
          %Protocol.Push{offset: 0} when not is_map_key(seeds, shard_id) ->
            task = spawn_push_task(sock, name, reader, frame, size)
            {seeds, Map.put(inflight, shard_id, {task, []})}

          _ ->
            seeds = handle_frame(sock, name, seeds, frame)
            send(reader, {:frame_done, size})
            {seeds, inflight}
        end
    end
  end

  # LINKED to the worker, like the worker is to the connection: a crashing task takes its worker
  # (and so the connection) down rather than leaving its shard silently unanswered. The shard lock
  # is taken inside `handle_push/2`, so the TASK is its holder.
  defp spawn_push_task(sock, name, reader, %Protocol.Push{shard_id: shard_id} = push, size) do
    worker = self()
    binding = Protocol.binding()

    :erlang.spawn_opt(
      fn ->
        # Labelled for `chaos.sh repl-cpu` role attribution (expert review 2026-10-20 #26).
        Process.set_label(:replication_follower_push_task)
        Protocol.adopt_binding(binding)
        _ = handle_frame(sock, name, %{}, push)
        send(reader, {:frame_done, size})
        send(worker, {:task_done, shard_id})
      end,
      [:link, fullsweep_after: 0]
    )
  end

  defp pop_inflight(inflight, shard_id) do
    {{_task, queued}, inflight} = Map.pop!(inflight, shard_id)
    {Enum.reverse(queued), inflight}
  end

  # A stopping worker finishes the frames its tasks are IN THE MIDDLE OF (the same "one frame"
  # overlap a stopping worker always had) and drops what queued behind them.
  defp await_tasks(inflight) when map_size(inflight) == 0, do: :ok

  defp await_tasks(inflight) do
    receive do
      {:task_done, shard_id} -> await_tasks(Map.delete(inflight, shard_id))
      {:frame, _frame, _size} -> await_tasks(inflight)
      :stop -> await_tasks(inflight)
    end
  end

  # SCOPE (fix-review R1-3): the refusal lasts only as long as the coordinator is registered. After
  # an idle-drop a still-alive DEPOSED owner can re-seed this node again; that is bounded by its own
  # self-fence (it cannot keep a quorum or flush under a stale epoch). A durable lineage floor that
  # outlives the coordinator was the rejected alternative (user decision 2026-10-10).
  defp coordinated_here?(name, shard_id) do
    # Cached in persistent_term at init: the setting is fixed for the follower's life, and the ETS
    # row it replaces was read on every push (expert review 2026-10-10 #P4).
    case :persistent_term.get({__MODULE__, :coordinator_registry, name}, nil) do
      # Only a coordinator that HOLDS the lease counts (`Fathom.Shard` marks its registry entry
      # `:lease_held` once `acquire_lease` succeeded). A coordinator is registered BEFORE the lease
      # is acquired, and while that acquire is pending a legitimate owner on another node may still
      # be shipping to us: refusing it would cost that owner this follower's ack (`:stale_epoch` is
      # a settled reject, so its quorum shrinks) for nothing.
      nil ->
        false

      registry ->
        match?([{_pid, :lease_held}], Registry.lookup(registry, shard_id))
    end
  rescue
    # Follower or registry not running: nothing is coordinated here.
    ArgumentError -> false
  end

  defp refused_coordinated(shard_id) do
    :telemetry.execute([:fathom, :replication, :refused_coordinated], %{count: 1}, %{
      shard_id: shard_id
    })
  end

  # A failed reply send means the connection is going away; the reader sees it on its next recv and
  # closes everything, so the worker only has to not crash on it.
  defp reply(sock, iodata), do: _ = :gen_tcp.send(sock, iodata)

  # A NODE THAT COORDINATES A SHARD IS NOT ITS REPLICA (expert review 2026-10-10 #1).
  #
  # Promotion fences the replica (lineage raised, torn) and then `forget/2`s it, which removes the
  # row the fence lived in. The deposed owner's next push was then `:unknown_shard`, its Session
  # re-seeded, and `stale_lineage_seed?/2` (which needs a row to compare against) accepted the seed
  # — so the deposed owner got acks from the NEW owner's own node and counted them toward its
  # quorum: writes acked by a primary the real owner never sees. While a coordinator for the shard
  # is registered here this node is the primary, never a follower of it, so every wire frame that
  # would create or mutate a replica is refused. No persistent fence row: the registry entry is the
  # fence, and it ends exactly when the coordinator stops, after which this node is a legitimate
  # follower again. Applies to WIRE frames only — `Recovery`'s own pull drives `begin_seed/3`
  # directly on a shard it is opening, with its coordinator already registered, and must not trip it.
  defp handle_frame(sock, name, seeds, %Protocol.Push{} = push) do
    if coordinated_here?(name, push.shard_id) do
      refused_coordinated(push.shard_id)
      reply(sock, Protocol.encode_reject(push.shard_id, :stale_epoch, 0))
    else
      reply(sock, handle_push(name, push))
    end

    seeds
  end

  defp handle_frame(_sock, name, seeds, %Protocol.SeedBegin{} = begin) do
    if coordinated_here?(name, begin.shard_id) do
      refused_coordinated(begin.shard_id)
      # Failed, not absent, so `seed_end` answers a reject rather than "no seed in flight".
      Map.put(discard_seed(seeds, begin.shard_id), begin.shard_id, %{name: name, failed: true})
    else
      begin_seed(name, seeds, begin)
    end
  end

  defp handle_frame(_sock, _name, seeds, {:seed_chunk, shard, part, seq, chunk}),
    do: write_chunk(seeds, shard, part, seq, chunk)

  defp handle_frame(sock, name, seeds, {:seed_end, shard}) do
    {result, seeds} = finish_seed(name, seeds, shard)
    reply(sock, encode_seed_result(shard, result))
    seeds
  end

  defp handle_frame(sock, name, seeds, {:position_query, shard}) do
    # How far along our replica is, so a node taking over the shard can decide whether we are worth
    # pulling from. Read-only and cheap on purpose: this is asked of every peer on a cold open, so
    # it must never touch the database or the object store.
    #
    # A TORN replica never OFFERS its position (`Promote.fresher?/2` would refuse it on arrival, and
    # a peer would first pay a whole database transfer to be told so), but it must not answer
    # "nothing" either: that was counted as a DEFINITE answer toward the responder floor while the
    # torn replica may hold the acked tail (expert review 2026-10-13 #5). It answers `:torn`, which
    # the querier treats as silent. An old querier cannot decode the code and also reads silent.
    answer =
      case state_of(name, shard) do
        %{torn: true} -> :torn
        other -> other
      end

    reply(sock, Protocol.encode_position(shard, answer))
    seeds
  end

  defp handle_frame(sock, name, seeds, {:replica_request, shard}) do
    _ = send_replica(sock, name, shard)
    seeds
  end

  defp handle_frame(sock, _name, seeds, {:seed_abort, shard}) do
    # The primary found its two halves no longer belong together. Drop the partial files and
    # answer, so the sender is not left waiting out its seed timeout.
    seeds = discard_seed(seeds, shard)
    reply(sock, Protocol.encode_reject(shard, :internal, 0))
    seeds
  end

  # THE ISOLATION GATE (expert review 2026-08-20 #1).
  #
  # `Protocol.decode/1` extracts `shard_id` as an arbitrary length-prefixed binary — any bytes at
  # all, including `..`, `/` and NUL — and every handler below turns it straight into a filesystem
  # path via wal_path/2 or db_path/2. Unauthenticated: this listener accepts any connection, and
  # the rollout runbook (`config/runtime.exs`) tells operators to enable REPLICATION_LISTEN
  # fleet-wide BEFORE any node ships, so on an A2 fleet it is live on every node.
  #
  # A `seed_begin(shard_id: "../fathom_shards/victim")` + chunks + `seed_end` had `install/3`
  # File.rename an attacker-supplied SQLite file over another tenant's LIVE database — and the
  # defaults make that a single `../`, since Follower.default_dir/0 and Shard.data_dir/0 are
  # literal siblings under System.tmp_dir!(). `forget/2` gave the same primitive for File.rm/1 and
  # `apply_write/4` for arbitrary append.
  #
  # Validating here rather than in each handler is deliberate: the frame boundary is the ONE place
  # every path flows through, and `Fathom.ShardId.valid?/1` is the allowlist the rest of the
  # codebase already enforces at exactly this boundary (`Snapshots`' id pattern, `Recovery`'s
  # pinned `^shard_id`). Deliberately NOT `Path.expand` +
  # prefix comparison: a second mechanism would drift from the first.
  #
  # An unparseable IDENTIFIER is as much a framing failure as an unparseable frame, so this returns
  # the shape the existing `{:error, reason}` clause already handles — log, discard partial seeds,
  # close. Reading on would mean trusting the rest of a stream a hostile or desynced peer produced.
  defp decode_validated(bytes) do
    case Protocol.decode(bytes) do
      {:ok, frame} = ok ->
        case frame_shard_id(frame) do
          nil -> ok
          id -> if Fathom.ShardId.valid?(id), do: ok, else: {:error, {:invalid_shard_id, id}}
        end

      other ->
        other
    end
  end

  # What a dispatched frame holds in its worker's mailbox, for the `@max_outstanding_bytes` bound.
  #
  # THE DECODED SIZE, NOT THE WIRE SIZE (expert review 2026-10-08 #17). Push payloads are zstd on the
  # wire by default (4f3bcfe) and the reader decompresses before dispatch, so a queued push holds its
  # whole raw payload while the bound counted the compressed bytes — loosening the 16 MiB cap c61cbe3
  # set by the compression ratio (13.6x on TPC-B, far more for zero-filled pages), in the subsystem
  # whose history includes multi-GiB binary OOMs. The larger of the two, so an uncompressed frame
  # costs exactly what it did.
  @doc false
  @spec held_bytes(term(), non_neg_integer()) :: non_neg_integer()
  def held_bytes(%Protocol.Push{payload: payload}, wire) when is_binary(payload),
    do: max(wire, byte_size(payload))

  def held_bytes({:seed_chunk, _id, _part, _seq, chunk}, wire) when is_binary(chunk),
    do: max(wire, byte_size(chunk))

  def held_bytes(_frame, wire), do: wire

  defp frame_shard_id(%Protocol.Push{shard_id: id}), do: id
  defp frame_shard_id(%Protocol.SeedBegin{shard_id: id}), do: id
  defp frame_shard_id({:seed_chunk, id, _part, _seq, _chunk}), do: id
  defp frame_shard_id({:seed_end, id}), do: id
  defp frame_shard_id({:position_query, id}), do: id
  defp frame_shard_id({:replica_request, id}), do: id
  defp frame_shard_id({:seed_abort, id}), do: id
  defp frame_shard_id(_), do: nil

  # ------------------------------------------------------------------------------------------
  # serving a pull — the survivor-selection direction
  # ------------------------------------------------------------------------------------------
  #
  # Answering `replica_request` streams OUR replica back to the asker, using the same
  # `seed_begin` / chunks / `seed_end` frames a primary uses to seed us. Nothing new is invented on
  # either side: the asker drives this module's own sink, so a pulled copy is indistinguishable
  # from a pushed one and `Promote` needs no new case.
  #
  # THE FILES ARE THE SOURCE OF TRUTH for generation, salt and offset — the same rule `recover/3`
  # follows, and for the same reason. Only `epoch` comes from ETS, because it is the primary's
  # lease epoch and appears nowhere on disk.
  #
  # What can move under us while we read, and what catches it:
  #
  #   * a PUSH APPENDS to the `-wal`. Harmless: we declared a byte count up front and stream that
  #     prefix. The extra bytes are simply not ours to send.
  #   * a PUSH RESETS the `-wal` (new epoch or new generation — `apply_write` truncates). The
  #     header's generation and salt both move, which is what the before/after check reads.
  #   * a full RE-SEED renames a new `.db` into place. The `-wal` is rewritten with it, so the same
  #     check fires. A re-seed that landed on byte-identical generation AND salt would slip
  #     through, and that is accepted: it means the two copies agree on lineage and position, and
  #     the asker still runs `quick_check` before serving a byte of it.
  defp send_replica(sock, name, shard_id) do
    case replica_offer(name, shard_id) do
      {:ok, offer} -> stream_replica(sock, name, shard_id, offer)
      :none -> :gen_tcp.send(sock, Protocol.encode_reject(shard_id, :unknown_shard, 0))
      {:error, _} -> :gen_tcp.send(sock, Protocol.encode_reject(shard_id, :internal, 0))
    end
  end

  # `offerable/2`, NOT `state_of/2` (expert review 2026-08-24 #13). `torn` was filtered in exactly
  # ONE place on the serving side — the `position_query` handler — while `replica_request` came
  # through here and applied no torn check at all. So a peer that became torn between answering
  # the position query and receiving the replica request handed over the incoherent `.db`/`-wal`
  # pair, and `Recovery` uses two separate TCP connections for those, with a torn transition
  # happening on every generation reset (i.e. every primary checkpoint).
  #
  # The pulling node could not tell: `torn` is not a field in the position frame, so
  # `Recovery.choose/3` cannot re-derive it, and the installer records the bytes through
  # `Follower.finish_seed/3` → `FollowerLog.seeded/4`, which stamps `torn: false` on the reasoning
  # that "a seed is the ONE event that rebuilds `.db` and `-wal` together" — true when the source
  # is a PRIMARY, false when the source is a follower. `Promote.stage/3`'s `quick_check` then
  # passes (it did in the 2026-08-12 rig failure, which is why `torn` exists at all) and the
  # tenant is served an empty or partial database over a working stored object, which is then
  # published to S3 under the lease fence.
  #
  # `Promote.fresher?/2`'s comment already claims this clause "keeps a torn replica from being
  # promoted locally AND from being pulled across the fleet". The fleet half rested on a filter
  # the pull path did not run; now both frames sit behind the same predicate, which is what
  # `offerable/2` was split out of `state_of/2` for.
  #
  # The panel also floated carrying `torn` in `SeedBegin` so the PULLER could verify. Not done:
  # that is a wire change, and it defends against a source that lies about itself, which is not
  # the threat model here — peers are trusted, and the race described is closed at the source.
  defp replica_offer(name, shard_id) do
    with state when state != nil <- offerable(name, shard_id),
         {:ok, header} <- Wal.read(wal_path(name, shard_id)),
         {:ok, %{size: db_size}} <- Fathom.FS.stat(db_path(name, shard_id)) do
      {:ok,
       %{
         epoch: state.epoch,
         # Passed on rather than re-derived: a pulled replica must rank against the object the same
         # way a pushed one does, and this node has no other source for the primary's lineage.
         lineage: Map.get(state, :lineage, 0),
         wal_gen: gen_of(header),
         salt1: salt_of(header),
         wal_size: size_of(header),
         wal_offset: offer_offset(header, state),
         db_size: db_size,
         header: header,
         state: state
       }}
    else
      nil -> :none
      {:error, :enoent} -> :none
      {:error, reason} -> {:error, reason}
    end
  rescue
    ArgumentError -> :none
  end

  defp stream_replica(sock, name, shard_id, offer) do
    begin = %Protocol.SeedBegin{
      shard_id: shard_id,
      epoch: offer.epoch,
      wal_gen: offer.wal_gen,
      salt1: offer.salt1,
      wal_offset: offer.wal_offset,
      db_size: offer.db_size,
      wal_size: offer.wal_size,
      lineage: offer.lineage
    }

    with :ok <- :gen_tcp.send(sock, Protocol.encode_seed_begin(begin)),
         :ok <- send_part(sock, shard_id, :db, db_path(name, shard_id), offer.db_size),
         :ok <- send_part(sock, shard_id, :wal, wal_path(name, shard_id), offer.wal_size),
         {:ok, after_} <- Wal.read(wal_path(name, shard_id)),
         :ok <- stable?(offer.header, after_),
         :ok <- db_only_unchanged(name, shard_id, offer) do
      :gen_tcp.send(sock, Protocol.encode_seed_end(shard_id))
    else
      other ->
        Logger.warning("replication pull of #{shard_id} aborted: #{inspect(other)}")
        # The asker is mid-stream, so the honest answer is the abort frame it already understands
        # rather than silence — it drops the partial files and falls back to the stored object.
        _ = :gen_tcp.send(sock, Protocol.encode_seed_abort(shard_id))
        :ok
    end
  end

  defp send_part(_sock, _shard_id, _part, _path, 0), do: :ok

  defp send_part(sock, shard_id, part, path, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          send_chunks(sock, shard_id, part, fd, 0, 0, size)
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp send_chunks(_sock, _shard_id, _part, _fd, offset, _seq, size) when offset >= size, do: :ok

  defp send_chunks(sock, shard_id, part, fd, offset, seq, size) do
    len = min(chunk_bytes(), size - offset)

    case :file.pread(fd, offset, len) do
      {:ok, bin} when byte_size(bin) == len ->
        case :gen_tcp.send(sock, Protocol.encode_seed_chunk(shard_id, part, seq, bin)) do
          :ok -> send_chunks(sock, shard_id, part, fd, offset + len, seq + 1, size)
          {:error, reason} -> {:error, reason}
        end

      {:ok, _short} ->
        {:error, :short_read}

      :eof ->
        {:error, :short_read}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp chunk_bytes,
    do: Application.get_env(:fathom, :replication_seed_chunk_bytes, 4 * 1024 * 1024)

  defp gen_of(:empty), do: 0
  defp gen_of(%{ckpt_seq: seq}), do: seq
  defp salt_of(:empty), do: 0
  defp salt_of(%{salt1: s}), do: s
  defp size_of(:empty), do: 0
  defp size_of(%{size: s}), do: s

  defp stable?(:empty, :empty), do: :ok
  defp stable?(%{ckpt_seq: g, salt1: s}, %{ckpt_seq: g, salt1: s}), do: :ok
  defp stable?(_, _), do: {:error, :changed_during_pull}

  # A replica whose `-wal` is EMPTY holds everything in its `.db`, so its offset is the position
  # the `.db` holds (`record_db_position/5`), not the WAL's zero bytes. Offering 0 made the puller
  # install `{lineage, ordinal, 0}`: below the position it was chosen on and, against an object
  # stamped mid-generation, below the object too — so the pulled replica was refused at the
  # recheck and the acked tail it carried was dropped for the stale object (expert review
  # 2026-10-15 #4 follow-up). A non-empty WAL keeps offering its own size, as before.
  defp offer_offset(:empty, state), do: state.next_offset
  defp offer_offset(header, _state), do: size_of(header)

  # `stable?/2` is trivially true for `:empty` on both sides, which cannot tell "nothing moved"
  # from "a seed with an empty WAL replaced the `.db` mid-stream". The offset streamed above is the
  # STATE's, so for a `.db`-only offer the state must be unchanged too, or the puller would stamp
  # the old position on a different database.
  defp db_only_unchanged(name, shard_id, %{header: :empty, state: state}) do
    if offerable(name, shard_id) == state, do: :ok, else: {:error, :changed_during_pull}
  end

  defp db_only_unchanged(_name, _shard_id, _offer), do: :ok

  # ------------------------------------------------------------------------------------------
  # streamed seeding
  # ------------------------------------------------------------------------------------------
  #
  # A seed is a whole database, so it arrives as `seed_begin` + N `seed_chunk`s + `seed_end`
  # rather than one frame. Bytes land in `.seeding` temp files and are installed by rename **only**
  # on `seed_end`.
  #
  # That deferral is the property worth having, and it is not just about memory. A seed interrupted
  # half-way — dropped connection, aborted by the primary, a chunk that never arrived — leaves the
  # follower with NO state for the shard, so the next push is refused `:unknown_shard` and it is
  # seeded again. The alternative is a database missing pages that opens cleanly and reads as
  # valid, which is the failure mode this whole module is built to avoid.
  #
  # Nothing is acked until the install succeeds, so the primary never records a follower as holding
  # bytes it does not have.

  @doc false
  @spec begin_seed(atom(), map(), Protocol.SeedBegin.t()) :: map()
  def begin_seed(name, seeds, %Protocol.SeedBegin{} = b) do
    # A second `seed_begin` for a shard already streaming means the previous one will never
    # complete; drop its files rather than leak them.
    seeds = discard_seed(seeds, b.shard_id)

    # THE WHOLE SEED HOLDS THE SHARD'S LOCK (expert review 2026-10-08 #5), from here until its
    # install or discard, so no other connection's push or seed can mutate the replica in between —
    # and the checks below read a state nothing else can move under them.
    id = b.shard_id

    case lock_shard(name, id, lock_wait_ms()) do
      {:ok, lock} ->
        case checked_begin(name, seeds, b, lock) do
          %{^id => %{lock: ^lock}} = opened ->
            opened

          refused ->
            release_reservation(name, id)
            unlock_shard(name, lock)
            refused
        end

      {:error, reason} ->
        Logger.warning(
          "replication follower refusing to seed #{b.shard_id}: its replica is busy " <>
            "(#{inspect(reason)}) — another connection is mutating it"
        )

        # Failed, not absent, so `seed_end` answers a reject the primary re-seeds on.
        Map.put(seeds, b.shard_id, %{name: name, failed: true})
    end
  end

  defp checked_begin(name, seeds, b, lock) do
    cond do
      # Never re-create a replica of a DELETED tenant (expert review 2026-09-29 #5): after
      # `Tombstones` erases it, a straggling push answers `:unknown_shard` and the primary's reflex
      # is to seed it straight back. Dropping the begin leaves no temps and never acks.
      Fathom.Tenants.Tombstones.tombstoned?(b.shard_id) ->
        Logger.info("replication follower refusing to seed #{b.shard_id}: tenant is deleted")
        seeds

      # A seed from a LOWER lineage than the replica we already hold is a deposed primary — or a
      # REPLAYED `seed_begin` (expert review 2026-09-29 #17): the frame MAC covers the header but no
      # nonce, so a captured seed-begin verifies forever. Lineage is the monotonic ownership counter,
      # so no legitimate seed ever goes backwards. The same fence `FollowerLog.decide/2` applies to
      # pushes; only fires when both sides state one (0 = unstated). This bounds a replay to the
      # CURRENT lineage; binding frames to a connection is the wire change parked with the finding.
      stale_lineage_seed?(name, b) ->
        Logger.warning(
          "replication follower refusing to seed #{b.shard_id}: seed lineage #{b.lineage} is " <>
            "below the replica's (deposed primary or replayed frame)"
        )

        seeds

      reserve_headroom?(name, b) ->
        open_seed_temps(name, seeds, b, lock)

      true ->
        refuse_seed(name, seeds, b)
    end
  end

  defp stale_lineage_seed?(name, %Protocol.SeedBegin{shard_id: id, lineage: pushed})
       when is_integer(pushed) and pushed > 0 do
    case state_of(name, id) do
      %{lineage: mine} when is_integer(mine) and mine > pushed -> true
      _ -> false
    end
  rescue
    ArgumentError -> false
  end

  defp stale_lineage_seed?(_name, _begin), do: false

  # DISK BACK-PRESSURE ON THE REPLICA STORE (expert review 2026-08-20 #23).
  #
  # A node acting as a follower stores a full `.db` + `-wal` copy of EVERY shard it follows, and
  # this store had no bound of any kind: no count cap, no byte cap, no free-space floor, no
  # retention. `forget/2` is called from promotions and from tombstoning (a deleted tenant's replica
  # is erased, expert review 2026-09-29 #5) — never for a live shard. It grows monotonically
  # from OTHER NODES' write traffic, and both directories default under `System.tmp_dir!()`, so out
  # of the box the replica store and the live shard data dir SHARE A VOLUME.
  #
  # The asymmetry with the warm cache is the tell. That cache holds a READ copy whose loss costs
  # only failover latency, and it has three independent bounds plus a disk_pressure event. This one
  # holds acked, quorum-durable writes and had a gauge and an alert rule.
  #
  # What a full volume does here is the unbounded-RPO failure AGENTS.md already documents: every
  # cold-open `pull` and every dirty shard's `VACUUM INTO` fails, while SQLite's own small WAL
  # appends keep succeeding — so this node's OWN tenants keep being ACKED writes that can never be
  # made durable, caused by its role as somebody else's follower, with no local signal.
  #
  # Refusing a NEW seed is the half that is unambiguous: a shard we do not yet hold is one whose
  # RPO simply stays at the stored object, which is the pre-A2 behaviour and always correct. What
  # to SHED once already full is a policy question (which replica, and what it does to the quorum's
  # effective redundancy) and is deliberately not decided here.
  #
  # RESERVED BYTES (expert review 2026-10-12 #18). The check used to read the volume's free space
  # and subtract only THIS seed's size, so N seeds beginning together each saw the same free space
  # and each passed — together they could blow through the floor. Each seed now RESERVES its
  # incoming bytes before checking and the check subtracts every live reservation, so concurrent
  # seeds see each other. Reserve-then-check (not check-then-reserve) so a race errs toward
  # refusing: two simultaneous begins may both refuse, never both pass. A refused seed is simply
  # re-seeded later, which is the documented cost of back-pressure.
  #
  # The reservation is released at install (success or failure), at discard, and when the begin is
  # refused. A crashed worker never gets to release, so a reservation whose owner is dead is
  # pruned on the next read rather than leaking free space forever. Bytes a seed has already
  # written are counted twice (reservation AND the lower free space) — conservative by design.
  #
  # The volume read is cached ~1 s and timeout-bounded: `:disksup` is a single process and a
  # per-seed `:infinity` call let a wedged disksup stall every seeding worker.
  defp reserve_headroom?(name, %Protocol.SeedBegin{} = b) do
    incoming = (b.db_size || 0) + (b.wal_size || 0)
    reserve(name, b.shard_id, incoming)

    ok? =
      case disk_free(name) do
        # Cannot read the volume — fail OPEN (as the removed WarmFollower did). Refusing to
        # replicate because a stat failed would turn an observability gap into a durability one.
        :error -> true
        {:ok, free} -> free - reserved_bytes(name) >= disk_free_floor_bytes()
      end

    unless ok?, do: release_reservation(name, b.shard_id)
    ok?
  end

  # Reservation rows live in the lock table under `{:reserved, shard_id}` keys (a tuple, so they can
  # never collide with a binary shard id's lock row); the disk-info cache under `{:disk, dir}`.
  defp reserve(name, shard_id, bytes) do
    :ets.insert(locks(name), {{:reserved, shard_id}, self(), bytes})
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec release_reservation(atom(), String.t()) :: :ok
  def release_reservation(name, shard_id) do
    :ets.match_delete(locks(name), {{:reserved, shard_id}, self(), :_})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Sum of live reservations; a row whose owner process died (crash path) is dropped on the way.
  defp reserved_bytes(name) do
    tab = locks(name)

    :ets.foldl(
      fn
        {{:reserved, _}, pid, bytes} = row, acc ->
          if Process.alive?(pid) do
            acc + bytes
          else
            :ets.delete_object(tab, row)
            acc
          end

        _other, acc ->
          acc
      end,
      0,
      tab
    )
  rescue
    ArgumentError -> 0
  end

  @disk_cache_ms 1_000
  @disk_timeout_ms 2_000

  defp disk_free(name) do
    dir = dir(name)
    tab = locks(name)
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(tab, {:disk, dir}) do
      [{_, at, result}] when now - at < @disk_cache_ms ->
        result

      _ ->
        result = read_disk_free(dir)
        :ets.insert(tab, {{:disk, dir}, now, result})
        result
    end
  rescue
    ArgumentError -> :error
  end

  defp read_disk_free(dir) do
    task = Task.async(fn -> Fathom.Admin.Measurements.disk_info(dir) end)

    case Task.yield(task, @disk_timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, %{free_bytes: free}}} -> {:ok, free}
      _ -> :error
    end
  end

  defp refuse_seed(name, seeds, %Protocol.SeedBegin{} = b) do
    Logger.warning(
      "replication follower REFUSING to seed #{b.shard_id}: the replica volume (#{dir(name)}) is " <>
        "below its free-space floor. That shard's RPO stays at its stored object — the pre-A2 " <>
        "behaviour — which is correct; a full volume would instead break THIS node's own tenants, " <>
        "whose flushes and cold-open pulls all need the same disk. Raise " <>
        "REPLICATION_DISK_FREE_FLOOR_BYTES, give REPLICATION_DIR its own volume, or add space."
    )

    :telemetry.execute([:fathom, :replication, :disk_pressure], %{count: 1}, %{
      shard_id: b.shard_id
    })

    seeds
  end

  defp disk_free_floor_bytes,
    do: Application.get_env(:fathom, :replication_disk_free_floor_bytes, 1024 * 1024 * 1024)

  # UNIQUE TEMP NAMES PER SEED (expert review 2026-10-08 #5 tier c). They were fixed per shard
  # (`<db>.seeding`), so a second connection's `seed_begin` reopened the SAME inode with `:write`
  # and truncated it while the first seed's fd was still writing into it: the first seed's later
  # chunks landed inside whatever the second had installed, its install removed the live `-wal`
  # before failing its rename, and its discard deleted the other seed's temp by path. The shard
  # lock now keeps two seeds of one shard from overlapping, but a lock taken over by age leaves the
  # overtaken holder alive — so each seed's files must be its own, and the paths ride in its state.
  defp open_seed_temps(name, seeds, %Protocol.SeedBegin{} = b, lock) do
    tag = Integer.to_string(System.unique_integer([:positive]))
    db_temp = seeding_path(db_path(name, b.shard_id), tag)
    wal_temp = seeding_path(wal_path(name, b.shard_id), tag)

    with {:ok, db_fd} <- :file.open(db_temp, [:write, :raw, :binary]),
         {:ok, wal_fd} <- open_or_close(wal_temp, db_fd) do
      Map.put(seeds, b.shard_id, %{
        name: name,
        lock: lock,
        db_temp: db_temp,
        wal_temp: wal_temp,
        epoch: b.epoch,
        wal_gen: b.wal_gen,
        salt1: b.salt1,
        wal_offset: b.wal_offset,
        db_size: b.db_size,
        wal_size: b.wal_size,
        # Carried through the seed so `finish_seed/3` can stamp it (expert review 2026-08-24 #12).
        # 0 when the sender predates the wire field or had none to claim.
        lineage: b.lineage,
        # The primary's ordinal for the seeded WAL (rig 2026-10-14 B1), stamped by `do_install/3`
        # under the shard lock. 0 = not stated. A pulled seed's `stamp_seed_ordinal/3` runs after
        # this and overrides it with the ordinal the offer was ranked on.
        wal_ordinal: b.wal_ordinal,
        db_fd: db_fd,
        wal_fd: wal_fd,
        db_written: 0,
        wal_written: 0,
        db_seq: 0,
        wal_seq: 0,
        failed: false
      })
    else
      {:error, reason} ->
        Logger.error(
          "replication seed could not open temps for #{b.shard_id}: #{inspect(reason)}"
        )

        Enum.each([db_temp, wal_temp], &File.rm/1)

        # Recorded as failed rather than absent so `seed_end` has something to refuse — an absent
        # entry and a broken one must not be told apart by the primary, but they must both reject.
        Map.put(seeds, b.shard_id, %{name: name, failed: true})
    end
  rescue
    ArgumentError -> Map.put(seeds, b.shard_id, %{name: name, failed: true})
  end

  defp open_or_close(path, other_fd) do
    case :file.open(path, [:write, :raw, :binary]) do
      {:ok, fd} ->
        {:ok, fd}

      {:error, _} = err ->
        _ = :file.close(other_fd)
        err
    end
  end

  @doc false
  @spec write_chunk(map(), String.t(), :db | :wal, non_neg_integer(), binary()) :: map()
  def write_chunk(seeds, shard_id, part, seq, bytes) do
    case Map.get(seeds, shard_id) do
      nil -> seeds
      %{failed: true} -> seeds
      state -> Map.put(seeds, shard_id, accept_chunk(state, part, seq, bytes))
    end
  end

  # Every refusal here marks the seed failed rather than raising: chunks carry no reply, so the
  # only way to report is to let `seed_end` reject. Silently accepting any of them would install a
  # database with a hole in it.
  defp accept_chunk(state, part, seq, bytes) do
    {seq_key, written_key, size_key, fd_key} = part_keys(part)

    cond do
      # Out of order or duplicated. TCP does not reorder, but a bug on either side can, and a
      # seed that loses a chunk silently is exactly what the sequence number exists to catch.
      seq != state[seq_key] ->
        fail(state, "chunk #{seq} out of order (expected #{state[seq_key]})")

      # The WAL must not start until the database is whole, or the two halves interleave and
      # `db_written` stops meaning what `seed_end` checks it against.
      part == :wal and state.db_written != state.db_size ->
        fail(state, "wal chunk before the database was complete")

      state[written_key] + byte_size(bytes) > state[size_key] ->
        fail(state, "#{part} overran its declared size")

      true ->
        case :file.write(state[fd_key], bytes) do
          :ok ->
            state
            |> Map.put(seq_key, seq + 1)
            |> Map.put(written_key, state[written_key] + byte_size(bytes))

          {:error, reason} ->
            fail(state, "write failed: #{inspect(reason)}")
        end
    end
  end

  defp part_keys(:db), do: {:db_seq, :db_written, :db_size, :db_fd}
  defp part_keys(:wal), do: {:wal_seq, :wal_written, :wal_size, :wal_fd}

  defp fail(state, why) do
    Logger.error("replication seed failed: #{why}")
    Map.put(state, :failed, true)
  end

  @doc """
  Commit the seed accumulated for `shard_id`, returning `{result, seeds}`.

  `{:ok, wal_offset}` or `{:error, reason}` rather than the encoded reply it used to return, so the
  **pull** side can drive this same sink without a socket to answer on
  (`Fathom.Shard.Replication.Recovery`). The install path is where a partial or transposed stream
  turns into a corrupt tenant database, and a second copy of it written for the pull direction is
  the kind of duplicate that stays subtly out of step until it matters.
  """
  @spec finish_seed(atom(), map(), String.t()) ::
          {{:ok, non_neg_integer()} | {:error, term()}, map()}
  def finish_seed(name, seeds, shard_id) do
    case Map.get(seeds, shard_id) do
      nil ->
        {{:error, :no_seed_in_flight}, seeds}

      %{failed: true} ->
        {{:error, :seed_failed}, discard_seed(seeds, shard_id)}

      %{db_written: db, db_size: db, wal_written: wal, wal_size: wal} = state ->
        # A seed that BEGAN before the tenant was deleted (expert review 2026-10-21 rv6 F5).
        # `checked_begin/4` refuses a seed for a tombstoned shard, but a seed already streaming when
        # the delete lands holds the shard's lock, so `Tombstones`' `forget/2` waits out its lock
        # wait and erases without it, and the install would then put the deleted tenant's replica
        # back on disk. Re-checked here, under the lock this seed holds: the tombstone is in the
        # set before the erase runs, so either this install sees it and discards, or the install
        # finished first and the erase that follows removes it.
        if Fathom.Tenants.Tombstones.tombstoned?(shard_id) do
          Logger.info("replication follower discarding a seed of #{shard_id}: tenant is deleted")
          {{:error, :tombstoned}, discard_seed(seeds, shard_id)}
        else
          {install(name, shard_id, state), Map.delete(seeds, shard_id)}
        end

      _short ->
        # Declared sizes not met. The stream ended early — refuse rather than install a truncated
        # database, which would open cleanly and be missing pages.
        Logger.error("replication seed for #{shard_id} ended short of its declared size")
        {{:error, :short_stream}, discard_seed(seeds, shard_id)}
    end
  end

  # The wire answer is unchanged: every failure above is still one opaque `:internal` reject. The
  # sender cannot act differently on any of them — all of them mean "re-seed" — and naming the
  # internal reason on the wire would only tell an unauthenticated peer about our filesystem.
  defp encode_seed_result(shard_id, {:ok, offset}), do: Protocol.encode_ack(shard_id, offset)

  defp encode_seed_result(shard_id, {:error, _}),
    do: Protocol.encode_reject(shard_id, :internal, 0)

  # Install by rename, `-wal` cleared first.
  #
  # Ordering is the same reasoning the monolithic version carried: an interruption must never leave
  # a WAL whose salts disagree with the database beside it. Removing the old `-wal` before renaming
  # the new `.db` means every intermediate state is either the old pair, a database with no WAL
  # (which SQLite reads as valid if stale), or the new pair.
  #
  # ETS goes last: until it is written the follower reports `:unknown_shard` and gets re-seeded, so
  # a failure anywhere above costs a re-seed rather than a corrupt shard.
  # DURABLE `torn` (expert review 2026-08-20 #11b).
  #
  # `torn` lived only in ETS, which dies with this GenServer. `recover/3` deliberately rebuilds
  # state from the files on boot — the right fix for the re-seed storm it documents — but torn
  # CANNOT be derived from the files: nothing on disk records that the `.db` and `-wal` are a
  # generation apart, and the `-shm` that would hint at it is deleted by `apply_write`. So every
  # recovered shard came back `torn: false`, and a node restart — a deploy, a crash, an OOM-kill,
  # all documented as routine — laundered every quarantined replica on that node into a promotable
  # one. `Promote.fresher?/2` then lets it through and `offerable/2` offers it to the whole fleet.
  #
  # The moduledoc reasons that losing ETS is safe because "the next push is refused with
  # `:unknown_shard`". `recover/3` removed exactly that safety; this restores it for the one bit
  # that mattered.
  #
  # A zero-byte marker beside the replica, written before the ETS row so a crash between the two
  # leaves the SAFE state (marker present, replica treated as torn) rather than the unsafe one.
  # ONLY TOUCHES THE FILE WHEN `torn` ACTUALLY CHANGES. This runs on EVERY push, and the ordinary
  # push is an `:append` that carries the previous torn value forward — so an unconditional
  # write/rm here would add a syscall to the replication hot path for no state change. It did, in
  # the first draft, and the pausable-peer transport test started intermittently reporting
  # `:disconnected` where it expects `:already_in_flight`. An `:ets.lookup` on a
  # `read_concurrency` table is strictly cheaper than the syscall it replaces.
  @doc """
  Record the WAL ordinal a PULLED replica was offered at (expert review 2026-08-26 #2, step 3b).

  A pulled seed installs `FollowerLog.seeded/5`'s state, which states no ordinal — `SeedBegin`
  carries none — so without this the survivor holds a replica `Promote.fresher?/2` can never rank
  and cross-fleet recovery ends where it began: cold-opening the stale object. The number comes
  from the offer the peer was CHOSEN on, which is the only statement about these bytes we have.

  Taking the offer's value is safe in the one direction that matters. If the peer moved between
  offering and streaming it can only have moved FORWARD — a new WAL takes a higher ordinal, a
  checkpoint likewise — so this either matches or UNDER-claims. An under-claim costs at most a
  promotion; an over-claim would cost the acked tail.

  No-op for an unstated (0) ordinal, and for a shard this follower does not hold.
  """
  @spec note_ordinal(atom(), String.t(), non_neg_integer()) :: :ok
  def note_ordinal(name \\ __MODULE__, shard_id, ordinal)

  def note_ordinal(_name, _shard_id, ordinal) when not (is_integer(ordinal) and ordinal > 0),
    do: :ok

  def note_ordinal(name, shard_id, ordinal) do
    # Read-modify-write under the shard's lock (expert review 2026-10-10 #22): a concurrent push's
    # `put_state` between the read and the put was overwritten by this stale copy. A busy replica
    # is skipped — the ordinal is an under-claim hint, and losing it costs at most a promotion.
    _ =
      with_shard_lock(name, shard_id, lock_wait_ms(), fn ->
        case state_of(name, shard_id) do
          %{} = state -> put_state(name, shard_id, %{state | wal_ordinal: ordinal})
          _ -> :ok
        end
      end)

    :ok
  end

  @doc false
  # Ride the offered ordinal in a pulled seed's own state, so `do_install/3` stamps it inside the
  # install — under the lock the seed already holds (expert review 2026-10-10 #22). Recovery used to
  # call `note_ordinal/3` after `finish_seed` had released the lock.
  @spec stamp_seed_ordinal(map(), String.t(), non_neg_integer()) :: map()
  def stamp_seed_ordinal(seeds, shard_id, ordinal) when is_integer(ordinal) and ordinal > 0 do
    case Map.get(seeds, shard_id) do
      %{failed: true} -> seeds
      %{} = seed -> Map.put(seeds, shard_id, Map.put(seed, :wal_ordinal, ordinal))
      nil -> seeds
    end
  end

  def stamp_seed_ordinal(seeds, _shard_id, _ordinal), do: seeds

  defp put_state(name, shard_id, state) do
    tab = table(name)

    {was_torn?, was_lineage, was_ordinal} =
      case :ets.lookup(tab, shard_id) do
        [{^shard_id, %{torn: t} = prev}] ->
          {t, Map.get(prev, :lineage, 0), {Map.get(prev, :wal_ordinal, 0), prev.salt1}}

        # No resident row: rows are derived on first use (expert review 2026-10-21 #5), so a write
        # can land on a replica whose row was never derived (a lineage-0 seed install) or was swept.
        # The sidecars on disk are what the row would have said; assuming "untorn, lineage 0" left a
        # stale torn marker behind an untorn install. `nil` ordinal always rewrites that sidecar.
        _ ->
          {Fathom.FS.exists?(torn_path(name, shard_id)), recovered_lineage(name, shard_id), nil}
      end

    # Marker BEFORE the ETS row, so a crash between the two leaves the SAFE state: the marker
    # present and the replica treated as torn, never the reverse.
    if was_torn? != state.torn, do: sync_torn_marker(name, shard_id, state)
    if state.lineage != was_lineage, do: sync_lineage(name, shard_id, state.lineage)
    ordinal = {Map.get(state, :wal_ordinal, 0), state.salt1}
    if ordinal != was_ordinal, do: sync_ordinal(name, shard_id, ordinal)
    :ets.insert(tab, {shard_id, state})
    touch(name, shard_id)
    state
  end

  # THE LINEAGE FENCE SURVIVES A RESTART (expert review 2026-09-29 #2c). Recovery used to come back
  # at lineage 0, which disarms `FollowerLog`'s lineage fence (it fires only when BOTH sides state
  # one), so the first primary to push after a follower restart was accepted as the owner — a
  # deposed one included. It changes once per ownership, so this is one small write per takeover,
  # not per frame. Written plainly (no fsync): losing it to an OS crash recovers 0, which is exactly
  # the pre-fix behaviour, never a wrong value.
  defp sync_lineage(name, shard_id, lineage) when is_integer(lineage) and lineage > 0,
    do: write_state_file(lineage_path(name, shard_id), Integer.to_string(lineage), :lineage)

  defp sync_lineage(_name, _shard_id, _lineage), do: :ok

  # THE ORDINAL SURVIVES A RESTART TOO (expert review 2026-10-15 #4, default D2). Recovery rebuilt
  # every replica at `wal_ordinal: 0` ("not stated"), which `Promote.fresher?/2` refuses to rank, so
  # after a follower restart (every deploy) a replica holding the acked tail was invisible to
  # failover until its next push, while `Recovery.answered/3` still counted it toward the floor.
  #
  # Persisted WITH THE SALT it describes, as `"<ordinal> <salt1>"`: the coordinator assigns one
  # ordinal per WAL salt (`Fathom.Shard.wal_ordinal/2`), so the pair is a fact about one WAL file.
  # Recovery adopts the ordinal ONLY when the salt matches the recovered `-wal`'s header, which makes
  # the write order against the WAL bytes irrelevant: a crash between the two leaves salts that
  # disagree and recovers 0, the pre-fix answer, never a wrong ordinal on the wrong WAL.
  #
  # Written once per WAL generation (a reset push or a seed), not per frame. Losing it to an OS
  # crash recovers 0, which is safe.
  defp sync_ordinal(name, shard_id, {ordinal, salt1})
       when is_integer(ordinal) and ordinal > 0 and is_integer(salt1) do
    write_state_file(ordinal_path(name, shard_id), "#{ordinal} #{salt1}", :ordinal)
  end

  defp sync_ordinal(name, shard_id, _ordinal) do
    Fathom.FS.rm(ordinal_path(name, shard_id))
    :ok
  end

  @doc false
  # The persisted ordinal for the WAL whose header salt is `salt1`, or 0. Public for the test.
  @spec recovered_ordinal(atom(), String.t(), non_neg_integer()) :: non_neg_integer()
  def recovered_ordinal(name \\ __MODULE__, shard_id, salt1) do
    with {:ok, text} <- Fathom.FS.read(ordinal_path(name, shard_id)),
         [n, s] <- String.split(String.trim(text), " "),
         {ordinal, ""} when ordinal > 0 <- Integer.parse(n),
         {^salt1, ""} <- Integer.parse(s) do
      ordinal
    else
      _ -> 0
    end
  end

  defp recovered_lineage(name, shard_id) do
    with {:ok, text} <- Fathom.FS.read(lineage_path(name, shard_id)),
         {n, ""} when n > 0 <- Integer.parse(String.trim(text)) do
      n
    else
      _ -> 0
    end
  end

  # DURABLE (expert review 2026-09-29 #26): the marker exists to survive a restart (2026-08-20
  # #11b), and a plain `File.write` survives a BEAM restart but not an OS crash, which can lose a
  # just-created file outright. fsync-before-rename, like every other file fathom trusts after a
  # crash. Written only when a replica first goes torn, so the cost is off every hot path.
  defp sync_torn_marker(name, shard_id, %{torn: true}) do
    Fathom.Shard.Storage.atomic_write(torn_path(name, shard_id), "")
  end

  defp sync_torn_marker(name, shard_id, _state) do
    Fathom.FS.rm(torn_path(name, shard_id))
    :ok
  end

  # OWNERSHIP IS RE-CHECKED BEFORE ANYTHING IS RENAMED (expert review 2026-10-08 #5). A seed whose
  # lock was taken over by age (`abandoned_lock?/3`) is no longer the replica's writer — another seed
  # or push owns the files now — so it removes only its own temps and refuses, which the primary
  # answers with a re-seed. The lock is released on every path, success included.
  defp install(name, shard_id, state) do
    synced = sync_seed_temps(state)
    :file.close(state.db_fd)
    :file.close(state.wal_fd)

    cond do
      synced != :ok ->
        Logger.error(
          "replication seed of #{shard_id} could not be synced before install " <>
            "(#{inspect(synced)}); discarding it"
        )

        rm_seed_temps(state)
        {:error, {:sync_failed, synced}}

      owns_lock?(name, shard_id, state.lock) ->
        do_install(name, shard_id, state)

      true ->
        Logger.warning(
          "replication seed of #{shard_id} lost its replica lock before install; discarding it"
        )

        rm_seed_temps(state)
        {:error, :lock_lost}
    end
  after
    release_reservation(name, shard_id)
    unlock_shard(name, state.lock)
  end

  defp do_install(name, shard_id, state) do
    db = db_path(name, shard_id)
    wal = wal_path(name, shard_id)

    # The `.db` is about to be replaced, so whatever the position file said about the old one is
    # void — invalidated BEFORE the rename, never after.
    with :ok <- invalidate_db_position(name, shard_id),
         :ok <- rm_if_present(wal),
         :ok <- Fathom.FS.rename(state.db_temp, db),
         :ok <- Fathom.FS.rename(state.wal_temp, wal) do
      seeded =
        FollowerLog.seeded(
          state.epoch,
          state.wal_gen,
          state.salt1,
          state.wal_offset,
          Map.get(state, :lineage, 0)
        )

      installed =
        put_state(name, shard_id, %{seeded | wal_ordinal: Map.get(state, :wal_ordinal, 0)})

      # A `.db`-ONLY seed — a pulled replica whose source's WAL was empty (`offer_offset/2`) — has
      # a known position for the `.db` alone, so it gets the same record an absorb leaves. Any
      # other seed's `.db` holds an unstated position BEFORE its WAL, so it gets none: if its WAL
      # is later lost, recovery answers torn, never a guess.
      if state.wal_size == 0 and state.wal_offset > 0,
        do: record_db_position(name, shard_id, installed, installed.wal_ordinal, state.wal_offset)

      Logger.debug(
        "replication seeded #{shard_id}: #{state.db_size}B db + #{state.wal_size}B wal " <>
          "at gen #{state.wal_gen} offset #{state.wal_offset}"
      )

      {:ok, state.wal_offset}
    else
      {:error, reason} ->
        Logger.error("replication seed install failed for #{shard_id}: #{inspect(reason)}")
        # The temps are unique to this seed, so nothing else will ever replace (and so remove)
        # them; whatever was not renamed goes now rather than waiting for the boot reaper.
        rm_seed_temps(state)
        {:error, reason}
    end
  rescue
    ArgumentError -> {:error, :follower_stopped}
  end

  @doc false
  @spec discard_seeds(map()) :: map()
  def discard_seeds(seeds), do: Enum.reduce(Map.keys(seeds), seeds, &discard_seed(&2, &1))

  @spec discard_seed(map(), String.t()) :: map()
  defp discard_seed(seeds, shard_id) do
    case Map.pop(seeds, shard_id) do
      {nil, seeds} ->
        seeds

      {state, rest} ->
        if state[:db_fd], do: :file.close(state.db_fd)
        if state[:wal_fd], do: :file.close(state.wal_fd)

        # This seed's OWN temps, by the paths it opened — never by a per-shard name, which another
        # connection's seed of the same shard could be writing (expert review 2026-10-08 #5).
        rm_seed_temps(state)

        if name = state[:name] do
          release_reservation(name, shard_id)
          unlock_shard(name, state[:lock])
        end

        rest
    end
  end

  defp rm_if_present(path) do
    case Fathom.FS.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      other -> other
    end
  end

  defp rm_seed_temps(state) do
    for key <- [:db_temp, :wal_temp], path = state[key], do: Fathom.FS.rm(path)
    :ok
  end

  @seeding_marker ".seeding"
  defp seeding_path(path, tag), do: path <> @seeding_marker <> "." <> tag

  # THE BOOT REAPER for seed temps (expert review 2026-10-08 #5 tier c). Unique names mean a seed
  # whose worker was killed (`close_connection/1`'s `@worker_stop_ms`), or whose follower died under
  # it, leaves files nothing will ever replace — the fixed `.seeding` name used to be overwritten by
  # the shard's next seed. AGE-GATED, because a Follower restart does not stop the old connections'
  # workers (their reader is an unlinked Task), so a seed still streaming into this directory must
  # keep its files: a live seed writes a chunk at least every few seconds, and the Session abandons
  # one after `@default_seed_expiry_ms` (120 s), so a temp untouched for `@stale_seed_temp_s` is
  # dead. Matches the pre-unique `<shard>.db.seeding` name too, so an upgrade does not strand those.
  @stale_seed_temp_s 15 * 60

  defp reap_stale_seed_temps(dir, entries) do
    cutoff =
      System.os_time(:second) -
        Application.get_env(:fathom, :replication_stale_seed_temp_s, @stale_seed_temp_s)

    reaped =
      entries
      |> Enum.filter(&String.contains?(&1, @seeding_marker))
      |> Enum.map(&Path.join(dir, &1))
      |> Enum.count(fn path ->
        match?({:ok, %{mtime: m}} when m < cutoff, Fathom.FS.stat(path, time: :posix)) and
          Fathom.FS.rm(path) == :ok
      end)

    if reaped > 0,
      do: Logger.info("replication follower reaped #{reaped} stale seed temp(s) in #{dir}")
  end

  defp list_dir(dir) do
    case File.ls(dir) do
      {:ok, entries} -> entries
      {:error, _} -> []
    end
  end

  # ERASE REPLICAS OF TENANTS DELETED WHILE THIS NODE WAS DOWN (expert review 2026-09-29 #5; moved
  # out of `init/1` by expert review 2026-10-21 #5). `Tombstones`' erase hook fires only for ids it
  # learns while this follower runs, so a replica of a tenant deleted before boot is erased here,
  # after the listener is up. Until this pass reaches it the bytes are still on disk, but they are
  # never SERVED: every read of a replica goes through `state_of/2`, whose lazy re-derive checks the
  # same tombstone set on both the locked and the busy path (`rederive/2`), and a seed for a
  # tombstoned shard is refused in `checked_begin/4`. User decision 2026-10-21: this window is
  # acceptable for the GDPR erase. One ETS read per `.db`; no file is opened for a live replica.
  defp erase_tombstoned_replicas(name, entries) do
    erased =
      entries
      |> Enum.filter(&String.ends_with?(&1, ".db"))
      |> Enum.map(&String.replace_suffix(&1, ".db", ""))
      |> Enum.filter(&Fathom.Tenants.Tombstones.tombstoned?/1)
      |> Enum.count(fn shard_id -> forget(name, shard_id) == :ok end)

    if erased > 0,
      do: Logger.info("replication follower erased #{erased} replica(s) of deleted tenant(s)")
  end

  # Returns the iodata to send back. All the judgement is in FollowerLog; this only performs it.
  #
  # The rescue covers a real shutdown race, not just a noisy test: if this follower stops while a
  # push is in flight, its ETS table is gone and every `:ets` call raises, taking down the handler
  # Task with an unhandled exit. A primary is waiting on a reply, so the right answer is to refuse
  # the push — which subtracts from its quorum immediately — rather than to die silently and make
  # it wait out the timeout.
  #
  # UNDER THE SHARD'S LOCK (expert review 2026-10-08 #5): decide → absorb → write → put_state is one
  # read-modify-write of the replica, and another connection's worker for the same shard must not
  # interleave with it. A push that cannot get the lock within `lock_wait_ms/0` is refused
  # `:internal` at our real position, the answer a failed write already gives.
  defp handle_push(name, %Protocol.Push{} = push) do
    case lock_shard(name, push.shard_id, lock_wait_ms()) do
      {:ok, lock} ->
        try do
          do_handle_push(name, push)
        after
          unlock_shard(name, lock)
        end

      {:error, reason} ->
        Logger.info(
          "replication follower refusing a push for #{push.shard_id}: its replica is busy " <>
            "(#{inspect(reason)}); the primary re-plans on its next commit"
        )

        :telemetry.execute([:fathom, :replication, :shard_lock_busy], %{count: 1}, %{
          shard_id: push.shard_id
        })

        expected = (state_of(name, push.shard_id) || %{next_offset: 0}).next_offset
        Protocol.encode_reject(push.shard_id, :internal, expected)
    end
  rescue
    ArgumentError ->
      Logger.warning("replication follower is shutting down; refusing #{push.shard_id}")
      Protocol.encode_reject(push.shard_id, :internal, 0)
  end

  # REBUILD THE PER-SHARD STATE FROM THE FILES — ON FIRST USE, NOT AT BOOT.
  #
  # This table is ETS, so it dies with the process while the replicas on disk do not. A restarted
  # node therefore held a full copy of every shard it followed and did not know it: `state_of/2`
  # returned nil, so every push was refused `:unknown_shard` and the primary re-sent an entire
  # DATABASE per shard, and `Promote` — which checks `state_of/2` first — would not promote from
  # bytes sitting right there. At any real follower count that is a re-seed storm after every
  # deploy.
  #
  # ROWS ARE DERIVED ON FIRST USE (expert review 2026-10-21 #5). This used to run for every replica
  # inside `init/1` (`recover/3`), O(replicas) of file I/O before the listener — and, through the
  # application's start order, before Hrana and /health — existed. Since the idle sweep
  # (2026-10-20-2027 #11) `state_of/2` re-derives a missing row under the shard's lock through this
  # same function, so the boot pass is gone. Why a row derived at first use equals the one the boot
  # pass would have inserted:
  #   * SAME FUNCTION, SAME FILES. `rederive_locked/2` calls `recover_shard/3`, which is all the
  #     boot pass called. The replica's files are written only by this follower's own workers under
  #     the shard lock. A push, a promotion fence and an ordinal note read the row through
  #     `state_of/2` — so through this derive — before their first write; a seed install replaces
  #     the whole replica (and a seed stating a lineage reads the row first, for the fence); the
  #     rest are `forget/2` (removes everything, the row included) and the seed temp reaper (temps
  #     only). Between boot and first use, the files a derive reads do not move.
  #   * LINEAGE FENCE: `recovered_lineage/2` reads the sidecar in both cases; the fence in
  #     `FollowerLog.decide/2` and `stale_lineage_seed?/2` sees the same number.
  #   * TORN TAIL: `Wal.read/1`'s `commit_extent` (2026-09-29 #26) and the torn marker are read by
  #     `derive_state/3` in both cases.
  #   * ASK / PROMOTE ANSWERS: the position query, `replica_offer/2`, `Recovery.local_state/2`,
  #     `PromoteOnOpen.replica_state/1` and `Promote` all read through `state_of/2`; none reads the
  #     table directly. A busy lock answers the unlocked derive (see `rederive/2`), which can only
  #     under-claim, and which also refuses a tombstoned shard.
  #   * `put_state/3` on a row that is not resident reads the previous torn/lineage from the
  #     sidecars, not from defaults, so a write that lands before any derive (a lineage-0 seed
  #     install) still clears a stale torn marker exactly as it did over a boot-derived row.
  #   * EPOCH: 0 in both cases (below).
  # What is NOT the same: `replica_shard_ids/1` (the rebalancer's warm set) lists only resident
  # rows, so after a restart it starts empty and refills as pushes and asks arrive — the trade the
  # sweep already accepted (user decision 2026-10-21). And a tombstoned replica's bytes stay on disk
  # until `erase_tombstoned_replicas/2` reaches them after listen, refused meanwhile.
  #
  # THE FILES ARE THE SOURCE OF TRUTH, deliberately, rather than a sidecar counter. Frames are
  # applied with `:file.write` in APPEND mode, so a persisted offset that lagged the file by even
  # one frame would make the primary re-send a range the follower then appends a SECOND time —
  # silent duplication inside the WAL. A WAL header cannot disagree with its own file that way:
  # `Wal.read/1` reports `ckpt_seq`, `salt1` and `size`, which are exactly `wal_gen`, `salt1` and
  # `next_offset`.
  #
  # EPOCH IS RECOVERED AS 0, deliberately low. It is the primary's lease epoch and appears nowhere
  # on disk. Guessing high would make this node reject the real primary as `:stale_epoch` and stall
  # the shard until the epoch caught up; guessing low costs exactly one round trip — the first push
  # is `pushed > epoch`, which routes to `decide_fresh`, which rejects a non-zero offset asking for
  # 0, and the primary answers with `{:reset, 0, _}`. That re-sends the current WAL, not the
  # database, and is what "cheap and self-correcting" looks like next to a full re-seed.
  #
  # A shard whose WAL cannot be read is simply not recovered: it stays unknown and gets a normal
  # seed. Skipping is always safe here; claiming bytes we cannot verify is not.
  defp recover_shard(tab, name, shard_id) do
    if Fathom.Tenants.Tombstones.tombstoned?(shard_id) do
      # A replica of a DELETED tenant (expert review 2026-09-29 #5): the delete happened while this
      # node was down, or before its Follower booted, so `Tombstones`' erase hook had nothing to
      # erase yet. Erase it here rather than recover it. `Tombstones` starts before the Follower in
      # the application tree, but its set is NOT necessarily loaded by the time this runs: it loads
      # in `handle_continue` (expert review 2026-10-21 rv6 F11 corrected the old claim that it was
      # loaded when the Follower starts). An id it has not loaded yet is erased when it is learned,
      # through `Tombstones`' erase hook, which calls `forget/2` once this Follower is running.
      forget(name, shard_id)
      false
    else
      recover_live_shard(tab, name, shard_id)
    end
  end

  defp recover_live_shard(tab, name, shard_id) do
    # `commit_extent`, NOT `size` (expert review 2026-09-29 #26). With `:replication_fsync` off (the
    # default) an OS crash — not a BEAM crash — can leave the WAL with a zero-filled or partial tail,
    # and `size` counted it as held. `complete_through_reset?/2` then compared that inflated offset
    # with the primary's `prev_extent`, passed, and the absorb cleared `torn` over a replica that is
    # missing frames. `Wal.read/1`'s commit extent walks back to the last frame that is a commit
    # under this WAL's salts, which a zeroed tail is not. Frames are written POSITIONALLY
    # (`write_frame/4`), so resuming at the lower offset overwrites the junk rather than appending
    # after it.
    case derive_state(name, shard_id) do
      nil ->
        false

      state ->
        :ets.insert(tab, {shard_id, state})
        touch(name, shard_id)
        true
    end
  end

  # What the replica's files say, or nil when they say nothing (no readable WAL, or no `.db`).
  # The one derive: first use after a restart, the re-derive of a swept row, and the sweep's own
  # equivalence check all call it, so they can never disagree.
  # `log?` false for the sweep's equivalence check, which runs every round on a row it keeps.
  defp derive_state(name, shard_id, log? \\ true) do
    with {:ok, header} <- Wal.read(wal_path(name, shard_id)),
         true <- Fathom.FS.exists?(db_path(name, shard_id)) do
      # Carry `torn` across the restart (expert review 2026-08-20 #11b). `FollowerLog.seeded/4`
      # stamps `torn: false` because a real SEED is the one event that rebuilds both files
      # together — but this is not a seed, it is a recovery, and the marker on disk is the only
      # thing that remembers the two files were a generation apart when we went down.
      torn? = Fathom.FS.exists?(torn_path(name, shard_id))
      lineage = recovered_lineage(name, shard_id)
      recovered_state(name, shard_id, header, torn?, lineage, log?)
    else
      _ -> nil
    end
  end

  # SWEEPING IDLE REPLICA ROWS (expert review 2026-10-20-2027 #11). Every shard this node ever
  # replicated kept a row here until a promotion or a tombstone forgot it — measured 247 B (plus
  # 103 B for its touch row now), ~80 MB per node at a million fleet shards over three nodes,
  # ~800 MB at ten million, nearly all for replicas nobody pushes to.
  # A row untouched (no `put_state/3`) for `:replication_row_idle_ms` (default 10 x
  # `:shard_idle_ms`) is dropped, and `state_of/2` re-derives it from the files on the next use,
  # through the same `derive_state/3` a restarted follower uses on first use.
  #
  # WHY DROPPING IT LOSES NOTHING. Every field of the row is either durable beside the replica or
  # recovered in the safe direction, which is what already makes a follower restart (every deploy)
  # safe:
  #   * `wal_gen`, `salt1`, `next_offset` — the `-wal` header and its commit extent (or, for an
  #     empty WAL, the `.db.position` record, else TORN);
  #   * `torn` — the durable marker, which `put_state/3` writes BEFORE the ETS row, so a torn row is
  #     never swept into an untorn one;
  #   * `lineage`, `wal_ordinal` — their sidecars, written by `put_state/3` when they change;
  #   * `epoch` — nowhere on disk, recovered as 0: the next push takes `decide_fresh/2`, the one
  #     round trip a restart already costs. A replica whose lineage is unstated (0) is NOT swept,
  #     because there the epoch is the only ownership evidence and re-deriving 0 would read as a
  #     new ownership and re-seed it (`FollowerLog.ownership_change?/2`).
  # And the sweep does not trust that argument blindly: under the shard's lock it re-derives the
  # row from the files and drops it only when the result is no less safe than the row — same
  # generation, salt, lineage and ordinal, no more bytes claimed, torn if the row is torn. Anything
  # else (a file write the row is ahead of, a missing sidecar) keeps the row.
  #
  # UNDER THE SHARD'S LOCK, never waited for: a push, seed or promotion holding it is using the row,
  # so the sweep skips the shard this round. The lazy re-derive takes the lock too, so a re-derived
  # row cannot interleave with `forget/2` removing the files.
  #
  # Not re-derived: `replica_shard_ids/1` (the rebalancer's warm set) lists only resident rows, so a
  # replica idle for the sweep window stops advertising warmth until its next push. A shard that
  # idle is not a placement candidate anyway, and listing it would mean a directory scan per beat.
  @sweep_chunk 500

  @doc false
  # Public for the test; the timer calls it every `:replication_row_sweep_ms`.
  @spec sweep_idle_rows(atom()) :: non_neg_integer()
  def sweep_idle_rows(name \\ __MODULE__) do
    cutoff = System.monotonic_time(:second) - div(row_idle_ms(), 1000)
    spec = [{{:"$1", :"$2"}, [{:<, :"$2", cutoff}], [:"$1"]}]
    sweep_chunks(name, :ets.select(touches(name), spec, @sweep_chunk), cutoff, 0)
  rescue
    ArgumentError -> 0
  end

  defp sweep_chunks(_name, :"$end_of_table", _cutoff, swept), do: swept

  defp sweep_chunks(name, {ids, cont}, cutoff, swept) do
    swept = swept + Enum.count(ids, &sweep_row(name, &1, cutoff))
    sweep_chunks(name, :ets.select(cont), cutoff, swept)
  end

  defp sweep_row(name, shard_id, cutoff) do
    case lock_shard(name, shard_id, 0) do
      {:ok, {:held, _} = lock} ->
        try do
          with [{_, touched}] when touched < cutoff <- :ets.lookup(touches(name), shard_id),
               [{_, %{lineage: lineage} = row}] when is_integer(lineage) and lineage > 0 <-
                 :ets.lookup(table(name), shard_id),
               true <- rederivable?(row, derive_state(name, shard_id, false)) do
            :ets.delete(table(name), shard_id)
            :ets.delete(touches(name), shard_id)
            true
          else
            _ -> false
          end
        after
          unlock_shard(name, lock)
        end

      _busy_or_stopped ->
        false
    end
  end

  # The files re-derive this row, or something safer: same WAL identity and ownership, no bytes the
  # row does not claim, and never less torn.
  defp rederivable?(_row, nil), do: false

  defp rederivable?(row, derived) do
    derived.wal_gen == row.wal_gen and derived.salt1 == row.salt1 and
      derived.lineage == row.lineage and
      Map.get(derived, :wal_ordinal, 0) == Map.get(row, :wal_ordinal, 0) and
      derived.next_offset <= row.next_offset and
      (derived.torn or not row.torn)
  end

  defp row_idle_ms do
    Application.get_env(:fathom, :replication_row_idle_ms) ||
      10 * Application.get_env(:fathom, :shard_idle_ms, 60_000)
  end

  defp touch(name, shard_id),
    do: :ets.insert(touches(name), {shard_id, System.monotonic_time(:second)})

  # A miss in `state_of/2`: the shard was never replicated here, or its row was swept. Only the
  # files can say which, and only under the shard's lock (see the sweep section). A busy lock is a
  # push, seed or promotion that is about to write the row itself; answer what the files say
  # without inserting it, never a definite "nothing" over a replica that is on disk (the
  # 2026-10-15 #4 bug class).
  #
  # WHY THE UNLOCKED BUSY-PATH READ IS HARMLESS (integration review of expert review 2026-10-20-2027
  # #11; an explicit `:replica_busy` answer was considered and rejected). Only callers OUTSIDE the
  # lock holder reach it (the holder's own reads are re-entrant): `PromoteOnOpen.replica_state/1`,
  # `Recovery.local_state/2`, `Promote.ensure_replica/2`, the tombstone presence check. It can see a
  # push or seed mid-write, but:
  #   * it can only UNDER-claim. `Wal.read/1`'s commit extent stops at the last commit frame, so
  #     frames a push has written but not committed are not counted; and a reset caught between
  #     rewriting the `-wal` and `put_state/3` reads a new salt whose ordinal sidecar (keyed on the
  #     salt it was written for) is absent, so `wal_ordinal` is 0 and `Promote.fresher?/2` refuses to
  #     rank it. Nothing transient can rank ABOVE what the replica durably holds;
  #   * no caller ACTS on it unlocked. Every promotion goes through `Promote.stage/4`, which takes
  #     this lock and fences with `expect:` the state that was ranked, so a value that moved is
  #     refused `:replica_changed`; the other readers only test presence or log;
  #   * a resident row read by the same callers is just as unlocked and just as about to change.
  # Answering `:replica_busy` instead would widen `state_of/2`'s contract for every call site and,
  # mapped to `nil` by any of them, reintroduce "no replica" over one on disk.
  defp rederive(name, shard_id) do
    if Fathom.ShardId.valid?(shard_id) do
      case with_shard_lock(name, shard_id, lock_wait_ms(), fn ->
             rederive_locked(name, shard_id)
           end) do
        # The tombstone check `recover_shard/3` makes on the locked path, made here too (expert
        # review 2026-10-21 #5): with no boot pass, a deleted tenant's replica can still be on disk
        # when this runs, and it must be refused, not served, until `forget/2` erases it.
        {:error, :replica_busy} ->
          unless Fathom.Tenants.Tombstones.tombstoned?(shard_id), do: derive_state(name, shard_id)

        {:error, :no_replica} ->
          nil

        state ->
          state
      end
    end
  end

  defp rederive_locked(name, shard_id) do
    tab = table(name)

    case :ets.lookup(tab, shard_id) do
      [{^shard_id, s}] ->
        s

      [] ->
        if recover_shard(tab, name, shard_id) do
          case :ets.lookup(tab, shard_id) do
            [{^shard_id, s}] -> s
            [] -> nil
          end
        end
    end
  end

  defp recovered_state(
         name,
         shard_id,
         %{ckpt_seq: gen, salt1: salt} = header,
         torn?,
         lineage,
         _log?
       ) do
    # Only for the WAL whose salt it was written for (expert review 2026-10-15 #4).
    ordinal = recovered_ordinal(name, shard_id, salt)

    %{
      FollowerLog.seeded(0, gen, salt, header.commit_extent, lineage)
      | torn: torn?,
        wal_ordinal: ordinal
    }
  end

  # AN EMPTY OR ABSENT `-wal` OVER A `.db` (expert review 2026-10-15 #4 follow-up). This used to
  # recover NOTHING, so the replica answered a definite "nothing" while its `.db` held the acked
  # writes an absorb (or a `.db`-only pulled seed) put there. Now it recovers the position the `.db`
  # alone holds (`record_db_position/5`) when that was recorded under the lineage it still has —
  # or, with no trustworthy record, TORN: unrankable, `:torn` (silent) to a position query, never
  # counted toward the floor, and re-seeded on its next push, which is what an unrecovered replica's
  # `:unknown_shard` already asked for. The torn flag is in-memory only; the files decide it again
  # on every restart. A follower that never held the shard has no `.db` and still answers "nothing".
  #
  # Epoch 0 as for every recovered replica, so the first push takes `decide_fresh/2` and resets
  # from offset 0 — nothing is ever appended to the empty WAL at the recorded offset.
  defp recovered_state(name, shard_id, :empty, torn?, lineage, log?) do
    case recovered_db_position(name, shard_id) do
      {:ok, %{lineage: ^lineage, wal_ordinal: ordinal, next_offset: extent}} ->
        %{FollowerLog.seeded(0, 0, 0, extent, lineage) | torn: torn?, wal_ordinal: ordinal}

      _ ->
        if log?,
          do:
            Logger.info(
              "replication follower recovered #{shard_id} with an empty WAL and no position for " <>
                "its .db; holding it as torn (unrankable) until it is re-seeded"
            )

        %{FollowerLog.seeded(0, 0, 0, 0, lineage) | torn: true}
    end
  end

  defp do_handle_push(name, %Protocol.Push{} = push) do
    # Read ONCE and keep it: the pre-reset state is what says how much of the OUTGOING generation
    # this follower actually holds, and `absorb_before_reset/4` needs that to decide whether it is
    # entitled to clear `torn` (#11a).
    prev = state_of(name, push.shard_id)

    case FollowerLog.decide(prev, push) do
      {:reject, reason, expected} ->
        Protocol.encode_reject(push.shard_id, reason, expected)

      {_accept, new_state} = decision ->
        cond do
          reseed?(prev, push) -> request_seed(name, prev, new_state, push)
          short_reset?(decision, prev, push) -> request_seed(name, prev, new_state, push)
          true -> apply_decision(name, decision, prev, push)
        end
    end
  end

  defp apply_decision(name, {:append, new_state}, _prev, push),
    do: apply_write(name, push, new_state, :append)

  defp apply_decision(name, {:reset_then_append, new_state}, prev, push) do
    # A checkpointed WAL: our bytes are meaningless now, so the file is replaced rather than
    # extended — but the pages in the WAL we are about to throw away are NOT meaningless, and
    # absorbing them first is what keeps this replica whole.
    absorbed =
      absorb_before_reset(name, push.shard_id, prev, new_state, reset_verdict(prev, push))

    apply_write(name, push, absorbed, :truncate)
  end

  # `:replication_reseed` (default ON) is the switch back to the pre-2026-09-30 behaviour — accept,
  # stay torn, never ask for a seed — for a fleet whose shards are too large to re-seed on every
  # ownership change inside `replication_timeout_ms`. Off does NOT reintroduce the hybrid (#1): an
  # ownership change still refuses to absorb and marks the replica torn, it just waits there.
  defp reseed?(prev, push) do
    FollowerLog.reseed?(prev, push) and
      Application.get_env(:fathom, :replication_reseed, true)
  end

  # A checkpoint reset this follower was SHORT for (#11a): the WAL it would absorb is missing the
  # outgoing generation's tail, so the result would be torn from the moment it is written. Ask for
  # the seed now rather than one push later — on a quiet shard "one push later" is unbounded.
  defp short_reset?({:reset_then_append, _}, prev, push),
    do:
      prev != nil and Application.get_env(:fathom, :replication_reseed, true) and
        reset_verdict(prev, push) == :short

  defp short_reset?(_decision, _prev, _push), do: false

  # ANSWER `:unknown_shard` — "I hold nothing you can build on" — which is the signal `Session`
  # already seeds on, so the primary side needed no new reason, no wire change and no new path: the
  # seed, its wait inside the commit, its dedup and its expiry are the ones the first-write seed has
  # used since 2026-08-11.
  #
  # NOTHING IS WRITTEN TO THE REPLICA FILES. Absorbing the old WAL is what built the hybrid (#1), and
  # appending the new ownership's frames onto a base it was not built from is the same hybrid one
  # step later. The files stay exactly as they were until `install/3` replaces them.
  #
  # The state IS updated, in two fields, and both are load-bearing:
  #
  #   * `torn: true`, durably (the marker). This is the safety net the finding's option A asked for:
  #     if the seed never arrives — refused for disk headroom, a shard too large for the deadline,
  #     `:replication_reseed` off, the primary dying first — the replica is un-promotable rather than
  #     a hybrid, and `reseed?/2` keeps asking on every push until a seed clears it;
  #   * `epoch` and `lineage` ADVANCE to the new ownership's. Keeping the old ones would let the
  #     DEPOSED owner, still shipping at the old lineage, pass the lineage fence as an equal and have
  #     its own seed accepted over this one. Advancing them fences it exactly as a completed reset
  #     would have, and the new owner's seed (same lineage) is still accepted by `begin_seed/3`.
  #
  # The position fields (`wal_gen`, `salt1`, `next_offset`, `wal_ordinal`) keep describing the files
  # that are actually on disk; nothing reads them for an append while `torn` holds.
  #
  # WHAT THIS COSTS, AND THE PARKED FIX (expert review 2026-10-01 perf #6(a), parked 2026-10-06).
  # Every open bumps the lineage, so every reopen after an idle drop re-sends the WHOLE database to
  # every follower. Measured on the chaos rig (256 tenants, replication on, drain every shard, run
  # the same tenants again): ~1,024 full seeds, ~48 KB each (the whole DB), ~10 MB of seed bytes per
  # node — ~11% of that round's traffic for these tiny TPC-B tenants. A 10 MB tenant writing a little
  # after each quiet spell would spend ~40 MB of seed to deliver ~1.6 MB of changes (~96%).
  #
  # The safe skip: a DIRTY idle drop uploads the raw `.db` after a TRUNCATE checkpoint (not a VACUUM
  # copy), so a follower that absorbs its own WAL often holds that exact file. If the follower sends
  # an MD5 of its absorbed `.db` and the new owner's `.db` hashes the same, the seed can carry the WAL
  # only — byte-identical base, so no hybrid. Not built because the plumbing is wide: the request is
  # a plain `:unknown_shard` reject that flows through Shipper, Quorum and Session's reject list, and
  # the alternative (hash in SeedBegin, follower answers reuse/full) breaks the seed's one-reply
  # contract the Shipper's waiter relies on. A clean drop (object = last VACUUM copy) must still seed.
  defp request_seed(name, prev, decided, push) do
    Logger.info(
      "replication follower asking for a seed of #{push.shard_id}: " <>
        if(prev.torn, do: "the replica is torn", else: "new ownership or a short reset")
    )

    put_state(name, push.shard_id, %{
      prev
      | torn: true,
        epoch: decided.epoch,
        lineage: decided.lineage
    })

    Protocol.encode_reject(push.shard_id, :unknown_shard, 0)
  end

  # Absorb our own WAL into our own `.db` before the reset discards it.
  #
  # THE POINT. A reset means the primary checkpointed: it drained its WAL into ITS `.db` and started
  # a new generation. We hold the same pages — they are in the WAL we received — so we can perform
  # the identical move locally. Nothing is fetched from a peer and nothing is fetched from S3; the
  # cost is one local checkpoint, proportional to WRITE VOLUME (a WAL bounded by one flush interval)
  # rather than to database size, which is what makes it affordable per shard per flush.
  #
  # Without it the reset orphans the `.db` in the old generation and every page the checkpoint moved
  # is in NEITHER file — the 2026-08-12 rig failure, where a promotion served a tenant an EMPTY
  # database over a working stored object
  # (`docs/reviews/a2-checkpoint-torn-replica-2026-08-12.md`).
  #
  # `Follower` otherwise NEVER opens the database, deliberately: opening checkpoints it, which
  # breaks the byte-offset alignment with the primary. This is the one moment that reasoning does
  # not apply — the alignment is being discarded by this very reset — which is exactly why the
  # exception is safe here and nowhere else.
  #
  # FAIL-SAFE: any failure leaves `torn: true` (what `FollowerLog.decide_fresh/2` set) and today's
  # behaviour. The replica keeps replicating and is simply not promotable until a seed rebuilds it,
  # which is strictly better than serving a torn pair. Success clears the flag, because the two
  # files are now back in step.
  # Did this follower hold the WHOLE of the generation the reset is replacing? (expert review
  # 2026-08-20 #11a — the mechanism #11b did not fix.)
  #
  # `prev_extent` is the primary's last shipped extent in the OUTGOING generation. A follower short
  # of it never received that generation's tail, so the WAL it is about to absorb is INCOMPLETE —
  # and absorbing an incomplete WAL yields a database that opens cleanly, passes `quick_check`, and
  # is quietly missing writes. `absorb_before_reset/4` then cleared `torn` and laundered it into a
  # promotable replica.
  #
  # **The follower cannot detect this alone**, which is the entire reason the field had to cross
  # the wire: a reset carries no statement about the generation it replaces, so "my offset is 4096"
  # is indistinguishable from complete and from four frames short.
  #
  # 0 means the primary made NO STATEMENT — an un-upgraded peer that never sets the field, or a
  # generation in which nothing was ever shipped. Both must read as "no evidence of a gap": marking
  # a replica torn on absence would make every replica in the fleet un-promotable for the length of
  # a rolling upgrade, which is the failure this subsystem exists to prevent, arriving by the door
  # marked safety.
  #
  # The seed half of #11a now exists (expert review 2026-09-29 #1): a follower short at a reset asks
  # for a seed on that same push (`short_reset?/3` → `request_seed/4`) instead of absorbing a WAL it
  # knows is incomplete. It waited on the rig's seed-rate measurement, because a rule that turns
  # "behind at a generation boundary" into "ship the whole database" could convert a lag spike into
  # a seed storm; measured 2026-09-30 (`./chaos.sh seed-rate`) at link speed, ~290 ms for 32 MiB on
  # 1 Gbit/s. It stays write-gated: only a push can provoke it.
  defp complete_through_reset?(_prev, %Protocol.Push{prev_extent: prev}) when prev in [0, nil],
    do: true

  defp complete_through_reset?(%{next_offset: held}, %Protocol.Push{prev_extent: prev})
       when is_integer(held),
       do: held >= prev

  # TORN IS STICKY (expert review 2026-09-29 #2). A replica that was ALREADY torn going into this
  # reset stays torn whatever the reset says, because absorbing a complete NEXT generation does not
  # put back the pages the earlier seam lost: a page written only in the short generation's missing
  # tail is in neither file, and nothing in the new WAL mentions it. `FollowerLog`'s own contract is
  # "cleared only by a seed"; this used to clear it on any complete reset, which laundered a replica
  # the #11a rule had just correctly marked short. The same hole let a follower RESTART launder it:
  # recovery reads the durable marker (#11b) into `prev.torn`, the first push after the restart is a
  # reset from offset 0, and that reset cleared the flag and deleted the marker #11b exists to keep.
  #
  # A torn replica is torn until it is re-seeded — and since expert review 2026-09-29 #1 it ASKS for
  # that seed on its next push (`FollowerLog.reseed?/2`), so this path is only reached with
  # `:replication_reseed` off. Until the seed lands, promotion falls back to the stored object: the
  # pre-A2 answer for that one shard on that one follower, not a loss.
  defp reset_verdict(%{torn: true}, _push), do: :already_torn

  # A NEW OWNERSHIP'S reset (expert review 2026-09-29 #1). Only reached with `:replication_reseed`
  # off — on, `request_seed/4` answers it first. See `absorb_before_reset/4`'s `:ownership` clause.
  defp reset_verdict(%{} = prev, push) do
    cond do
      FollowerLog.ownership_change?(prev, push) -> :ownership
      complete_through_reset?(prev, push) -> :complete
      true -> :short
    end
  end

  # No prior state: the reset IS the first frame for this shard.
  defp reset_verdict(nil, _push), do: :complete

  # NEVER ABSORB ACROSS AN OWNERSHIP CHANGE (expert review 2026-09-29 #1). The absorb is sound only
  # when the pushing primary's base IS the state our old WAL leads to — true for a checkpoint within
  # one ownership, and not knowable across two: after a restore or revert the new owner's `.db` is
  # OLDER bytes, after an unflushed takeover it is the stale object, and a follower lagging at the
  # handoff holds a short tail. `prev_extent` cannot help, because a new owner's session has no
  # record of this follower and always states 0. So the replica stays torn (what `decide_fresh/2`
  # set) and waits for a seed instead of becoming a promotable hybrid of two ownerships.
  defp absorb_before_reset(_name, _shard_id, _prev, new_state, :ownership), do: new_state

  defp absorb_before_reset(name, shard_id, prev, new_state, verdict) do
    db = db_path(name, shard_id)
    wal = wal_path(name, shard_id)

    # Nothing to absorb: a shard we hold no files for (the reset IS the first frame) is not torn,
    # it is simply new — `apply_write` is about to lay down a whole generation from offset 0. An
    # empty WAL over an existing `.db` has nothing to absorb either, but it does NOT make a torn
    # replica whole, so a sticky verdict survives this branch too.
    if Fathom.FS.exists?(db) and wal_bytes(wal) > 0 do
      case absorb_recording_position(name, shard_id, prev, verdict, db, wal) do
        :ok when verdict == :already_torn ->
          # Absorbing is still right — the pages we hold are real — but it cannot clear the flag.
          new_state

        :ok when verdict == :short ->
          # The checkpoint SUCCEEDED and the replica is still torn, which is the whole point: the
          # local move was fine, the INPUT to it was short. Absorbing incomplete pages produces a
          # database that looks healthy, so the flag is the only thing standing between it and a
          # promotion.
          Logger.warning(
            "shard #{shard_id}: absorbed a SHORT WAL before a reset — this replica was behind at " <>
              "the generation boundary and stays torn; it will not be promoted until it is re-seeded"
          )

          new_state

        :ok ->
          %{new_state | torn: false}

        {:error, reason} ->
          Logger.warning(
            "shard #{shard_id}: could not absorb the WAL before a reset (#{inspect(reason)}); " <>
              "the replica stays torn and will not be promoted until it is re-seeded"
          )

          new_state
      end
    else
      if verdict == :already_torn, do: new_state, else: %{new_state | torn: false}
    end
  end

  defp wal_bytes(path) do
    case Fathom.FS.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end

  # `TRUNCATE` rather than PASSIVE so the WAL is actually emptied and cannot be re-applied on top of
  # the new generation we are about to write into the same file. The `-shm` goes with it: a stale
  # shared-memory index describes a WAL that no longer exists, and SQLite trusts it.
  #
  # THE RESULT ROW IS CHECKED, NOT JUST THE CALL (2026-10-09, found while fixing expert review
  # 2026-10-08 #5). The pragma returns `{:ok, [[busy, log, checkpointed]]}` even when it could not
  # finish: a reader holding an older snapshot (a peer-recovery or promote read on this replica)
  # stops the backfill at its mark, leaving `checkpointed < log`. Any `{:ok, _}` used to count as
  # absorbed, so the caller cleared `torn` and the reset then truncated the WAL holding the frames
  # that never reached the `.db` — a replica marked whole and promotable, silently missing pages.
  # `busy = 1` with every frame backfilled is fine (the reset rewrites the WAL itself), so the test
  # is `log == checkpointed`. Same defect class as `Migrator.Copy`'s fold (review #25, 73d95f6).
  @doc false
  @spec checkpoint_into_db(Path.t()) :: :ok | {:error, term()}
  #
  # A LEAN HANDLE (perf review 2026-10-14 #13). This used `Connection.open/1` — extension load and
  # disable, the pragma guard, page_size/max_page_count, WAL enable, five pragmas: 361 us of a 450 us
  # absorb, inline on the reset push's ack path. Absorbing needs only a handle that recovers the WAL
  # and checkpoints it, so this opens raw, sets the cancellable busy timeout (NOT the pragma, see
  # `Connection.configure/2`) and `synchronous=FULL` (unchanged: the absorb fsyncs the `.db` before
  # the WAL is truncated, so a crash cannot leave a replica short AND unmarked), and runs the
  # checkpoint. No tenant SQL ever runs here, so no extension, authorizer or tenant configure.
  #
  # NO `journal_mode` SET OR CHECK (probed 2026-10-14): `Connection.open/1` enabled WAL and failed
  # the open on anything else, but the caller only gets here when a non-empty `-wal` exists, and
  # SQLite opens a database in WAL mode whenever one is present (`pagerOpenWalIfPresent`), whatever
  # the header says. The only file a raw handle could see as non-WAL is one with no WAL at all,
  # which the caller never absorbs.
  def checkpoint_into_db(db) do
    case absorb_into_db(db) do
      {:ok, _extent} -> :ok
      {:error, _} = err -> err
    end
  end

  # THE EXTENT THE `.db` NOW HOLDS comes from SQLite, not from the WAL's own claim (expert review
  # 2026-10-15 #4 follow-up). `absorb_before_reset/5` records it as the `.db`'s position, and
  # `Wal.read/1`'s commit extent is found by salts and commit markers WITHOUT checksums — a frame
  # whose checksum broke stops SQLite's replay there (the 2026-10-15 #15 measurement), so the WAL
  # would claim more than the `.db` received. A PASSIVE checkpoint first reports `[busy, log,
  # checkpointed]` with `log` = the frames SQLite validated (TRUNCATE rewinds `mxFrame` before it
  # answers, so its row cannot say this), and the TRUNCATE after it only empties the file: the same
  # backfill and fsyncs as before, plus one pragma.
  @doc false
  @spec absorb_into_db(Path.t()) :: {:ok, pos_integer()} | {:error, term()}
  def absorb_into_db(db) do
    case Sqlite3.open(db) do
      {:ok, conn} ->
        try do
          with :ok <- Sqlite3.set_busy_timeout(conn, 5000),
               :ok <- Sqlite3.execute(conn, "PRAGMA synchronous=FULL") do
            absorb_checkpoint(conn)
          else
            {:error, reason} -> {:error, {:open_failed, reason}}
          end
        after
          _ = Sqlite3.close(conn)
          Fathom.FS.rm(db <> "-shm")
        end

      {:error, reason} ->
        {:error, {:open_failed, reason}}
    end
  end

  defp absorb_checkpoint(conn) do
    with {:ok, [[_busy, frames, frames]]} <- pragma_rows(conn, "PRAGMA wal_checkpoint(PASSIVE)"),
         {:ok, [[page]]} <- pragma_rows(conn, "PRAGMA page_size"),
         {:ok, [[_busy, log, log]]} <- pragma_rows(conn, "PRAGMA wal_checkpoint(TRUNCATE)") do
      {:ok, Wal.header_bytes() + frames * (page + @wal_frame_header_bytes)}
    else
      {:ok, [row]} -> {:error, {:absorb_incomplete, row}}
      {:error, reason} -> {:error, {:checkpoint_failed, reason}}
      other -> {:error, {:checkpoint_failed, other}}
    end
  end

  # `prepare` + `fetch_all`: `Sqlite3.execute/2` discards the result row, and the row is the point.
  defp pragma_rows(conn, sql) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql) do
      result = Sqlite3.fetch_all(conn, stmt)
      _ = Sqlite3.release(conn, stmt)
      result
    end
  end

  # WHAT THE `.db` ALONE HOLDS (expert review 2026-10-15 #4 follow-up). A replica whose `-wal` is
  # empty or absent at restart was not recovered at all (`Wal.read/1` answers `:empty`), so it
  # answered a DEFINITE "nothing" — counted toward the recovery floor, and outranked by a staler
  # peer or the stored object — while its `.db` held quorum-acked writes. The ordinary way to get
  # there: an absorb (`absorb_before_reset/5`) checkpoints the old generation into the `.db` and
  # truncates the WAL, and a crash before the new generation's bytes land (or an OS crash that
  # loses those unsynced bytes; the absorb itself is synchronous=FULL) leaves `.db` + empty WAL.
  #
  # So the absorb records `"<lineage> <ordinal> <extent> end"` beside the `.db`: the ordinal of the
  # WAL it absorbed (only when that WAL's header salt is the one the state's ordinal belongs to)
  # and the extent SQLite validated (`absorb_into_db/1`). Recovery adopts it ONLY for an empty WAL
  # and only when the lineage still matches; otherwise an empty-WAL replica recovers TORN
  # (unrankable, answers `:torn`, re-seeds on its next push) — never a definite "nothing".
  #
  # The record is INVALIDATED before anything moves the `.db` (an absorb, a seed install) and
  # written only after, so a crash in between leaves no record rather than a stale one: a stale
  # record would UNDER-claim, the dangerous direction (a definite position lower than the `.db`
  # holds). Invalidated in place (truncate, then a datasync under `:replication_fsync`) rather than
  # removed, so its ordering against the checkpoint's own fsyncs holds without the directory fsync
  # the BEAM cannot do. A torn or partial record (an OS crash mid-write) lacks the trailing `end`
  # and reads as no record.
  #
  # COST, measured 2026-10-15 (300 alternating absorbs of a 20-frame WAL, macOS): the PASSIVE count
  # adds ~10 us to a ~335 us absorb; the first draft's file handling (a `Wal.read/1`, a stat, an
  # open/close to invalidate, a `File.write` to record) added ~140 us more. So the absorb path holds
  # ONE descriptor across the checkpoint — opened truncating (the invalidation), written after —
  # and reads only the 20 header bytes it needs for the salt: ~+80 us in all (335 -> 415 us p50).
  # Once per WAL generation, on the reset push that already pays a SQLite open and checkpoint; the
  # append path, i.e. every other push, is untouched.
  defp absorb_recording_position(name, shard_id, prev, verdict, db, wal) do
    ordinal = absorbed_ordinal(prev, wal)

    case :file.open(db_position_path(name, shard_id), [:write, :raw, :binary]) do
      {:ok, fd} ->
        try do
          with :ok <- maybe_sync(fd),
               {:ok, extent} <- timed_absorb(db) do
            # From the moment the checkpoint truncates the WAL, the `.db` alone holds every frame
            # it absorbed, and until the new generation's bytes land it is ALL this replica holds.
            # Recorded for any non-torn base (a `:short` absorb included: the record states how far
            # the `.db` got, which is true however short of the primary that is). A failed record
            # write leaves no record, which recovers torn: safe.
            _ = if verdict != :already_torn, do: write_db_position(fd, prev, ordinal, extent)
            :ok
          end
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, {:position_file, reason}}
    end
  end

  # The absorb's wall time (expert review 2026-10-17 #5): it fsyncs inline on the reset push's ack
  # path, and no follower timing existed to say how long that is on a real filesystem.
  defp timed_absorb(db) do
    t0 = System.monotonic_time()
    result = absorb_into_db(db)

    :telemetry.execute(
      [:fathom, :replication, :follower_absorb],
      %{duration: System.monotonic_time() - t0},
      %{result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    result
  end

  defp write_db_position(fd, %{lineage: lineage}, ordinal, extent)
       when is_integer(lineage) and lineage > 0 and is_integer(ordinal) and ordinal > 0 do
    with :ok <- :file.write(fd, db_position_text(lineage, ordinal, extent)) do
      if fsync?(),
        do: with(:ok <- :file.datasync(fd), do: emit_state_sync(:db_position)),
        else: :ok
    end
  end

  defp write_db_position(_fd, _prev, _ordinal, _extent), do: :ok

  defp db_position_text(lineage, ordinal, extent), do: "#{lineage} #{ordinal} #{extent} end"

  # The seed-install side of the same record (`do_install/3`): rare, so the plain helpers.
  defp record_db_position(name, shard_id, %{lineage: lineage}, ordinal, extent)
       when is_integer(lineage) and lineage > 0 and is_integer(ordinal) and ordinal > 0 do
    write_state_file(
      db_position_path(name, shard_id),
      db_position_text(lineage, ordinal, extent),
      :db_position
    )
  end

  defp record_db_position(_name, _shard_id, _prev, _ordinal, _extent), do: :ok

  defp invalidate_db_position(name, shard_id) do
    path = db_position_path(name, shard_id)

    if Fathom.FS.regular?(path) do
      with {:ok, fd} <- :file.open(path, [:write, :raw, :binary]) do
        synced = maybe_sync(fd)
        _ = :file.close(fd)
        synced
      end
    else
      :ok
    end
  end

  # The ordinal of the WAL about to be absorbed: the state's, if the file's header is the salt that
  # ordinal was recorded for; otherwise 0 ("not stated"), and no record is written. A state whose
  # salt no longer names the file on disk (a reset write that failed part-way) must not lend its
  # ordinal to whatever the checkpoint finds there.
  defp absorbed_ordinal(%{salt1: salt, wal_ordinal: n}, wal) when is_integer(n) and n > 0 do
    if wal_header_salt(wal) == salt, do: n, else: 0
  end

  defp absorbed_ordinal(_prev, _wal), do: 0

  # Magic (bytes 0..3) and salt-1 (bytes 16..19) of a WAL header; nil if it is not one.
  defp wal_header_salt(wal) do
    with {:ok, fd} <- :file.open(wal, [:read, :raw, :binary]) do
      try do
        case :file.pread(fd, 0, 20) do
          {:ok, <<magic::32, _::binary-size(12), salt::32>>} when magic in @wal_magics -> salt
          _ -> nil
        end
      after
        :file.close(fd)
      end
    else
      _ -> nil
    end
  end

  @doc false
  # The `.db`-only position persisted by `record_db_position/5`, or `:none`. Public for the test.
  @spec recovered_db_position(atom(), String.t()) ::
          {:ok, %{lineage: pos_integer(), wal_ordinal: pos_integer(), next_offset: pos_integer()}}
          | :none
  def recovered_db_position(name \\ __MODULE__, shard_id) do
    with {:ok, text} <- Fathom.FS.read(db_position_path(name, shard_id)),
         [l, n, x, "end"] <- String.split(String.trim(text), " "),
         {lineage, ""} when lineage > 0 <- Integer.parse(l),
         {ordinal, ""} when ordinal > 0 <- Integer.parse(n),
         {extent, ""} when extent > 0 <- Integer.parse(x) do
      {:ok, %{lineage: lineage, wal_ordinal: ordinal, next_offset: extent}}
    else
      _ -> :none
    end
  end

  # AN APPEND NEVER CREATES THE WAL (expert review 2026-10-08 #21). An append extends a WAL this
  # replica already holds; one that is missing, or shorter than the push's offset, is a replica that
  # lost bytes it acked — a worker killed mid-install between removing the old WAL and renaming the
  # new one in, or a `forget/2` (promotion, tenant erase) racing an in-flight push. Opening for
  # write CREATED it, the pwrite at `offset > 0` left that many zero bytes in front — an invalid
  # header SQLite ignores outright — and the state advanced `torn: false`, so the replica read as
  # current and promotable while serving the `.db` alone. After a `forget`, it also put the ETS row
  # and a WAL back for a shard this node had dropped, which nothing ever cleaned up.
  #
  # So: with no state row (`forget/2` deletes it first), the shard is not followed here any more —
  # refuse `:unknown_shard` and write nothing. With state but a WAL shorter than the offset (absent
  # counts as empty), mark the replica torn (durably) and refuse `:unknown_shard`, which makes the
  # primary re-seed it. An append at offset 0 may still create the file: it starts with the WAL
  # header, so what it writes is a valid WAL.
  defp apply_write(name, %Protocol.Push{} = push, new_state, :append) do
    # ONE stat per append push (perf review 2026-10-14 #22): the precondition's size check and the
    # held-fd inode check both read the same path, so the info is taken once here and handed down.
    stat = :file.read_file_info(wal_path(name, push.shard_id), [:raw])

    case append_precondition(name, push.shard_id, push.offset, stat) do
      :ok ->
        do_apply_write(name, push, new_state, :append, stat)

      :not_followed ->
        Protocol.encode_reject(push.shard_id, :unknown_shard, 0)

      :wal_missing ->
        Logger.warning(
          "replication: #{push.shard_id}'s replica WAL is missing or shorter than the push offset " <>
            "#{push.offset}; marking it torn and asking for a re-seed"
        )

        case state_of(name, push.shard_id) do
          nil -> :ok
          prev -> put_state(name, push.shard_id, %{prev | torn: true})
        end

        Protocol.encode_reject(push.shard_id, :unknown_shard, 0)
    end
  end

  defp apply_write(name, push, new_state, mode), do: do_apply_write(name, push, new_state, mode)

  # `stat` is `:file.read_file_info(path, [:raw])`; absent or unreadable counts as an empty WAL, as
  # `wal_bytes/1` did. Size is element 1 of the raw `file_info` record.
  defp append_precondition(name, shard_id, offset, stat) do
    cond do
      state_of(name, shard_id) == nil -> :not_followed
      stat_size(stat) < offset -> :wal_missing
      true -> :ok
    end
  end

  defp stat_size({:ok, info}), do: elem(info, 1)
  defp stat_size(_), do: 0

  defp do_apply_write(name, push, new_state, mode, stat \\ nil)

  defp do_apply_write(name, %Protocol.Push{} = push, new_state, mode, stat) do
    path = wal_path(name, push.shard_id)

    written =
      if mode == :append,
        do: write_held(path, push.offset, push.payload, stat),
        else: write_frame(path, push.offset, push.payload, mode)

    case written do
      :ok ->
        # State advances ONLY after the bytes are down. Advancing first and failing the write would
        # leave the follower claiming an offset it does not hold, which the primary would believe.
        put_state(name, push.shard_id, new_state)
        Protocol.encode_ack(push.shard_id, new_state.next_offset)

      {:error, reason} ->
        Logger.error("replication write failed for #{push.shard_id}: #{inspect(reason)}")
        # Report where we actually are, so the primary retries rather than assuming we advanced.
        expected = (state_of(name, push.shard_id) || %{next_offset: 0}).next_offset
        Protocol.encode_reject(push.shard_id, :internal, expected)
    end
  end

  # POSITIONAL, not append. A frame is written at its OWN offset, so a re-send is idempotent
  # (expert review 2026-08-31 #1). The previous `[:append]` open wrote at EOF, and only stayed
  # correct because `FollowerLog.decide/2` guarantees `push.offset == next_offset` — an ETS value
  # never checked against the file's actual size. When a write's bytes landed but the follower's
  # state did not advance (a failed close/put_state), the primary re-sent from the un-advanced
  # offset and the append laid a SECOND copy of the frame after the first. A duplicate frame breaks
  # SQLite's cumulative WAL checksum chain: on promote/recover SQLite treats the duplicate as
  # end-of-WAL and silently drops every acked frame after it. `pwrite` at the decided offset writes
  # the same bytes to the same place under a re-send, which removes the unverified EOF invariant.
  @doc false
  def write_frame(path, offset, payload, mode) do
    {open_modes, write} =
      case mode do
        # Reset: `[:write]` truncates the file on open, laying the new generation from position 0
        # (a reset push carries offset 0). Truncate here is the point — the old generation must go.
        :truncate ->
          {[:write, :raw, :binary], fn fd -> :file.write(fd, payload) end}

        # Append: `[:read, :write]` creates-if-absent and does NOT truncate, so pwrite lands the
        # frame at its own offset over any bytes a prior (un-acked) attempt already put there.
        _append ->
          {[:read, :write, :raw, :binary], fn fd -> :file.pwrite(fd, offset, payload) end}
      end

    with {:ok, fd} <- :file.open(path, open_modes) do
      try do
        with :ok <- write.(fd), :ok <- maybe_sync(fd) do
          :file.close(fd)
        else
          {:error, _reason} = err ->
            _ = :file.close(fd)
            err
        end
      rescue
        e ->
          _ = :file.close(fd)
          reraise e, __STACKTRACE__
      end
    end
  end

  # A HELD FILE PER SHARD for appends (expert review 2026-10-01 perf #7, its last part). Opening
  # and closing the WAL around every push cost 42-47 us p50 against 5 us for the pwrite alone, on a
  # 17 KB frame — and it is on the commit path, since the primary waits for this ack.
  #
  # THE HAZARD a held file brings, and the check that removes it: other processes REPLACE or DELETE
  # this file — a seed from a new primary arrives on another connection (another worker) and renames
  # a new WAL into place, Recovery installs a pulled replica the same way, a tenant delete removes it.
  # A stale handle would then write into an unlinked inode: acked, and gone. So every append first
  # compares the path's inode with the one the handle was opened on, and reopens on a mismatch.
  # That is exact rather than heuristic: while this handle is open its inode cannot be freed, so no
  # new file at the path can reuse it. Costs one `stat` (~13 us), so the net is ~18 us per push
  # instead of ~45. A reset (`:truncate`) still goes through `write_frame/4`; it truncates the same
  # inode, so a held handle stays valid across it.
  #
  # Per WORKER (the process dictionary of the connection's per-shard worker that owns these shards)
  # and closed when it exits. Capped so a follower of thousands of shards cannot exhaust its fds.
  #
  # THE CAP IS AN FD BUDGET, AND IT MULTIPLIES (expert review 2026-10-08 #28): held fds per node =
  # cap × `:replication_follower_workers` × inbound primaries — 64 × 8 × 4 = 2,048 on a 5-node
  # fleet, held for as long as the connections live, on top of each served shard's own fds. At
  # Linux's default `nofile` of 1024 that alone exceeds the limit. Configurable with
  # `REPLICATION_HELD_WAL_FDS` (0 turns the cache off: open-pwrite-close per push, the pre-perf-#7
  # cost); size it from `nofile` alongside the served-density budget (docs/configuration.md). Above
  # the cap a worker serving more active shards than it holds misses on most pushes.
  @default_held_fds_cap 64

  defp held_fds_cap,
    do: Application.get_env(:fathom, :replication_held_wal_fds, @default_held_fds_cap)

  defp write_held(path, offset, payload, stat) do
    if held_fds_cap() > 0,
      do: write_cached(path, offset, payload, stat),
      else: write_frame(path, offset, payload, :append)
  end

  defp write_cached(path, offset, payload, stat) do
    key = {__MODULE__, :held_fds}
    held = Process.get(key, %{})

    case held_fd(held, path, offset, stat) do
      {:ok, fd, held} ->
        with :ok <- :file.pwrite(fd, offset, payload),
             :ok <- maybe_sync(fd) do
          Process.put(key, held)
          :ok
        else
          {:error, _} = err ->
            Process.put(key, drop_held(held, path))
            err
        end

      {:error, _} = err ->
        Process.put(key, drop_held(held, path))
        err
    end
  end

  defp held_fd(held, path, offset, stat) do
    case {Map.get(held, path), stat} do
      {{fd, inode, _used}, {:ok, info}} when elem(info, 11) == inode ->
        held_fd_result(:hit)
        {:ok, fd, Map.put(held, path, {fd, inode, System.monotonic_time()})}

      {_stale_or_none, stat} ->
        held_fd_result(:miss)
        held = drop_held(held, path)

        # Creates a missing file ONLY for an append at offset 0, which begins with the WAL header
        # (expert review 2026-10-08 #21). Otherwise the file must exist, and the inode is checked
        # against the stat taken before the open — `[:read, :write]` creates a missing file, so a
        # WAL removed in between (a `forget/2` racing this push) shows up as a different inode and
        # the write is refused rather than landing in a fresh, headerless file.
        expected =
          case stat do
            {:ok, before} -> {:ok, elem(before, 11)}
            {:error, :enoent} when offset == 0 -> {:ok, :created}
            {:error, _} = err -> err
          end

        with {:ok, inode} <- expected,
             {:ok, fd} <- :file.open(path, [:read, :write, :raw, :binary]),
             {:ok, info} <- :file.read_file_info(fd),
             :ok <- same_inode(fd, inode, elem(info, 11)) do
          held = evict_oldest(held)
          {:ok, fd, Map.put(held, path, {fd, elem(info, 11), System.monotonic_time()})}
        end
    end
  end

  # Held-WAL-fd hit/miss counter (expert review 2026-10-19 #16). The cache has a cliff: past the cap
  # per worker, cyclic access over more active shards than fds drops to ~0% hits (45 vs 18 us per
  # push on a miss). This surfaces the rate before anyone changes the cap. Bounded `result` tag.
  defp held_fd_result(result),
    do: :telemetry.execute([:fathom, :replication, :held_fd], %{count: 1}, %{result: result})

  defp same_inode(_fd, :created, _got), do: :ok
  defp same_inode(_fd, inode, inode), do: :ok

  defp same_inode(fd, _expected, _got) do
    _ = :file.close(fd)
    {:error, :wal_replaced}
  end

  defp drop_held(held, path) do
    case Map.pop(held, path) do
      {{fd, _inode, _used}, rest} ->
        _ = :file.close(fd)
        rest

      {nil, rest} ->
        rest
    end
  end

  defp evict_oldest(held) do
    if map_size(held) < held_fds_cap(), do: held, else: evict_lru(held)
  end

  defp evict_lru(held) do
    {path, _} = Enum.min_by(held, fn {_path, {_fd, _inode, used}} -> used end)
    drop_held(held, path)
  end

  # SEED INSTALL AND THE STATE FILES HONOUR `:replication_fsync` (expert review 2026-10-15 #21,
  # default D13). With fsync on, a push was datasynced before its ack but a SEED was not: its temps
  # were renamed into place straight from the page cache, so an OS crash could leave a `.db`/`-wal`
  # that looks whole (the rename survived) over zero-filled pages, and recovery would rank it. The
  # temps are now datasynced before they are closed and renamed, and the lineage/ordinal files are
  # written fsync-then-rename. Off (the default) is unchanged: RAM-ack, durability from replica
  # count.
  #
  # NOT DONE: an fsync of the DIRECTORY after the rename. The BEAM cannot open a directory for
  # fsync portably (`:file.open` on a dir is `eisdir`; see `Storage.with_atomic_temp/2`). Losing a
  # rename to an OS crash leaves the PREVIOUS file (or none), which recovery treats as an older
  # replica or no replica — never as torn bytes under a whole-looking name.
  defp sync_seed_temps(state) do
    if fsync?() do
      with :ok <- :file.datasync(state.db_fd),
           :ok <- :file.datasync(state.wal_fd) do
        emit_state_sync(:seed)
      end
    else
      :ok
    end
  end

  defp write_state_file(path, body, what) do
    if fsync?() do
      with :ok <- Fathom.Shard.Storage.atomic_write(path, body), do: emit_state_sync(what)
    else
      Fathom.FS.write(path, body)
    end
  end

  # An observable for the test: an fsync is otherwise invisible short of an OS crash. Only on the
  # rare paths above (a seed, a new lineage, a new WAL generation), never per push.
  defp emit_state_sync(what) do
    :telemetry.execute([:fathom, :replication, :follower_state_sync], %{count: 1}, %{what: what})
  end

  defp fsync?, do: Application.get_env(:fathom, :replication_fsync, false) == true

  defp maybe_sync(fd) do
    if Application.get_env(:fathom, :replication_fsync, false), do: :file.datasync(fd), else: :ok
  end
end
