defmodule Fathom.Shard.Replication.Promote do
  @moduledoc """
  Turning a follower's replica into this node's primary — Phase 2 A2.
  See `docs/a2-quorum-replication.md`.

  This is where A2's whole reason for existing is collected. Every other piece moves bytes; this
  one is the piece that makes those bytes worth having, because a replica holds writes the S3
  object does not, and until they can be served they are just a copy nobody reads.

  ## Nothing calls this automatically, and that is deliberate

  Fathom already has failover: the LB remaps the subdomain and a survivor cold-opens the shard from
  S3, stealing the lease. That path is untouched and still correct — it just recovers to the last
  flush, which is the ~300 s RPO A2 exists to close.

  Wiring promotion into `Fathom.Shard`'s cold open is the actual payoff and is deliberately a
  separate change, because the seam there does not fit. `warm_or_cold_pull/2` validates a local copy
  by proving it **equals** the stored object (`If-None-Match` → 304). A replica is deliberately
  **fresher** than S3, so it fails that test by construction and needs a different provenance story
  — the one below. That code is also where AGENTS.md records repeated lock-leak bugs, so it gets its
  own review rather than riding along with this.

  ## The fence is the lease, and nothing else

  Promotion acquires the shard's lease, which bumps the epoch. From that moment the old primary's
  pushes carry a **lower** epoch and every follower refuses them with `:stale_epoch` — a branch
  `FollowerLog.decide/2` already implements and already tests. No new stop-the-old-writer mechanism
  is invented here, and none should be: the lease is the fencing token fathom already trusts
  everywhere else.

  If the lease cannot be taken because a live node holds it, promotion **refuses**. Two primaries
  for one shard is the one outcome worse than a stale one.

  ## Why the WAL is checkpointed on the way in

  Gate 1 measured that a follower's first clean open-and-close **checkpoints**: the `.db` grows and
  the `-wal` is deleted. For a follower that is corruption — its byte offsets stop matching the
  primary's — which is why `Follower` never opens the database. For a *promotion* it is exactly
  what we want, so it is done explicitly rather than left to happen as a side effect of closing the
  connection. After it, the `.db` is a complete standalone database and the byte-offset
  relationship with the old primary is over, which is correct: there is no old primary any more.

  ## Fresh provenance, not a borrowed etag

  The replica is **ahead** of the stored object. The coordinator's normal flush is fenced with
  `If-Match: <the etag we last wrote>`, and this node never wrote one — so promotion uses the
  unconditional `Storage.flush/2` and then reads the resulting etag back. Fencing against an etag
  we never wrote would be a lie in one direction; skipping the upload would leave the shard's
  durable object older than the data being served, which is the same RPO hole A2 set out to close.
  """

  require Logger

  alias Fathom.Shard
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Storage

  @lease_ttl_ms 30_000

  @typedoc "What a successful promotion did, for the operator and the tests."
  @type result :: %{
          shard_id: String.t(),
          epoch: non_neg_integer(),
          etag: String.t() | nil,
          bytes: non_neg_integer()
        }

  @doc """
  Is this node's replica strictly ahead of what the stored object contains?

  **Pure**, like `FollowerLog.decide/2` and `Quorum.settle/1`, and for the same reason: this is the
  decision that picks one of two lineages of a tenant's database and discards the other. Getting it
  wrong in one direction costs a failover's worth of RPO; in the other it silently drops
  acknowledged writes. It needs to be reachable from a plain unit test, not only from a failover.

  Both arguments are `nil`-tolerant and every uncertain case answers `false`:

    * no replica — nothing to promote;
    * **no stamp** — the object predates position stamping, or was written by a caller that passed
      none. Unknown is not "empty": an unstamped object is never overridable, which is what makes
      this feature inert (rather than dangerous) until a shard's next flush.

  The comparison is lexicographic on `{epoch, wal_gen, offset}` — see `t:Storage.position/0` for
  why that is a total order on how far along a copy is. `>` and not `>=`: equal positions mean the
  two hold the same history, and the stored object is then the one with provenance.
  """
  @spec fresher?(map() | nil, Storage.position() | nil) :: boolean()
  def fresher?(nil, _stamp), do: false
  def fresher?(_replica, nil), do: false

  # A TORN replica is never fresher than anything, however far ahead its offset reads.
  #
  # Its `.db` is a generation behind its `-wal` (see `FollowerLog`'s `t:t/0`), so the two do not
  # compose into a database at all — the position is a true statement about a WAL and a false one
  # about a copy of the shard. This clause is the fix for the 2026-08-12 rig failure where exactly
  # that pair was promoted and the tenant was served an EMPTY database over a working stored object
  # (`docs/reviews/a2-checkpoint-torn-replica-2026-08-12.md`).
  #
  # It lives HERE rather than at the promote call sites because `fresher?/2` is also what
  # `Recovery.choose/3` filters peer offers with — so one clause keeps a torn replica from being
  # promoted locally AND from being pulled across the fleet, instead of two rules that can drift.
  #
  # ABOVE the position comparison, because that clause matches any map with the three keys and
  # would otherwise win.
  def fresher?(%{torn: true}, _stamp), do: false

  def fresher?(%{epoch: e1, wal_gen: g1, next_offset: o1}, %{
        epoch: e2,
        wal_gen: g2,
        offset: o2
      }) do
    {e1, g1, o1} > {e2, g2, o2}
  end

  def fresher?(_replica, _stamp), do: false

  @doc """
  Promote this node's replica of `shard_id` to be the live shard.

  Options:

    * `:follower` — the `Follower` instance holding the replica (defaults to `Follower`).

  Returns `{:ok, result}` or:

    * `{:error, {:lease_held, owner}}` — a live node still owns the shard. Refused rather than
      risked; this is the split-brain case.
    * `{:error, :no_replica}` — nothing to promote. Distinct from a failure so a caller sweeping
      several shards can tell "not here" from "broken".
    * `{:error, :coordinator_running}` — this node is already serving the shard, so its own
      coordinator owns those files and promotion would be writing underneath a live writer.
  """
  @spec promote(String.t(), keyword()) :: {:ok, result()} | {:error, term()}
  def promote(shard_id, opts \\ []) do
    follower = Keyword.get(opts, :follower, Follower)

    with :ok <- ensure_no_coordinator(shard_id),
         :ok <- ensure_replica(follower, shard_id),
         {:ok, lease} <- acquire(shard_id) do
      # `after`, not a success-path release. An exception between acquiring and releasing is the
      # exact shape of the lock leaks AGENTS.md records being found four times over in the
      # coordinator, and every one of them was a path that "could not" raise.
      try do
        install_and_publish(follower, shard_id, lease)
      rescue
        e ->
          Logger.error("promotion of #{shard_id} crashed: #{Exception.message(e)}")
          {:error, {:promote_failed, Exception.message(e)}}
      after
        # Released rather than held. A lease held by something that is not a coordinator renews
        # nothing but still reads as a live owner, so it would block every other node from opening
        # the shard until it expired. Handing back a free lease over freshly-flushed bytes lets the
        # ordinary open path take it — from either this node or another.
        Storage.release_lease(shard_id, lease)
      end
    end
  end

  @doc """
  Build a standalone, verified database from this node's replica at `temp`.

  Split out so the **coordinator's cold open can reuse it while already holding the lease** —
  `promote/2` acquires one, and acquiring a second under the same owner would silently reclaim
  rather than fence, which is not a check at all.

  Staged into a temp rather than straight onto the live path, and that is not tidiness: a failure
  half-way through the copy or the checkpoint would otherwise leave the shard's live file holding
  replica bytes while its provenance sidecar still names the stored object. The coordinator would
  then serve one lineage and fence with another's etag. The same temp-then-promote discipline the
  pull path uses, for the same reason.
  """
  @spec stage(atom(), String.t(), Path.t()) :: :ok | {:error, term()}
  def stage(follower, shard_id, temp) do
    with :ok <- install(follower, shard_id, temp),
         :ok <- checkpoint_and_verify(temp) do
      :ok
    end
  end

  defp install_and_publish(follower, shard_id, lease) do
    path = Shard.db_path(shard_id)
    temp = "#{path}.promote.#{System.unique_integer([:positive])}"

    try do
      # The rename is the last step that can leave the live path holding replica bytes, so it comes
      # only after staging has verified them. Publishing follows, because a local file the store
      # does not know about is the fork the provenance sidecar exists to catch.
      with :ok <- stage(follower, shard_id, temp),
           :ok <- File.rename(temp, path),
           :ok <- Storage.flush(shard_id, path),
           {:ok, etag} <- Storage.object_etag(shard_id) do
        # Only now does the follower stop being a replica of this shard. Doing it earlier would
        # mean a failure above left the node holding neither a replica nor a primary.
        Follower.forget(follower, shard_id)

        bytes = File.stat!(path).size

        Logger.info(
          "promoted #{shard_id} from a local replica at epoch #{lease.epoch} " <>
            "(#{bytes}B, etag #{inspect(etag)})"
        )

        {:ok, %{shard_id: shard_id, epoch: lease.epoch, etag: etag, bytes: bytes}}
      end
    after
      Enum.each(["", "-wal", "-shm"], &File.rm(temp <> &1))
    end
  end

  # The `.db` and `-wal` move together or not at all: they are one lineage, and a database paired
  # with the wrong WAL is the corrupt-looking state `Follower`'s install ordering also guards.
  # Copied rather than renamed so a failure part-way leaves the replica intact and the promotion
  # retryable.
  defp install(follower, shard_id, path) do
    File.mkdir_p!(Path.dirname(path))

    with :ok <- File.cp(Follower.db_path(follower, shard_id), path),
         :ok <- copy_wal(follower, shard_id, path) do
      :ok
    else
      {:error, reason} -> {:error, {:install_failed, reason}}
    end
  end

  # A replica whose WAL is absent or empty is a complete database on its own — the primary had
  # checkpointed. Removing any stale local `-wal` is still required, or SQLite would apply
  # somebody else's frames to these pages.
  defp copy_wal(follower, shard_id, path) do
    src = Follower.wal_path(follower, shard_id)
    dst = path <> "-wal"

    _ = File.rm(dst)
    _ = File.rm(path <> "-shm")

    case File.stat(src) do
      {:ok, %{size: size}} when size > 0 -> File.cp(src, dst)
      _ -> :ok
    end
  end

  # Fold the WAL into the database, then prove the result is readable.
  #
  # The checkpoint is explicit rather than a side effect of closing the connection. Both would
  # work — gate 1 measured that a clean close checkpoints — but "it happens when we close" is the
  # kind of load-bearing accident that survives until someone adds a second reader and it quietly
  # stops happening.
  #
  # `quick_check` is the gate on serving these bytes at all: they arrived as raw byte ranges
  # appended to a WAL by a process that never opened the database, so this is the first time
  # anything has asked SQLite whether they make sense.
  defp checkpoint_and_verify(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        try do
          with {:ok, _} <- Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", []),
               {:ok, %{rows: [["ok"]]}} <- Connection.query(conn, "PRAGMA quick_check", []) do
            :ok
          else
            {:ok, %{rows: rows}} -> {:error, {:quick_check, rows}}
            {:error, reason} -> {:error, {:checkpoint_failed, reason}}
          end
        after
          Connection.close(conn)
        end

      {:error, reason} ->
        {:error, {:open_failed, reason}}
    end
  end

  # A coordinator for this shard on this node owns those files and is renewing its own lease.
  # Copying over them would be writing underneath a live writer, and the lease acquire below would
  # not even notice — it is the same owner string.
  defp ensure_no_coordinator(shard_id) do
    case Registry.lookup(Fathom.ShardRegistry, shard_id) do
      [] -> :ok
      [{_pid, _}] -> {:error, :coordinator_running}
    end
  rescue
    ArgumentError -> :ok
  end

  defp ensure_replica(follower, shard_id) do
    if Follower.state_of(follower, shard_id) &&
         File.exists?(Follower.db_path(follower, shard_id)) do
      :ok
    else
      {:error, :no_replica}
    end
  rescue
    ArgumentError -> {:error, :no_replica}
  end

  defp acquire(shard_id) do
    case Storage.acquire_lease(shard_id, owner(), @lease_ttl_ms) do
      {:ok, lease} -> {:ok, lease}
      {:error, {:held, other}} -> {:error, {:lease_held, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp owner, do: Fathom.Shard.Heartbeat.owner()
end
