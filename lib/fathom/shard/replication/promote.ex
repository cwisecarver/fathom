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

  # A replica with NO LINEAGE cannot be ranked (expert review 2026-08-24 #12). `0` means "not
  # stated" — seeded by a peer that predates the wire field, or while `Protocol.lineage_wire?/0`
  # was off — and the honest answer is that we do not know, which means falling back to the stored
  # object. Above the comparison so it cannot be reached with a 0 on either side.
  def fresher?(%{lineage: 0}, _stamp), do: false
  def fresher?(_replica, %{epoch: 0}), do: false

  # NO ORDINAL ⇒ UNKNOWN ⇒ THE STORED OBJECT WINS (expert review 2026-08-26 #2, step 3b).
  #
  # 0 is "not stated": a replica seeded but not yet pushed to, one whose primary has
  # `Protocol.ordinal_wire?/0` off, or a peer that predates the field. It is NOT a zeroth
  # generation, and it must never be RANKED — in either direction. Above the comparison so a 0
  # cannot reach it.
  #
  # THE CONSEQUENCE IS DELIBERATE AND IT IS LARGE: while `REPLICATION_ORDINAL_WIRE` is off — the
  # default — every replica carries 0, so promote-on-open and cross-fleet recovery are INERT. That
  # is the point. The ordering they used until this commit was `wal_gen`, which is SQLite's
  # `ckpt_seq` and restarts at 0 whenever SQLite recreates the `-wal`, so it could rank two
  # unrelated WALs and promote a replica over a NEWER object — losing the acked tail silently. A
  # feature that is off is never worse than off; one that is subtly wrong on a data-loss path is.
  # Land, roll the fleet out, then flip the gate.
  def fresher?(%{wal_ordinal: 0}, _stamp), do: false

  # LINEAGE against LINEAGE. The replica's `epoch` is the primary's LOCK epoch, which
  # `release_lease` resets to 1 on every clean idle-drop, drain and handoff; the object's position
  # stamp carries the monotonic LINEAGE in its `epoch` slot (`Fathom.Shard.stamp_epoch/1` →
  # `Storage.next_lineage/1`). The 2026-08-20 #8 fix split one number into two and migrated only
  # the object side, so this compared a reset-to-1 counter against a never-resetting one: from a
  # shard's SECOND replicating open onward every replica read as not-fresher, and promote-on-open
  # plus `Recovery.best_replica/3` went inert fleet-wide — silently, recovering to the last flush
  # exactly as if A2 were switched off.
  #
  # The unsafe direction was reachable too: on a gate-toggled fleet a flush with
  # `lineage: :disabled` stamps the LOCK epoch into the position slot, so a replica stranded at a
  # higher lock epoch from an earlier crash-steal could outrank a NEWER object.
  #
  # `epoch` is still carried on the replica and is still the right value for `FollowerLog.decide/2`'s
  # fencing check — the two are not interchangeable and neither replaces the other.
  #
  # THE SECOND COMPONENT IS THE ORDINAL, NOT `wal_gen` (expert review 2026-08-26 #2, step 3b), and
  # that is what makes this an order at all. `wal_gen` is SQLite's `ckpt_seq`: it counts checkpoints
  # WITHIN one WAL file and restarts at 0 when SQLite deletes and recreates that file, which it does
  # after every Hrana stream on a quiet shard. Measured on this codebase, two consecutive streams
  # both read ckpt_seq=0 with salts 977542977 then 978380554 — same number, unrelated WALs. So
  # `{g1, o1} > {g2, o2}` was comparing positions from different files and could rank a replica
  # above an object that is strictly newer.
  #
  # The ordinal is assigned by the shard coordinator (`Fathom.Shard.wal_ordinal/2`), which answers
  # the SAME number for the same salt and a higher one for a new salt. The coordinator stamps the
  # object with it and `Replication.Session` pushes it to the replicas, so both sides are on one
  # scale rather than two counters that happen to look alike.
  #
  # `wal_gen` is still carried and is still right for `FollowerLog.decide/2`'s generation checks,
  # which ask a different question — is this frame from before a checkpoint I already applied.
  # The two are not interchangeable and neither replaces the other.
  #
  # A STAMP WITH NO ORDINAL FALLS THROUGH TO THE CATCH-ALL, which is the guard that matters: it
  # answers false — unknown, so the object wins — and specifically NOT "equal", which would be a
  # claim about a relationship nobody measured.
  def fresher?(%{lineage: l1, wal_ordinal: n1, next_offset: o1}, %{
        epoch: l2,
        wal_ordinal: n2,
        offset: o2
      })
      when is_integer(l1) and is_integer(l2) and is_integer(n1) and n1 > 0 and is_integer(n2) and
             n2 > 0 do
    {l1, n1, o1} > {l2, n2, o2}
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
      # FENCED publish, and the sidecar stamped from the etag it returns (expert review
      # 2026-08-20 #17).
      #
      # This used to be the 2-arity `Storage.flush/2` — the UNCONDITIONAL PUT, and the only
      # production caller of it. The moduledoc argues the replica is ahead of the object so we must
      # not fence on an etag WE wrote, which is true and is not an argument for fencing on nothing:
      # reading the object's CURRENT etag and requiring it to still be there costs one HEAD and
      # turns a blind clobber into a refusal.
      #
      # A `nil` etag (no stored object at all) is a legitimate first publish and the 3-arity
      # accepts it, so a brand-new shard is unaffected.
      with :ok <- stage(follower, shard_id, temp),
           {:ok, expected} <- current_object_etag(shard_id),
           :ok <- File.rename(temp, path),
           {:ok, etag, _carried} <- Storage.flush(shard_id, path, expected),
           :ok <- stamp_provenance(shard_id) do
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
      {:ok, lease} ->
        # RE-CHECK AFTER THE ACQUIRE (expert review 2026-08-20 #17). ensure_no_coordinator/1 runs
        # before this, and nothing stopped a checkout from starting a coordinator in between —
        # which, under the old same-owner string, the acquire could not detect either. Now the
        # owner is distinct so the acquire genuinely fences a coordinator's lease, and this closes
        # the remaining window where one appeared between the two.
        case ensure_no_coordinator(shard_id) do
          :ok ->
            {:ok, lease}

          {:error, _} = err ->
            Storage.release_lease(shard_id, lease)
            err
        end

      {:error, {:held, other, _stealable_at}} ->
        {:error, {:lease_held, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A DISTINCT owner string, never `Heartbeat.owner()` (expert review 2026-08-20 #17). This module's
  # own moduledoc names the hazard: a second acquire under the SAME owner takes
  # `acquire_existing`'s same-owner RECLAIM branch, "which is not a check at all". Mirrors the
  # migrator's `migrator@<node>@<token>`, so a coordinator's lease is genuinely fenced rather than
  # silently reclaimed out from under a live writer.
  defp owner, do: "promote@" <> Fathom.Shard.Heartbeat.owner()

  # The object's CURRENT etag, to fence the publish on. `:absent`/no object is a legitimate first
  # publish, so it fences on nil rather than refusing.
  defp current_object_etag(shard_id) do
    case Storage.object_etag(shard_id) do
      {:ok, etag} -> {:ok, etag}
      {:error, :not_found} -> {:ok, nil}
      {:error, _} = err -> err
    end
  end

  # The live path was written OUT OF BAND — this module renamed a file onto it without going
  # through a coordinator — so nothing stamped `<path>.etag` (expert review 2026-08-20 #17). Every
  # other writer of that path does: promote_pull/2, apply_flush_verdict/2, promote_replica/6.
  #
  # Without it, the next cold open reads `:missing` provenance, `resolve_fork/4` QUARANTINES the
  # promoted database as a planted file (`.forked.<ts>`, an ERROR log naming a fork that did not
  # happen) and re-pulls the stale object. When the publish above succeeded that is a spurious
  # alarm plus a full body transfer; when it FAILED, the lease is released anyway and the only copy
  # of the recovered writes is quarantined while the stale object is served. With
  # `:adopt_unprovenanced_warm` on it is worse: the file is adopted with the store's current etag,
  # which is exactly the clobber `shard.ex` documents.
  defp stamp_provenance(shard_id) do
    # `stamp_local_provenance/1` is the public seam that exists for exactly this — an out-of-band
    # writer of the live path — and it re-reads the object's etag itself, so it cannot record one
    # that disagrees with the store.
    Fathom.Shard.stamp_local_provenance(shard_id)
    :ok
  rescue
    e ->
      Logger.warning(
        "promoted #{shard_id} but could not stamp its provenance sidecar (#{inspect(e)}); the " <>
          "next cold open will treat the promoted file as a fork and re-pull the stored object"
      )

      :ok
  end
end
