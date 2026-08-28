defmodule Fathom.Shard.Replication.FollowerLog do
  @moduledoc """
  The follower's accept/reject decision — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  **Deliberately a pure function**, with the socket and the filesystem kept out in
  `Fathom.Shard.Replication.Follower`. Every way this can corrupt a tenant's database is a decision
  made here — accepting frames from a deposed primary, appending across a checkpoint seam, or
  writing a delta at the wrong offset — and none of those needs a socket to test. The same
  reasoning as `WarmFollower.headroom?/4` and `Snapshots.Retention.plan/3`: the branch that must
  never be wrong should be reachable from a plain unit test.

  The failure mode being defended against is the quiet one. A follower that appends the wrong bytes
  does not raise; it produces a SQLite file that looks fine until it is promoted, at which point the
  data is wrong and the primary that could have corrected it is gone.
  """

  alias Fathom.Shard.Replication.Protocol.Push

  @typedoc """
  What the follower knows about one shard it is following.

  `nil` state means "never seen this shard", which is distinct from offset 0 — a fresh follower
  must be *seeded* (the base `.db` + `-wal` copied, as `Storage.pull/2` already does) before frames
  mean anything, and accepting a mid-stream delta into an empty directory would fabricate a
  database out of a fragment.

  `torn` means **the `.db` this follower holds is a generation behind its `-wal`**, so the two no
  longer compose into a database. It is set by every `decide_fresh/2` route and cleared only by a
  seed, and it exists because of a rig failure on 2026-08-12 where a promotion served a tenant an
  EMPTY database over a working stored object
  (`docs/reviews/a2-checkpoint-torn-replica-2026-08-12.md`).

  The sequence is ordinary, not exotic: a follower is seeded with the primary's `.db` AND its
  current `-wal`; the primary then checkpoints — which every durability flush does, so every
  `:shard_flush_interval_ms` — moving pages into ITS `.db` and restarting the WAL with fresh salts.
  We truncate our WAL to match the new generation and our `.db` stays where it was, so every page
  the checkpoint moved is now in NEITHER of our two files.

  Nothing in the position marked that. `wal_gen` is the field that should have — `t:Storage.
  position/0` says it "increases on every checkpoint" — but `ckpt_seq` restarts when SQLite
  recreates the WAL file, which is exactly why the `salt1` clause below had to exist underneath it.
  So the seam is invisible to a `{epoch, wal_gen, offset}` comparison, and `Promote.fresher?/2`
  ranked a torn replica as strictly ahead of the object and promoted it.

  `lineage` is a DIFFERENT COUNTER from `epoch`, and that distinction is the whole of expert review
  2026-08-24 #12. `epoch` is the primary's LOCK epoch — what `decide/2` fences pushes against, and
  what `release_lease` RESETS TO 1 on every clean idle-drop, drain and handoff. `lineage` is the
  monotonic ownership counter the stored object's position stamp carries, which never resets.
  `Promote.fresher?/2` compares a replica against that stamp, so it has to compare lineages:
  comparing the lock epoch against it made promotion inert from a shard's SECOND replicating open
  onward — silently, because a reset epoch of 1 loses to every stamp.

  `0` means "not stated" — a seed from a peer that predates the wire field, or one taken while
  `Protocol.lineage_wire?/0` was off. `fresher?/2` refuses to rank those rather than guessing,
  which is the same inert-but-safe behaviour as before.
  """
  @type t :: %{
          epoch: non_neg_integer(),
          wal_gen: non_neg_integer(),
          salt1: non_neg_integer(),
          next_offset: non_neg_integer(),
          torn: boolean(),
          lineage: non_neg_integer(),
          wal_ordinal: non_neg_integer()
        }

  @type decision ::
          {:append, t()}
          | {:reset_then_append, t()}
          | {:reject, atom(), non_neg_integer()}

  @doc """
  Decide what to do with `push` given the follower's current state for that shard.

  Returns `{:reject, reason, expected_offset}` rather than a bare reason so the primary can rewind
  and re-send from `expected_offset`. A gap should cost a retransmit, not a full re-seed from S3.
  """
  @spec decide(t() | nil, Push.t()) :: decision()
  def decide(nil, _push), do: {:reject, :unknown_shard, 0}

  def decide(%{epoch: epoch}, %Push{epoch: pushed}) when pushed < epoch do
    # A deposed primary still shipping. This is THE fence: the lease epoch is already fathom's
    # fencing token (Fathom.Shard.Storage), so a node that lost the lease and has not noticed yet
    # cannot land writes on top of the new owner's. Expected offset is meaningless to a stale
    # primary, so 0 — it must re-acquire, not rewind.
    {:reject, :stale_epoch, 0}
  end

  def decide(%{epoch: epoch} = state, %Push{epoch: pushed} = push) when pushed > epoch do
    # A NEW primary took over. Its WAL is its own; nothing about our offsets carries across an
    # ownership change, so treat this exactly like a generation reset rather than trying to splice.
    decide_fresh(%{state | epoch: pushed}, push)
  end

  # A DIFFERENT WAL, NOT A LATER ONE — AND THIS MUST OUTRANK BOTH `wal_gen` COMPARISONS BELOW.
  # `ckpt_seq` counts checkpoints within one WAL file, so it restarts at 0 when SQLite deletes and
  # recreates the file — which it does the moment the last connection to the shard closes, i.e.
  # after every Hrana stream on a quiet shard. The generation then goes BACKWARDS or stays equal
  # while the bytes are from a completely new lineage.
  #
  # `salt1` is the WAL's identity and is what `Primary.plan/3` has always keyed on
  # (`when seq != gen or s != salt`). Until it crossed the wire the two sides disagreed
  # permanently: the primary shipped `{:reset, 0, _}` on the salt change, the follower saw the same
  # generation and demanded its old offset, and NO commit could ever satisfy both. That deadlock
  # was every write after the first failing `{:no_quorum, :impossible}` on the chaos rig, at a
  # fixed offset, forever.
  #
  # THIS CLAUSE USED TO SIT BELOW THE TWO `wal_gen` CLAUSES, which made it reachable only when the
  # generations were EQUAL — so the comment above described a hazard the code handled in only one
  # of its two directions (expert review 2026-08-20 #7). The backwards direction is the ordinary
  # one: a PASSIVE checkpoint takes ckpt_seq to 1, the last stream closes, SQLite unlinks and
  # recreates the WAL at ckpt_seq 0 with a fresh salt, and 1 → 0 was answered `:stale_wal_gen`.
  # That reject is in `Session`'s `@settled_rejects`, so the primary's per-follower record was
  # never corrected and the next plan re-derived the same reset — forever, for every follower at
  # once, i.e. FILO_NO_QUORUM on every write to that tenant until the Follower process restarted.
  #
  # Ordering rule: the epoch clauses stay ABOVE this one. A deposed primary must be fenced whatever
  # its salt says; salt only identifies which WAL, never who owns the shard.
  def decide(%{salt1: salt} = state, %Push{salt1: pushed} = push) when pushed != salt do
    decide_fresh(%{state | salt1: pushed}, push)
  end

  def decide(%{wal_gen: gen}, %Push{wal_gen: pushed}) when pushed < gen do
    # Frames from before a checkpoint we have already applied. Dropping them is correct and safe:
    # the data they carry is already in our main database file.
    {:reject, :stale_wal_gen, 0}
  end

  def decide(%{wal_gen: gen} = state, %Push{wal_gen: pushed} = push) when pushed > gen do
    # The primary checkpointed: its WAL was truncated and rewritten with fresh salts, so byte
    # offsets restart and are NOT comparable to ours. Appending across that seam is the corruption
    # this field exists to prevent — discard and start the new generation from its beginning.
    decide_fresh(%{state | wal_gen: pushed}, push)
  end

  def decide(%{next_offset: next} = state, %Push{offset: off} = push) when off == next do
    {:append, %{merge_ordinal(state, push) | next_offset: next + byte_size(push.payload)}}
  end

  def decide(%{next_offset: next}, %Push{}) do
    # Same generation, wrong place. A lost, duplicated or reordered push. Retryable by construction:
    # tell the primary where we actually are.
    {:reject, :offset_mismatch, next}
  end

  # A new epoch or a new WAL generation both mean "your offsets are meaningless now". The only
  # payload we can accept is one that starts at the beginning of the new generation.
  defp decide_fresh(state, %Push{offset: 0} = push) do
    {:reset_then_append,
     %{
       state
       | wal_gen: push.wal_gen,
         salt1: push.salt1,
         next_offset: byte_size(push.payload),
         # TORN. Every route here means "your offsets are meaningless now", and the reason they are
         # meaningless is that the primary's `.db` moved on without us: a checkpoint drained pages
         # into it, or a new primary brought its own. We can make our WAL match the new generation
         # — `apply_write(_, :truncate)` does — but we cannot conjure the pages that left the old
         # one, and they are not in our `.db` either.
         #
         # So the replica keeps REPLICATING (this is not a reject; the alternative deadlocks the
         # commit path, which is why the salt clause exists) but stops being a COPY of anything
         # until a seed rebuilds the pair. `Promote.fresher?/2` refuses it while this is set, which
         # is the whole fix: promotion falls back to the stored object, which is always correct.
         torn: true,
         # TAKEN OUTRIGHT, never merged — this is a DIFFERENT WAL, and carrying the previous
         # generation's ordinal across the seam is precisely the unsound comparison #2 exists to
         # remove. An unstated ordinal lands here as 0, which `Promote.fresher?/2` refuses to rank,
         # so the replica falls back to the stored object rather than to a stale number.
         wal_ordinal: stated_ordinal(push)
     }}
  end

  defp decide_fresh(_state, %Push{}), do: {:reject, :offset_mismatch, 0}

  # The ordinal the push STATES, or 0 for "not stated" — a peer that predates the field, or one
  # whose `Protocol.ordinal_wire?/0` gate is off. A real ordinal is always >= 1: the coordinator
  # starts its counter at 0 and `handle_call({:wal_ordinal, salt}, …)` replies `current + 1` for the
  # first salt it sees, so 0 is unambiguously "unknown" rather than a zeroth generation.
  defp stated_ordinal(%Push{wal_ordinal: n}) when is_integer(n) and n > 0, do: n
  defp stated_ordinal(_), do: 0

  # On an APPEND the WAL has not changed, so the ordinal has not either — but a push may now STATE
  # one that this follower has never recorded (the gate was flipped on mid-session). Take it when it
  # is stated, keep what we have when it is not; never write 0 over a known ordinal, which would
  # move a rankable replica back to "unknown" for no reason.
  defp merge_ordinal(state, push) do
    case stated_ordinal(push) do
      0 -> state
      n -> %{state | wal_ordinal: n}
    end
  end

  @doc """
  State for a shard that has just been seeded from storage.

  `wal_gen` and `next_offset` come from the seed, not from zero: a pull copies the primary's `.db`
  AND its current `-wal`, so the follower already holds bytes and the first delta it can accept
  continues from there.
  """
  @spec seeded(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: t()
  def seeded(epoch, wal_gen, salt1, wal_bytes, lineage \\ 0) do
    # `torn: false` — a seed is the ONE event that rebuilds `.db` and `-wal` together, so it is the
    # only thing that can clear it. See the `torn` note on `t:t/0`.
    #
    # `lineage` rides along untouched through every `decide/2` clause — they all update named fields
    # on the existing map — which is why only the SEED has to carry it. A push that could bring a
    # STALE one (a new primary taking over an existing follower) routes through `decide_fresh/2`,
    # whose only accepting clause sets `torn: true`, and a torn replica is refused by
    # `Promote.fresher?/2` and `Follower.offerable/2` alike.
    %{
      epoch: epoch,
      wal_gen: wal_gen,
      salt1: salt1,
      next_offset: wal_bytes,
      torn: false,
      lineage: lineage,
      # 0 = "not stated". A seed cannot know it: `SeedBegin` carries no ordinal, and inventing one
      # here would put this replica on a scale the primary never assigned. The first push on the
      # seeded WAL supplies it, and until then `Promote.fresher?/2` refuses to rank — the object
      # wins, which is the pre-A2 answer and always correct.
      wal_ordinal: 0
    }
  end
end
