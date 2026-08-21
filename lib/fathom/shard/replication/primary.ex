defmodule Fathom.Shard.Replication.Primary do
  @moduledoc """
  What a primary should ship next — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  **Pure**, like `FollowerLog` and `Quorum`. Given what we last shipped and what the WAL header says
  now, decide the byte range. The caller does the reading and the shipping.

  This is the send-side counterpart to `FollowerLog`, and it defends the same silent failure from
  the other end: a primary that computes the wrong range hands a follower bytes that will splice
  two unrelated WAL generations together, and neither side errors.

  ## The three ways a WAL moves

  1. **Frames appended** — same generation, size grew. Ship `[offset, size)`.
  2. **The WAL was reset** — a checkpoint truncated it and SQLite rewrote the header with a new
     checkpoint sequence and new salts. Byte offsets restart, so ship from **0**, header included,
     and let the follower discard what it had. `FollowerLog` accepts a generation change only at
     offset 0 for exactly this reason.
  3. **Nothing happened** — size unchanged. Ship nothing rather than an empty push; a commit that
     wrote no frames (a no-op UPDATE) must not cost a network round trip.

  ## Why the salt is checked and not just the sequence number

  Both are read from the header. Corroborating them is cheap and the failure they catch is not
  theoretical: if the sequence number looks unchanged but the salt moved, the file is not the one
  we think we are appending to, and the safe response is to treat it as a new generation. The
  expensive direction is a needless full re-ship; the cheap-looking direction is corruption.
  """

  alias Fathom.Shard.Replication.Wal

  # A WAL holding only its header has nothing committed. Needed as an attribute rather than a call
  # because it is used in a guard.
  @header_bytes Wal.header_bytes()

  @type state :: %{
          wal_gen: non_neg_integer(),
          salt1: non_neg_integer(),
          offset: non_neg_integer()
        }

  @type plan ::
          {:append, non_neg_integer(), pos_integer()}
          | {:reset, non_neg_integer(), pos_integer()}
          | :nothing

  @typedoc """
  The most WAL one push may carry. `:infinity` (or a non-positive integer) means unbounded.
  """
  @type max_bytes :: pos_integer() | 0 | :infinity

  @doc """
  Decide the byte range to ship, unbounded.

  Equivalent to `plan(state, header, :infinity)`. Kept as the plain two-arity form because every
  pure test of the three WAL cases is written against it and the cap is orthogonal to all of them.
  """
  @spec plan(state() | nil, Wal.header() | :empty) :: plan()
  def plan(state, header), do: plan(state, header, :infinity)

  @doc """
  Decide the byte range to ship, carrying at most `max` bytes.

  Returns `{:append, offset, len}`, `{:reset, 0, len}` (ship the current WAL from its header), or
  `:nothing`.

  ## Why the length is capped, and why capping it is safe

  A push carries the delta since the follower's LAST ACKED position, so a follower that is not
  acking makes the next delta larger, which takes longer to send, which makes the next one larger
  still. That positive feedback is the 1024-tenant OOM
  (`docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`): measured on one shipper 40 s apart, the
  queued MESSAGE count fell (8,265 → 8,195) while the binary held DOUBLED (6,893 → 15,798 MB) and
  the mean payload went 832 KB → 1,593 KB. Bounding the message count cannot help when the messages
  are what grow; bounding the delta attacks the loop itself.

  A capped plan is a **partial** delta, and nothing downstream needs to change to accept one:

    * `FollowerLog.decide/2` matches a contiguous append at `next_offset` of ANY length, and
      `decide_fresh/2` accepts a capped reset because it only requires `offset == 0`;
    * `advance/2` records `offset + len`, so the next plan resumes exactly where this one stopped.

  A follower may therefore hold a WAL ending mid-frame between rounds. That is safe for the same
  reason crash recovery is: SQLite validates frame checksums and stops at the first bad one, so a
  torn tail reads as "not there yet", never as corruption. It is also never observable as a weaker
  guarantee, because `Session` does not ack the commit until the loop has handed over every byte
  through the commit point — an intermediate round can only leave a follower at a position it had
  already been acked at.
  """
  @spec plan(state() | nil, Wal.header() | :empty, max_bytes()) :: plan()
  def plan(_state, :empty, _max), do: :nothing

  # Never shipped this shard. Everything the WAL currently holds is new to the follower.
  def plan(nil, %{commit_extent: size}, max) when size > @header_bytes,
    do: {:reset, 0, cap(size, max)}

  def plan(%{wal_gen: gen, salt1: salt}, %{ckpt_seq: seq, salt1: s, commit_extent: size}, max)
      when seq != gen or s != salt do
    # Case 2: the WAL was reset. Offsets from the old generation mean nothing now.
    {:reset, 0, cap(size, max)}
  end

  def plan(%{offset: offset}, %{commit_extent: size}, max) when size > offset do
    {:append, offset, cap(size - offset, max)}
  end

  def plan(%{offset: offset}, %{commit_extent: size}, max) when size < offset do
    # The WAL shrank without the generation moving. SQLite should not do this, so we do not know
    # what we are looking at — re-ship everything rather than compute a range from an assumption
    # that has already proven false.
    {:reset, 0, cap(size, max)}
  end

  def plan(%{}, %{}, _max), do: :nothing

  # A non-positive cap means "off", matching how `:replication_max_queue` spells the same thing.
  # Folding that in here rather than at the call site keeps every caller from having to remember it.
  defp cap(len, max) when is_integer(max) and max > 0 and len > max, do: max
  defp cap(len, _max), do: len

  @doc """
  The state to record after a plan has been shipped and acked.

  Only ever called on success. Advancing before the quorum acks would leave the primary believing
  followers hold bytes they refused, and the next delta would start past a gap none of them have.

  `offset + len` rather than the header's `size`, which is what makes a capped plan resumable: after
  a partial ship this records where the payload actually stopped, so the next `plan/3` picks up from
  there instead of skipping the remainder.
  """
  @spec advance(Wal.header(), plan()) :: state()
  def advance(%{ckpt_seq: seq, salt1: salt}, {kind, offset, len})
      when kind in [:append, :reset] do
    %{wal_gen: seq, salt1: salt, offset: offset + len}
  end
end
