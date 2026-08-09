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

  @type state :: %{
          wal_gen: non_neg_integer(),
          salt1: non_neg_integer(),
          offset: non_neg_integer()
        }

  @type plan ::
          {:append, non_neg_integer(), pos_integer()}
          | {:reset, non_neg_integer(), pos_integer()}
          | :nothing

  @doc """
  Decide the byte range to ship.

  Returns `{:append, offset, len}`, `{:reset, 0, len}` (ship the whole current WAL, header first),
  or `:nothing`.
  """
  @spec plan(state() | nil, Wal.header() | :empty) :: plan()
  def plan(_state, :empty), do: :nothing

  # Never shipped this shard. Everything the WAL currently holds is new to the follower.
  def plan(nil, %{size: size}) when size > 0, do: {:reset, 0, size}

  def plan(%{wal_gen: gen, salt1: salt}, %{ckpt_seq: seq, salt1: s, size: size})
      when seq != gen or s != salt do
    # Case 2: the WAL was reset. Offsets from the old generation mean nothing now.
    {:reset, 0, size}
  end

  def plan(%{offset: offset}, %{size: size}) when size > offset do
    {:append, offset, size - offset}
  end

  def plan(%{offset: offset}, %{size: size}) when size < offset do
    # The WAL shrank without the generation moving. SQLite should not do this, so we do not know
    # what we are looking at — re-ship everything rather than compute a range from an assumption
    # that has already proven false.
    {:reset, 0, size}
  end

  def plan(%{}, %{}), do: :nothing

  @doc """
  The state to record after a plan has been shipped and acked.

  Only ever called on success. Advancing before the quorum acks would leave the primary believing
  followers hold bytes they refused, and the next delta would start past a gap none of them have.
  """
  @spec advance(Wal.header(), plan()) :: state()
  def advance(%{ckpt_seq: seq, salt1: salt}, {kind, offset, len})
      when kind in [:append, :reset] do
    %{wal_gen: seq, salt1: salt, offset: offset + len}
  end
end
