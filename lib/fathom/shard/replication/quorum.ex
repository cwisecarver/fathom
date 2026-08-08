defmodule Fathom.Shard.Replication.Quorum do
  @moduledoc """
  Tracking one push across N followers until Q of them ack — Phase 2 A2.
  See `docs/a2-quorum-replication.md`.

  **Pure, like `FollowerLog`.** A quorum is a counting problem, and every way it can be wrong —
  returning early, double-counting one follower, or blocking forever when the quorum has become
  unreachable — is visible without a socket.

  ## Measured reasons this shape matters

  Gate 2 measured what the alternatives cost (`test/fathom/shard/wal_quorum_bench_test.exs` and the
  RTT sweep in `deploy/chaos/a2_rtt_split.exs`):

    * Waiting for **all** N instead of Q is **32×** worse with a straggler on loopback, and **82×**
      worse with two far followers at 60 ms. Hence `q < n` is enforced at construction, not left to
      config review.
    * A quorum only pays off when followers **differ**. With four equidistant followers 2-of-4
      tracked 4-of-4 exactly. That is a placement rule rather than a code one, but it is why this
      module counts *whoever answers first* and never prefers a particular follower.

  ## Failing fast when the quorum is unreachable

  `reject/4` exists so the caller does not sit on a timeout it can already prove will expire. Once
  `n - rejected < q`, no set of future acks can reach Q, and the honest answer is `:impossible`
  immediately — a commit blocked on an impossible quorum is an outage, and the operator needs it to
  surface as an error rather than as latency.
  """

  @enforce_keys [:n, :q, :offset]
  defstruct [:n, :q, :offset, acked: MapSet.new(), rejected: MapSet.new()]

  @type t :: %__MODULE__{
          n: pos_integer(),
          q: pos_integer(),
          offset: non_neg_integer(),
          acked: MapSet.t(),
          rejected: MapSet.t()
        }

  @type outcome ::
          {:reached, t()}
          | {:pending, t()}
          | {:impossible, t()}

  @doc """
  Start tracking a push to `n` followers needing `q` acks, expecting them at `offset`.

  Raises on `q >= n` or `q < 1`. This is a boot/config error, and the design doc's central measured
  finding is that `Q = N` silently converts every follower from redundancy into a liability — so it
  must be impossible to construct, not merely discouraged in a comment.
  """
  @spec new(pos_integer(), pos_integer(), non_neg_integer()) :: t()
  def new(n, q, offset) when is_integer(n) and is_integer(q) do
    cond do
      q < 1 ->
        raise ArgumentError, "write quorum must be at least 1, got #{q}"

      q >= n ->
        raise ArgumentError,
              "write quorum #{q} must be < #{n} followers: Q=N tolerates zero follower failures " <>
                "and inherits the slowest replica's latency (measured 32-82x worse — see " <>
                "docs/a2-quorum-replication.md)"

      true ->
        %__MODULE__{n: n, q: q, offset: offset}
    end
  end

  @doc """
  Record an ack from `follower` for `next_offset`.

  An ack for an offset other than the one this push produces is treated as a **rejection**, not an
  ack. The two sides disagreeing about the follower's position is exactly the divergence the
  offset field exists to catch, and counting it toward the quorum would let a follower that is
  writing the wrong bytes satisfy a commit.
  """
  @spec ack(t(), term(), non_neg_integer()) :: outcome()
  def ack(%__MODULE__{offset: expected} = state, follower, next_offset)
      when next_offset != expected do
    reject(state, follower, :offset_mismatch, next_offset)
  end

  def ack(%__MODULE__{} = state, follower, _next_offset) do
    # Idempotent: a duplicate ack from the same follower must not count twice, or a single chatty
    # follower could satisfy a quorum by itself.
    state = %{state | acked: MapSet.put(state.acked, follower)}
    settle(state)
  end

  @doc """
  Record a refusal (or a dead connection) from `follower`.

  `_expected` is accepted and ignored here on purpose: rewinding and re-sending is the shipper's
  job, and threading it through the counter would make this module care about retransmission.
  """
  @spec reject(t(), term(), atom(), non_neg_integer()) :: outcome()
  def reject(%__MODULE__{} = state, follower, _reason, _expected) do
    state = %{state | rejected: MapSet.put(state.rejected, follower)}
    settle(state)
  end

  defp settle(%__MODULE__{} = state) do
    acked = MapSet.size(state.acked)
    rejected = MapSet.size(state.rejected)

    cond do
      acked >= state.q -> {:reached, state}
      state.n - rejected < state.q -> {:impossible, state}
      true -> {:pending, state}
    end
  end

  @doc "How many more acks are needed. Zero once the quorum is reached."
  @spec remaining(t()) :: non_neg_integer()
  def remaining(%__MODULE__{} = s), do: max(0, s.q - MapSet.size(s.acked))
end
