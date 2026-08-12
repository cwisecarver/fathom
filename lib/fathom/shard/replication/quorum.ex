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

  @enforce_keys [:n, :q]
  defstruct [:n, :q, acked: MapSet.new(), rejected: MapSet.new()]

  @type t :: %__MODULE__{
          n: pos_integer(),
          q: pos_integer(),
          acked: MapSet.t(),
          rejected: MapSet.t()
        }

  @type outcome ::
          {:reached, t()}
          | {:pending, t()}
          | {:impossible, t()}

  @doc """
  Start tracking a push to `n` followers needing `q` acks.

  **Offset-free.** It used to carry a single expected offset, which quietly assumed every follower
  was at the same position — true only until one falls behind and needs a catch-up delta. The
  expectation is now per-follower and lives in `Fathom.Shard.Replication`, which knows what it sent
  to whom; this module counts.

  Raises on `q >= n` or `q < 1`. This is a boot/config error, and the design doc's central measured
  finding is that `Q = N` silently converts every follower from redundancy into a liability — so it
  must be impossible to construct, not merely discouraged in a comment.
  """
  @spec new(pos_integer(), pos_integer()) :: t()
  def new(n, q) when is_integer(n) and is_integer(q) do
    cond do
      q < 1 ->
        raise ArgumentError, "write quorum must be at least 1, got #{q}"

      q >= n ->
        raise ArgumentError,
              "write quorum #{q} must be < #{n} followers: Q=N tolerates zero follower failures " <>
                "and inherits the slowest replica's latency (measured 32-82x worse — see " <>
                "docs/a2-quorum-replication.md)"

      true ->
        %__MODULE__{n: n, q: q}
    end
  end

  @doc """
  Record an ack from `follower`.

  The caller must already have checked that the acked offset is the one it expected from THAT
  follower — an ack for a different position means the two sides disagree about where the follower
  is, and counting it would let a replica holding the wrong bytes satisfy a commit. See
  `Fathom.Shard.Replication.collect/4`, which rejects instead of acking in that case.
  """
  @spec ack(t(), term()) :: outcome()
  def ack(%__MODULE__{} = state, follower) do
    # Idempotent: a duplicate ack from the same follower must not count twice, or a single chatty
    # follower could satisfy a quorum by itself.
    state = %{state | acked: MapSet.put(state.acked, follower)}
    settle(state)
  end

  @doc """
  Record a refusal (or a dead connection) from `follower`.

  Why it refused is not this module's business: rewinding, catching up and re-seeding are all the
  caller's job, and threading a reason through the counter would make it care about retransmission.
  """
  @spec reject(t(), term()) :: outcome()
  def reject(%__MODULE__{} = state, follower) do
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
