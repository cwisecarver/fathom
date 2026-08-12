defmodule Fathom.Shard.Replication do
  @moduledoc """
  A2 quorum replication — the fan-out that turns N shippers into one commit decision.
  See `docs/a2-quorum-replication.md`.

  Everything underneath is deliberately small and separable: `Protocol` is the wire format,
  `FollowerLog` and `Quorum` are pure decisions, `Follower` and `Shipper` are socket shells. This
  module is the only place they meet.

  **Nothing calls this from the commit path yet.** The WAL hook that will feed it exists
  (`native/fathom_udf/src/wal.rs`), but wiring the two together puts a network round trip inside a
  tenant's COMMIT, and that step deserves its own review rather than arriving as a side effect of
  building the transport.
  """

  require Logger

  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Quorum
  alias Fathom.Shard.Replication.Shipper

  @default_timeout_ms 5_000

  @doc """
  Send every push and return immediately, without counting a single ack.

  Used when the followers that are ALREADY current satisfy the quorum by themselves. Their bytes
  are in, so the commit is quorum-durable the moment it is planned — but the laggards still have to
  be sent their delta or they never catch up, and the tenant must not wait for them to do it. Their
  acks are picked up by the next commit's late-reply drain.

  Not shipping to them at all is the tempting simplification and it is the one that corrupts the
  invariant: a follower only leaves the laggard set by being sent bytes, so skipping it would leave
  it behind permanently while the shard reported a healthy quorum.
  """
  @spec ship_async([{GenServer.server(), Protocol.Push.t()}]) :: :ok
  def ship_async(pushes), do: Enum.each(pushes, fn {shipper, p} -> Shipper.push(shipper, p) end)

  @doc """
  Ship a **per-follower** push and return once `q` followers have acked.

  `pushes` is `[{shipper, push}]` — each follower gets the delta from *its own* position. That is
  what makes catch-up possible: a replica that fell behind is not sent the same bytes as one that is
  current, it is sent everything it is missing. In the common case every push is identical and the
  encode is shared.

  Returns `{:ok, acked, rejects}` or `{:error, {:no_quorum, reason, rejects}}`.

  **`acked` is which followers actually confirmed**, not merely that the quorum was met. The caller
  must advance only those: advancing a follower that rejected would leave the primary believing it
  holds bytes it refused, and the next delta would start past a gap only that follower has.

  `rejects` is `[{shipper, reason, follower_offset}]`. The reason separates a follower that needs
  seeding (`:unknown_shard`) from one that needs catching up (`:offset_mismatch`), and the offset is
  where the follower says it actually is — which lets the primary correct its own record instead of
  guessing.

  Stragglers are **not** waited for and **not** cancelled. That is the entire measured value of a
  quorum: gate 2 recorded 2-of-4 at 1.6 ms against 4-of-4 at 134 ms with two followers 60 ms away,
  and the difference is exactly this function choosing to stop counting.

  ## `q` is the RESIDUAL quorum over the followers that still need bytes

  `pushes` carries only the followers with something to receive; one that is already current is not
  in it. So the caller passes `q - already_current`, not the configured quorum, and `n` is the
  subset size. Passing the configured `q` against a shrunken `n` is what made `Quorum.new/2` raise
  `q >= n` mid-commit — a config guard firing on a per-commit subset, which is not a config error at
  all. The guard still holds on the subset: with `u` followers current out of `total`,
  `q - u < total - u` reduces to `q < total`, which `Fleet.validate_quorum!/0` enforces at boot. A
  residual of zero never reaches here — see `ship_async/1`.
  """
  @spec ship_quorum([{GenServer.server(), Protocol.Push.t()}], pos_integer(), timeout()) ::
          {:ok, [pid()], [{pid(), atom(), non_neg_integer()}]}
          | {:error, {:no_quorum, :impossible | :timeout, [{pid(), atom(), non_neg_integer()}]}}
  def ship_quorum(pushes, q, timeout \\ @default_timeout_ms) do
    n = length(pushes)
    # Raises on q >= n. That is intentional and load-bearing — see Quorum.new/2.
    quorum = Quorum.new(n, q)

    # What each follower should report back. Per-follower because their starting offsets differ;
    # a single expected offset was the assumption that made catch-up impossible.
    expected =
      Map.new(pushes, fn {shipper, p} ->
        {resolve(shipper), p.offset + byte_size(p.payload)}
      end)

    shard_id = pushes |> hd() |> elem(1) |> Map.fetch!(:shard_id)
    for {shipper, p} <- pushes, do: Shipper.push(shipper, p)

    deadline = System.monotonic_time(:millisecond) + timeout
    collect(quorum, shard_id, deadline, {[], []}, expected)
  end

  # Replies identify their sender by pid; callers may pass names. Normalise so the expectation map
  # can be keyed the same way the replies arrive.
  defp resolve(pid) when is_pid(pid), do: pid
  defp resolve(name), do: Process.whereis(name) || name

  # `rejects` accumulates {shipper, reason} so the caller can act on WHY a follower refused rather
  # than only on the fact that the quorum failed. The one that matters is `:unknown_shard`, which
  # is not a failure at all but the signal that a follower has never been seeded — see
  # `Fathom.Shard.Replication.Session`, which turns it into a seed rather than an alert.
  defp collect(quorum, shard_id, deadline, {acked, rejects}, expected) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, {:no_quorum, :timeout, rejects}}
    else
      receive do
        {:repl_reply, from, {:ack, ^shard_id, next}} ->
          # An ack for a position we did not send that follower means the two sides disagree about
          # where it is. Counting it would let a replica holding the wrong bytes satisfy a commit,
          # so it is a rejection — and reported as one, carrying the follower's own offset so the
          # caller can correct its record.
          if Map.get(expected, from) == next do
            quorum
            |> Quorum.ack(from)
            |> settle(shard_id, deadline, {[from | acked], rejects}, expected)
          else
            quorum
            |> Quorum.reject(from)
            |> settle(
              shard_id,
              deadline,
              {acked, [{from, :offset_mismatch, next} | rejects]},
              expected
            )
          end

        {:repl_reply, from, {:reject, ^shard_id, reason, follower_offset}} ->
          log_reject(shard_id, reason)

          quorum
          |> Quorum.reject(from)
          |> settle(
            shard_id,
            deadline,
            {acked, [{from, reason, follower_offset} | rejects]},
            expected
          )
      after
        remaining -> {:error, {:no_quorum, :timeout, rejects}}
      end
    end
  end

  defp settle({:reached, _}, _shard, _deadline, {acked, rejects}, _expected),
    do: {:ok, acked, rejects}

  # A LOST QUORUM ALWAYS LOGS ITS REASONS, even when the individual rejects are the quiet ones.
  #
  # `log_reject/2` deliberately says nothing for `:offset_mismatch` and `:unknown_shard`, because
  # per-reject they are routine and self-correcting. But when they are what COST the quorum, the
  # commit fails, the tenant gets a 503, and — until this line — nothing anywhere named a cause.
  # That is how a total replication failure stayed invisible on the chaos rig (2026-08-11): every
  # write returned FILO_NO_QUORUM while the node logs showed only successful seeds, so the
  # investigation had to reach for the release RPC to learn something the failure itself knew.
  #
  # Routine per-event, alarming in aggregate: exactly the shape that belongs at the decision point
  # rather than at the event.
  defp settle({:impossible, _}, shard, _deadline, {_acked, rejects}, _expected) do
    Logger.warning(
      "replication quorum IMPOSSIBLE for #{shard}: " <>
        inspect(Enum.map(rejects, fn {_from, reason, at} -> {reason, at} end))
    )

    {:error, {:no_quorum, :impossible, rejects}}
  end

  defp settle({:pending, q}, shard, deadline, tally, expected),
    do: collect(q, shard, deadline, tally, expected)

  # `:offset_mismatch` is the routine one — a lost or reordered push, retryable by rewinding — so
  # it does not deserve the same volume as a fence trip. `:stale_epoch` means a deposed primary is
  # still shipping, which is a real event an operator wants to see.
  defp log_reject(_shard_id, :offset_mismatch), do: :ok
  defp log_reject(_shard_id, :unknown_shard), do: :ok

  defp log_reject(shard_id, reason) do
    Logger.warning("replication rejected for #{shard_id}: #{inspect(reason)}")
  end
end
