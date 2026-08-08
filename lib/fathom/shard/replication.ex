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
  Ship `push` to every shipper and return once `q` of them have acked.

  Returns `:ok`, or `{:error, {:no_quorum, reason}}` where reason is `:impossible` (too many
  followers refused for `q` to be reachable) or `:timeout`.

  The stragglers are **not** waited for and **not** cancelled — they keep applying the frames and
  catch up on their own. That is the entire measured value of a quorum: gate 2 recorded 2-of-4 at
  1.6 ms against 4-of-4 at 134 ms with two followers 60 ms away, and the difference is exactly this
  function choosing to stop counting.
  """
  @spec ship_quorum([GenServer.server()], Protocol.Push.t(), pos_integer(), timeout()) ::
          :ok | {:error, {:no_quorum, :impossible | :timeout}}
  def ship_quorum(shippers, %Protocol.Push{} = push, q, timeout \\ @default_timeout_ms) do
    n = length(shippers)
    # Raises on q >= n. That is intentional and load-bearing — see Quorum.new/3.
    quorum = Quorum.new(n, q, push.offset + byte_size(push.payload))

    for s <- shippers, do: Shipper.push(s, push)

    deadline = System.monotonic_time(:millisecond) + timeout
    collect(quorum, push.shard_id, deadline)
  end

  defp collect(quorum, shard_id, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, {:no_quorum, :timeout}}
    else
      receive do
        {:repl_reply, from, {:ack, ^shard_id, next}} ->
          quorum |> Quorum.ack(from, next) |> settle(shard_id, deadline)

        {:repl_reply, from, {:reject, ^shard_id, reason, expected}} ->
          log_reject(shard_id, reason)
          quorum |> Quorum.reject(from, reason, expected) |> settle(shard_id, deadline)
      after
        remaining -> {:error, {:no_quorum, :timeout}}
      end
    end
  end

  defp settle({:reached, _}, _shard_id, _deadline), do: :ok
  defp settle({:impossible, _}, _shard_id, _deadline), do: {:error, {:no_quorum, :impossible}}
  defp settle({:pending, q}, shard_id, deadline), do: collect(q, shard_id, deadline)

  # `:offset_mismatch` is the routine one — a lost or reordered push, retryable by rewinding — so
  # it does not deserve the same volume as a fence trip. `:stale_epoch` means a deposed primary is
  # still shipping, which is a real event an operator wants to see.
  defp log_reject(_shard_id, :offset_mismatch), do: :ok

  defp log_reject(shard_id, reason) do
    Logger.warning("replication rejected for #{shard_id}: #{inspect(reason)}")
  end
end
