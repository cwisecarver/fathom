defmodule Mix.Tasks.Fathom.Lock.Repair do
  @shortdoc "Repair a shard's UNDECODABLE lock object (operator remedy)"

  @moduledoc """
  Rewrites a shard's lock when -- and only when -- the stored lock body does not decode
  (expert review 2026-10-15 #23, decision D14).

      mix fathom.lock.repair <shard_id> [--epoch-floor N] [--claim N]

  A corrupt lock fails the shard CLOSED on both storage backends
  (`{:error, :corrupt_lock}` from `acquire_lease/3`, `fathom.storage.lock_corrupt` telemetry, an
  error log naming this task). It is deliberately not repaired automatically: the body carried the
  lock's EPOCH and its A2 `claim`, and a rewrite that guessed them could move the fencing token
  backward -- the hazard AGENTS.md records against "delete the lock instead of rolling it back".

  This task writes a released-style marker: the sentinel owner (`Storage.released_owner/0`), expiry
  0, so the next `acquire_lease/3` takes the ordinary STEAL path (epoch + 1, takeover claim above the
  marker's). Its epoch and claim are chosen to only ever go UP:

    * epoch -- the highest of the `"epoch"` integer salvaged from the corrupt bytes (if any) and
      `--epoch-floor`; when neither exists, the current unix time in seconds (an epoch moves once
      per takeover, so that is above any real value); plus 2.
    * claim -- only when replication is on: `Storage.takeover_claim/2` over `--claim - 1` (default:
      one above the object's current lineage). If the corrupt lock's holder crashed repeatedly under
      replication, pass `--claim` above the highest lineage any follower holds.

  Refuses (exit 1, nothing written) when the lock is decodable, absent, or the claim cannot be
  computed. On S3 the write is `If-Match` the corrupt body's etag, so a concurrent fix wins.
  See docs/runbooks/lock-repair.md.
  """

  use Mix.Task

  require Logger

  alias Fathom.Shard.Storage

  @impl true
  def run(args) do
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:req)

    {opts, rest, _} = OptionParser.parse(args, strict: [epoch_floor: :integer, claim: :integer])

    case rest do
      [shard] ->
        case repair(shard, opts) do
          {:ok, lease} ->
            Mix.shell().info(
              "repaired lock for #{shard}: marker epoch=#{lease.epoch} " <>
                "claim=#{inspect(Map.get(lease, :claim))}"
            )

          {:error, reason} ->
            Mix.raise("lock repair refused/failed: #{inspect(reason)}")
        end

      _ ->
        Mix.raise("usage: mix fathom.lock.repair <shard_id> [--epoch-floor N] [--claim N]")
    end
  end

  @doc """
  The repair itself, callable without Mix. `{:ok, marker}`, or `{:error, reason}` with
  `:lock_not_corrupt` / `:no_lock` / `{:transient_lookup, :lineage_unreadable}` as the refusals.
  """
  @spec repair(String.t(), keyword()) :: {:ok, Storage.lease()} | {:error, term()}
  def repair(shard_id, opts \\ []) do
    backend = Application.get_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)

    with {:ok, marker} <- backend.repair_corrupt_lock(shard_id, &build(shard_id, &1, opts)) do
      Logger.warning(
        "shard #{shard_id}: operator repaired an undecodable lock -> marker epoch #{marker.epoch}"
      )

      :telemetry.execute([:fathom, :storage, :lock_repaired], %{count: 1}, %{shard_id: shard_id})
      {:ok, marker}
    end
  end

  defp build(shard_id, raw, opts) do
    epoch = marker_epoch(raw, opts[:epoch_floor])
    previous_claim = if opts[:claim], do: opts[:claim] - 1

    case Storage.takeover_claim(shard_id, previous_claim) do
      :unknown ->
        {:error, {:transient_lookup, :lineage_unreadable}}

      claim ->
        marker = %{owner: Storage.released_owner(), epoch: epoch, expires_at_ms: 0}
        {:ok, if(is_integer(claim), do: Map.put(marker, :claim, claim), else: marker)}
    end
  end

  # Best-effort salvage of an epoch from bytes that did not decode as a lease: a truncated or
  # damaged JSON object may still name one. Only ever a floor that the +2 goes above, never trusted
  # as the value.
  defp marker_epoch(raw, floor) do
    salvaged =
      case Regex.run(~r/"epoch"\s*:\s*(\d+)/, raw) do
        [_, n] -> String.to_integer(n)
        _ -> nil
      end

    base = Enum.max(Enum.reject([salvaged, floor, 0], &is_nil/1))
    if(base > 0, do: base, else: System.system_time(:second)) + 2
  end
end
