defmodule Fathom.Tenants do
  @moduledoc """
  Tenant lifecycle orchestration (expert review 2026-07-14 #15) — the whole-shard
  operations the migration engine deliberately left out of scope: **delete** (GDPR
  Article 17 erasure / offboarding) and **export** (data portability).

  A "tenant" is one shard — one SQLite file. Deletion is the hard case: a full erase
  must reach the live stored object + lock, every retained migration version, every
  snapshot, the owner node's local file, every warm-follower cache copy fleet-wide, the
  directory row, and any pending per-shard Oban jobs — and it must leave a **tombstone**
  so novel-shard admission can't silently re-mint the subdomain as an empty shard.

  ## Ordering (why it's safe)

  `delete/1` sets the re-mint guard **first and synchronously** — tombstone the directory
  row, then broadcast so every node's `Fathom.Tenants.Tombstones` ETS set refuses the id
  and drops its warm-follower copy — and only then enqueues `Fathom.Tenants.DeleteJob` for
  the durable, retryable physical erase. So from the instant a delete starts, no new
  stream can open the shard.

  Purging the stored objects is safe even if a coordinator on another node still holds the
  lease: every coordinator flush is **fenced** (`Storage.flush/3` uses `If-Match`), so once
  the live object is gone the next flush 412s and the coordinator self-fences (dropping its
  buffered writes — exactly the erase we want) instead of re-creating the object. The home
  coordinator is drained first so the common single-home case never needs the self-fence.
  """
  require Logger

  import Ecto.Query, only: [from: 2]

  alias Fathom.{Directory, Shards}
  alias Fathom.Shard.Storage
  alias Fathom.ShardId
  alias Fathom.Tenants.{DeleteJob, Tombstones}

  # Per-shard Oban workers a delete must cancel so a queued migration/revert/retirement/
  # handoff can't run against (or resurrect) a shard being erased. The DeleteJob itself is
  # excluded — it must not cancel its own kind.
  @per_shard_workers ~w(
    Fathom.Migrator.ShardMigrationJob
    Fathom.Migrator.RevertJob
    Fathom.Migrator.RetirementJob
    Fathom.Rebalancer.HandoffJob
  )

  @doc """
  Deletes a tenant: tombstones the directory row and broadcasts the tombstone fleet-wide
  (both synchronous, so re-mint is blocked immediately), then enqueues `DeleteJob` for the
  physical erase. Returns `{:ok, :scheduled}`, `{:error, :invalid_shard_id}`, or the
  directory changeset error.
  """
  @spec delete(String.t()) :: {:ok, :scheduled} | {:error, term()}
  def delete(shard_id) do
    with {:ok, id} <- cast(shard_id),
         {:ok, _row} <- Directory.tombstone(id) do
      broadcast_deleted(id)
      enqueue_delete(id)
      {:ok, :scheduled}
    end
  end

  @doc """
  The physical erase, run by `DeleteJob` (and directly callable/testable). Cancels pending
  per-shard jobs, drains the home coordinator, and purges every stored object. Idempotent —
  every step tolerates already-gone state. Returns `:ok`, or `{:error, reason}` on a storage
  failure so the job retries.
  """
  @spec purge(String.t()) :: :ok | {:error, term()}
  def purge(shard_id) do
    with {:ok, id} <- cast(shard_id) do
      cancel_pending_jobs(id)
      # Stop the home coordinator BEFORE deleting storage: while its lease is still valid the
      # shutdown flushes/releases cleanly and never self-fences, so no `.fenced.<ts>` copy of
      # the tenant's data is left behind (the erase's whole point). Then note whether another
      # LIVE node still owns it (it will self-fence on its next fenced flush against the
      # now-gone object — a documented cross-node residual).
      Shards.stop(id)
      note_remote_holder(id)

      case Storage.purge_shard(id) do
        :ok ->
          rm_local(id)

          :telemetry.execute([:fathom, :tenants, :deleted], %{count: 1}, %{shard_id: id})
          :ok

        {:error, reason} ->
          Logger.error("tenant #{id}: storage purge failed (#{inspect(reason)}) — will retry")
          {:error, reason}
      end
    end
  end

  @doc """
  True if `shard_id` has been deleted (tombstoned). Checked O(1) on the admission path so a
  request for an erased subdomain is refused instead of re-minting an empty shard.
  """
  @spec tombstoned?(String.t()) :: boolean()
  def tombstoned?(shard_id), do: Tombstones.tombstoned?(shard_id)

  @doc """
  Announces `shard_id`'s deletion fleet-wide: records it in THIS node's tombstone set
  immediately (so re-mint is blocked the instant a delete starts) and pushes it over Oban's
  LISTEN/NOTIFY so every other node tombstones it and purges its warm-follower copy. Best-effort
  — a node that's down misses the push and converges on the periodic tombstone refresh. See
  `Fathom.Tenants.Tombstones`.
  """
  @spec broadcast_deleted(String.t()) :: :ok
  def broadcast_deleted(shard_id) do
    Tombstones.put(shard_id)
    Oban.Notifier.notify(Oban, Tombstones.channel(), %{shard_id: shard_id})
    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp cast(shard_id) do
    case ShardId.cast(shard_id) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, :invalid_shard_id}
    end
  end

  defp enqueue_delete(id) do
    %{shard_id: id}
    |> DeleteJob.new()
    |> Oban.insert()
    |> case do
      {:ok, _job} ->
        :ok

      {:error, reason} ->
        # The tombstone already blocks new traffic; a failed enqueue leaves a recoverable
        # half-deleted state that re-running delete/1 (or a manual purge) completes.
        Logger.error("tenant #{id}: failed to enqueue DeleteJob (#{inspect(reason)})")
        :ok
    end
  end

  defp cancel_pending_jobs(id) do
    Oban.cancel_all_jobs(
      from(j in Oban.Job,
        where: j.worker in ^@per_shard_workers,
        # Cancel the safely-cancellable states; an EXECUTING job holds the lease and is
        # handled by the drain + fenced-flush self-fence, not a cancel.
        where: j.state in ["scheduled", "available", "retryable"],
        where: fragment("?->>'shard_id' = ?", j.args, ^id)
      )
    )
  rescue
    _ -> {:ok, 0}
  catch
    :exit, _ -> {:ok, 0}
  end

  # Remove every local working file for the shard after the coordinator is stopped: the live
  # db + its WAL/SHM AND any quarantine copies (`.fenced.<ts>` / `.forked.<ts>` / `.corrupt.<ts>`)
  # a prior partial run or self-fence could have left — otherwise an erased tenant's data
  # lingers on local disk. The glob is anchored at `<id>.db` so it can't match a sibling id
  # (`<id>9.db` doesn't start with `<id>.db`).
  defp rm_local(id) do
    base = Fathom.Shard.db_path(id)
    for path <- Path.wildcard(base <> "*"), do: File.rm(path)
    :ok
  rescue
    _ -> :ok
  end

  defp note_remote_holder(id) do
    case Storage.lease_holder(id) do
      {:held, owner} when owner != nil ->
        # Only a DIFFERENT live node is the cross-node case worth surfacing. Our own
        # just-stopped coordinator can leave the lock a beat before purge_shard deletes it —
        # that's not a remote holder, so don't cry wolf.
        if owner == Fathom.Shard.Heartbeat.owner(), do: :ok, else: warn_remote_holder(id, owner)

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp warn_remote_holder(id, owner) do
    # A LIVE node still owns the lease — the purge deletes the live object out from under it
    # and that node self-fences on its next fenced flush. Surface it so a cross-node erase
    # relying on the self-fence is observable, not silent.
    Logger.warning(
      "tenant #{id}: purging while #{owner} still holds the lease — it will self-fence " <>
        "on its next flush (cross-node delete)"
    )

    :telemetry.execute([:fathom, :tenants, :purge_while_held], %{count: 1}, %{
      shard_id: id,
      owner: owner
    })
  end
end
