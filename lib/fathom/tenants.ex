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

  This module is the entry point; the durable, cross-node work runs in
  `Fathom.Tenants.DeleteJob`. The admission re-mint guard is `Fathom.Tenants.Tombstones`
  (an ETS set checked O(1) off the Postgres hot path).
  """
  alias Fathom.Tenants.Tombstones

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
end
