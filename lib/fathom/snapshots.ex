defmodule Fathom.Snapshots do
  @moduledoc """
  Point-in-time snapshots and restore for a shard (expert review 2026-07-14 #12).

  Because a tenant *is* one SQLite object, a snapshot is just a server-side copy of
  that object under a time-labeled key (`<shard>@snap-<id>`), and a restore is the
  copy back — no WAL-replay engine or copy-on-write page server. This is the
  fathom-managed mechanism (backend-uniform across `Local` and `S3`, so it's fully
  testable without a real object store); S3 bucket versioning (`docs/durability.md`)
  layers underneath as defense-in-depth.

  ## What a snapshot captures

  `create/2` copies the shard's **live stored object** — i.e. its last durably
  flushed state. A shard flushes when it goes idle (and on drain), and every flush
  writes a complete, checkpointed DB file, so a snapshot is always a consistent
  database, though it may lag writes still buffered on a live coordinator. To
  snapshot the very latest state, drain the shard (or let it idle-flush) first.

  ## Restore safety

  `restore/3` **drains the shard on this node first** (flush + stop the coordinator
  + release the lease) so nothing flushes the old bytes over the copy-back; the next
  request cold-opens the restored object. It refuses (`{:error, {:shard_busy, _}}`)
  if the shard can't be drained (active connections). In fathom's one-home-per-shard
  model, draining the home node is sufficient; if a tenant might be served elsewhere,
  quiesce it first. Restore is destructive to the current live object — take a fresh
  snapshot before restoring an older one if you might want to undo.
  """

  alias Fathom.Shard.Storage
  alias Fathom.ShardId
  alias Fathom.Shards

  @drain_timeout 30_000

  @doc """
  Snapshots `shard_id`'s current stored object. `opts[:label]` adds a
  human-readable suffix to the generated (timestamp-based) id. Returns
  `{:ok, snapshot_id}`.
  """
  @spec create(String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def create(shard_id, opts \\ []) do
    with {:ok, id} <- cast(shard_id) do
      snapshot_id = new_snapshot_id(Keyword.get(opts, :label))

      case Storage.snapshot(id, snapshot_id) do
        :ok -> {:ok, snapshot_id}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc "Lists `shard_id`'s stored snapshots (newest first)."
  @spec list(String.t()) :: {:ok, [Storage.snapshot()]} | {:error, term()}
  def list(shard_id) do
    with {:ok, id} <- cast(shard_id), do: Storage.list_snapshots(id)
  end

  @doc """
  Restores `shard_id` to `snapshot_id`. Drains any coordinator on this node first,
  then refuses unless no **live** node still owns the shard's lease — a cross-node
  safety check (`Storage.lease_holder/1`) so the copy-back can't be clobbered by a
  writer on another node, regardless of where restore is invoked. Returns
  `{:error, {:shard_busy, reason}}` if the local coordinator won't drain, or
  `{:error, {:held, owner}}` if a live node owns it. `opts[:drain_timeout]`
  overrides the drain wait (default #{@drain_timeout}ms).
  """
  @spec restore(String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def restore(shard_id, snapshot_id, opts \\ []) do
    with {:ok, id} <- cast(shard_id) do
      case Shards.drain(id, Keyword.get(opts, :drain_timeout, @drain_timeout)) do
        :ok ->
          case Storage.lease_holder(id) do
            :free -> Storage.restore_snapshot(id, snapshot_id)
            {:held, owner} -> {:error, {:held, owner}}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, {:shard_busy, reason}}
      end
    end
  end

  @doc "Deletes a stored snapshot (idempotent)."
  @spec drop(String.t(), String.t()) :: :ok | {:error, term()}
  def drop(shard_id, snapshot_id) do
    with {:ok, id} <- cast(shard_id), do: Storage.drop_snapshot(id, snapshot_id)
  end

  defp cast(shard_id) do
    case ShardId.cast(shard_id) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, :invalid_shard_id}
    end
  end

  # A sortable, filename-safe id: compact UTC timestamp + a short uniquifier (so two
  # snapshots in the same second don't collide) + an optional sanitized label.
  defp new_snapshot_id(label) do
    ts = DateTime.utc_now() |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    uniq = System.unique_integer([:positive, :monotonic]) |> Integer.to_string()
    base = "#{ts}-#{String.slice(uniq, -4, 4)}"

    case sanitize_label(label) do
      "" -> base
      lbl -> "#{base}-#{lbl}"
    end
  end

  defp sanitize_label(nil), do: ""

  defp sanitize_label(label) when is_binary(label) do
    label
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9-]+/, "-")
    |> String.trim("-")
    |> String.slice(0, 40)
  end
end
