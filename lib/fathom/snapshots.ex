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

  alias Fathom.Directory
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Storage
  alias Fathom.ShardId
  alias Fathom.Shards

  @drain_timeout 30_000

  # The path-traversal / key-escape gate for the caller-supplied `snapshot_id`, the
  # sibling of `ShardId` for the `@snap-<snapshot_id>` key segment (expert review
  # 2026-07-19 #1). `restore/3` and `drop/2` take an operator-supplied id that flows
  # straight into `Path.join(dir(), "<shard>@snap-<snapshot_id>.db")` (Local) and the
  # S3 object key — so a `/` (or `..`) would escape the shard's key prefix and turn the
  # control plane into an arbitrary-file read/write/delete primitive. Same conservative
  # charset as `ShardId`: no dot (blocks `..`), no slash, no whitespace/control chars.
  # Case is preserved (unlike shard ids) — a generated id carries uppercase `T`/`Z` from
  # its UTC timestamp. 128 chars covers a timestamp + uniquifier + a 40-char label.
  @snapshot_id_pattern ~r/^[a-zA-Z0-9_-]{1,128}$/

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
  then refuses unless no **live** node still owns the shard's lease
  (`Storage.lease_holder/1`). The copy-back itself is **etag-fenced** (If-Match the
  live object, captured at the lease-free instant), so a write that races in after
  the drain — a fresh checkout that acquired the freed lease and flushed, on this or
  any node — aborts the restore with `{:error, :superseded}` instead of being
  silently clobbered (expert review 2026-07-18 #2). Returns
  `{:error, {:shard_busy, reason}}` if the local coordinator won't drain, or
  `{:error, {:held, owner}}` if a live node owns it. `opts[:drain_timeout]`
  overrides the drain wait (default #{@drain_timeout}ms).

  **Schema-version guard (expert review #7).** A snapshot carries the schema version its bytes were
  captured at (its file's `PRAGMA user_version`). Restoring one whose version differs from the
  directory's current `schema_version` — e.g. a `vN-1` snapshot after the fleet cut to `vN` — would
  leave the directory claiming a version the file no longer has: the laggard sweep (`schema_version <
  head`) then believes the shard is migrated and never converges it, and `vN`-expecting app code reads
  a `vN-1` schema. So a cross-version restore is **refused** with
  `{:error, {:schema_version_mismatch, %{snapshot: v, directory: v}}}` unless `opts[:force]` is true;
  on `force` (or a same-version restore) the directory's `schema_version` is reconciled to the restored
  version afterward, so the laggard sweep re-migrates it forward to head.
  """
  @spec restore(String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def restore(shard_id, snapshot_id, opts \\ []) do
    force? = Keyword.get(opts, :force, false)

    with {:ok, id} <- cast(shard_id),
         {:ok, snapshot_id} <- cast_snapshot_id(snapshot_id),
         {:ok, snap_version} <- snapshot_user_version(id, snapshot_id),
         :ok <- check_schema_boundary(id, snap_version, force?) do
      case Shards.drain(id, Keyword.get(opts, :drain_timeout, @drain_timeout)) do
        :ok ->
          case Storage.lease_holder(id) do
            :free ->
              # Capture the live etag at the lease-free instant and restore under an If-Match fence
              # (expert review 2026-07-18 #2): a fresh checkout that acquires the freed lease and
              # flushes between here and the copy-back moves the etag, so the restore aborts with
              # {:error, :superseded} instead of silently clobbering those acked writes.
              case Storage.object_etag(id) do
                {:ok, etag} ->
                  case Storage.restore_snapshot(id, snapshot_id, etag) do
                    :ok ->
                      # Align the directory to the restored file's version so it never lies about the
                      # schema (#7); a below-head version lets the laggard sweep converge it forward.
                      reconcile_directory_schema(id, snap_version)
                      :ok

                    other ->
                      other
                  end

                {:error, reason} ->
                  {:error, reason}
              end

            {:held, owner} ->
              {:error, {:held, owner}}

            {:error, reason} ->
              {:error, reason}
          end

        {:error, reason} ->
          {:error, {:shard_busy, reason}}
      end
    end
  end

  # The schema version the snapshot's bytes carry (its file's PRAGMA user_version), read by pulling
  # the snapshot to a temp — the version the live object will have after the copy-back. Guards the
  # cross-version restore and drives the post-restore directory reconcile (#7).
  defp snapshot_user_version(id, snapshot_id) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "fathom_snaprestore_#{id}_#{System.unique_integer([:positive])}.db"
      )

    try do
      case Storage.pull_snapshot(id, snapshot_id, tmp) do
        {:ok, nil} -> {:error, :snapshot_not_found}
        {:ok, _etag} -> read_user_version(tmp)
        {:error, reason} -> {:error, reason}
      end
    after
      for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    end
  end

  defp read_user_version(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        result =
          case Connection.query(conn, "PRAGMA user_version", []) do
            {:ok, %{rows: [[v]]}} when is_integer(v) -> {:ok, v}
            other -> {:error, {:user_version_unreadable, other}}
          end

        Connection.close(conn)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Refuse a restore that would cross a schema-migration boundary unless force: true (#7). With no
  # directory row there is nothing to skew, so allow it. Fails OPEN on a directory (Postgres) error —
  # a control-plane read must never break the restore (the data path never hard-depends on Postgres);
  # any skew a restore-during-outage leaves is caught by `mix fathom.directory reconcile` (#6).
  defp check_schema_boundary(id, snap_version, force?) do
    case directory_schema_version(id) do
      {:ok, dir_version} when dir_version != snap_version and not force? ->
        {:error, {:schema_version_mismatch, %{snapshot: snap_version, directory: dir_version}}}

      _ ->
        :ok
    end
  end

  defp reconcile_directory_schema(id, snap_version) do
    case directory_schema_version(id) do
      {:ok, v} when v != snap_version ->
        try do
          Directory.reconcile_schema_version(id, snap_version)
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end

      _ ->
        :ok
    end
  end

  # The directory's schema_version for `id`, or `:none` (no row, or a Postgres error — fail open).
  defp directory_schema_version(id) do
    case Directory.get(id) do
      {:ok, %{schema_version: v}} -> {:ok, v}
      _ -> :none
    end
  rescue
    _ -> :none
  catch
    :exit, _ -> :none
  end

  @doc "Deletes a stored snapshot (idempotent)."
  @spec drop(String.t(), String.t()) :: :ok | {:error, term()}
  def drop(shard_id, snapshot_id) do
    with {:ok, id} <- cast(shard_id),
         {:ok, snapshot_id} <- cast_snapshot_id(snapshot_id),
         do: Storage.drop_snapshot(id, snapshot_id)
  end

  defp cast(shard_id) do
    case ShardId.cast(shard_id) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, :invalid_shard_id}
    end
  end

  # The `snapshot_id` gate (see @snapshot_id_pattern). Untrusted, so non-binaries and
  # any traversal/escape char are rejected before the id reaches storage-key construction.
  defp cast_snapshot_id(snapshot_id) when is_binary(snapshot_id) do
    if snapshot_id =~ @snapshot_id_pattern,
      do: {:ok, snapshot_id},
      else: {:error, :invalid_snapshot_id}
  end

  defp cast_snapshot_id(_), do: {:error, :invalid_snapshot_id}

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
