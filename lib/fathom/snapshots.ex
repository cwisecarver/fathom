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
  alias Fathom.Shard.{SqliteHeader, Storage}
  alias Fathom.ShardId
  alias Fathom.Shards
  alias Fathom.Snapshots.Retention

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

  `opts[:auto]` marks the snapshot as SCHEDULER-CREATED, which is the only thing the automatic
  retention policy will ever delete. It is a separate flag rather than a label because a label is
  operator-supplied and could otherwise forge the marker — see `new_snapshot_id/2`.
  """
  @spec create(String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def create(shard_id, opts \\ []) do
    with {:ok, id} <- cast(shard_id) do
      snapshot_id =
        new_snapshot_id(Keyword.get(opts, :label), Keyword.get(opts, :auto, false) == true)

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
  `{:error, {:held, owner, stealable_at_ms | nil}}` if a live node owns it. `opts[:drain_timeout]`
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
         {:ok, snapshot_id} <- cast_snapshot_id(snapshot_id) do
      # ONE download for the whole restore (expert review 2026-08-26 #25). The snapshot is pulled
      # to `tmp`, its `PRAGMA user_version` is read from THOSE bytes, and THOSE bytes are what gets
      # promoted. Previously this pulled the snapshot to a temp, read four bytes at offset 60,
      # deleted the temp, and then `Storage.restore_snapshot/3` downloaded the identical key a
      # second time — so the version that GATED the restore was read from one download while a
      # different download's bytes became the live object. `tmp` is deleted on every path.
      #
      # Built from the CAST id, never the caller's raw string: this is a filesystem path, and
      # `cast/1` is what rules out a traversal component in it.
      tmp = restore_temp(id)

      try do
        do_restore(id, snapshot_id, opts, force?, tmp)
      after
        for suffix <- ["", "-wal", "-shm"], do: File.rm(tmp <> suffix)
      end
    end
  end

  defp do_restore(id, snapshot_id, opts, force?, tmp) do
    with {:ok, snap_version} <- snapshot_user_version(id, snapshot_id, tmp),
         :ok <- check_schema_boundary(id, snap_version, force?) do
      case Shards.drain(id, Keyword.get(opts, :drain_timeout, @drain_timeout)) do
        :ok ->
          # HOLD the lease across the whole restore, do not merely PROBE it (expert review
          # 2026-08-20 #12). This used to read `Storage.lease_holder(id)` and proceed on `:free`,
          # which leaves a window nothing closes:
          #
          #   1. the probe says :free and `object_etag/1` returns E;
          #   2. a request lands on ANOTHER node, which cold-opens, acquires the freed lease, pulls
          #      the object at E and starts serving — it has not flushed, so its `state.etag` is E;
          #   3. our If-Match: E copy-back SUCCEEDS; the object is now E' (the snapshot's bytes);
          #   4. that node's next flush PUTs If-Match: E → 412 → `reconcile_superseded/1` re-checks
          #      the LOCK, finds it still its own, resyncs the fence to E' and stays dirty;
          #   5. the interval after that PUTs its PRE-RESTORE bytes with If-Match: E' — which
          #      succeeds. The restore is silently undone.
          #
          # Every one of those steps is a path implemented deliberately and correctly on its own.
          # The If-Match fence closes the FIRST-order race (a flush between the etag read and the
          # copy-back aborts with :superseded) and is kept; holding the lease is what stops a new
          # owner from appearing at all. `Fathom.Tenants.fork_into_leased_dst/2` already does this
          # for the fork path — same shape, same reason.
          #
          # DISTINCT owner string, not a coordinator's: `acquire_existing` treats a same-owner
          # acquire as a silent RECLAIM rather than a fence, which is not a check at all (the
          # hazard `Promote.acquire/1` carries in its own moduledoc).
          owner = restore_owner()

          case Storage.acquire_lease(id, owner, restore_lease_ttl()) do
            {:ok, lease} ->
              try do
                case Storage.object_etag(id) do
                  {:ok, etag} ->
                    # The bytes at `tmp` are the bytes `snapshot_user_version/3` version-checked
                    # (#25) — same fence as `restore_snapshot/3`, one fewer full-object GET.
                    case Storage.restore_snapshot_from_file(id, tmp, etag) do
                      :ok ->
                        # Align the directory to the restored file's version so it never lies about
                        # the schema (#7); a below-head version lets the laggard sweep converge it
                        # forward. Inside the lease, so it lands before anyone can re-open.
                        reconcile_directory_schema(id, snap_version)
                        :ok

                      other ->
                        other
                    end

                  {:error, reason} ->
                    {:error, reason}
                end
              after
                Storage.release_lease(id, lease)
              end

            {:error, {:held, owner, at}} ->
              {:error, {:held, owner, at}}

            {:error, reason} ->
              {:error, reason}
          end

        {:error, reason} ->
          {:error, {:shard_busy, reason}}
      end
    end
  end

  # The schema version the snapshot's bytes carry (its file's PRAGMA user_version), read from the
  # pulled bytes at `tmp` — the version the live object will have after the copy-back. Guards the
  # cross-version restore and drives the post-restore directory reconcile (#7).
  #
  # `tmp` is the CALLER's and is deliberately NOT deleted here (#25): it is the same file
  # `restore/3` promotes, which is what makes the version that gated the restore and the bytes that
  # land on live one and the same. Deleting it here was the second download's whole cause.
  defp snapshot_user_version(id, snapshot_id, tmp) do
    case Storage.pull_snapshot(id, snapshot_id, tmp) do
      # `{:absent, _}` = no bytes written, incl. a steal sentinel (expert review
      # 2026-08-01 #24) — previously a sentinel read as a real snapshot of an empty db.
      {:absent, _} -> {:error, :snapshot_not_found}
      {:ok, _etag} -> read_user_version(tmp)
      {:error, reason} -> {:error, reason}
    end
  end

  defp restore_temp(shard_id) do
    Path.join(
      System.tmp_dir!(),
      "fathom_snaprestore_#{shard_id}_#{System.unique_integer([:positive])}.db"
    )
  end

  # Read user_version straight from the SQLite header, NOT by opening the file (expert review
  # 2026-08-31 #23) — opening it would run journal_mode=WAL on a VACUUM INTO snapshot and mutate the
  # very bytes `restore/3` then promotes. The header parse lives in `Fathom.Shard.SqliteHeader`,
  # shared with `Fathom.RestoreDrillJob` (self-review 2026-08-31 #2); kept as a public passthrough so
  # the snapshot-restore path is the entry point its no-mutation test asserts against.
  @doc false
  def read_user_version(path), do: SqliteHeader.user_version(path)

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

  # Never a coordinator's owner string — see the acquire above.
  defp restore_owner, do: "snapshot-restore@#{node()}@#{System.unique_integer([:positive])}"

  defp restore_lease_ttl, do: Application.get_env(:fathom, :shard_lease_ttl_ms, 30_000)

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
  # `auto?` is the SCHEDULER's provenance, and it is deliberately NOT a label (expert review
  # 2026-08-20 #14). `Retention.auto?/1` decides what the automatic policy may delete by matching
  # a trailing `-auto` on the id — and that id used to be built from a user-supplied label, so an
  # operator could produce the reserved marker three ways:
  #
  #     "auto" / "AUTO"                                        -> "auto"
  #     "pre-migration auto"                                   -> "pre-migration-auto"
  #     "manual snapshot taken by ops before automatic cleanup" -> "...-before-auto"
  #
  # The third is the dangerous one: the sanitized string is 52 characters and the `slice(0, 40)`
  # lands exactly on `-before-auto`, so the TRUNCATION CREATES the marker out of a label whose
  # meaning was the opposite. `docs/durability.md` states the property as absolute — "an
  # operator's deliberate create is invisible to the automatic policy" — and the snapshot someone
  # took *because they were worried* was the one retention deleted, silently.
  #
  # The suffix FORMAT is unchanged, because snapshots already in storage carry it and
  # `Retention.auto?/1` must keep matching them. What changed is that only this flag can produce it.
  defp new_snapshot_id(label, auto?) do
    ts = DateTime.utc_now() |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    uniq = System.unique_integer([:positive, :monotonic]) |> Integer.to_string()
    base = "#{ts}-#{String.slice(uniq, -4, 4)}"

    suffix =
      case {sanitize_label(label), auto?} do
        {"", true} -> "-" <> Retention.auto_label()
        {"", false} -> ""
        {lbl, true} -> "-#{lbl}-" <> Retention.auto_label()
        {lbl, false} -> "-#{lbl}"
      end

    base <> suffix
  end

  defp sanitize_label(nil), do: ""

  defp sanitize_label(label) when is_binary(label) do
    label
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9-]+/, "-")
    |> String.trim("-")
    |> String.slice(0, 40)
    |> String.trim("-")
    |> strip_reserved_suffix()
  end

  # Drop a trailing reserved marker a user label produced — including one the 40-char slice
  # created. Repeated, because "...-auto-auto" truncates to "...-auto" just as readily. Dropping
  # the word is lossless in the way that matters: it removes a token that would otherwise assert
  # provenance the snapshot does not have, and the rest of the operator's label survives.
  defp strip_reserved_suffix(lbl) do
    marker = Retention.auto_label()

    cond do
      lbl == marker ->
        ""

      String.ends_with?(lbl, "-" <> marker) ->
        lbl |> String.replace_suffix("-" <> marker, "") |> strip_reserved_suffix()

      true ->
        lbl
    end
  end
end
