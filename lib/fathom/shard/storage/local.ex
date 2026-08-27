defmodule Fathom.Shard.Storage.Local do
  @moduledoc """
  Filesystem-backed shard storage: a local directory stands in for the object
  store. The default backend for dev and tests — it exercises the full
  pull-on-wake / flush-on-idle path without needing S3 credentials.

  Configure the "remote" directory (defaults to a temp dir):

      config :fathom, Fathom.Shard.Storage.Local, dir: "/var/fathom/remote"
  """
  @behaviour Fathom.Shard.Storage

  require Logger

  alias Fathom.Shard.Storage

  @impl true
  def pull(shard_id, local_path) do
    # STREAMS rather than reading the object into a binary (expert review 2026-08-26 #26). This is
    # the mass-warming path, so the old shape held one shard-sized binary per concurrent pull on
    # the binary heap — the same defect review 2026-07-24 #25 fixed in `pull_if_changed/3` two
    # functions below, whose comment states the reasoning. Byte-identical etag (`file_etag/1` is
    # the same SHA-256 over the same bytes) and the identical fsync-before-rename contract.
    case file_etag(remote_path(shard_id)) do
      {:ok, etag} ->
        # Atomic (temp + rename) so a concurrent reader never sees a half-copied file (#24).
        # Return the content-hash etag so the coordinator can fence its first flush (#15).
        case Storage.atomic_copy(remote_path(shard_id), local_path) do
          :ok -> {:ok, etag}
          err -> err
        end

      # Brand-new shard — no object, nothing written, no etag yet. `{:absent, nil}` rather
      # than `{:ok, nil}` so the "were bytes written" answer is explicit for every caller
      # (expert review 2026-08-01 #24); see the `pull/2` callback doc.
      {:error, :enoent} ->
        {:absent, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def flush(shard_id, local_path) do
    # Atomic (temp + rename) so a crash mid-copy can't leave a torn "durable" object (#28).
    #
    # Clears the position stamp, because a PUT on S3 replaces ALL user metadata and an unstamped
    # object is what this unfenced path produces there. Leaving the old stamp here would be a
    # backend disagreement in the dangerous direction: it would describe bytes that are no longer
    # stored — a migration copy or a benchmark write — and a replica past that position would be
    # promoted over them.
    with :ok <- Storage.atomic_copy(local_path, remote_path(shard_id)) do
      rm_ok(position_path(shard_id))
    end
  end

  @impl true
  def flush(shard_id, local_path, expected_etag, position \\ nil, lineage \\ nil) do
    # Model S3's If-Match / If-None-Match:* fence with the content-hash etag: only write if the
    # remote's current etag still matches what the coordinator last saw (or, for a brand-new
    # shard, only if no object exists). A mismatch means a stealer flushed in the window since
    # the fence check → superseded, don't clobber (finding #15).
    #
    # Runs UNDER the per-shard mutex (expert review #28): the read-compare-write must be
    # atomic to faithfully double S3's server-side conditional PUT. Without it two concurrent
    # fenced flushes both read the same `current`, both pass the compare, and both write
    # (last-write-wins) — a split-brain the fence exists to prevent, invisible to any test
    # driven through this backend. #38 mutexed the LOCK ops but not this DATA flush.
    with_lock_mutex(shard_id, fn ->
      current =
        case file_etag(remote_path(shard_id)) do
          {:ok, etag} -> etag
          {:error, :enoent} -> nil
          {:error, reason} -> throw({:error, reason})
        end

      cond do
        expected_etag != current ->
          {:error, :superseded}

        true ->
          # atomic_copy streams (File.cp) and carries the same fsync-before-rename crash contract
          # as atomic_write, so the durability shape is unchanged — it just never materializes the
          # shard as a binary.
          #
          # Hash the DESTINATION, not the source (expert review 2026-08-01 #44). The returned etag
          # is what the coordinator fences its NEXT flush with, so it has to describe the object
          # that is now stored. Hashing `local_path` after the copy describes a file that can have
          # changed since — the two are only guaranteed identical when the source is quiescent.
          #
          # The window is real where the flush source is the LIVE database rather than a VACUUM
          # temp (`upload_for_drop/1`). The rolling-deploy path terminates the Edge plane before
          # the DataPlane, which is load-bearing and is what kept this Low — but `Shards.stop/1`'s
          # force-stop terminates a coordinator while its streams are live, and that path has no
          # such ordering. A fence etag that does not describe the stored object makes the next
          # flush 412 and the shard self-fence away acknowledged writes.
          with :ok <- Storage.atomic_copy(local_path, remote_path(shard_id)),
               :ok <- write_position(shard_id, position),
               :ok <- write_lineage(shard_id, lineage),
               {:ok, etag} <- file_etag(remote_path(shard_id)) do
            {:ok, etag}
          end
      end
    end)
  catch
    {:error, _} = err -> err
  end

  # The position stamp S3 carries as object metadata. A companion file here, written AFTER the
  # object so a crash between the two leaves a stamp-less object rather than a stamp describing
  # bytes that were never stored — the first reads as "unknown" (never overridable), the second
  # would be a lie a replica could act on.
  #
  # A flush with no position REMOVES any previous stamp rather than leaving it: a stale stamp
  # describing older bytes is exactly the over-claim-in-reverse that loses writes.
  defp write_position(shard_id, nil), do: rm_ok(position_path(shard_id))

  defp write_position(shard_id, position) do
    Storage.atomic_write(position_path(shard_id), Storage.encode_position(position))
  end

  # UNLIKE the position, a `nil` lineage LEAVES any previous value in place. The two are not
  # symmetric: a stale POSITION describes bytes that moved and would lose writes, while the lineage
  # describes ownership history and must only ever go up. A caller with no lineage to claim (a
  # migration copy, a benchmark) has no business erasing the shard's history — and erasing it would
  # reintroduce exactly the reset this key exists to prevent.
  defp write_lineage(_shard_id, nil), do: :ok

  defp write_lineage(shard_id, lineage) when is_integer(lineage) do
    Storage.atomic_write(lineage_path(shard_id), Integer.to_string(lineage))
  end

  # Not gated on the object existing, unlike `object_position/1`. A lineage outliving its object is
  # not a lie about bytes — it is a true statement about how many owners this shard has had, and
  # keeping it is what stops a re-created shard reusing numbers a replica still remembers.
  def object_lineage(shard_id) do
    case File.read(lineage_path(shard_id)) do
      {:ok, raw} ->
        case Integer.parse(String.trim(raw)) do
          {n, ""} when n >= 0 -> {:ok, n}
          _ -> {:ok, nil}
        end

      {:error, :enoent} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def object_position(shard_id) do
    # Gated on the OBJECT existing, not the stamp: a leftover stamp beside a deleted object would
    # otherwise describe bytes that are gone.
    if File.exists?(remote_path(shard_id)) do
      case File.read(position_path(shard_id)) do
        {:ok, raw} -> {:ok, Storage.parse_position(String.trim(raw))}
        {:error, :enoent} -> {:ok, nil}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, nil}
    end
  end

  defp rm_ok(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      other -> other
    end
  end

  @impl true
  def object_etag(shard_id) do
    # The sharpest instance of #26: this read the WHOLE object into a binary purely to hash it,
    # two functions above a streaming hasher that returns the same value. It sits on the hot
    # resolution paths (six call sites in shard.ex), so the binary was allocated and discarded on
    # every ownership re-check.
    case file_etag(remote_path(shard_id)) do
      {:ok, etag} -> {:ok, etag}
      {:error, :enoent} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  # NOT atomic here, and worth saying plainly rather than letting a reader assume the double
  # matches S3: the etag is a hash of the object file and the stamp is a companion file, so this is
  # two reads where S3 is one HEAD. A writer landing between them yields a MIXED head.
  #
  # That is the SAFE direction and cannot hide a bug. The recovery path uses this head to answer
  # "is the object still the one I compared against", and a mixed head can only ever disagree with
  # the earlier read — declining a promotion that might have been fine. It can never agree
  # falsely, which is the answer that would matter. The single-writer lease makes the window
  # unreachable in practice anyway; this is a dev/test backend and the note is for whoever ports
  # the next one.
  @impl true
  def object_head(shard_id) do
    with {:ok, etag} when not is_nil(etag) <- object_etag(shard_id),
         {:ok, position} <- object_position(shard_id),
         {:ok, lineage} <- object_lineage(shard_id) do
      {:ok, %{etag: etag, position: position, lineage: lineage}}
    else
      {:ok, nil} -> {:ok, nil}
      {:error, _} = error -> error
    end
  end

  # A content hash stands in for S3's opaque etag: it changes iff the bytes change,
  # which is exactly the freshness signal `pull_if_changed/3` needs. (S3's real 304
  # skips the body; the Local double reads it to hash, which is fine on local disk.)
  @impl true
  def pull_if_changed(shard_id, local_path, etag) do
    # The warm-follower revalidation loop calls this for every cached shard on every poll, and it
    # used to read AND fully hash the entire object just to conclude nothing changed — O(cached ×
    # shard bytes) of disk read and hashing per poll, where the S3 backend does a bodiless 304
    # (expert review 2026-07-24 #25).
    case file_etag(remote_path(shard_id)) do
      {:ok, current} when current == etag ->
        {:ok, :unchanged}

      {:ok, current} ->
        # Atomic so a warm-cache rewrite can't be read half-old/half-new by a promotion (#24).
        case Storage.atomic_copy(remote_path(shard_id), local_path) do
          :ok -> {:ok, {:written, current}}
          err -> err
        end

      {:error, :enoent} ->
        {:ok, :absent}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # THE ONLY object hasher in this module, and the digest every fence in it compares.
  #
  # It computes SHA-256 by STREAMING the file rather than reading it whole into a binary (expert
  # review 2026-07-24 #25). It had a whole-file twin, `content_etag/1`, used by five call sites
  # that #25 did not reach; expert review 2026-08-26 #26 converted those and the twin is gone, so
  # there is no longer a cheaper-looking wrong choice to reach for. SHARD_STORAGE=local is a supported production selection and
  # local-NVMe is a stated deployment, so a fenced flush used to hold two shard-sized binaries at
  # once (the local body and the remote body) plus two full hash passes — `2 * N * shard_bytes` of
  # transient binary heap at N concurrent flushes. Byte-identical output, so the emulated If-Match
  # fence is unchanged; this is the shape S3.stat_and_md5/1 already uses.
  @hash_chunk 256 * 1024

  defp file_etag(path) do
    path
    |> File.stream!(@hash_chunk)
    |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
    |> then(&{:ok, &1})
  rescue
    e in File.Error -> {:error, e.reason}
  end

  # --- versioned copies (blue/green migration) ---

  @impl true
  def retain(shard_id, version) do
    # Atomic (temp + rename) so a retained version copy is never left torn (#28).
    Storage.atomic_copy(remote_path(shard_id), version_path(shard_id, version))
  end

  @impl true
  def restore(shard_id, version) do
    Storage.atomic_copy(version_path(shard_id, version), remote_path(shard_id))
  end

  @impl true
  def restore(shard_id, version, expected_etag) do
    # Fenced restore (expert review 2026-07-14 #4): mirror flush/3's emulated conditional write
    # so the revert's copy-back only lands if live still matches the etag the migrator captured
    # at pull. A steal since the read-only fence moves live's content-hash etag → :superseded, and
    # the revert aborts instead of clobbering the newer owner's object. Under the per-shard mutex
    # so the read-compare-write is atomic — the same faithful double of S3's conditional PUT the
    # flush fence relies on (expert review #28).
    with_lock_mutex(shard_id, fn ->
      # Streams both sides (#26): the old shape held the source body AND the live body as
      # binaries simultaneously, inside the per-shard mutex. `File.stat/1` first so a missing
      # source still returns {:error, :enoent} BEFORE the etag comparison, exactly as the
      # `File.read` it replaces did — otherwise a missing source with a stale etag would report
      # :superseded, which is a different and more alarming answer.
      with {:ok, _} <- File.stat(version_path(shard_id, version)) do
        current =
          case file_etag(remote_path(shard_id)) do
            {:ok, etag} -> etag
            {:error, :enoent} -> nil
            {:error, reason} -> throw({:error, reason})
          end

        cond do
          expected_etag != current -> {:error, :superseded}
          true -> Storage.atomic_copy(version_path(shard_id, version), remote_path(shard_id))
        end
      end
    end)
  catch
    {:error, _} = err -> err
  end

  @impl true
  def drop_version(shard_id, version) do
    case File.rm(version_path(shard_id, version)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def fork_from(template_id, version, dst_shard_id) do
    # Fork-from-template (finding #10): retained snapshot -> the new tenant's live
    # object. Atomic (temp + rename) like retain/restore, so a torn copy is never
    # adopted. A missing snapshot is {:error, :enoent} (File.cp through atomic_copy).
    Storage.atomic_copy(version_path(template_id, version), remote_path(dst_shard_id))
  end

  @impl true
  def drop_live(shard_id) do
    # The stamp goes with the object. `object_position/1` already gates on the object existing, so
    # a dangling stamp could not be acted on — but leaving residue that only one reader knows to
    # ignore is how the next reader gets it wrong.
    _ = rm_ok(position_path(shard_id))

    case File.rm(remote_path(shard_id)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def fork_shard(src_id, dst_id) do
    # Fork a live shard to a new id (#14): guard both ends, then an atomic (temp+rename) copy so a
    # torn fork is never adopted. Refuse if the dst already exists (no clobber) or the src doesn't.
    # The exists-check + copy run UNDER the dst mutex (expert review 2026-07-18 #14) so they are
    # atomic — two concurrent forks (or a fork racing an organic first flush, which also takes this
    # mutex) can't both pass File.exists? and then clobber; the loser sees the object → :dst_exists.
    # This is the storage-layer double of the caller's dst-lease guard (Fathom.Tenants.fork).
    with_lock_mutex(dst_id, fn ->
      cond do
        File.exists?(remote_path(dst_id)) -> {:error, :dst_exists}
        not File.exists?(remote_path(src_id)) -> {:error, :no_source}
        true -> Storage.atomic_copy(remote_path(src_id), remote_path(dst_id))
      end
    end)
  end

  # --- point-in-time snapshots (#12) ---

  @impl true
  def snapshot(shard_id, snapshot_id) do
    # Atomic (temp + rename), like retain/1 — a snapshot copy is never left torn.
    Storage.atomic_copy(remote_path(shard_id), snapshot_path(shard_id, snapshot_id))
  end

  @impl true
  def restore_snapshot(shard_id, snapshot_id) do
    Storage.atomic_copy(snapshot_path(shard_id, snapshot_id), remote_path(shard_id))
  end

  @impl true
  def restore_snapshot(shard_id, snapshot_id, expected_etag) do
    # Fenced snapshot restore (expert review 2026-07-18 #2): the snapshot counterpart of the
    # fenced migration restore/3. Under the per-shard mutex so the read-compare-write is atomic —
    # if live's content-hash etag no longer matches what the caller captured (a write raced in
    # after the drain), abort with :superseded instead of clobbering it.
    with_lock_mutex(shard_id, fn ->
      # Streams both sides (#26): the old shape held the source body AND the live body as
      # binaries simultaneously, inside the per-shard mutex. `File.stat/1` first so a missing
      # source still returns {:error, :enoent} BEFORE the etag comparison, exactly as the
      # `File.read` it replaces did — otherwise a missing source with a stale etag would report
      # :superseded, which is a different and more alarming answer.
      with {:ok, _} <- File.stat(snapshot_path(shard_id, snapshot_id)) do
        current =
          case file_etag(remote_path(shard_id)) do
            {:ok, etag} -> etag
            {:error, :enoent} -> nil
            {:error, reason} -> throw({:error, reason})
          end

        cond do
          expected_etag != current -> {:error, :superseded}
          true -> Storage.atomic_copy(snapshot_path(shard_id, snapshot_id), remote_path(shard_id))
        end
      end
    end)
  catch
    {:error, _} = err -> err
  end

  @impl true
  def pull_snapshot(shard_id, snapshot_id, local_path) do
    # Streams (#26). The restore drill pulls every snapshot of every sampled shard, so this was up
    # to ~35 serial full-shard binaries per shard per run.
    case file_etag(snapshot_path(shard_id, snapshot_id)) do
      {:ok, etag} ->
        case Storage.atomic_copy(snapshot_path(shard_id, snapshot_id), local_path) do
          :ok -> {:ok, etag}
          err -> err
        end

      # `{:absent, nil}`, matching pull/2 (expert review 2026-08-01 #24).
      {:error, :enoent} ->
        {:absent, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def drop_snapshot(shard_id, snapshot_id) do
    case File.rm(snapshot_path(shard_id, snapshot_id)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def list_snapshots(shard_id) do
    prefix = "#{shard_id}@snap-"

    case File.ls(dir()) do
      {:ok, names} ->
        snaps =
          names
          |> Enum.filter(&(String.starts_with?(&1, prefix) and String.ends_with?(&1, ".db")))
          |> Enum.map(fn name ->
            id = name |> String.replace_prefix(prefix, "") |> String.replace_suffix(".db", "")

            bytes =
              case File.stat(Path.join(dir(), name)) do
                {:ok, %File.Stat{size: size}} -> size
                _ -> 0
              end

            %{id: id, bytes: bytes}
          end)
          |> Enum.sort_by(& &1.id, :desc)

        {:ok, snaps}

      {:error, :enoent} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def purge_shard(shard_id) do
    # Full tenant erasure (#15): sweep the store dir for every object belonging to
    # this id and delete each. Idempotent — a missing dir or object is `:ok`.
    case File.ls(dir()) do
      {:ok, names} ->
        names
        |> Enum.filter(&shard_object?(&1, shard_id))
        |> Enum.reduce_while(:ok, fn name, :ok ->
          case File.rm(Path.join(dir(), name)) do
            :ok -> {:cont, :ok}
            {:error, :enoent} -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def put_tombstone(shard_id) do
    # A durable tombstone marker under a `tombstones/` subdir — a distinct namespace `purge_shard`
    # never lists (it scans dir()'s top level, and `tombstones` there is not a shard object), so it
    # outlives full erasure and a Postgres directory restore (#6). Shard ids are filename-safe
    # (`[a-zA-Z0-9_-]`, ShardId), so the id IS the filename; the body is empty (the key is the fact).
    path = Path.join([dir(), "tombstones", shard_id])
    File.mkdir_p!(Path.dirname(path))
    File.write(path, "")
  end

  @impl true
  def tombstoned_ids do
    case File.ls(Path.join(dir(), "tombstones")) do
      {:ok, names} -> {:ok, names}
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def put_token_floor(shard_id, version) do
    path = Path.join([dir(), "tokenfloors", shard_id])
    File.mkdir_p!(Path.dirname(path))
    File.write(path, Integer.to_string(version))
  end

  @impl true
  def read_token_floor(shard_id) do
    case File.read(Path.join([dir(), "tokenfloors", shard_id])) do
      {:ok, body} -> {:ok, parse_token_floor(body)}
      {:error, :enoent} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  # Only ever fed `File.read/1`'s `{:ok, body}`, so `body` is always a binary — a
  # `parse_token_floor(_), do: nil` catch-all here was unreachable and is gone. The guard stays
  # because it documents the input; the `_ -> nil` INSIDE handles unparseable content, which is the
  # case that actually happens.
  defp parse_token_floor(body) when is_binary(body) do
    case Integer.parse(String.trim(body)) do
      {n, _} when n >= 0 -> n
      _ -> nil
    end
  end

  # A stored file belongs to `shard_id` iff the character AFTER the id is `.` (the
  # live `.db`, the `.lock`, an atomic-write `.db.tmp…` temp) or `@` (a `@<version>`
  # / `@snap-<id>` copy). Matching the delimiter — not a bare prefix — is what keeps
  # purging `acme` from ever deleting `acme2.db`.
  defp shard_object?(name, shard_id) do
    case String.split_at(name, String.length(shard_id)) do
      {^shard_id, "." <> _} -> true
      {^shard_id, "@" <> _} -> true
      _ -> false
    end
  end

  @impl true
  def stored_usage do
    dir()
    |> Path.join("*.db")
    |> Path.wildcard()
    # Count only live objects (`<shard>.db`), excluding retained `<shard>@<version>.db` copies.
    |> Enum.reject(&String.contains?(Path.basename(&1), "@"))
    |> Enum.reduce({0, 0}, fn path, {count, bytes} ->
      case File.stat(path) do
        {:ok, %File.Stat{size: size}} -> {count + 1, bytes + size}
        _ -> {count, bytes}
      end
    end)
  end

  # --- leasing ---
  #
  # The lease is a `<shard_id>.lock` JSON file next to the `.db` object. A fresh
  # lease is created with `O_EXCL` so concurrent first-creators can't both win.
  # Steal/reclaim/renew/release are read-modify-write, made ATOMIC by a per-shard
  # node-local critical section (expert review #38): this backend's domain is one
  # machine, so serializing in-VM gives it the same exactly-one-winner semantics
  # S3's conditional writes give the production fence — and lets the contention
  # paths (two concurrent stealers) be tested against the behaviour, not just
  # request shapes.

  @impl true
  def acquire_lease(shard_id, owner, ttl_ms) do
    with_lock_mutex(shard_id, fn -> do_acquire_lease(shard_id, owner, ttl_ms) end)
  end

  defp do_acquire_lease(shard_id, owner, ttl_ms) do
    now = Storage.now_ms()

    case read_lock(shard_id) do
      # Our own lease (live or stale) — reclaim it, keeping the epoch.
      {:ok, %{owner: ^owner, epoch: epoch}} ->
        write_lock(shard_id, %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms})

      # Someone else holds it — liveness is *their heartbeat* (with the lock's own TTL as the
      # fallback when they run no heartbeat — see owner_live?/3).
      {:ok, %{owner: other, epoch: epoch, expires_at_ms: lock_exp}} ->
        case owner_live?(shard_id, other, now, lock_exp) do
          :live ->
            {:error, {:held, other}}

          :dead ->
            # took_over: the caller revalidates its speculative pull (expert review
            # #3). Local has no steal-time etag touch — content-hash etags can't
            # change without the bytes changing, and this backend is single-node
            # (the in-VM Registry already serializes coordinators), so the S3 zombie
            # scenario can't arise here.
            case write_lock(shard_id, %{
                   owner: owner,
                   epoch: epoch + 1,
                   expires_at_ms: now + ttl_ms
                 }) do
              {:ok, lease} -> {:ok, Map.put(lease, :took_over, true)}
              other -> other
            end

          # Fail closed: don't steal a possibly-live owner on a heartbeat read blip.
          {:error, reason} ->
            {:error, {:transient_lookup, reason}}
        end

      :enoent ->
        create_lock(shard_id, %{owner: owner, epoch: 1, expires_at_ms: now + ttl_ms})

      # A CORRUPT LOCK IS NOT A FENCE, SO IT MUST NOT BE ONE (expert review 2026-08-20 #31).
      #
      # `:corrupt_lock` had two producers (here and in the S3 backend) and NOT ONE consumer: it
      # propagated out of `acquire_lease` → `handle_continue` →
      # `{:stop, {:shutdown, {:lease_unavailable, :corrupt_lock}}}` → `{:error, _}` at checkout,
      # and the tenant was down permanently with no operator remedy short of deleting a file by
      # hand. The likeliest way to get one was `create_lock/2` above silently succeeding over a
      # zero-length file on a full disk.
      #
      # An undecodable lock carries no owner and no epoch, so it CANNOT be fencing anything —
      # nobody can be holding a lease it describes, and no `check_lease`/`renew_lease` can match
      # it. Treating it as absent is therefore safe in the direction that matters, and it is the
      # only reading that lets the tenant come back.
      #
      # Deliberately confined to ACQUIRE. `check_lease/2` and `do_renew_lease/3` still fail closed
      # on it: those run while a lease is believed HELD, and repairing under a holder would be a
      # silent takeover rather than a recovery. Loud, because a corrupt lock means a disk problem
      # that will produce others.
      {:error, :corrupt_lock} ->
        Logger.error(
          "shard #{shard_id}: lock file is undecodable (a full or failing disk is the usual " <>
            "cause). It names no owner and no epoch, so it fences nothing — recreating it. " <>
            "Check the volume backing #{lock_path(shard_id)}."
        )

        :telemetry.execute([:fathom, :storage, :lock_repaired], %{count: 1}, %{shard_id: shard_id})

        case File.rm(lock_path(shard_id)) do
          ok when ok == :ok ->
            create_lock(shard_id, %{owner: owner, epoch: 1, expires_at_ms: now + ttl_ms})

          {:error, :enoent} ->
            create_lock(shard_id, %{owner: owner, epoch: 1, expires_at_ms: now + ttl_ms})

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Is `other` still alive? Its heartbeat object is the primary source of truth. Goes
  # through the Storage dispatch (resolves to this backend in prod) so a faulty
  # backend can inject a heartbeat-read failure for the fail-closed test. The
  # steal_margin absorbs inter-node clock skew (see Storage.steal_margin_ms/0): only
  # steal once the heartbeat is expired by more than the margin.
  defp owner_live?(shard_id, other, now, lock_expires_at_ms) do
    case stealable_at(shard_id, other, lock_expires_at_ms) do
      {:ok, at} -> if now <= at, do: :live, else: :dead
      {:error, _} = error -> error
    end
  end

  # The instant `other`'s hold becomes stealable. THE single source of the liveness rule — both
  # `owner_live?/3` (is now past it) and `lease_stealable_at/1` (how long until it) derive from
  # this, so a caller predicting a steal can never disagree with the code performing it.
  #
  # That divergence was a real bug: `Shards.holder_stealable_soon?/2` predicted from the heartbeat
  # ALONE while `acquire_lease` required both signals lapsed (#12), so a checkout could hold and
  # retry its whole crash-failover budget waiting for a steal that could not happen yet.
  defp stealable_at(shard_id, other, lock_expires_at_ms) do
    margin = Storage.steal_margin_ms()

    case Storage.read_heartbeat(other) do
      # Owner-match verification (expert review round-2 #3), mirroring the S3 backend.
      #
      # `max/2` is #12: a lapsed heartbeat alone does NOT make the owner dead, its lock TTL still
      # counts. Taking only the heartbeat here was exactly backwards from the `:not_found` branch
      # below, which DOES fall back to the lock TTL — a stale-but-present heartbeat was strictly
      # WORSE than an absent one. Fixed in S3 (`s3.ex`) and missed here until #30's legacy-mode
      # test found it; `SHARD_STORAGE=local` is a supported production selection.
      {:ok, %{owner: ^other, expires_at_ms: exp}} ->
        {:ok, max(exp, lock_expires_at_ms) + margin}

      # A heartbeat that isn't `other`'s is no signal — fall back to the lock, same as :not_found.
      {:ok, _mismatch} ->
        {:ok, lock_expires_at_ms + margin}

      # No heartbeat object at all (`heartbeat_server: false` legacy mode, or the owner's
      # heartbeat was cleared): fall back to the lock's OWN TTL for liveness (finding #11), so
      # a live owner renewing its lock per-shard isn't instantly stolen. Heartbeat stays primary.
      :not_found ->
        # This node's PROVEN-DEAD previous incarnation (round-2 #34: heartbeat verified
        # stale/frozen and cleared — see Fathom.Shard.Heartbeat): its recently-renewed locks
        # would otherwise block the restarted node for TTL+margin. Exact owner match only.
        # Stealable at 0 ⇒ stealable now, and `soon?` reads it as such.
        # NOW ALSO REQUIRES THE RENEWAL PROBE (expert review 2026-08-20 #10). A heartbeat proof
        # says the heartbeat OBJECT stopped; it does not say the process did. See
        # `Storage.fast_steal_ok?/4` for why the finding's own condition was rejected.
        if Storage.fast_steal_ok?(other, shard_id, lock_expires_at_ms, Storage.now_ms()),
          do: {:ok, 0},
          else: {:ok, lock_expires_at_ms + margin}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def lease_stealable_at(shard_id) do
    case read_lock(shard_id) do
      {:ok, %{owner: other, expires_at_ms: lock_exp}} ->
        case stealable_at(shard_id, other, lock_exp) do
          {:ok, at} -> {:held, other, at}
          {:error, reason} -> {:error, reason}
        end

      :enoent ->
        :free

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def lease_holder(shard_id) do
    now = Storage.now_ms()

    case read_lock(shard_id) do
      {:ok, %{owner: other, expires_at_ms: lock_exp}} ->
        case owner_live?(shard_id, other, now, lock_exp) do
          :live -> {:held, other}
          :dead -> :free
          {:error, reason} -> {:error, reason}
        end

      :enoent ->
        :free

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def check_lease(shard_id, %{owner: owner, epoch: epoch}) do
    # NO mutex here (expert review #28 follow-up): check_lease is the read-only flush
    # fence, called on every durability flush and every lapse revalidation — a hot path.
    # Routing it through the VM-wide :global.trans (the per-shard mutex) added lease-
    # renewal contention that delayed steal detection past the lease tests' timeouts.
    # The atomic write_lock below already makes a torn read impossible (the rename is
    # atomic, so a reader sees whole-old or whole-new), which is the only correctness
    # property this read needs; mutation ordering against it is not required.
    case read_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}} -> :ok
      {:ok, _other} -> {:error, :superseded}
      :enoent -> {:error, :superseded}
      {:error, reason} -> {:error, reason}
    end
  end

  # --- node heartbeat ---

  @impl true
  def renew_heartbeat(owner, ttl_ms) do
    hb = %{owner: owner, expires_at_ms: Storage.now_ms() + ttl_ms}

    # atomic_write, not a bare File.write (expert review #39): open-truncate-write lets a
    # concurrent read_heartbeat (another shard's acquire_lease / owner_live?) observe the
    # empty/partial file between truncate and write — a spurious :corrupt_heartbeat that
    # fails lease acquisition closed, and a semantic divergence from S3's atomic PUT that
    # this backend is the test double for. Temp+rename gives readers whole-old or whole-new.
    case Storage.atomic_write(heartbeat_path(owner), Storage.encode_heartbeat(hb)) do
      :ok -> {:ok, hb}
      err -> err
    end
  end

  @impl true
  def read_heartbeat(owner) do
    case File.read(heartbeat_path(owner)) do
      {:ok, body} ->
        case Storage.decode_heartbeat(body) do
          {:ok, hb} -> {:ok, hb}
          :error -> {:error, :corrupt_heartbeat}
        end

      {:error, :enoent} ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def clear_heartbeat(owner) do
    case File.rm(heartbeat_path(owner)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def renew_lease(shard_id, lease, ttl_ms) do
    with_lock_mutex(shard_id, fn -> do_renew_lease(shard_id, lease, ttl_ms) end)
  end

  defp do_renew_lease(shard_id, %{owner: owner, epoch: epoch}, ttl_ms) do
    now = Storage.now_ms()

    case read_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}} ->
        write_lock(shard_id, %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms})

      # Owner or epoch changed, or the lock vanished — we've been superseded.
      {:ok, _other} ->
        {:error, :superseded}

      :enoent ->
        {:error, :superseded}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def release_lease(shard_id, lease) do
    with_lock_mutex(shard_id, fn -> do_release_lease(shard_id, lease) end)
  end

  # Conditional release (the S3 backend's finding-#22 fix, mirrored): only remove
  # the lock while it is still OURS — under the mutex, a stealer's fresh lock can
  # never be deleted by a stale owner's release racing it.
  defp do_release_lease(shard_id, %{owner: owner, epoch: epoch}) do
    # REPORT THE REMOVAL (expert review 2026-08-20 #31). This used to discard `File.rm/1`'s result
    # and return `:ok` unconditionally — the Local twin of the S3 bug AGENTS.md records at length,
    # where "a 412 was reported as `:ok`, collapsing two opposite situations". Here the two are
    # "the lock is someone else's" (a correct no-op, finding #22) and "the lock is ours and the
    # unlink FAILED" (a leak reported as success). Because this backend is the default for the
    # whole suite, reporting them identically also hid the class from `mix test`.
    #
    # `:enoent` is `:ok`: the lock being already gone is the outcome this asked for.
    case read_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}} ->
        case File.rm(lock_path(shard_id)) do
          :ok -> :ok
          {:error, :enoent} -> :ok
          {:error, reason} -> {:error, reason}
        end

      _ ->
        :ok
    end
  end

  defp create_lock(shard_id, lease) do
    path = lock_path(shard_id)
    File.mkdir_p!(Path.dirname(path))

    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        # CHECK BOTH RESULTS (expert review 2026-08-20 #31). `IO.binwrite/2` returns
        # `{:error, :enospc}` / `{:error, :eio}`, and `File.close/1` is where a BUFFERED write's
        # error actually surfaces. Both were dropped and `{:ok, lease}` returned unconditionally,
        # so on a full or failing disk the caller believed it held a lease over a ZERO-LENGTH lock
        # file — which then reads as `:corrupt_lock` for the rest of that tenant's life.
        #
        # `write_lock/2` immediately below already carries the comment naming this exact hazard and
        # routes through `Storage.atomic_write/2`; `create_lock/2` was never given the same
        # treatment. It cannot simply reuse it: the `:exclusive` open IS the create race (this
        # backend's equivalent of S3's `PUT If-None-Match: *`), and a temp+rename would clobber a
        # winner instead of losing to it.
        #
        # The partial file is removed on failure so the next acquire takes the clean `:enoent`
        # path rather than the repair path below.
        with :ok <- IO.binwrite(io, Storage.encode_lease(lease)),
             :ok <- File.close(io) do
          {:ok, lease}
        else
          {:error, reason} ->
            _ = File.close(io)
            _ = File.rm(path)
            {:error, reason}
        end

      # Lost the create race — whoever won holds it.
      {:error, :eexist} ->
        case read_lock(shard_id) do
          {:ok, %{owner: owner}} -> {:error, {:held, owner}}
          _ -> {:error, :lock_race}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_lock(shard_id, lease) do
    # atomic_write, not a truncating File.write (expert review #28/#39): a concurrent
    # reader (check_lease, owner_live?) must never observe the empty/partial file
    # between truncate and write — that reads as :corrupt_lock and fails the fence
    # closed spuriously. Callers already hold the per-shard mutex; this covers a read
    # racing the write. Mirrors the renew_heartbeat fix.
    case Storage.atomic_write(lock_path(shard_id), Storage.encode_lease(lease)) do
      :ok -> {:ok, lease}
      err -> err
    end
  end

  defp read_lock(shard_id) do
    case File.read(lock_path(shard_id)) do
      {:ok, body} ->
        case Storage.decode_lease(body) do
          {:ok, lease} -> {:ok, lease}
          :error -> {:error, :corrupt_lock}
        end

      {:error, :enoent} ->
        :enoent

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp remote_path(shard_id), do: Path.join(dir(), "#{shard_id}.db")
  defp position_path(shard_id), do: Path.join(dir(), "#{shard_id}.db.pos")
  defp lineage_path(shard_id), do: Path.join(dir(), "#{shard_id}.db.lineage")
  defp version_path(shard_id, version), do: Path.join(dir(), "#{shard_id}@#{version}.db")

  defp snapshot_path(shard_id, snapshot_id),
    do: Path.join(dir(), "#{shard_id}@snap-#{snapshot_id}.db")

  defp lock_path(shard_id), do: Path.join(dir(), "#{shard_id}.lock")
  # One heartbeat object per node (owner), under a subdir so it never collides with
  # a shard id (owner strings like "fathom@host" are valid filenames).
  # Per-shard node-local critical section for lock mutations (expert review #38).
  # :global.trans with a node-local scope — no cluster involved (fathom has no BEAM
  # cluster; this backend runs on one machine). Infinite retries: the sections are
  # microseconds long, so waiting is correct and :aborted is unreachable in practice.
  defp with_lock_mutex(shard_id, fun) do
    case :global.trans({{__MODULE__, shard_id}, self()}, fun, [node()]) do
      :aborted -> {:error, :lock_mutex_busy}
      result -> result
    end
  end

  # Encode the owner for parity with the S3 backend (expert review round-2 #3): `#` is a
  # legal filename char so Local never needed it, but keying both doubles identically
  # keeps the fence's test-double faithful (the S3 backend must encode — a raw `#` is a
  # URI fragment there).
  defp heartbeat_path(owner), do: Path.join([dir(), "heartbeats", URI.encode_www_form(owner)])

  # Public (@doc false) for the same reason as `Fathom.Shard.data_dir/0`: the test suite and
  # `test_helper.exs` sweep the same directory this backend writes into, and duplicating the
  # config/default at each of those sites is how they drift apart.
  @doc false
  def dir do
    Application.get_env(:fathom, __MODULE__, [])[:dir] ||
      Path.join(System.tmp_dir!(), "fathom_remote")
  end
end
