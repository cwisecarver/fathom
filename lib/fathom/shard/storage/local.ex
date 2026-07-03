defmodule Fathom.Shard.Storage.Local do
  @moduledoc """
  Filesystem-backed shard storage: a local directory stands in for the object
  store. The default backend for dev and tests — it exercises the full
  pull-on-wake / flush-on-idle path without needing S3 credentials.

  Configure the "remote" directory (defaults to a temp dir):

      config :fathom, Fathom.Shard.Storage.Local, dir: "/var/fathom/remote"
  """
  @behaviour Fathom.Shard.Storage

  alias Fathom.Shard.Storage

  @impl true
  def pull(shard_id, local_path) do
    case File.read(remote_path(shard_id)) do
      {:ok, body} ->
        # Atomic (temp + rename) so a concurrent reader never sees a half-copied file (#24).
        # Return the content-hash etag so the coordinator can fence its first flush (#15).
        case Storage.atomic_write(local_path, body) do
          :ok -> {:ok, content_etag(body)}
          err -> err
        end

      # Brand-new shard — no object, nothing written, no etag yet.
      {:error, :enoent} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def flush(shard_id, local_path) do
    # Atomic (temp + rename) so a crash mid-copy can't leave a torn "durable" object (#28).
    Storage.atomic_copy(local_path, remote_path(shard_id))
  end

  @impl true
  def flush(shard_id, local_path, expected_etag) do
    # Model S3's If-Match / If-None-Match:* fence with the content-hash etag: only write if the
    # remote's current etag still matches what the coordinator last saw (or, for a brand-new
    # shard, only if no object exists). A mismatch means a stealer flushed in the window since
    # the fence check → superseded, don't clobber (finding #15).
    with {:ok, body} <- File.read(local_path) do
      current =
        case File.read(remote_path(shard_id)) do
          {:ok, remote_body} -> content_etag(remote_body)
          {:error, :enoent} -> nil
          {:error, reason} -> throw({:error, reason})
        end

      cond do
        expected_etag != current ->
          {:error, :superseded}

        true ->
          case Storage.atomic_write(remote_path(shard_id), body) do
            :ok -> {:ok, content_etag(body)}
            err -> err
          end
      end
    end
  catch
    {:error, _} = err -> err
  end

  @impl true
  def object_etag(shard_id) do
    case File.read(remote_path(shard_id)) do
      {:ok, body} -> {:ok, content_etag(body)}
      {:error, :enoent} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  # A content hash stands in for S3's opaque etag: it changes iff the bytes change,
  # which is exactly the freshness signal `pull_if_changed/3` needs. (S3's real 304
  # skips the body; the Local double reads it to hash, which is fine on local disk.)
  @impl true
  def pull_if_changed(shard_id, local_path, etag) do
    case File.read(remote_path(shard_id)) do
      {:ok, body} ->
        current = content_etag(body)

        if etag == current do
          {:ok, :unchanged}
        else
          # Atomic so a warm-cache rewrite can't be read half-old/half-new by a promotion (#24).
          case Storage.atomic_write(local_path, body) do
            :ok -> {:ok, {:written, current}}
            err -> err
          end
        end

      {:error, :enoent} ->
        {:ok, :absent}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp content_etag(body), do: Base.encode16(:crypto.hash(:sha256, body), case: :lower)

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
  def drop_version(shard_id, version) do
    case File.rm(version_path(shard_id, version)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # --- leasing ---
  #
  # The lease is a `<shard_id>.lock` JSON file next to the `.db` object. A fresh
  # lease is created with `O_EXCL` so concurrent first-creators can't both win;
  # steal/reclaim/renew read-modify-write. On a single machine (the only place the
  # filesystem backend runs) the local Registry already guarantees one coordinator
  # per shard, so the read-modify-write race can't actually happen here — the lock
  # file makes the Local backend a faithful test double for the S3 fence semantics.

  @impl true
  def acquire_lease(shard_id, owner, ttl_ms) do
    now = Storage.now_ms()

    case read_lock(shard_id) do
      # Our own lease (live or stale) — reclaim it, keeping the epoch.
      {:ok, %{owner: ^owner, epoch: epoch}} ->
        write_lock(shard_id, %{owner: owner, epoch: epoch, expires_at_ms: now + ttl_ms})

      # Someone else holds it — liveness is *their heartbeat* (with the lock's own TTL as the
      # fallback when they run no heartbeat — see owner_live?/3).
      {:ok, %{owner: other, epoch: epoch, expires_at_ms: lock_exp}} ->
        case owner_live?(other, now, lock_exp) do
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

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Is `other` still alive? Its heartbeat object is the primary source of truth. Goes
  # through the Storage dispatch (resolves to this backend in prod) so a faulty
  # backend can inject a heartbeat-read failure for the fail-closed test. The
  # steal_margin absorbs inter-node clock skew (see Storage.steal_margin_ms/0): only
  # steal once the heartbeat is expired by more than the margin.
  defp owner_live?(other, now, lock_expires_at_ms) do
    margin = Storage.steal_margin_ms()

    case Storage.read_heartbeat(other) do
      {:ok, %{expires_at_ms: exp}} when now <= exp + margin -> :live
      {:ok, _stale} -> :dead
      # No heartbeat object at all (`heartbeat_server: false` legacy mode, or the owner's
      # heartbeat was cleared): fall back to the lock's OWN TTL for liveness (finding #11), so
      # a live owner renewing its lock per-shard isn't instantly stolen. Heartbeat stays primary.
      :not_found -> if now <= lock_expires_at_ms + margin, do: :live, else: :dead
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def check_lease(shard_id, %{owner: owner, epoch: epoch}) do
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
  def renew_lease(shard_id, %{owner: owner, epoch: epoch}, ttl_ms) do
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
  def release_lease(shard_id, %{owner: owner, epoch: epoch}) do
    case read_lock(shard_id) do
      {:ok, %{owner: ^owner, epoch: ^epoch}} -> File.rm(lock_path(shard_id))
      _ -> :ok
    end

    :ok
  end

  defp create_lock(shard_id, lease) do
    path = lock_path(shard_id)
    File.mkdir_p!(Path.dirname(path))

    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        IO.binwrite(io, Storage.encode_lease(lease))
        File.close(io)
        {:ok, lease}

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
    path = lock_path(shard_id)
    File.mkdir_p!(Path.dirname(path))

    case File.write(path, Storage.encode_lease(lease)) do
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
  defp version_path(shard_id, version), do: Path.join(dir(), "#{shard_id}@#{version}.db")
  defp lock_path(shard_id), do: Path.join(dir(), "#{shard_id}.lock")
  # One heartbeat object per node (owner), under a subdir so it never collides with
  # a shard id (owner strings like "fathom@host" are valid filenames).
  defp heartbeat_path(owner), do: Path.join([dir(), "heartbeats", owner])

  defp dir do
    Application.get_env(:fathom, __MODULE__, [])[:dir] ||
      Path.join(System.tmp_dir!(), "fathom_remote")
  end
end
