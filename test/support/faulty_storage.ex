defmodule Fathom.Test.FaultyStorage do
  @moduledoc """
  Test storage backend: delegates to `Fathom.Shard.Storage.Local`, but fails the operation named
  in `config :fathom, :storage_fault` (one of `:acquire | :renew | :pull | :read_heartbeat | :flush`) so a
  test can simulate a node being partitioned from the lease store / object store (S3). Used by the
  S6 chaos tests to prove the node FAILS CLOSED — it must never serve without a confirmed lease, and
  must never steal a shard when it can't read the current owner's heartbeat.
  """
  @behaviour Fathom.Shard.Storage

  alias Fathom.Shard.Storage.Local

  defp fault, do: Application.get_env(:fathom, :storage_fault)

  # Generic "run this 0-arity fun right before op X, inside the backend call" hook, set via
  # `config :fathom, :faulty_before, {op, fun}`. Lets a test inject an event mid-operation —
  # e.g. force a heartbeat lapse during acquire_lease to exercise the fence-baseline ordering
  # (finding #5) — without coupling this module to what the event is.
  defp run_before(op) do
    case Application.get_env(:fathom, :faulty_before) do
      {^op, fun} when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  # --- lock-etag semantics (expert review 2026-08-01 #9) -------------------------------------
  #
  # `Local` identifies a lock by `{owner, epoch}` only. The S3 backend also carries
  # `:lock_etag` — the etag of the lock object WE last wrote — and its release fast path is a
  # conditional `DELETE … If-Match: <that etag>`, where a 412 is a no-op ("the lock is not the
  # one I wrote").
  #
  # That difference makes a whole bug class INVISIBLE to `mix test`: any path that releases with
  # a lease whose `lock_etag` is stale succeeds against `Local` and silently leaks the lock
  # object against S3. Finding #9 was exactly that, on the drain path. So model the contract
  # here: stamp a fresh etag on every acquire/renew, and make release conditional on it.
  #
  # `:lock_etag_strict` gates it (default ON for this backend) so a test that predates the
  # contract can opt out rather than be rewritten.
  # :persistent_term, not ETS — an ETS table is owned by the process that created it, and the
  # first creator here is a shard coordinator, so the table died with it mid-test.
  defp bump_lock_etag(shard_id) do
    etag = "lock-#{System.unique_integer([:positive])}"
    :persistent_term.put({__MODULE__, :lock_etag, shard_id}, etag)
    etag
  end

  defp current_lock_etag(shard_id),
    do: :persistent_term.get({__MODULE__, :lock_etag, shard_id}, nil)

  defp strict_lock_etag?, do: Application.get_env(:fathom, :lock_etag_strict, true)

  @impl true
  def acquire_lease(shard_id, owner, ttl_ms) do
    run_before(:acquire)

    if fault() == :acquire do
      {:error, {:transient_lookup, :s3_unreachable}}
    else
      case Local.acquire_lease(shard_id, owner, ttl_ms) do
        {:ok, lease} -> {:ok, Map.put(lease, :lock_etag, bump_lock_etag(shard_id))}
        other -> other
      end
    end
  end

  @impl true
  def renew_lease(shard_id, lease, ttl_ms) do
    renew_delay()

    if fault() == :renew do
      {:error, {:transient, :s3_unreachable}}
    else
      case Local.renew_lease(shard_id, lease, ttl_ms) do
        # A renew REWRITES the lock object, so its etag rotates — the detail that made #9's
        # stale-lease release a silent no-op against real S3.
        {:ok, renewed} -> {:ok, Map.put(renewed, :lock_etag, bump_lock_etag(shard_id))}
        other -> other
      end
    end
  end

  # Optional artificial lease-renew latency (ms) so a test can make the legacy-mode flush fence's
  # renew PUT slow — proving it runs OFF the coordinator process and doesn't block the mailbox
  # (expert review 2026-07-18 #18).
  defp renew_delay do
    case Application.get_env(:fathom, :storage_renew_delay_ms) do
      ms when is_integer(ms) and ms > 0 -> Process.sleep(ms)
      _ -> :ok
    end
  end

  @impl true
  def pull(shard_id, local_path) do
    delay()

    if fault() == :pull,
      do: {:error, :s3_unreachable},
      else: Local.pull(shard_id, local_path)
  end

  # Optional artificial pull latency (ms) so a test can make a cold open slow enough to
  # exercise the checkout call timeout without a real slow backend.
  defp delay do
    case Application.get_env(:fathom, :storage_pull_delay_ms) do
      ms when is_integer(ms) and ms > 0 -> Process.sleep(ms)
      _ -> :ok
    end
  end

  # Optional artificial flush latency (ms) so a test can make a coordinator's terminate
  # flush-to-storage slow — the "slow/hung S3" the admission-path eviction budget guards
  # against (expert review 2026-07-14 #7).
  defp flush_delay do
    case Application.get_env(:fathom, :storage_flush_delay_ms) do
      ms when is_integer(ms) and ms > 0 -> Process.sleep(ms)
      _ -> :ok
    end
  end

  @impl true
  def pull_if_changed(shard_id, local_path, etag) do
    delay()

    if fault() == :pull do
      {:error, :s3_unreachable}
    else
      result = Local.pull_if_changed(shard_id, local_path, etag)
      # run_before(:promote) fires between a 304 and the caller's warm-cache copy,
      # exercising the promotion TOCTOU (#13): the follower swaps fresher bytes into
      # the cache path after the freshness check validated the older etag.
      if result == {:ok, :unchanged}, do: run_before(:promote)
      result
    end
  end

  @impl true
  def read_heartbeat(owner) do
    if fault() == :read_heartbeat,
      do: {:error, :s3_unreachable},
      else: Local.read_heartbeat(owner)
  end

  @impl true
  def flush(shard_id, local_path) do
    flush_delay()

    cond do
      fault() == :flush -> {:error, :s3_unreachable}
      fault() == :flush_too_large -> {:error, {:object_too_large, 6_000_000_000}}
      true -> Local.flush(shard_id, local_path)
    end
  end

  @impl true
  def flush(shard_id, local_path, expected_etag) do
    # run_before(:flush) lets a test steal the shard (overwrite the object) in the window
    # between the coordinator's fence check and this write, exercising the fenced flush (#15).
    run_before(:flush)
    flush_delay()

    cond do
      fault() == :flush ->
        {:error, :s3_unreachable}

      # The PERMANENT flush failure (#37): past the S3 single-PUT ceiling, every retry fails
      # identically forever while the shard keeps acking writes. Staging a real 5 GiB object is
      # not an option, so the backend reports the same error shape the S3 backend does.
      fault() == :flush_too_large ->
        {:error, {:object_too_large, 6_000_000_000}}

      # THE LOST RESPONSE (expert review 2026-08-01 #4). The PUT lands server-side and the
      # response never gets back — a documented real case on the S3 path
      # (Mint.TransportError{reason: :closed}). The object advances; the coordinator believes
      # the flush failed and keeps its old fence etag. The NEXT flush then 412s against our
      # own earlier write, and the "lock still ours" reconcile used to conclude the object was
      # current and mark the shard clean, discarding everything written in between.
      fault() == :flush_lands_then_errors ->
        _ = Local.flush(shard_id, local_path, expected_etag)
        {:error, :s3_unreachable}

      true ->
        Local.flush(shard_id, local_path, expected_etag)
    end
  end

  # `run_before(:object_etag)` is what lets a test RAISE from a storage call that the coordinator
  # makes DIRECTLY (not inside a rescued Task) while it already holds the lease — the warm open's
  # `post_lease_warm_check/3`. Every other fault mode here returns an `{:error, _}` tuple, and a
  # tuple takes the open's ordinary failure path, which already released. The un-covered class was
  # an EXCEPTION (a `Req.TransportError` raised out of a HEAD under load), which skipped the
  # release entirely and stranded the lock. See the guard in `Fathom.Shard.handle_continue/2`.
  @impl true
  def object_etag(shard_id) do
    run_before(:object_etag)
    Local.object_etag(shard_id)
  end

  # Mirrors S3's conditional release: If-Match the etag WE last wrote. A mismatch is a 412,
  # which S3 treats as a no-op — the lock stays. Releasing with a lease whose lock_etag has
  # since rotated (a legacy-mode renew) therefore LEAKS the lock, which is finding #9.
  @impl true
  def release_lease(shard_id, %{lock_etag: etag} = lease) when is_binary(etag) do
    if strict_lock_etag?() and current_lock_etag(shard_id) != etag do
      :ok
    else
      Local.release_lease(shard_id, lease)
    end
  end

  def release_lease(shard_id, lease), do: Local.release_lease(shard_id, lease)

  @impl true
  def check_lease(shard_id, lease) do
    # A dedicated hook (separate from run_before/:faulty_before, whose single slot a test
    # may already be using to force the data-PUT 412) to hold the periodic flush's
    # 412-reconcile lock re-check WHILE the 412 is in flight — proving that re-check runs
    # off the coordinator process (expert review 2026-07-14 #8). The two hooks fire on
    # different ops: :faulty_before {:flush, ...} on the PUT, :faulty_check_lease here.
    case Application.get_env(:fathom, :faulty_check_lease) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end

    Local.check_lease(shard_id, lease)
  end

  @impl true
  def lease_holder(shard_id), do: Local.lease_holder(shard_id)
  @impl true
  def lease_stealable_at(shard_id), do: Local.lease_stealable_at(shard_id)
  @impl true
  def renew_heartbeat(owner, ttl_ms), do: Local.renew_heartbeat(owner, ttl_ms)
  @impl true
  def clear_heartbeat(owner), do: Local.clear_heartbeat(owner)
  @impl true
  def retain(shard_id, version) do
    # run_before(:retain) lets a test steal the shard mid-migration (the migrator retains the
    # prev version just before the copy + its pre-flush fence), exercising the self-fence (#7).
    run_before(:retain)
    Local.retain(shard_id, version)
  end

  @impl true
  def restore(shard_id, version) do
    # run_before(:restore) lets a test steal the shard (change the live object) in the window
    # between the migrator's read-only fence and this copy-back — the revert counterpart of the
    # {:flush, ...} hook (expert review 2026-07-14 #4). Also on the arity-2 (unconditional) clause
    # so the pre-fix reproduction, which calls restore/2, injects the steal at the same point.
    run_before(:restore)
    Local.restore(shard_id, version)
  end

  @impl true
  def restore(shard_id, version, expected_etag) do
    run_before(:restore)
    Local.restore(shard_id, version, expected_etag)
  end

  @impl true
  def drop_version(shard_id, version), do: Local.drop_version(shard_id, version)

  @impl true
  def put_tombstone(shard_id), do: Local.put_tombstone(shard_id)
  @impl true
  def tombstoned_ids, do: Local.tombstoned_ids()
  @impl true
  def put_token_floor(shard_id, version), do: Local.put_token_floor(shard_id, version)
  @impl true
  def read_token_floor(shard_id), do: Local.read_token_floor(shard_id)
  @impl true
  def pull_snapshot(shard_id, snapshot_id, local_path),
    do: Local.pull_snapshot(shard_id, snapshot_id, local_path)

  @impl true
  def fork_from(template_id, version, dst_shard_id) do
    # run_before(:fork_from) lets a test inject an event just before the fork's snapshot
    # copy lands at the dst live object (fork-from-template, finding #10).
    run_before(:fork_from)

    if fault() == :fork_from,
      do: {:error, :s3_unreachable},
      else: Local.fork_from(template_id, version, dst_shard_id)
  end

  @impl true
  def drop_live(shard_id), do: Local.drop_live(shard_id)

  @impl true
  def snapshot(shard_id, snapshot_id), do: Local.snapshot(shard_id, snapshot_id)
  @impl true
  def list_snapshots(shard_id), do: Local.list_snapshots(shard_id)
  @impl true
  def restore_snapshot(shard_id, snapshot_id), do: Local.restore_snapshot(shard_id, snapshot_id)

  @impl true
  def restore_snapshot(shard_id, snapshot_id, expected_etag),
    do: Local.restore_snapshot(shard_id, snapshot_id, expected_etag)

  @impl true
  def drop_snapshot(shard_id, snapshot_id), do: Local.drop_snapshot(shard_id, snapshot_id)

  @impl true
  def purge_shard(shard_id), do: Local.purge_shard(shard_id)

  @impl true
  def fork_shard(src_id, dst_id), do: Local.fork_shard(src_id, dst_id)
end
