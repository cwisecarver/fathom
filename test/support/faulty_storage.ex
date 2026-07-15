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

  @impl true
  def acquire_lease(shard_id, owner, ttl_ms) do
    run_before(:acquire)

    if fault() == :acquire,
      do: {:error, {:transient_lookup, :s3_unreachable}},
      else: Local.acquire_lease(shard_id, owner, ttl_ms)
  end

  @impl true
  def renew_lease(shard_id, lease, ttl_ms) do
    if fault() == :renew,
      do: {:error, {:transient, :s3_unreachable}},
      else: Local.renew_lease(shard_id, lease, ttl_ms)
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

    if fault() == :flush,
      do: {:error, :s3_unreachable},
      else: Local.flush(shard_id, local_path)
  end

  @impl true
  def flush(shard_id, local_path, expected_etag) do
    # run_before(:flush) lets a test steal the shard (overwrite the object) in the window
    # between the coordinator's fence check and this write, exercising the fenced flush (#15).
    run_before(:flush)
    flush_delay()

    if fault() == :flush,
      do: {:error, :s3_unreachable},
      else: Local.flush(shard_id, local_path, expected_etag)
  end

  @impl true
  def object_etag(shard_id), do: Local.object_etag(shard_id)

  @impl true
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
  def drop_snapshot(shard_id, snapshot_id), do: Local.drop_snapshot(shard_id, snapshot_id)
end
