defmodule Fathom.Shard do
  @moduledoc """
  Per-shard coordinator: one process per active shard, owning the shard's
  *lifecycle*.

  - **On wake** (`init`) it makes the SQLite file available: a present local copy
    is authoritative (it may hold un-flushed writes), so it only pulls from
    `Fathom.Shard.Storage` on a cold start.
  - **Per stream** it hands out the file path (`checkout/1`) and tracks the
    checked-out connections — by monitoring the stream process — so it knows when
    the shard is active.
  - **On idle** (no connections checked out for the idle window) it checkpoints
    the WAL, flushes the file back to storage, drops the local copy, and stops.

  It does **not** hold a shared database connection: each Hrana stream opens its
  own `Fathom.Shard.Connection`, so per-stream transactions are isolated. The
  idle flush runs only while zero connections are checked out, so it can safely
  checkpoint-and-copy the file whole; the periodic durability flush (below) runs
  while writers are active, so it takes a *consistent snapshot* instead.

  ## Durability window

  The idle flush alone leaves a gap: between flushes, writes live only on local
  disk, so a busy shard that never idles could lose an unbounded amount of data if
  the node dies. A **periodic durability flush** closes that gap — every
  `:shard_flush_interval_ms` it snapshots the live database with SQLite
  `VACUUM INTO` (transactionally consistent even with active writers, unlike a raw
  copy) and uploads the snapshot, *without* dropping the local copy or releasing
  the lease. The data-loss window is bounded to the interval. It is fenced like the
  idle flush (re-checks the lease first; self-fences on a lost lease) so it never
  clobbers a new owner. Set the interval to `0` to disable (idle-only). This is a
  full-file snapshot each interval; incremental WAL streaming is future work.

  ## Cross-node single-writer leasing

  Before it pulls, the coordinator **acquires a lease** on the shard through
  `Fathom.Shard.Storage` (`owner` = this node). If another node holds a live lease
  it refuses to start (`{:stop, {:shard_held, owner}}`), so only one node ever
  opens a given shard. It **renews** the lease every `ttl/3` for its whole life; if
  a renewal comes back `:superseded` (another node stole an expired lease during a
  GC pause or partition) it **self-fences** — stops *without flushing*, because the
  new owner has taken over from the last good flush and our local copy would
  clobber theirs. The lease carries a monotonic **epoch** (the fencing token) and
  is released on a clean flush. The flush itself re-checks ownership first, so a
  lease lost between the last renewal and idle never results in a clobbering write.
  """
  # :temporary, not :transient — a crashed coordinator must NOT be auto-restarted (finding #8). A
  # restart would come back with conns: %{} while the surviving Filo stream processes (a separate
  # supervision tree) still hold open connections to the file and keep writing; the empty
  # coordinator would later flush-and-drop the file under those live writers, or (if no checkout
  # arrives) hold the shard + lease forever. Find-or-start (Fathom.Shards.ensure) recreates the
  # coordinator on the next real checkout, which re-adopts the present local file (warm+dirty).
  use GenServer, restart: :temporary

  require Logger

  alias Fathom.Shard.{Connection, Fence, Heartbeat, Storage, WarmFollower, WriteCounter}

  @default_idle_ms 60_000
  @default_lease_ttl_ms 30_000
  @default_flush_interval_ms 30_000
  @pull_timeout 60_000
  # Above the coordinator's own open budget (@pull_timeout + lease acquire + any inline
  # flush), so a slow-but-normal cold open never trips the caller's checkout timeout.
  @default_checkout_timeout @pull_timeout + 15_000

  def start_link(shard_id) do
    GenServer.start_link(__MODULE__, shard_id, name: via(shard_id))
  end

  @doc "Registry `:via` tuple addressing the coordinator for `shard_id`."
  def via(shard_id) do
    {:via, Registry, {Fathom.ShardRegistry, shard_id}}
  end

  @doc """
  Checks out the shard for the calling process: ensures the file is available,
  registers the caller as an active connection (so the shard won't flush/idle
  while it is in use), and returns `{:ok, ref, path}`. Pass `ref` to `checkin/2`
  when the connection closes.
  """
  def checkout(pid) when is_pid(pid) do
    # The open path (handle_continue) can legitimately block a queued :checkout for up to
    # @pull_timeout (a large cold pull, cross-region S3), and an inline durability flush can
    # add more. The default 5s GenServer.call timeout is far below that, so a slow-but-normal
    # cold open made the FIRST checkout fail with {:error, :timeout} on the headline path —
    # and the coordinator later processed the stale call, registering a phantom connection
    # for a caller that had already given up. Give the call a budget above the coordinator's
    # own open budget; the coordinator still bounds the pull with @pull_timeout, so this
    # never waits forever on a genuinely stuck open. Configurable for real-S3 tuning.
    GenServer.call(pid, :checkout, checkout_timeout())
  catch
    # The coordinator self-stopped while opening storage (lease held by another
    # node, or pull failed) — its `{:shutdown, reason}` exit reaches the pending
    # call. Surface the reason as a clean error rather than crashing the caller.
    :exit, {{:shutdown, reason}, _} -> {:error, reason}
    :exit, {:noproc, _} -> {:error, :unavailable}
    :exit, {reason, _} -> {:error, reason}
  end

  @doc "Releases a connection previously checked out with `checkout/1`."
  def checkin(pid, ref) when is_pid(pid), do: GenServer.cast(pid, {:checkin, ref})

  @doc """
  Whether the shard holds local writes not yet flushed to storage — i.e. its write counter
  (`Fathom.Shard.WriteCounter`, bumped lock-free per write by `Fathom.ShardExecutor`) is ahead of
  the coordinator's `flushed_through` watermark. Used by tests; the coordinator itself checks this
  internally on each flush decision. Finding #27 replaced a per-write cast with this ETS signal.
  """
  def dirty?(pid) when is_pid(pid), do: GenServer.call(pid, :dirty?)

  @doc """
  Asks the coordinator to stand down: refuse new checkouts, let in-flight
  connections finish (up to `drain_timeout` ms), then flush + drop + release the
  lease and stop. If the drain times out it resumes serving and sends `reply_to`
  `{:drain_aborted, pid}`. Fire-and-forget; `Fathom.Shards.drain/2` watches for the
  process to exit (success) or the abort message (still busy).
  """
  def request_drain(pid, drain_timeout, reply_to) when is_pid(pid),
    do: GenServer.cast(pid, {:drain, drain_timeout, reply_to})

  @impl true
  def init(shard_id) do
    # Trap exits so a supervisor `:shutdown` (SIGTERM, rolling deploy) runs terminate/2 —
    # the final flush + lease release. Without this the coordinator is killed outright, so
    # up to a full flush interval of acknowledged writes is lost on a *clean* shutdown and
    # the lease isn't released for a fast planned failover. The parent's exit is handled by
    # the gen_server engine (it invokes terminate/2); the only other linked process is the
    # transient pull Task, whose result await_pull already consumes — its stray EXIT is
    # ignored in handle_info below.
    Process.flag(:trap_exit, true)

    # Return immediately so DynamicSupervisor.start_child doesn't serialize concurrent
    # shard opens on blocking S3 I/O (start_child waits for init). The lease acquire +
    # pull run in handle_continue. A `:checkout` call queues until the continue
    # completes (OTP runs continue before any other message), so callers still only
    # get a connection once the shard is open and fenced. On open failure the continue
    # stops the coordinator and the pending `:checkout` exit is mapped to `{:error, _}`
    # by `checkout/1`.
    {:ok, %{id: shard_id}, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, %{id: shard_id} = state) do
    open_started = System.monotonic_time()
    path = db_path(shard_id)
    File.mkdir_p!(Path.dirname(path))
    owner = to_string(node())
    ttl = lease_ttl_ms()

    # Overlap the lease acquire with the storage pull to cut the cold-open toward
    # one round-trip (they hit independent objects: `.lock` vs `.db`). Fence-first
    # is preserved where it matters — we still only SERVE after the lease confirms.
    # The pull writes to a TEMP file and is promoted to the real path only once the
    # lease is ours, so losing the lease race never leaves a stale local copy that a
    # later open would wrongly treat as authoritative (a present local copy wins).
    # A present local copy means a warm restart (may hold un-flushed writes) — skip
    # the pull entirely.
    warm? = File.exists?(path)
    pull_task = if warm?, do: nil, else: start_pull(shard_id, path)

    # Sample the heartbeat generation BEFORE acquire_lease. The fence's invariant is
    # "generation unchanged since acquire ⇒ no steal was possible," which only holds if the
    # baseline predates ownership. Sampling it after acquire (and after the pull await, up to
    # @pull_timeout later) let a lapse-and-steal in that gap bump the generation before we read
    # it — the coordinator then recorded the post-steal value as its baseline and every later
    # flush passed the fence unconditionally, clobbering the stealer (finding #5). nil ⇒ legacy
    # mode; Heartbeat.running?/0 already guards an early call.
    acquire_gen = if Heartbeat.running?(), do: Fence.generation(), else: nil

    case Storage.acquire_lease(shard_id, owner, ttl) do
      {:ok, lease} ->
        emit_lease(:acquired, shard_id, %{epoch: lease.epoch})
        result = open_with_lease(shard_id, path, owner, ttl, lease, pull_task, warm?, acquire_gen)
        maybe_emit_cold_open(result, shard_id, warm?, open_started)
        result

      # `{:shutdown, _}` (not a bare reason) so the `:transient` child is NOT
      # restarted — a held shard must stay down, not restart-loop.
      {:error, {:held, holder}} ->
        emit_lease(:held, shard_id, %{holder: holder})
        abandon_pull(pull_task, path)
        {:stop, {:shutdown, {:shard_held, holder}}, state}

      # Fail closed: without a confirmed lease we must not open the shard.
      {:error, reason} ->
        abandon_pull(pull_task, path)
        {:stop, {:shutdown, {:lease_unavailable, reason}}, state}
    end
  end

  defp open_with_lease(shard_id, path, owner, ttl, lease, pull_task, warm?, acquire_gen) do
    case await_pull(pull_task, path, shard_id) do
      {:ok, etag} ->
        # No idle timer yet: a coordinator is always checked out right after it
        # starts, and the timer is (re)armed only when the last connection checks
        # back in. This avoids a start-vs-checkout idle race. The lease renewal
        # timer runs from the start, independent of checkouts.
        # `acquire_gen` (captured in handle_continue BEFORE acquire_lease — see finding #5)
        # fixes the liveness mode for this shard's life: non-nil ⇒ heartbeat mode (the node
        # heartbeat proves liveness, so NO per-shard lease renewal — the F1 storm — and the
        # generation gates the flush fence); nil ⇒ legacy mode (per-shard renewal + renew-PUT
        # fence, so a heartbeat outage degrades gracefully instead of dropping shards).
        state = %{
          id: shard_id,
          path: path,
          conns: %{},
          idle_ms: idle_ms(),
          timer: nil,
          owner: owner,
          lease: lease,
          ttl_ms: ttl,
          renew_timer: nil,
          acquire_gen: acquire_gen,
          # The current etag of the remote object (nil = brand-new / no object yet). The flush
          # fence If-Matches it so a stale PUT can't clobber a stealer (finding #15); each
          # successful flush advances it.
          etag: etag,
          lease_lost: false,
          # High-water mark of the shard's write counter (Fathom.Shard.WriteCounter) as of the
          # last successful flush; the shard is dirty iff count(id) > flushed_through (unflushed?/1).
          # A cold pull is clean (local == storage); a warm restart may hold un-flushed writes, so
          # init_flushed_through/2 seeds it one behind ⇒ dirty. Clean shards skip the durability
          # upload — the durability-storm fix (findings #1/#2, #27).
          flushed_through: init_flushed_through(shard_id, warm?),
          flush_timer: nil,
          draining: false,
          drain_timer: nil,
          drain_reply_to: nil
        }

        state = if acquire_gen == nil, do: schedule_renew(state), else: state
        {:noreply, schedule_flush(state)}

      {:error, reason} ->
        # Couldn't make the file available — give the lease back so another node
        # can try, rather than holding a shard we can't serve.
        Storage.release_lease(shard_id, lease)
        {:stop, {:shutdown, {:pull_failed, reason}}, %{id: shard_id}}
    end
  end

  # Pull into a temp file, off the init process. Wrapped so the task can never
  # crash the coordinator — it always returns `:ok | {:error, reason}`.
  defp start_pull(shard_id, path) do
    temp = pull_temp(path)

    Task.async(fn ->
      try do
        warm_or_cold_pull(shard_id, temp)
      rescue
        e -> {:error, {:pull_exception, e}}
      catch
        :exit, reason -> {:error, {:pull_exit, reason}}
      end
    end)
  end

  # Fill `temp` with the shard's current bytes. When the warm-follower holds a cached
  # copy for this shard, validate its freshness against storage before promoting it — a
  # warm cache may lag the owner's latest flush, so a stale copy must NEVER be served:
  #
  #   * `:unchanged` (304) — the cache equals storage's current object: copy it into
  #     `temp` with no full transfer (the warm-standby fast path). If the follower
  #     evicted it in the gap, fall back to a cold pull so we never promote a gone file.
  #   * `{:written, _}` (200) — the cache was stale: `temp` already holds the fresh bytes.
  #   * `:absent` (404) — no object (brand-new shard): leave `temp` absent.
  #
  # With no validatable warm copy (no cache / no recorded etag) it's an ordinary cold
  # pull — unchanged from before the warm follower existed. This runs speculatively,
  # overlapped with the lease acquire; `temp` is promoted to the live path only after
  # the lease confirms, so fence-first still holds (we only serve once we own the shard).
  # Returns `{:ok, etag_or_nil}` (the current object etag, threaded into the coordinator's
  # flush fence — finding #15) or `{:error, reason}`.
  defp warm_or_cold_pull(shard_id, temp) do
    case WarmFollower.cached_etag(shard_id) do
      nil ->
        Storage.pull(shard_id, temp)

      etag ->
        case Storage.pull_if_changed(shard_id, temp, etag) do
          {:ok, :unchanged} ->
            emit_warm(shard_id, :hit)
            # 304 ⇒ the cache equals storage's current object, so `etag` IS the current etag.
            case promote_warm_cache(shard_id, temp) do
              :ok -> {:ok, etag}
              {:ok, _} = ok -> ok
              {:error, _} = error -> error
            end

          {:ok, {:written, new_etag}} ->
            emit_warm(shard_id, :stale)
            {:ok, new_etag}

          {:ok, :absent} ->
            {:ok, nil}

          {:error, _} = error ->
            error
        end
    end
  end

  # A 304 says the cached bytes equal storage's current object — copy them into `temp`
  # (the warm promotion). If the cache vanished under us (the follower evicted it between our
  # etag read and now), fall back to a fresh cold pull (which returns `{:ok, etag}`) rather
  # than promote nothing.
  defp promote_warm_cache(shard_id, temp) do
    case File.cp(WarmFollower.cache_path(shard_id), temp) do
      :ok -> :ok
      {:error, _} -> Storage.pull(shard_id, temp)
    end
  end

  # Resolve the shard's current object etag for the flush fence (#15), returning
  # `{:ok, etag_or_nil}` or `{:error, reason}`. `nil` task ⇒ a local copy already existed
  # (warm restart): no pull ran, so fetch the etag with a HEAD so the first fenced flush can
  # If-Match it. Otherwise await the speculative pull and promote its temp into place.
  defp await_pull(nil, _path, shard_id) do
    case Storage.object_etag(shard_id) do
      {:ok, etag} -> {:ok, etag}
      {:error, reason} -> {:error, {:etag_unavailable, reason}}
    end
  end

  defp await_pull(task, path, _shard_id) do
    case Task.yield(task, @pull_timeout) || Task.shutdown(task) do
      {:ok, {:ok, etag}} ->
        promote_pull(path, etag)

      {:ok, {:error, _} = error} ->
        rm_pull_temp(path)
        error

      nil ->
        rm_pull_temp(path)
        {:error, :pull_timeout}

      {:exit, reason} ->
        rm_pull_temp(path)
        {:error, {:pull_crashed, reason}}
    end
  end

  # A new shard has no object, so the pull wrote no temp — leave the path absent (the first
  # connection creates it empty). Otherwise move the temp into place. Carries the object etag
  # through for the flush fence.
  defp promote_pull(path, etag) do
    temp = pull_temp(path)

    case if(File.exists?(temp), do: File.rename(temp, path), else: :ok) do
      :ok -> {:ok, etag}
      {:error, _} = err -> err
    end
  end

  # Lease lost / errored: kill the speculative pull and drop its temp file.
  defp abandon_pull(nil, _path), do: :ok

  defp abandon_pull(task, path) do
    Task.shutdown(task, :brutal_kill)
    rm_pull_temp(path)
    :ok
  end

  defp pull_temp(path), do: path <> ".pull"
  defp rm_pull_temp(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(pull_temp(path) <> &1))

  # While standing down, refuse new checkouts so the shard can drain. The caller
  # retries elsewhere (and, once the migrator holds the lease, lands on :held).
  @impl true
  def handle_call(:checkout, _from, %{draining: true} = state) do
    {:reply, {:error, :draining}, state}
  end

  def handle_call(:checkout, {caller, _tag}, state) do
    ref = Process.monitor(caller)
    state = %{cancel_idle(state) | conns: Map.put(state.conns, ref, true)}
    {:reply, {:ok, ref, state.path}, state}
  end

  def handle_call(:dirty?, _from, state), do: {:reply, unflushed?(state), state}

  @impl true
  def handle_cast({:checkin, ref}, state), do: stop_when_drained(release(state, ref))

  # Begin standing down. With no connections we stop immediately (terminate
  # flushes + releases). Otherwise wait for the drain, bounded by a timer.
  def handle_cast({:drain, _timeout, _reply_to}, %{conns: conns} = state)
      when map_size(conns) == 0 do
    {:stop, :normal, state}
  end

  def handle_cast({:drain, timeout, reply_to}, state) do
    timer = Process.send_after(self(), :drain_timeout, timeout)

    {:noreply,
     %{cancel_idle(state) | draining: true, drain_timer: timer, drain_reply_to: reply_to}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    stop_when_drained(release(state, ref))
  end

  # We trap exits (see init/1). The only process linked to us is the transient pull
  # Task (Task.async), and await_pull already consumes its result via Task.yield/
  # shutdown — so its terminal EXIT signal is redundant noise here. Ignore it. The
  # parent supervisor's EXIT is handled by the gen_server engine (→ terminate/2), not
  # routed to handle_info, so this never swallows a shutdown.
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  # Drain window elapsed with connections still open: give up standing down and
  # resume serving (the migration reschedules). No force-close, no torn flush.
  def handle_info(:drain_timeout, %{conns: conns} = state) when map_size(conns) == 0 do
    {:stop, :normal, state}
  end

  def handle_info(:drain_timeout, state) do
    Logger.info("shard #{state.id}: drain timed out with connections still open; resuming")
    if state.drain_reply_to, do: send(state.drain_reply_to, {:drain_aborted, self()})
    {:noreply, resume_serving(state)}
  end

  def handle_info(:idle_timeout, %{conns: conns} = state) when map_size(conns) == 0 do
    {:stop, :normal, state}
  end

  def handle_info(:idle_timeout, state), do: {:noreply, state}

  def handle_info(:renew_lease, state) do
    case Storage.renew_lease(state.id, state.lease, state.ttl_ms) do
      {:ok, lease} ->
        emit_lease(:renewed, state.id)
        {:noreply, schedule_renew(%{state | lease: lease})}

      # Another node took the lease — self-fence: stop without flushing so we
      # never clobber the new owner's copy.
      {:error, :superseded} ->
        emit_lease(:superseded, state.id)

        Logger.error(
          "shard #{state.id}: lease superseded by another node; self-fencing (stop without flush)"
        )

        {:stop, {:shutdown, :lease_lost}, %{state | lease_lost: true}}

      # Transient store error — a blip is not loss of ownership; keep serving and
      # retry. If it persists past the TTL another node will steal and the next
      # renewal will come back :superseded.
      {:error, reason} ->
        Logger.warning(
          "shard #{state.id}: lease renewal transient error (#{inspect(reason)}); retrying"
        )

        {:noreply, schedule_renew(state)}
    end
  end

  # Periodic durability flush: snapshot + upload the live shard without dropping
  # it, so a busy shard's data-loss window is bounded to the flush interval rather
  # than the whole session. Fenced like the idle flush.
  def handle_info(:durability_flush, state) do
    cond do
      # Clean: local == storage, so there's nothing to upload and no clobber risk.
      # Skip entirely (no fence, no PUT) — this is the durability-flush storm fix: at
      # a million mostly-idle/read-only shards, durability PUTs now track *writes*,
      # not open-shard count.
      not unflushed?(state) ->
        {:noreply, schedule_flush(state)}

      # Nothing written to disk yet (brand-new shard).
      not File.exists?(state.path) ->
        {:noreply, schedule_flush(state)}

      true ->
        case Fence.check(fence_ctx(state)) do
          {:ok, updates} ->
            state = Map.merge(state, updates)
            # Capture the write count BEFORE snapshotting: a write landing during the (blocking)
            # snapshot/upload bumps the counter past this, so the shard stays dirty and re-flushes
            # next interval — never silently cleared (findings #1/#2/#27).
            flushed_to = WriteCounter.count(state.id)

            case snapshot_and_upload(state) do
              # Uploaded; advance the fence etag and the flushed watermark (clears dirty).
              {:ok, new_etag} ->
                {:noreply, schedule_flush(%{state | flushed_through: flushed_to, etag: new_etag})}

              # The data PUT's If-Match failed: a stealer flushed in the upload window since our
              # fence check. Self-fence — do NOT retry over the new owner (finding #15).
              {:error, :superseded} ->
                Logger.error(
                  "shard #{state.id}: object superseded during durability flush; self-fencing"
                )

                {:stop, {:shutdown, :lease_lost}, %{state | lease_lost: true}}

              # Snapshot/PUT failed transiently: don't advance the watermark, so it stays dirty and
              # the next interval retries — and a later idle-drop flushes before dropping instead of
              # deleting un-stored writes.
              {:error, _reason} ->
                {:noreply, schedule_flush(state)}
            end

          # Lost the lease — self-fence (stop without flushing).
          :superseded ->
            Logger.error(
              "shard #{state.id}: lease superseded during durability flush; self-fencing"
            )

            {:stop, {:shutdown, :lease_lost}, %{state | lease_lost: true}}

          # Couldn't confirm ownership (transient store error / heartbeat not valid) —
          # don't write, keep dirty, retry next interval.
          :skip ->
            Logger.warning("shard #{state.id}: durability flush skipped, ownership unconfirmed")
            {:noreply, schedule_flush(state)}
        end
    end
  end

  @impl true
  def terminate(_reason, %{conns: conns, lease_lost: true} = state)
      when map_size(conns) == 0 do
    # We no longer own the shard; flushing our local copy would clobber the node
    # that took over. Drop it and leave the lease alone (it's theirs now).
    Fathom.ShardLoad.forget(state.id)
    WriteCounter.forget(state.id)
    drop_local(state.path)
    :ok
  end

  def terminate(_reason, %{conns: conns} = state) when map_size(conns) == 0 do
    Fathom.ShardLoad.forget(state.id)
    # flush_and_drop reads the write counter (unflushed?/1) to decide whether to upload before
    # dropping, so forget the counter AFTER it — forgetting first would zero the count and make a
    # dirty shard look clean, skipping the flush and losing the writes (findings #1/#27).
    flush_and_drop(state)
    WriteCounter.forget(state.id)
    :ok
  end

  # Stopping with connections still open (or from an early pre-open state) — always
  # drop the shard's load row so a stopped shard never leaks a counter.
  def terminate(_reason, state) do
    Fathom.ShardLoad.forget(state.id)
    WriteCounter.forget(state.id)
    :ok
  end

  # --- connection tracking ---

  defp release(state, ref) do
    Process.demonitor(ref, [:flush])
    conns = Map.delete(state.conns, ref)
    state = %{state | conns: conns}

    cond do
      map_size(conns) > 0 -> state
      # Standing down: don't arm idle — stop_when_drained will stop us.
      state.draining -> state
      true -> schedule_idle(state)
    end
  end

  # Stop (flushing + releasing via terminate) once the last connection drains
  # while standing down; otherwise carry on.
  defp stop_when_drained(%{conns: conns, draining: true} = state) when map_size(conns) == 0 do
    {:stop, :normal, state}
  end

  defp stop_when_drained(state), do: {:noreply, state}

  defp resume_serving(state) do
    %{cancel_drain(state) | draining: false, drain_reply_to: nil}
  end

  defp cancel_drain(%{drain_timer: nil} = state), do: state

  defp cancel_drain(%{drain_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | drain_timer: nil}
  end

  # --- flush ---

  defp flush_and_drop(state) do
    if unflushed?(state), do: flush_then_drop(state), else: drop_clean(state)
  end

  defp drop_clean(state) do
    # Clean: local == storage. Drop the local copy without re-uploading (nothing to
    # flush, no clobber possible) and release the lease for a clean handoff.
    if File.exists?(state.path) do
      drop_local(state.path)
      Storage.release_lease(state.id, state.lease)
    end

    :ok
  end

  defp flush_then_drop(state) do
    if File.exists?(state.path) do
      # Fence the flush: confirm we still own the shard right before uploading, so a
      # lease lost between acquire and now can't clobber a newer owner. See
      # Fathom.Shard.Fence.
      case Fence.check(fence_ctx(state)) do
        {:ok, _updates} ->
          case upload_for_drop(state) do
            {:ok, _new_etag} ->
              drop_local(state.path)
              Storage.release_lease(state.id, state.lease)

            # A stealer flushed since the fence check — don't clobber. We're stopping, so drop
            # the local copy (the new owner is authoritative) without writing over it (#15).
            {:error, :superseded} ->
              Logger.warning(
                "shard #{state.id}: object superseded before flush; dropping local without clobbering"
              )

              drop_local(state.path)

            {:error, reason} ->
              Logger.warning(
                "shard #{state.id}: flush failed, keeping local copy (#{inspect(reason)})"
              )
          end

        :superseded ->
          Logger.warning(
            "shard #{state.id}: lost lease before flush; dropping local without flushing"
          )

          drop_local(state.path)

        :skip ->
          Logger.warning(
            "shard #{state.id}: ownership unconfirmed before flush; keeping local copy"
          )
      end
    end

    :ok
  end

  # Upload the shard for a drop. The cheap path folds the WAL into the main file in
  # place (checkpoint) and uploads it. When the checkpoint can't complete — a
  # lingering reader's mark blocks TRUNCATE (e.g. a brutally-killed stream's
  # connection awaiting NIF-resource GC), or the open fails under fd pressure —
  # uploading just the main file would silently miss the committed WAL frames that
  # drop_local/1 is about to delete, so fall back to a VACUUM INTO snapshot, which
  # captures WAL content regardless.
  defp upload_for_drop(state) do
    case checkpoint(state.path) do
      :ok ->
        Storage.flush(state.id, state.path, state.etag)

      {:error, reason} ->
        Logger.warning(
          "shard #{state.id}: pre-drop checkpoint incomplete (#{inspect(reason)}); snapshot-flushing instead"
        )

        snapshot_and_upload(state)
    end
  end

  # The fence decision itself lives in Fathom.Shard.Fence (unit-tested there). This
  # projects the coordinator state down to the fields the fence reads; the caller
  # merges the returned `updates` (a possibly-refreshed lease/generation) back in.
  defp fence_ctx(state),
    do: %{id: state.id, lease: state.lease, ttl_ms: state.ttl_ms, acquire_gen: state.acquire_gen}

  # The shard is dirty — holds writes not yet flushed to storage — iff its write counter has
  # advanced past the last successfully-flushed watermark (finding #27). Lock-free ETS read.
  defp unflushed?(state), do: WriteCounter.count(state.id) > state.flushed_through

  # Seed the flushed watermark at open. A cold pull is clean (local == storage) ⇒ watermark ==
  # count ⇒ not dirty. A warm restart may hold un-flushed local writes (incl. after a crash where
  # the ETS row survived) ⇒ bump the counter one ahead of the seeded watermark ⇒ dirty (matches the
  # old `dirty: warm?`). The bump runs only from this coordinator during init, before any stream is
  # checked out, so it can't race a real write.
  defp init_flushed_through(shard_id, warm?) do
    n = WriteCounter.count(shard_id)
    if warm?, do: WriteCounter.bump(shard_id)
    n
  end

  # Fold the WAL into the main file so a single object captures the whole shard.
  # `wal_checkpoint(TRUNCATE)` reports "couldn't fold the WAL" as data, not an
  # error — a `(busy, log, checkpointed)` row with busy=1 — so only a 0-busy row
  # counts as success. The busy wait is shortened below: on a blocked checkpoint
  # the caller snapshot-flushes instead, so burning the shutdown budget waiting on
  # a zombie reader buys nothing.
  defp checkpoint(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        Connection.exec(conn, "PRAGMA busy_timeout=1000")
        result = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
        Connection.close(conn)

        case result do
          {:ok, %{rows: [[0, _log, _checkpointed]]}} -> :ok
          {:ok, %{rows: [[busy, _log, _checkpointed]]}} -> {:error, {:checkpoint_busy, busy}}
          {:ok, other} -> {:error, {:checkpoint_unexpected, other}}
          {:error, reason} -> {:error, {:checkpoint_failed, reason}}
        end

      other ->
        {:error, {:checkpoint_open_failed, other}}
    end
  end

  defp drop_local(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))

  # Snapshot the live DB to a temp file and upload it (fenced by the object etag), keeping the
  # working copy. Returns `{:ok, new_etag}` when both the snapshot and the conditional upload
  # land, `{:error, :superseded}` when the object changed under us (a stealer flushed — the
  # caller self-fences), or `{:error, reason}` on a transient failure (the caller keeps the
  # shard dirty so the write isn't lost on a later idle-drop).
  defp snapshot_and_upload(state) do
    temp = "#{state.path}.snap.#{System.unique_integer([:positive])}"

    try do
      with :ok <- snapshot(state.path, temp),
           {:ok, new_etag} <- Storage.flush(state.id, temp, state.etag) do
        {:ok, new_etag}
      else
        {:error, :superseded} = superseded ->
          superseded

        {:error, reason} = error ->
          Logger.warning(
            "shard #{state.id}: durability flush failed (#{inspect(reason)}); keeping dirty"
          )

          error
      end
    after
      Enum.each(["", "-wal", "-shm"], &File.rm(temp <> &1))
    end
  end

  # A transactionally-consistent copy of the live shard, safe to run while writers
  # are active (unlike a raw file copy). `VACUUM INTO` requires a string literal
  # path, so the destination (built from a validated shard id) is single-quote
  # escaped defensively.
  defp snapshot(path, dest) do
    with {:ok, conn} <- Connection.open(path) do
      result = Connection.query(conn, "VACUUM INTO '#{String.replace(dest, "'", "''")}'", [])
      Connection.close(conn)

      case result do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  # --- helpers ---

  # Public for Fathom.Shards' novel-shard check (a present local file means the shard
  # exists — an authoritative un-flushed copy — so it is never "novel").
  @doc false
  def db_path(id), do: Path.join(data_dir(), "#{id}.db")

  defp data_dir do
    Application.get_env(
      :fathom,
      :shard_data_dir,
      Path.join(System.tmp_dir!(), "fathom_shards")
    )
  end

  defp idle_ms, do: Application.get_env(:fathom, :shard_idle_ms, @default_idle_ms)
  defp lease_ttl_ms, do: Application.get_env(:fathom, :shard_lease_ttl_ms, @default_lease_ttl_ms)

  defp checkout_timeout,
    do: Application.get_env(:fathom, :shard_checkout_timeout_ms, @default_checkout_timeout)

  # --- telemetry (see Fathom.Telemetry) ---

  # Lease lifecycle counters: `op` is :acquired | :held | :renewed | :superseded. The renewal
  # stream is the per-node S3 lease RPS; :superseded is the self-fence rate (split-brain signal).
  defp emit_lease(op, shard_id, meta \\ %{}) do
    :telemetry.execute(
      [:fathom, :shard, :lease, op],
      %{count: 1},
      Map.put(meta, :shard_id, shard_id)
    )
  end

  # Warm-promotion outcome at cold-open: `:hit` = the follower cache was
  # storage-current and promoted without a full transfer (the warm-standby win);
  # `:stale` = it lagged the owner's latest flush and was re-pulled fresh. Only fires
  # when a validatable warm copy existed.
  defp emit_warm(shard_id, result) do
    :telemetry.execute(
      [:fathom, :shard, :warm, :promoted],
      %{count: 1},
      %{shard_id: shard_id, result: result}
    )
  end

  # Cold-open latency (native time units; Telemetry.Metrics converts). Only a successful open
  # ({:noreply, _}) has a meaningful cold-open duration; a refused/failed start does not.
  defp maybe_emit_cold_open({:noreply, _state}, shard_id, warm?, started) do
    :telemetry.execute(
      [:fathom, :shard, :cold_open],
      %{duration: System.monotonic_time() - started},
      %{shard_id: shard_id, warm: warm?}
    )
  end

  defp maybe_emit_cold_open(_result, _shard_id, _warm?, _started), do: :ok

  defp flush_interval_ms,
    do: Application.get_env(:fathom, :shard_flush_interval_ms, @default_flush_interval_ms)

  defp schedule_idle(state) do
    state = cancel_idle(state)
    %{state | timer: Process.send_after(self(), :idle_timeout, state.idle_ms)}
  end

  defp cancel_idle(%{timer: nil} = state), do: state

  defp cancel_idle(%{timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | timer: nil}
  end

  # Renew well inside the TTL (every third) so a couple of transient failures
  # don't lapse the lease.
  defp schedule_renew(state) do
    state = cancel_renew(state)
    interval = max(div(state.ttl_ms, 3), 1)
    %{state | renew_timer: Process.send_after(self(), :renew_lease, interval)}
  end

  defp cancel_renew(%{renew_timer: nil} = state), do: state

  defp cancel_renew(%{renew_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | renew_timer: nil}
  end

  # Arm the periodic durability flush. A non-positive interval disables it
  # (idle-only durability).
  defp schedule_flush(state) do
    state = cancel_flush(state)

    case flush_interval_ms() do
      interval when is_integer(interval) and interval > 0 ->
        %{state | flush_timer: Process.send_after(self(), :durability_flush, interval)}

      _ ->
        state
    end
  end

  defp cancel_flush(%{flush_timer: nil} = state), do: state

  defp cancel_flush(%{flush_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | flush_timer: nil}
  end
end
