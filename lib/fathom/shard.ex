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

  alias Fathom.Admin.FlushWatermark
  alias Fathom.Shard.{Connection, Fence, Heartbeat, Storage, WarmFollower, WriteCounter}

  @default_idle_ms 60_000
  @default_lease_ttl_ms 30_000
  @default_flush_interval_ms 30_000
  @pull_timeout 60_000
  # Supervisor shutdown budget for terminate/2's WHOLE exit path: settling any
  # in-flight durability-flush task (settle_yield_ms/0 caps that at a third of this
  # budget — expert review #18) and then the final fence + checkpoint + full-file
  # S3 PUT + lease release. The worker default (5 000 ms) brutal-killed the flush
  # mid-PUT on rolling deploys — a multi-MB shard at real S3 latency, queued behind
  # every sibling coordinator flushing through the same Finch pool, easily exceeds
  # 5 s — silently defeating the clean-shutdown durability guarantee trap_exit exists
  # for. Bounded (not :infinity) so a hung storage call can't wedge the deploy; the
  # in-flight PUT it may still cut off is etag-conditional, so never torn. When
  # tuning `:shard_shutdown_ms`, size it to cover settle + drop-flush together.
  @default_shutdown_ms 60_000

  # Overrides `use GenServer`'s generated child_spec to attach the shutdown budget
  # (config-tunable: bigger shards / slower links warrant more).
  @doc false
  def child_spec(arg) do
    arg
    |> super()
    |> Map.put(:shutdown, Application.get_env(:fathom, :shard_shutdown_ms, @default_shutdown_ms))
  end

  # How long the terminate path waits for an in-flight durability-flush task before
  # starting its own drop-flush. A flat 30 s yield could eat over half the 60 s
  # shutdown budget BEFORE the drop-flush (fence RTT + checkpoint + full-file PUT +
  # lease release) even began — on a rolling deploy every coordinator settles and
  # flushes through one Finch pool at once, reintroducing the #5 brutal-kill-mid-PUT
  # (expert review round-2 #18). Cap the settle at a third of the configured budget so
  # at least two thirds always remain for the drop-flush itself.
  @doc false
  def settle_yield_ms do
    shutdown_ms = Application.get_env(:fathom, :shard_shutdown_ms, @default_shutdown_ms)
    min(30_000, div(shutdown_ms, 3))
  end

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
    # The op tag makes a later abandon PRECISE (expert review round-2 #33): it names
    # exactly this call's grant, so abandoning a timed-out checkout can never drop a
    # LIVE grant the same caller holds from an earlier checkout of the same shard
    # (which would let a flush run under an active writer — the #8 hazard).
    do_checkout(pid, make_ref())
  end

  defp do_checkout(pid, op) do
    # The open path (handle_continue) can legitimately block a queued :checkout for up to
    # @pull_timeout (a large cold pull, cross-region S3), and an inline durability flush can
    # add more. The default 5s GenServer.call timeout is far below that, so a slow-but-normal
    # cold open made the FIRST checkout fail with {:error, :timeout} on the headline path —
    # and the coordinator later processed the stale call, registering a phantom connection
    # for a caller that had already given up. Give the call a budget above the coordinator's
    # own open budget; the coordinator still bounds the pull with @pull_timeout, so this
    # never waits forever on a genuinely stuck open. Configurable for real-S3 tuning.
    GenServer.call(pid, {:checkout, op}, checkout_timeout())
  catch
    # The coordinator self-stopped while opening storage (lease held by another
    # node, or pull failed) — its `{:shutdown, reason}` exit reaches the pending
    # call. Surface the reason as a clean error rather than crashing the caller.
    :exit, {{:shutdown, reason}, _} ->
      {:error, reason}

    :exit, {:noproc, _} ->
      {:error, :unavailable}

    # The call timed out but the :checkout message is still in the coordinator's
    # mailbox (expert review #26): when it is later dequeued, the coordinator would
    # register a phantom connection for this caller — the late reply is dropped by the
    # call alias, no checkin ever arrives, and the monitor only fires if the CALLER
    # dies (a Filo stream or Oban runner can live for hours). The shard then never
    # idles: pinned open, lease held forever, drains aborting :busy. Compensate with a
    # cast that is FIFO-ordered behind the stale :checkout, telling the coordinator to
    # drop exactly THIS call's grant (round-2 #33: by op tag, never by pid alone).
    :exit, {:timeout, _} ->
      GenServer.cast(pid, {:abandon_checkout, self(), op})
      {:error, :timeout}

    :exit, {reason, _} ->
      {:error, reason}
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

    # Reap THIS shard's orphaned temps from externally-killed pulls/snapshots
    # (expert review round-2 #27) — uniquely-named `.dl.*`/`.snap.*` files no
    # fixed-suffix sweeper matches, an unbounded disk leak. Age-gated well past the
    # pull timeout so a concurrent task's live temp is never touched; a previous
    # coordinator for this shard is necessarily dead (registry-unique), so its
    # stale temps are provably garbage.
    reaped = Storage.reap_stale_temps(path, 2 * @pull_timeout)
    if reaped > 0, do: Logger.info("shard #{shard_id}: reaped #{reaped} orphaned temp(s)")
    # The wildcard's readdir garbage (O(data-dir entries) of binaries) would
    # otherwise sit on this LONG-LIVED coordinator's heap until its next natural
    # GC — measured as +68% fanout_kb_per_shard at 1000 open shards. Collect it
    # now, while the heap holds nothing else worth keeping.
    :erlang.garbage_collect()
    # node()#<boot nonce> — boot-scoped lease identity (expert review #6): a lock
    # left by a previous incarnation of this node name is a FOREIGN owner, so it is
    # stolen (liveness check + epoch bump), never silently reclaimed at the same
    # epoch, and the epoch stays a real fencing token across incarnations.
    owner = Heartbeat.owner()
    ttl = lease_ttl_ms()

    # Overlap the lease acquire with the storage pull to cut the cold-open toward
    # one round-trip (they hit independent objects: `.lock` vs `.db`). Fence-first
    # is preserved where it matters — we still only SERVE after the lease confirms.
    # The pull writes to a TEMP file and is promoted to the real path only once the
    # lease is ours, so losing the lease race never leaves a stale local copy that a
    # later open would wrongly treat as authoritative (a present local copy wins).
    # A present local copy means a warm restart (may hold un-flushed writes) — skip
    # the pull entirely.
    # Expert review #1: a present local file is only authoritative if it derives from
    # the stored object's current lineage. A crashed node whose shard was stolen,
    # written, and released elsewhere still has its old file — quarantine it (and
    # re-pull) rather than adopt the store's current etag and clobber the newer
    # lineage with a stale fork.
    warm? = File.exists?(path) and not quarantined_fork?(shard_id, path)
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
    with {:ok, etag0} <- await_pull(pull_task, path, shard_id),
         {:ok, etag} <- revalidate_takeover(shard_id, path, lease, etag0, warm?) do
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
        # Read the generation BEFORE the count: a WriteCounter restart between the two
        # then leaves a stale generation ⇒ dirty (the safe direction), never a fresh
        # generation legitimizing a dead table's watermark (expert review #11).
        counter_gen: WriteCounter.generation(),
        flushed_through: init_flushed_through(shard_id, warm?),
        flush_timer: nil,
        # The in-flight off-process durability flush (expert review #27), the watermark
        # it will advance flushed_through to on success, and the counter generation that
        # watermark was captured under (expert review #11 — restoring a watermark from a
        # table that died mid-flight would read clean against the fresh table's counts).
        flush_task: nil,
        flush_pending: nil,
        flush_pending_gen: nil,
        draining: false,
        drain_timer: nil,
        drain_reply_to: nil,
        # A lapse revalidation is scheduled (round-2 #26) — repeat broadcasts
        # coalesce onto the pending jittered timer.
        lapse_revalidate_pending: false
      }

      state = if acquire_gen == nil, do: schedule_renew(state), else: state
      # Heartbeat mode: subscribe to lapse broadcasts so a steal window is
      # revalidated proactively (expert review #34), not just lazily at the next
      # flush. Legacy mode revalidates via its own per-shard renewals.
      if acquire_gen != nil, do: subscribe_lapse()
      # Publish the initial flush watermark so the metrics layer can derive RPO/dirtiness for
      # this shard without a per-coordinator GenServer call (Fathom.Admin.FlushWatermark).
      FlushWatermark.record(state.id, state.flushed_through, state.counter_gen)
      {:noreply, schedule_flush(state)}
    else
      {:error, reason} ->
        # Couldn't make the file available — give the lease back so another node
        # can try, rather than holding a shard we can't serve.
        Storage.release_lease(shard_id, lease)
        {:stop, {:shutdown, {:pull_failed, reason}}, %{id: shard_id}}
    end
  end

  # Expert review #3: a TAKEOVER's speculative pull (started before the lease
  # confirmed) can capture bytes from before the old owner's final in-flight flush
  # landed — and the steal touched the object's etag, so serving/fencing with the
  # pulled etag would mean stale reads followed by our own first flush self-fencing.
  # Re-check the current etag once (only on takeovers — a fresh epoch-1 create has no
  # prior owner, so the cold-open fast path pays nothing): a cold pull that mismatches
  # is re-pulled; a warm takeover adopts the post-touch etag (the #1 provenance check
  # already established the local file continues the stored lineage, and the touch
  # moved no bytes).
  defp revalidate_takeover(shard_id, path, lease, etag, warm?) do
    cond do
      # Round-2 #19 (merged M15): a NON-takeover warm open must ALSO re-check its
      # provenance AFTER the lease is held. quarantined_fork?'s HEAD runs before
      # the acquire, and in that gap another owner can do a full
      # acquire→flush→release cycle — release deletes the lock, so our acquire is
      # a fresh epoch-1 create with no took_over, and the un-revalidated warm file
      # would serve a STALE lineage (then lose its first-cycle accepted writes at
      # the self-fence). One HEAD on the warm path converts this into an ordinary
      # quarantine. Cold non-takeover opens pay nothing.
      lease[:took_over] != true ->
        if warm?, do: post_lease_warm_check(shard_id, path, etag), else: {:ok, etag}

      # The S3 steal-touch threaded its lineage through the lease (round-2 #6):
      # no extra HEAD (a separate read widened the laundering window), and the
      # warm branch adopts the post-touch etag ONLY when the touch's SOURCE was
      # this file's own provenance.
      is_binary(lease[:touch_post_etag]) ->
        revalidate_touched(shard_id, path, lease, etag, warm?)

      # Legacy/Local takeover (no steal-touch — Local's content-hash etags can't
      # rotate without the bytes changing, and it is single-node): the original
      # HEAD-based re-check, including the #29 vanished-object branch.
      true ->
        legacy_revalidate(shard_id, path, etag, warm?)
    end
  end

  defp revalidate_touched(shard_id, path, lease, etag, warm?) do
    post = lease[:touch_post_etag]
    pre = lease[:touch_pre_etag]

    cond do
      # The speculative pull already captured the post-touch object.
      etag == post ->
        {:ok, etag}

      warm? ->
        # Round-2 #6: the old warm branch assumed ANY etag change since the fork
        # check was "our own touch, moved no bytes" and stamped the local file's
        # sidecar with it — FORGED provenance. But the dead owner's in-flight
        # fenced flush can land between the fork check and the touch: the touched
        # object then holds the ZOMBIE's acknowledged durable writes while the
        # local warm file is a diverged lineage, and the forged sidecar let the
        # next flush If-Match-clobber them unrecoverably. Adopt the post-touch
        # etag ONLY when the touch's SOURCE (`pre`) is this file's own
        # provenance; anything else is a fork — quarantine and re-pull, the #1
        # contract.
        case read_etag_sidecar(path) do
          {:ok, provenance} when is_binary(pre) and provenance == pre ->
            write_etag_sidecar(path, post)
            {:ok, post}

          _ ->
            if quarantine_fork!(shard_id, path, :diverged) == :ok do
              repull(shard_id, path)
            else
              # The quarantine rename failed — never re-pull over the un-moved
              # recovery copy (round-2 #14).
              {:error, :quarantine_failed}
            end
        end

      true ->
        # A cold pull that raced the touch captured pre-touch bytes: re-pull.
        repull(shard_id, path)
    end
  end

  defp legacy_revalidate(shard_id, path, etag, warm?) do
    case Storage.object_etag(shard_id) do
      {:ok, ^etag} ->
        {:ok, etag}

      # The object VANISHED between the pull and this re-check (expert review
      # round-2 #29). Returning the stale pulled etag made the first flush
      # deterministically self-fence (If-Match against a gone object never
      # succeeds), dropping every write accepted in that cycle. Adopt the
      # brand-new contract instead — same stance as quarantined_fork?'s
      # deliberately-deleted-object case: serve the pulled copy and fence with
      # nil, so the first flush RECREATES the object via If-None-Match:* (never a
      # clobber). Drop the now-wrong provenance sidecar so a crash before that
      # flush doesn't strand the next open fencing on the vanished etag.
      {:ok, nil} ->
        File.rm(etag_sidecar(path))
        {:ok, nil}

      {:ok, current} when warm? ->
        write_etag_sidecar(path, current)
        {:ok, current}

      {:ok, _newer} ->
        repull(shard_id, path)

      {:error, reason} ->
        {:error, {:revalidate_failed, reason}}
    end
  end

  defp repull(shard_id, path) do
    case Storage.pull(shard_id, pull_temp(path)) do
      {:ok, new_etag} -> promote_pull(path, new_etag)
      {:error, reason} -> {:error, {:revalidate_failed, reason}}
    end
  end

  # See revalidate_takeover's #19 branch: the post-lease authority check for a
  # warm, non-takeover open.
  defp post_lease_warm_check(shard_id, path, etag) do
    case Storage.object_etag(shard_id) do
      # Unchanged since the pre-lease provenance check — the common warm restart.
      {:ok, ^etag} ->
        {:ok, etag}

      # Object gone with a live local copy: the un-flushed brand-new stance — serve
      # warm, fence with nil so the first flush RECREATES it (an If-Match against a
      # gone object could never succeed), and drop the dangling sidecar.
      {:ok, nil} ->
        File.rm(etag_sidecar(path))
        {:ok, nil}

      # The release-in-gap fork (#19): another owner acquired, flushed, and
      # released between our fork check and our acquire. The stored lineage moved
      # past our local copy — quarantine it (#1 contract, honoring #14's
      # failed-rename rule) and serve the store's current bytes.
      {:ok, _moved} ->
        if quarantine_fork!(shard_id, path, :diverged) == :ok do
          repull(shard_id, path)
        else
          {:error, :quarantine_failed}
        end

      # Unreachable store: keep availability, fenced by the provenance etag — a
      # genuinely-forked flush 412s rather than clobbers.
      {:error, _unreachable} ->
        {:ok, etag}
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
        # Identity of the cache file the sidecar `etag` describes, captured BEFORE the
        # freshness check — the promotion re-checks it after the copy (expert review #13).
        pre_stat = warm_cache_stat(shard_id)

        case Storage.pull_if_changed(shard_id, temp, etag) do
          {:ok, :unchanged} ->
            emit_warm(shard_id, :hit)
            # 304 ⇒ the cache equals storage's current object, so `etag` IS the current etag.
            case promote_warm_cache(shard_id, temp, etag, pre_stat) do
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
  #
  # TOCTOU (expert review #13): between our 304 and this copy, the follower's poll can
  # atomically swap FRESHER bytes into the cache path (a dying old owner's final flush,
  # landed after our freshness check). The cp would then copy the newer bytes while the
  # coordinator records the OLDER etag as provenance — its first fenced flush 412s
  # against an object that is effectively its own lineage, discarding acknowledged
  # post-open writes with no competing owner. So after the copy, require the cache file
  # to still be the very inode we validated (the follower's atomic_write promotion
  # always replaces the inode) AND its sidecar to still name `etag`; any doubt falls
  # back to a fresh cold pull (spurious transfer, never wrong provenance).
  defp promote_warm_cache(shard_id, temp, etag, pre_stat) do
    with :ok <- File.cp(WarmFollower.cache_path(shard_id), temp),
         true <- pre_stat != nil and warm_cache_stat(shard_id) == pre_stat,
         ^etag <- WarmFollower.cached_etag(shard_id) do
      :ok
    else
      _ -> Storage.pull(shard_id, temp)
    end
  end

  # Identity triple for the follower's cache file. The inode is the load-bearing part:
  # a swapped-in fresh pull is a rename, which always changes it (mtime/size guard the
  # exotic in-place rewrite).
  defp warm_cache_stat(shard_id) do
    case File.stat(WarmFollower.cache_path(shard_id), time: :posix) do
      {:ok, %File.Stat{inode: inode, mtime: mtime, size: size}} -> {inode, mtime, size}
      {:error, _} -> nil
    end
  end

  # Resolve the shard's current object etag for the flush fence (#15), returning
  # `{:ok, etag_or_nil}` or `{:error, reason}`. `nil` task ⇒ a local copy already existed
  # (warm restart): no pull ran, so fetch the etag with a HEAD so the first fenced flush can
  # If-Match it. Otherwise await the speculative pull and promote its temp into place.
  defp await_pull(nil, path, shard_id) do
    # Warm restart: fence with the local copy's PROVENANCE etag (the sidecar written
    # at pull/flush time), never "whatever the store holds now" (expert review #1) —
    # adopting the current etag let a stale fork flush over a newer lineage with a
    # valid If-Match. quarantined_fork?/2 already re-pulled a mismatched copy when the
    # store was reachable, so this normally equals the current etag; when the store
    # was unreachable at open, fencing with the provenance etag makes a forked flush
    # 412 (self-fence) instead of clobbering. A missing sidecar is a legacy warm file
    # from before provenance tracking: fall back to adopting the current etag, once.
    case read_etag_sidecar(path) do
      {:ok, etag} ->
        {:ok, etag}

      # Normally unreachable — quarantined_fork?/2 already quarantined a corrupt
      # sidecar before warm? could hold — but if it ever surfaces (the quarantine's
      # sidecar rm failed), NEVER adopt-current off unknown provenance (expert
      # review #12); fail the open instead.
      :corrupt ->
        {:error, :sidecar_corrupt}

      :missing ->
        Logger.warning(
          "shard #{shard_id}: warm file has no provenance sidecar (legacy); adopting current etag"
        )

        case Storage.object_etag(shard_id) do
          {:ok, etag} -> {:ok, etag}
          {:error, reason} -> {:error, {:etag_unavailable, reason}}
        end
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

    if File.exists?(temp) do
      # Remove any stale sidecars BEFORE the pulled file lands (expert review #18): a
      # crash between drop_local's two File.rm calls (db deleted, -wal not yet) leaves
      # an orphan WAL, and SQLite's first open would run WAL recovery against it —
      # replaying a different generation's frames into the freshly pulled database
      # (resurrected deletes, torn pages, or a malformed db, then flushed back as the
      # durable object). A pulled object is always a self-contained checkpointed
      # image, so any sidecars next to it are by definition stale.
      File.rm(path <> "-wal")
      File.rm(path <> "-shm")

      case File.rename(temp, path) do
        :ok ->
          # Record the pulled object's etag as the local copy's provenance (expert
          # review #1). Best-effort: a failed sidecar write degrades to the legacy
          # adopt-current warm path, never fails the open.
          write_etag_sidecar(path, etag)
          {:ok, etag}

        {:error, _} = err ->
          err
      end
    else
      # Brand-new shard: no object, no local file — no provenance either.
      File.rm(etag_sidecar(path))
      {:ok, etag}
    end
  end

  # Lease lost / errored: kill the speculative pull and drop its temp file.
  defp abandon_pull(nil, _path), do: :ok

  defp abandon_pull(task, path) do
    Task.shutdown(task, :brutal_kill)
    rm_pull_temp(path)
    :ok
  end

  # Best-effort, mirroring Heartbeat.broadcast_lapse: a missing PubSub (the
  # scale/bench harness) must not fail an open — the flush-time fence still guards.
  defp subscribe_lapse do
    Phoenix.PubSub.subscribe(Fathom.PubSub, Heartbeat.topic())
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp pull_temp(path), do: path <> ".pull"
  defp rm_pull_temp(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(pull_temp(path) <> &1))

  # While standing down, refuse new checkouts so the shard can drain. The caller
  # retries elsewhere (and, once the migrator holds the lease, lands on :held).
  @impl true
  def handle_call({:checkout, _op}, _from, %{draining: true} = state) do
    {:reply, {:error, :draining}, state}
  end

  def handle_call({:checkout, op}, {caller, _tag}, state) do
    ref = Process.monitor(caller)
    # The value carries the caller AND the call's op tag, so an abandon names
    # exactly one grant (round-2 #33) instead of every grant this pid holds.
    state = %{cancel_idle(state) | conns: Map.put(state.conns, ref, {caller, op})}
    {:reply, {:ok, ref, state.path}, state}
  end

  def handle_call(:dirty?, _from, state), do: {:reply, unflushed?(state), state}

  @impl true
  def handle_cast({:checkin, ref}, state), do: stop_when_drained(release(state, ref))

  # A caller's checkout call timed out (expert review #26): drop the conn granted to
  # it that it never received. Ordering makes this exact: the cast is sent AFTER the
  # call gave up, so per-pair FIFO puts it BEHIND the stale :checkout — the phantom
  # grant (if the coordinator processed it) is always visible here. Matching on the
  # call's op TAG (round-2 #33), not the pid alone: a caller that checks the same
  # shard out twice must not lose its LIVE grant to the timed-out one's abandon —
  # dropping a grant under an active writer re-opens the #8 flush-under-writer
  # hazard the conn tracking exists to prevent.
  def handle_cast({:abandon_checkout, caller, op}, state) do
    refs = for {ref, {pid, tag}} <- state.conns, pid == caller and tag == op, do: ref
    stop_when_drained(Enum.reduce(refs, state, &release(&2, &1)))
  end

  # Begin standing down. With no connections we stop immediately (terminate
  # flushes + releases). Otherwise wait for the drain, bounded by a timer.
  def handle_cast({:drain, _timeout, _reply_to}, %{conns: conns} = state)
      when map_size(conns) == 0 do
    {:stop, :normal, state}
  end

  # A drain is already in progress (expert review #29): refuse the second one
  # immediately rather than overwriting the first's timer/reply_to. Overwriting
  # leaked the FIRST timer live — when it fired it aborted the SECOND drain long
  # before its own window and stranded the first caller on its 30 s safety net —
  # and a leaked timer surviving into a resumed-serving state could stop an
  # idle-but-serving shard early.
  def handle_cast({:drain, _timeout, reply_to}, %{draining: true} = state) do
    if reply_to, do: send(reply_to, {:drain_aborted, self()})
    {:noreply, state}
  end

  def handle_cast({:drain, timeout, reply_to}, state) do
    timer = Process.send_after(self(), :drain_timeout, timeout)

    {:noreply,
     %{cancel_idle(state) | draining: true, drain_timer: timer, drain_reply_to: reply_to}}
  end

  # The in-flight durability-flush task's reply (expert review #27). Matched BEFORE
  # the generic :DOWN clause below so the task's monitor is never mistaken for a
  # connection release.
  @impl true
  def handle_info({ref, result}, %{flush_task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | flush_task: nil}

    case result do
      # Uploaded; advance the fence etag, the provenance sidecar, and the flushed
      # watermark captured when the task started (clears dirty up to that point).
      {:ok, new_etag} ->
        write_etag_sidecar(state.path, new_etag)

        state = %{
          state
          | flushed_through: state.flush_pending,
            counter_gen: state.flush_pending_gen,
            etag: new_etag
        }

        # Advance the published watermark (clears dirty up to flush_pending for the RPO reader).
        FlushWatermark.record(state.id, state.flushed_through, state.counter_gen)
        {:noreply, schedule_flush(state)}

      # The data PUT's If-Match failed (412). The OBJECT changed — but a 412 is NOT
      # proof of a STEAL (expert review #2): our own PUT can land server-side while its
      # response is lost (Req retries the idempotent PUT, and the retry 412s against our
      # first attempt's own write), and the next flush then fences with our stale etag
      # and 412s against our own durable bytes. Only a real steal bumps the LOCK, so
      # re-check it before self-fencing off a shard we still own and dropping
      # acknowledged writes unrecoverably.
      {:error, :superseded} ->
        case reconcile_superseded(state) do
          {:ok, state} -> {:noreply, schedule_flush(state)}
          :superseded -> {:stop, {:shutdown, :lease_lost}, %{state | lease_lost: true}}
        end

      # Snapshot/PUT failed transiently: don't advance the watermark, so it stays
      # dirty and the next interval retries — and a later idle-drop flushes before
      # dropping instead of deleting un-stored writes.
      {:error, _reason} ->
        {:noreply, schedule_flush(state)}
    end
  end

  # The flush task crashed before replying: treat like a transient flush failure.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{flush_task: %Task{ref: ref}} = state) do
    Logger.warning("shard #{state.id}: durability flush task crashed (#{inspect(reason)})")
    {:noreply, schedule_flush(%{state | flush_task: nil})}
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

  # A stale timer firing after the drain already resolved (resume_serving cancelled
  # it, but a fire could already be in flight) must be inert — pre-guard it could
  # stop an idle-but-serving shard early (expert review #29).
  def handle_info(:drain_timeout, %{draining: false} = state), do: {:noreply, state}

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

  # The WriteCounter's table died with its owner and restarted empty (expert review
  # #14): every count is now 0 while our flushed_through watermark may be ahead, so a
  # dirty shard would compare clean and drop_clean would delete un-flushed writes.
  # Unknown state must flush: force the watermark below any possible count; the next
  # successful flush re-anchors it to the fresh counter. The generation compare in
  # unflushed?/1 (expert review #11) is the primary guard — it holds even when this
  # message can't land (an in-flight flush restoring flush_pending over the -1, or a
  # coordinator already in terminate); this handler remains as the eager belt.
  def handle_info(:write_counter_reset, state) do
    {:noreply, %{state | flushed_through: -1}}
  end

  # Proactive revalidation on a heartbeat lapse (expert review #34): the Heartbeat
  # moduledoc promised coordinators revalidate on the lapse broadcast instead of
  # waiting for their next flush, but nothing subscribed — a superseded coordinator
  # kept ACCEPTING writes it would later discard for up to a full flush interval
  # (unboundedly, with the durability flush disabled).
  #
  # Round-2 #26: the broadcast reaches EVERY open coordinator at the same instant,
  # and revalidating INLINE made it a synchronous O(open-shards) storm — a
  # check_lease GET per shard through one connection pool, fired exactly when the
  # node just proved unhealthy: mailboxes blocked, checkouts queued fleet-wide.
  # Defer behind a per-shard random jitter instead, coalescing repeat lapses onto
  # the one pending timer (the handler re-reads the CURRENT generation via the
  # fence when it fires, so a coalesced later lapse is still covered). The flush
  # fence remains the hard guard throughout the jitter window.
  def handle_info({:heartbeat_lapsed, gen}, state) do
    if state.acquire_gen != nil and gen != state.acquire_gen and
         not state.lapse_revalidate_pending do
      Process.send_after(self(), :revalidate_lapse, :rand.uniform(lapse_jitter_ms()))
      {:noreply, %{state | lapse_revalidate_pending: true}}
    else
      {:noreply, state}
    end
  end

  def handle_info(:revalidate_lapse, state) do
    state = %{state | lapse_revalidate_pending: false}

    case Fence.check(fence_ctx(state)) do
      {:ok, updates} ->
        {:noreply, Map.merge(state, updates)}

      :superseded ->
        Logger.error(
          "shard #{state.id}: lease superseded (heartbeat lapse broadcast); self-fencing"
        )

        {:stop, {:shutdown, :lease_lost}, %{state | lease_lost: true}}

      # Ownership unconfirmed (transient) — the next flush's fence remains the guard.
      :skip ->
        {:noreply, state}
    end
  end

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
  #
  # The snapshot + upload run OFF-PROCESS in a monitored task (expert review #27):
  # inline, the fence RTT + full-shard VACUUM INTO + full-object PUT blocked the
  # coordinator's mailbox every interval, so every :checkout / :checkin / :DOWN for
  # the shard queued behind seconds of I/O — recurring p99 checkout spikes on
  # exactly the hot (write-active) shards, the same blocking pattern the cold-open
  # path was already restructured to avoid. The watermark design already tolerates
  # concurrent writes during the snapshot; the fence check and watermark capture
  # stay in the coordinator, the result is applied when the task replies, and at
  # most one flush task is in flight.
  def handle_info(:durability_flush, state) do
    cond do
      # A flush is already in flight — don't stack a second snapshot/PUT; the next
      # interval retries anything the in-flight one doesn't cover.
      state.flush_task != nil ->
        {:noreply, schedule_flush(state)}

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
            # Capture the write count BEFORE snapshotting: a write landing during the
            # snapshot/upload bumps the counter past this, so the shard stays dirty and
            # re-flushes next interval — never silently cleared (findings #1/#2/#27).
            # Generation before count, same reasoning as init (expert review #11).
            pending_gen = WriteCounter.generation()
            flushed_to = WriteCounter.count(state.id)
            snapshot_state = Map.take(state, [:id, :path, :etag])
            task = Task.async(fn -> snapshot_and_upload(snapshot_state) end)

            {:noreply,
             %{
               state
               | flush_task: task,
                 flush_pending: flushed_to,
                 flush_pending_gen: pending_gen
             }}

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
    _ = settle_flush_task(state)
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    WriteCounter.forget(state.id)
    FlushWatermark.forget(state.id)
    drop_local(state.path)
    :ok
  end

  def terminate(_reason, %{conns: conns} = state) when map_size(conns) == 0 do
    # Settle any in-flight flush task FIRST (expert review #27): its conditional PUT
    # may land at any moment, and the drop-flush below fences on the etag — racing
    # them would make one 412 spuriously (the drop-flush's :superseded branch drops
    # the local copy WITHOUT uploading writes made after the task's snapshot).
    state = settle_flush_task(state)
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    # flush_and_drop reads the write counter (unflushed?/1) to decide whether to upload before
    # dropping, so forget the counter AFTER it — forgetting first would zero the count and make a
    # dirty shard look clean, skipping the flush and losing the writes (findings #1/#27).
    flush_and_drop(state)
    WriteCounter.forget(state.id)
    FlushWatermark.forget(state.id)
    :ok
  end

  # Stopping with connections still open (or from an early pre-open state) — always
  # drop the shard's load row so a stopped shard never leaks a counter.
  def terminate(_reason, state) do
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    WriteCounter.forget(state.id)
    FlushWatermark.forget(state.id)
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

            # A data-PUT 412 (expert review #2): re-check the lock before treating it as
            # a steal. Lock still ours ⇒ our own PUT landed (lost response); the object
            # is durably ours ⇒ drop + release like a clean flush. Genuinely superseded ⇒
            # keep the local copy (NOT drop_local, which is unrecoverable): the next
            # open's provenance-sidecar check (#1) then arbitrates recoverably.
            {:error, :superseded} ->
              case Storage.check_lease(state.id, state.lease) do
                :ok ->
                  Logger.info(
                    "shard #{state.id}: flush 412 but lock still ours; object is durable, dropping"
                  )

                  drop_local(state.path)
                  Storage.release_lease(state.id, state.lease)

                _ ->
                  Logger.warning(
                    "shard #{state.id}: object superseded before flush; keeping local for recovery"
                  )
              end

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

  # Disambiguate a data-PUT 412 during the periodic durability flush (expert review #2).
  # A 412 means the object changed since our fence etag; re-check the LOCK to tell "our
  # own lost-but-applied PUT" (lock still ours — the single writer, so the object is our
  # own bytes) from a real steal (lock superseded). Returns `{:ok, state}` (resync the
  # fence etag to the object's current value and advance the watermark — our writes up
  # to flush_pending are durable — keep serving) or `:superseded` (self-fence). On an
  # unconfirmable lock read, don't self-fence on uncertainty: keep dirty and retry.
  defp reconcile_superseded(state) do
    case Storage.check_lease(state.id, state.lease) do
      :ok ->
        case Storage.object_etag(state.id) do
          {:ok, etag} when not is_nil(etag) ->
            write_etag_sidecar(state.path, etag)

            state = %{
              state
              | etag: etag,
                flushed_through: state.flush_pending,
                counter_gen: state.flush_pending_gen
            }

            FlushWatermark.record(state.id, state.flushed_through, state.counter_gen)
            {:ok, state}

          _ ->
            {:ok, state}
        end

      {:error, :superseded} ->
        :superseded

      {:error, _transient} ->
        {:ok, state}
    end
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
  # The watermark only means anything against the table it was captured from: if the
  # WriteCounter restarted since (fresh empty table, counts near 0), a watermark from the
  # dead table would read clean until that many NEW writes accumulate, and drop_clean would
  # delete un-uploaded writes (expert review #11). Generation mismatch ⇒ unknown ⇒ dirty.
  # This also covers the terminate path, where the reset broadcast can no longer be
  # processed — the generation is read from persistent_term, not the mailbox.
  defp unflushed?(state) do
    state.counter_gen != WriteCounter.generation() or
      WriteCounter.count(state.id) > state.flushed_through
  end

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

  defp drop_local(path), do: Enum.each(["", "-wal", "-shm", ".etag"], &File.rm(path <> &1))

  # --- etag provenance sidecar (expert review #1) ---
  #
  # `<path>.etag` records which stored-object version the live local file derives
  # from — written on every pull promotion and successful flush, removed with the
  # local copy. A warm restart compares it to the store's current etag: equal ⇒ the
  # local file continues the stored lineage (and may hold newer un-flushed writes);
  # different ⇒ the lineages FORKED (another node wrote and released while we were
  # down) and serving or flushing our copy would clobber acknowledged writes.

  defp etag_sidecar(path), do: path <> ".etag"

  defp write_etag_sidecar(_path, nil), do: :ok

  defp write_etag_sidecar(path, etag) do
    # A plain write, deliberately NOT atomic_write: the sidecar has a single writer
    # (this coordinator) and is read only at open, before any writer exists, so no
    # torn CONCURRENT read is possible — and a torn value after a crash merely reads
    # as a mismatch ⇒ a spurious, recoverable quarantine (the safe direction). The
    # temp+rename pattern costs ~5× more (two APFS-journaled metadata ops) on the
    # timed cold-open path.
    case File.write(etag_sidecar(path), etag) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("etag sidecar write failed: #{inspect(reason)}")
    end
  end

  defp read_etag_sidecar(path) do
    case File.read(etag_sidecar(path)) do
      # A zero-length sidecar is a TORN WRITE, not "no provenance" (expert review
      # #12): File.write is O_TRUNC and power loss classically leaves the truncated
      # empty file. Mapping it to :missing fell through to the legacy adopt-current
      # branch — the exact clobber the sidecar exists to prevent. Corrupt routes to
      # quarantine instead (spurious but recoverable, the safe direction).
      {:ok, ""} -> :corrupt
      {:ok, etag} -> {:ok, etag}
      # Only a truly ABSENT sidecar is legacy (pre-provenance warm file).
      {:error, :enoent} -> :missing
      # Unreadable (eacces/eio/...) is unknown provenance, same safe direction.
      {:error, _} -> :corrupt
    end
  end

  # Warm-restart fork detection. true ⇒ the local copy was quarantined (renamed to
  # `<path>.forked*`, preserved for operator recovery) and the open proceeds COLD,
  # pulling the store's current lineage. An unreachable store falls back to serving
  # warm fenced by the provenance etag (a forked flush then 412s and self-fences
  # instead of clobbering).
  defp quarantined_fork?(shard_id, path) do
    case read_etag_sidecar(path) do
      :missing ->
        false

      # Torn/unreadable sidecar (expert review #12): the local copy's provenance is
      # unknown, so it cannot be trusted to continue the stored lineage — quarantine
      # it and re-pull, exactly as a detected fork. A FAILED quarantine (the rename
      # never moved the copy) must not report quarantined, or the cold-open's
      # promote_pull would overwrite the un-moved recovery copy (expert review #14).
      :corrupt ->
        quarantine_fork!(shard_id, path, :corrupt_sidecar) == :ok

      {:ok, sidecar_etag} ->
        case Storage.object_etag(shard_id) do
          {:ok, ^sidecar_etag} ->
            false

          # The object is GONE but we have provenance from one — treat as fork-adjacent?
          # No: a deliberately deleted object with a live local copy is the un-flushed
          # brand-new case; serve warm and let the fenced flush recreate it.
          {:ok, nil} ->
            false

          # Same failed-quarantine rule as :corrupt above (expert review #14).
          {:ok, _newer} ->
            quarantine_fork!(shard_id, path, :diverged) == :ok

          {:error, _unreachable} ->
            false
        end
    end
  end

  # Returns :ok when the local copy was moved aside (the caller opens COLD), or
  # {:error, reason} when the main-file rename failed — the copy never moved, so the
  # caller must NOT report it quarantined (expert review #14: promote_pull's rename
  # would overwrite the recovery copy the quarantine exists to preserve).
  defp quarantine_fork!(shard_id, path, reason) do
    # Unique per quarantine (expert review #14): a fixed `.forked` name + rm-first
    # destroyed the FIRST fork's recovery copy whenever the same shard forked twice —
    # and a crash-looping node in a stolen/written/released environment is exactly
    # where repeat forks happen. A quarantine's sole purpose is preserving
    # acknowledged writes; never delete a prior one.
    dest =
      path <> ".forked.#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"

    # The main-file rename is load-bearing; the WAL/SHM companions are best-effort
    # (promote_pull removes stale ones before landing the pulled object).
    case File.rename(path, dest) do
      :ok ->
        Enum.each(["-wal", "-shm"], &File.rename(path <> &1, dest <> &1))
        File.rm(etag_sidecar(path))

        cause =
          case reason do
            :corrupt_sidecar ->
              "provenance sidecar torn/unreadable (crash mid-write), lineage unknown (expert review #12)"

            :diverged ->
              "another node wrote and released while this one was down (expert review #1)"
          end

        Logger.error(
          "shard #{shard_id}: local copy FORKED from the stored lineage — #{cause}; " <>
            "quarantined at #{dest} and re-pulling. " <>
            "Operator recovery: the forked writes live in that file."
        )

        :telemetry.execute([:fathom, :shard, :forked], %{count: 1}, %{shard_id: shard_id})
        :ok

      {:error, _} = error ->
        Logger.error(
          "shard #{shard_id}: fork quarantine FAILED (#{inspect(error)}); the local copy " <>
            "stays in place and the open serves it warm, fenced by the provenance etag — " <>
            "never overwritten by a re-pull (expert review #14)."
        )

        error
    end
  end

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

  # Wait out an in-flight durability-flush task and fold its result into the state
  # (expert review #27) — used on the terminate path so the final drop-flush never
  # races the task's conditional PUT. A task error/timeout leaves the state as-is
  # (still dirty ⇒ the drop-flush uploads everything itself).
  defp settle_flush_task(%{flush_task: nil} = state), do: state

  defp settle_flush_task(%{flush_task: task} = state) do
    case Task.yield(task, settle_yield_ms()) || Task.shutdown(task) do
      {:ok, {:ok, new_etag}} ->
        write_etag_sidecar(state.path, new_etag)

        %{
          state
          | flush_task: nil,
            flushed_through: state.flush_pending,
            counter_gen: state.flush_pending_gen,
            etag: new_etag
        }

      _ ->
        %{state | flush_task: nil}
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

  # Round-2 #26: spread the post-lapse revalidation fan-out across the jitter window.
  defp lapse_jitter_ms, do: Application.get_env(:fathom, :lapse_revalidate_jitter_ms, 2_000)

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
