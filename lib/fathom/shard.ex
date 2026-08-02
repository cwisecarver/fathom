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

  alias Fathom.Shard.{
    Connection,
    Fence,
    FlushGate,
    Heartbeat,
    Storage,
    WarmFollower,
    WriteCounter
  }

  @default_idle_ms 60_000
  @default_lease_ttl_ms 30_000
  # 5s node-loss RPO for busy shards (write-gated — clean shards skip the PUT). Was 30s;
  # the fathom.rpo sweep confirmed p99 loss ≈ interval (docs/durability.md), so 5s trades
  # ~6× more PUTs on write-active shards for a ~6× tighter loss window.
  @default_flush_interval_ms 5_000
  # Spread each flush timer by ±25% (expert review #17) so a mass re-home's phase-aligned timers
  # drift apart instead of firing N snapshots + PUTs in lockstep every interval. 0 disables.
  @default_flush_jitter_ratio 0.25
  # Short retry after the node-wide flush cap (Fathom.Shard.FlushGate) refused a slot (#17); the
  # shard stayed dirty, so it just re-attempts soon rather than waiting a full interval.
  @default_flush_backoff_ms 250
  @pull_timeout 60_000
  # Synchronous force-flush (`:flush_now`) budget: a checkpoint + fenced full-file PUT (possibly
  # queued behind sibling flushes through the shared Finch pool) at real S3 latency. Generous so a
  # flush-before-fork of a large template doesn't time out; bounded so a hung store can't wedge a caller.
  @flush_now_timeout 60_000
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
  # How much slower a clean-but-served shard polls, given the writer now signals the
  # clean→dirty edge. The safety net for a lost signal (see schedule_flush/1).
  @clean_poll_multiplier 10

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

  # Per-coordinator memory policy (expert review 2026-07-24 #9). At 30k coordinators/node, retained
  # heap dominates per-shard cost — this file's own `init` already relies on that: dropping its
  # one-shot `:erlang.garbage_collect()` measured +53% fanout_kb_per_shard (16.99 → 26.06 KiB). But
  # that shrink was one-time; afterwards ERTS defaults (`fullsweep_after: 65535`, effectively never)
  # let everything surviving two minor collections sit in the old heap for the process's life —
  # flush-cycle terms, Task/monitor bookkeeping, lease churn, Logger metadata. The measured gap
  # between an idle coordinator that never served (~16 KiB) and one that did (~43 KiB) is that
  # unreclaimable residue: ~810 MB/node at 30k.
  #
  # `fullsweep_after: 0` — a coordinator's live set is a small map, so every collection is cheap and
  # a full sweep is the only thing that reclaims the old heap AND returns pages.
  # `hibernate_after` — only useful because #10 stopped the idle 5 s tick; before that a message
  # always arrived first, so any value above the interval never fired and any value below it cost a
  # hibernate/wake pair per tick.
  #
  # `max_heap_size` is deliberately ABSENT and must stay absent: a coordinator killed at the heap
  # limit mid-`terminate/2` skips the final fence + checkpoint + PUT + lease release — a durability
  # regression that can also strand a lease.
  @hibernate_after_ms 30_000

  def start_link(shard_id) do
    GenServer.start_link(__MODULE__, shard_id,
      name: via(shard_id),
      spawn_opt: [fullsweep_after: 0],
      hibernate_after: @hibernate_after_ms
    )
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
  Force-flushes the coordinator's current on-disk state to storage WITHOUT dropping or stopping
  it (it keeps serving) — the flush-before-fork primitive. Blocks until the shard is durably
  clean; returns `:ok`, or `{:error, reason}` on a flush error / lease steal. See
  `Fathom.Shards.flush/1` for the by-id, registry-resolving wrapper.
  """
  def flush_now(pid) when is_pid(pid), do: GenServer.call(pid, :flush_now, flush_now_timeout())

  # Configurable so a test can exercise the timeout path without a 60s wait (expert review #14);
  # defaults to @flush_now_timeout in prod. Off the query hot path (this is the fork-before-flush
  # primitive), so the per-call env read is negligible.
  defp flush_now_timeout,
    do: Application.get_env(:fathom, :flush_now_timeout_ms, @flush_now_timeout)

  @doc """
  Tell the coordinator this shard has gone clean → dirty (expert review 2026-08-01 #42).

  A served-but-clean shard polls its durability flush at a reduced cadence, since a coordinator
  cannot otherwise learn that a stream wrote. `Fathom.ShardExecutor` sends this once per
  checkout, on the first write, which restores the full flush rate. Fire-and-forget: the
  reduced cadence is itself the safety net, so a lost signal costs RPO, never data.
  """
  @spec signal_dirty(pid()) :: :ok
  def signal_dirty(pid) when is_pid(pid), do: GenServer.cast(pid, :became_dirty)

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

    # Clear THIS shard's DETERMINISTIC orphaned pull temps by direct name — O(1), no
    # directory scan (expert review 2026-07-14 #2). The old `Storage.reap_stale_temps/2`
    # here globbed `<base>.{dl,snap,tmp,pull}*`, and `Path.wildcard` can't prefix-optimize
    # a `*`-in-filename pattern, so it full-`readdir`d the fleet-sized shard data dir on
    # EVERY open — tens-to-hundreds of ms at 30k–105k shards, re-paid on every idle
    # re-open. A stale `.pull` would otherwise be adopted by promote_pull/2 as this
    # shard's db, so this direct-name reap is ALSO the pull's collision guard. The
    # uniquely-suffixed orphans (`.dl.*`/`.snap.*`/`.tmp.*` from externally-killed
    # pulls/snapshots — expert review round-2 #27) are swept amortized by
    # `Fathom.Shard.TempReaper`, off this hot path. Age-gated well past the pull timeout
    # so a concurrent task's live temp is never touched; a previous coordinator for this
    # shard is necessarily dead (registry-unique), so its stale `.pull*` are provably garbage.
    reaped =
      Storage.reap_named_temps(
        [path <> ".pull", path <> ".pull-wal", path <> ".pull-shm"],
        2 * @pull_timeout
      )

    if reaped > 0, do: Logger.info("shard #{shard_id}: reaped #{reaped} orphaned pull temp(s)")
    # Force a full-sweep GC on this LONG-LIVED coordinator after the memory-heavy init
    # (GenServer setup + lease acquire + pull). :erlang.garbage_collect/0 also SHRINKS the
    # heap back from its init high-water mark, which would otherwise sit resident until the
    # next natural GC — measured as +53% fanout_kb_per_shard (16.99 → 26.06 KiB/shard) when
    # dropped. (It formerly also collected the per-open wildcard-readdir garbage, now removed
    # in favor of Storage.reap_named_temps/2 above — but the heap-shrink is the load-bearing
    # part at density, independent of that.)
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
    #
    # The fork check's HEAD runs CONCURRENTLY with the lease acquire (review 2026-07-23
    # #22): it ran sequentially before it, so a warm restart paid HEAD → lock PUT →
    # post-lease HEAD = 3 serialized RTTs where a cold open pays ~1. Independent objects
    # (`.db` HEAD vs `.lock` PUT) — the same overlap argument as the acquire∥pull below —
    # and the verdict is consumed at exactly the same point (before any serving decision),
    # so every quarantine/adopt branch is unchanged. The rare diverged-warm case now pulls
    # sequentially after the acquire instead of overlapped — the common warm path drops to
    # 2 RTTs (the post-lease re-check stays: it is the release-in-gap authority, round-2 #19).
    file? = File.exists?(path)
    # Gathers evidence only — the verdict is `resolve_fork/4`, after the lease is held. See
    # `fork_evidence/2`: this HEAD races the steal-touch that `acquire_lease` performs, so a
    # decision made here can mistake our own touch for a fork.
    # Rescued like start_pull/2 (expert review 2026-08-01 #33). fork_evidence/2 does a storage
    # HEAD; unrescued, a raise there became an exit in the awaiting coordinator — and since
    # handle_continue exits with a NON-{:shutdown, _} reason, release_lease never ran and
    # terminate/2 fell to the pre-open clause that has no lease to release. The lock survived,
    # and while this node's heartbeat stayed fresh every peer read {:held, us}.
    fork_task =
      if file? do
        Task.async(fn ->
          try do
            fork_evidence(shard_id, path)
          rescue
            _ -> :unreachable
          catch
            :exit, _ -> :unreachable
          end
        end)
      end

    pull_task = if file?, do: nil, else: start_pull(shard_id, path)

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
        # Consume the overlapped fork-check verdict here — the same pre-serving point the
        # sequential check used. :infinity mirrors the old inline call (the HEAD carries the
        # storage layer's own timeouts). A diverged local copy was quarantined by the task,
        # so this open proceeds cold with a (late, rare-path) pull.
        # The verdict is taken HERE, with the lease in hand, so `touch_pre_etag`/`touch_post_etag`
        # can distinguish our own steal-touch from a genuine fork. Same consumption point as
        # before (before any serving decision) — only the decision moved.
        # BOUNDED (expert review 2026-08-01 #33). This was `Task.await(fork_task, :infinity)` —
        # the one unbounded wait in the open path, while every other blocking step here is
        # bounded (the pull is a `Task.yield(_, @pull_timeout)`, `:checkout` has its own
        # timeout). The task does a storage HEAD, and the S3 backend sets no explicit
        # receive/pool timeout, so the ceiling was a library default rather than a bound fathom
        # owns — and a hung HEAD held the lease open indefinitely.
        #
        # A timeout maps to `:unreachable`, which `resolve_fork/4` already treats as the
        # fail-safe: keep the local copy and serve warm, fenced by the provenance etag, so a
        # forked flush 412s and self-fences rather than clobbering.
        warm? =
          file? and
            not resolve_fork(await_fork_evidence(fork_task), shard_id, path, lease)

        pull_task = pull_task || if warm?, do: nil, else: start_pull(shard_id, path)
        result = open_with_lease(shard_id, path, owner, ttl, lease, pull_task, warm?, acquire_gen)
        maybe_emit_cold_open(result, shard_id, warm?, open_started)
        result

      # `{:shutdown, _}` (not a bare reason) so the `:transient` child is NOT
      # restarted — a held shard must stay down, not restart-loop.
      {:error, {:held, holder}} ->
        emit_lease(:held, shard_id, %{holder: holder})
        abandon_fork_check(fork_task)
        abandon_pull(pull_task, path)
        {:stop, {:shutdown, {:shard_held, holder}}, state}

      # Fail closed: without a confirmed lease we must not open the shard.
      {:error, reason} ->
        abandon_fork_check(fork_task)
        abandon_pull(pull_task, path)
        {:stop, {:shutdown, {:lease_unavailable, reason}}, state}
    end
  end

  # Stop an in-flight overlapped fork check on the no-open paths. Its only side effect
  # (quarantining a diverged copy) is one the sequential pre-lease check also produced
  # before a refused acquire, so completing or aborting mid-flight are both safe.
  defp abandon_fork_check(nil), do: :ok
  defp abandon_fork_check(task), do: Task.shutdown(task, :brutal_kill)

  # Bounded consumption of the overlapped fork-evidence HEAD (expert review 2026-08-01 #33).
  # Anything other than a clean, timely verdict is `:unreachable` — resolve_fork/4's fail-safe.
  defp await_fork_evidence(task) do
    case Task.yield(task, @pull_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, verdict} -> verdict
      _ -> :unreachable
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
        # Monotonic-ms instant the shard last went idle (all connections checked in), or
        # nil while busy — the coalesced idle-timer's comparison stamp (review 2026-07-23
        # #17; see schedule_idle/1).
        idle_since: nil,
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
        # The monotonic-ms instant the periodic fence first came back not-valid (heartbeat stale),
        # or nil while ownership is confirmed. Once it has held for ttl + steal_margin the coordinator
        # publishes the write-fence (Fathom.Shard.WriteFence) so ShardExecutor refuses writes on this
        # provably-stealable node — the RPO circuit-breaker (expert review #3). Cleared the moment a
        # fence confirms ownership again.
        not_valid_since: nil,
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
        # Whether the in-flight flush task reserved a Fathom.Shard.FlushGate slot (expert review
        # #17): true only when the node-wide concurrent-flush cap is enabled and we acquired. The
        # slot is released at every site that clears flush_task (completion, crash, terminate).
        flush_slot_held: false,
        flush_pending: nil,
        flush_pending_gen: nil,
        # Callers of a synchronous force-flush (`:flush_now`, the flush-before-fork primitive):
        # each waits until the shard is durably clean. Replied to (and cleared) when a flush
        # task lands the shard clean, or with `{:error, _}` on a flush error / lease steal.
        flush_waiters: [],
        # Consecutive re-kicks of a still-dirty flush that has waiters (expert review
        # 2026-08-01 #32). Bounds what used to be an unthrottled fence→VACUUM→PUT loop.
        flush_rekicks: 0,
        # The in-flight legacy-mode lease-renewal task (expert review 2026-08-01 #29), so the
        # renew PUT no longer blocks the coordinator mailbox every ttl/3.
        renew_task: nil,
        # Consecutive durability-flush failures (expert review #27): a persistent S3 failure
        # (auth/bucket-policy change) would otherwise only Logger.warning per interval and let the
        # RPO grow unbounded and silently. We count consecutive failures, emit
        # [:fathom, :shard, :flush, :failed] telemetry, and ESCALATE to Logger.error past a
        # threshold so it's alertable. Reset to 0 on any durable flush.
        flush_failures: 0,
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
      # provenance AFTER the lease is held. fork_evidence/2's HEAD runs before
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

      # Expert review #15: the speculative pull captured the object the crash-steal
      # touch used as its If-Match SOURCE (etag == pre). The touch is a self-copy — it
      # rotates the etag but moves no bytes — so the pulled bytes are byte-identical to
      # the post-touch object. Adopt the post-touch etag with NO re-pull (the same
      # provenance argument the warm branch makes via its sidecar), stamping the sidecar
      # to `post` so a later warm restart fences with the store's real etag. This is the
      # warm standby's HEADLINE scenario — a data-bearing crash failover: the survivor's
      # live path is empty (the warm copy lives in the follower cache, so warm? is false),
      # so without this every such failover discarded the promoted/pulled copy for a full
      # body re-pull, the exact cost the warm follower exists to avoid. `pre != post` is
      # guaranteed by confirm_rotation, and `etag == post` was already handled above, so
      # `etag == pre` unambiguously means the pre-touch object. Only reachable on the cold
      # (non-warm) takeover path — the warm clause above catches warm? == true first.
      is_binary(pre) and etag == pre ->
        write_etag_sidecar(path, post)
        {:ok, post}

      true ->
        # A cold pull that raced the touch captured pre-touch bytes from a diverged
        # lineage (etag != pre — a zombie flush landed before the touch): re-pull.
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
      # brand-new contract instead — same stance as fork_evidence/2's
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
      # `promote_pull/2` already distinguishes the two by checking whether the temp exists, so
      # both shapes route through it: bytes ⇒ promote + stamp the object's etag as provenance;
      # no bytes ⇒ the born-empty branch, which stamps the "no object" sentinel sidecar. The
      # fence etag is carried through either way (expert review 2026-08-01 #24).
      {:ok, new_etag} -> promote_pull(path, new_etag)
      {:absent, new_etag} -> promote_pull(path, new_etag)
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
    # valid If-Match. fork_evidence/2 + resolve_fork/4 already re-pulled a mismatched copy when the
    # store was reachable, so this normally equals the current etag; when the store
    # was unreachable at open, fencing with the provenance etag makes a forked flush
    # 412 (self-fence) instead of clobbering. A missing sidecar is a legacy warm file
    # from before provenance tracking: fall back to adopting the current etag, once.
    case read_etag_sidecar(path) do
      {:ok, etag} ->
        {:ok, etag}

      # Normally unreachable — resolve_fork/4 already quarantined a corrupt
      # sidecar before warm? could hold — but if it ever surfaces (the quarantine's
      # sidecar rm failed), NEVER adopt-current off unknown provenance (expert
      # review #12); fail the open instead.
      :corrupt ->
        {:error, :sidecar_corrupt}

      # Born here against no stored object (2026-08-01 #2). Fence with nil, which the storage
      # layer renders as `If-None-Match: *`: the first flush can only CREATE. If a peer created
      # the object in the meantime, that flush 412s and self-fences instead of overwriting it.
      # resolve_fork/4 has already quarantined this case when the store was reachable; this is
      # the fence for when it was not.
      :no_object ->
        {:ok, nil}

      # No sidecar ⇒ unknown provenance. resolve_fork/4 normally quarantined this and the open
      # came through the COLD path, so reaching here means the quarantine rename FAILED (or
      # :adopt_unprovenanced_warm is on). Adopting the store's current etag is what let a
      # planted or stale file flush over a live lineage with a valid If-Match, so only do it
      # when explicitly configured; otherwise fail the open and leave the copy untouched for
      # an operator.
      :missing ->
        if adopt_unprovenanced_warm?() do
          Logger.warning(
            "shard #{shard_id}: warm file has no provenance sidecar; adopting current etag " <>
              "(:adopt_unprovenanced_warm)"
          )

          case Storage.object_etag(shard_id) do
            {:ok, etag} -> {:ok, etag}
            {:error, reason} -> {:error, {:etag_unavailable, reason}}
          end
        else
          Logger.error(
            "shard #{shard_id}: warm file has no provenance sidecar and could not be " <>
              "quarantined; refusing to open rather than adopt an unknown lineage"
          )

          {:error, :no_provenance}
        end
    end
  end

  defp await_pull(task, path, _shard_id) do
    case Task.yield(task, @pull_timeout) || Task.shutdown(task) do
      {:ok, {:ok, etag}} ->
        promote_pull(path, etag)

      # No bytes written — a brand-new shard, or a steal sentinel (expert review 2026-08-01
      # #24). `promote_pull/2`'s else branch is exactly this case: it stamps the "derived from
      # no object" sentinel sidecar and carries the fence etag, so the first flush is a
      # conditional create rather than a blind overwrite.
      {:ok, {:absent, etag}} ->
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

  defp adopt_unprovenanced_warm?,
    do: Application.get_env(:fathom, :adopt_unprovenanced_warm, false)

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

      # Establish provenance BEFORE the pulled file becomes authoritative (expert
      # review 2026-07-14 #5). Writing the sidecar AFTER the rename left a crash
      # window: an authoritative warm `.db` with NO sidecar. The next warm open reads
      # `:missing` (fork_evidence → :no_sidecar ⇒ warm, await_pull(nil) → adopt the store's
      # CURRENT etag), so if another node stole+wrote+released the shard in between
      # (reachable on a persisted/remounted `:shard_data_dir`), the stale local copy
      # is served and fenced with the NEW lineage's etag — its first flush If-Matches
      # and clobbers the newer owner's writes (the #1 clobber, through the promote's
      # crash hole). Sidecar-first inverts the residue to an orphan `<path>.etag` with
      # no `.db`, which is harmless: warm detection gates on File.exists?(path) (the
      # `.db`, see handle_continue's `warm?`), and the brand-new-open branch below
      # File.rm's a stale sidecar before landing. The sidecar path is `<path>.etag`,
      # derived from the FINAL path, so it's writable before the rename.
      write_etag_sidecar(path, etag)

      case File.rename(temp, path) do
        :ok ->
          {:ok, etag}

        {:error, _} = err ->
          err
      end
    else
      # Brand-new shard: no object, so the pull wrote no temp. This used to `File.rm` the
      # sidecar, which left every born-empty shard with NO PROVENANCE BY CONSTRUCTION — and
      # "absent sidecar" was then read as "legacy file, adopt whatever etag the store holds
      # now". That is the clobber the sidecar exists to prevent, reachable with no attacker at
      # all (expert review 2026-08-01 #2):
      #
      #   node A opens a new shard and takes writes, dying before its first flush. The LB
      #   reroutes to B, which serves, flushes (the object now EXISTS), idles and releases
      #   cleanly. A comes back on a persisted :shard_data_dir, sees its local `.db`, reads
      #   `:missing`, opens WARM, adopts the store's current etag — and its first flush
      #   If-Matches successfully, destroying every write B acknowledged.
      #
      # So record the absence EXPLICITLY. `@no_object_sentinel` means "this file was created
      # locally against no stored object", which is a provenance claim the open path can check
      # (see fork_evidence/2) rather than an absence it has to guess about.
      write_no_object_sidecar(path)
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
    # Per-shard concurrent-stream cap (expert review 2026-07-14 #26): bound how many streams one
    # tenant can hold open at once, so a single tenant can't monopolize a node's streams (and,
    # combined with the timeout/row caps, can't wedge the shard un-drainable). Off by default
    # (`:max_checkouts_per_shard` unset ⇒ unlimited); the caller maps the refusal to a 503.
    case max_checkouts() do
      cap when is_integer(cap) and cap > 0 and map_size(state.conns) >= cap ->
        {:reply, {:error, :shard_at_stream_capacity}, state}

      _ ->
        ref = Process.monitor(caller)
        # The value carries the caller AND the call's op tag, so an abandon names
        # exactly one grant (round-2 #33) instead of every grant this pid holds.
        # Clearing idle_since (not cancelling the timer) is what defers idle-stop: a
        # pending timer fires as a cheap no-op/re-arm instead of paying a
        # cancel_timer + send_after pair per stream cycle (review 2026-07-23 #17).
        state = %{state | conns: Map.put(state.conns, ref, {caller, op}), idle_since: nil}

        # Re-arm the durability timer if the idle+clean state disarmed it (review 2026-07-24 #10).
        # Deliberately on the GRANT, not on checkin: the timer must be live for the whole window in
        # which this connection can write. Cheap — only when there is no timer, i.e. once per
        # idle→busy transition, never per stream cycle.
        state = if state.flush_timer == nil, do: do_schedule_flush(state), else: state

        # Publish the busy count so the at-capacity eviction probe skips this shard while it serves
        # (expert review #14) — otherwise a long-lived held stream ages to the LRU front but can't be
        # evicted, starving admission. A best-effort hint, no-op unless eviction is enabled.
        Fathom.Shards.Lru.record_conns(state.id, map_size(state.conns))
        {:reply, {:ok, ref, state.path}, state}
    end
  end

  def handle_call(:dirty?, _from, state), do: {:reply, unflushed?(state), state}

  # Synchronous force-flush (the flush-before-fork primitive): make the current on-disk state
  # durable WITHOUT dropping/stopping — the coordinator keeps serving. Replies :ok once the
  # shard is durably clean, or {:error, _} on a flush error / lease steal. Contract: guarantees
  # durability of writes that completed before the call, given no concurrent writer (the
  # fork-a-quiesced-template use case); a live writer can keep it re-flushing until the caller's
  # call timeout. Reuses the exact off-process durability-flush path — no separate write path.
  def handle_call(:flush_now, from, state) do
    if not unflushed?(state) and state.flush_task == nil do
      # Clean and nothing in flight: local == storage already.
      {:reply, :ok, state}
    else
      state = %{state | flush_waiters: [from | state.flush_waiters]}
      # Kick a flush now if one isn't already running (the result handler replies to waiters).
      if state.flush_task == nil, do: send(self(), :durability_flush)
      {:noreply, state}
    end
  end

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

  # A stream wrote for the first time on its checkout (expert review 2026-08-01 #42). A clean
  # served shard polls at a reduced cadence, so this is the edge that restores full-rate
  # flushing. Idempotent and cheap: the executor sends it at most once per checkout.
  def handle_cast(:became_dirty, state), do: {:noreply, schedule_flush(state)}

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

  # An evict-if-idle probe (timeout 0, from the at-capacity eviction path) against a BUSY
  # shard: leave it COMPLETELY untouched — never flip to :draining (expert review 2026-07-14
  # #20). The general clause below would set draining: true and arm a 0 ms timer that
  # immediately resumes, but in that window a concurrent checkout of this healthy bystander
  # hits the draining guard and gets {:error, :draining} — a spurious 503 caused by an
  # unrelated tenant's admission. A busy shard is never evictable anyway, so just report the
  # abort (the caller moves to the next LRU candidate) and change no state.
  def handle_cast({:drain, 0, reply_to}, %{conns: conns} = state) when map_size(conns) > 0 do
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
    state = release_flush_slot(%{state | flush_task: nil})

    # The task replies a fully-RESOLVED verdict — the FENCE and the flush (incl. the #8
    # 412-reconcile) all ran off-process (expert review 2026-07-18 #18), so this handler does ZERO
    # Storage S3 I/O and never blocks the coordinator mailbox: the exact stall #27 removed, now
    # covering the legacy-mode renew PUT too. Only local state moves here.
    case result do
      # Fence confirmed ownership off-process (#18): merge the refreshed lease/gen, then apply the
      # flush verdict below. All its Storage S3 I/O ran in the task; only local state moves here
      # (write_etag_sidecar is local disk, FlushWatermark.record a local ETS insert).
      {:fenced, flush_result, updates} ->
        # Ownership was confirmed off-process — lift any write-fence and reset the not-valid clock
        # (expert review #3). A dirty shard keeps flushing each interval, so a healed partition
        # clears the fence here on its next flush.
        state = clear_write_fence(Map.merge(state, updates))

        # The verdict fold is SHARED with the terminate path's settle_flush_task/1
        # (expert review 2026-08-01 #3): the two used to be separate `case`s and drifted —
        # settle matched a reply shape this task stopped producing at f8ecf63, so every
        # graceful stop silently discarded the flush result. One function, two callers.
        case apply_flush_verdict(state, flush_result) do
          {:flushed, state} ->
            {:noreply, schedule_flush(settle_waiters(state, :flushed))}

          # Still dirty — re-kick for any flush_now waiter (else it waits a whole flush interval).
          {:dirty, state} ->
            {:noreply, schedule_flush(settle_waiters(state, :flushed))}

          # 412 AND the task's lock re-check found the lock SUPERSEDED — a real steal. Never
          # clobber the new owner: self-fence (stop without flushing). Fail any flush_now waiters.
          {:superseded, state} ->
            {:stop, {:shutdown, :lease_lost},
             settle_waiters(%{state | lease_lost: true}, {:error, :lease_lost})}

          {{:error, reason}, state} ->
            {:noreply, schedule_flush(settle_waiters(state, {:error, reason}))}
        end

      # The pre-flush fence found a real steal (lock superseded) — self-fence, never clobber the new
      # owner (a distinct cause from the 412-reconcile's :superseded above).
      :fence_superseded ->
        Logger.error("shard #{state.id}: lease superseded during durability flush; self-fencing")

        {:stop, {:shutdown, :lease_lost},
         settle_waiters(%{state | lease_lost: true}, {:error, :lease_lost})}

      # Ownership unconfirmed (transient store error / heartbeat not valid): keep dirty, retry next
      # interval, feeding the RPO-alerting counter (#8) — the loss window grows like a PUT failure.
      # Ownership unconfirmed (a transient store error, or the heartbeat not valid). Settle any
      # flush_now waiter with an explicit error (expert review 2026-08-01 #32): this branch
      # never called settle_waiters at all, so during a storage partition a caller blocked the
      # full 60s @flush_now_timeout instead of getting a retryable answer immediately.
      :fence_skip ->
        {:noreply,
         schedule_flush(
           settle_waiters(
             record_flush_failure(note_not_valid(state), :ownership_unconfirmed),
             {:error, :ownership_unconfirmed}
           )
         )}
    end
  end

  # The flush task crashed before replying: treat like a transient flush failure.
  def handle_info({ref, result}, %{renew_task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    apply_renew_result(result, %{state | renew_task: nil})
  end

  # The renewal task crashed: treat as transient and retry on the next tick.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{renew_task: %Task{ref: ref}} = state) do
    Logger.warning("shard #{state.id}: lease renewal task crashed (#{inspect(reason)}); retrying")
    {:noreply, schedule_renew(%{state | renew_task: nil})}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{flush_task: %Task{ref: ref}} = state) do
    Logger.warning("shard #{state.id}: durability flush task crashed (#{inspect(reason)})")

    state =
      release_flush_slot(record_flush_failure(%{state | flush_task: nil}, {:task_crash, reason}))

    # Dirty (the task never landed) — re-kick so a flush_now waiter retries rather than hangs.
    {:noreply, schedule_flush(settle_waiters(state, :flushed))}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    # A stream holding a checked-out connection that dies ABNORMALLY (a crash — a clean
    # close casts {:checkin, ref} which demonitors first, so :normal never reaches here for
    # a tracked conn) may have fsynced a committed write to the local WAL in the microsecond
    # window between Connection.query returning (commit done) and the stream's post-hoc
    # WriteCounter.bump — leaving the write durable-on-disk but UN-counted. The coordinator
    # can't know, so conservatively force the shard dirty: an idle flush_and_drop then flushes
    # (unflushed? is true) rather than drop_clean deleting the possibly-uncounted committed
    # write (expert review 2026-07-14 #14). Zero per-statement cost (fires only on stream
    # death); over-dirtying is the documented safe direction — an extra flush, never lost data.
    if reason != :normal and Map.has_key?(state.conns, ref), do: WriteCounter.bump(state.id)

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

  # Coalesced idle deadline (review 2026-07-23 #17): the timer is armed at most once and
  # compares against the idle_since stamp when it fires — stopping only after a full
  # idle_ms of CONTINUOUS idleness (same contract as the old cancel/rearm-per-cycle
  # shape, without the per-stream timer churn on the hottest shard's coordinator).
  def handle_info(:idle_timeout, state) do
    state = %{state | timer: nil}
    now = System.monotonic_time(:millisecond)

    cond do
      # Busy, draining, or the stamp was cleared by a checkout: no stop. The next
      # release-to-idle arms a fresh timer.
      map_size(state.conns) > 0 or state.draining or state.idle_since == nil ->
        {:noreply, state}

      now - state.idle_since >= state.idle_ms ->
        {:stop, :normal, state}

      # A newer stream cycle moved the stamp — re-arm for the remainder.
      true ->
        remaining = state.idle_ms - (now - state.idle_since)
        {:noreply, %{state | timer: Process.send_after(self(), :idle_timeout, remaining)}}
    end
  end

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
        {:noreply, clear_write_fence(Map.merge(state, updates))}

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

  # Legacy-mode periodic lease renewal, run OFF-PROCESS (expert review 2026-08-01 #29).
  #
  # `f8ecf63` moved `Fence.check` off-process precisely because in legacy mode it does a
  # renew PUT that blocked the coordinator mailbox — but this timer, which does the SAME PUT
  # every `ttl/3` (10s at defaults), was left inline. So the coordinator went unresponsive for
  # a storage RTT (plus Req's retry ladder on a blip) every 10 seconds: `:checkout` queued,
  # adding a full RTT to p99 cold-open; `:checkin` was delayed, so idle detection and eviction
  # lagged; and the eviction probe's 2s budget could expire against a merely-blocked
  # coordinator, which is #34's leak trigger.
  #
  # Same monitored-task shape the flush already uses. The renewal is a plain PUT with no local
  # state to protect, so there is nothing to serialise against.
  def handle_info(:renew_lease, %{renew_task: nil} = state) do
    id = state.id
    lease = state.lease
    ttl = state.ttl_ms
    task = Task.async(fn -> Storage.renew_lease(id, lease, ttl) end)
    {:noreply, %{state | renew_task: task}}
  end

  # A renewal is still in flight — the store is slower than the renew interval. Skip this tick
  # rather than stacking PUTs; the in-flight one will reschedule when it lands.
  def handle_info(:renew_lease, state), do: {:noreply, state}

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
        # Node-wide concurrent-flush cap (expert review #17): over the cap, reschedule with a short
        # backoff and stay dirty (the safe direction — the flush just waits), so a mass re-home's
        # phase-aligned timers can't fire N snapshots + PUTs in lockstep and starve cold-open pulls.
        # :disabled (no cap configured — the default) and :ok (slot reserved) both proceed to flush.
        case FlushGate.try_acquire() do
          :full ->
            {:noreply, schedule_flush_backoff(state)}

          gate ->
            # Capture the write count BEFORE snapshotting: a write landing during the snapshot/upload
            # bumps the counter past this, so the shard stays dirty and re-flushes next interval —
            # never silently cleared (findings #1/#2/#27). Generation before count, same reasoning as
            # init (#11).
            pending_gen = WriteCounter.generation()
            flushed_to = WriteCounter.count(state.id)

            # Run the FENCE and the flush together off-process (expert review 2026-07-18 #18). In
            # legacy mode (heartbeat down) Fence.check does a renew PUT; running it inline blocked the
            # coordinator mailbox a storage RTT every interval — the p99 checkout stall #27 removed,
            # reintroduced for the degraded mode. The task fences, then (on {:ok}) snapshots + uploads,
            # and replies a fully resolved verdict — extending the #8 pattern (the 412-reconcile
            # already runs off-process). No EXTRA task: the flush already ran in one; the fence just
            # moves into it. fence_ctx carries the lease/ttl/acquire_gen the fence needs;
            # snapshot_state what flush_and_reconcile needs.
            fence_ctx = fence_ctx(state)
            snapshot_state = Map.take(state, [:id, :path, :etag, :lease])
            task = Task.async(fn -> fenced_flush(fence_ctx, snapshot_state) end)

            {:noreply,
             %{
               state
               | flush_task: task,
                 flush_slot_held: gate == :ok,
                 flush_pending: flushed_to,
                 flush_pending_gen: pending_gen
             }}
        end
    end
  end

  @impl true
  def terminate(_reason, %{lease_lost: true} = state) do
    # We no longer own the shard; flushing our local copy would clobber the node that took over.
    # But don't DESTROY it: a dirty local holds acked-but-unflushed committed writes — exactly the
    # class of data the fork path carefully preserves — so quarantine it (.fenced.<ts>) for recovery
    # instead of drop_local (expert review 2026-07-14 #5). A clean copy (an in-flight flush task
    # landed before the steal) is just dropped. Use the SETTLED state, and decide BEFORE forgetting
    # the write counter (unflushed?/1 and the lost-window delta both read it).
    #
    # This clause covers a self-fence with connections STILL CHECKED OUT (conns > 0) too — a busy
    # self-fence, which is the *likely* case (self-fencing peaks under load: GC pauses / missed
    # heartbeat renewals). Before expert review 2026-07-18 #1 that landed in the catch-all clause,
    # which abandoned the dirty local at its normal path with no quarantine, log, or telemetry —
    # silently stranding acked writes. quarantine_fenced!'s File.rename is safe while streams hold
    # the file open (Unix keeps the fd on the renamed inode); the streams tear down when their
    # monitor on this dying coordinator fires.
    state = settle_flush_task(state)
    # Reply to any pending flush_now caller before we exit (expert review 2026-07-18 #4): we
    # self-fenced WITHOUT flushing, so the on-disk state is NOT durable in storage. Without this,
    # the caller's GenServer.call gets a bare :DOWN → Shards.flush catches the exit as :ok → the
    # keystone-fork treats it as "source durably flushed" and forks stale bytes.
    state = settle_waiters(state, {:error, :lease_lost})
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    Fathom.Shard.WriteFence.forget(state.id)
    if unflushed?(state), do: quarantine_fenced!(state), else: drop_local(state.path)
    WriteCounter.forget(state.id)
    FlushWatermark.forget(state.id)
    :ok
  end

  def terminate(_reason, %{conns: conns} = state) when map_size(conns) == 0 do
    # Settle any in-flight flush task FIRST (expert review #27): its conditional PUT
    # may land at any moment, and the drop-flush below fences on the etag — racing
    # them would make one 412 spuriously (the drop-flush's :superseded branch drops
    # the local copy WITHOUT uploading writes made after the task's snapshot).
    state = settle_flush_task(state)
    # A flush_now caller pending at idle/drain time gets an explicit error rather than a bare
    # exit that Shards.flush would mask as a false :ok (expert review 2026-07-18 #4). The retry
    # is correct: after the drop-flush the shard is cold and Shards.flush returns :ok if storage
    # is current — never forking stale bytes on the strength of a swallowed exit.
    state = settle_waiters(state, {:error, :coordinator_stopped})
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    Fathom.Shard.WriteFence.forget(state.id)
    # flush_and_drop reads the write counter (unflushed?/1) to decide whether to upload before
    # dropping, so forget the counter AFTER it — forgetting first would zero the count and make a
    # dirty shard look clean, skipping the flush and losing the writes (findings #1/#27).
    flush_and_drop(state)
    WriteCounter.forget(state.id)
    FlushWatermark.forget(state.id)
    :ok
  end

  # A fully-open coordinator force-stopped with connections STILL CHECKED OUT and its lease intact —
  # the rolling-deploy / SIGTERM path (terminate/2 runs on the supervisor EXIT before the queued
  # `:DOWN`s from Bandit-killed streams are processed, so conns > 0) and the busy-delete path
  # (`Fathom.Shards.stop` force-terminates without draining). Before expert review 2026-07-19 #2 this
  # fell to the catch-all below, which flushed NOTHING: a dirty shard lost up to a full flush interval
  # of acked writes on ephemeral disk (containers), despite trap_exit + the shutdown budget existing
  # precisely to flush on a clean stop. Settle the in-flight flush task FIRST — its result-fold stamps
  # the provenance sidecar, closing the window where a killed task's already-landed PUT makes the next
  # warm open read sidecar(old) != object(new) and FALSE-quarantine a fork (the #21 spurious-fork
  # window, here on the shutdown path). Then flush the dirty state via the SAME fenced flush_and_drop
  # the idle-stop clause uses (drops local + releases the lease for a fast planned failover; its
  # 412/superseded/corrupt-local branches all apply) — UNLESS the shard is being DELETED.
  # `Fathom.Tenants.delete` tombstones the id before `DeleteJob` → `purge` → `Shards.stop`, so a
  # tombstoned id here means "about to be erased": skip the flush, since `purge_shard` deletes the
  # object next and `stop()` blocks through this terminate (purge always runs AFTER — no resurrection
  # even if the tombstone hasn't reached this node's ETS yet; worst case is one wasted upload purge
  # then deletes). `tombstoned?` is an O(1) public-ETS read that rescues a missing table to `false`,
  # so a shutdown-ordering edge (Tombstones dies first) fails toward flushing — the safe direction.
  def terminate(_reason, %{conns: conns, lease: lease} = state)
      when map_size(conns) > 0 and not is_nil(lease) do
    state = settle_flush_task(state)
    state = settle_waiters(state, {:error, :coordinator_stopped})
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    Fathom.Shard.WriteFence.forget(state.id)
    # flush_and_drop reads the write counter (unflushed?/1) to decide whether to upload, so forget
    # the counter AFTER it — forgetting first would zero the count and skip a dirty shard's flush.
    unless Fathom.Tenants.Tombstones.tombstoned?(state.id), do: flush_and_drop(state)
    WriteCounter.forget(state.id)
    FlushWatermark.forget(state.id)
    :ok
  end

  # Stopping from an early pre-open state (init/handle_continue failed before a lease/conns were set,
  # so the state is the minimal `%{id: ...}` that matches no clause above) — always drop the shard's
  # load row so a stopped shard never leaks a counter.
  def terminate(_reason, state) do
    # A flush_now caller pending when the coordinator is force-stopped (a pre-open failure) gets an
    # explicit error, not a swallowed exit → false :ok (expert review 2026-07-18 #4). settle_waiters
    # is a no-op on the empty/pre-open waiter list.
    state = settle_waiters(state, {:error, :coordinator_stopped})
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    Fathom.Shard.WriteFence.forget(state.id)
    WriteCounter.forget(state.id)
    FlushWatermark.forget(state.id)
    :ok
  end

  # --- connection tracking ---

  defp release(state, ref) do
    Process.demonitor(ref, [:flush])
    conns = Map.delete(state.conns, ref)
    state = %{state | conns: conns}

    # Update the busy count and re-stamp recency (#14): a just-released shard was recently used, so it
    # should not read as the LRU-coldest, and its lowered count lets the eviction probe consider it.
    Fathom.Shards.Lru.record_conns(state.id, map_size(conns))
    Fathom.Shards.Lru.touch(state.id)

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

  # A settled flush task can report the lock SUPERSEDED after terminate/2 has already
  # dispatched past its `lease_lost: true` clause (expert review 2026-08-01 #3). Honour it
  # here rather than uploading over the node that took the shard: quarantine a dirty local for
  # recovery, exactly as that clause does.
  defp flush_and_drop(%{lease_lost: true} = state) do
    if unflushed?(state), do: quarantine_fenced!(state), else: drop_local(state.path)
    :ok
  end

  defp flush_and_drop(state) do
    if unflushed?(state), do: flush_then_drop(state), else: drop_clean(state)
  end

  defp drop_clean(state) do
    # Clean: local == storage. Drop the local copy without re-uploading (nothing to
    # flush, no clobber possible) and release the lease for a clean handoff.
    if File.exists?(state.path), do: drop_local(state.path)

    # Release regardless of the local file (review 2026-07-23 #19): a coordinator can stop
    # clean with NO local file at all — a checkout abandoned before any connection opened on
    # a brand-new shard (the pull found no object; exqlite only creates the file on first
    # connection open). The old file-gated release leaked the lock object: the next
    # same-owner open silently downgraded from the optimistic 1-RTT create to the reclaim
    # path, and a foreign node saw {:held, us} for as long as our heartbeat stayed fresh.
    if is_map(state.lease), do: Storage.release_lease(state.id, state.lease)

    :ok
  end

  defp flush_then_drop(state) do
    if File.exists?(state.path) do
      # Fence the flush: confirm we still own the shard right before uploading, so a
      # lease lost between acquire and now can't clobber a newer owner. See
      # Fathom.Shard.Fence.
      case Fence.check(fence_ctx(state)) do
        {:ok, updates} ->
          # MERGE the refreshed lease (expert review 2026-08-01 #9). In legacy mode
          # (acquire_gen == nil) Fence.check performs a renew_lease PUT, which ROTATES the
          # lock's etag. release_lease's fast path is a conditional
          # `DELETE … If-Match: lease.lock_etag`, so releasing with the pre-fence lease
          # If-Matched an etag that no longer exists ⇒ 412 ⇒ release_lease reported :ok and
          # THE LOCK OBJECT SURVIVED THE DROP.
          #
          # The leaked lock names this node, and while its Heartbeat is running `owner_live?`
          # reports :live forever — so every peer gets {:error, {:held, us}} indefinitely and
          # the shard is unopenable by any survivor. The rebalancer handoff breaks identically:
          # its step-3 drain lands here, so the target the LB was already flipped to is refused.
          #
          # Every other release site already threads the refreshed lease back; this one did not.
          state = Map.merge(state, updates)

          case upload_for_drop(state) do
            {:ok, new_etag} ->
              # Stamp the new etag into the provenance sidecar BEFORE dropping (expert
              # review 2026-07-14 #21). We're about to delete everything, so the write
              # is normally invisible — but a crash after this upload lands (object now
              # at new_etag) and before drop_local completes leaves the local `.db`
              # with its sidecar still at the OLD etag. The next warm open then reads
              # sidecar(old) != object(new) and FALSE-quarantines a FORK (error log,
              # [:fathom,:shard,:forked], a leaked `.forked.<ts>` recovery copy) — a
              # spurious alarm, since the local db equals the object we just uploaded
              # (a clean crash-recovery of identical bytes). Writing it first closes the
              # window so recovery is an ordinary clean warm restart, matching the
              # periodic-flush, settle, and reconcile sites that already stamp on success.
              write_etag_sidecar(state.path, new_etag)
              # Record the durable-flush time before we drop + release (#28): the shard's writes
              # reached storage, so it's NOT dirty-at-loss even though its coordinator is gone.
              Fathom.Directory.Recorder.record_flush(state.id)
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
                  retry_drop_upload(state)

                _ ->
                  Logger.warning(
                    "shard #{state.id}: object superseded before flush; keeping local for recovery"
                  )
              end

            # quick_check failed (expert review #4): upload_for_drop already quarantined the corrupt
            # local and refused the flush, so the good stored object is untouched. Release the lease
            # so the next open pulls that good object (there's no valid local copy left to keep).
            {:error, {:corrupt_local, _reason}} ->
              Storage.release_lease(state.id, state.lease)

            {:error, reason} ->
              Logger.warning(
                "shard #{state.id}: flush failed, keeping local copy (#{inspect(reason)})"
              )
          end

        :superseded ->
          # Lost the lease before we could flush (expert review #5). flush_then_drop only runs on a
          # dirty shard, so the local copy holds acked-but-unflushed writes — quarantine them for
          # recovery instead of drop_local. The stored object is the new owner's; we don't clobber it.
          Logger.warning(
            "shard #{state.id}: lost lease before flush; quarantining local instead of flushing"
          )

          quarantine_fenced!(state)

        :skip ->
          Logger.warning(
            "shard #{state.id}: ownership unconfirmed before flush; keeping local copy"
          )
      end
    else
      # Dirty, but there is no local file to flush (expert review 2026-08-01 #11). This whole
      # function used to be one `if File.exists?`, so this case fell out the bottom having done
      # NOTHING: no upload (correct — there is nothing to upload), but also no lease release, no
      # log, and no telemetry. `drop_clean/1` had exactly this hole fixed on the clean side by
      # review 2026-07-23 #19.
      #
      # It needs no exotic input: a `WriteCounter` restart broadcasts `:write_counter_reset`,
      # which sets `flushed_through: -1` and so marks EVERY open coordinator dirty at once —
      # including ones that never created a file (a checkout abandoned before any connection
      # opened on a brand-new shard). Each then idle-stops through here and strands its lock,
      # and a peer sees {:held, us} for as long as this node keeps beating.
      #
      # Holding the lock buys nothing: the local copy cannot be recovered because it does not
      # exist. Release it, and make the event alertable rather than silent.
      Logger.error(
        "shard #{state.id}: marked dirty but the local file is GONE; nothing to flush, " <>
          "releasing the lease so the shard is not stranded"
      )

      :telemetry.execute(
        [:fathom, :shard, :flush, :failed],
        %{count: 1},
        %{shard_id: state.id, reason: :local_file_missing}
      )

      if is_map(state.lease), do: Storage.release_lease(state.id, state.lease)
    end

    :ok
  end

  # The off-process periodic durability flush the coordinator's flush task runs (expert
  # review 2026-07-14 #8): snapshot + upload, AND — on a data-PUT 412 — the lock/etag
  # re-check, ALL inside the task so NO Storage S3 call runs in the coordinator's
  # flush-result handler (the handler then applies a resolved verdict purely). Returns:
  #   {:ok, new_etag}         — uploaded; advance the fence + watermark.
  #   {:reconciled, obj_etag} — 412 but the lock is still ours; obj_etag resyncs the fence
  #                             (nil ⇒ object etag unreadable, keep serving, no resync).
  #   :superseded             — 412 AND the lock was stolen; the coordinator self-fences.
  #   {:error, reason}        — a transient store error anywhere; keep dirty, retry.
  # The periodic durability flush's off-process body (expert review 2026-07-18 #18): fence FIRST,
  # then (only if ownership is confirmed) snapshot + upload. Fence.check's legacy-mode renew PUT and
  # the revalidate check_lease no longer block the coordinator mailbox. Replies:
  #   * `{:fenced, verdict, updates}` — ownership confirmed; `verdict` is flush_and_reconcile's
  #     result and `updates` the refreshed lease/gen the coordinator merges back.
  #   * `:fence_superseded` — a real steal (lock superseded) → the coordinator self-fences.
  #   * `:fence_skip` — ownership unconfirmed (transient store error / heartbeat not valid) → retry.
  # The fence stays the hard pre-write guard: no upload happens unless it returns `{:ok, _}`, and the
  # 412-reconcile inside flush_and_reconcile is the second guard on the PUT itself.
  defp fenced_flush(fence_ctx, snapshot_state) do
    case Fence.check(fence_ctx) do
      {:ok, updates} ->
        # Flush against the fence-refreshed lease (the 412-reconcile re-checks the lock with it).
        {:fenced, flush_and_reconcile(Map.merge(snapshot_state, updates)), updates}

      :superseded ->
        :fence_superseded

      :skip ->
        :fence_skip
    end
  end

  defp flush_and_reconcile(state) do
    started = System.monotonic_time()

    result =
      case snapshot_and_upload(state) do
        {:error, :superseded} -> reconcile_superseded(state)
        other -> other
      end

    # Flush-COST telemetry (expert review 2026-07-18 #20 — the cost side of the flush-interval knob,
    # complementing the RPO benefit measured by Fathom.Rpo). The VACUUM INTO snapshot + object PUT
    # ran here in the flush task; emit the duration + outcome so operators can watch the flush RATE
    # (a tighter interval flushes proportionally more often) and per-flush cost, and so
    # `mix fathom.rpo --cost` can quantify it. Un-tagged by shard (cardinality) beyond metadata.
    :telemetry.execute(
      [:fathom, :shard, :flush],
      %{duration: System.monotonic_time() - started},
      %{shard_id: state.id, outcome: flush_outcome(result)}
    )

    result
  end

  defp flush_outcome({:ok, _}), do: :uploaded
  defp flush_outcome({:reconciled, _}), do: :reconciled
  defp flush_outcome(:superseded), do: :superseded
  defp flush_outcome({:error, _}), do: :error

  # Disambiguate a data-PUT 412 during the periodic durability flush (expert review #2),
  # OFF the coordinator process (expert review 2026-07-14 #8 — runs inside the flush task).
  # A 412 means the object changed since our fence etag; re-check the LOCK to tell "our
  # own lost-but-applied PUT" (lock still ours — the single writer, so the object is our
  # own bytes) from a real steal (lock superseded). Returns `{:reconciled, etag}` (the
  # object's current etag to resync the fence to — nil when unreadable), `:superseded`
  # (self-fence), or `{:error, _transient}` (unconfirmable lock read — don't self-fence on
  # uncertainty; keep dirty and retry). The sidecar/watermark advance stays in the handler.
  defp reconcile_superseded(state) do
    case Storage.check_lease(state.id, state.lease) do
      :ok ->
        case Storage.object_etag(state.id) do
          {:ok, etag} when not is_nil(etag) -> {:reconciled, etag}
          _ -> {:reconciled, nil}
        end

      {:error, :superseded} ->
        :superseded

      {:error, _transient} = error ->
        error
    end
  end

  # A 412 on the DROP flush, with the lock still ours (expert review 2026-08-01 #4). This used
  # to log "object is durable, dropping" and `drop_local` — which is only sound if the object
  # holds THIS attempt's bytes. It need not: a previous interval's PUT can have landed with its
  # response lost, after which the tenant committed more writes; the object then holds the
  # OLDER snapshot and dropping unlinks everything since. And because finding #3 made the
  # terminate path reach this branch deterministically on a rolling deploy, "need not" was in
  # practice "usually does not".
  #
  # A 412 is proof the object moved, nothing more — so re-fence to where it actually is and
  # upload again. Success ⇒ our bytes really are durable ⇒ drop + release. Failure ⇒ KEEP the
  # local copy: the next open's provenance check arbitrates recoverably, which is always
  # better than an unrecoverable unlink.
  defp retry_drop_upload(state) do
    with {:ok, etag} when not is_nil(etag) <- Storage.object_etag(state.id),
         {:ok, new_etag} <- upload_for_drop(%{state | etag: etag}) do
      Logger.info(
        "shard #{state.id}: flush 412 with lock ours; re-fenced to the object and re-uploaded"
      )

      write_etag_sidecar(state.path, new_etag)
      Fathom.Directory.Recorder.record_flush(state.id)
      drop_local(state.path)
      Storage.release_lease(state.id, state.lease)
    else
      other ->
        Logger.error(
          "shard #{state.id}: flush 412 with lock ours, but the re-fenced upload failed " <>
            "(#{inspect(other)}); KEEPING the local copy — it holds acked writes the stored " <>
            "object may not"
        )
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
    # checkpoint_and_verify/1 runs BOTH on one connection (review 2026-07-24 #31) and reports which
    # stage failed, because the two failures need opposite handling: a corrupt local db must refuse
    # the flush, while a merely-busy checkpoint must fall back to the snapshot path.
    case checkpoint_and_verify(state.path) do
      :ok ->
        Storage.flush(state.id, state.path, state.etag)

      # Integrity failed (expert review 2026-07-14 #4). The checkpoint-then-raw-upload fast path
      # uploads the bytes as-is, so a locally corrupted db (disk/fs/memory fault, exqlite/OS bug)
      # would be flushed with a valid If-Match by the legitimate owner — the fence can't help, it's
      # not a steal — permanently clobbering the last good stored object. Refuse the flush (the good
      # object stays authoritative), quarantine the corrupt copy for forensics, and alarm.
      {:error, {kind, _} = reason} when kind in [:quick_check, :quick_check_failed] ->
        quarantine_corrupt!(state, reason)
        {:error, {:corrupt_local, reason}}

      # Checkpoint incomplete (typically busy). Fall back to snapshot_and_upload, whose VACUUM INTO
      # captures WAL content regardless — and which reads every page, so it fails on corruption on
      # its own. quick_check is deliberately NOT run on this branch, matching the pre-fold ordering.
      {:error, reason} ->
        Logger.warning(
          "shard #{state.id}: pre-drop checkpoint incomplete (#{inspect(reason)}); snapshot-flushing instead"
        )

        snapshot_and_upload(state)
    end
  end

  @doc """
  Stamp provenance on a shard file that was placed in the live data dir out-of-band.

  Cold-open refuses to serve a local `.db` with no provenance sidecar — it cannot tell a
  legitimate copy from one planted by a tenant or left behind by a node whose shard has since
  been taken over (expert review 2026-08-01 #2). Everything fathom writes there normally gets a
  sidecar automatically (`promote_pull/2` on a pull, `write_etag_sidecar/2` on a flush).

  Harnesses and fixtures that build a shard file directly must therefore declare what it
  derives from. This records the stored object's current etag when one exists, or the
  "no stored object" sentinel when it does not — the same two claims the real paths make.
  """
  @spec stamp_local_provenance(String.t()) :: :ok
  def stamp_local_provenance(shard_id) do
    path = db_path(shard_id)

    case Storage.object_etag(shard_id) do
      {:ok, etag} when not is_nil(etag) -> write_etag_sidecar(path, etag)
      _ -> write_no_object_sidecar(path)
    end

    :ok
  end

  # Run `PRAGMA quick_check` on the file at `path`; `:ok` iff SQLite reports the single row "ok".
  # Cheap (shards are small by premise). `@doc false` public so the corrupt-flush guard is
  # testable directly (expert review 2026-07-14 #4).
  @doc false
  @spec verify_integrity(Path.t()) :: :ok | {:error, term()}
  def verify_integrity(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        result = Connection.query(conn, "PRAGMA quick_check", [])
        Connection.close(conn)

        case result do
          {:ok, %{rows: [["ok"]]}} -> :ok
          {:ok, %{rows: rows}} -> {:error, {:quick_check, rows}}
          {:error, reason} -> {:error, {:quick_check_failed, reason}}
        end

      other ->
        {:error, {:quick_check_open_failed, other}}
    end
  end

  # A local db failed quick_check: move it aside (preserve for forensics) and drop its now-stale
  # WAL/SHM so the next open pulls the last-good stored object instead of adopting the corrupt
  # local copy. Loud error + telemetry so an operator sees it (the good object is still safe).
  defp quarantine_corrupt!(state, reason) do
    dest = "#{state.path}.corrupt.#{System.system_time(:second)}"
    _ = File.rename(state.path, dest)
    Enum.each(["-wal", "-shm"], &File.rm(state.path <> &1))

    Logger.error(
      "shard #{state.id}: local db failed quick_check (#{inspect(reason)}); REFUSING flush so the " <>
        "last good stored object stays authoritative; quarantined corrupt copy to #{dest}"
    )

    :telemetry.execute([:fathom, :shard, :corrupt_flush], %{count: 1}, %{shard_id: state.id})
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
  # Checkpoint and integrity-check on ONE connection (expert review 2026-07-24 #31). These ran as
  # two separate Connection.open → work → close cycles back to back, and each open costs a
  # File.mkdir_p!, an sqlite3_open, and several pragma round-trips. Worse, in WAL mode the close of
  # the LAST connection triggers SQLite's close-time checkpoint and unlinks -wal/-shm, so the second
  # open immediately recreated the WAL index files the first close had just deleted — pure churn, on
  # the path every idle drop and every rolling-deploy teardown traverses (tens of thousands of
  # cycles per full turnover at 10k-30k shards/node).
  #
  # Ordering is preserved exactly: quick_check still runs AFTER the checkpoint and BEFORE the raw
  # upload — it is the 2026-07-14 #4 corruption gate, and the only guard on the
  # checkpoint-then-raw-upload fast path. A busy checkpoint still short-circuits WITHOUT running
  # quick_check, falling through to snapshot_and_upload (whose VACUUM INTO reads every page and so
  # fails on corruption anyway).
  #
  # This connection keeps synchronous=FULL: #11's relaxation is scoped to the snapshot connection,
  # and a checkpoint at synchronous=OFF can corrupt the main database on power loss.
  defp checkpoint_and_verify(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        Connection.set_busy_timeout(conn, 1000)
        cp = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])

        result =
          case classify_checkpoint(cp) do
            :ok -> classify_quick_check(Connection.query(conn, "PRAGMA quick_check", []))
            err -> err
          end

        Connection.close(conn)
        result

      other ->
        {:error, {:checkpoint_open_failed, other}}
    end
  end

  defp classify_checkpoint(result) do
    case result do
      {:ok, %{rows: [[0, _log, _checkpointed]]}} -> :ok
      {:ok, %{rows: [[busy, _log, _checkpointed]]}} -> {:error, {:checkpoint_busy, busy}}
      {:ok, other} -> {:error, {:checkpoint_unexpected, other}}
      {:error, reason} -> {:error, {:checkpoint_failed, reason}}
    end
  end

  defp classify_quick_check(result) do
    case result do
      {:ok, %{rows: [["ok"]]}} -> :ok
      {:ok, %{rows: rows}} -> {:error, {:quick_check, rows}}
      {:error, reason} -> {:error, {:quick_check_failed, reason}}
    end
  end

  defp drop_local(path), do: Enum.each(["", "-wal", "-shm", ".etag"], &File.rm(path <> &1))

  # Self-fence quarantine (expert review 2026-07-14 #5): the crash-steal counterpart to
  # quarantine_fork!. On a self-fence (a stealer took our lease during a GC pause/partition) the
  # local copy holds acked-but-unflushed committed writes; rename it aside (.fenced.<ts>) instead of
  # deleting it, so those writes survive for forensics/recovery, and emit a structured event with
  # the un-flushed count so the lost window is enumerable. The loss itself is within the documented
  # RPO contract — this just stops needlessly destroying a healthy-disk copy the store never got.
  defp quarantine_fenced!(state) do
    dest =
      state.path <>
        ".fenced.#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"

    case File.rename(state.path, dest) do
      :ok ->
        Enum.each(["-wal", "-shm"], &File.rename(state.path <> &1, dest <> &1))
        File.rm(etag_sidecar(state.path))
        unflushed = max(WriteCounter.count(state.id) - state.flushed_through, 0)

        Logger.error(
          "shard #{state.id}: lease superseded with #{unflushed} un-flushed write(s) — quarantined " <>
            "the local copy to #{dest} instead of dropping it (acked writes preserved for recovery; " <>
            "within the RPO contract, the stored object is the new owner's)."
        )

        :telemetry.execute(
          [:fathom, :shard, :fenced_quarantine],
          %{count: 1, unflushed: unflushed},
          %{shard_id: state.id}
        )

        :ok

      {:error, _} = error ->
        Logger.error(
          "shard #{state.id}: fence quarantine FAILED (#{inspect(error)}); dropping local copy"
        )

        drop_local(state.path)
        error
    end
  end

  # --- etag provenance sidecar (expert review #1) ---
  #
  # `<path>.etag` records which stored-object version the live local file derives
  # from — written on every pull promotion and successful flush, removed with the
  # local copy. A warm restart compares it to the store's current etag: equal ⇒ the
  # local file continues the stored lineage (and may hold newer un-flushed writes);
  # different ⇒ the lineages FORKED (another node wrote and released while we were
  # down) and serving or flushing our copy would clobber acknowledged writes.

  defp etag_sidecar(path), do: path <> ".etag"

  # "Derived from: no stored object." A real etag is a hex content hash, so this can never
  # collide with one. Written when a brand-new shard is born locally, so that "no object" is a
  # POSITIVE provenance claim rather than an absent sidecar (expert review 2026-08-01 #2).
  @no_object_sentinel "-"

  defp write_no_object_sidecar(path), do: write_etag_sidecar(path, @no_object_sentinel)

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
      # An explicit "born locally against no stored object" claim (2026-08-01 #2) — distinct
      # from an absent sidecar, which means unknown provenance.
      {:ok, @no_object_sentinel} -> :no_object
      {:ok, etag} -> {:ok, etag}
      # A truly ABSENT sidecar is now UNKNOWN provenance, not "legacy, trust it". Every file
      # this node creates carries a sidecar — a pulled object gets the object's etag, a
      # born-empty shard gets the sentinel — so an absent one is a file fathom did not write
      # here: a pre-provenance legacy copy, or a planted one.
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
  # OBSERVE ONLY — decides nothing and has no side effects (2026-07-26).
  #
  # This runs CONCURRENTLY with `acquire_lease` (review 2026-07-23 #22, for the latency win), and
  # `acquire_lease` STEAL-TOUCHES the data object to rotate its etag. So this HEAD races that
  # touch, and it used to quarantine the moment the store's etag differed from the sidecar — which
  # on a lost race means quarantining a perfectly good warm copy because it saw OUR OWN touch.
  #
  # Consequence, reproduced under load on 2026-07-26 (`RevalidateTouchedTest` B1, 3 failures in 8
  # runs at load average 19): a spurious `.forked.<ts>` file — a signal that is supposed to mean
  # preserved acked-but-unflushed writes and to be investigated — plus a full-body re-pull on
  # exactly the failover path the warm follower exists to make cheap. It degrades when the machine
  # is busy, which is when failovers happen. No data loss; the re-pull is correct.
  #
  # The verdict now belongs to `resolve_fork/4`, which runs AFTER the lease is held and can tell
  # our own touch from a real fork because it has `touch_pre_etag`/`touch_post_etag`. The HEAD
  # stays overlapped, so the latency win is unchanged.
  defp fork_evidence(shard_id, path) do
    case read_etag_sidecar(path) do
      :missing ->
        :no_sidecar

      # We recorded "born here against no stored object". If the store still has no object,
      # that claim holds and the copy is ours to serve. If an object now EXISTS, somebody else
      # created it while we were away — our copy is a fork of a lineage we never saw, and
      # serving or flushing it would destroy their writes (expert review 2026-08-01 #2,
      # trigger B: die before the first flush, peer takes over, serves, flushes, releases).
      :no_object ->
        case Storage.object_etag(shard_id) do
          {:ok, nil} -> :no_object_confirmed
          {:ok, store_etag} -> {:orphaned, store_etag}
          {:error, _unreachable} -> :unreachable
        end

      # Torn/unreadable sidecar (expert review #12): provenance unknown, so the copy cannot be
      # trusted to continue the stored lineage. Unrelated to the touch race — but the quarantine
      # itself still moves to resolve_fork/4 so this task stays side-effect free.
      :corrupt ->
        :corrupt

      {:ok, sidecar_etag} ->
        case Storage.object_etag(shard_id) do
          {:ok, ^sidecar_etag} ->
            :match

          # The object is GONE but we have provenance from one — treat as fork-adjacent?
          # No: a deliberately deleted object with a live local copy is the un-flushed
          # brand-new case; serve warm and let the fenced flush recreate it.
          {:ok, nil} ->
            :absent

          {:ok, store_etag} ->
            {:diverged, sidecar_etag, store_etag}

          {:error, _unreachable} ->
            :unreachable
        end
    end
  end

  # The post-lease verdict. Returns true when the local copy is a fork that was successfully moved
  # aside (so the caller opens COLD); false means keep it and open warm.
  #
  # A FAILED quarantine (the rename never moved the copy) must NOT report quarantined, or the
  # cold-open's promote_pull would overwrite the un-moved recovery copy (expert review #14) —
  # hence `== :ok` on every quarantine branch, exactly as before.
  # An absent sidecar is UNKNOWN provenance and now fails closed (expert review 2026-08-01 #2).
  # This used to return false — "keep it, open warm" — which is what made both triggers work:
  # a file planted by a tenant (via ATTACH or VACUUM INTO, before 286b530 closed those) was
  # adopted as authoritative for a shard that had never been opened, and a legitimate
  # born-empty shard that failed over and back clobbered the peer that took it.
  #
  # Quarantining is recoverable — the copy is preserved as `<path>.forked.<ts>` and the open
  # proceeds cold from the stored object. Adopting was not: it destroyed the other lineage
  # with a valid If-Match and looked like an ordinary flush.
  #
  # `:adopt_unprovenanced_warm` restores the old behaviour for an operator carrying
  # pre-provenance files they would rather adopt than re-pull. Default OFF.
  defp resolve_fork(:no_sidecar, shard_id, path, _lease) do
    if adopt_unprovenanced_warm?() do
      Logger.warning(
        "shard #{shard_id}: warm file has no provenance sidecar; adopting it because " <>
          ":adopt_unprovenanced_warm is on. This cannot distinguish a legacy file from a " <>
          "planted or forked one."
      )

      false
    else
      quarantine_fork!(shard_id, path, :no_sidecar) == :ok
    end
  end

  # Provenance says "no stored object" and the store agrees — our own brand-new shard.
  defp resolve_fork(:no_object_confirmed, _shard_id, _path, _lease), do: false

  # Provenance says "no stored object" but one exists: a peer created the lineage while we
  # were down. Never serve or flush over it.
  defp resolve_fork({:orphaned, _store_etag}, shard_id, path, _lease),
    do: quarantine_fork!(shard_id, path, :orphaned) == :ok

  defp resolve_fork(:match, _shard_id, _path, _lease), do: false
  defp resolve_fork(:absent, _shard_id, _path, _lease), do: false
  defp resolve_fork(:unreachable, _shard_id, _path, _lease), do: false

  defp resolve_fork(:corrupt, shard_id, path, _lease),
    do: quarantine_fork!(shard_id, path, :corrupt_sidecar) == :ok

  defp resolve_fork({:diverged, sidecar, store}, shard_id, path, lease) do
    # OUR OWN steal-touch, not a fork. Both halves are required: the touch SOURCED this file's
    # provenance (`pre == sidecar`, so the local bytes are the lineage the touch copied) and the
    # store now holds exactly that touch's output (`post == store`). A self-copy moves no bytes, so
    # the warm copy is still correct — this is the same argument `revalidate_touched/5`'s warm
    # branch already makes before adopting `post`.
    #
    # Anything else is a real fork. In particular the zombie-flush race (B2) is NOT captured here:
    # there the touch sources the ZOMBIE's etag, so `pre != sidecar` and we still quarantine.
    if lease[:touch_pre_etag] == sidecar and lease[:touch_post_etag] == store do
      false
    else
      quarantine_fork!(shard_id, path, :diverged) == :ok
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

            :no_sidecar ->
              "no provenance sidecar — this node did not create this file, so its lineage is " <>
                "unknown: a pre-provenance legacy copy, or one planted by a tenant " <>
                "(expert review 2026-08-01 #2). Set :adopt_unprovenanced_warm to adopt instead."

            :orphaned ->
              "recorded as born against NO stored object, but an object now exists — a peer " <>
                "created the lineage while this node was down (expert review 2026-08-01 #2)"
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

  # Track a failed durability flush (expert review #27). Emits [:fathom, :shard, :flush, :failed]
  # with the running consecutive count every time, and ESCALATES to Logger.error past a threshold —
  # so a persistent S3 failure (auth / bucket-policy change) that would otherwise grow the RPO
  # silently (a per-interval warning only) becomes an alertable, escalating signal. The shard stays
  # dirty and keeps retrying regardless; this is observability, not a behavior change.
  defp record_flush_failure(state, reason) do
    n = state.flush_failures + 1

    :telemetry.execute([:fathom, :shard, :flush, :failed], %{count: 1, consecutive: n}, %{
      shard_id: state.id
    })

    # `object_too_large` is NOT transient (expert review 2026-07-24 #37). Every other flush failure
    # — auth, bucket policy, reachability — can succeed on the next interval, which is why the
    # generic path waits for a threshold before escalating. This one cannot: the object is over the
    # 5 GiB single-PUT ceiling and every retry fails identically, forever, while the shard keeps
    # acking writes it can never make durable. Escalate on the FIRST occurrence, with its own event
    # so it can be alerted separately, and name the actual remedy — waiting does nothing.
    case reason do
      {:object_too_large, size} ->
        :telemetry.execute([:fathom, :shard, :flush, :too_large], %{count: 1, size: size}, %{
          shard_id: state.id
        })

        Logger.error(
          "shard #{state.id}: durability flush is PERMANENTLY failing — the object is #{size} bytes, " <>
            "past the #{Application.get_env(:fathom, :s3_max_single_put, 5 * 1024 * 1024 * 1024)}-byte " <>
            "single-PUT ceiling. This will NEVER succeed on retry: acked writes cannot be made " <>
            "durable, the RPO is unbounded, and snapshot/fork/retain are disabled for this shard. " <>
            "Reduce the shard (delete rows + VACUUM) or split the tenant; :shard_max_page_count " <>
            "is the write-time brake that prevents reaching this state."
        )

      _ ->
        :ok
    end

    if n >= flush_failure_alert_threshold() do
      Logger.error(
        "shard #{state.id}: durability flush FAILED #{n}× consecutively (#{inspect(reason)}) — RPO is " <>
          "growing unbounded; the shard stays dirty and keeps retrying. Check S3 reachability, " <>
          "credentials, and bucket policy."
      )
    end

    %{state | flush_failures: n}
  end

  # --- write-fence circuit-breaker (expert review 2026-07-19 #3) ---

  # The periodic fence came back not-valid (heartbeat stale — the cut-from-storage symptom). Record
  # WHEN that started; once it has held for ttl + steal_margin — the point past which a peer may
  # legitimately have stolen the shard — publish the write-fence so ShardExecutor refuses NEW writes
  # (reads keep serving from the local copy). This collapses the loss window from the whole partition
  # duration to ~ttl + steal_margin: without it the coordinator keeps ACKing writes for the entire
  # partition, then self-fences on heal and quarantines every one of them. Gated by
  # `:fence_writes_when_stealable` (default: prod) — off, the flag is never set, so the executor's
  # lock-free read is always false and nothing is refused.
  defp note_not_valid(state) do
    if fence_writes_when_stealable?() do
      now = System.monotonic_time(:millisecond)
      since = state.not_valid_since || now

      if not Fathom.Shard.WriteFence.fenced?(state.id) and
           now - since >= stealable_after_ms(state) do
        Fathom.Shard.WriteFence.fence(state.id)
        :telemetry.execute([:fathom, :shard, :write_fenced], %{count: 1}, %{shard_id: state.id})

        Logger.warning(
          "shard #{state.id}: heartbeat not valid for > ttl+steal_margin — fencing writes (node " <>
            "provably stealable); reads still serve. Writes 503 FILO_STALE_LEASE until ownership " <>
            "is reconfirmed."
        )
      end

      %{state | not_valid_since: since}
    else
      state
    end
  end

  # A fence just reconfirmed ownership — lift any write-fence and reset the not-valid clock. Cheap
  # no-op when the clock was never started (the common, healthy path).
  defp clear_write_fence(%{not_valid_since: nil} = state), do: state

  defp clear_write_fence(state) do
    Fathom.Shard.WriteFence.unfence(state.id)
    %{state | not_valid_since: nil}
  end

  # How long after the FIRST `:not_valid` a peer may legitimately have stolen the shard.
  #
  # This used to be `ttl + steal_margin`, which double-counts (expert review 2026-08-01 #13).
  # The clock here starts when `valid_for_write?/1` first returns `:not_valid`, and that fires
  # EARLY by design — at `now + margin >= deadline`, i.e. at `last_renew + ttl - margin`, where
  # margin is `ttl/3`. So waiting another full `ttl + steal_margin` from that point put the
  # fence at roughly `last_renew + 2·ttl`, while a peer may steal at
  # `last_renew + ttl + steal_margin`. At the defaults (ttl 30s, steal margin 5s) that is ~20s
  # of continued write ACKs after another node could already own the shard — every one of them
  # quarantined on partition-heal, and outside the RPO docs/single-writer.md advertises.
  #
  # The remaining distance from the first `:not_valid` to stealable is exactly the margin that
  # made it fire early, plus the steal margin.
  defp stealable_after_ms(_state), do: Heartbeat.margin_ms() + Storage.steal_margin_ms()

  defp fence_writes_when_stealable?,
    do: Application.get_env(:fathom, :fence_writes_when_stealable, false)

  # Reply to synchronous force-flush (`:flush_now`) waiters. On a durable flush: if the shard is
  # now clean, reply :ok and clear them; if a write landed during the flush and left it dirty
  # again, kick one more flush and keep waiting (so the caller only sees :ok once its writes are
  # durable). On an error/steal: reply the error and clear, so a caller never blocks to timeout.
  # A pre-open / minimal state (e.g. a lease-acquire failure in handle_continue, before the full
  # state is built) has no :flush_waiters key and never accreted a waiter — nothing to settle.
  defp settle_waiters(state, _outcome) when not is_map_key(state, :flush_waiters), do: state
  defp settle_waiters(%{flush_waiters: []} = state, _outcome), do: state

  # How many times a flush_now waiter may re-kick a still-dirty flush before it is failed.
  # Each iteration is a fence (a lock GET, or a renew PUT in legacy mode) + a whole-file
  # VACUUM INTO + a full-object PUT, so an unbounded loop is expensive for every co-tenant.
  @max_flush_rekicks 8

  defp settle_waiters(state, :flushed) do
    cond do
      not unflushed?(state) ->
        for w <- state.flush_waiters, do: GenServer.reply(w, :ok)
        %{state | flush_waiters: [], flush_rekicks: 0}

      # Still dirty after a flush that had waiters. Re-kick, but BACKED OFF and BOUNDED
      # (expert review 2026-08-01 #32).
      #
      # This used to be a bare `send(self(), :durability_flush)` — no backoff, no jitter,
      # bypassing schedule_flush_backoff/1 — and two of the three paths that reach it are
      # FAILURE paths where the shard is dirty by construction (a `{:reconciled, nil}` verdict
      # and the flush-task `:DOWN` clause). So a failing store span the loop as fast as the
      # BEAM could spawn tasks: fence, VACUUM the whole file, PUT it, fail, immediately again,
      # for up to the 60s flush_now timeout. A crash-looping flush task was the sharpest
      # version — spawn, crash, :DOWN, respawn, with no delay at all.
      state.flush_rekicks < @max_flush_rekicks ->
        schedule_flush_backoff(%{state | flush_rekicks: state.flush_rekicks + 1})

      # Not converging. Tell the caller so it can retry deliberately, rather than holding it to
      # the full timeout while the node burns I/O on its behalf.
      true ->
        for w <- state.flush_waiters, do: GenServer.reply(w, {:error, :flush_not_converging})
        %{state | flush_waiters: [], flush_rekicks: 0}
    end
  end

  defp settle_waiters(state, {:error, _reason} = err) do
    for w <- state.flush_waiters, do: GenServer.reply(w, err)
    %{state | flush_waiters: []}
  end

  # Reset the consecutive-failure counter on any durable flush. No-op (and silent) on the happy
  # path; logs a one-line recovery only when clearing a real streak.
  defp clear_flush_failures(%{flush_failures: 0} = state), do: state

  defp clear_flush_failures(state) do
    Logger.info(
      "shard #{state.id}: durability flush recovered after #{state.flush_failures} consecutive failure(s)"
    )

    %{state | flush_failures: 0}
  end

  defp flush_failure_alert_threshold,
    do: Application.get_env(:fathom, :flush_failure_alert_threshold, 3)

  # Snapshot the live DB to a temp file and upload it (fenced by the object etag), keeping the
  # working copy. Returns `{:ok, new_etag}` when both the snapshot and the conditional upload
  # land, `{:error, :superseded}` when the object changed under us (a stealer flushed — the
  # caller self-fences), or `{:error, reason}` on a transient failure (the caller keeps the
  # shard dirty so the write isn't lost on a later idle-drop).
  defp snapshot_and_upload(state) do
    temp = "#{state.path}.snap.#{System.unique_integer([:positive])}"

    try do
      # Integrity BEFORE the snapshot: quarantine_corrupt!/2 renames the live path, so checking
      # after would spend a full VACUUM on a file we are about to refuse and move aside.
      with :ok <- verify_snapshot(state, temp),
           :ok <- snapshot(state.path, temp),
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

  # Integrity-gate the PERIODIC flush (expert review 2026-08-01 #14).
  #
  # Review 2026-07-14 #4 added a `quick_check` gate so a locally-corrupted database is never
  # flushed over the last good stored object — but it lived in exactly one place: the
  # checkpoint-then-raw-upload fast path of `upload_for_drop/1`, which runs only on the
  # idle/terminate drop. The periodic flush, which is the DOMINANT producer of the durable
  # object, went straight to the PUT with no check at all.
  #
  # The old justification was that `VACUUM INTO` reads every page so it fails on corruption by
  # itself. Only partly true, and wrong in the direction that matters: `VACUUM INTO` REBUILDS
  # indexes from table content, so the classic index/table divergence `quick_check` exists to
  # catch is silently NORMALISED AWAY rather than detected — and the normalised file is then
  # uploaded, correctly If-Match-fenced by the legitimate owner, as the new durable truth.
  #
  # Check the SNAPSHOT TEMP, not the live file: it is the artifact about to become the durable
  # object, and it is about to be fully read for the Content-MD5 anyway, so the page reads are
  # already paid for. Reuses the drop path's quarantine + refuse-the-flush behaviour, so the
  # last good stored object stays authoritative.
  # CHECKS THE LIVE FILE, NOT THE SNAPSHOT TEMP — and that distinction is the whole point.
  #
  # The review recommended checking the temp, on the reasoning that it is about to be read for
  # the Content-MD5 anyway so the pages are free. Measured before implementing, that is wrong:
  # `VACUUM INTO` REBUILDS indexes from table content, so it *repairs* index/table divergence on
  # the way out. Probe on a database with a deliberately corrupted index root page:
  #
  #     quick_check on the live db   -> "Tree 3 page 3 cell 199: Offset 23130 out of range …"
  #     VACUUM INTO                  -> OK
  #     quick_check on the VACUUMed copy -> "ok"      (200 rows intact)
  #
  # So the temp is clean by construction for exactly the corruption class `quick_check` exists
  # to catch, and checking it would have shipped a gate that never fires — while the divergence
  # is silently normalised away and uploaded, correctly If-Match-fenced, as the new durable
  # truth. The live file is the only place the evidence still exists.
  #
  # Cost is one full page scan per periodic flush, on a path that already reads the whole file
  # (VACUUM INTO) and the whole temp (MD5 + PUT). Shards are small by fathom's premise, which is
  # what makes that acceptable; `:verify_flush_integrity` turns it off for a deployment that
  # disagrees.
  defp verify_snapshot(state, _temp) do
    if verify_flush_integrity?() do
      case verify_integrity(state.path) do
        :ok ->
          :ok

        {:error, reason} ->
          quarantine_corrupt!(state, reason)
          {:error, {:corrupt_local, reason}}
      end
    else
      :ok
    end
  end

  defp verify_flush_integrity?,
    do: Application.get_env(:fathom, :verify_flush_integrity, true)

  # Wait out an in-flight durability-flush task and fold its result into the state
  # (expert review #27) — used on the terminate path so the final drop-flush never
  # races the task's conditional PUT. A task error/timeout leaves the state as-is
  # (still dirty ⇒ the drop-flush uploads everything itself).
  #
  # THE SHAPE MATTERS (expert review 2026-08-01 #3). This used to match `{:ok, {:ok, etag}}`,
  # which `fenced_flush/2` stopped producing at f8ecf63 ("run the periodic-flush fence
  # off-process, audit #18") — that commit changed the reply to `{:fenced, verdict, updates}`
  # and updated handle_info/2 but not this function. The match was therefore UNREACHABLE and
  # every terminate fell to the `_` clause, proceeding with a stale `state.etag` and stale
  # `flushed_through`. The drop-flush then PUT with the pre-task etag against an object the
  # task had already advanced ⇒ deterministic 412 ⇒ the "lock still ours, object is durable"
  # branch ⇒ `drop_local` of every write committed after the task's snapshot. Silent,
  # unrecoverable, on the ordinary rolling-deploy path, under a log line asserting durability.
  #
  # The fold is now shared with handle_info/2 so the two cannot drift again.
  defp settle_flush_task(%{flush_task: nil} = state), do: state

  defp settle_flush_task(%{flush_task: task} = state) do
    settled =
      case Task.yield(task, settle_yield_ms()) || Task.shutdown(task) do
        {:ok, {:fenced, flush_result, updates}} ->
          {status, state} =
            state |> Map.merge(updates) |> apply_flush_verdict(flush_result)

          # A superseded verdict means a peer owns the shard now. Carry that into the state so
          # flush_and_drop/1 quarantines instead of clobbering the new owner — terminate/2 has
          # already dispatched, so the `lease_lost: true` clause can no longer catch it.
          %{state | flush_task: nil, lease_lost: status == :superseded or state.lease_lost}

        # Task crashed, timed out, or was shut down: leave the flush state untouched. Still
        # dirty ⇒ the drop-flush uploads everything itself.
        _ ->
          %{state | flush_task: nil}
      end

    release_flush_slot(settled)
  end

  # Fold a completed flush task's verdict into local state. Pure w.r.t. the fence — all the
  # Storage I/O already ran in the task — so both the live handler and the terminate-path
  # settle can call it. Returns `{status, state}`.
  defp apply_flush_verdict(state, {:ok, new_etag}) do
    # Uploaded; advance the fence etag, the provenance sidecar, and the flushed watermark
    # captured when the task started (clears dirty up to that point).
    write_etag_sidecar(state.path, new_etag)

    state = %{
      state
      | flushed_through: state.flush_pending,
        counter_gen: state.flush_pending_gen,
        etag: new_etag
    }

    # Advance the published watermark (clears dirty up to flush_pending for the RPO reader).
    FlushWatermark.record(state.id, state.flushed_through, state.counter_gen)
    # Persist the durable-flush time so it survives node death (#28) — off the hot path.
    Fathom.Directory.Recorder.record_flush(state.id)
    {:flushed, clear_flush_failures(state)}
  end

  # The data PUT's If-Match failed (412) and the task's lock re-check found the lock STILL
  # OURS. That tells us the object CHANGED — it does NOT tell us the object holds THIS
  # attempt's bytes (expert review 2026-08-01 #4). The benign reading (our own PUT landed and
  # its response was lost) and the lossy one are indistinguishable from here:
  #
  #   t0  flush A PUTs If-Match E0. It LANDS; the response is lost (a documented real case on
  #       this path — see storage/s3.ex's transport-error handling). Shard stays dirty,
  #       state.etag stays E0.
  #   t1  the tenant commits more writes.
  #   t2  flush B PUTs If-Match E0 ⇒ 412 (object is at E1) ⇒ lock still ours ⇒ "reconciled".
  #
  # Advancing `flushed_through` here — which is what this branch used to do — marks the shard
  # CLEAN while the object holds only t0's bytes, and the next idle `drop_clean` unlinks the
  # rest. So: resync the fence etag ONLY and stay dirty. The next interval re-flushes with the
  # correct etag. Cost in the benign case is one extra PUT; cost of the old behaviour in the
  # lossy case was silent, unrecoverable loss that `loss-report` reported as safe.
  #
  # The sidecar IS advanced: we are the single writer, so our local copy is a descendant of
  # whatever the object currently holds — same lineage, plus more writes. That is exactly what
  # the provenance check needs to not false-quarantine a fork.
  defp apply_flush_verdict(state, {:reconciled, object_etag}) when not is_nil(object_etag) do
    write_etag_sidecar(state.path, object_etag)
    {:dirty, clear_flush_failures(%{state | etag: object_etag})}
  end

  # 412 + lock ours, but the object's etag came back nil/unreadable: keep serving with no etag
  # resync; stays dirty and retries next interval.
  defp apply_flush_verdict(state, {:reconciled, nil}), do: {:dirty, state}

  defp apply_flush_verdict(state, :superseded), do: {:superseded, state}

  # Snapshot/PUT (or the reconcile's lock read) failed transiently: don't advance the
  # watermark, so it stays dirty and the next interval retries — and a later idle-drop
  # flushes before dropping instead of deleting un-stored writes.
  defp apply_flush_verdict(state, {:error, reason}),
    do: {{:error, reason}, record_flush_failure(state, reason)}

  # Release the FlushGate slot this flush reserved, exactly once (expert review #17). Called at
  # every site that clears flush_task — completion, task crash, and terminate — so the node-wide
  # counter tracks live flush tasks. A no-op when no slot was held (cap disabled, or we didn't
  # acquire), so it's safe to call unconditionally on each of those paths.
  defp release_flush_slot(%{flush_slot_held: true} = state) do
    FlushGate.release()
    %{state | flush_slot_held: false}
  end

  defp release_flush_slot(state), do: state

  # A transactionally-consistent copy of the live shard, safe to run while writers
  # are active (unlike a raw file copy). `VACUUM INTO` requires a string literal
  # path, so the destination (built from a validated shard id) is single-quote
  # escaped defensively.
  defp snapshot(path, dest) do
    with {:ok, conn} <- Connection.open(path) do
      # The snapshot temp inherits this connection's safety_level (SQLite's sqlite3RunVacuum:
      # `pgflags = db->aDb[iDb].safety_level | ...`), so at synchronous=FULL every periodic flush
      # force-fsynced a full shard-sized file that `snapshot_and_upload/1` unlinks seconds later
      # (expert review 2026-07-24 #11). At ~640 KiB/shard × 1,000 write-active shards that is
      # ~128 MB per interval of pure-waste synchronous device writes, plus the SSD endurance.
      #
      # The temp has NO durability requirement: it is read back in-process for the Content-MD5 and
      # the PUT, and a crash mid-snapshot just leaves the shard dirty to be re-snapshotted next
      # interval. The durable artifact is the S3 object, still gated by content-md5 over these same
      # bytes. VACUUM INTO takes only a READ transaction on the source, so the live shard's WAL +
      # per-commit synchronous=FULL is untouched — the RPO contract's local layer is unaffected.
      #
      # Scoped to this connection only. Emphatically NOT for `checkpoint/1`: a checkpoint at
      # synchronous=OFF can corrupt the main database on power loss.
      Connection.exec(conn, "PRAGMA synchronous=OFF")

      result = Connection.query(conn, "VACUUM INTO '#{String.replace(dest, "'", "''")}'", [])

      # RESTORE IT BEFORE ANYTHING ELSE TOUCHES THIS CONNECTION (expert review 2026-08-01 #5).
      # The relaxation above is for the throwaway VACUUM temp only, but it stayed in effect for
      # the checkpoint and the close below — violating the rule the comment right above states.
      #
      # `synchronous` is a pager property and SQLite derives checkpoint sync flags from it
      # (`pPager->walSyncFlags`), so at OFF a checkpoint neither fsyncs the WAL before backfill
      # nor fsyncs the main database after. TWO checkpoints ran that way: the explicit PASSIVE,
      # and SQLite's close-time checkpoint — which, on the last connection to a WAL database,
      # also UNLINKS `-wal`/`-shm`. And this is frequently the last connection: the periodic
      # flush fires on any dirty shard, which is routinely one with zero checked-out streams.
      #
      # Main-database pages written without fsync, followed by deletion of the only recovery
      # source, is a torn database on power loss — silent, unrecoverable, and then uploaded as
      # the durable object.
      Connection.exec(conn, "PRAGMA synchronous=FULL")

      # Checkpoint here, where nobody is waiting (expert review 2026-07-24 #4). Without this the
      # only thing truncating the WAL in steady state was SQLite's autocheckpoint, which runs
      # INLINE INSIDE A COMMITTING TENANT STATEMENT on whichever connection happens to cross the
      # threshold — a periodic multi-ms-to-tens-of-ms spike billed to an arbitrary client query,
      # and under synchronous=FULL it fsyncs the main database too. Worse, the VACUUM INTO above
      # is a long-lived reader that HOLDS BACK that passive checkpoint (docs/durability.md notes
      # the same interaction from the other side), so the WAL kept growing past the threshold and
      # the tenant that eventually won the race paid a larger checkpoint than the default implies.
      #
      # PASSIVE specifically: it never blocks writers and never waits on readers — it moves what it
      # can and returns — so it cannot add latency to a concurrent tenant. A `busy` result is the
      # normal "readers active" outcome and is ignored; the next interval retries. Runs only after
      # a successful snapshot, i.e. after this connection's read transaction has ended, which is
      # what unblocks the checkpointer in the first place.
      if match?({:ok, _}, result),
        do: Connection.query(conn, "PRAGMA wal_checkpoint(PASSIVE)", [])

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

  # Public (@doc false) so Fathom.Shard.TempReaper sweeps the same directory this
  # coordinator writes its temps into, without duplicating the config/default.
  @doc false
  def data_dir do
    Application.get_env(
      :fathom,
      :shard_data_dir,
      Path.join(System.tmp_dir!(), "fathom_shards")
    )
  end

  # The three quarantine-file kinds the coordinator writes to preserve acked-but-unflushed / corrupt
  # local copies (`.db.fenced.*` self-fence, `.db.forked.*` diverged lineage, `.db.corrupt.*` failed
  # quick_check). Naming lives at each quarantine_* site; this is the one enumerator (expert review
  # #23), shared by the TempReaper retention sweep and `mix fathom.shard quarantines`.
  @quarantine_kinds ["fenced", "forked", "corrupt"]

  @doc false
  def quarantine_kinds, do: @quarantine_kinds

  @doc false
  @spec quarantine_files(Path.t()) :: [Path.t()]
  def quarantine_files(dir \\ data_dir()) do
    for kind <- @quarantine_kinds, f <- Path.wildcard(Path.join(dir, "*.db.#{kind}.*")), do: f
  end

  # Round-2 #26: spread the post-lapse revalidation fan-out across the jitter window.
  defp lapse_jitter_ms, do: Application.get_env(:fathom, :lapse_revalidate_jitter_ms, 2_000)

  defp idle_ms, do: Application.get_env(:fathom, :shard_idle_ms, @default_idle_ms)
  defp lease_ttl_ms, do: Application.get_env(:fathom, :shard_lease_ttl_ms, @default_lease_ttl_ms)

  defp checkout_timeout,
    do: Application.get_env(:fathom, :shard_checkout_timeout_ms, @default_checkout_timeout)

  # Per-shard concurrent-stream cap (#26); unset ⇒ unlimited.
  defp max_checkouts, do: Application.get_env(:fathom, :max_checkouts_per_shard)

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

  defp flush_jitter_ratio,
    do: Application.get_env(:fathom, :shard_flush_jitter_ratio, @default_flush_jitter_ratio)

  defp flush_backoff_ms,
    do: Application.get_env(:fathom, :shard_flush_backoff_ms, @default_flush_backoff_ms)

  # Stamp the idle instant and arm ONE timer only when none is pending (review
  # 2026-07-23 #17) — the :idle_timeout handler re-arms for the remainder when the
  # stamp has moved, so a hot 1↔0 stream cycle pays a map put here instead of a
  # cancel_timer + send_after pair on every cycle.
  defp schedule_idle(state) do
    state = %{state | idle_since: System.monotonic_time(:millisecond)}

    if state.timer,
      do: state,
      else: %{state | timer: Process.send_after(self(), :idle_timeout, state.idle_ms)}
  end

  defp cancel_idle(%{timer: nil} = state), do: state

  defp cancel_idle(%{timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | timer: nil}
  end

  # Renew well inside the TTL (every third) so a couple of transient failures
  # don't lapse the lease.
  defp apply_renew_result(result, state) do
    case result do
      {:ok, lease} ->
        emit_lease(:renewed, state.id)
        {:noreply, schedule_renew(clear_write_fence(%{state | lease: lease}))}

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

  # Idle AND clean ⇒ provably unwritable until the next checkout, so don't burn a timer
  # (expert review 2026-07-24 #10). WriteCounter.bump/1 is reachable only from
  # ShardExecutor.run_statement/3 and the abnormal-stream-death path, both of which require a
  # checked-out connection — so a coordinator with zero conns and nothing unflushed cannot become
  # dirty before `:checkout` re-arms it. At 30k coordinators/node the old unconditional re-arm was
  # ~6,000 wakeups/s of pure no-op (timer insert + :rand + send + dispatch + an ETS lookup + a heap
  # touch that defeats page-out), and — the bigger cost — it made `hibernate_after` unusable at any
  # value, since a message always arrived within one interval.
  #
  # RPO is unchanged: the contract bounds loss of *acknowledged writes*, and this state has none
  # and can acquire none. A DIRTY coordinator keeps ticking exactly as before. Writes that bypass
  # the Hrana path (the migrator, `mix fathom.shard`) were already invisible to WriteCounter.
  defp schedule_flush(%{conns: conns} = state) when map_size(conns) == 0 do
    if unflushed?(state), do: do_schedule_flush(state), else: cancel_flush(state)
  end

  # BUSY BUT CLEAN — a held stream doing only reads (expert review 2026-08-01 #42). The
  # argument review 2026-07-24 #10 made for the idle case applies verbatim, and the
  # served-density ceiling is where it bites: ~2,000 no-op wakeups/s per node at 10k served
  # shards, every one of those coordinators pinned out of `hibernate_after` with its heap
  # resident.
  #
  # A clean busy shard can become dirty only via a write, and `ShardExecutor` casts
  # `:became_dirty` on the FIRST write of a checkout — at most one message per stream, strictly
  # fewer than today for read-heavy traffic — which re-arms the timer. The poll is now
  # edge-triggered rather than periodic.
  #
  # The SAFETY NET matters more than the optimisation: this is the durability timer, and a lost
  # signal would mean un-flushed acknowledged writes. So a clean busy shard does not disarm
  # outright, it re-arms at a much slower cadence. A missed signal then costs up to
  # @clean_poll_multiplier intervals of extra RPO, never unbounded loss.
  defp schedule_flush(state) do
    if unflushed?(state),
      do: do_schedule_flush(state),
      else: do_schedule_flush(state, @clean_poll_multiplier)
  end

  # Arm the periodic durability flush. A non-positive interval disables it
  # (idle-only durability).
  defp do_schedule_flush(state, multiplier \\ 1) do
    state = cancel_flush(state)

    case flush_interval_ms() do
      interval when is_integer(interval) and interval > 0 ->
        %{
          state
          | flush_timer:
              Process.send_after(
                self(),
                :durability_flush,
                jitter_interval(interval * multiplier)
              )
        }

      _ ->
        state
    end
  end

  # Reschedule after the node-wide flush cap (Fathom.Shard.FlushGate) refused a slot (#17): a short,
  # jittered backoff so the shard retries soon (it stayed dirty) without the backed-off shards
  # forming their own lockstep retry herd. Bounded well below the full interval.
  defp schedule_flush_backoff(state) do
    state = cancel_flush(state)
    delay = jitter_interval(flush_backoff_ms())
    %{state | flush_timer: Process.send_after(self(), :durability_flush, delay)}
  end

  # Decorrelate flush timers across coordinators (expert review #17): a mass re-home phase-aligns
  # their fixed-interval timers, so every interval N snapshots + PUTs fire in lockstep and starve
  # cold-open pulls. Spread each scheduled delay by ±ratio so the timers drift apart. Ratio 0
  # disables it (deterministic tests). :rand is per-process-seeded, so each coordinator picks
  # independently.
  defp jitter_interval(base) when is_integer(base) and base > 0 do
    case flush_jitter_ratio() do
      ratio when is_number(ratio) and ratio > 0 ->
        spread = round(base * min(ratio, 1.0))
        if spread > 0, do: max(1, base - spread + :rand.uniform(2 * spread + 1) - 1), else: base

      _ ->
        base
    end
  end

  defp jitter_interval(base), do: base

  defp cancel_flush(%{flush_timer: nil} = state), do: state

  defp cancel_flush(%{flush_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | flush_timer: nil}
  end
end
