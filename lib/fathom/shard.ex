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

  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Recovery

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

  # Target node-wide flush-gate PROBE rate, used to scale the refusal backoff to the open-shard
  # count (expert review 2026-08-26 #16). 2 000/s leaves small fleets on the fixed 250 ms floor
  # (100 shards derives 50 ms, which the floor overrides) and only starts stretching the backoff
  # once the herd is big enough for the poll itself to be the cost.
  @default_gate_probe_target_per_sec 2_000
  @pull_timeout 60_000
  # Synchronous force-flush (`:flush_now`) budget: a checkpoint + fenced full-file PUT (possibly
  # queued behind sibling flushes through the shared Finch pool) at real S3 latency. Generous so a
  # flush-before-fork of a large template doesn't time out; bounded so a hung store can't wedge a caller.
  @flush_now_timeout 60_000

  # Target rate for heartbeat-lapse revalidations, in `check_lease` GETs per second, used to derive
  # the jitter spread from the open-shard count (expert review 2026-08-26 #13b). 500/s is the value
  # at which a 1 000-shard fleet gets exactly the 2 000 ms window that used to be hardcoded for
  # every fleet size. See `derived_lapse_spread_ms/0`.
  @default_lapse_revalidations_per_sec 500
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

  @spec start_link(Fathom.ShardId.t()) :: GenServer.on_start()
  def start_link(shard_id) do
    GenServer.start_link(__MODULE__, shard_id,
      name: via(shard_id),
      spawn_opt: [fullsweep_after: 0],
      hibernate_after: @hibernate_after_ms
    )
  end

  @doc "Registry `:via` tuple addressing the coordinator for `shard_id`."
  @spec via(Fathom.ShardId.t()) :: {:via, module(), {module(), Fathom.ShardId.t()}}
  def via(shard_id) do
    {:via, Registry, {Fathom.ShardRegistry, shard_id}}
  end

  @doc """
  Checks out the shard for the calling process: ensures the file is available,
  registers the caller as an active connection (so the shard won't flush/idle
  while it is in use), and returns `{:ok, ref, path}`. Pass `ref` to `checkin/2`
  when the connection closes.
  """
  @spec checkout(pid()) :: {:ok, reference(), Path.t()} | {:error, term()}
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
  @spec checkin(pid(), reference()) :: :ok
  def checkin(pid, ref) when is_pid(pid), do: GenServer.cast(pid, {:checkin, ref})

  @doc """
  Whether the shard holds local writes not yet flushed to storage — i.e. its write counter
  (`Fathom.Shard.WriteCounter`, bumped lock-free per write by `Fathom.ShardExecutor`) is ahead of
  the coordinator's `flushed_through` watermark. Used by tests; the coordinator itself checks this
  internally on each flush decision. Finding #27 replaced a per-write cast with this ETS signal.
  """
  @spec dirty?(pid()) :: boolean()
  def dirty?(pid) when is_pid(pid), do: GenServer.call(pid, :dirty?)

  @doc """
  This coordinator's lease epoch — A2 replication's fencing token.

  `{:error, :no_lease}` when the shard is not currently held, which a replication session must
  treat as "do not ship": frames from a node without a lease are exactly what the follower's epoch
  check exists to refuse.
  """
  @spec epoch(pid()) :: {:ok, non_neg_integer()} | {:error, :no_lease}
  def epoch(pid) when is_pid(pid), do: GenServer.call(pid, :epoch)

  @doc """
  This coordinator's LINEAGE for the wire, or `0` when it has none to state.

  A different number from `epoch/1`, and the distinction is expert review 2026-08-24 #12. The lock
  epoch resets to 1 on every clean release, so it cannot order a replica against the stored object;
  the lineage is monotonic across a shard's whole life and is what the object's position stamp
  carries. A seed ships both — see `Fathom.Shard.Replication.Protocol`'s `@seed_begin_lin`.

  `0` for `:disabled` (replication off, so `open_lineage/1` never read one) and `:unknown` (the
  HEAD failed). Both mean "no claim", and `Promote.fresher?/2` refuses to rank a replica carrying
  it rather than guessing.
  """
  @spec lineage(pid()) :: non_neg_integer()
  def lineage(pid) when is_pid(pid), do: GenServer.call(pid, :lineage)

  @doc """
  Force-flushes the coordinator's current on-disk state to storage WITHOUT dropping or stopping
  it (it keeps serving) — the flush-before-fork primitive. Blocks until the shard is durably
  clean; returns `:ok`, or `{:error, reason}` on a flush error / lease steal. See
  `Fathom.Shards.flush/1` for the by-id, registry-resolving wrapper.
  """
  @spec flush_now(pid()) :: :ok | {:error, term()}
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
  @spec request_drain(pid(), non_neg_integer(), pid()) :: :ok
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
        # EVERYTHING from here to the built state runs while we HOLD the lock, so any abnormal
        # exit in between strands it. The comment on `fork_task` above describes this hazard
        # precisely and review 2026-08-01 #33 fixed ONE instance of it (a raise inside
        # `fork_evidence/2`) by rescuing inside that Task. The class survived: `resolve_fork/4`,
        # `revalidate_takeover/5` and `promote_pull/2` all run storage/File calls DIRECTLY in this
        # process, unrescued. `start_pull/2` is rescued and returns `{:error, _}`, which reaches
        # the `else` branch of `open_with_lease/8` and releases — but a raise from the coordinator's
        # own HEAD (`Req.TransportError` under load is the realistic trigger) does not.
        #
        # Found on the rig 2026-08-04: a 300-tenant rollout stranded `roll8` with NO local file, NO
        # coordinator, NO quarantine copy, and the lock held by its own node — the signature of an
        # open that died after the acquire — with `Req.TransportError socket closed` warnings in
        # the same window. `terminate/2` cannot save it: `handle_continue` has not returned, so the
        # GenServer still holds the pre-open `%{id: shard_id}` state and the catch-all clause has
        # no lease to release.
        #
        # So guard the whole region rather than each call: release, then stop with `{:shutdown, _}`
        # like every other open failure (a `:transient` child is NOT restarted, and `checkout/1`
        # maps the exit to `{:error, _}`), instead of crashing into a restart loop that would
        # re-acquire and re-strand.
        try do
          open_after_acquire(
            shard_id,
            path,
            owner,
            ttl,
            lease,
            pull_task,
            fork_task,
            file?,
            acquire_gen,
            open_started
          )
        rescue
          e -> abandon_open(shard_id, lease, state, e, __STACKTRACE__)
        catch
          :exit, reason -> abandon_open(shard_id, lease, state, {:exit, reason}, __STACKTRACE__)
        end

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

  # The post-acquire half of the open, extracted so the whole lease-holding region sits inside one
  # try/rescue above. Unchanged logic — only the enclosing scope moved.
  defp open_after_acquire(
         shard_id,
         path,
         owner,
         ttl,
         lease,
         pull_task,
         fork_task,
         file?,
         acquire_gen,
         open_started
       ) do
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
  end

  # The lock is ours and the open died before the coordinator could ever hold it in state. Give it
  # back, loudly. Without this the lock names a LIVE node forever (`owner_live?` reads our own fresh
  # heartbeat), so the shard serves — our own next open reclaims at the same incarnation — but no
  # FOREIGN owner can ever take it: no failover, and no migration (the migrator acquires as
  # `migrator@<node>@<token>`, a different owner string even on this same node).
  #
  # `release_lease` is itself a storage call and this path exists BECAUSE storage is misbehaving, so
  # its own failure must not mask the original error. Log at error either way: an open that crashed
  # after taking the lock is never routine.
  defp abandon_open(shard_id, lease, state, reason, stacktrace) do
    released =
      try do
        Storage.release_lease(shard_id, lease)
      rescue
        e -> {:error, e}
      catch
        :exit, r -> {:error, {:exit, r}}
      end

    Logger.error(
      "shard #{shard_id}: open FAILED after the lease was acquired (#{inspect(reason)}); " <>
        "lease release: #{inspect(released)}\n" <> Exception.format_stacktrace(stacktrace)
    )

    :telemetry.execute(
      [:fathom, :shard, :open, :failed],
      %{count: 1},
      %{shard_id: shard_id, reason: reason, released: released}
    )

    {:stop, {:shutdown, {:open_failed, reason}}, state}
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
         {:ok, etag1} <- revalidate_takeover(shard_id, path, lease, etag0, warm?) do
      # NOT a `with` clause, and returning a bare etag rather than `{:ok, etag}`. Both are for the
      # same measured reason: `handle_continue/2` garbage-collects this coordinator early to shrink
      # its init high-water mark, so ANY allocation after that point stays resident for the shard's
      # life. Wrapping this in a tuple per open cost +36% fanout_kb_per_shard (3.93 → 5.45 KiB) and
      # the bench gate refused the commit. With the gate off this is now one `Application.get_env`
      # and the binding we already had.
      #
      # It cannot fail the open either, which is why it needs no error channel: a promotion that
      # does not happen leaves the ordinary path, and the ordinary path is correct.
      # Computed BEFORE the promote, and passed into it, so that a promoted replica and every
      # later flush of this coordinator stamp the SAME lineage — they are one ownership. See
      # open_lineage/1 for why it is read once and never recomputed.
      lineage = open_lineage(shard_id)
      etag = maybe_promote_replica(shard_id, path, lease, etag1, lineage)

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
        # The lineage counter every position stamp from this coordinator carries in its `epoch`
        # slot (expert review 2026-08-20 #8). Fixed for the coordinator's life; see open_lineage/1
        # and stamp_epoch/1. `:disabled` when replication is off, which is the no-op default.
        lineage: lineage,
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
        # Was `flush_timer` armed at the SLOW (clean-poll) cadence? Read by the `:became_dirty`
        # handler to tell "this shard is loafing and a write should speed it up" from "a flush is
        # already counting down and must not be restarted" (expert review 2026-08-24 #7).
        flush_timer_slow?: false,
        # Has ANY write landed while the current set of connections has been held? Sticky for the
        # life of the checkout, cleared when the last connection checks in (expert review
        # 2026-08-26 #4). This is what distinguishes a genuinely read-only held stream — the case
        # the slow durability cadence exists for — from a writing one.
        wrote_during_checkout?: false,
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
        # The heartbeat-lapse revalidation EPISODE, as one tri-state (expert review 2026-08-26
        # #13a): `false` = none in progress, `true` = the jitter timer is armed, `%Task{}` = the
        # off-process `check_lease` GET is in flight.
        #
        # ONE FIELD ON PURPOSE, and the reason is measured, not stylistic. Adding a separate
        # `lapse_task` key made this map 31 keys and cost **+51% on `fanout_gc_kb_per_shard`**
        # (3.65 -> 5.49 KiB/shard, reproduced three times, and reproduced by that ONE LINE against
        # an otherwise-unmodified HEAD). The coordinator's process heap sits right on an Erlang
        # heap size-class boundary, so one more word per process rounds every coordinator up to the
        # next class — ~1.8 KiB each, which at fathom's scale IS the node-density floor. The bench
        # gate blocked it, correctly.
        #
        # So: before adding a field here, bench it. This map is not free to grow.
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

      # Lift any write fence a PREVIOUS coordinator for this shard left behind (expert review
      # 2026-08-20 #6). The fence is a node-global ETS row published by note_not_valid/1, and every
      # WriteFence.forget/1 call site is inside terminate/2 — which is not guaranteed to run. The
      # clause that would run for a fenced shard does a fence + checkpoint + full-object PUT against
      # the object store, and that state is reached precisely when the object store is UNREACHABLE:
      # the case most likely to blow the :shard_shutdown_ms budget and be brutally killed. The row
      # then survived, and no successor could clear it either, because fresh state starts
      # `not_valid_since: nil` and clear_write_fence/1 short-circuits on exactly that. Result: the
      # tenant read fine and every write 503'd FILO_STALE_LEASE forever, across coordinator
      # restarts and across the partition healing, recoverable only by restarting the node — a
      # permanent outage produced by the circuit breaker that exists to BOUND loss.
      #
      # Unconditional and idempotent by design: we are here only because acquire_lease succeeded,
      # so this coordinator holds a valid lease and is by definition not stealable. Clearing is
      # always correct at this point, which is why it needs no state to consult — consulting state
      # is what failed.
      Fathom.Shard.WriteFence.unfence(state.id)

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
              :ok ->
                {:ok, etag}

              {:ok, _} = ok ->
                ok

              # THE COLD-PULL FALLBACK CAN LEGITIMATELY FIND NO OBJECT (expert review 2026-08-20
              # #34). `promote_warm_cache/4` falls back to `Storage.pull/2` on any doubt about the
              # cache, and that function's documented return set includes `{:absent, etag_or_nil}`
              # — the shape review 2026-08-01 #24 introduced SPECIFICALLY because collapsing it
              # into `{:ok, _}` fabricated empty databases. This case matched only `:ok`,
              # `{:ok, _}` and `{:error, _}`, so `{:absent, nil}` raised `CaseClauseError`, was
              # caught by `start_pull/2`'s rescue as `{:error, {:pull_exception, _}}`, and
              # `open_with_lease/8` released the lease and failed the checkout — turning a benign
              # brand-new-shard state into an error and a spurious `[:fathom, :shard, :open,
              # :failed]`.
              #
              # PASSED THROUGH rather than folded to `{:ok, nil}`: `await_pull/3` has its own
              # `{:absent, etag}` clause, and folding would hide the distinction from
              # `promote_pull/2`, which uses it to stamp the "derived from no object" sentinel
              # sidecar so the first flush is a conditional CREATE rather than a blind overwrite.
              #
              # Dialyzer cannot see this class: `Storage.pull/2` dispatches through
              # `backend().pull(...)`, so its success typing is `term()` — the "wrappers over
              # dynamic dispatch are not checked" case AGENTS.md § Typing records.
              {:absent, _} = absent ->
                absent

              {:error, _} = error ->
                error
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
  # `Storage.atomic_copy/2`, NOT a bare `File.cp/2` (expert review 2026-08-24 #9). Every other
  # path that materializes a shard file fsyncs before the rename, and `Storage.with_atomic_temp/2`
  # states why: rename-without-data-fsync is atomic against a process crash but NOT against power
  # loss — afterwards the name can exist with zero-length or partial content (guaranteed on XFS,
  # heuristic on ext4). This 304 fast path was the one exception, and it is the WORST place for it:
  # `promote_pull/2` then writes the provenance sidecar with the store's current etag and renames
  # the temp onto `<id>.db`, so a power cut in that window leaves a torn file whose sidecar MATCHES
  # the stored object. The next open reads `:match`, opens warm, and seeds dirty. A zero-length
  # file is a valid empty SQLite database and `PRAGMA quick_check` returns `ok`, so
  # `verify_and_snapshot/2`'s integrity gate never fires — the tenant is served an empty database and
  # the next periodic flush PUTs it over the good stored object with a valid If-Match.
  #
  # The cold-pull fallback on the `else` branch was already fsynced (`Storage.pull/2` promotes
  # through `promote_temp/2`), so only the warm path — the failover path the follower exists to
  # accelerate — was exposed. The inode/etag TOCTOU re-check below is unaffected and stays.
  defp promote_warm_cache(shard_id, temp, etag, pre_stat) do
    with :ok <- Storage.atomic_copy(WarmFollower.cache_path(shard_id), temp),
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
  #
  # BUT IT NO LONGER SWALLOWS SILENTLY (expert review 2026-08-26 #40). A bare `rescue _ -> :ok`
  # means a PubSub misconfiguration turns proactive revalidation off by construction, with a green
  # suite and no log: a superseded coordinator then keeps ACKing writes until its next flush, which
  # is exactly the defect review #34 was written to fix. The failure is still non-fatal — the
  # flush-time fence remains the hard guard — but it is now visible.
  defp subscribe_lapse do
    Phoenix.PubSub.subscribe(Fathom.PubSub, Heartbeat.topic())
  rescue
    e -> lapse_subscribe_failed(e)
  catch
    :exit, reason -> lapse_subscribe_failed(reason)
  end

  defp lapse_subscribe_failed(reason) do
    Logger.warning(
      "shard: could not subscribe to the heartbeat lapse topic (#{inspect(reason)}); this " <>
        "coordinator will NOT revalidate proactively on a lapse and falls back to the flush-time " <>
        "fence, so a superseded lease is noticed up to one flush interval later"
    )

    :ok
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

  # The lease epoch, for A2 replication's fence (`Fathom.Shard.Replication`). Read ONCE per
  # replication session and cached there, not per commit — a GenServer hop on every write would
  # put this coordinator's mailbox in front of the shard's write path, which is the cost
  # WriteCounter's lock-free ETS exists to avoid. The session monitors this process and stops when
  # it does, so a cached epoch can never outlive the ownership it describes.
  def handle_call(:epoch, _from, %{lease: %{epoch: epoch}} = state),
    do: {:reply, {:ok, epoch}, state}

  def handle_call(:epoch, _from, state), do: {:reply, {:error, :no_lease}, state}

  # `:disabled` / `:unknown` both mean "no lineage to claim" and travel as 0 — see `lineage/1`.
  def handle_call(:lineage, _from, %{lineage: n} = state) when is_integer(n),
    do: {:reply, n, state}

  def handle_call(:lineage, _from, state), do: {:reply, 0, state}

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
  #
  # AN EDGE, NOT A RESCHEDULE (expert review 2026-08-24 #7). This used to call
  # `schedule_flush/1` unconditionally, and for a shard that is already dirty that routes to
  # `do_schedule_flush/2`, whose first act is `cancel_flush/1` — cancelling the pending
  # `:durability_flush` and arming a brand-new FULL interval.
  #
  # `ShardExecutor.signal_dirty_once/2` keys its "already signalled" flag on the CONNECTION, and
  # every Hrana stream is its own `Filo.Stream` process with its own connection — so this cast
  # arrives once per WRITING STREAM, not once per shard. django-libsql under Django's default
  # `CONN_MAX_AGE=0` opens a fresh stream per request, so a tenant whose streams turn over faster
  # than `:shard_flush_interval_ms` (5 s) restarted the countdown before it could ever fire. The
  # periodic durability flush — the thing that bounds the loss window to the interval — therefore
  # never ran on the hottest, most write-active shards, the ones it exists for. Those flushed only
  # at idle-drop (60 s of ZERO streams), at drain, or at a clean terminate, so on node death the
  # RPO for a continuously busy tenant was the whole session.
  #
  # The checkout path already knew this hazard: it re-arms only `if state.flush_timer == nil`,
  # commented "never per stream cycle". This is the same guard, widened by one case so the
  # slow→fast transition #42 exists for still happens: re-arm when there is no timer, or when the
  # armed one is the CLEAN-POLL timer. A fast timer already counting down is left alone.
  #
  # Note the tempting one-liner — dropping `cancel_flush/1` from `do_schedule_flush/2` — is wrong:
  # every other caller depends on it, and removing it leaks timers everywhere.
  #
  # THE STICKY HALF (expert review 2026-08-26 #4). `ShardExecutor`'s `signal_dirty_once/2` is keyed
  # on the CONNECTION in the stream's process dictionary, so this cast arrives at most ONCE for the
  # lifetime of that stream. The re-arm above is therefore not enough on its own: a shard re-enters
  # the slow cadence every time a flush leaves it clean, and the second and subsequent writes on the
  # same stream send nothing to lift it again.
  #
  #   t=0  write → this cast → fast timer
  #   t=5  flush lands, shard now clean → schedule_flush → 50 s timer
  #   t=6  same stream writes again → signal_dirty_once/2 already flagged → NO CAST
  #        …dirty, with a 50 s timer armed
  #
  # So `docs/durability.md`'s "loss ≈ the flush interval" silently became 10× the interval for any
  # stream that writes less often than the interval — the django-libsql WebSocket shape AGENTS.md
  # calls the primary production client path. Invisible under `CONN_MAX_AGE=0`, where each request
  # is a fresh stream and the flag resets, which is what the tests and the chaos rig use.
  #
  # One cast per stream is exactly enough to set a STICKY flag, and stickiness is the right shape:
  # the slow cadence exists for a held stream doing only READS, so "this checkout has written" is
  # precisely the condition that should disqualify it. Cleared when the last connection checks in
  # (`release/2`), so a later read-only checkout gets the optimisation back.
  def handle_cast(:became_dirty, state) do
    state = %{state | wrote_during_checkout?: true}

    if state.flush_timer == nil or state.flush_timer_slow? do
      {:noreply, schedule_flush(state)}
    else
      {:noreply, state}
    end
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
            {:noreply, settle_flushed_and_reschedule(state)}

          # Still dirty — re-kick for any flush_now waiter (else it waits a whole flush interval).
          {:dirty, state} ->
            {:noreply, settle_flushed_and_reschedule(state)}

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

  # The lapse-revalidation task's reply (expert review 2026-08-26 #13a). Clearing the field to
  # `false` ends the episode, so a later lapse re-arms the timer normally.
  def handle_info({ref, result}, %{lapse_revalidate_pending: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    apply_lapse_verdict(result, %{state | lapse_revalidate_pending: false})
  end

  # The renewal task crashed: treat as transient and retry on the next tick.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{renew_task: %Task{ref: ref}} = state) do
    Logger.warning("shard #{state.id}: lease renewal task crashed (#{inspect(reason)}); retrying")
    {:noreply, schedule_renew(%{state | renew_task: nil})}
  end

  # The lapse-revalidation task crashed before replying, so ownership was neither confirmed nor
  # refuted. Treated as `:skip` rather than as nothing: `note_not_valid/1` does not fence, it
  # starts a clock that arms only after `margin + steal_margin` of CONTINUOUS failure and that any
  # later success clears. Starting it costs a self-clearing timer; NOT starting it on a node that
  # really is cut from storage is the unbounded-loss-window hole review 2026-08-24 #19 closed.
  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{lapse_revalidate_pending: %Task{ref: ref}} = state
      ) do
    Logger.warning("shard #{state.id}: lapse revalidation task crashed (#{inspect(reason)})")
    apply_lapse_verdict(:skip, %{state | lapse_revalidate_pending: false})
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{flush_task: %Task{ref: ref}} = state) do
    Logger.warning("shard #{state.id}: durability flush task crashed (#{inspect(reason)})")

    state =
      release_flush_slot(record_flush_failure(%{state | flush_task: nil}, {:task_crash, reason}))

    # Dirty (the task never landed) — re-kick so a flush_now waiter retries rather than hangs.
    {:noreply, settle_flushed_and_reschedule(state)}
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

  # `FlushWatermark` restarted with an empty table (expert review 2026-08-26 #17). Re-assert this
  # shard's row so the RPO gauge stops under-reporting; without it a shard only re-published after
  # its next SUCCESSFUL flush, which is exactly what does not happen on the shard the gauge most
  # needs to show. Pure ETS insert, no storage or lease interaction.
  def handle_info(:republish_flush_watermark, state) do
    FlushWatermark.record(state.id, state.flushed_through, state.counter_gen)
    {:noreply, state}
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
    # Truthy covers BOTH in-progress shapes: `true` (timer armed) and `%Task{}` (check in
    # flight). Either way this lapse coalesces onto the episode already running.
    if state.acquire_gen != nil and gen != state.acquire_gen and
         not state.lapse_revalidate_pending do
      Process.send_after(self(), :revalidate_lapse, :rand.uniform(lapse_jitter_ms()))
      {:noreply, %{state | lapse_revalidate_pending: true}}
    else
      {:noreply, state}
    end
  end

  # A lease-touching task is already in flight — skip this episode rather than racing it
  # (expert review 2026-08-26 #13a). Mirrors `handle_info(:renew_lease, %{flush_task: t})`.
  #
  # This is MUTUAL EXCLUSION, not politeness, and the hazard is `Fence.check/2` merging a STALE
  # lease back over a fresh one. In heartbeat mode `revalidate/2` returns `ctx.lease` unchanged, so
  # the lapse task captures L1 and hands L1 back; if a concurrent flush task degrades to
  # `legacy/2` (the heartbeat process died mid-flush) it renews and returns L2, and whichever reply
  # lands second wins. L1 landing last means the next flush PUTs `If-Match: <stale lock_etag>`,
  # gets a 412, and the coordinator reads it as a steal — it self-fences and quarantines the dirty
  # local WITHOUT flushing. That is exactly the failure expert review 2026-08-24 #8 paid for once
  # already, and the reason `:renew_lease` grew the same guard.
  #
  # Skipping loses nothing: an in-flight flush is running its OWN `Fence.check` right now and
  # applies the same three verdicts. `lapse_revalidate_pending` is cleared so a later lapse re-arms.
  #
  # `renew_task` is in the guard for locality, not because it can co-occur: `schedule_renew/1` is
  # armed only at open when `acquire_gen == nil`, and this handler is reachable only from the
  # broadcast clause above, which requires `acquire_gen != nil`. The two are mode-exclusive — but
  # that argument spans three call sites, and the guard is one clause.
  def handle_info(:revalidate_lapse, %{flush_task: t} = state) when t != nil,
    do: {:noreply, %{state | lapse_revalidate_pending: false}}

  def handle_info(:revalidate_lapse, %{renew_task: t} = state) when t != nil,
    do: {:noreply, %{state | lapse_revalidate_pending: false}}

  # A check from an earlier lapse is still in flight — coalesce onto it rather than firing a
  # second GET. (`lapse_revalidate_pending` holds the Task while the check runs; see its
  # definition for why the task does not get a field of its own.)
  def handle_info(:revalidate_lapse, %{lapse_revalidate_pending: %Task{}} = state),
    do: {:noreply, state}

  # RUNS OFF-PROCESS (expert review 2026-08-26 #13a). `Fence.check/2` here is not a local decision:
  # in heartbeat mode a lapse routes to `revalidate/2`, which is a real `Storage.check_lease/2`
  # object-store GET plus Req's retry ladder. Inline, that blocked THIS coordinator's mailbox for a
  # storage RTT — and the broadcast reaches every open coordinator, so at the shipped
  # `:max_open_shards` the node stalls its whole checkout/checkin path at the exact moment it has
  # just proved it could not keep one small object fresh.
  #
  # This is the same stall the module already removed twice and documented both times: review
  # 2026-07-18 #18 moved the flush fence off-process, review 2026-08-01 #29 moved the legacy renew
  # PUT. This was the third and last inline round trip.
  #
  # Same monitored-task shape as those two: the task returns a fully RESOLVED verdict and the reply
  # handler does ZERO storage I/O, only local state moves.
  def handle_info(:revalidate_lapse, state) do
    ctx = fence_ctx(state)
    {:noreply, %{state | lapse_revalidate_pending: Task.async(fn -> Fence.check(ctx) end)}}
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
  # A DURABILITY FLUSH IS ALREADY RENEWING — do not race it (expert review 2026-08-24 #8).
  #
  # In legacy mode the flush task's `Fence.check` takes `Fence.legacy/2`, which renews the lease
  # with the SAME cached `lock_etag` this timer would use. The task fences first and replies only
  # after the whole VACUUM INTO + full-object PUT completes, so its refreshed etag lives inside
  # the task while `state.lease` stays stale for the entire flush — seconds at real S3. A tick
  # landing in that window PUT `If-Match: <stale etag>`, got a 412, and the coordinator read it as
  # a steal: it logged "lease superseded by another node; self-fencing", stopped with
  # `{:shutdown, :lease_lost}`, and quarantined the dirty local WITHOUT flushing it. Every
  # un-flushed acked write lost, and a false split-brain alarm, with no peer involved.
  #
  # `S3.renew_lease/3` now disambiguates that 412 as well (a lock still naming our own
  # `{owner, epoch}` is our own write), so this guard is the belt to that fix's braces: it stops
  # the collision happening rather than recovering from it, and saves the extra GET + PUT.
  # Rescheduling rather than dropping the tick, because the flush result path does not re-arm this
  # timer — a bare `{:noreply, state}` here would end renewals for the coordinator's whole life.
  # Deferring is safe: the flush's own fence renewed the lease at flush start.
  def handle_info(:renew_lease, %{flush_task: flush_task} = state) when flush_task != nil,
    do: {:noreply, schedule_renew(state)}

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

      # A lapse revalidation is in flight (expert review 2026-08-26 #13a). The OTHER half of the
      # mutual exclusion at `handle_info(:revalidate_lapse, %{flush_task: t})`: both run
      # `Fence.check/2` and both merge its `lease` into coordinator state, so letting them overlap
      # lets the later reply put a STALE `lock_etag` back — and a stale etag on the next flush PUT
      # is a 412 the coordinator reads as a steal (review 2026-08-24 #8). A lapse check is one GET,
      # so deferring one flush tick behind it is the same cost as deferring behind another flush.
      match?(%Task{}, state.lapse_revalidate_pending) ->
        {:noreply, schedule_flush(state)}

      # Clean: local == storage, so there's nothing to upload and no clobber risk.
      # Skip entirely (no fence, no PUT) — this is the durability-flush storm fix: at
      # a million mostly-idle/read-only shards, durability PUTs now track *writes*,
      # not open-shard count.
      not unflushed?(state) ->
        {:noreply, schedule_flush(state)}

      # Nothing written to disk yet (brand-new shard).
      #
      # SETTLE THE WAITERS (expert review 2026-08-20 #28). This branch is not merely the
      # brand-new-shard case it was written for: `flush_then_drop/1`'s own comment records that a
      # `WriteCounter` restart broadcasts `:write_counter_reset`, which sets `flushed_through: -1`
      # and marks EVERY open coordinator dirty at once — including ones that never created a file.
      # A `:flush_now` caller parked in `flush_waiters` then hit this branch every interval and was
      # never answered, blocking the full 60 s `@flush_now_timeout` before `Fathom.Shards.flush/1`
      # mapped the exit to `{:error, :flush_timeout}`. That is the flush-before-fork primitive, so
      # `Tenants.fork(flush_source: true)`, the `/api/tenants/:id` flush endpoint and export all
      # failed spuriously — on a shard where there is provably nothing to make durable — while
      # pinning a web or Oban process for a minute.
      #
      # `:ok` rather than an error, and it is sound: every write goes through SQLite to
      # `state.path`, so no file means no writes landed. The dirty flag here is a conservative
      # UNKNOWN, not evidence of anything to upload.
      not File.exists?(state.path) ->
        {:noreply, schedule_flush(settle_waiters(state, :nothing_to_flush))}

      true ->
        # Node-wide concurrent-flush cap (expert review #17): over the cap, reschedule with a short
        # backoff and stay dirty (the safe direction — the flush just waits), so a mass re-home's
        # phase-aligned timers can't fire N snapshots + PUTs in lockstep and starve cold-open pulls.
        # :disabled (no cap configured — the default) and :ok (slot reserved) both proceed to flush.
        case FlushGate.try_acquire() do
          :full ->
            {:noreply, gate_full(state)}

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

            # `:lineage` is in this list because the flush STAMPS it (expert review 2026-08-20 #8),
            # and leaving it out is not a subtle failure: `lineage_to_store/1` is reached through a
            # STRICT `state.lineage`, so an omission raises inside the task and every flush stops
            # writing a position stamp. That is deliberate. A `Map.get` default here would have
            # made the same omission silent — a shard flushing with no stamp forever, which is the
            # `carry_meta` failure mode (a hand-picked key list that quietly loses an entry) one
            # layer up.
            snapshot_state = Map.take(state, [:id, :path, :etag, :lease, :lineage])

            # THE SLOT IS RESERVED BEFORE THE STATE THAT OWNS ITS RELEASE EXISTS (expert review
            # 2026-08-20 #15) — the same class AGENTS.md records for `acquire_lease` → built state.
            # `Task.async/1` calls `spawn_link`, which RAISES `:system_limit` when the process table
            # is full: the density regime fathom explicitly targets, 30k coordinators plus one
            # process per held stream. A raise here leaked the node-global slot AND crashed the
            # coordinator into a terminate/2 whose state still said `flush_slot_held: false`, so
            # nothing released it there either. The cap is single digits, so a handful of those
            # answer `:full` forever and every dirty shard on the node stops flushing.
            #
            # `FlushGate.sweep/0` reclaims a slot from a dead holder within one sweep interval; this
            # closes the window immediately for the one failure that is synchronous and local.
            task =
              try do
                Task.async(fn -> fenced_flush(fence_ctx, snapshot_state) end)
              rescue
                e ->
                  if gate == :ok, do: FlushGate.release()
                  reraise e, __STACKTRACE__
              catch
                kind, reason ->
                  if gate == :ok, do: FlushGate.release()
                  :erlang.raise(kind, reason, __STACKTRACE__)
              end

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
    # PUBLISH THE WRITE FENCE FIRST (expert review 2026-08-24 #6). This clause documents just
    # above that it covers a self-fence with connections STILL CHECKED OUT — "the *likely* case" —
    # yet it published no fence at all. We have lost the lease, so a write ACKed here is not ours
    # to take; worse, `quarantine_fenced!` renames the live file out from under the open fds, so
    # such a write lands in the `.fenced.<ts>` inode and is invisible to the tenant forever. The
    # fence turns that into a retryable 503.
    Fathom.Shard.WriteFence.fence(state.id)

    state = settle_flush_task(state)
    # Reply to any pending flush_now caller before we exit (expert review 2026-07-18 #4): we
    # self-fenced WITHOUT flushing, so the on-disk state is NOT durable in storage. Without this,
    # the caller's GenServer.call gets a bare :DOWN → Shards.flush catches the exit as :ok → the
    # keystone-fork treats it as "source durably flushed" and forks stale bytes.
    state = settle_waiters(state, {:error, :lease_lost})
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    if unflushed?(state), do: quarantine_fenced!(state), else: drop_local(state.path)
    # …and lift it only AFTER the rename, for the same reason it was published.
    Fathom.Shard.WriteFence.forget(state.id)
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
    #
    # Tombstone guard shared with the `conns > 0` clause below (review 2026-08-26 #9): this clause
    # is the COMMON state for a tenant delete, and it used to flush unconditionally.
    flush_and_drop_unless_tombstoned(state)
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
    # Refuse NEW writes for the duration of the shutdown (expert review 2026-08-20 #9). Streams do
    # not learn this coordinator is going away until its `:DOWN` fires, which is after terminate/2
    # returns — so without this they keep committing through a checkpoint + VACUUM INTO + PUT that
    # has already snapshotted past them. Fencing turns "acked, then not in the object" into a
    # retryable 503, which is the difference between losing a write and asking for it again.
    #
    # Safe to publish unconditionally now: expert review #6 made `open_with_lease/8` clear the
    # fence on every successful lease acquire, so a row left behind by a coordinator that never
    # reached its own cleanup is lifted by the next open rather than stranding the tenant.
    Fathom.Shard.WriteFence.fence(state.id)

    state = settle_flush_task(state)
    state = settle_waiters(state, {:error, :coordinator_stopped})
    Fathom.ShardLoad.forget(state.id)
    Fathom.ShardLatency.forget(state.id)
    Fathom.Shards.Lru.forget(state.id)
    # flush_and_drop reads the write counter (unflushed?/1) to decide whether to upload, so forget
    # the counter AFTER it — forgetting first would zero the count and skip a dirty shard's flush.
    flush_and_drop_unless_tombstoned(state)
    # THE FENCE IS NOT LIFTED HERE AT ALL (expert review 2026-08-26 #6).
    #
    # 2026-08-24 #6 moved this `forget` down so the fence covered the checkpoint + VACUUM INTO +
    # full-object PUT rather than only three ETS deletes. Correct as far as it went — but even
    # here it is too early. `flush_and_drop/1` RELEASES THE LEASE, and `terminate/2` has not
    # returned, so per `drop_local_unless_serving/1` the streams have not learned the coordinator
    # is dying. `fenced?/1` is a node-global ETS read consulted per statement, so the instant this
    # ran, still-live streams could ACK writes again — on a shard this node no longer holds a lease
    # for, into a file the next open will quarantine as a fork. Acknowledged, unrecoverable, and
    # outside the RPO contract docs/durability.md advertises.
    #
    # Clearing it here was only ever belt-and-braces, as the comment it replaces said: the fence is
    # cleared unconditionally by `open_with_lease/8` on the next successful acquire, which is also
    # what covers a brutal kill that never reaches this line. So the belt is removed and the braces
    # do the job.
    #
    # Residual cost is one leaked ETS row for a shard this node never re-opens — bounded by the
    # open-shard count, reclaimed on the next open of that shard, and cheap next to acking a write
    # into an unowned file. Do NOT "tidy" this back: the sibling `lease_lost: true` clause is safe
    # doing its own cleanup only because it RENAMES the file first, so a late write lands in an
    # already-quarantined inode. This clause has no such protection.
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

  # Skip the drop-flush for a TOMBSTONED shard, on BOTH terminate paths (expert review 2026-08-26 #9).
  #
  # This guard existed only in the `conns > 0` clause. The `conns == 0` clause — which is the
  # COMMON state for a tenant being deleted, since a shard being erased is usually not mid-request —
  # called `flush_and_drop/1` unconditionally. So `Tenants.purge/1` paid a full checkpoint +
  # `VACUUM INTO` + full-object PUT + lease release for bytes that `Storage.purge_shard/1` erases on
  # the very next line, and (before #9's other half) held the node's shard supervisor for the whole
  # upload while doing it. The asymmetry was an oversight, not a decision.
  #
  # Skipping the whole of `flush_and_drop/1` — not just the upload — is right because
  # `Tenants.purge/1` finishes the job: `purge_shard/1` deletes every object INCLUDING the `.lock`,
  # and `rm_local/1` removes the local file. That is exactly why the `conns > 0` clause was already
  # safe doing this, and the reasoning transfers unchanged.
  #
  # A shard that is NOT tombstoned still flushes on both paths, which is what keeps a rolling
  # deploy's acked writes durable (review 2026-07-19 #2).
  defp flush_and_drop_unless_tombstoned(state) do
    unless Fathom.Tenants.Tombstones.tombstoned?(state.id), do: flush_and_drop(state)
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
      map_size(conns) > 0 ->
        state

      # Standing down: don't arm idle — stop_when_drained will stop us.
      state.draining ->
        %{state | wrote_during_checkout?: false}

      true ->
        # Last connection gone, so the next checkout starts a fresh "has this stream written?"
        # question (#4). Without this reset a shard that ever wrote would never take the slow
        # cadence again, and the optimisation review 2026-08-01 #42 added would be dead.
        schedule_idle(%{state | wrote_during_checkout?: false})
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
    if File.exists?(state.path), do: drop_local_unless_serving(state)

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
              drop_local_unless_serving(state)
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

                # A GENUINE steal: the lock is someone else's, so there is nothing of ours to
                # release and the conditional DELETE would no-op anyway.
                {:error, :superseded} ->
                  Logger.warning(
                    "shard #{state.id}: object superseded before flush; keeping local for recovery"
                  )

                # ANYTHING ELSE IS NOT A STEAL (expert review 2026-08-24 #10). This used to be a
                # bare `_`, conflating the steal above with a TRANSIENT store error — where we
                # very probably still hold the lock, and returning here strands it. A stranded
                # lock names a LIVE node: `owner_live?` reads this node's fresh heartbeat forever,
                # so no peer, failover, migrator or rebalancer handoff can ever take the shard,
                # while it keeps serving normally because its own node silently reclaims at the
                # same incarnation. The migration job that then cannot drain it snoozes with
                # `failed: 0`, an empty errors array and nothing above [info] — the exact silent
                # signature of the 2026-08-04 rig straggler, and the THIRD instance of this
                # lock-leak class (see `keep_local_release_lease/3`'s own note).
                other ->
                  keep_local_release_lease(
                    state,
                    "lock re-check inconclusive after a flush 412",
                    other
                  )
              end

            # quick_check failed (expert review #4): the flush was refused, so the good stored
            # object is untouched. Release the lease so the next open pulls it.
            #
            # THE PREMISE THAT IT WAS ALREADY QUARANTINED HOLDS FOR ONLY ONE OF THE TWO ROUTES HERE
            # (expert review 2026-08-24 #17). `upload_for_drop/1` calls `quarantine_corrupt!/2` when
            # `checkpoint_and_verify/1` ITSELF reports corruption. When the checkpoint merely comes
            # back BUSY — a lingering reader, entirely ordinary — it short-circuits without running
            # `quick_check` and falls through to `snapshot_and_upload/1`, whose `verify_and_snapshot/2`
            # runs `quick_check` on the live file and returns the SAME `{:corrupt_local, reason}`
            # deliberately WITHOUT quarantining, because that function also runs while the shard is
            # being served (renaming the live path out from under open streams is the data-loss
            # path review #2 closed).
            #
            # So on the busy route the corrupt `.db` was left at the live path with its provenance
            # sidecar still matching the stored object, and the lease released. The next open then
            # reads `warm? == true`, `fork_evidence` answers `:match`, and the coordinator SERVES
            # the corrupt database instead of cold-pulling the last-good object — the opposite of
            # what releasing the lease was meant to achieve.
            #
            # Quarantining here is safe for the reason `quarantine_corrupt!/2`'s own hazard note
            # requires: this is the drop path, the coordinator is terminating. The `File.exists?`
            # guard is what makes it idempotent — on the checkpoint route the file has already been
            # renamed away, and a second call would emit a duplicate error and telemetry event.
            #
            # The panel proposed a distinct `{:corrupt_local_unquarantined, _}` tag instead. Not
            # taken: that tag is also matched by `apply_flush_verdict/2` on the SERVING path, where
            # not quarantining is correct and must stay, so re-tagging means changing a value two
            # paths agree on to fix one of them. The guard is local to the path that differs.
            {:error, {:corrupt_local, reason}} ->
              if File.exists?(state.path), do: quarantine_corrupt!(state, reason)
              Storage.release_lease(state.id, state.lease)

            {:error, reason} ->
              keep_local_release_lease(state, "flush failed", reason)
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
          keep_local_release_lease(state, "ownership unconfirmed before flush", :fence_skip)
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

  # The drop path could not flush, but we may still hold the lock. KEEP the local copy — it holds
  # acked-but-unflushed writes — and GIVE UP the lock. The two are separable, and only the copy is
  # load-bearing for recovery.
  #
  # Found by `chaos.sh rollout` on 2026-08-04 (docs/reviews/fleet-rollout-2026-08-04.md), the third
  # instance of the leaked-lock class after #9 and #11: a 300-tenant rollout stranded exactly one
  # tenant, twice, a different shard each time. Both call sites below previously logged and returned,
  # holding the lock. A lock naming a LIVE node is never stealable — `owner_live?` reads this node's
  # fresh heartbeat forever — so the shard became permanently unmigratable and unfailoverable while
  # continuing to serve normally (its own node re-acquires silently at the same incarnation, which is
  # exactly why the damage is so quiet). The migration that then cannot drain it snoozes forever: a
  # snooze burns no attempt, so the Oban job sits in `scheduled` with an EMPTY errors array,
  # `failed: 0`, no quarantine, and nothing logged above `[info]`.
  #
  # Releasing costs nothing we were actually keeping. The conditional `DELETE … If-Match` no-ops if
  # the lock is no longer ours, so the `:skip` (ownership-unconfirmed) case is safe by construction.
  # If a peer then opens the shard it pulls the last durable object, and OUR divergent copy is
  # arbitrated on this node's next open by the provenance sidecar (#1) — quarantined recoverably as
  # `.forked.<ts>`, the same path a node crash with un-flushed writes already takes. Holding the lock
  # instead does not make the un-flushed writes durable; it only makes the tenant unmovable.
  #
  # Telemetry (not just a log) so the un-flushed-writes-still-on-local state is alertable, matching
  # what #11 established for the sibling branch.
  defp keep_local_release_lease(state, what, reason) do
    Logger.warning(
      "shard #{state.id}: #{what} (#{inspect(reason)}); keeping local copy for recovery and " <>
        "releasing the lease so the shard is not stranded"
    )

    :telemetry.execute(
      [:fathom, :shard, :flush, :failed],
      %{count: 1},
      %{shard_id: state.id, reason: reason}
    )

    if is_map(state.lease), do: Storage.release_lease(state.id, state.lease)
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
      drop_local_unless_serving(state)
      Storage.release_lease(state.id, state.lease)
    else
      # …and RELEASE THE LOCK while keeping it (expert review 2026-08-24 #10). This branch is
      # reached having just CONFIRMED via `check_lease/2` that the lock is still ours, so simply
      # returning strands it on a live node — `owner_live?` reads this node's fresh heartbeat
      # forever, so the shard becomes permanently un-handoff-able while serving normally, and the
      # migration job that cannot drain it snoozes silently with `failed: 0`. Keeping the LOCAL
      # COPY is right (it holds acked writes the object may not, and the next open's provenance
      # check arbitrates recoverably); keeping the LOCK is not — holding it does not make those
      # writes durable, it only makes the tenant unmovable. Same distinction
      # `keep_local_release_lease/3` was extracted for.
      other ->
        keep_local_release_lease(
          state,
          "flush 412 with lock ours, but the re-fenced upload failed",
          other
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
    # THE GENERATION MUST BE CAPTURED HERE, BEFORE THE CHECKPOINT (expert review 2026-08-20 #4).
    # checkpoint_and_verify/1 runs `wal_checkpoint(TRUNCATE)` and then closes the connection, which
    # on the last one unlinks `-wal`/`-shm` — so reading the WAL afterwards finds nothing and, on
    # the old code, stamped this object `{epoch, 0, 0}`: the LOWEST position for the epoch, on the
    # most complete copy of the shard that will ever exist. Every clean idle-drop, graceful drain
    # and rebalance handoff went out that way, and any lagging replica then outranked it.
    pre = Fathom.Shard.Replication.Wal.read(state.path <> "-wal")

    # THE FAST PATH IS ONLY SAFE WITH NOTHING CHECKED OUT (expert review 2026-08-26 #3).
    #
    # The fast path hands `state.path` — the LIVE database — to `Storage.flush/5`, which reads it
    # once for the Content-MD5 and a SECOND time to stream the body. On the `conns > 0` terminate
    # clause (rolling deploy / SIGTERM / `Shards.stop`) live streams still hold connections: the
    # stream `:DOWN` cannot fire until `terminate/2` RETURNS, as `drop_local_unless_serving/1`
    # records. So a write can land between those two reads.
    #
    # The guard between them (`S3.stat_and_md5/1`) compares `size` and `mtime` from `File.stat/1`,
    # and mtime is SECOND-resolution — so a same-size, same-second in-place page rewrite is
    # invisible. That is exactly what an inline autocheckpoint does when a still-live stream's
    # commit crosses `wal_autocheckpoint=4000`.
    #
    # S3's BadDigest catches the mismatch, but a BadDigest is a FAILED FLUSH, not a saved one: it
    # routes to `keep_local_release_lease/3`, which keeps the local copy and releases the lease. On
    # the stated ephemeral-container-disk deployment "keep the local copy" is not recovery, and this
    # is the ONE flush `trap_exit` + `:shard_shutdown_ms` exist to guarantee. On `Storage.Local`
    # it is worse: there is no Content-MD5 equivalent at all (it copies, then hashes the
    # DESTINATION), so the same race yields a torn object stamped as good.
    #
    # `snapshot_and_upload/1`'s `VACUUM INTO` temp is quiescent by construction, so it has no such
    # window. This costs little in practice: with live streams holding read marks,
    # `wal_checkpoint(TRUNCATE)` usually returns `busy` and the code already fell through to the
    # snapshot path — this makes that the DECIDED behaviour rather than a lucky one.
    if map_size(state.conns) > 0 do
      emit_drop_route(state, :snapshot)
      snapshot_and_upload(state)
    else
      emit_drop_route(state, :checkpoint)
      checkpoint_then_upload(state, pre)
    end
  end

  # Which route the drop-flush took. Exists so the #3 routing decision is TESTABLE: both routes are
  # otherwise silent on success, and the fallback's "checkpoint incomplete" warning does not
  # distinguish them — measured, a TRUNCATE with a connection still checked out SUCCEEDS, so
  # pre-fix this path really did upload the live file rather than falling through to the snapshot
  # as the audit assumed. Without a route signal that difference is unobservable from a test.
  defp emit_drop_route(state, route) do
    :telemetry.execute(
      [:fathom, :shard, :drop_flush, :route],
      %{count: 1},
      %{shard_id: state.id, route: route, conns: map_size(state.conns)}
    )
  end

  defp checkpoint_then_upload(state, pre) do
    # checkpoint_and_verify/1 runs BOTH on one connection (review 2026-07-24 #31) and reports which
    # stage failed, because the two failures need opposite handling: a corrupt local db must refuse
    # the flush, while a merely-busy checkpoint must fall back to the snapshot path.
    case checkpoint_and_verify(state.path) do
      :ok ->
        # The WAL has just been folded into the database, so the object holds everything through
        # the end of `pre`'s generation — see flush_position/2 for why that is claimed as gen + 1.
        Storage.flush(
          state.id,
          state.path,
          state.etag,
          flush_position(state, pre),
          lineage_to_store(state.lineage)
        )

      # Integrity failed (expert review 2026-07-14 #4). The checkpoint-then-raw-upload fast path
      # uploads the bytes as-is, so a locally corrupted db (disk/fs/memory fault, exqlite/OS bug)
      # would be flushed with a valid If-Match by the legitimate owner — the fence can't help, it's
      # not a steal — permanently clobbering the last good stored object. Refuse the flush (the good
      # object stays authoritative), quarantine the corrupt copy for forensics, and alarm.
      # This used to key off the tuple tag (`kind in [:quick_check, :quick_check_failed]`), which is
      # wrong in BOTH directions — see classify_integrity_failure/1 (expert review 2026-08-20 #2).
      # A transient `:busy` quarantined a healthy local copy along with its unflushed acked writes;
      # and had the tag list been narrowed to `:quick_check` alone (the reviewer's own suggestion),
      # a genuinely malformed file would have stopped being quarantined at all, because SQLite
      # reports bad bytes through the ERROR channel, not as rows.
      {:error, reason} ->
        if classify_integrity_failure(reason) == :corrupt do
          # Integrity failed (expert review 2026-07-14 #4). The checkpoint-then-raw-upload fast path
          # uploads the bytes as-is, so a locally corrupted db (disk/fs/memory fault, exqlite/OS bug)
          # would be flushed with a valid If-Match by the legitimate owner — the fence can't help,
          # it's not a steal — permanently clobbering the last good stored object. Refuse the flush
          # (the good object stays authoritative), quarantine the corrupt copy, and alarm. Safe here
          # and only here: the coordinator is terminating and releases the lease.
          quarantine_corrupt!(state, reason)
          {:error, {:corrupt_local, reason}}
        else
          # Checkpoint incomplete (typically busy), or integrity indeterminate. Fall back to
          # snapshot_and_upload, whose VACUUM INTO captures WAL content regardless — and which reads
          # every page, so it fails on corruption on its own.
          Logger.warning(
            "shard #{state.id}: pre-drop checkpoint incomplete (#{inspect(reason)}); snapshot-flushing instead"
          )

          snapshot_and_upload(state)
        end
    end
  end

  # SQLite reports "these bytes are bad" through TWO different channels, and the difference is not
  # the tuple tag (expert review 2026-08-20 #2, corrected against a really-corrupted file rather
  # than against the finding's prescription). A mildly damaged db returns rows describing the
  # damage — `{:quick_check, rows}`. A badly damaged one makes the query itself ERROR, arriving as
  # `{:quick_check_failed, "database disk image is malformed"}` — the same shape a plain `:busy`
  # produces. Measured: overwriting one b-tree page with garbage yields the ERROR form, not rows.
  #
  # So classify on the REASON, not the tag. Defaulting to :unknown is the safe direction: both
  # verdicts refuse the flush, so the stored object is protected either way, and the only cost of
  # guessing :unknown is a retry — whereas guessing :corrupt discards a healthy local copy on the
  # drop path, including its unflushed acked writes.
  @corrupt_messages [
    "database disk image is malformed",
    "file is not a database",
    "malformed database schema",
    "database corruption"
  ]

  defp classify_integrity_failure({:quick_check, _}), do: :corrupt
  defp classify_integrity_failure({:quick_check_failed, reason}), do: corrupt_reason(reason)
  defp classify_integrity_failure({:quick_check_open_failed, reason}), do: corrupt_reason(reason)
  defp classify_integrity_failure(_), do: :unknown

  defp corrupt_reason(reason) when is_binary(reason) do
    down = String.downcase(reason)
    if Enum.any?(@corrupt_messages, &String.contains?(down, &1)), do: :corrupt, else: :unknown
  end

  defp corrupt_reason(reason) when is_atom(reason) do
    if reason in [:corrupt, :not_a_db], do: :corrupt, else: :unknown
  end

  defp corrupt_reason({_, reason}), do: corrupt_reason(reason)
  defp corrupt_reason(_), do: :unknown

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
  #
  # ONLY SAFE WHERE THE SHARD IS PROVABLY NOT BEING SERVED — i.e. the drop path, where the
  # coordinator is terminating and releases the lease (expert review 2026-08-20 #2). Calling this
  # while streams are checked out renames the live path out from under them AND unlinks the -shm,
  # so the next connection to open `state.path` creates a BRAND-NEW EMPTY DATABASE: two sets of
  # connections to one tenant, one seeing data and one seeing nothing. Worse, the shard is still
  # dirty with an unchanged `state.etag`, so the next periodic flush snapshots that empty file and
  # PUTs it over the good stored object with a valid If-Match — destroying the only good copy.
  # The serving path therefore refuses the flush and leaves the file alone; see verify_and_snapshot/2.
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
  # FAILS DIRTY (expert review 2026-08-01 #39). Both `count/1` and `bump/1` rescue `ArgumentError`
  # when the table is absent, so in the window where `WriteCounter.init/1` has bumped the
  # persistent_term generation but not yet created the table, a WARM shard seeded
  # `flushed_through = 0` from a rescued count and its dirtying bump silently no-opped. Once the
  # fresh table existed, `unflushed?/1` then read CLEAN and idle/shutdown took `drop_clean/1` —
  # deleting the local `.db`/`-wal`/`-shm` with un-flushed acked writes still in them.
  #
  # `bump/1`'s rescue is fine for a per-write bump ("the next write re-dirties"); it is wrong for
  # the dirtiness SEED, where there is no next write. `-1` is the existing "unknown ⇒ dirty"
  # sentinel — the same value `:write_counter_reset` sets (see `handle_info/2`) — so it needs no
  # new convention: `unflushed?/1` compares `count > flushed_through` and any count beats -1.
  defp init_flushed_through(shard_id, warm?) do
    n = WriteCounter.count(shard_id)

    if warm? do
      try do
        WriteCounter.bump_strict(shard_id)
        n
      rescue
        ArgumentError ->
          Logger.warning(
            "shard #{shard_id}: WriteCounter table absent while seeding a warm open — " <>
              "failing DIRTY so un-flushed local writes are not dropped as clean"
          )

          -1
      end
    else
      n
    end
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

  # Unlink the local copy — UNLESS streams are still holding it (expert review 2026-08-20 #9).
  #
  # `terminate/2`'s `conns > 0` clause runs on the rolling-deploy / SIGTERM / force-stop path, and
  # the stream-side teardown is ASYNCHRONOUS: `Filo.Stream` learns only via its monitor on this
  # coordinator, and that `:DOWN` cannot fire until terminate/2 RETURNS. So for the whole duration
  # of checkpoint + VACUUM INTO + a full-object PUT — budgeted up to :shard_shutdown_ms, and
  # seconds at real S3 latency — live streams keep committing writes that are neither in the
  # snapshot we just uploaded nor, after an unlink, anywhere at all. Those writes were ACKED.
  #
  # The sibling `lease_lost` clause already reasons about exactly this and RENAMES for it
  # ("safe while streams hold the file open — Unix keeps the fd on the renamed inode"). This path
  # was added later and unlinked instead.
  #
  # KEEPING the file beats renaming it here. We stamped the sidecar with the etag we just uploaded,
  # so a later warm open on this node reads sidecar == object, adopts the local copy, and RECOVERS
  # the terminate-window writes. A rename would preserve them for forensics only. If another node
  # takes over in between, the object's etag moves, the sidecar no longer matches, and the existing
  # fork detection quarantines it — the designed behaviour, unchanged.
  defp drop_local_unless_serving(%{conns: conns} = state) when map_size(conns) > 0 do
    Logger.warning(
      "shard #{state.id}: stopped with #{map_size(conns)} connection(s) still checked out; " <>
        "KEEPING the local copy so writes acked during shutdown are not unlinked out from " <>
        "under the streams that made them (the next warm open adopts it)"
    )

    :telemetry.execute([:fathom, :shard, :drop_deferred], %{count: 1}, %{shard_id: state.id})
    :ok
  end

  defp drop_local_unless_serving(state), do: drop_local(state.path)

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

  # --- A2 lineage -----------------------------------------------------------------------

  # The lineage counter this coordinator stamps for its whole life (expert review 2026-08-20 #8).
  #
  # WHY IT EXISTS. The position stamp's `epoch` slot used to carry the LOCK epoch, and the lock
  # epoch is not monotonic: `release_lease` DELETES the lock object, so the next `acquire_lease`
  # takes the optimistic create path and starts again at 1. The number therefore climbed on
  # crash-steals and RESET on every clean idle-drop, drain and rebalance handoff — while
  # `Promote.fresher?/2` and the replication ordering read it as the high-order component of a
  # total order. A shard that had been dropped cleanly could be outranked by a replica holding a
  # number from an earlier ownership.
  #
  # READ ONCE, NEVER RECOMPUTED. Within one ownership the order is (lineage, wal_gen, offset). A
  # lineage that moved between two of this coordinator's own flushes would make its own stamps
  # incomparable, which is worse than the bug being fixed.
  #
  # THREE ANSWERS, and each means something different downstream (see stamp_epoch/1):
  #
  #   `:disabled` — replication is off. The whole computation is skipped, INCLUDING the HEAD, so
  #     an ordinary node allocates nothing and pays no extra round trip. Nothing consumes the
  #     epoch slot as an ordering key with A2 off, so the lock epoch stays in it exactly as
  #     before — which is what keeps the gated `cold_open_p50_us` and `fanout_kb_per_shard`
  #     paths bit-for-bit unchanged.
  #   integer — the lineage to stamp.
  #   `:unknown` — replication is ON but the store could not be read. A warm open does not touch
  #     the store for its bytes, so this is reachable while still serving correctly. There is no
  #     lineage to claim, so nothing is stamped and the object becomes un-overridable — the same
  #     safe direction flush_position/2 already takes for an unreadable WAL.
  defp open_lineage(shard_id) do
    # Gate first and return an atom, mirroring promote_on_open?/0 at the call site above: a node
    # that has not enabled replication must not pay for a feature it does not run.
    # `Fleet.replicating?/0`, the owned predicate, rather than a re-inlined config read: the
    # lineage rides the SHIPPING gate, not the listen gate. A node that ships is a node whose
    # stamps get compared against replicas; a node that only receives has no stamps of its own.
    if Fleet.replicating?() do
      case Storage.object_head(shard_id) do
        {:ok, head} -> Storage.next_lineage(head)
        {:error, _} -> :unknown
      end
    else
      :disabled
    end
  end

  # --- A2 promote-on-open ---------------------------------------------------------------
  #
  # A survivor may hold a REPLICA of this shard that is newer than the stored object — that is the
  # entire point of A2, and until this ran the replica sat on disk while the open served the last
  # flush. Runs here, after `revalidate_takeover/5`, because this decides WHICH BYTES ARE SERVED
  # and must not happen before the lease and the fork verdict are settled.
  #
  # Every branch that is not a proven win returns the caller's etag unchanged, so the ordinary open
  # path is bit-for-bit what it was. That includes every error: a failed promotion attempt must not
  # fail an open, because the ordinary path is still correct — it just recovers less.
  #
  # `Promote.fresher?/2` is what makes this safe. The replica is promoted only when it is STRICTLY
  # ahead of what the object claims, the object's claim is an over-claim by construction
  # (`flush_position/1`), and an unstamped object is never overridable at all — so an object
  # written before stamping existed, or by a node that has not been upgraded, is left alone.
  defp maybe_promote_replica(shard_id, path, lease, etag, lineage) do
    # The gate is checked FIRST and returns the caller's own binding, so a node that has not
    # enabled this allocates nothing at all on its open path. See the note at the call site.
    #
    # `follower_running?/0` is hoisted to sit beside the gate rather than living inside
    # `nothing_to_promote?/1`, and that is a DEFAULT-ON decision: the gate is now on everywhere, so
    # this `if` is the open path for every node that is not part of a replicating fleet, and it
    # should cost one `Process.whereis` rather than a walk through two more predicates. No follower
    # means no replica table to read AND nowhere for a pulled replica to install, so both branches
    # below would have declined anyway — `Recovery.search/5` says so in as many words.
    if promote_on_open?() and follower_running?() do
      try_promote(shard_id, path, lease, etag, lineage)
    else
      etag
    end
  end

  defp follower_running?, do: Process.whereis(Follower) != nil

  # TWO PATHS, and the split is about what a cold open is allowed to pay for.
  #
  # The local-only path checks ETS first and reaches the object store ONLY when this node actually
  # holds a replica — so a node with promote-on-open enabled pays nothing extra on the vast
  # majority of opens, which is why it was written that way and why it is kept bit-for-bit.
  #
  # The fleet path cannot be that lazy: deciding whether a PEER is worth asking requires the
  # object's position first, so it costs one stamp read on every promote-eligible open plus a
  # concurrent round trip to each peer. That is the price of closing the RPO gap on a failover the
  # LB routed to a node holding no replica, and it is why `Recovery` is a separate gate rather than
  # part of `:replication_promote_on_open`.
  # The middle branch is a COST check that became load-bearing when the gate started defaulting ON
  # (2026-08-25): `try_promote_from_fleet/5` opens with `Storage.object_head/1`, so without it every
  # cold open on every node — including nodes that follow nothing and have no peers — would pay an
  # object-store round trip to reach a decision that was already determined.
  #
  # BOTH halves of `nothing_to_promote?/1` are required, and dropping either is a real regression
  # rather than a tidier condition. An earlier draft short-circuited on `fleet_reachable?/1` alone
  # and fell through to the LOCAL path, which `promote_on_open_test.exs:435` immediately caught: a
  # node with a local replica and no peers still runs the fleet DECISION (`best_replica/3`
  # short-circuits on its own replica before opening a socket), and only that path performs the
  # mid-flight `recheck_object/4`. Routing it to the local path silently dropped the re-read and
  # promoted against a stamp that had moved.
  defp try_promote(shard_id, path, lease, etag, lineage) do
    cond do
      not Recovery.enabled?() -> try_promote_local(shard_id, path, lease, etag, lineage)
      nothing_to_promote?(shard_id) -> etag
      true -> try_promote_from_fleet(shard_id, path, lease, etag, lineage)
    end
  end

  # Provably nothing for the fleet path to find, established without touching the object store: no
  # replica of this shard in the local follower's table, AND nobody to ask. Those are exactly the
  # two conditions under which `Recovery.best_replica/3` returns `:none` — its local short-circuit
  # needs a local replica, and `search/5` bails when the peer set is empty or no follower runs — so
  # skipping straight to `etag` is what the long way round would have produced anyway.
  defp nothing_to_promote?(shard_id) do
    replica_state(shard_id) == nil and not Recovery.fleet_reachable?()
  end

  defp try_promote_local(shard_id, path, lease, etag, lineage) do
    with replica when replica != nil <- replica_state(shard_id),
         {:ok, stamp} <- Storage.object_position(shard_id),
         true <- Promote.fresher?(replica, stamp) do
      promote_replica(shard_id, path, lease, etag, replica, stamp, lineage)
    else
      _ -> etag
    end
  end

  # `Recovery.best_replica/3` re-checks this node's own replica and short-circuits the network when
  # it already wins, so there is one call here rather than a local branch and a fleet branch that
  # could drift apart on what "fresher" means.
  #
  # `fresher?` is asserted again afterwards even though `Recovery` only ever returns a replica that
  # passed it. It is one comparison on a path that ends in overwriting a tenant's stored database,
  # and the alternative is trusting a promise made by a different module about bytes that arrived
  # over an unauthenticated socket.
  #
  # `object_head/1` rather than `object_position/1` because the decision needs the object's stamp
  # AND the etag it will be fenced with to describe the SAME version — see the callback. The head
  # is then RE-READ after the transfer and compared (`Recovery.recheck/3`): everything between the
  # two reads is network, bounded in seconds by the peer query and in database-size by the pull,
  # and a flush landing in there left the promote decision resting on a version that no longer
  # exists. That was never unsafe — the fenced publish 412s — but the cost was a whole transfer, a
  # snapshot, and a log line claiming the object was behind when by then it was not.
  defp try_promote_from_fleet(shard_id, path, lease, etag, lineage) do
    started = System.monotonic_time(:millisecond)

    with {:ok, head} <- Storage.object_head(shard_id),
         {:ok, replica} <- Recovery.best_replica(shard_id, position_of(head)),
         true <- Promote.fresher?(replica, position_of(head)),
         :ok <- recheck_object(shard_id, head, replica, started) do
      promote_replica(shard_id, path, lease, etag, replica, position_of(head), lineage)
    else
      _ -> etag
    end
  end

  defp position_of(nil), do: nil
  defp position_of(%{position: position}), do: position

  # The re-read. A failure to READ is treated as "moved" — declining is the conservative answer and
  # this whole path is an optimization over opening from the stored object, so an unreadable store
  # is a reason to take the ordinary path rather than to promote on a comparison we can no longer
  # confirm.
  defp recheck_object(shard_id, before, replica, started) do
    now =
      case Storage.object_head(shard_id) do
        {:ok, head} -> head
        {:error, reason} -> {:unreadable, reason}
      end

    case now do
      {:unreadable, reason} ->
        decline_promotion(shard_id, {:object_head_unreadable, reason}, started)

      head ->
        case Recovery.recheck(before, head, replica) do
          :ok -> :ok
          {:error, reason} -> decline_promotion(shard_id, reason, started)
        end
    end
  end

  # Counted, not just logged. A promotion abandoned here means a database was transferred across
  # the network for nothing, and that is invisible in the success telemetry by construction —
  # `recovered_from_peer` fires on the pull, which DID happen.
  defp decline_promotion(shard_id, reason, started) do
    elapsed = System.monotonic_time(:millisecond) - started

    Logger.warning(
      "shard #{shard_id}: abandoning replica promotion after #{elapsed}ms — the stored object " <>
        "changed while we were recovering (#{inspect(reason)}); opening from the stored object"
    )

    :telemetry.execute(
      [:fathom, :replication, :promotion_raced],
      %{count: 1, duration_ms: elapsed},
      %{shard_id: shard_id, reason: elem_reason(reason)}
    )

    {:error, reason}
  end

  # Every reason that reaches here today IS a tagged tuple (`Recovery.recheck/3` returns
  # `{:object_moved, _, _}` / `{:object_advanced, _}`, and the other caller passes
  # `{:object_head_unreadable, _}`), which dialyzer proves — a second `defp` clause for the bare
  # case was reported as unreachable. It is written as ONE total clause rather than deleted,
  # because this runs inside `decline_promotion/3` on the shard-open path: a future reason that is
  # a plain atom would otherwise raise FunctionClauseError from a telemetry label and fail an open
  # that the ordinary stored-object path would have served fine.
  defp elem_reason(reason) do
    if is_tuple(reason) and tuple_size(reason) > 0, do: elem(reason, 0), else: reason
  end

  # Default ON since 2026-08-25 (`REPLICATION_PROMOTE_ON_OPEN=false` turns it off). Free on a node
  # that holds no replicas: `try_promote_local/5` checks ETS before it touches the object store, so
  # a node outside a replicating fleet pays one ETS miss per cold open and nothing else.
  defp promote_on_open?, do: Application.get_env(:fathom, :replication_promote_on_open, true)

  # THE `Process.whereis` IS NOT REDUNDANT WITH THE RESCUE, it is the whole cost of this function.
  # `Follower.state_of/2` is an `:ets.lookup` on a table the follower owns, so on a node running no
  # follower it RAISED and this clause rescued — raising is control flow here, and building an
  # `ArgumentError` plus its stacktrace is orders of magnitude more expensive, and more garbage,
  # than the lookup it replaces. That was survivable while `:replication_promote_on_open` was
  # opt-in. Defaulting it on (2026-08-25) put it on every cold open of every node, and the bench
  # gate caught it immediately: `fanout_kb_per_shard` +52.7% while its GC'd twin moved +1.1% and
  # `served_kb_per_shard` -0.1% — i.e. per-open garbage, exactly the shape an exception makes.
  #
  # No follower process ⇒ no replica table ⇒ no replica, so the early return is also the truthful
  # answer rather than a shortcut. The rescue stays as a backstop for the race where the follower
  # dies between the two calls.
  defp replica_state(shard_id) do
    if Process.whereis(Follower), do: Follower.state_of(Follower, shard_id)
  rescue
    ArgumentError -> nil
  end

  # Ordering here is the whole risk, so it is worth stating:
  #
  #   1. SNAPSHOT the stored object first. This is the least reversible thing A2 does — it declares
  #      one of two lineages the winner and overwrites the other, on the strength of a comparison
  #      that is new code. A server-side copy costs no body transfer and is the difference between
  #      a bad hour and permanent loss if the comparison is ever wrong.
  #   2. STAGE into a temp and verify it there. Nothing touches the live path until the replica has
  #      been checkpointed into a standalone database and passed `quick_check`.
  #   3. PUBLISH from the temp, FENCED with the etag we hold. A 412 means someone wrote the object
  #      since our pull, so the replica is no longer provably newer — abandon and serve the
  #      ordinary path.
  #   4. Only then move the temp onto the live path and stamp the new etag as its provenance.
  #
  # A failure at 1–3 leaves the shard exactly as the ordinary open left it. A failure at 4 is the
  # one that cannot be shrugged off — the object is now the replica while the local file is not —
  # so it fails the open, which releases the lease and lets a clean open pull the bytes we just
  # published.
  defp promote_replica(shard_id, path, lease, etag, replica, stamp, lineage) do
    temp = "#{path}.promote.#{System.unique_integer([:positive])}"
    follower = Follower

    try do
      with :ok <- snapshot_before_promotion(shard_id),
           :ok <- Promote.stage(follower, shard_id, temp),
           {:ok, new_etag} <-
             Storage.flush(
               shard_id,
               temp,
               etag,
               flush_position(%{lease: lease, path: temp, lineage: lineage}),
               lineage_to_store(lineage)
             ) do
        case File.rename(temp, path) do
          :ok ->
            Enum.each(["-wal", "-shm"], &File.rm(path <> &1))
            write_etag_sidecar(path, new_etag)
            Follower.forget(follower, shard_id)

            Logger.warning(
              "shard #{shard_id}: PROMOTED a local replica over the stored object " <>
                "(replica #{inspect(replica)} > object #{inspect(stamp)}); " <>
                "pre-promotion state snapshotted"
            )

            :telemetry.execute(
              [:fathom, :shard, :replica_promoted],
              %{count: 1},
              %{shard_id: shard_id, epoch: lease.epoch}
            )

            new_etag

          {:error, reason} ->
            # The ONE failure here that cannot be shrugged off: the object is now the replica while
            # the live path is not, so serving on would serve a lineage the store disagrees with.
            # Raising routes to `abandon_open/5`, which releases the lease and stops the
            # coordinator — a clean re-open then pulls exactly what was just published.
            raise "shard #{shard_id}: promoted object published but the local rename failed " <>
                    "(#{inspect(reason)}); refusing to serve a diverged local copy"
        end
      else
        {:error, reason} ->
          Logger.warning(
            "shard #{shard_id}: replica promotion declined (#{inspect(reason)}); " <>
              "opening from the stored object"
          )

          etag
      end
    after
      Enum.each(["", "-wal", "-shm"], &File.rm(temp <> &1))
    end
  end

  # Best-effort, and deliberately not fatal: a shard with no stored object yet has nothing to
  # snapshot, and a snapshot backend hiccup should not block a recovery that is otherwise sound.
  # It is attempted first precisely because it is the cheap insurance on the irreversible step.
  defp snapshot_before_promotion(shard_id) do
    case Fathom.Snapshots.create(shard_id, label: "pre-promotion") do
      {:ok, _snapshot_id} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "shard #{shard_id}: could not snapshot before promoting a replica " <>
            "(#{inspect(reason)}); proceeding"
        )

        :ok
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
  # WHEN that started; once it has held for `stealable_after_ms/1` — the point past which a peer may
  # legitimately have stolen the shard — publish the write-fence so ShardExecutor refuses NEW writes
  # (reads keep serving from the local copy). This collapses the loss window from the whole partition
  # duration to ~ttl + steal_margin measured from the last renewal: without it the coordinator keeps ACKing writes for the entire
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
          "shard #{state.id}: heartbeat not valid for > margin+steal_margin — fencing writes (node " <>
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
    was_fenced? = Fathom.Shard.WriteFence.fenced?(state.id)
    Fathom.Shard.WriteFence.unfence(state.id)

    # The closing half of the `:write_fenced` breaker (expert review 2026-08-26 #13a). Only the
    # OPENING was observable, so an operator watching a partition heal could see writes start being
    # refused and never see them start being accepted again — the recovery had to be inferred from
    # the absence of 503s. Emitted only on a real transition, so a healthy shard's every-flush
    # `clear_write_fence` is silent.
    if was_fenced? do
      :telemetry.execute([:fathom, :shard, :write_unfenced], %{count: 1}, %{shard_id: state.id})
    end

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
  # Settle a completed flush's waiters and re-arm the periodic timer — UNLESS the settle already
  # armed one (expert review 2026-08-24 #21).
  #
  # `settle_waiters(state, :flushed)`'s still-dirty branch calls `schedule_flush_backoff/1`, which
  # arms `:durability_flush` at `flush_backoff_ms()` (250 ms, jittered). All three call sites used
  # to wrap it as `schedule_flush(settle_waiters(state, :flushed))` — and `schedule_flush/1` on a
  # dirty shard routes to `do_schedule_flush/2`, whose first act is `cancel_flush/1`. So the
  # backoff was cancelled and replaced by a full `flush_interval_ms()` (5 s) on every re-kick, and
  # review 2026-08-01 #32's "BACKED OFF and BOUNDED" re-kick was backed off 20× further than
  # intended. `gate_full/1` is the one path that was NOT wrapped, which is what showed the wrapping
  # was accidental rather than a decision.
  #
  # The `@max_flush_rekicks` bound survives either way, so there was never a runaway — but a
  # `:flush_now` caller needing re-kicks waited ~8 × (5 s + flush duration) instead of
  # ~8 × 250 ms + flush durations, which exceeds the 60 s `@flush_now_timeout`. So
  # `Fathom.Shards.flush/1` returned the opaque `{:error, :flush_timeout}` rather than the
  # actionable `{:error, :flush_not_converging}` #32 introduced, and `Tenants.fork(flush_source:
  # true)`, the `/api/tenants/:id` flush endpoint and export failed spuriously while pinning a
  # web/Oban process for a minute.
  #
  # `flush_rekicks` is the signal: it is incremented exactly when the backoff is armed, and reset
  # to 0 on both settling branches, so a strict increase means "the re-kick branch ran".
  defp settle_flushed_and_reschedule(state) do
    before = state.flush_rekicks
    settled = settle_waiters(state, :flushed)

    if settled.flush_rekicks > before, do: settled, else: schedule_flush(settled)
  end

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

  # There is no local file, so there is nothing this node could upload and nothing the caller's
  # completed writes are waiting on. Distinct from `:flushed` because that clause re-kicks while
  # `unflushed?/1` is true — and a `WriteCounter` reset leaves a fileless coordinator marked dirty
  # forever, so `:flushed` here would rekick to `:flush_not_converging` and report a failure for a
  # shard that has nothing to fail at. See the `not File.exists?` branch of `:durability_flush`.
  defp settle_waiters(state, :nothing_to_flush) do
    for w <- state.flush_waiters, do: GenServer.reply(w, :ok)
    %{state | flush_waiters: [], flush_rekicks: 0}
  end

  defp settle_waiters(state, {:error, _reason} = err) do
    for w <- state.flush_waiters, do: GenServer.reply(w, err)
    %{state | flush_waiters: []}
  end

  # The node-wide flush cap is full (expert review 2026-08-20 #28). With no waiter this is the
  # original behaviour: back off and try again, staying dirty, which is the safe direction.
  #
  # With a waiter it is not, because the wait was unbounded. Backing off forever answered nobody,
  # and combined with a leaked gate slot (#15) it never converged at all — the caller burned the
  # whole 60 s timeout while the coordinator was, correctly, doing nothing. So a waiter's patience
  # is spent against the SAME `@max_flush_rekicks` budget a still-dirty flush uses, and then it is
  # told plainly. `{:error, :flush_gate_full}` rather than `:flush_timeout` because the two ask for
  # different responses: this one says the node is saturated and the call is worth retrying, where
  # a timeout says nothing at all.
  #
  # BOTH BRANCHES NOW USE THE POPULATION-DERIVED GATE BACKOFF (expert review 2026-08-26 #16), not
  # the fixed 250 ms. See `schedule_gate_backoff/1`.
  #
  # The finding also asked to increment `flush_rekicks` on the no-waiter branch "so it is bounded".
  # NOT DONE, and deliberately: the unboundedness that mattered is the RATE, which the derived
  # backoff caps at one flush interval. Giving up entirely on a no-waiter shard would be wrong —
  # it is still dirty and must eventually flush, and there is nobody to tell. Incrementing there
  # would also silently spend the WAITER budget: a shard that had been bouncing off the gate would
  # answer the next `flush_now` caller `{:error, :flush_gate_full}` immediately, with none of the
  # patience that branch exists to give. Implement the finding, not the prescription.
  defp gate_full(%{flush_waiters: []} = state), do: schedule_gate_backoff(state)

  defp gate_full(state) do
    if state.flush_rekicks < @max_flush_rekicks do
      schedule_gate_backoff(%{state | flush_rekicks: state.flush_rekicks + 1})
    else
      for w <- state.flush_waiters, do: GenServer.reply(w, {:error, :flush_gate_full})
      schedule_gate_backoff(%{state | flush_waiters: [], flush_rekicks: 0})
    end
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
  # How much of this shard's history the bytes being flushed contain — Phase 2 A2. Lets a failover
  # order this object against a node's local REPLICA, which nothing else can do (an etag is a
  # content hash with no ordering; the lock carries the holder's epoch, not the object's).
  #
  # READ AFTER THE SNAPSHOT, DELIBERATELY, AND REVERSING THIS LOSES WRITES. The snapshot is a
  # consistent read taken at some instant; writes can land while it runs. Reading the WAL
  # afterwards therefore reports a position at or beyond what the snapshot captured — the object
  # claims at least as much as it holds. That direction is safe: a replica is promoted only when it
  # is strictly ahead of the claim, so an over-claim costs at most one flush interval of RPO.
  #
  # Reading BEFORE the snapshot would under-claim, and a replica sitting between the claim and the
  # truth would be judged fresher and promoted — silently dropping exactly the writes that landed
  # during the snapshot. `FlushWatermark`'s own count is captured before snapshotting for the
  # opposite reason (a missed write must leave the shard dirty), which is why it cannot be reused
  # here however similar it looks.
  #
  # `nil` when the WAL is unreadable or there is no lease: an absent stamp reads as "unknown" and
  # makes the object un-overridable, which is the same safe answer.
  #
  # `pre` IS THE WAL HEADER READ BEFORE THE FLUSH MUTATED ANYTHING, and it exists because the
  # read-after rule above breaks in one case (expert review 2026-08-20 #4). The flush can DESTROY
  # the WAL it is about to be measured from: `upload_for_drop/1` runs `wal_checkpoint(TRUNCATE)`
  # and `snapshot/2` is frequently the LAST connection, whose close unlinks `-wal`/`-shm`.
  # `Wal.read/1` answers `{:ok, :empty}` for an absent or sub-header file exactly as it does for a
  # never-written one, so the read-after landed on the `:empty` branch and stamped `{epoch, 0, 0}`
  # — THE MINIMUM FOR THE EPOCH — onto the most complete copy of the shard that will ever exist.
  # Every lagging replica then outranks it in `Promote.fresher?/2` and is promoted over it.
  #
  # So: over-claim from `pre` instead. A checkpoint FOLDS the WAL into the database, so an object
  # written after one holds everything through the end of generation `gen`; stamping `gen + 1`
  # with offset 0 is strictly greater than any replica at `{epoch, gen, ≤N}` and keeps the
  # over-claim direction the comment above requires. This is not "read before the snapshot" — the
  # live read still happens after, and still wins whenever the WAL survives.
  defp flush_position(state), do: flush_position(state, {:ok, :empty})

  defp flush_position(state, pre) do
    case stamp_epoch(state) do
      nil -> nil
      epoch -> flush_position(state, pre, epoch)
    end
  end

  # What fills the position stamp's `epoch` slot (expert review 2026-08-20 #8). Three inputs, and
  # the lease is required by ALL of them: no lease means no ownership to order, which is the
  # pre-existing rule this keeps.
  #
  #   integer lineage — stamp it. Unlike the lock epoch it never resets. See open_lineage/1.
  #   `:unknown`      — replication is on but no lineage could be read at open. Stamp NOTHING; an
  #                     absent stamp reads as "unknown" and makes the object un-overridable, the
  #                     same safe answer this function gives for an unreadable WAL.
  #   `:disabled`     — replication is off. Leave the LOCK epoch in the slot exactly as before, so
  #                     a non-replicating node is bit-for-bit unchanged. The `Map.get` default
  #                     covers the synthetic states built by callers that carry no lineage key.
  defp stamp_epoch(%{lease: %{epoch: lock}} = state) when is_integer(lock) do
    case Map.get(state, :lineage, :disabled) do
      n when is_integer(n) -> n
      :unknown -> nil
      :disabled -> lock
    end
  end

  defp stamp_epoch(_state), do: nil

  # What gets WRITTEN to the object's own lineage metadata key, as opposed to what fills the
  # position stamp. Only a real integer: `:disabled` and `:unknown` both mean "this coordinator has
  # no lineage to claim", and the backends take `nil` as leave-any-previous-value-alone. Erasing a
  # shard's lineage would reintroduce exactly the reset the key exists to prevent.
  defp lineage_to_store(n) when is_integer(n), do: n
  defp lineage_to_store(_), do: nil

  defp flush_position(state, pre, epoch) do
    case Fathom.Shard.Replication.Wal.read(state.path <> "-wal") do
      # commit_extent, not `size` (expert review 2026-08-20 #5): the file length is a high-water
      # mark that can include frames from a rolled-back transaction. A follower's position is
      # now the committed extent, so the object's stamp has to be on the same scale or the two
      # are not comparable and the object would always look ahead.
      {:ok, %{ckpt_seq: gen, commit_extent: extent}} ->
        %{epoch: epoch, wal_gen: gen, offset: extent}

      {:ok, :empty} ->
        position_after_checkpoint(epoch, pre)

      {:error, _} ->
        nil
    end
  end

  # The WAL is empty or gone AFTER the flush. What that means depends entirely on what was there
  # BEFORE, and the two cases are opposite.
  #
  # Known generation ⇒ the checkpoint folded it in ⇒ claim the next generation at offset 0.
  defp position_after_checkpoint(epoch, {:ok, %{ckpt_seq: gen}}),
    do: %{epoch: epoch, wal_gen: gen + 1, offset: 0}

  # Empty before AND after is AMBIGUOUS, and `nil` is the only safe answer. It reads like "a brand
  # new shard at generation 0" — which is one of the two situations — but it is equally a shard
  # whose WAL was truncated and unlinked by an EARLIER cycle, which may sit at any generation with
  # replicas holding frames from it. `{epoch, 0, 0}` would lose to every one of them. `nil` means
  # "unknown", making the object un-overridable, and that costs nothing real: an empty WAL at flush
  # time means there are no un-folded writes, so the object IS complete and preferring it over any
  # replica is correct. The price is that promote-on-open will not fire for this shard — i.e. the
  # pre-A2 behaviour, which AGENTS.md already calls "never worse than off".
  defp position_after_checkpoint(_epoch, _pre), do: nil

  defp snapshot_and_upload(state) do
    temp = "#{state.path}.snap.#{System.unique_integer([:positive])}"

    # The generation, captured before anything mutates the WAL (expert review 2026-08-20 #4).
    # `snapshot/2` is FREQUENTLY the last connection — its own comment says the periodic flush
    # "fires on any dirty shard, which is routinely one with zero checked-out streams" — and the
    # last close unlinks `-wal`/`-shm`. Only consulted when the post-snapshot read finds nothing;
    # the live read still happens after the snapshot and still wins whenever the WAL survives, so
    # the over-claim rule below is unchanged.
    pre = Fathom.Shard.Replication.Wal.read(state.path <> "-wal")

    try do
      # Integrity BEFORE the snapshot: quarantine_corrupt!/2 renames the live path, so checking
      # after would spend a full VACUUM on a file we are about to refuse and move aside.
      with :ok <- verify_and_snapshot(state, temp),
           :ok <- recheck_before_put(state),
           {:ok, new_etag} <-
             Storage.flush(
               state.id,
               temp,
               state.etag,
               flush_position(state, pre),
               lineage_to_store(state.lineage)
             ) do
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

  # Re-check ownership between the SNAPSHOT and the PUT (expert review 2026-08-01 #28).
  #
  # `Fence.check` runs once, before this whole sequence, and `valid_for_write?`'s contract is
  # "valid WITH MARGIN … it won't expire mid-write". The margin is a hardcoded `ttl/3` (10 s at
  # the default 30 s TTL) and is not derived from the write it covers — which is
  # `verify_and_snapshot` (a full `quick_check` page scan, added by #14) + `VACUUM INTO` + a
  # full-object PUT. Measured on this codebase against `Storage.Local`, no network at all:
  #
  #     1.4 MB      8.9 ms          71 MB     265 ms
  #     13.8 MB    55.4 ms         284 MB   1,070 ms
  #
  # Dead linear at ~3.8 ms/MB, so the LOCAL half alone reaches the 10 s margin around 2.6 GB —
  # under the 4 GiB size cap `Connection` enforces, and before a single byte goes over the wire.
  # The promise is not deliverable for a large shard, and it fails in a biased direction: big
  # shards take the `:superseded` self-fence path and quarantine an interval of acked writes,
  # small ones never do.
  #
  # Re-checking here means the margin only has to cover the PUT rather than scan + VACUUM + PUT.
  # It does not make the promise true at every size — nothing cheap does, and a lease renewal
  # that outlives the upload is a different design — but it removes the part of the exposure that
  # is pure local CPU and I/O, which at 284 MB is already a full second.
  #
  # HEARTBEAT MODE ONLY, and that is the point. `Heartbeat.valid_for_write?/1` is a lock-free ETS
  # read (`heartbeat.ex`), so this costs nothing. In LEGACY mode the equivalent check is a renew
  # PUT — a store round trip per flush — so it is skipped there rather than paying network to
  # shorten a network-bound window. (An earlier reading of this finding dismissed the whole fix as
  # "a round trip per flush"; that is true only of the legacy path.)
  defp recheck_before_put(%{acquire_gen: gen}) when is_integer(gen) do
    case heartbeat_recheck(gen) do
      :ok ->
        :ok

      _ ->
        # Do NOT upload: our liveness went unconfirmed while we were building the snapshot, so a
        # peer may be taking over. Staying dirty and retrying is the safe direction — the same
        # verdict `Fence.check` produces for :not_valid, reached one step later.
        {:error, :ownership_unconfirmed_before_put}
    end
  end

  defp recheck_before_put(_state), do: :ok

  # A DEAD HEARTBEAT PROCESS MUST DEGRADE, NOT CRASH THE FLUSH (expert review 2026-08-24 #18).
  #
  # Every other caller of `Heartbeat.valid_for_write?/1` wraps it so a dead heartbeat degrades
  # gracefully — `Fence.heartbeat_valid/2` catches `:exit` and answers `:legacy`,
  # `Fence.generation/1` catches `:exit` and answers `nil`. This one, added later by review
  # 2026-08-01 #28, called it BARE. When the heartbeat process is down its named ETS status table
  # dies with it, `status/0` answers `:down`, and the fallback is a `GenServer.call` that exits
  # `:noproc`.
  #
  # The consequence is not a crash loop, it is a SILENT STOP TO DURABILITY. A coordinator opened in
  # heartbeat mode keeps `acquire_gen` as an integer for its whole life (`Fence.revalidate/2`
  # preserves it deliberately), so while the heartbeat is down `Fence.check/2` correctly degrades
  # to `:legacy` and renews the lock — and then the flush task dies here. That lands on the
  # `{:DOWN, …}` flush-task clause, which records a transient failure and reschedules, and never
  # reaches `note_not_valid/1`, so the write circuit-breaker cannot arm on that path either. The
  # shard stays dirty, keeps ACKing writes, and never flushes: unbounded RPO on a node that looks
  # healthy. The supervisor normally restarts the heartbeat in milliseconds, which bounds it in the
  # common case — but the entire point of the `:legacy` branch is the case where it does not.
  #
  # PROCEEDING is the right direction of the two safe ones. `Fence.check/2` has already run for
  # this flush and, in the degraded mode, performed the legacy renew PUT — which IS the ownership
  # proof in that mode. Failing closed here would stop durability for a reason the fence itself did
  # not object to, which is the failure this finding is about.
  defp heartbeat_recheck(gen) do
    Heartbeat.valid_for_write?(gen)
  catch
    :exit, _ -> :ok
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
  #
  # ONE CONNECTION, NOT TWO (expert review 2026-08-26 #12). This used to be `verify_snapshot/2`
  # followed by a separate `snapshot/2`, each with its own `Connection.open` … `Connection.close`
  # on the SAME path. Two costs, both per flush per write-active shard:
  #
  #   * A second full connection open — `File.dir?`/`mkdir_p`, `sqlite3_open`, seven pragmas, the
  #     `PRAGMA page_size` prepare/fetch/release, and an extension load (enable → load → disable),
  #     all dirty-IO NIFs. Measured at roughly 429 µs on this machine (review #10's probe).
  #   * A WAL delete/recreate cycle. `snapshot/2`'s own comment records that the periodic flush
  #     "fires on any dirty shard, which is routinely one with zero checked-out streams", so the
  #     verify connection's close was FREQUENTLY THE LAST ONE — triggering SQLite's close-time
  #     checkpoint and unlinking `-wal`/`-shm`, which the snapshot connection then recreated.
  #
  # `checkpoint_and_verify/1` describes this identical churn verbatim as the reason the DROP path
  # was merged onto one connection (review 2026-07-24 #31). `verify_snapshot/2` arrived later
  # (review 2026-08-01 #14) and never got the same treatment: the rare path was fixed and the
  # dominant one was not.
  #
  # On a replicating fleet the extra close-time checkpoint also started a new WAL generation, which
  # is the `{:reset, 0, size}` full-WAL-reship path — and it is the mechanism that manufactures the
  # stale `wal_gen` in review #2, so folding it narrows that hazard's frequency too.
  #
  # THE THREE GUARDS THAT MUST SURVIVE ANY FUTURE EDIT HERE:
  #   (a) `quick_check` runs on the LIVE file, never on the VACUUM temp — see the probe above.
  #   (b) It SHORT-CIRCUITS before the VACUUM: a corrupt shard must not pay a full snapshot.
  #   (c) The extension stays loaded on this connection (`Connection.open/2` does it): `VACUUM INTO`
  #       rebuilds indexes and `quick_check` verifies them, so a Django expression index over a UDF
  #       would fail "no such function" without it.
  defp verify_and_snapshot(state, temp) do
    case Connection.open(state.path) do
      {:ok, conn} ->
        try do
          with :ok <- run_quick_check(conn) do
            do_snapshot(conn, temp)
          end
        after
          Connection.close(conn)
        end

      other ->
        # An open failure is "I could not check it", not "it is corrupt" — same classification the
        # separate `verify_integrity/1` path applied.
        integrity_verdict({:error, {:quick_check_open_failed, other}})
    end
  end

  # `quick_check` on the open connection, gated by `:verify_flush_integrity`. Split out so the
  # short-circuit above reads as one line and (b) cannot be accidentally reordered past the VACUUM.
  defp run_quick_check(conn) do
    if verify_flush_integrity?() do
      case Connection.query(conn, "PRAGMA quick_check", []) do
        {:ok, %{rows: [["ok"]]}} -> :ok
        {:ok, %{rows: rows}} -> integrity_verdict({:error, {:quick_check, rows}})
        {:error, reason} -> integrity_verdict({:error, {:quick_check_failed, reason}})
      end
    else
      :ok
    end
  end

  # Classify a failed integrity check. "I could not check it" is not "it is corrupt" (expert review
  # 2026-08-20 #2): an open failure under fd pressure (EMFILE, on a node whose density is the design
  # premise), a :busy from a concurrent writer, or a :query_timeout on a large quick_check all used
  # to take the corruption branch. Both verdicts REFUSE the flush, so the good stored object stays
  # authoritative either way; what differs is the alarm — :corrupt_local is permanent and escalates
  # on first occurrence, :integrity_unknown is transient and just retries next interval.
  #
  # NEITHER branch quarantines. This runs while the shard is being SERVED, and quarantine_corrupt!/2
  # renames the live file and unlinks its -shm; that rename is what made this a data-loss path
  # rather than a refused flush.
  defp integrity_verdict({:error, reason}) do
    # "I could not check it" is not "it is corrupt" (expert review 2026-08-20 #2). An
    # Sqlite3.open failure under fd pressure (EMFILE — on a node whose density is the design
    # premise), a :busy from a concurrent writer, or a :query_timeout on a large quick_check all
    # used to take the corruption branch. Both verdicts REFUSE the flush, so the good stored
    # object stays authoritative either way; what differs is the alarm — :corrupt_local is
    # permanent and escalates on the first occurrence, :integrity_unknown is transient and just
    # retries next interval.
    #
    # NEITHER branch quarantines. This path runs while the shard is being SERVED, and
    # quarantine_corrupt!/2 renames the live file and unlinks its -shm; see the hazard note on
    # that function. That rename is what made this a data-loss path rather than a refused flush.
    case classify_integrity_failure(reason) do
      :corrupt -> {:error, {:corrupt_local, reason}}
      :unknown -> {:error, {:integrity_unknown, reason}}
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

  # The LIVE db failed quick_check while we are serving it (expert review 2026-08-20 #2).
  #
  # Escalate on the FIRST occurrence instead of waiting for `flush_failure_alert_threshold/0`, for
  # the same reason `object_too_large` does in record_flush_failure/2: retrying cannot fix it, so a
  # threshold only delays the alarm while the RPO grows. Keep serving, stay dirty — the good stored
  # object is untouched and stays authoritative for the next open. The `corrupt_flush` event is the
  # same one quarantine_corrupt!/2 emits on the drop path, so the existing alert keeps firing.
  #
  # DELIBERATELY NOT stopping the coordinator, though the finding recommended it — recorded here so
  # the next reader does not "simplify" it back. The catastrophic chain is rename-the-live-file →
  # a new connection creates an EMPTY db at that path → the next flush PUTs the empty db over the
  # good object under a valid If-Match. Not renaming breaks that at step one, so stopping buys
  # nothing for the data-loss property it was proposed to fix. Meanwhile the codebase already has a
  # settled policy for "this flush can never succeed" — `object_too_large` alarms loudly and keeps
  # serving — and stopping here would be a second, inconsistent policy carrying a new failure mode
  # of its own (a restart loop whenever the check false-positives).
  defp apply_flush_verdict(state, {:error, {:corrupt_local, _} = reason}) do
    :telemetry.execute([:fathom, :shard, :corrupt_flush], %{count: 1}, %{shard_id: state.id})

    Logger.error(
      "shard #{state.id}: the LIVE local db failed quick_check (#{inspect(reason)}) while serving. " <>
        "REFUSING every flush so the last good stored object stays authoritative — which means this " <>
        "shard's acked writes are NOT being made durable and its RPO is growing without bound. The " <>
        "local file is left in place on purpose (renaming it under live streams is the data-loss " <>
        "path review #2 closed). Drain the shard: the drop path quarantines it safely and the next " <>
        "open cold-pulls the good object."
    )

    {{:error, reason}, record_flush_failure(state, reason)}
  end

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
  # Takes an ALREADY-OPEN connection (review 2026-08-26 #12) so the verify and the snapshot share
  # one handle. Opening and closing is the caller's job — `verify_and_snapshot/2` owns the `after`.
  defp do_snapshot(conn, dest) do
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

    case result do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # --- helpers ---

  # Public for Fathom.Shards' novel-shard check (a present local file means the shard
  # exists — an authoritative un-flushed copy — so it is never "novel").
  @doc false
  @spec db_path(Fathom.ShardId.t()) :: Path.t()
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
  # The spread a lapse revalidation is jittered over, DERIVED FROM THE POPULATION being spread
  # (expert review 2026-08-26 #13b).
  #
  # Round-2 #26 replaced an inline O(open-shards) storm with a jittered one, but the window was a
  # fixed 2 000 ms no matter how many coordinators were subscribed. The lapse broadcast reaches
  # every one of them, so at the shipped `:max_open_shards` of 10 000 that is up to ~5 000
  # `check_lease` GETs/second offered to one connection pool — fired at the exact instant the node
  # has just proved it could not keep one small object fresh. The TAIL was bounded by the pool, not
  # by the jitter, which is another way of saying it was not bounded.
  #
  # Spread = population / target rate. `@default_lapse_revalidations_per_sec` is 500 for a reason
  # worth keeping: at 1 000 open shards it yields exactly the 2 000 ms that used to be hardcoded,
  # so the old behaviour is the value this formula takes at one specific fleet size instead of at
  # every fleet size.
  #
  # CAPPED AT `Heartbeat.margin_ms()` (10 s at defaults), which is the audit's own guard and not a
  # round number: the write-fence's arming instant is measured from the first `:not_valid`, so a
  # spread longer than the margin would push revalidation past the moment the breaker is supposed
  # to decide, eroding the circuit breaker the spread exists to protect. Above ~5 000 shards the
  # cap binds and the achieved rate is `open / margin_seconds` — still 5x better than the fixed
  # window, and the honest ceiling for this mechanism. Beyond that the fix is a node-wide token
  # bucket, which the audit names and this does not build.
  #
  # FLOORED AT 1: `:rand.uniform/1` raises on 0, and an empty registry is reachable (the broadcast
  # can land as the last coordinator stops).
  #
  # An explicit `:lapse_revalidate_jitter_ms` still WINS, so an operator who pinned the old knob
  # keeps exactly what they set.
  # PURE IN `open_shards` so it can be tested without racing the registry (AGENTS.md: test a
  # concurrency-adjacent decision at the pure-function level). `@doc false` public for the same
  # reason `Shards.report_fork/2` is — the classification is the thing worth pinning, and it is
  # not observable through the timer it feeds.
  @doc false
  @spec lapse_spread_ms(non_neg_integer()) :: pos_integer()
  def lapse_spread_ms(open_shards) do
    case Application.get_env(:fathom, :lapse_revalidate_jitter_ms) do
      ms when is_integer(ms) and ms > 0 ->
        ms

      _ ->
        target =
          Application.get_env(
            :fathom,
            :lapse_revalidations_per_sec,
            @default_lapse_revalidations_per_sec
          )

        (open_shards * 1000 / target)
        |> round()
        |> max(1)
        |> min(Heartbeat.margin_ms())
    end
  end

  defp lapse_jitter_ms, do: lapse_spread_ms(Fathom.Shards.open_count())

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

  # The gate-refusal backoff, scaled to the herd (expert review 2026-08-26 #16). DISTINCT from
  # `schedule_flush_backoff/1`, which serves the still-dirty-after-a-successful-flush case: that
  # one means "there is more work and nobody is stopping me", and slowing it would delay a flush
  # that can actually run. This one means "the node is saturated and I was turned away", where the
  # probe itself is the cost. Only the second is scaled.
  defp schedule_gate_backoff(state) do
    state = cancel_flush(state)

    delay =
      jitter_interval(
        FlushGate.backoff_ms(
          Fathom.Shards.open_count(),
          gate_probe_target_per_sec(),
          flush_backoff_ms(),
          flush_interval_ms()
        )
      )

    %{
      state
      | flush_timer: Process.send_after(self(), :durability_flush, delay),
        flush_timer_slow?: false
    }
  end

  defp gate_probe_target_per_sec,
    do:
      Application.get_env(
        :fathom,
        :flush_gate_probe_target_per_sec,
        @default_gate_probe_target_per_sec
      )

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

  # The lapse revalidation's verdict, applied back in the coordinator (expert review 2026-08-26
  # #13a). Every branch is identical to what the inline version did — only WHERE the check_lease
  # GET ran changed. No storage I/O here by construction.
  defp apply_lapse_verdict({:ok, updates}, state) do
    {:noreply, clear_write_fence(Map.merge(state, updates))}
  end

  defp apply_lapse_verdict(:superseded, state) do
    Logger.error("shard #{state.id}: lease superseded (heartbeat lapse broadcast); self-fencing")

    {:stop, {:shutdown, :lease_lost}, %{state | lease_lost: true}}
  end

  # Ownership unconfirmed (transient) — the next flush's fence remains the guard for the
  # REFUSAL, but the CLOCK starts here (expert review 2026-08-24 #19).
  #
  # `note_not_valid/1` was reachable from exactly one place: the `:fence_skip` branch of the
  # durability-flush result. `handle_info(:durability_flush, …)` short-circuits at
  # `not unflushed?(state)` BEFORE any fence check, so on a shard that is not dirty the clock
  # never started. A read-mostly shard on a node cut from storage therefore observed nothing:
  # the heartbeat lapsed, `ttl + steal_margin` passed, a peer became entitled to steal — and
  # ten minutes later the first client write was ACKed, because the fence had never armed.
  # Only then did the clock start, from zero, and writes kept being accepted for another full
  # `margin + steal_margin`. `docs/single-writer.md` promises the loss window collapses to
  # ~`ttl + steal_margin`; on that path it was unbounded relative to when ownership was lost.
  #
  # Here is the right moment BY CONSTRUCTION, which is why this is stamped here rather than
  # back-dated from the heartbeat's deadline as the finding proposed. Back-dating can only make
  # the breaker arm EARLIER, and earlier on a healthy node is a tenant write outage; whether a
  # stale `mono_deadline_ms` can survive a heartbeat restart and cause exactly that could not be
  # settled by reading. This path cannot fire early: it runs only after `mark_lapse/1` has
  # edge-detected a real lapse and bumped the generation, and `Fence.check/2` has then failed to
  # reconfirm ownership against storage. Its worst case is the status quo, not an outage.
  #
  # STILL BOUNDED BY THE NEXT FLUSH TICK, and worth knowing: this starts the clock but does not
  # publish the fence, because at lapse time the node is not yet provably stealable — the delay
  # IS the semantics. So the first write after a long lapse is still accepted, and the fence
  # arms on the flush tick that write triggers, with the elapsed time now measured from the
  # lapse instead of from the write. Closing that last gap needs a one-shot re-check timer per
  # lapse episode; deliberately not bundled here.
  defp apply_lapse_verdict(:skip, state), do: {:noreply, note_not_valid(state)}

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
  # THE `:became_dirty` SIGNAL FIRES AT MOST ONCE PER STREAM, EVER (expert review 2026-08-26 #4).
  #
  # The edge-triggered design above rests on `ShardExecutor` casting `:became_dirty` on a write.
  # That cast is guarded by `signal_dirty_once/2`, keyed on the CONNECTION in the stream's process
  # dictionary — so it fires once for the lifetime of that stream. But a shard enters the slow
  # cadence many times per stream lifetime:
  #
  #   t=0  the stream writes → :became_dirty → fast timer. Flag set.
  #   t=5  the flush lands → this function runs → conns > 0 and now CLEAN → 50 s timer.
  #   t=6  the same stream writes again → signal_dirty_once/2 sees its flag → NO CAST.
  #        The shard is dirty with a 50 s timer armed.
  #
  # Nothing rescued it: the checkout re-arm is gated on `flush_timer == nil`, and the
  # `:became_dirty` handler's slow-timer branch is correct but never receives a message. So
  # `docs/durability.md`'s "loss ≈ the flush interval" silently became 10× the interval for any
  # stream writing less often than the interval — the django-libsql WebSocket shape AGENTS.md calls
  # the primary production client path. Invisible under `CONN_MAX_AGE=0`, where every request is a
  # fresh stream and the flag resets, which is the configuration the tests and the chaos rig use.
  #
  # The fix needs no new signalling: a flush that actually UPLOADED writes is itself evidence of an
  # active writer, so stay fast for the next interval and let the absence of uploads be what slows
  # the shard down. `unflushed?/1` cannot answer this — immediately after a successful flush it is
  # false by construction, which is precisely the moment the slow timer was being armed.
  defp schedule_flush(state) do
    cond do
      unflushed?(state) -> do_schedule_flush(state)
      state.wrote_during_checkout? -> do_schedule_flush(state)
      true -> do_schedule_flush(state, @clean_poll_multiplier)
    end
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
              ),
            flush_timer_slow?: multiplier > 1
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

    %{
      state
      | flush_timer: Process.send_after(self(), :durability_flush, delay),
        flush_timer_slow?: false
    }
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

  defp cancel_flush(%{flush_timer: nil} = state), do: %{state | flush_timer_slow?: false}

  defp cancel_flush(%{flush_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | flush_timer: nil, flush_timer_slow?: false}
  end
end
