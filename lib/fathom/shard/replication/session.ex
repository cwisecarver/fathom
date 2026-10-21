defmodule Fathom.Shard.Replication.Session do
  @moduledoc """
  One replication session per shard — the commit-path integration for A2.
  See `docs/a2-quorum-replication.md`.

  Owns the shard's `Primary` state (`{wal_gen, salt1, offset}`) and is the **serialization point**
  for shipping. One process per shard, so two streams that commit back to back cannot both compute
  a delta from the same stale offset and ship overlapping ranges.

  ## Why this is not in `Fathom.Shard`

  The coordinator would have been the obvious home — it is already one process per shard and
  already owns the lease this module fences with. It is also ~3000 lines of intricate
  lease/fence/flush state machine, and a synchronous multi-millisecond network wait inside its
  mailbox would sit in front of every checkout and every durability flush for that shard. Keeping
  the wait in a separate process means a slow follower delays commits for its shard and nothing
  else.

  ## The gate, and what a tenant sees when the quorum fails

  Off unless `:replication_enabled`. When on, a committing statement does not return to the client
  until `q` followers have acked.

  **The local commit has already happened by then** — SQLite committed before the WAL could be
  read, and there is no un-commit. So a quorum failure returns an error for a write that IS durable
  locally and WILL reach S3 on the next flush. That is deliberate and it is the honest direction:
  the client asked for a quorum-durable write and did not get one, so it must not be told it did.
  The cost is an **at-least-once** hazard — a client that retries may apply the write twice — which
  is the same hazard any commit-ack-lost path already has (`docs/durability.md`), not a new one.

  Reporting success instead would be worse in the way that matters: it would make the failure
  invisible exactly when the data is least protected.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication
  alias Fathom.Shard.Replication.LagGauge
  alias Fathom.Shard.Replication.Primary
  alias Fathom.Shard.Replication.Protocol, as: ReplProtocol
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Protocol.SeedBegin
  alias Fathom.Shard.Replication.SeedGate
  alias Fathom.Shard.Replication.SeedPin
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Shard.Replication.WalSync

  @registry Fathom.Shard.Replication.SessionRegistry

  # One Session lives per replicated shard, so the fleet carries thousands of them, almost all idle
  # between commits. Same treatment as `Fathom.Shard` and `Shipper` (expert review 2026-10-10 #P1):
  # `fullsweep_after: 0` because the live set is small (a position map and a held fd) and ERTS's
  # default effectively never full-sweeps, so garbage from past pushes lingers; `hibernate_after`
  # compacts an idle Session's heap to almost nothing. Idle hibernation costs a wake-up copy only.
  @spawn_opt [fullsweep_after: 0]
  @hibernate_after_ms 30_000

  # Reject reasons after which NO reply is outstanding from that follower, so its `inflight`
  # expectation can be dropped. Everything here is either the follower's own answer
  # (`:stale_wal_gen`, `:stale_epoch`, `:unknown_shard`, `:internal`) or a dead socket
  # (`:disconnected`, where `Shipper.drop/2` has already failed every waiter on it).
  #
  # DELIBERATELY ABSENT: `:already_in_flight` and `:overloaded`. Both are generated LOCALLY by our
  # own `Shipper` — the frame never reached the follower, which is therefore still on the hook for
  # an EARLIER push. Clearing on those discards the expectation that the earlier reply needs, and
  # `reconcile/2`'s `:offset_mismatch` branch then finds nothing and throws the correction away, so
  # the primary never learns where the follower actually is and re-plans from a position it has
  # already been told is wrong — a laggard stranded permanently rather than for one round.
  #
  # Found by `replication_seed_test.exs`'s "a held laggard reply strands it" once
  # `Fathom.Test.PausablePeer` made the race deterministic; it is a regression this list narrows
  # from the first draft of `3a6c6a3`, which cleared on every reason.
  @settled_rejects [:disconnected, :stale_wal_gen, :stale_epoch, :unknown_shard, :internal]

  # How long a seed may be OUTSTANDING before `start_seeds/3` treats its entry as stale, kills the
  # task and allows a fresh one (expert review 2026-08-20 #26). A monitor covers a task that died;
  # this covers one that is alive and wedged, which a socket stream can be. Comfortably past
  # `await_seed_reply/4`'s own 30 s wait plus a large transfer, so a seed that is merely slow is
  # never cut off — the entry only has to expire before the follower gives up asking, and it asks
  # again on every commit. Configurable so it can be exercised without a two-minute test.
  @default_seed_expiry_ms 120_000

  # How much SHORTER the handler's internal deadline is than the caller's `GenServer.call` timeout
  # (expert review 2026-08-31 #21). The caller starts its call timer BEFORE the message is delivered
  # and the reply travels back after the handler finishes, so an internal deadline equal to the call
  # timeout means a commit that runs right to the deadline is abandoned by the caller — surfacing a
  # near-miss quorum as `{:session_down, :timeout}` instead of the handler's clean `{:no_quorum, _}`
  # (or success). The margin covers that head-start + reply delivery; floored at half the timeout so
  # a large margin config can't gut a small timeout.
  @default_reply_margin_ms 100
  # How many times one commit re-plans after its WAL restarted mid-round (expert review 2026-10-20
  # #9) before failing. A restart race is transient; this only bounds a failure that is not.
  @max_replans 3
  # How long `ensure_started/3` waits for a stale predecessor to stop politely before killing it.
  @stale_stop_ms 1_000

  # ------------------------------------------------------------------------------------------
  # api
  # ------------------------------------------------------------------------------------------

  @doc "Whether commit-path replication is on. Off by default."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :replication_enabled, false)

  @doc """
  Replicate everything committed to `wal_path` since the last successful ship.

  Synchronous by design: this is the quorum wait, and the point of A2 is that the client's success
  is gated on it. Returns `:ok` when nothing needed shipping, too.

  ## Options

    * `sync: true` — the caller's handle runs `synchronous=NORMAL` under the deferred WAL sync
      (`Fathom.Shard.Replication.WalSync`, expert review 2026-10-21 #7). The round then reports
      `:ok` only after an `fdatasync` of the `-wal` that STARTED after this caller's COMMIT has
      succeeded, as well as the quorum. Default `false`: SQLite already synced the commit (FULL).
  """
  @spec commit(String.t(), Path.t(), pid(), keyword()) :: :ok | {:error, term()}
  def commit(shard_id, wal_path, coordinator, opts \\ []) do
    # CALLER-SIDE TOTAL SPAN (expert review 2026-10-17 #10). `:replication, :commit` is timed inside
    # `handle_call`, so it misses the Session-mailbox queue wait and `ensure_started`, both of which
    # the tenant also waits on. This one covers the whole call; the gap between the two is the queue.
    t0 = System.monotonic_time()
    reply = commit_call(shard_id, wal_path, coordinator, Keyword.get(opts, :sync, false))

    :telemetry.execute(
      [:fathom, :replication, :commit_total],
      %{duration: System.monotonic_time() - t0},
      %{outcome: commit_outcome(reply)}
    )

    reply
  end

  defp commit_call(shard_id, wal_path, coordinator, sync?) do
    with {:ok, pid} <- ensure_started(shard_id, coordinator) do
      # The caller's own give-up time travels with the request (expert review 2026-10-20 #5(a)),
      # so a request still queued when its caller has gone is dropped instead of executed. Taken
      # immediately before the call, on the same clock `GenServer.call`'s timer runs on.
      t = timeout()

      # The deferred-sync request carries a fifth element (2026-10-21 #7). The four-tuple stays
      # the FULL caller's shape, so a flag-off node's messages do not change.
      msg =
        if sync?,
          do: {:commit, wal_path, :checkpoint, now_ms() + t, true},
          else: {:commit, wal_path, :checkpoint, now_ms() + t}

      GenServer.call(pid, msg, t)
    end
  catch
    :exit, reason -> {:error, {:session_down, reason}}
  end

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, opts[:shard_id]},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  def start_link(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)

    GenServer.start_link(__MODULE__, opts,
      name: {:via, Registry, {@registry, shard_id, Keyword.fetch!(opts, :coordinator)}},
      spawn_opt: @spawn_opt,
      hibernate_after: @hibernate_after_ms
    )
  end

  @doc """
  Forget a shard's replication state — used when it drains, and by tests.

  A no-op when replication is not running. The registry only exists under `Fleet`, so with the
  feature off (or already shut down) there is nothing to look up, and a caller tidying up should
  not have to know which. Raising here made teardown fail in tests that had stopped `Fleet` first.
  """
  @spec stop(String.t()) :: :ok
  def stop(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] -> GenServer.stop(pid, :normal)
      [] -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  # A registered Session is only reusable by the coordinator it was built for (expert review
  # 2026-10-12 #12). The Registry value is that coordinator's pid. A successor coordinator (the
  # shard drained and reopened) that found its predecessor's Session still registered used to
  # route its first commit into a Session about to stop on the old coordinator's DOWN: the call
  # exited `:session_down`, surfaced as a 503 for a write already applied locally, and the
  # client's retry duplicated it. So a Session of a different (or dead) coordinator is stopped
  # SYNCHRONOUSLY and replaced. The Registry drops the entry on the Session's exit, a moment after
  # our DOWN, hence the bounded retry rather than assuming the slot is already free.
  #
  # The WHOLE replacement is bounded by one deadline, `commit_deadline_budget/0` (expert review
  # 2026-10-13 #34). It used to be up to 100 retries of a 5 s stop plus a 5 s DOWN wait, outside the
  # caller's call timeout, so a predecessor wedged in a long call blocked a tenant commit for
  # minutes. A predecessor that ignores a polite stop is KILLED: it belongs to a coordinator that is
  # gone or replaced, so there is nothing in it worth protecting. Past the deadline the commit fails
  # retryably with `{:error, :stale_session}`.
  defp ensure_started(shard_id, coordinator) do
    deadline = System.monotonic_time(:millisecond) + commit_deadline_budget()
    ensure_started(shard_id, coordinator, deadline)
  end

  defp ensure_started(shard_id, coordinator, deadline) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, ^coordinator}] ->
        {:ok, pid}

      [{pid, _other}] ->
        if System.monotonic_time(:millisecond) < deadline do
          stop_and_wait(pid, deadline)
          Process.sleep(1)
          ensure_started(shard_id, coordinator, deadline)
        else
          {:error, :stale_session}
        end

      [] ->
        case DynamicSupervisor.start_child(
               Fathom.Shard.Replication.SessionSupervisor,
               {__MODULE__, shard_id: shard_id, coordinator: coordinator}
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, _pid}} ->
            if System.monotonic_time(:millisecond) < deadline,
              do: ensure_started(shard_id, coordinator, deadline),
              else: {:error, :stale_session}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # A polite stop first (capped well inside the deadline), then a kill. The DOWN of a killed process
  # is prompt, but the wait is still capped by the deadline so this cannot overrun it.
  defp stop_and_wait(pid, deadline) do
    ref = Process.monitor(pid)
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    try do
      GenServer.stop(pid, :normal, min(remaining, @stale_stop_ms))
    catch
      :exit, _ -> Process.exit(pid, :kill)
    end

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> Process.demonitor(ref, [:flush])
    end
  end

  @doc false
  def timeout, do: Application.get_env(:fathom, :replication_timeout_ms, 5_000)

  defp reply_margin,
    do: Application.get_env(:fathom, :replication_reply_margin_ms, @default_reply_margin_ms)

  # The handler's internal commit deadline BUDGET — shorter than the caller's `GenServer.call`
  # timeout by `reply_margin`, so the handler always replies before the caller gives up (#21).
  # Floored at half the call timeout so a large margin can't gut a small timeout.
  @doc false
  def commit_deadline_budget, do: max(div(timeout(), 2), timeout() - reply_margin())

  # ------------------------------------------------------------------------------------------
  # server
  # ------------------------------------------------------------------------------------------

  @impl true
  def init(opts) do
    coordinator = Keyword.fetch!(opts, :coordinator)

    # The cached epoch must never outlive the ownership it describes. Monitoring the coordinator
    # means a steal — which stops it — also stops this session, so the next commit starts a fresh
    # one and re-reads the epoch. Without this, a deposed node would keep shipping under its old
    # epoch and rely entirely on the follower to notice.
    Process.monitor(coordinator)

    {:ok,
     %{
       shard_id: Keyword.fetch!(opts, :shard_id),
       coordinator: coordinator,
       epoch: nil,
       # Resolved alongside `epoch` by `with_epoch/1` and cached with it. A DIFFERENT counter --
       # see `Fathom.Shard.lineage/1`. 0 means "no claim", which is also what a coordinator with
       # replication off reports.
       lineage: 0,
       # The coordinator's fence generation at open (expert review 2026-09-05 #5), resolved
       # alongside `epoch`/`lineage` in `with_epoch/1` and cached with them (fixed for the
       # coordinator's life in heartbeat mode). An integer gates the commit ack on
       # `Heartbeat.valid_for_write?/1`; `nil` (legacy mode, no heartbeat) leaves the ack ungated.
       acquire_gen: nil,
       # The WAL the last push shipped from, and that WAL's monotone ordinal (expert review
       # 2026-08-26 #2). Cached together so the coordinator is asked only when the salt CHANGES —
       # a WAL recreate, which is rare — because this sits on the tenant's synchronous commit path
       # and `Fathom.Shard.wal_ordinal/2` is a GenServer.call. `nil`/0 until the first push.
       wal_salt: nil,
       wal_ordinal: 0,
       # The WAL this shard commits through, remembered from the last commit. A follower's reply can
       # arrive long after the call that provoked it returned, and acting on `:unknown_shard` means
       # reading the shard's files — so the handler needs the path without a caller to supply it.
       # `nil` until the first commit, which is also the first moment a reply can exist.
       wal_path: nil,
       # A held, inode-revalidated read fd for `wal_path` (expert review 2026-08-26 #32). One
       # commit used to do THREE `open`/`close` pairs on this file — `Wal.read/1` in `ship/5`,
       # `Wal.read_delta/3` per range, and `Wal.read/1` again for the stability re-check — all on
       # dirty-IO schedulers on the tenant's synchronous write path. Measured at 123 µs/commit
       # against 19 µs for a held fd, i.e. ~104 µs of the 288 µs `replication_cost_test` attributes
       # to replication.
       #
       # `nil` when nothing is held, which is the starting state and the state after any read
       # error. `Fathom.Shard.Replication.Wal` revalidates it by INODE on every use, so an
       # unlink+recreate reopens rather than silently serving the old generation.
       wal_fd: nil,
       # The armed quiet-shard fd release timer (expert review 2026-10-20-2027 #2), nil when none.
       # See `handle_info({:release_wal_fds, _}, _)`.
       fd_release_ref: nil,
       # PER-FOLLOWER position: %{shipper => %{wal_gen, salt1, offset}}.
       #
       # A single shared offset was the original shape and it made catch-up impossible: followers
       # seeded at different times sit at different positions, so one delta cannot be correct for
       # all of them. A replica that fell behind was sent bytes starting past where it actually
       # was, rejected them forever, and stayed silently un-replicated while the shard reported a
       # healthy quorum. Each follower now gets the delta from ITS own position.
       followers: %{},
       # What each follower WOULD be at if it accepts the push currently outstanding to it:
       # %{shipper => %{wal_gen, salt1, offset}}.
       #
       # A reply is only meaningful against the push that provoked it, and the two arrive at
       # different times: `ship_quorum/3` returns at the Q-th ack, so the rest answer into this
       # mailbox afterwards and are read by a later `drain_late_replies/2` with no plan in scope.
       # Recording the expectation at send time is what lets that drain act on a straggler's answer
       # at all — an ack advances the follower to this position, and an `:offset_mismatch` keeps the
       # generation and salt from the same record while taking the offset the follower reported.
       inflight: %{},
       # When each shipper's CURRENT outstanding push was sent (monotonic ms), set only when an entry
       # is created. Lets `ship_planned/4` skip a follower whose shipper provably still holds this
       # shard's waiter without trusting an `inflight` entry that outlived its push (expert review
       # 2026-10-17 #9). Never cleaned: bounded by the follower count, and read only beside `inflight`.
       inflight_at: %{},
       # Shippers with a seed in flight. A seed can be a whole database, so starting a second one
       # for the same follower because the next commit also saw :unknown_shard would multiply a
       # large transfer by the write rate.
       # %{shipper => %{ref: monitor_ref, pid: pid, started_at: monotonic_ms}}.
       #
       # Was a bare `MapSet` of shippers, which made a lost seed task PERMANENT (expert review
       # 2026-08-20 #26): the only thing that removed an entry was a `{:seeded, ...}` message from
       # the task itself, and the guard in `start_seeds/3` then refused to start another. A task
       # killed under memory pressure, an unforeseen raise inside `do_seed/5`, or a reap therefore
       # left that `{shard, follower}` pair unseedable for the life of the Session, with every
       # subsequent `:unknown_shard` from that follower silently swallowed — silent, permanent
       # under-replication of one shard while the quorum reports healthy.
       seeding: %{},
       # Bytes per ms of the last completed seed, nil until one completes (expert review 2026-10-20
       # #6): lets a missed-quorum commit skip waiting for a seed that cannot land in time.
       seed_rate: nil,
       # Deferred-retry bookkeeping. `catchup_ref` is the armed timer (nil when none); `commits`
       # counts commit cycles (observability and tests). Whether a fired timer runs a cycle is
       # decided from follower positions, not from this count — see `handle_info(:catch_up, ...)`.
       catchup_ref: nil,
       # Whether the armed catch-up is for REAL lag (expert review 2026-10-21 #25) — the thing
       # `LagGauge` and the `:catchup` event count. False for a timer armed only as a safety net
       # behind fresh in-flight pushes, which every N > Q commit leaves at reply. See `arm_if_short/1`.
       catchup_lag: false,
       commits: 0,
       # Consecutive catch-up rounds that were ALSO refused. Drives the backoff in
       # `retry_delay_ms/2`; reset by any commit that comes back with no rejects.
       catchup_fails: 0,
       # CHECKPOINT OWNERSHIP (expert review 2026-10-15 #1) — see "Checkpoints of a replicated WAL"
       # below. `registered` is the registry KEY (canonical `.db` path) under which this Session
       # told the extension's commit hook to leave the file alone — nil until the first commit,
       # `:unavailable` if registration failed; `last_header` the most recent WAL header any
       # read in THIS call returned; `seen` the furthest commit extent this Session has read in
       # each of the last few WAL generations, as `[{wal_gen, salt1, extent}]`, newest first;
       # `ckpt_at` the `{salt1, extent}` of the last checkpoint attempt, for rate limiting.
       registered: nil,
       last_header: nil,
       seen: [],
       ckpt_at: nil,
       # WHO this commit cycle is for (expert review 2026-10-20-2027 #5): `:tenant` (a caller's
       # commit), `:catch_up` (the retry timer) or `:idle_close` (the coordinator's covered-idle-close
       # ask). Set by `run_commit/6` for the cycle's duration; read only as a telemetry tag.
       cycle: :tenant,
       # DEFERRED WAL SYNC (expert review 2026-10-21 #7, `Fathom.Shard.Replication.WalSync`).
       # `wal_sync` is the one fdatasync in flight, `%{token, pid, mref, started}`, or nil. A round
       # that cannot learn its result before its deadline leaves it here, and the NEXT tenant round
       # must resolve it before it may ack anything. `wal_sync_poison` is the reason of the first
       # failed fdatasync: once set, every tenant round on this Session fails (see `WalSync`'s
       # moduledoc for why one failure must fail every later ack).
       wal_sync: nil,
       wal_sync_poison: nil,
       # The long-lived fd every round's fdatasync goes through, `WalSync.held()` (expert review
       # 2026-10-21 rv6 F1): a fresh fd per round misses a write-back error another fd consumed.
       # Opened at the first deferred round on a WAL inode, retired (synced, then closed) on an
       # inode change and by the quiet-shard fd release.
       wal_sync_fd: nil
     }}
  end

  # Deliberately `case`, not `with`/`else`. The first version used `with {:ok, state} <- ...` and
  # then referenced `state` in the else branch — where it is still the OUTER binding, so the epoch
  # `with_epoch/1` had just resolved was silently `nil` again. The seed task then crashed encoding
  # `nil::64` inside a `Task.start`, which swallows it, and the only symptom was followers that
  # were never seeded. Nesting keeps the epoch-bearing state in scope where it is used.
  @impl true
  def handle_call({:commit, wal_path}, from, state),
    do: handle_call({:commit, wal_path, :checkpoint}, from, state)

  # `mode` is `:checkpoint` for a tenant's commit and `:no_checkpoint` for the idle catch-up retry.
  # A checkpoint opens connections on the shard's files, which is only safe while a stream holds a
  # checked-out connection (so the coordinator cannot be dropping or unlinking those files); a
  # tenant's commit always does, the catch-up timer never does.
  def handle_call({:commit, wal_path, mode}, from, state),
    do: handle_call({:commit, wal_path, mode, nil}, from, state)

  # A COMMIT WHOSE CALLER HAS ALREADY GIVEN UP IS NOT EXECUTED (expert review 2026-10-20 #5(a)).
  #
  # `GenServer.call`'s timeout leaves the request in this mailbox. Executing it anyway, with a fresh
  # deadline, is what made the 2026-10-19 same-shard collapse METASTABLE: once one stall (a seed, a
  # far-behind follower) made calls time out, every abandoned call still ran — each able to burn
  # another full deadline — so the queue drained at one caller timeout per entry while new callers
  # kept joining it. Minutes of errors from a transient.
  #
  # Dropping it is safe: its frames were committed before it called, so they are at or below the
  # extent the NEXT live call reads, and that call ships them and gates them on the same quorum and
  # the same ownership check. Nobody is waiting for this reply (a late reply to an expired call is
  # discarded by the caller's alias). A request without a deadline (the catch-up timer's, or an
  # older caller's two- or three-tuple) is never dropped.
  #
  # The internal deadline is also bounded by the CALLER'S: a call that waited 4 s in the queue no
  # longer gets a fresh `commit_deadline_budget/0` that would outlive its caller by 4 s.
  def handle_call({:commit, wal_path, mode, caller_deadline}, from, state),
    do: handle_call({:commit, wal_path, mode, caller_deadline, false}, from, state)

  # The fifth element is the deferred-sync request (expert review 2026-10-21 #7): this caller's
  # handle is `synchronous=NORMAL` and its ack needs the round's fdatasync.
  def handle_call({:commit, wal_path, mode, caller_deadline, sync?}, _from, state) do
    now = now_ms()

    if is_integer(caller_deadline) and now >= caller_deadline do
      :telemetry.execute([:fathom, :replication, :commit_dropped], %{count: 1}, %{})
      {:reply, {:error, :timeout}, state}
    else
      wal_path
      |> run_commit(
        mode,
        commit_deadline(now, caller_deadline),
        state,
        origin(mode),
        sync? == true
      )
      |> with_fd_release()
    end
  end

  # Whether quorum-acked replicas hold everything up to `header` — the question the coordinator's
  # flush asks before it checkpoints (`checkpoint_after_snapshot/2`).
  #
  # The same laggard rule as the threshold checkpoint (expert review 2026-10-20 #8): Q-covered but
  # a connected follower still owed frames ⇒ not yet, below `laggard_wait_bytes/0`, with the
  # laggard kicked; the next flush asks again.
  def handle_call({:covers, header}, _from, state) do
    cond do
      not covered?(state, header) ->
        {:reply, false, state}

      Map.get(header, :commit_extent, 0) < laggard_wait_bytes() and
          laggards(state, header) != [] ->
        :telemetry.execute(
          [:fathom, :replication, :checkpoint_deferred],
          %{count: 1},
          %{source: :flush}
        )

        {:reply, false, arm_catchup(state, [])}

      true ->
        {:reply, true, state}
    end
  end

  def handle_call(:lag_info, _from, state), do: {:reply, build_lag_info(state), state}

  defp now_ms, do: System.monotonic_time(:millisecond)

  # A `:no_checkpoint` request from outside is the catch-up's shape (tests drive it that way); the
  # Session's own internal cycles name their origin through `internal_cycle/2`.
  defp origin(:checkpoint), do: :tenant
  defp origin(_), do: :catch_up

  # A cycle with no caller (the catch-up timer, the covered-idle-close ask): exactly the
  # `handle_call({:commit, wal_path, :no_checkpoint}, ...)` path with no caller deadline (so never
  # dropped), tagged with its origin. `deadline` defaults to that path's; the idle-close ask passes
  # `now` for a single non-blocking round (see `idle_close_verdict/1`).
  defp internal_cycle(state, origin, deadline \\ nil) do
    deadline = deadline || commit_deadline(now_ms(), nil)

    {:reply, _discarded, state} =
      run_commit(state.wal_path, :no_checkpoint, deadline, state, origin, false)

    # The cycle's `Wal.read_held/2` reopens the fds the quiet-shard release (#2) closed, so it must
    # re-arm that release like a tenant commit does (`with_fd_release/1`), or a catch-up on a quiet
    # shard pins the -wal/-shm until the coordinator's idle stop (integration review of expert
    # review 2026-10-20-2027 #2). Armed here, not in each caller, so every internal origin is covered.
    arm_fd_release(state)
  end

  # The handler's own budget, further bounded by the caller's deadline less the reply margin (the
  # same margin `commit_deadline_budget/0` keeps, so the reply still lands before the caller's timer).
  # A caller with less than the margin left gets what it has left rather than nothing: a commit with
  # nothing to wait for (a `:nothing` plan, an already-current quorum) still finishes in time.
  @doc false
  # `number()`: the budget is built from application env values dialyzer cannot prove integral.
  @spec commit_deadline(integer(), integer() | nil) :: number()
  def commit_deadline(now, nil), do: now + commit_deadline_budget()

  def commit_deadline(now, caller_deadline) do
    own = now + commit_deadline_budget()
    capped = caller_deadline - reply_margin()
    if capped > now, do: min(own, capped), else: min(own, caller_deadline)
  end

  defp run_commit(wal_path, mode, deadline, state, origin, sync?) do
    state = ensure_registered(%{state | cycle: origin}, wal_path)
    {pin, state} = if mode == :checkpoint, do: maybe_pin(state, wal_path), else: {nil, state}
    t0 = System.monotonic_time()
    # The catch-up (no caller) holds the Session no longer than its own retry interval: it now
    # drives laggards to current itself (expert review 2026-10-20 #3), and a tenant commit arriving
    # meanwhile queues behind it.
    deadline =
      if mode == :no_checkpoint, do: min(deadline, now_ms() + catchup_ms()), else: deadline

    # GROUP COMMIT (expert review 2026-10-20 #5(b)), before ANY header read: see `coalesce_queued/2`.
    {batch, sync?} =
      if mode == :checkpoint, do: coalesce_queued(wal_path, [], sync?), else: {[], sync?}

    # DEFERRED WAL SYNC (expert review 2026-10-21 #7). Only tenant rounds ack anyone, so only they
    # (and, defensively, any request that asked for a sync whatever its mode) resolve an earlier
    # sync, check the poison and start a sync. The fdatasync starts HERE: after
    # `coalesce_queued/3` gathered every request this round answers (so after every one of their
    # COMMITs returned) and before `do_commit/4`, so that it overlaps the push.
    gated? = mode == :checkpoint or sync?
    {pre, state} = if gated?, do: settle_wal_sync(state, deadline), else: {:ok, state}
    state = maybe_start_wal_sync(state, wal_path, sync? and pre == :ok)

    {:reply, reply, state} = do_commit(wal_path, state, deadline, mode)

    # No caller in the round hears `:ok` unless the fdatasync started for it succeeded too.
    {reply, state} =
      if gated?, do: gate_on_wal_sync(reply, pre, state, deadline), else: {reply, state}

    for from <- batch, do: GenServer.reply(from, reply)

    # Commit -> quorum-ack latency (admin observability 2026-10-15 P1). ONE execute per commit
    # cycle, after the ack work and before the checkpoint start so it measures only what the tenant
    # waits on. The outcome is a bounded 4-value tag; shard_id is deliberately not a tag. `batch` is
    # how many callers this cycle answered.
    #
    # `mode` (expert review 2026-10-20-2027 #5): `:tenant | :catch_up | :idle_close`. The catch-up
    # and idle-close cycles ran through this same execute untagged, so the tenant-visible commit
    # histogram (and the rig's commit counts) mixed in cycles no tenant waited on. Bounded 3 values.
    :telemetry.execute(
      [:fathom, :replication, :commit],
      %{duration: System.monotonic_time() - t0, batch: length(batch) + 1},
      %{outcome: commit_outcome(reply), mode: state.cycle}
    )

    {:reply, reply, start_checkpoint(state, pin, wal_path)}
  end

  # GROUP COMMIT (expert review 2026-10-20 #5(b)). N streams committing to one shard used to be
  # served one call at a time: the first shipped everything committed so far, and every later
  # queued call still paid drain + `Wal.read_held` (stat + header + EOF preads on dirty-IO) + plan +
  # ownership gate, to ship nothing -- ~3 wasted dirty-IO jobs per coalescable commit.
  #
  # So, BEFORE the header read, every commit request for the same WAL already in the mailbox joins
  # this cycle and gets its reply. Correct because each such caller's SQLite commit RETURNED before
  # it sent its request, and its request is in the mailbox before our header read: its frames are at
  # or below the extent this cycle reads, so the same quorum ack and the same ownership gate cover
  # them. A request arriving after this drain is NOT coalesced (its frames may be past the extent)
  # and runs in the next cycle, which is today's behaviour. A request whose caller has already given
  # up is dropped, as in `handle_call/3` (#5(a)). Only tenant commits (`:checkpoint`) coalesce.
  #
  # Reads the call envelopes directly; `GenServer.reply/2` is the documented way to answer them.
  #
  # Returns `{callers, sync?}`. `sync?` is true when the round's own caller or ANY coalesced one runs
  # a deferred-sync handle (expert review 2026-10-21 #7); one fdatasync then covers the whole group.
  defp coalesce_queued(wal_path, acc, sync?) do
    receive do
      {:"$gen_call", from, {:commit, ^wal_path, :checkpoint, dl}} ->
        coalesce_one(wal_path, acc, sync?, from, dl, false)

      {:"$gen_call", from, {:commit, ^wal_path, :checkpoint, dl, s?}} ->
        coalesce_one(wal_path, acc, sync?, from, dl, s? == true)

      {:"$gen_call", from, {:commit, ^wal_path}} ->
        coalesce_queued(wal_path, [from | acc], sync?)
    after
      0 -> {acc, sync?}
    end
  end

  defp coalesce_one(wal_path, acc, sync?, from, dl, s?) do
    if is_integer(dl) and now_ms() >= dl do
      :telemetry.execute([:fathom, :replication, :commit_dropped], %{count: 1}, %{})
      GenServer.reply(from, {:error, :timeout})
      coalesce_queued(wal_path, acc, sync?)
    else
      coalesce_queued(wal_path, [from | acc], sync? or s?)
    end
  end

  # ------------------------------------------------------------------------------------------
  # deferred WAL sync (expert review 2026-10-21 #7); see `Fathom.Shard.Replication.WalSync`
  # ------------------------------------------------------------------------------------------

  # One fdatasync, in its own process so it overlaps the push. `spawn_monitor`, not a link, for the
  # same reason as the seed tasks: nothing the sync does may take down the shard's serialization
  # point. The body cannot raise past its `try`, so the result message always arrives unless the
  # process is killed; a kill surfaces as DOWN, which poisons (an unknown outcome is a failure).
  #
  # The fd is resolved HERE, in the Session, before the spawn (rv6 F1): it must outlive the task.
  # An open failure, or a failed last sync of a retired inode, is delivered through the same
  # task message as a sync result, so `apply_wal_sync/2` decides it exactly like one (an open
  # error fails the round, a datasync error poisons).
  defp maybe_start_wal_sync(%{wal_sync_poison: nil, wal_sync: nil} = state, wal_path, true) do
    parent = self()
    token = make_ref()

    {job, held} =
      case WalSync.hold(wal_path, state.wal_sync_fd) do
        {:ok, held} -> {fn -> WalSync.sync_held(wal_path, held) end, held}
        {:error, reason, held} -> {fn -> {:error, reason} end, held}
      end

    {pid, mref} =
      spawn_monitor(fn ->
        result =
          try do
            job.()
          catch
            kind, reason -> {:error, {:datasync, {kind, reason}}}
          end

        send(parent, {:wal_synced, token, result})
      end)

    %{
      state
      | wal_sync: %{token: token, pid: pid, mref: mref, started: System.monotonic_time()},
        wal_sync_fd: held
    }
  end

  defp maybe_start_wal_sync(state, _wal_path, _start?), do: state

  # Resolve the sync in flight, waiting until `deadline` at most. `{:ok, state}` when there is none
  # or it succeeded; `{{:error, {:wal_sync_failed, _}}, state}` otherwise, including still running
  # (it then stays in `wal_sync`, and the next tenant round or `handle_info/2` resolves it).
  defp settle_wal_sync(%{wal_sync: nil} = state, _deadline), do: {:ok, state}

  defp settle_wal_sync(%{wal_sync: %{token: token, mref: mref}} = state, deadline) do
    receive do
      {:wal_synced, ^token, result} ->
        Process.demonitor(mref, [:flush])
        apply_wal_sync(state, result)

      {:DOWN, ^mref, :process, _pid, reason} ->
        apply_wal_sync(state, {:error, {:datasync, {:sync_task_down, reason}}})
    after
      max(deadline - now_ms(), 0) -> {{:error, {:wal_sync_failed, :pending}}, state}
    end
  end

  defp apply_wal_sync(%{wal_sync: sync} = state, result) do
    state = %{state | wal_sync: nil}

    :telemetry.execute(
      [:fathom, :replication, :wal_sync],
      %{duration: System.monotonic_time() - sync.started},
      %{outcome: wal_sync_outcome(result)}
    )

    case result do
      :ok ->
        {:ok, state}

      # No sync ran, so the kernel dropped nothing. This round fails; the next round's sync covers
      # the same pages.
      {:error, {:open, _} = reason} ->
        {{:error, {:wal_sync_failed, reason}}, state}

      # A sync that FAILED, or a result nothing here expects: POISON (fail-safe; see `WalSync`).
      other ->
        reason = poison_reason(other)
        {{:error, {:wal_sync_failed, reason}}, poison(state, reason)}
    end
  end

  # Refuse every later tenant ack on this Session. The FIRST reason is kept: it names the cause.
  defp poison(%{wal_sync_poison: nil} = state, reason) do
    Logger.error(
      "shard #{state.shard_id}: WAL fdatasync failed (#{inspect(reason)}); refusing every " <>
        "further write ack on this shard until it is reopened"
    )

    %{state | wal_sync_poison: reason}
  end

  defp poison(state, _reason), do: state

  # The quiet-shard fd release also retires the sync fd (rv6 F1; why the last sync makes that safe
  # is in `WalSync.retire/1`). Not while a sync is in flight: it uses the fd, and closing it under
  # the task would turn its result into a spurious poison. The next release retries.
  defp retire_sync_fd(%{wal_sync: nil, wal_sync_fd: held} = state) when held != nil do
    case WalSync.retire(held) do
      :ok -> %{state | wal_sync_fd: nil}
      {:error, reason} -> poison(%{state | wal_sync_fd: nil}, reason)
    end
  end

  defp retire_sync_fd(state), do: state

  defp poison_reason({:error, reason}), do: reason
  defp poison_reason(other), do: {:unexpected, other}

  defp wal_sync_outcome(:ok), do: :ok
  defp wal_sync_outcome({:error, {:open, _}}), do: :open_error
  defp wal_sync_outcome(_), do: :error

  # The tenant round's reply, gated on the deferred sync. A poisoned Session never acks; an earlier
  # sync still unresolved at the start of the round (`pre`) fails it; and the round's own sync must
  # finish `:ok` by the deadline. A round that requested no sync with none in flight passes `reply`
  # through unchanged: that is the flag-off path, at the cost of two map reads.
  defp gate_on_wal_sync(reply, pre, state, deadline) do
    {own, state} = settle_wal_sync(state, deadline)

    # The failure this round itself saw comes first (it names the cause); the poison catches every
    # round after it.
    cond do
      pre != :ok ->
        {pre, state}

      own != :ok ->
        {own, state}

      state.wal_sync_poison != nil ->
        {{:error, {:wal_sync_failed, {:poisoned, state.wal_sync_poison}}}, state}

      true ->
        {reply, state}
    end
  end

  @doc false
  @spec commit_outcome(term()) :: :ok | :no_quorum | :timeout | :other
  def commit_outcome(:ok), do: :ok
  def commit_outcome({:error, {:no_quorum, :timeout}}), do: :timeout
  def commit_outcome({:error, {:no_quorum, _}}), do: :no_quorum
  def commit_outcome({:error, :timeout}), do: :timeout
  def commit_outcome(_), do: :other

  @type lag_info :: %{
          shard_id: String.t(),
          catchup_fails: non_neg_integer(),
          catching_up?: boolean(),
          inflight_pushes: non_neg_integer(),
          seeds_in_flight: non_neg_integer(),
          behind_followers: non_neg_integer(),
          unacked_bytes: non_neg_integer()
        }

  @doc """
  On-demand lag snapshot of one Session, read from state it already holds (no per-commit
  bookkeeping). `unacked_bytes` is the largest gap between the newest WAL extent this Session has
  read and any follower's acknowledged offset in that same generation; a follower in a different
  generation counts as behind but adds no bytes (the gap is not comparable). Returns `nil` when the
  Session is gone or does not answer within `timeout` ms: a Session mid-commit is busy, not lagging.
  """
  @spec lag_info(String.t() | pid(), timeout()) :: lag_info() | nil
  def lag_info(shard_or_pid, timeout \\ 1_000)

  def lag_info(pid, timeout) when is_pid(pid) do
    GenServer.call(pid, :lag_info, timeout)
  catch
    :exit, _ -> nil
  end

  def lag_info(shard_id, timeout) when is_binary(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] -> lag_info(pid, timeout)
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp build_lag_info(state) do
    {behind, bytes} =
      case state.seen do
        [{gen, salt, extent} | _] ->
          Enum.reduce(state.followers, {0, 0}, fn
            {_, %{wal_gen: ^gen, salt1: ^salt, offset: o}}, {n, b} when is_integer(o) ->
              if o < extent, do: {n + 1, max(b, extent - o)}, else: {n, b}

            _, {n, b} ->
              {n + 1, b}
          end)

        [] ->
          {0, 0}
      end

    %{
      shard_id: state.shard_id,
      catchup_fails: state.catchup_fails,
      catching_up?: state.catchup_lag,
      inflight_pushes: map_size(state.inflight),
      seeds_in_flight: map_size(state.seeding),
      behind_followers: behind,
      unacked_bytes: bytes
    }
  end

  # `deadline` is one deadline for the whole call, taken before any work (`commit_deadline/2`): a
  # seed wait that reset the clock could outlive the caller's `GenServer.call` timeout, and a reply
  # nobody is waiting for is worse than a late failure — the caller has already given up and the
  # tenant already has an error, while this process is still holding the shard's serialization point.
  #
  # `commit_deadline_budget/0` is SHORTER than the caller's call timeout by a reply margin (#21),
  # so the handler finishes and replies BEFORE the caller gives up — turning a near-miss quorum
  # into a clean `{:no_quorum, _}` reply rather than a `{:session_down, :timeout}` call exit.
  defp do_commit(wal_path, state, deadline, mode) do
    state = %{state | commits: state.commits + 1}

    case with_epoch(state) do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:ok, state} ->
        # The seeds already in flight when this call began. Only seeds started AFTER this point —
        # by this call — are awaited on a missed quorum (expert review 2026-10-20 #6).
        prior = seed_tokens(state)

        # Drain first: a straggler's reply that landed while the PREVIOUS call was still running has
        # not reached `handle_info/2` yet, and leaving it would let the next `collect/4` mistake it
        # for an answer to the push about to be sent.
        state =
          %{state | wal_path: wal_path}
          |> drain_late_replies(wal_path)
          |> await_stragglers_if_short(wal_path, deadline)

        case ship(state, wal_path, state.epoch, deadline, mode) do
          {:ok, new_state, rejects} ->
            new_state = start_seeds(new_state, wal_path, rejects)

            new_state
            |> drain_late_replies(wal_path)
            # A commit can SUCCEED on the quorum while a follower was refused by OUR OWN shipper.
            # That follower is now behind with nothing scheduled to fix it, and the tenant saw
            # `:ok` — so nothing anywhere looks wrong. This is the arm that matters.
            |> arm_if_rejected(rejects)
            # A follower the quorum did not need may still be short (expert review 2026-10-20 #3:
            # the commit no longer waits for it). The catch-up timer makes it good off the reply path.
            |> arm_if_short()
            # Gate the ack on confirmed ownership (expert review 2026-09-05 #5) — a zombie that
            # reached quorum on stale-epoch followers must not report a write durable. See
            # `ack_if_owned/1`.
            |> ack_if_owned()

          # NOTHING TO SHIP IS NOT PROOF OF OWNERSHIP (expert review 2026-09-29 #7). Streams commit
          # concurrently, and one call reads a WAL header that already covers another stream's
          # frames and ships both. If THAT call's gate refused (a lapsed or zombie owner), the
          # followers still advanced — and the second stream's commit then plans `:nothing` for
          # every follower. It used to reply `:ok` straight away, so the first write was refused
          # and the next was acked quorum-durable by a node that no longer owned the shard. The
          # frames this commit is acking were shipped by someone; the ownership question is the same.
          {:nothing, state} ->
            ack_if_owned(state)

          # Out of time mid-catch-up. Answered here rather than falling into the seed-and-retry
          # clause below, because nothing about this is a seeding problem: the followers are known,
          # seeded and progressing, just not fast enough. Running the seed dance would start no
          # seeds, wait for none, and spend one more round of shipping past a deadline that has
          # already expired.
          {:error, {:no_quorum, :catching_up}, new_state, _rejects} ->
            {:reply, {:error, {:no_quorum, :catching_up}}, new_state}

          # `new_state` already has every rejecter's position corrected from what IT reported, so
          # the next commit plans a catch-up delta from where each actually is.
          #
          # A follower answering :unknown_shard has never been seeded, which is not a fault to
          # alert on but the signal to send it a base copy.
          #
          # This USED TO fail the commit outright and seed out of band, on the reasoning that a
          # seed can be a whole database and blocking a tenant's write on a multi-megabyte transfer
          # is the worst place to put that cost. The reasoning was sound; the consequence was not
          # measured until the chaos rig ran it (2026-08-11): the FIRST write to every shard came
          # back `FILO_NO_QUORUM`, i.e. an `OperationalError` on an unchanged Django app's first
          # INSERT for that tenant, once per tenant, forever. Trading a guaranteed error for a
          # bounded wait is the better side of that deal — and for the small shards fathom's whole
          # thesis is about, the wait is milliseconds.
          #
          # Still bounded, and still fails honestly: `await_seeds/4` waits only for seeds THIS
          # commit started, only until the caller's own deadline, and a shard too large to seed
          # inside it fails exactly as it did before, having spent time it was going to spend
          # anyway. Seeding remains write-gated — nothing seeds a shard nobody writes to, which is
          # the storm the durability `dirty` flag exists to prevent.
          #
          # "THIS commit started" was the comment's promise and not the code's until expert review
          # 2026-10-20 #6: `await_seeds` waited until NO seed was in flight, so a commit that missed
          # its quorum while an earlier commit's large seed streamed (145 s+ on the rig) sat out its
          # whole deadline behind a seed it could not use — and under #5(a)'s queue, every queued
          # commit did the same. `prior` excludes those; their results still land in `handle_info/2`.
          {:error, {:no_quorum, why}, new_state, rejects} ->
            seeded_state = start_seeds(new_state, wal_path, rejects)

            case await_seeds(seeded_state, deadline, wal_path, prior) do
              {:ok, ready_state} ->
                retry_after_seed(ready_state, wal_path, deadline, why, mode)

              {:timeout, waited_state} ->
                {:reply, {:error, {:no_quorum, why}}, waited_state}
            end

          # A read failure keeps what the earlier rounds learned (expert review 2026-10-20 #9):
          # followers' advances, outstanding expectations and the reopened fd. Replying with the
          # PRE-ship state discarded them, so late acks for pushes really in flight were rejected
          # and the next commit re-planned from stale positions (`:offset_mismatch` rounds).
          {:error, reason, failed_state} ->
            {:reply, {:error, reason}, failed_state}
        end
    end
  end

  # A seed that lands while the session is IDLE — the path for any seed the commit that started it
  # did not wait out (a big shard, or one started by a late `:repl_reply` reject). The bookkeeping
  # lives in `apply_seed_result/3` so this and the in-call wait cannot drift:
  #
  #   * record where the seed left THIS follower, or the primary's next delta starts from wherever
  #     it last thought the follower was — nowhere, for a fresh one — and it rejects every frame
  #     while holding perfectly good bytes. Per-follower, because seeds land at different times and
  #     therefore different offsets;
  #   * drop any outstanding expectation for it, because a seed replaces everything the follower
  #     held and a push in flight when it started describes a position that no longer exists.
  #     Letting that reply land afterwards would overwrite the seed with a stale position.
  @impl true
  def handle_info({:seeded, shipper, token, result}, state) do
    {:noreply, apply_seed_result(state, shipper, token, result)}
  end

  # A follower's reply that arrives while the session is IDLE — which is where almost all of them
  # arrive. `ship_quorum/3` returns at the Q-th ack, the call returns, and the stragglers answer
  # into a mailbox nobody is reading; only replies that land *during* a call are seen by
  # `drain_late_replies/2`. Before these clauses existed the catch-all below swallowed them, and the
  # consequences were exactly the ones that made this refactor necessary:
  #
  #   * the follower outside the quorum was never advanced, so the next commit planned a delta it
  #     had already applied, it refused, and it stayed a full delta behind — permanently, if the
  #     tenant stopped writing;
  #   * an `:unknown_shard` from a never-seeded follower was dropped, so whether it was EVER seeded
  #     came down to whether its reply happened to beat the quorum.
  #
  # Both look like replication bugs from the outside and neither is: the answer was received,
  # correct, and thrown away.
  @impl true
  def handle_info({:repl_reply, from, {:ack, _shard, next}}, state) do
    # `forget_inflight/2` for the same reason `drain_late_replies/2` and `settle_inflight/3` do it:
    # `settle_late_ack/3` deliberately declines to advance an ack for a position we did not record
    # (believing it would move the primary past bytes the follower does not hold) — but the follower
    # HAS answered, so the expectation must go either way. Without this the entry survived a
    # mismatched late ack and `inflight` claimed that follower was still mid-flight forever.
    #
    # This clause was the one place the rule was not applied, which is exactly the shape of gap a
    # rule spread across three call sites produces. Found as a full-suite flake in
    # `replication_commit_test.exs`'s "a follower that rejects is no longer recorded as owing a
    # reply", not by reading.
    {:noreply, state |> settle_late_ack(from, next) |> forget_inflight(from)}
  end

  def handle_info({:repl_reply, from, {:reject, _shard, reason, follower_offset}}, state) do
    rejects = [{from, reason, follower_offset}]

    {:noreply,
     state
     |> reconcile(rejects)
     |> seed_if_possible(rejects)
     |> arm_if_rejected(rejects)}
  end

  # DEFERRED RETRY for a push OUR OWN shipper refused — `:already_in_flight` (that shard's single
  # waiter is still held by an earlier push) or `:overloaded` (the node byte budget). Neither reached
  # the follower, and nothing else re-sends them, so without this the follower stays behind until the
  # shard's NEXT WRITE. On a busy shard that is one round; on a QUIET one it is unbounded — and quiet
  # is exactly when it bites, because `deliver/5` takes the fire-and-forget `ship_async/1` branch
  # precisely when the other followers are already current, i.e. when no further write is coming.
  #
  # IT RE-ENTERS `handle_call/3` RATHER THAN SHIPPING ITSELF, and that is the whole design.
  #
  # The first attempt (2026-08-19) was a parallel `catch_up/1` that planned and shipped on its own.
  # It cost **-15% throughput and 35x the errors** at 512 tenants, and after being guarded it still
  # flaked — because it was a SECOND WRITER of `inflight`, and in particular it skipped the
  # `drain_late_replies/2` that `handle_call/3` runs FIRST for exactly this reason: a straggler's
  # reply that has not yet reached `handle_info/2` would otherwise be mistaken for an answer to the
  # push about to be sent. Reusing the commit path inherits that drain, `settle_inflight/3` between
  # rounds, the seeding and the reconciliation — every ordering rule the path already encodes —
  # instead of re-deriving them alongside it.
  #
  # A fake `from` is safe: the clause ignores it (`_from`). Blocking here is fine because this only
  # runs on a shard with no traffic, and anything that does arrive queues behind it — which IS the
  # serialization this is staying inside rather than routing around.
  # The coordinator's covered-idle-close ask (expert review 2026-10-20 #7): may its idle drain run
  # the close-time checkpoint now? See `request_idle_close/2`. Always answered, `:ready`, `:wait` or
  # `:retry`; the coordinator re-asks on `:wait` and gives up (drains anyway) after a bounded number
  # of asks.
  def handle_info({:idle_close?, coordinator, token}, state) do
    {verdict, state} = idle_close_verdict(state)
    send(coordinator, {:idle_close_verdict, token, verdict})
    {:noreply, arm_fd_release(state)}
  end

  # RELEASE THE HELD WAL / -shm FDS ON A QUIET SHARD (expert review 2026-10-20-2027 #2).
  #
  # `wal_fd` pins the `-wal` inode and its `-shm` wal-index. When the shard's last SQLite connection
  # closes (the pool drain grace, or a stream's own close with pooling off), SQLite checkpoints and
  # UNLINKS both files, but this Session lives as long as the coordinator (60 s idle) and kept both
  # fds open on the deleted inodes until then: 2 fds per shard touched in the last minute, invisible
  # to `du`, holding up to the WAL's size of disk each. 1k distinct shards/s is ~120k fds — past a
  # 65,536 nofile, i.e. `emfile` for every open on the node.
  #
  # So: if no commit (tenant or catch-up) has run for a whole `fd_release_ms/0`, close them. A timer
  # rather than a coordinator notification because the last close happens on several paths
  # (the pool's off-process close chain, a non-pooled stream's own close, terminate), and none of
  # them needs to know this process exists; a quiet shard's fds go at most ~2x the interval after
  # its last commit. A busy shard pays one timer per interval and no syscall.
  #
  # DURABILITY: closing the handle is the Session's starting state (`wal_fd: nil`). The next
  # `Wal.read_held/2` stats, reopens and re-pins the inode, and with no cursor derives the extent
  # from the wal-index or the backward walk — exactly as a fresh Session's first commit does. No
  # position, `seen` or follower record is touched, so nothing about what was shipped or acked
  # changes; only a stale cursor (a walk-length hint) is dropped.
  def handle_info({:release_wal_fds, at}, %{commits: at} = state) do
    state = retire_sync_fd(state)
    {:noreply, %{state | wal_fd: Wal.close_held(state.wal_fd), fd_release_ref: nil}}
  end

  # A commit ran since the timer was armed: the shard is busy, look again one interval later.
  def handle_info({:release_wal_fds, _at}, state),
    do: {:noreply, arm_fd_release(%{state | fd_release_ref: nil})}

  def handle_info(:catch_up, %{wal_path: nil} = state),
    do: {:noreply, clear_catchup(state)}

  # WHETHER A FIRED TIMER RUNS A CYCLE IS DECIDED IN MEMORY (expert review 2026-10-20-2027 #6/#17).
  #
  # It used to run whenever no commit had landed since the timer was (re)armed, `commits ==
  # catchup_at`, on the theory that a commit re-plans for every follower. Since the commit stops at
  # its quorum (2026-10-20 #3) every commit leaves its non-quorum straggler's ack unread at reply,
  # so `arm_if_short/1` armed or REFRESHED the timer on nearly every commit, the refresh kept
  # `catchup_at == commits`, and the timer fired on BUSY shards about once per `catchup_ms`: a full
  # cycle (dirty-IO header read, plan, ownership gate) that planned nothing, because by then the
  # straggler's ack had landed. Measured at prod settings (catchup 1000 ms, 3 followers Q=2,
  # loopback): +15% Session cycles at 3 commits/s — the rig's per-tenant rate — +4% at 20/s.
  #
  # Now the timer reads the follower positions it already holds against the newest extent this
  # Session has read (`catch_up_need/1`):
  #
  #   * `:none` — every connected, non-seeding follower has ACKED that extent: a cycle would plan
  #     `:nothing` for all of them. Clear the timer, read nothing.
  #   * `:pending` — the only followers not current have a push in flight (fresh, within
  #     `timeout()`): their replies are already on the way and land in `handle_info/2`; a cycle now
  #     would skip them `:already_in_flight` and, as a catch-up, sit in `settle_inflight/3` waiting
  #     for those same replies with tenant commits queued behind it. Re-arm, read nothing. A reply that
  #     never comes ages past `timeout()` and the follower then counts as short.
  #   * `:short` — some follower is behind and free to push to: run the cycle, as before.
  #
  # Durability: the catch-up never gates an ack (a tenant's ack rests on `quorum_holds?/2` inside its
  # own cycle), so this only changes WHEN a non-quorum follower is brought current, never what a
  # tenant was told. The one thing a header read could see that `seen` cannot is frames committed
  # after this Session's last read whose commit call has not run (queued, or dropped as expired,
  # #5(a)); those are the next live call's to ship, exactly as before — the timer was never armed for
  # them. The old `catchup_at` refresh existed only to stop the count-based rule from dropping a
  # timer that was still needed; with no count rule there is nothing to strand.
  #
  # LAG ACCOUNTING (expert review 2026-10-21 #25). Arms are usually QUIET now (`arm_if_short/1`), so
  # the fired timer is where lag is judged. A `:pending` fire whose in-flight push has outlived a
  # catch-up interval is real lag: mark it (`escalate/1`). A `:short` fire — a follower still behind
  # with nothing in flight an interval after the arm — is marked for the cycle; that cycle's own
  # re-arm (loud while `catchup_lag`) keeps the mark if the follower is still behind, and the mark is
  # cleared here when the cycle left nothing armed, so it never outlives its timer.
  def handle_info(:catch_up, state) do
    case catch_up_need(state) do
      :none ->
        {:noreply, clear_catchup(state)}

      :pending ->
        state = if lagging?(state), do: escalate(state), else: state
        {:noreply, rearm_catchup(state)}

      :short ->
        LagGauge.mark(self(), true)
        state = internal_cycle(%{state | catchup_ref: nil, catchup_lag: true}, :catch_up)
        {:noreply, if(state.catchup_ref == nil, do: clear_catchup(state), else: state)}
    end
  end

  def handle_info({:DOWN, _ref, :process, coordinator, _reason}, %{coordinator: coordinator} = s) do
    # The shard moved or drained. Our offset describes a WAL this node no longer owns.
    {:stop, :normal, s}
  end

  # A SEED TASK DIED WITHOUT ANSWERING (expert review 2026-08-20 #26).
  #
  # `{:seeded, ...}` used to be the only thing that cleared an entry, so a task killed under memory
  # pressure, an unforeseen raise inside `do_seed/5`, or a reap left that `{shard, follower}` pair
  # unseedable for the life of the Session — and `start_seeds/3` then swallowed every subsequent
  # `:unknown_shard` from that follower. Silent, permanent under-replication of one shard WHILE THE
  # QUORUM REPORTS HEALTHY, which is the same failure class the late-reply drain was built to
  # eliminate, reached by a different route.
  #
  # `:normal` is the ordinary end of a task that already sent its result and whose entry is
  # therefore gone, so this clause only ever finds something on the failure path.
  # A deferred WAL sync whose round already replied (it was still running at that round's deadline).
  # Its outcome must still land, because a failure poisons the Session (expert review 2026-10-21 #7).
  # Both clauses sit above the seed DOWN clause, which would otherwise swallow the sync task's DOWN.
  def handle_info({:wal_synced, token, result}, %{wal_sync: %{token: token, mref: mref}} = state) do
    Process.demonitor(mref, [:flush])
    {_verdict, state} = apply_wal_sync(state, result)
    {:noreply, state}
  end

  def handle_info({:DOWN, mref, :process, _pid, reason}, %{wal_sync: %{mref: mref}} = state) do
    {_verdict, state} = apply_wal_sync(state, {:error, {:datasync, {:sync_task_down, reason}}})
    {:noreply, state}
  end

  # A checkpoint of this WAL failed with SQLITE_IOERR (expert review 2026-10-21 rv6 F1): this
  # Session's threshold checkpoint, or the coordinator's flush checkpoint. A checkpoint fsyncs the
  # WAL before backfilling, so the error may be the write-back failure of a deferred commit's
  # frames, now consumed by the checkpoint's fd and invisible to the next round's fdatasync. Poison.
  #
  # Only with the deferred sync configured. Under FULL every acked frame was fsynced by its own
  # COMMIT, which reported its own error, so a later checkpoint error cannot un-ack anything, and
  # poisoning there would turn a transient checkpoint error into a shard-wide write outage.
  def handle_info({:checkpoint_io_error, reason}, state) do
    if WalSync.configured?(),
      do: {:noreply, poison(state, {:checkpoint, reason})},
      else: {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state),
    do: {:noreply, apply_seed_down(state, ref, reason)}

  def handle_info(_, state), do: {:noreply, state}

  # A STOPPED SESSION MUST NOT LEAVE A SEED STREAMING (expert review 2026-08-20 #26).
  #
  # The Session stops when its coordinator goes DOWN — the shard moved or drained — and an
  # unsupervised task started before that would go on streaming an entire tenant database to a
  # follower for a shard this node no longer owns, stamped with an epoch it no longer holds.
  # `spawn_monitor` deliberately does not link (a raise inside `do_seed/5` must not take down the
  # shard's serialization point), so the cancellation is explicit here instead.
  @impl true
  def terminate(_reason, %{seeding: seeding} = state) do
    LagGauge.mark(self(), false)
    for {_shipper, %{pid: pid}} <- seeding, do: Process.exit(pid, :kill)

    # Release the held WAL read fd (expert review 2026-08-26 #32). The VM would reclaim it when the
    # process dies, but closing explicitly keeps the fd's lifetime tied to the session's rather
    # than to GC — at a million shards an fd per open session is not a rounding error.
    _ = Wal.close_held(Map.get(state, :wal_fd))
    close_sync_fd(state)
    unregister(state)
  end

  def terminate(_reason, state) do
    LagGauge.mark(self(), false)
    _ = Wal.close_held(Map.get(state, :wal_fd))
    close_sync_fd(state)
    unregister(state)
  end

  # No last sync here: a Session stops with its coordinator, and a poison does not outlive that.
  defp close_sync_fd(%{wal_sync_fd: %{fd: fd}}), do: :file.close(fd)
  defp close_sync_fd(_state), do: :ok

  # The quiet-shard fd release (#2, `handle_info({:release_wal_fds, _}, _)`). Armed whenever a held
  # fd exists and no timer is; carries the commit count at arm time so the fire can tell quiet from
  # busy without a syscall.
  defp with_fd_release({:reply, reply, state}), do: {:reply, reply, arm_fd_release(state)}

  defp arm_fd_release(%{wal_fd: fd, fd_release_ref: nil} = state) when fd != nil do
    ref = Process.send_after(self(), {:release_wal_fds, state.commits}, fd_release_ms())
    %{state | fd_release_ref: ref}
  end

  defp arm_fd_release(state), do: state

  @doc false
  # How long a Session keeps its WAL/-shm fds with no commit (#2). Long enough that a steadily
  # writing shard never reopens; short against the coordinator's 60 s idle stop.
  def fd_release_ms, do: Application.get_env(:fathom, :replication_fd_release_ms, 1_000)

  # A reply cannot exist before the commit that provoked it, so `wal_path` is always set by the time
  # this runs. Matching on it anyway keeps a reply that somehow arrived first from crashing the
  # session on a nil path — losing a straggler's seed is recoverable, losing the session is not.
  defp seed_if_possible(%{wal_path: nil} = state, _rejects), do: state
  defp seed_if_possible(state, rejects), do: start_seeds(state, state.wal_path, rejects)

  # Read the lease epoch once. `{:error, :no_lease}` must NOT be shipped past — frames from a node
  # without a lease are exactly what the follower's epoch check refuses.
  defp with_epoch(%{epoch: e} = state) when is_integer(e), do: {:ok, state}

  defp with_epoch(state) do
    case Fathom.Shard.epoch(state.coordinator) do
      # The LINEAGE is read alongside the lock epoch and cached the same way (expert review
      # 2026-08-24 #12). They are different counters: the lock epoch resets to 1 on every clean
      # release, so it cannot order a replica against the stored object, whose position stamp
      # carries the monotonic lineage. A seed ships both. Read here rather than at seed time so it
      # costs one call per session, on the path that already pays one.
      {:ok, epoch} ->
        # All three reads are cached for the session's life, so none may cache a guess. A
        # coordinator that does not answer in time (busy in `await_closing`, or in an inline S3
        # HEAD) yields NOTHING cached: lineage 0 would silently disarm the follower lineage fence and
        # promote ranking, and a nil `acquire_gen` would silently drop the ack gate to legacy
        # (expert review 2026-10-12 #11). The commit is refused retryably; the next one re-reads.
        with {:ok, lineage} <- lineage_of(state),
             {:ok, acquire_gen} <- acquire_gen_of(state) do
          {:ok, %{state | epoch: epoch, lineage: lineage, acquire_gen: acquire_gen}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  catch
    # A coordinator busy past the call timeout used to exit HERE, crashing the Session (a full
    # rebuild and reseed of the shard's followers) for what is a transient (expert review
    # 2026-10-12 #11). `{:error, _}` is the retryable-503 shape the commit path already returns.
    :exit, _ -> {:error, :coordinator_busy}
  end

  # An exit is "unknown", NOT lineage 0 ("no claim"): the caller refuses the commit and caches
  # nothing, so the next commit re-reads (expert review 2026-10-12 #11).
  defp lineage_of(state) do
    {:ok, Fathom.Shard.lineage(state.coordinator)}
  catch
    :exit, _ -> {:error, :lineage_unknown}
  end

  # The coordinator's fence generation, read once and cached beside `epoch` (expert review
  # 2026-09-05 #5). `nil` in legacy mode (no heartbeat), which leaves the ack ungated. An exit is NOT
  # legacy: caching nil for a busy coordinator would drop the ack gate for the session's life, so it
  # is surfaced and the commit refused (expert review 2026-10-12 #11).
  defp acquire_gen_of(state) do
    {:ok, Fathom.Shard.acquire_gen(state.coordinator)}
  catch
    :exit, _ -> {:error, :coordinator_busy}
  end

  # Gate a quorum-durable commit ack on confirmed ownership (expert review 2026-09-05 #5).
  #
  # HEARTBEAT MODE ONLY: `Heartbeat.valid_for_write?/1` is a lock-free ETS read, so a zombie (VM/GC
  # pause past the TTL) or S3-partitioned owner that reached quorum on stale-epoch followers is
  # caught BEFORE it tells the tenant "committed" — an ack the S3 etag fence would then discard,
  # losing a write reported quorum-durable. `:not_valid` refuses with a RETRYABLE
  # `{:error, :ownership_unconfirmed}`, which `ShardExecutor` maps to a 503 the client retries
  # against the successor. `:revalidate` (the fence generation moved after a heartbeat lapse) is NOT
  # a permanent refusal: the session re-reads the coordinator's revalidated generation once and
  # re-checks (expert review 2026-09-18 #4), so a transient lapse no longer freezes the session into
  # refusing every write for its lifetime. The safe direction only: it can refuse a commit this node
  # should not ack, never wrongly ack one, and the ack path never runs an inline S3 GET (the
  # coordinator's `Fence.revalidate/2` did the read-only ownership check).
  #
  # LEGACY MODE (`acquire_gen == nil`) is BEST-EFFORT, pinned by `replication_commit_test.exs`'s
  # "legacy mode ... leaves the ack ungated" (expert review 2026-09-18 #5) — see the legacy clause
  # at the bottom of the function for the full contract and why it is a deliberate deferral.
  defp ack_if_owned(state) do
    case state.acquire_gen do
      gen when is_integer(gen) ->
        case heartbeat_ok(gen) do
          :ok ->
            {:reply, :ok, state}

          # A heartbeat lapse bumped the fence generation since this session cached `acquire_gen`
          # in `with_epoch/1` (expert review 2026-09-18 #4). The COORDINATOR recovers — a lapse
          # arms `:revalidate_lapse`, and the next flush fence runs `Fence.revalidate/2`, which
          # confirms ownership with a read-only `check_lease` and advances `acquire_gen` to the new
          # generation. The SESSION, however, froze its copy at open and has no refresh path, so
          # `valid_for_write?/1` would read `:revalidate` for the rest of the session's life and
          # refuse EVERY write on a node that fully retains ownership — a transient blip turned into
          # a durable per-tenant write outage. Re-read the coordinator's (revalidated,
          # ownership-confirmed) generation ONCE and re-check. The write that provoked this ack
          # keeps the shard dirty, so the coordinator's flush fence revalidates promptly; until it
          # does, the re-read returns the same stale generation and we refuse with a RETRYABLE 503
          # — never a false ack — and the client's retry succeeds once the coordinator has advanced.
          :revalidate ->
            case fetch_acquire_gen(state) do
              # A COORDINATOR THAT CANNOT ANSWER IS NOT "LEGACY MODE" (expert review 2026-09-29 #8).
              # The re-read used to catch an exit and return nil, and nil read as "the heartbeat
              # degraded, ack best-effort" — but in heartbeat mode a live coordinator never reports
              # nil (`Fence.revalidate/2` preserves the generation). An exit here means the
              # coordinator is gone or wedged: the zombie case, where it has just self-fenced on
              # `:superseded` and quarantined the very write this commit is about to ack. Refuse with
              # the same retryable 503; the client's retry reaches the real owner.
              :unreachable ->
                {:reply, {:error, :ownership_unconfirmed}, state}

              {:ok, g} when is_integer(g) ->
                refreshed = %{state | acquire_gen: g}

                case heartbeat_ok(g) do
                  :ok -> {:reply, :ok, refreshed}
                  _ -> {:reply, {:error, :ownership_unconfirmed}, refreshed}
                end

              # The coordinator ANSWERED nil: its heartbeat process went down since acquire and it
              # degraded to legacy mode. Ack per legacy best-effort semantics (legacy clause below).
              {:ok, nil} ->
                {:reply, :ok, %{state | acquire_gen: nil}}
            end

          _ ->
            {:reply, {:error, :ownership_unconfirmed}, state}
        end

      # LEGACY MODE (`acquire_gen == nil`, heartbeat server not running) is BEST-EFFORT: the ack is
      # not gated on a live ownership check (expert review 2026-09-18 #5). `valid_for_write?/1` is a
      # heartbeat-mode mechanism (a nil gen reads `:revalidate` and would gate every legacy commit),
      # and legacy mode has no cheap in-memory ownership signal — the only live check is a read-only
      # `check_lease` S3 GET, which on this per-commit path would add an S3 round-trip to every
      # commit's critical path in the degraded window. The exposure is real but bounded: a
      # legacy-mode zombie that reached quorum on stale-epoch followers can ack a write the fleet
      # will DISCARD on fence resolution, but the FOLLOWER epoch/lineage fence is the backstop that
      # actually keeps that write from being served. Prod runs heartbeat, so the deployed fleet
      # never reaches this clause. Gating it on an inline `check_lease` (S3 latency per legacy commit
      # vs. coverage) is a deliberate deferral, not an oversight; the "legacy mode ... leaves the ack
      # ungated" test in `replication_commit_test.exs` pins this best-effort contract explicitly so
      # it cannot change silently.
      _ ->
        {:reply, :ok, state}
    end
  end

  # Re-read the coordinator's current (revalidated) fence generation (expert review 2026-09-18 #4),
  # keeping "the coordinator answered nil" (legacy) distinct from "the coordinator did not answer"
  # (expert review 2026-09-29 #8). Never crashes the commit.
  defp fetch_acquire_gen(state) do
    {:ok, Fathom.Shard.acquire_gen(state.coordinator)}
  catch
    :exit, _ -> :unreachable
  end

  # A dead heartbeat process degrades only while its LAST GOOD deadline has not passed. Past it the
  # lease may be stealable and nothing is renewing it (a crash loop renews nothing), so the ack is
  # refused (expert review 2026-10-12 #23). The coordinator's flush fence degrades to the legacy
  # renew PUT, but that proves ownership only on flush ticks, not per commit. No recorded deadline
  # (never published) keeps the old `:ok`.
  #
  # Same verdict as the live check (expert review 2026-10-13 #18): refuse at `now + margin >=
  # deadline` (the live check's inequality, margin from `Heartbeat.margin_ms/0`, which falls back to
  # the configured value when the process is down), and refuse after a CLEAN stop (`:stopped`), which
  # has already cleared the heartbeat object so the node's keyspace is stealable.
  defp heartbeat_ok(gen) do
    Fathom.Shard.Heartbeat.valid_for_write?(gen)
  catch
    :exit, _ ->
      case Fathom.Shard.Heartbeat.last_deadline_ms() do
        :stopped ->
          :not_valid

        deadline when is_integer(deadline) ->
          if System.monotonic_time(:millisecond) + Fathom.Shard.Heartbeat.margin_ms() >= deadline,
            do: :not_valid,
            else: :ok

        nil ->
          :ok
      end
  end

  # The monotone ordinal for the WAL identified by `salt`, cached against that salt.
  #
  # STABILITY is the property that matters here, not just monotonicity. The coordinator answers the
  # same number for the same salt however often it is asked, so the ordinal this session pushes to
  # replicas and the one the coordinator stamps on the object are THE SAME NUMBER. Two independent
  # counters would put the object and its replicas on different scales, which is the bug rather
  # than the fix — see `Fathom.Shard.wal_ordinal/2`.
  defp with_ordinal(%{wal_salt: salt, wal_ordinal: n} = state, salt) when is_integer(n),
    do: {n, state}

  defp with_ordinal(state, salt) do
    n = ordinal_of(state, salt)
    {n, %{state | wal_salt: salt, wal_ordinal: n}}
  end

  # Never fails the session, exactly like `lineage_of/1`: a coordinator that cannot state an ordinal
  # yields 0 ("not stated"), which `Promote.fresher?/2` refuses to rank. Losing a promotion is the
  # correct degradation; refusing to replicate is not.
  defp ordinal_of(state, salt) do
    Fathom.Shard.wal_ordinal(state.coordinator, salt)
  catch
    :exit, _ -> 0
  end

  # SHIP UNTIL CAUGHT UP, IN BOUNDED ROUNDS.
  #
  # `Primary.plan/3` caps how much WAL one push may carry, which is what breaks the feedback loop
  # behind the 1024-tenant OOM (`docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`). The cost of
  # that cap is that one round no longer necessarily hands a follower everything, so a commit that
  # returned after one round could report success while the quorum was still short of the bytes it
  # just committed — the exact lie `Session`'s moduledoc refuses to tell.
  #
  # So the round repeats until A WRITE QUORUM HOLDS THIS COMMIT'S BYTES, or the caller's own deadline
  # passes. This is the SAME trade the seeding path made deliberately in `handle_call/3` below (see
  # the long comment there): a bounded wait beats a guaranteed error, and the failure at the end of
  # it is exactly as honest as the one round produced before.
  #
  # THE STOPPING RULE IS THE QUORUM CONTRACT, NOT "EVERY FOLLOWER IS CURRENT" (expert review
  # 2026-10-20 #3). The loop used to end only when EVERY planned follower — seeding and skipped
  # in-flight ones included, none of which were sent anything — reached the extent of a header
  # RE-READ each round, and `settle_inflight/3` waited for every outstanding follower between
  # rounds. So one follower more than `max_push_bytes` behind (a seed in flight, a non-quorum laggard
  # under 16-32 writers) made EVERY commit on the shard loop to its deadline and fail
  # `{:no_quorum, :catching_up}`, although Q followers had acked every byte of it in round 1: the
  # root of the 2026-10-19 same-shard collapse (1,700 -> 64-170 ops/s). Now a commit ends when
  # `quorum_holds?/2`: Q followers (ACKED positions, not plans; seeding ones excluded) hold the
  # extent of the FIRST round's header — this commit's extent. Exactly the single-round path's
  # contract and the moduledoc's. A generation change in a later round retargets to the new header
  # (the old frames were checkpointed; the reset carries what is left), as before.
  #
  # Followers left short at reply time are not abandoned: `arm_if_short/1` arms the catch-up timer,
  # and the catch-up (`mode == :no_checkpoint`, no caller waiting) keeps the old stronger rule —
  # it runs until every follower it could push to is current — under a deadline no longer than its
  # own retry interval, so a quiet shard's laggards still converge without a tenant waiting on them.
  #
  # Between rounds a tenant commit waits only until enough followers are free to make the quorum
  # (`await_stragglers_if_short/3`), never for a straggler the quorum does not need.
  #
  # The deadline is the caller's, threaded in rather than computed here. A second clock started at
  # this depth could outlive the `GenServer.call` that is waiting on it, and a reply nobody is
  # listening for is worse than a late failure — the tenant already has its error while this process
  # still holds the shard's serialization point.
  #
  # Rejects are ACCUMULATED across rounds and merged per shipper, latest wins. Dropping an
  # intermediate round's rejects would usually be survivable (an `:unknown_shard` follower plans the
  # same thing next round and refuses again, so it resurfaces) but only by accident, and a seed
  # deferred to the next commit is precisely the silent under-replication the late-reply drain
  # exists to prevent.
  # `target == nil` (the first round) separates "this commit wrote no frames" (nothing to plan on
  # the FIRST round) from "the catch-up converged" (nothing left on a LATER round). They used to be
  # told apart by whether any reject had been seen, so a multi-round commit with no rejects returned
  # `:nothing` from its last round and `handle_call/3` replied from the PRE-ship state, discarding
  # every follower's advance (2026-10-02, the rig's residual `offset_mismatch` storm). A later round
  # always returns its state.
  defp ship(state, wal_path, epoch, deadline, mode),
    do: ship(state, wal_path, epoch, deadline, mode, %{}, nil)

  defp ship(state, wal_path, epoch, deadline, mode, seen, target) do
    case Wal.read_held(state.wal_fd, wal_path) do
      {:ok, header, fd} ->
        state = %{state | wal_fd: fd} |> note_header(header)

        case ship_planned(state, wal_path, epoch, header, deadline) do
          # Nothing left to plan. On the first round that means the commit wrote no frames; on a
          # later one it means every follower is current.
          #
          # It carries THIS state, not just the atom (round-4 rig rpo regression 2026-10-20). The
          # header read above may have CLOSED the held fd — `Wal.read_held/2` releases it and hands
          # back `nil` when the WAL is absent or shorter than a header (a flush's TRUNCATE, or the
          # last close unlinking `-wal`). Replying from the pre-ship state kept the closed fd, and
          # the next commit reused it once the WAL had the same inode again (the truncated file
          # regrowing; on Linux, a recreated `-wal` reusing the freed inode number): `pread` on a
          # closed fd is `{:error, :einval}`, so the first write after a flush failed its quorum.
          :nothing when target == nil ->
            {:nothing, state}

          :nothing ->
            {:ok, state, seen_rejects(seen)}

          {:ok, new_state, rejects, sent_complete?} ->
            seen = merge_rejects(seen, rejects)
            target = retarget(target, header)
            held? = quorum_holds?(new_state, target)

            cond do
              held? and (mode != :no_checkpoint or sent_complete?) ->
                {:ok, new_state, seen_rejects(seen)}

              # Out of time. A catch-up whose quorum holds has nothing to report; a commit whose
              # quorum does not hold its bytes fails rather than acks.
              past?(deadline) and held? ->
                {:ok, new_state, seen_rejects(seen)}

              past?(deadline) ->
                {:error, {:no_quorum, :catching_up}, new_state, seen_rejects(seen)}

              true ->
                # Re-read the header next round rather than reusing it. Other streams commit to
                # this shard concurrently, so the WAL may have grown again; planning from a stale
                # header would ship a delta that is already short by the time it lands.
                new_state
                |> between_rounds(wal_path, deadline, mode)
                |> ship(wal_path, epoch, deadline, mode, seen, target)
            end

          {:error, {:no_quorum, why}, new_state, rejects} ->
            {:error, {:no_quorum, why}, new_state, seen_rejects(merge_rejects(seen, rejects))}

          # THE WAL RESTARTED UNDER THIS ROUND (expert review 2026-10-20 #9): between the header read
          # and the payload/recheck reads, a checkpoint plus a write began a new generation. The
          # stability check correctly refused to ship bytes stamped with the old identity — and
          # then FAILED THE TENANT'S COMMIT, a write that is durable locally and would have shipped
          # had the round simply re-read (2-13 per pool-fleet run). The comments here always said
          # "the next round re-plans"; now it does: re-read the header and plan again, inside the
          # deadline, carrying this round's state. A new generation retargets (`retarget/2`), and
          # the reset it plans carries `prev_extent`, so a follower missing frames the restart
          # folded away is re-seeded rather than laundered complete. Bounded by the deadline AND a
          # few attempts, so a persistent failure cannot spin on dirty-IO reads.
          {:replan, reason, new_state} ->
            if past?(deadline) or replans(seen) >= @max_replans do
              read_failed(new_state, reason)
            else
              ship(new_state, wal_path, epoch, deadline, mode, bump_replans(seen), target)
            end

          {:error, _reason, _state} = failed ->
            failed
        end

      {:error, reason, fd} ->
        read_failed(%{state | wal_fd: fd}, reason)
    end
  end

  # Re-plans after a restart race this call has made, kept beside the per-shipper rejects in `seen`
  # under a key no shipper can be (shippers are pids or names; this is a tagged tuple).
  defp replans(seen), do: Map.get(seen, {__MODULE__, :replans}, 0)
  defp bump_replans(seen), do: Map.update(seen, {__MODULE__, :replans}, 1, &(&1 + 1))

  # The first round's header is the commit's target; a later round in a DIFFERENT generation (the
  # log restarted between rounds) retargets to the new header.
  defp retarget(nil, header), do: header
  defp retarget(%{ckpt_seq: g, salt1: s} = target, %{ckpt_seq: g, salt1: s}), do: target
  defp retarget(_target, header), do: header

  # LET THE ROUND SETTLE BEFORE STARTING THE NEXT ONE — but only as far as the next round needs.
  #
  # Two failures made the original loop wait for every outstanding push between rounds, both found
  # the first time it ran against three real followers: a straggler's late answer read by the next
  # round's `collect` as a reply to ITSELF (an `:offset_mismatch` that rewound the follower), and a
  # push to a follower whose shipper still held this shard's single waiter (refused
  # `:already_in_flight` every round). Since expert review 2026-10-17 #9 a follower with a push in
  # flight is SKIPPED rather than pushed to, so neither can happen to it any more — and waiting for
  # it is exactly what made one held follower cost every commit its deadline (2026-10-20 #3).
  #
  # So a tenant commit waits only until enough followers are free to make the quorum. The catch-up,
  # which has no caller and whose purpose IS the laggards, still settles every outstanding push.
  defp between_rounds(state, wal_path, deadline, :no_checkpoint),
    do: settle_inflight(state, wal_path, deadline)

  defp between_rounds(state, wal_path, deadline, _mode),
    do: await_stragglers_if_short(state, wal_path, deadline)

  # Does a write quorum of CURRENT followers hold everything up to `target`, by acked position?
  # Stricter than `covered?/2` (the checkpoint's question) in two ways that matter for an ACK: only
  # followers in the current membership count, and a follower being seeded does not — its recorded
  # position predates the seed it asked for, so it may no longer hold those bytes.
  defp quorum_holds?(state, %{ckpt_seq: gen, salt1: salt, commit_extent: extent}) do
    held =
      Enum.count(shippers(), fn s ->
        not Map.has_key?(state.seeding, s) and
          match?(
            %{wal_gen: ^gen, salt1: ^salt, offset: o} when is_integer(o) and o >= extent,
            Map.get(state.followers, s)
          )
      end)

    held >= quorum()
  end

  # The per-shipper rejects, without `bump_replans/1`'s counter.
  defp seen_rejects(seen), do: seen |> Map.delete({__MODULE__, :replans}) |> Map.values()

  defp merge_rejects(seen, rejects) do
    Enum.reduce(rejects, seen, fn {shipper, _reason, _at} = r, acc ->
      Map.put(acc, shipper, r)
    end)
  end

  defp past?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  # Plan each follower independently, then group by plan so identical ones share a single `pread`
  # and a single encode. In steady state every follower is at the same offset and there is exactly
  # one group; a laggard simply forms a second, larger one.
  #
  # A follower planned `:nothing` is not merely skipped, it is COUNTED. It already holds every byte
  # this commit would replicate, so it satisfies the quorum with no round trip — and dropping it
  # from the tally was a bug in two directions at once. The visible half: `n` shrank to the size of
  # the push list while `q` stayed the configured value, so `Quorum.new/2` raised `q >= n` inside a
  # tenant's commit. The quiet half: a follower that is up to date would have been treated as one
  # that had not answered, so the commit waited on strictly worse replicas than the ones it already
  # had.
  defp ship_planned(state, wal_path, epoch, header, deadline) do
    {plans, current} =
      Enum.reduce(shippers(), {[], 0}, fn shipper, {plans, current} ->
        case Primary.plan(Map.get(state.followers, shipper), header, max_push_bytes()) do
          :nothing -> {plans, current + 1}
          plan -> {[{shipper, plan} | plans], current}
        end
      end)

    if plans == [] do
      :nothing
    else
      # A FOLLOWER BEING SEEDED IS PLANNED BUT NOT PUSHED TO (2026-10-02). It still counts in the
      # quorum's denominator, as a refusal, so `Quorum.new/2`'s q < n guard sees the same fleet; it
      # simply is not sent a push it could only refuse. Pushing to it raced the seed for the
      # shipper's single per-shard waiter: when the push won, the seed failed `:already_in_flight`,
      # the replica stayed torn, its next reply re-requested a seed, and the loop repeated — the
      # rig's dominant 1024-tenant error source. The seed carries everything up to its start, and
      # the frames committed meanwhile follow as an append once its result lands.
      #
      # A FOLLOWER WITH A PUSH STILL IN FLIGHT IS SKIPPED THE SAME WAY (expert review 2026-10-17 #9).
      # Its shipper holds this shard's single waiter, so a new push would be refused locally
      # `:already_in_flight` (~12% of commits on the rig) — after being read, compressed, charged to
      # the byte budget and cast, plus a warning inside the commit wait. It counts as the same
      # up-front refusal the shipper's reply would have been, and the reason is the same one, so
      # `reconcile/2` (keeps the outstanding expectation), `arm_if_rejected/2` (schedules the retry)
      # and `@settled_rejects` (absent: the entry stays) behave exactly as for the local refusal.
      # "In flight" is bounded to `timeout()` since the push was sent: an `inflight` entry can
      # outlive its push (a local `:overloaded` refusal keeps one), and a follower must not be
      # skipped forever on that — past the bound it is pushed to as before and the shipper decides.
      now = System.monotonic_time(:millisecond)

      {skipped, sendable} =
        Enum.split_with(plans, fn {s, _} ->
          Map.has_key?(state.seeding, s) or in_flight?(state, s, now)
        end)

      skipped =
        Enum.map(skipped, fn {s, _} ->
          {s, if(Map.has_key?(state.seeding, s), do: :seeding, else: :already_in_flight)}
        end)

      built = build_round(state, wal_path, epoch, header, sendable, skipped)

      case built do
        {:ok, pushes, state} ->
          # Completeness over what was SENT (expert review 2026-10-20 #3): a skipped follower was
          # sent nothing, so its capped plan says nothing about this round.
          tag(
            deliver(state, header, sendable, pushes, current, skipped, deadline),
            sendable,
            header
          )

        {:error, reason, state} when reason in [:checkpoint_during_push, :short_read] ->
          {:replan, reason, state}

        {:error, reason, state} ->
          read_failed(state, reason)
      end
    end
  end

  # A ROUND THAT SENDS NOTHING READS NOTHING (expert review 2026-10-20-2027 #17). With every planned
  # follower skipped (in flight or seeding), `build_pushes/5` still paid the header re-check (a
  # dirty-IO stat + pread) and resolved an ordinal, to build zero pushes. The re-check exists to
  # keep mis-stamped BYTES off the wire (`stable?/3`); with no bytes it guards nothing. The round's
  # verdict is unchanged: `deliver/7` counts the current followers (their recorded ACKED positions
  # against this round's inode-validated header) and the skipped ones as refusals, exactly as it did
  # when handed the empty push list.
  defp build_round(state, _wal_path, _epoch, _header, [], _skipped), do: {:ok, [], state}

  defp build_round(state, wal_path, epoch, header, sendable, skipped) do
    t_build = System.monotonic_time()
    built = build_pushes(state, wal_path, epoch, header, sendable)
    stage_duration(:build, t_build, reset_round?(sendable), skipped != [], state.cycle)
    built
  end

  defp in_flight?(state, shipper, now) do
    Map.has_key?(state.inflight, shipper) and
      now - Map.get(state.inflight_at, shipper, 0) < timeout()
  end

  # Did this round hand every follower it SENT to the whole WAL, or did `Primary.plan/3`'s cap stop
  # short? Only the catch-up's stopping rule reads it now; a tenant commit stops on
  # `quorum_holds?/2` (expert review 2026-10-20 #3).
  #
  # Computed from the plans rather than signalled out of `plan/3`, so the cap stays a property of
  # the pure planner and `deliver/5`'s return shape is left alone. `{:reset, 0, len}` compares the
  # same way `{:append, off, len}` does — a reset that reached `size` is complete.
  defp tag({:ok, state, rejects}, plans, header),
    do: {:ok, state, rejects, complete?(plans, header)}

  defp tag(other, _plans, _header), do: other

  # Against the COMMIT EXTENT, the same bound `Primary.plan/3` ships to (expert review 2026-08-20 #5),
  # not the file size. After a checkpoint the WAL keeps its high-water mark, so a round that shipped
  # everything committed still compared short of `size` and forced a pointless extra round on every
  # commit until the extent outgrew the old file (2026-10-02).
  defp complete?(plans, %{commit_extent: extent}),
    do: Enum.all?(plans, fn {_shipper, {_kind, off, len}} -> off + len >= extent end)

  # Public (`@doc false`) only so the 2026-10-12 #33 regression test can hand it a header taken
  # BEFORE a log reset — the exact window between `ship/5`'s header read and this function's payload
  # reads, which nothing else can land a reset inside deterministically. Production reaches it
  # through `ship_planned/4`. Same precedent as `do_seed/6`.
  @doc false
  def build_pushes(state, wal_path, epoch, header, plans) do
    # One read per DISTINCT range, not per follower.
    ranges = plans |> Enum.map(fn {_s, {_k, off, len}} -> {off, len} end) |> Enum.uniq()

    # The held fd is threaded through the fold rather than kept in state, because these reads are
    # a tight loop and the caller reassembles state once at the end (expert review 2026-08-26 #32).
    {payloads, fd} =
      Enum.reduce_while(ranges, {{:ok, %{}}, state.wal_fd}, fn {off, len}, {{:ok, acc}, fd} ->
        case Wal.read_delta_held(fd, wal_path, off, len) do
          {:ok, bin, fd} -> {:cont, {{:ok, Map.put(acc, {off, len}, bin)}, fd}}
          {:error, r, fd} -> {:halt, {{:error, r}, fd}}
        end
      end)

    build_hook()

    # RE-READ THE HEADER AND CONFIRM IT DID NOT MOVE (expert review 2026-08-20 #16).
    #
    # The header above came from a `Wal.read/1` in `ship/5`; the payloads come from separate
    # `open`+`pread` pairs here. Nothing serialises the two against the coordinator's flush task,
    # which runs OFF-PROCESS by design — `shard.ex` does not reference this module at all — and
    # `native/fathom_udf/src/wal.rs` gives the tenant's OWN commit thread a second uncoordinated
    # source of the same generation change at 4000 frames.
    #
    # `read_delta/3`'s `byte_size(bin) == len` guard only fires if the file SHRANK. A checkpoint
    # that restarts the log without shrinking it — a PASSIVE checkpoint followed by a write, which
    # `Wal`'s own moduledoc says need not shrink the file — leaves the requested range perfectly
    # readable, so the pread succeeds and returns bytes from the NEW generation stamped with the
    # OLD generation's ckpt_seq and salt1.
    #
    # The follower then records our generation while holding frames whose per-frame salts differ,
    # so SQLite's checksums make them read as a torn tail rather than as corruption: silently
    # dropped frames on a replica we already acked to the tenant as quorum-durable. Worse, the
    # follower's boot recovery re-derives generation and salt FROM THE FILE, erasing the
    # discrepancy instead of surfacing it.
    #
    # `stable?/2` is the same check the SEED path already runs one function away, for the same
    # reason. Failing here re-plans on the next round, which is correct and cheap; shipping
    # mis-stamped bytes is neither.
    # The re-check reads through the SAME held fd, so it is one stat + one pread rather than a
    # third open/close pair. With the `-shm` fd held it is one INDEX pread and no stat (expert
    # review 2026-10-20-2027 #16): an in-place restart moves the index salts first and drops to the
    # stat + header path, and an unlink+recreate leaves the held fds on a frozen, fully
    # checkpointed generation whose bytes are consistent with its own header — see
    # `Wal.recheck_held/2`. Without the index, the inode revalidation in the stat path catches a
    # recreate the original salt-only check could miss. It reads the header only (`stable?/3` compares
    # nothing else), skipping the commit-extent pread `read_held/2` would add (expert review
    # 2026-10-19 #17).
    {recheck, fd} =
      case Wal.recheck_held(fd, wal_path) do
        {:ok, after_, fd} -> {{:ok, after_}, fd}
        {:error, reason, fd} -> {{:error, reason}, fd}
      end

    state = %{state | wal_fd: fd}

    with {:ok, by_range} <- payloads,
         {:ok, after_} <- recheck,
         :ok <- stable?(header, after_, :checkpoint_during_push) do
      # Resolved ONCE per round, after `stable?/3`: every push in this batch ships from the same
      # header, so they all carry the same ordinal, and asking after the stability check means a
      # WAL that turned out to have been recreated mid-read never bumps the counter for bytes that
      # are not going to be shipped.
      {ordinal, state} = with_ordinal(state, header.salt1)

      # Compressed ONCE per distinct range (perf review 2026-10-01 #32): followers in step share a
      # range, so N followers cost one zstd pass, not N. Only the ordinal frame carries it.
      zby_range =
        if ReplProtocol.ordinal_wire?() and ReplProtocol.compress?(),
          do:
            Map.new(by_range, fn {range, bytes} ->
              {range, ReplProtocol.compress_payload(bytes)}
            end),
          else: %{}

      {:ok,
       for {shipper, {kind, off, len}} <- plans do
         {shipper,
          %Push{
            shard_id: state.shard_id,
            epoch: epoch,
            wal_gen: header.ckpt_seq,
            # The WAL's identity, not just its generation. `ckpt_seq` restarts at 0 when SQLite
            # recreates a deleted WAL, so the generation alone cannot tell the follower that a new
            # lineage began — see the @version 2 note in `Protocol`.
            salt1: header.salt1,
            # A reset must arrive at offset 0 or FollowerLog refuses it — the follower has to
            # discard its old generation rather than splice across the seam.
            offset: if(kind == :reset, do: 0, else: off),
            # How far the generation being DISCARDED got (expert review 2026-08-20 #11a). Only
            # meaningful on a reset; 0 on an append, where the follower's own offset already says
            # everything and 0 is the protocol's "no statement".
            prev_extent: if(kind == :reset, do: outgoing_extent(state, shipper), else: 0),
            # WHICH WAL, on a scale that ORDERS (expert review 2026-08-26 #2). `salt1` above says
            # the WAL changed; only this says which of two WALs is newer, and `Promote.fresher?/2`
            # needs the latter. Emitted on the wire only while `Protocol.ordinal_wire?/0` is on —
            # but resolved either way, so the coordinator's counter tracks reality regardless of
            # the gate and the object stamp can use it before the fleet is upgraded.
            wal_ordinal: ordinal,
            # THE OWNERSHIP LINEAGE (expert review 2026-09-05 #4/#12), cached beside `epoch` by
            # `with_epoch/1`. On the wire only while `Protocol.ordinal_wire?/0` is on (it rides the
            # same `@push_ord_lin` frame as the ordinal). Lets the follower's `decide/2` fence on the
            # monotonic lineage instead of the reset-prone lock epoch, and refresh a frozen lineage.
            lineage: state.lineage,
            payload: Map.fetch!(by_range, {off, len}),
            zpayload: Map.get(zby_range, {off, len})
          }}
       end, state}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  # TEST SEAM (expert review 2026-10-20 #9): lands a WAL restart between `build_pushes/5`'s payload
  # reads and its header re-check, the window no test could otherwise hit deterministically. Compiled
  # in only where `:replication_test_hooks` is set at compile time (config/test.exs); everywhere else
  # it is an empty function.
  if Application.compile_env(:fathom, :replication_test_hooks, false) do
    defp build_hook do
      case Application.get_env(:fathom, :replication_build_hook) do
        f when is_function(f, 0) -> f.()
        _ -> :ok
      end
    end
  else
    defp build_hook, do: :ok
  end

  # The furthest this primary is KNOWN to have shipped in the generation `shipper` is about to
  # discard — the high-water mark across every follower still sitting in that same generation.
  #
  # Why a max across peers rather than the primary's own number: by the time a reset is planned the
  # outgoing WAL has already been checkpointed away, so its final size is not readable. What IS
  # readable is where each follower got to, and a peer that reached 8192 in a generation where this
  # one reached 4096 is positive evidence of 4096 missing bytes.
  #
  # Matched on `{wal_gen, salt1}`, not `wal_gen` alone: `ckpt_seq` restarts at 0 when SQLite
  # recreates a deleted WAL, so the generation number alone would compare two unrelated lineages
  # and manufacture a gap out of a fresh WAL.
  #
  # **The honest limitation**: with a single follower the max is that follower's own offset, so this
  # never fires and the replica is treated as complete. That is deliberate — it only ever reports a
  # gap it has EVIDENCE for, and inventing one would mark replicas torn across the fleet. What IS
  # closed since expert review 2026-09-29 #1 is the case this number could never see: a NEW owner's
  # session has no record of any follower and always states 0, so an ownership change is now
  # detected by the follower itself (`FollowerLog.reseed?/2`) and answered with a seed request.
  # A single follower short WITHIN one ownership is still unstated — the remaining limitation.
  #
  # AND THE SESSION'S OWN READS (expert review 2026-10-15 #1). Every commit extent this Session has
  # READ in that generation is evidence too: those frames were committed, so a follower short of
  # them is short whether or not any peer received them. This is what catches the frames a
  # checkpoint can still erase after the Session read them but before a quorum held them — commits
  # that failed their quorum and were then folded away by the close-time checkpoint, or by the
  # forced checkpoint at the WAL cap (`start_checkpoint/3`). Frames the Session never read are the
  # case this CANNOT see, which is why checkpoints of a replicated WAL are pinned instead.
  #
  # AND A WHOLE GENERATION MISSED (round-4 replication review 2026-10-20). Everything above measures
  # the follower against ITS OWN generation. A follower still in g-1 when this header is g+1 — down
  # or disconnected for the whole of g — held all of g-1, so it was told "complete", absorbed, and
  # cleared `torn` over a replica missing every page only g wrote: untorn, ranked equal to its
  # peers, promotable, and short of quorum-acked writes. So when the generations this Session read
  # show one WITH FRAMES between the follower's and the current one, the follower is short whatever
  # it holds of its own: `own + 1` is the smallest statement that says so, and the follower answers
  # it with a seed request (`Follower.short_reset?/3`). `missed_generation?/3` has the rule.
  defp outgoing_extent(state, shipper) do
    case Map.get(state.followers, shipper) do
      %{wal_gen: gen, salt1: salt, offset: own} when is_integer(own) ->
        if missed_generation?(state.seen, gen, salt),
          do: own + 1,
          else: own_generation_extent(state, gen, salt, own)

      _ ->
        0
    end
  end

  defp own_generation_extent(state, gen, salt, own) do
    peers =
      state.followers
      |> Map.values()
      |> Enum.filter(&match?(%{wal_gen: ^gen, salt1: ^salt, offset: o} when is_integer(o), &1))
      |> Enum.map(& &1.offset)
      |> Enum.max(fn -> own end)

    case Enum.find(state.seen, &match?({^gen, ^salt, _}, &1)) do
      {_, _, read} -> max(peers, read)
      nil -> peers
    end
  end

  # THE QUORUM WAIT GETS WHAT IS LEFT OF THE COMMIT'S DEADLINE, NOT A FRESH `timeout()` (expert
  # review 2026-10-20 #4). `collect/6` used to hand `ship_quorum` the full call timeout every round,
  # so a single round already outlived `commit_deadline_budget/0` — the handler then replied after
  # the caller's `GenServer.call` had given up (`{:session_down, :timeout}`, the exact outcome the
  # 2026-08-31 #21 reply margin exists to prevent), and a multi-round or post-seed commit could hold
  # the Session for up to one more `timeout()` per round past its caller. A shorter wait can only
  # refuse, never ack, so this is safety-neutral. Out of time before sending: refuse without sending
  # — and without recording an expectation for a push that never left.
  defp deliver(state, header, plans, pushes, current, skipped, deadline) do
    needed = max(quorum() - current, 0)
    remaining = deadline - System.monotonic_time(:millisecond)

    if needed > 0 and remaining <= 0 do
      {:error, {:no_quorum, :timeout}, state, for({s, reason} <- skipped, do: {s, reason, 0})}
    else
      send_round(state, header, plans, pushes, skipped, needed, remaining)
    end
  end

  defp send_round(state, header, plans, pushes, skipped, needed, remaining) do
    # Recorded BEFORE anything is sent, because a reply can be observed by a later commit's drain
    # with no plan in scope. See the `inflight` note in `init/1`.
    #
    # AN OUTSTANDING EXPECTATION IS NEVER OVERWRITTEN (expert review 2026-08-20 #27). The argument
    # order is the fix and it is easy to "tidy" back, so: `Map.merge/2` lets the SECOND map win,
    # and this used to be `merge(state.inflight, expectations(...))` — so the earlier push's
    # expectation, the one a pending reply still needs, was destroyed before the pushes were even
    # sent.
    #
    # That defeated `@settled_rejects` exactly where it was written to help. It deliberately
    # EXCLUDES `:already_in_flight` and `:overloaded` so `reconcile/2` keeps the entry "so that
    # reply can still be reconciled" — but by the time the local refusal happened the entry it
    # kept was the NEW one. The genuine ack for the earlier push then failed
    # `settle_late_ack/3`'s offset check, declined to advance, and `forget_inflight/2` dropped it
    # anyway: a follower that acked bytes it really holds was not advanced, the next commit planned
    # from a stale position, and the follower rejected `:offset_mismatch` before the record was
    # corrected. One wasted round trip and one wasted (up to 1 MiB) payload per occurrence, at the
    # ~10k `:already_in_flight` per node AGENTS.md records at 512 tenants.
    #
    # Old-wins is correct rather than merely safer, because a `Shipper` holds exactly ONE waiter
    # per shard and a `Session` is per-shard: if `inflight` already has an entry for a shipper,
    # that shipper is still holding this shard's waiter, so the push about to be sent WILL be
    # refused locally and has no expectation to record.
    now = System.monotonic_time(:millisecond)

    sent_at =
      for {shipper, _} <- plans, not Map.has_key?(state.inflight, shipper), into: %{} do
        {shipper, now}
      end

    state = %{
      state
      | inflight: Map.merge(expectations(plans, header), state.inflight),
        inflight_at: Map.merge(state.inflight_at, sent_at)
    }

    case needed do
      # Followers that are already current carry the quorum on their own. The laggards still get
      # their bytes — skipping them is how a replica falls behind forever — but the tenant does not
      # wait for an ack it does not need.
      0 ->
        Replication.ship_async(pushes)

        # A follower skipped for an outstanding push gets no reply to reject, so report the refusal
        # that reply would have been: it arms the catch-up retry, which is what re-sends to a
        # laggard on a quiet shard (expert review 2026-10-17 #9). Seeding skips stay silent, as before.
        {:ok, state, for({s, :already_in_flight} <- skipped, do: {s, :already_in_flight, 0})}

      _ ->
        collect(state, pushes, needed, skipped, reset_round?(plans), remaining)
    end
  end

  # Per-round stage timing for the commit path (expert review 2026-10-17 #10): `:build` is the WAL
  # read + stability re-check + compress, `:collect` is the quorum wait. Bounded `stage` tag.
  #
  # `reset` (expert review 2026-10-19 #9, MEASURE ONLY) is true when any follower in the round was
  # planned a `{:reset, ...}` — the first commit after a checkpoint, which waits for the follower's
  # absorb (two fsync-bearing checkpoints) before it acks. Tagging it lets the rig compare reset vs
  # plain `:collect` p99 and decide whether the async-absorb redesign is worth building. Bounded
  # (`true | false`).
  #
  # `skipped` (expert review 2026-10-20-2027 #5) is true when the round skipped a follower (a push
  # still in flight, or a seed) — the "Q of FREE followers" round #8 asks about: compare `:collect`
  # p99 with `skipped=true` against `false` before building push pipelining. `mode` is the cycle's
  # origin, as on `[:fathom, :replication, :commit]`. Both bounded.
  defp stage_duration(stage, t0, reset?, skipped?, mode) do
    :telemetry.execute(
      [:fathom, :replication, :commit_stage],
      %{duration: System.monotonic_time() - t0},
      %{stage: stage, reset: reset?, skipped: skipped?, mode: mode}
    )
  end

  defp reset_round?(plans),
    do: Enum.any?(plans, fn {_shipper, {kind, _, _}} -> kind == :reset end)

  defp collect(state, pushes, needed, skipped, reset?, timeout) do
    t_collect = System.monotonic_time()

    result =
      Replication.ship_quorum(pushes, needed, timeout,
        skip: skipped,
        shard_id: state.shard_id
      )

    stage_duration(:collect, t_collect, reset?, skipped != [], state.cycle)

    case result do
      {:ok, acked, rejects} ->
        # Advance ONLY the followers that actually ACKED — not everyone the quorum succeeded
        # without. Advancing a rejecter would leave the primary believing it holds bytes it
        # refused, and the next delta would start past a gap only that follower has; it would then
        # reject forever while the shard reported a healthy quorum. Found by the late-joiner test.
        state = Enum.reduce(acked, state, &advance(&2, &1))

        # Rejects are returned on SUCCESS too, and they matter just as much here. A quorum of 2 out
        # of 3 succeeds while the third answers `:unknown_shard`, and that reply is consumed into
        # this list rather than arriving late — so if only the failure path acted on it, whether a
        # straggler ever got seeded would come down to whether its reply beat the quorum. It did
        # not, reliably: the late-joiner test hung here.
        {:ok, reconcile(state, rejects), rejects}

      {:error, {:no_quorum, why, rejects}} ->
        {:error, {:no_quorum, why}, reconcile(state, rejects), rejects}
    end
  end

  defp expectations(plans, header) do
    Map.new(plans, fn {shipper, plan} -> {shipper, Primary.advance(header, plan)} end)
  end

  # `collect/4` has already checked that this follower reported exactly the position we shipped it,
  # so the recorded expectation IS its new state and there is nothing left to verify. Late acks go
  # through `settle_late_ack/3`, which does that check itself.
  defp advance(state, shipper) do
    key = shipper

    case Map.pop(state.inflight, key) do
      {nil, _} -> state
      {pos, rest} -> %{state | followers: Map.put(state.followers, key, pos), inflight: rest}
    end
  end

  # A follower that refused told us where it actually is. Believe it: correcting our record here is
  # what turns the next commit's plan into a catch-up delta from that follower's real position,
  # with no separate retransmission machinery. `:unknown_shard` is deliberately NOT reconciled — it
  # means the follower holds nothing at all, and the answer is a seed, not an offset.
  #
  # The correction is applied to the OUTSTANDING push's expectation, not to whatever we last
  # believed. `FollowerLog` only answers `:offset_mismatch` once it has agreed with the push about
  # the epoch and the generation, so the generation and salt we just shipped are the follower's, and
  # only the offset was wrong. Reading them off the previous record instead meant a follower we had
  # no record for — a late joiner, or one whose ack was lost — had its correction thrown away, and
  # the primary went on planning from a position it had already been told was wrong.
  defp reconcile(state, rejects) do
    Enum.reduce(rejects, state, fn
      {shipper, :offset_mismatch, at}, acc ->
        key = shipper

        case Map.pop(acc.inflight, key) do
          {nil, _} ->
            acc

          {pos, rest} ->
            %{acc | followers: Map.put(acc.followers, key, %{pos | offset: at}), inflight: rest}
        end

      # A reason after which nothing is outstanding ENDS THE WAIT. `inflight` means "this follower
      # still owes us a reply", and leaving these behind made the map claim a follower was mid-flight
      # forever — which `settle_inflight/3` then burns a whole deadline on.
      #
      # Only `:offset_mismatch` above CONSUMES the entry (it needs the recorded generation and salt
      # to build the correction); these merely drop it. `:already_in_flight` and `:overloaded` fall
      # through to the catch-all and KEEP it — see `@settled_rejects` for why that distinction is
      # load-bearing rather than tidy.
      {shipper, reason, _at}, acc when reason in @settled_rejects ->
        %{acc | inflight: Map.delete(acc.inflight, shipper)}

      # Our own shipper refused locally; the follower never saw this frame and still owes a reply to
      # an earlier one. Keep the expectation so that reply can still be reconciled.
      _local_refusal, acc ->
        acc
    end)
  end

  defp read_failed(state, reason) do
    Logger.warning("replication read failed for #{state.shard_id}: #{inspect(reason)}")
    {:error, reason, state}
  end

  # ------------------------------------------------------------------------------------------
  # Checkpoints of a replicated WAL (expert review 2026-10-15 #1)
  # ------------------------------------------------------------------------------------------
  #
  # THE BUG. A stream commits, THEN calls `commit/3`, which reads the WAL itself. A checkpoint in
  # between that backfills every frame lets the next writer on another stream RESTART the log in
  # place, and the commit's frames are gone before this Session ever reads them. The Session then
  # sees a new generation, plans a reset carrying only the new frames, and the followers ack it —
  # the first commit is acked quorum-durable and exists on no replica, and the frames after it sit
  # on a base that lacks it. (`replication_unshipped_checkpoint_test.exs` reproduces it.)
  #
  # THE RULE (`docs/a2-quorum-replication.md`): checkpoint only what the write quorum has acked.
  #
  # THE MECHANISM IS SQLITE'S OWN READ MARKS, not a watermark the checkpoint consults. A watermark
  # check is a time-of-check race — a commit landing between "everything is shipped" and the
  # checkpoint is backfilled with the rest. A held read snapshot instead BOUNDS the checkpoint inside
  # SQLite: a PASSIVE checkpoint never backfills past the oldest reader's mark (a reader on mark 0
  # blocks backfill altogether), and the log restarts only once everything is backfilled. So:
  #
  #   1. take a read snapshot P (the "pin") FIRST;
  #   2. read the WAL header AFTER it — its extent E covers everything committed before P;
  #   3. checkpoint only if a quorum holds E, and only while still pinned. The backfill then stops
  #      at or before P <= E, every backfilled frame is quorum-held, and a restart (which needs all
  #      frames backfilled) can only discard quorum-held frames.
  #
  # Every checkpoint of a replicated WAL that fathom runs while streams are live goes through that:
  #
  #   * the 4000-frame commit hook (`native/fathom_udf/src/wal.rs`) STEPS ASIDE for a file this
  #     Session registered (`ensure_registered/2`), and this Session takes its place at the same
  #     threshold, pinning BEFORE the commit's ship and checkpointing after it, in a task that owns
  #     the pin and a checkout grant, off the reply path (`maybe_pin/2`, `start_checkpoint/3`) — so
  #     it pays the open only on a commit that crosses the threshold;
  #   * the coordinator's post-snapshot PASSIVE (`Fathom.Shard.do_snapshot/3`) calls
  #     `checkpoint_after_snapshot/2`, which pins and asks this Session `{:covers, header}`;
  #   * a tenant's own `PRAGMA wal_checkpoint` is refused on a registered file by the extension's
  #     authorizer (`pragma_guard.rs`).
  #
  # The close-time checkpoint of the pool drain grace's LAST close is now COVERED, though not pinned
  # (expert review 2026-10-20 #7): the coordinator asks this Session first (`request_idle_close/2`),
  # which ships whatever a connected follower is still owed and answers `:ready` only when nothing a
  # quorum or a connected follower lacks would be folded — and, having read the header, closes the
  # "never read" case below for that path. No pin is needed there: the shard is idle, and any stream
  # that checks out in between makes the coordinator discard the verdict. Past a bounded number of
  # asks the coordinator drains anyway, with the uncovered properties below.
  #
  # NOT covered, deliberately: that bounded fallback, the other drains (write fence, terminate) and
  # the drop path's TRUNCATE. All run only with no stream checked out, so the unshipped frames they
  # can erase are:
  #
  #   * frames of commits that FAILED their quorum (a 503). This Session read them, and
  #     `outgoing_extent/2` counts what the Session read, so a follower missing them is marked short
  #     at the next reset and re-seeded rather than laundered complete;
  #   * frames of a commit whose stream died AFTER SQLite committed and BEFORE it called `commit/3`
  #     (a crash, a kill, a stream timeout in that window). Nobody acked that write, but nobody
  #     READ it either, so if an idle close then folds it away before any other commit ships it, the
  #     next reset cannot mention it and the followers lack it while later acked frames sit on top.
  #     This is the one residual laundering case: it needs a stream death inside a microseconds-wide
  #     window AND the shard to go idle before its next commit, and closing it would need the
  #     close-time checkpoint itself to be gated (a connection-close hook SQLite does not offer).
  #
  # Also not covered: a node with the extension NOT loaded, where SQLite's built-in autocheckpoint
  # owns the slot (the extension ships with fathom).
  #
  # THE BOUND. A skipped checkpoint must not grow the WAL forever. Under a healthy quorum the pinned
  # checkpoint is covered as soon as the commit that triggered it ships, so it runs every
  # `checkpoint_step_bytes/0` past the threshold exactly as often as the hook did. Under a quorum
  # that keeps failing nothing ships, so past `checkpoint_force_bytes/0` the checkpoint runs
  # anyway — still pinned, so it backfills only frames this Session has READ, which the
  # `outgoing_extent/2` rule above turns into a re-seed of every follower that lacks them. That is
  # the chosen trade: availability of the local WAL bound over keeping replicas incrementally
  # current through an outage that is already failing every write.
  #
  # COST per commit: two map updates (`note_header/2`) and an integer comparison (`checkpoint_due?/1`).
  # A commit that crosses the threshold additionally opens the pin (~0.4 ms) before its ship and,
  # after it, waits for one spawn + coordinator checkout handshake; the checkpoint connection's open
  # and the PASSIVE backfill run in the task, off the reply path and off this Session.

  @frame_bytes 4096 + 24
  @default_checkpoint_bytes 4000 * @frame_bytes
  @default_checkpoint_step_bytes 1000 * @frame_bytes
  @seen_generations 4

  # The hook's 4000-frame threshold at the default 4 KiB page, in bytes because the header is.
  defp checkpoint_bytes,
    do: Application.get_env(:fathom, :replication_checkpoint_bytes, @default_checkpoint_bytes)

  # How much further a WAL must grow before a skipped or ineffective attempt is retried.
  defp checkpoint_step_bytes,
    do:
      Application.get_env(
        :fathom,
        :replication_checkpoint_step_bytes,
        @default_checkpoint_step_bytes
      )

  # Past this, an uncovered checkpoint runs anyway (see THE BOUND above).
  defp checkpoint_force_bytes,
    do: Application.get_env(:fathom, :replication_checkpoint_force_bytes, 4 * checkpoint_bytes())

  @doc """
  The coordinator's post-snapshot checkpoint, on the flush's own connection `conn`.

  Replication off (or no shard id): the unconditional `PRAGMA wal_checkpoint(PASSIVE)` it always
  was. Replication on: pinned and gated on the quorum holding the WAL — see "Checkpoints of a
  replicated WAL" above. Never raises; a skipped checkpoint is retried at the next flush.
  """
  #
  # `opts[:before_checkpoint]` is a TEST seam (a 0-arity fun run after the covers check and before
  # the PASSIVE): it is how `replication_unshipped_checkpoint_test.exs` lands a commit inside that
  # window, proving the pin — not the covers check — is what protects a late commit.
  #
  # STARVATION, KNOWN AND ACCEPTED: the header is read here, outside the Session, so on a shard with
  # a commit in flight it is usually a few frames ahead of what the quorum has acked and `covers`
  # answers false. On a steadily written shard this checkpoint therefore rarely runs, and the WAL is
  # bounded by the Session's own threshold checkpoint (which pins before its ship, so it is covered
  # as soon as that ship lands). It still runs on every quiet shard, which is where it matters.
  #
  # COUNTED, AND SKIPPED WHEN SMALL UNDER REPLICATION (expert review 2026-10-20 #15). Every outcome
  # emits `[:fathom, :shard, :flush_checkpoint]` tagged `result` (`:ran | :busy |
  # :skipped_not_covered | :skipped_small | :skipped_no_session | :error`) and `replicated`; before,
  # only the Session's threshold checkpoints were counted, so nothing showed that a write-active
  # replicated shard took a WAL generation reset (follower absorb = 2 fsyncs, plus a reset round on
  # the next commit) on EVERY flush — the measured 22-25% txn/s gap between a 5 s and a 30 s flush
  # interval. Under replication the PASSIVE now runs only once the WAL's commit extent reaches
  # `flush_checkpoint_min_bytes/0` (1/4 of the Session threshold, ~4 MB): the snapshot does not
  # need it (VACUUM INTO reads `.db` + WAL), the Session's threshold checkpoint bounds a busy WAL,
  # and the covered idle close (#7) folds a quiet one. The original reason for this checkpoint
  # (2026-07-24 #4: SQLite's autocheckpoint billed to a tenant statement) does not apply under A2,
  # where the extension hook steps aside and the Session checkpoints off the reply path.
  # Replication OFF is unchanged: the unconditional PASSIVE.
  @spec checkpoint_after_snapshot(reference(), String.t() | nil, keyword()) ::
          {:ok, map()} | {:error, term()} | :skipped
  def checkpoint_after_snapshot(conn, shard_id, opts \\ []) do
    replicated? = is_binary(shard_id) and enabled?()

    {result, outcome} =
      if replicated? do
        case lookup(shard_id) do
          # No Session: nothing has shipped for this shard under this coordinator, and the
          # extension hook has not been told to step aside, so there is nothing to gate on.
          # Skipping is the safe direction; the WAL stays bounded by the hook and the idle close.
          nil -> {:skipped, :skipped_no_session}
          pid -> pinned_flush_checkpoint(conn, pid, Fathom.Shard.db_path(shard_id), opts)
        end
      else
        passive(conn)
      end

    :telemetry.execute(
      [:fathom, :shard, :flush_checkpoint],
      %{count: 1},
      %{result: outcome, replicated: replicated?}
    )

    result
  end

  @doc """
  The REPLICATION-OFF flush PASSIVE, run BEFORE the pinned-live route takes its pin rather than
  after a `VACUUM INTO` (expert review 2026-10-21 #11). Counted like `checkpoint_after_snapshot/3`
  (`replicated: false`).

  Never call it for a replicated shard: under A2 a flush checkpoint is pinned and quorum-gated
  (`checkpoint_after_snapshot/3`), and the user decision of 2026-10-21 is that the replicated flush
  gains NO new checkpoint (each one is a WAL-generation reset the followers pay for).
  """
  @spec checkpoint_before_pin(reference()) :: {:ok, map()} | {:error, term()}
  def checkpoint_before_pin(conn) do
    {result, outcome} = passive(conn)

    :telemetry.execute(
      [:fathom, :shard, :flush_checkpoint],
      %{count: 1},
      %{result: outcome, replicated: false}
    )

    result
  end

  defp passive(conn) do
    case Connection.query(conn, "PRAGMA wal_checkpoint(PASSIVE)", []) do
      {:ok, %{rows: [[1 | _] | _]}} = r -> {r, :busy}
      {:ok, _} = r -> {r, :ran}
      other -> {other, :error}
    end
  end

  # 0 disables the size gate (every flush checkpoints, as before #15) — the rig A/B lever.
  defp flush_checkpoint_min_bytes,
    do:
      Application.get_env(
        :fathom,
        :replication_flush_checkpoint_min_bytes,
        div(checkpoint_bytes(), 4)
      )

  @doc """
  The coordinator's covered-idle-close ask (expert review 2026-10-20 #7), sent from the coordinator
  that is about to drain an idle shard's pool — whose last close is SQLite's close-time checkpoint.

  Asynchronous on purpose: the Session calls its coordinator while it works (epoch, ordinal,
  checkout grants), so a synchronous ask from the coordinator could deadlock until a timeout. The
  answer arrives at the caller as `{:idle_close_verdict, token, :ready | :wait | :retry}` (`:retry`:
  the WAL header could not be read, so nothing was judged). Returns `:none`
  when there is no Session to ask (replication off, or nothing shipped yet under this coordinator):
  the caller then drains as it always did.
  """
  @spec request_idle_close(String.t(), reference()) :: :asked | :none
  def request_idle_close(shard_id, token) do
    if enabled?() do
      case lookup(shard_id) do
        nil ->
          :none

        pid ->
          send(pid, {:idle_close?, self(), token})
          :asked
      end
    else
      :none
    end
  end

  defp lookup(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] -> pid
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp pinned_flush_checkpoint(conn, session, db_path, opts) do
    wal = db_path <> "-wal"

    # The size gate reads the header BEFORE pinning, so a small WAL — the common case on a flush —
    # costs one header read and never the pin's connection open (~0.4 ms). The decision to RUN is
    # still made on a header read under the pin, below.
    if small_wal?(wal) do
      {:skipped, :skipped_small}
    else
      case with_pin(db_path, fn -> covered_passive(conn, session, wal, opts) end) do
        :skipped ->
          {:skipped, :error}

        {result, _outcome} = verdict ->
          # rv6 F1: the coordinator's checkpoint fsyncs this WAL through ITS fd, so an IOERR it
          # consumed is one the Session's next fdatasync will not see. Tell the Session.
          if WalSync.io_error?(result), do: send(session, {:checkpoint_io_error, result})
          verdict
      end
    end
  catch
    :exit, _ -> {:skipped, :skipped_not_covered}
  end

  defp covered_passive(conn, session, wal, opts) do
    # `index: true` (expert review 2026-10-20 #1/#17): the covers-check must not count a torn
    # stale-tail frame, and it is one index pread instead of the backward walk.
    with {:ok, %{} = header} <- Wal.read(wal, index: true),
         true <- GenServer.call(session, {:covers, header}, 1_000) do
      if hook = opts[:before_checkpoint], do: hook.()
      passive(conn)
    else
      {:ok, :empty} -> {:skipped, :skipped_small}
      _ -> {:skipped, :skipped_not_covered}
    end
  end

  defp small_wal?(wal) do
    case flush_checkpoint_min_bytes() do
      min when is_integer(min) and min > 0 ->
        case Wal.read(wal, index: true) do
          {:ok, %{commit_extent: e}} -> e < min
          {:ok, :empty} -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  @doc """
  Run `fun.(pin_conn)` while a read snapshot is pinned on `db_path` (expert review 2026-10-20 #28).

  The pin is a read transaction on its own handle, so no checkpoint can backfill into the
  `.db` while it is held (the checkpointer takes read-lock 0 exclusively to write pages, and
  limits itself to the oldest read mark): the main file's bytes are frozen for the duration, and
  `pin_conn` reads exactly them. `:skipped` if the pin could not be opened.

  Unlike the `:ro` pins of `pinned_flush_checkpoint/4`, this handle is opened READ-WRITE (and only
  ever reads): the flush's caller may hold no other connection, so this close can be the LAST one,
  and the last close must run SQLite's close-time checkpoint and unlink `-wal`/`-shm` exactly as
  the flush's old snapshot connection did. A `:ro` handle leaves a zero-length `-wal` and a stale
  `-shm` behind (`PoolGraceTest` pins the unlink).
  """
  @spec with_flush_pin(Path.t(), (reference() -> result), keyword()) :: result | :skipped
        when result: var
  def with_flush_pin(db_path, fun, opts \\ []) when is_function(fun, 1) do
    with {:ok, _} <- Fathom.FS.stat(db_path),
         {:ok, pin} <- Connection.open(db_path) do
      try do
        # `:before_pin` runs on this handle BEFORE its read transaction begins (expert review
        # 2026-10-21 #11: the replication-off flush's PASSIVE, moved ahead of the pin so the pin
        # can find the WAL fully backfilled).
        if before = opts[:before_pin], do: before.(pin)

        case pin_snapshot(pin) do
          :ok -> fun.(pin)
          {:error, _} -> :skipped
        end
      after
        release_pin(pin)
      end
    else
      _ -> :skipped
    end
  end

  # Holds a read snapshot on `db_path` around `fun`. The pin's connection is closed only after
  # `fun` returns, and every caller holds another connection to the same file across it (a stream's,
  # or the flush's), so this close is never the LAST one — the close that runs SQLite's close-time
  # checkpoint and unlinks the WAL.
  defp with_pin(db_path, fun) do
    case open_pin(db_path) do
      {:ok, pin} ->
        try do
          fun.()
        after
          release_pin(pin)
        end

      _ ->
        :skipped
    end
  end

  defp open_pin(db_path) do
    with {:ok, _} <- Fathom.FS.stat(db_path),
         {:ok, conn} <- Connection.open(db_path, scope: :ro) do
      case pin_snapshot(conn) do
        :ok ->
          {:ok, conn}

        {:error, reason} ->
          Connection.close(conn)
          {:error, reason}
      end
    end
  end

  defp release_pin(conn) do
    _ = Connection.rollback_if_open(conn)
    Connection.close(conn)
  end

  # Tell the extension's commit hook to leave this WAL to us. Once per Session, on its first commit
  # (a stream is checked out, but nothing here touches the shard's files: the registry is
  # process-wide, reached through a private in-memory connection).
  #
  # `registered` keeps the KEY the extension answered, not the path we passed (review follow-up
  # 2026-10-15 M1): `terminate/2` can run after the drop path unlinked the `.db`, and a release
  # by a path that no longer resolves through a symlinked directory missed the key — leaving the
  # file "replicated", i.e. never checkpointed by the hook, for the life of the node.
  #
  # Any outcome other than a key LATCHES `:unavailable` (review follow-up L): with the extension
  # absent this used to retry the `:memory:` open and extension load on every commit.
  defp ensure_registered(%{registered: nil} = state, wal_path) do
    db_path = String.replace_suffix(wal_path, "-wal", "")

    case wal_registry(db_path, 1) do
      {:ok, key} when is_binary(key) -> %{state | registered: key}
      _ -> %{state | registered: :unavailable}
    end
  end

  defp ensure_registered(state, _wal_path), do: state

  # A Session KILLED (no `terminate/2`) leaves its registration behind: that file's WAL is then
  # checkpointed only by its next Session (which registers and later releases its own) and the idle
  # close — never unbounded, and on a fleet with replication on, the state it should be in anyway.
  defp unregister(%{registered: key}) when is_binary(key) do
    _ = wal_registry(key, -1)
    :ok
  end

  defp unregister(_state), do: :ok

  defp wal_registry(db_path, delta) do
    with {:ok, conn} <- Exqlite.Sqlite3.open(":memory:") do
      try do
        case Fathom.Shard.Extension.load(conn) do
          :ok -> registry_call(conn, db_path, delta)
          other -> other
        end
      after
        Exqlite.Sqlite3.close(conn)
      end
    end
  rescue
    e -> {:error, e}
  end

  defp registry_call(conn, db_path, delta) do
    with {:ok, stmt} <- Exqlite.Sqlite3.prepare(conn, "SELECT fathom_wal_replicated(?1, ?2)") do
      try do
        with :ok <- Exqlite.Sqlite3.bind(stmt, [db_path, delta]),
             {:row, [answer]} <- Exqlite.Sqlite3.step(conn, stmt) do
          {:ok, answer}
        else
          other -> {:error, other}
        end
      after
        Exqlite.Sqlite3.release(conn, stmt)
      end
    end
  end

  # Every header a ship reads: the latest one (for the checkpoint decision) and the furthest extent
  # read per generation (for `outgoing_extent/2`).
  defp note_header(state, %{ckpt_seq: gen, salt1: salt, commit_extent: extent} = header) do
    seen =
      case state.seen do
        [{^gen, ^salt, x} | rest] -> [{gen, salt, max(x, extent)} | rest]
        seen -> Enum.take([{gen, salt, extent} | seen], @seen_generations)
      end

    %{state | last_header: header, seen: seen}
  end

  defp note_header(state, _empty), do: state

  # Did a follower recorded in generation `{gen, salt}` miss a whole generation WITH FRAMES since?
  # (round-4 replication review 2026-10-20, `outgoing_extent/2`.) `seen` is newest first and its
  # head is the header being planned from (`ship/7` notes it before planning).
  #
  #   * the follower's generation IS the head: a same-generation reset (the WAL shrank), nothing
  #     skipped;
  #   * it is further down: every generation between it and the head was skipped by it, and one
  #     holding a commit (extent past the bare WAL header) is pages it never received;
  #   * it is not in `seen` at all: older than the horizon when `seen` is full — everything listed
  #     after the head was skipped. With `seen` NOT full this Session has read every generation since
  #     it started, so an unlisted one is a generation it never read (a seed's own header after a
  #     restart race): no evidence, the old rule applies.
  #
  # A false positive costs one seed (an ack lost after the follower applied a push, say); a false
  # negative is a promotable replica missing acked writes.
  defp missed_generation?([{gen, salt, _} | _], gen, salt), do: false

  defp missed_generation?([_head | older] = seen, gen, salt) do
    case Enum.split_while(older, &(not match?({^gen, ^salt, _}, &1))) do
      {between, [_found | _]} -> holds_frames?(between)
      {between, []} -> length(seen) >= @seen_generations and holds_frames?(between)
    end
  end

  defp missed_generation?([], _gen, _salt), do: false

  defp holds_frames?(generations),
    do: Enum.any?(generations, fn {_, _, extent} -> extent > Wal.header_bytes() end)

  defp checkpoint_due?(%{last_header: %{commit_extent: e, salt1: s}, ckpt_at: at}) do
    e >= checkpoint_bytes() and
      case at do
        {^s, last} -> e >= last + checkpoint_step_bytes()
        _ -> true
      end
  end

  defp checkpoint_due?(_state), do: false

  # Pin BEFORE this commit's ship reads the header (step 1 above). `last_header` is cleared so the
  # decision after the ship uses only a header read under the pin.
  defp maybe_pin(state, wal_path) do
    if checkpoint_due?(state) do
      %{salt1: s, commit_extent: e} = state.last_header
      state = %{state | ckpt_at: {s, e}, last_header: nil}

      case open_pin(String.replace_suffix(wal_path, "-wal", "")) do
        {:ok, pin} -> {pin, state}
        _ -> {nil, state}
      end
    else
      {nil, state}
    end
  end

  # The decision is made HERE (it needs the follower positions); the checkpoint itself runs in a
  # task that owns the pin, OFF the commit reply path (review follow-up 2026-10-15 M2). It used to
  # run inline before the reply, where the PASSIVE backfill plus two opens (each with a 5 s busy
  # timeout) could push the reply past the committer's call timeout — a `{:session_down, :timeout}`
  # 503 for a write that had shipped — and stalled every queued ship of the shard behind it.
  defp start_checkpoint(state, nil, _wal_path), do: state

  defp start_checkpoint(state, pin, wal_path) do
    case checkpoint_verdict(state) do
      :skip ->
        release_pin(pin)
        state

      # #8: Q-covered, but a connected follower has not been sent everything. Skip this attempt —
      # `ckpt_at` (set by `maybe_pin/2`) makes the next one due one `checkpoint_step_bytes/0` later
      # — and kick the laggard with the catch-up retry, which on a busy shard the next commit
      # pre-empts anyway (a commit re-plans for every follower).
      :deferred ->
        release_pin(pin)

        :telemetry.execute(
          [:fathom, :replication, :checkpoint_deferred],
          %{count: 1},
          %{source: :threshold}
        )

        arm_catchup(state, [])

      verdict ->
        spawn_checkpoint(state, pin, wal_path, verdict)
        state
    end
  end

  defp checkpoint_verdict(%{last_header: %{commit_extent: e} = header} = state) do
    cond do
      covered?(state, header) ->
        if e < laggard_wait_bytes() and laggards(state, header) != [],
          do: :deferred,
          else: :covered

      e >= checkpoint_force_bytes() ->
        Logger.warning(
          "shard #{state.shard_id}: WAL at #{e} bytes with frames no quorum holds; " <>
            "checkpointing anyway, followers missing them will be re-seeded"
        )

        :forced

      true ->
        :skip
    end
  end

  defp checkpoint_verdict(_state), do: :skip

  # WHY THE TASK CHECKS THE SHARD OUT FIRST. Opening, and above all CLOSING, a connection on the
  # shard's files is only safe while the coordinator cannot be dropping them: the close of the last
  # connection runs SQLite's close-time checkpoint and unlinks `-wal` BY PATH, which after a drop and
  # a reopen would be a different coordinator's WAL. Inline, the committer's own checkout covered
  # that. Off the reply path the committer may check in at once, so the task takes a checkout grant
  # of its own (the coordinator monitors it, so a crashed task releases it) BEFORE this Session
  # replies, and holds it until the pin is released. The handshake costs the committer one
  # coordinator round trip, on the rare commit that starts a checkpoint; if the grant cannot be had
  # within `@checkpoint_handshake_ms` the checkpoint is abandoned and the next due commit retries.
  @checkpoint_handshake_ms 1_000

  defp spawn_checkpoint(state, pin, wal_path, verdict) do
    parent = self()
    tag = make_ref()
    coordinator = state.coordinator
    shard_id = state.shard_id

    {pid, mref} =
      spawn_monitor(fn ->
        case safe_checkout(coordinator) do
          {:ok, ref} ->
            send(parent, {tag, :held})

            result =
              try do
                # TEST seam: a 0-arity fun run inside the task before the checkpoint, so
                # `replication_unshipped_checkpoint_test.exs` can hold a checkpoint open and show
                # the commit's reply (and the next commit's ship) do not wait for it. Unset in prod.
                if hook = Application.get_env(:fathom, :replication_checkpoint_hook), do: hook.()
                result = checkpoint(wal_path)
                # rv6 F1: an IOERR here may have consumed a deferred commit's write-back error.
                if WalSync.io_error?(result), do: send(parent, {:checkpoint_io_error, result})
                result
              after
                release_pin(pin)
                Fathom.Shard.checkin(coordinator, ref)
              end

            # AFTER the pin is released, not inside the try: the pin is an open read snapshot, and
            # while it is held the next writer cannot restart the log. Emitted before the release,
            # the event told a listener "checkpointed" while a write made right then still appended
            # to the old log (replication_unshipped_checkpoint_test flaked on exactly that under
            # full-suite load, seed 527250, 2026-10-19).
            :telemetry.execute(
              [:fathom, :replication, :checkpoint],
              %{count: 1},
              %{shard_id: shard_id, verdict: verdict, result: result}
            )

          _ ->
            release_pin(pin)
            send(parent, {tag, :refused})
        end
      end)

    # A task that died or was killed before the handshake never released the pin, and leaving it to
    # GC would close it at an arbitrary later moment — possibly after the committer checked in, the
    # unsafe close described above. Release it here, while the committer's grant still stands.
    receive do
      {^tag, _} ->
        :ok

      {:DOWN, ^mref, :process, ^pid, _} ->
        quietly_release(pin)
    after
      @checkpoint_handshake_ms ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^mref, :process, ^pid, _} -> :ok
        end

        quietly_release(pin)
    end

    Process.demonitor(mref, [:flush])
  end

  defp quietly_release(pin) do
    release_pin(pin)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  # The cap-EXEMPT internal grant (expert review 2026-10-20 #13): a tenant at
  # `:max_checkouts_per_shard` used to refuse this task its grant, abandoning the checkpoint until it
  # ran FORCED at the WAL cap and re-seeded every lagging follower.
  defp safe_checkout(coordinator) do
    case Fathom.Shard.checkout_internal(coordinator) do
      {:ok, ref, _path} -> {:ok, ref}
      other -> other
    end
  catch
    :exit, reason -> {:error, reason}
  end

  # The checkpoint runs on a connection of its own: SQLite refuses a checkpoint on a connection
  # with an open transaction, and the pin is one.
  defp checkpoint(wal_path) do
    with {:ok, conn} <- Connection.open(String.replace_suffix(wal_path, "-wal", "")) do
      try do
        Connection.query(conn, "PRAGMA wal_checkpoint(PASSIVE)", [])
      after
        Connection.close(conn)
      end
    end
  end

  # Does a write quorum of followers hold everything up to `header`'s commit extent, in its
  # generation? Read from the per-follower ack positions, so it means exactly "acked".
  defp covered?(_state, :empty), do: true

  defp covered?(state, %{ckpt_seq: gen, salt1: salt, commit_extent: extent}) do
    held =
      for %{wal_gen: ^gen, salt1: ^salt, offset: o} <- Map.values(state.followers),
          is_integer(o),
          do: o

    case Enum.at(Enum.sort(held, :desc), quorum() - 1) do
      nil -> false
      o -> o >= extent
    end
  end

  # THE LAGGARDS OF A GENERATION (expert review 2026-10-20 #7/#8): followers that a restart of
  # `header`'s WAL would leave SHORT — and therefore re-seed, a full db+wal transfer, at the next
  # reset (`prev_extent` → `Follower.short_reset?/3`). Q-coverage alone leaves up to N−Q of them.
  #
  # A follower counts only if it is CONNECTED in the sense this Session can see without a call: it
  # holds a position IN THIS VERY GENERATION (it acked something since the last restart). One that
  # is down, or never shipped, sits in an older generation or nowhere, is re-seeded whatever we do,
  # and must not hold the WAL hostage — it drops out at the next restart instead of every one.
  # Seeding followers are excluded for the same reason (the seed carries everything up to its start).
  #
  # A push already IN FLIGHT to it counts as sent: the reset that follows travels the same shipper
  # behind it, so the follower holds those bytes by the time it judges the reset. What makes a
  # follower short is bytes never SENT — a push skipped `:already_in_flight` or refused locally.
  defp laggards(state, %{ckpt_seq: gen, salt1: salt, commit_extent: extent}) do
    for {shipper, %{wal_gen: ^gen, salt1: ^salt, offset: o}} <- state.followers,
        is_integer(o),
        not Map.has_key?(state.seeding, shipper),
        sent_through(state, shipper, gen, salt, o) < extent,
        do: shipper
  end

  defp laggards(_state, _empty), do: []

  defp sent_through(state, shipper, gen, salt, acked) do
    case Map.get(state.inflight, shipper) do
      %{wal_gen: ^gen, salt1: ^salt, offset: o} when is_integer(o) -> max(o, acked)
      _ -> acked
    end
  end

  # #8: how far a Q-covered WAL may grow past the threshold while a connected laggard is still owed
  # frames, before the checkpoint stops waiting for it and falls back to Q-coverage alone. 2x the
  # threshold: one `checkpoint_step_bytes/0` deferral at a time (the `ckpt_at` rate limit), and
  # `checkpoint_force_bytes/0` (4x) still bounds the WAL.
  defp laggard_wait_bytes, do: 2 * checkpoint_bytes()

  # The covered-idle-close verdict (#7, `request_idle_close/2`). `:ready` when the close-time
  # checkpoint would fold nothing a connected follower lacks: a quorum holds the WAL and no connected
  # follower is owed frames. Otherwise ship first — through the catch-up path, so every ordering rule
  # of the commit path applies (drain late replies, reconcile, seed) — and judge again on the header
  # that ship read. Still owed after that: `:wait`, with the catch-up armed; the coordinator re-asks,
  # and its bound decides when to drain regardless.
  #
  # ONE ROUND, NO WAITING (expert review 2026-10-20-2027 #15). The ship used to be a full catch-up
  # cycle: up to `catchup_ms` (1 s) of rounds with `settle_inflight/3` between them, waiting on a
  # far-behind or slow follower's acks — inside the Session, ~250 ms after the last checkin, which is
  # exactly when a bursty tenant (Django CONN_MAX_AGE=0) opens its next connection and commits; that
  # commit queued behind the whole catch-up. Now the ship runs with a deadline of NOW: one round,
  # sent fire-and-forget when the quorum already holds (`deliver/7`, `needed == 0`), and nothing
  # sent if it does not (a refusal, never an ack — no tenant is waiting on this cycle). The bytes a
  # laggard still needs after that are the re-ask's (each ask is one more non-blocking round) and the
  # catch-up timer's, off this path.
  #
  # Durability: unchanged. The verdict is still computed from a header read AFTER the ask arrived
  # (the property the coordinator's bounded fallback rests on), and `:ready` still requires
  # `nothing_owed?/2`: a quorum ACKED the extent and every connected follower was SENT it. A push
  # sent in this round counts as sent, by the same argument `laggards/2` makes: the reset that
  # follows the close travels the same shipper behind it, and a push that then fails leaves that
  # follower short at the reset — re-seeded, never laundered.
  defp idle_close_verdict(%{wal_path: nil} = state), do: {:ready, state}

  defp idle_close_verdict(state) do
    case Wal.read_held(state.wal_fd, state.wal_path) do
      {:ok, :empty, fd} ->
        {:ready, %{state | wal_fd: fd}}

      {:ok, header, fd} ->
        state = note_header(%{state | wal_fd: fd}, header)

        if nothing_owed?(state, header) do
          {:ready, state}
        else
          state = internal_cycle(state, :idle_close, now_ms())

          header = state.last_header || header

          if nothing_owed?(state, header),
            do: {:ready, state},
            else: {:wait, arm_catchup(state, [])}
        end

      # `:retry`, not `:wait`: nothing was read, so this is not the answer the coordinator's bounded
      # fallback needs (round-4 replication review 2026-10-20; `Fathom.Shard.covered_idle_drain/1`).
      {:error, _reason, fd} ->
        {:retry, %{state | wal_fd: fd}}
    end
  end

  defp nothing_owed?(state, header), do: covered?(state, header) and laggards(state, header) == []

  # ------------------------------------------------------------------------------------------
  # seeding
  # ------------------------------------------------------------------------------------------

  # Replies that arrived AFTER the quorum was decided.
  #
  # `ship_quorum/4` returns the moment the outcome is known — at Q acks, or as soon as too few
  # followers remain — and that early return is the entire measured value of a quorum. The cost is
  # that the remaining followers answer into this mailbox afterwards, and those answers matter for
  # two reasons:
  #
  #   1. **They are the only way a straggler gets seeded.** With N=3 and Q=2, a failing commit
  #      reports `:impossible` after just two rejects, so a third unseeded follower is never in the
  #      reject list. Once the other two are seeded the quorum then SUCCEEDS at two acks, and that
  #      follower's `:unknown_shard` is never observed again — it stays permanently un-seeded while
  #      the shard reports a healthy quorum. Silent under-replication, found by the seeding test.
  #   2. **They would otherwise accumulate here forever**, one per straggler per commit, for the
  #      life of the shard.
  #
  # Non-blocking (`after 0`): this runs on the commit path and must never wait for a follower that
  # the quorum already decided not to wait for.
  # WAIT FOR THE PREVIOUS COMMIT'S STRAGGLERS WHEN THEY WOULD COST THIS ONE ITS QUORUM (2026-10-02).
  #
  # `ship_quorum/4` returns at the Q-th ack, so the previous commit's other followers are usually
  # still mid-flight when a tenant's next statement arrives — and a `Shipper` holds ONE waiter per
  # shard, so pushing to them now is refused `:already_in_flight`. With N=4/Q=2 that is harmless
  # while two followers are free; but add one seeding, torn or overloaded follower and the commit
  # fails `{:no_quorum, :impossible}` although every follower would have answered milliseconds
  # later. On the chaos rig at 1024 tenants that was the single largest quorum-failure reason.
  #
  # So only when the followers that are FREE (no outstanding push, no seed in flight) number fewer
  # than the quorum, wait for the outstanding replies first, bounded by this commit's own deadline.
  # In steady state two followers are always free and this costs nothing; it trades a few ms of
  # latency for a commit that would otherwise have failed outright.
  #
  # It waits only until ENOUGH are free, not for all of them: a dead follower with an outstanding
  # push must not make every commit sit out its whole deadline.
  defp await_stragglers_if_short(state, wal_path, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if map_size(state.inflight) == 0 or free_followers(state) >= quorum() or remaining <= 0 do
      state
    else
      refs = seed_refs(state)

      receive do
        {:repl_reply, from, {:ack, _shard, next}} ->
          state
          |> settle_late_ack(from, next)
          |> forget_inflight(from)
          |> await_stragglers_if_short(wal_path, deadline)

        {:repl_reply, from, {:reject, _shard, reason, follower_offset}} ->
          rejects = [{from, reason, follower_offset}]

          state
          |> reconcile(rejects)
          |> start_seeds(wal_path, rejects)
          |> await_stragglers_if_short(wal_path, deadline)

        {:seeded, shipper, token, result} ->
          state
          |> apply_seed_result(shipper, token, result)
          |> await_stragglers_if_short(wal_path, deadline)

        {:DOWN, ref, :process, _pid, reason} when is_map_key(refs, ref) ->
          state
          |> apply_seed_down(ref, reason)
          |> await_stragglers_if_short(wal_path, deadline)
      after
        remaining -> state
      end
    end
  end

  defp free_followers(state) do
    Enum.count(shippers(), fn s ->
      not Map.has_key?(state.inflight, s) and not Map.has_key?(state.seeding, s)
    end)
  end

  defp drain_late_replies(state, wal_path) do
    receive do
      # A late ACK is not noise to be thrown away — it is a follower reporting that it holds the
      # bytes we sent it, and the ONLY notification we will ever get. Discarding it left that
      # follower pinned at its previous position, so the next commit planned a delta it had already
      # applied; it refused, and the file diverged from the primary's for a whole round trip while
      # the quorum reported healthy. That is what broke byte-identity on the second commit.
      {:repl_reply, from, {:ack, _shard, next}} ->
        state
        |> settle_late_ack(from, next)
        |> forget_inflight(from)
        |> drain_late_replies(wal_path)

      {:repl_reply, from, {:reject, _shard, reason, follower_offset}} ->
        rejects = [{from, reason, follower_offset}]

        state
        |> reconcile(rejects)
        |> start_seeds(wal_path, rejects)
        |> drain_late_replies(wal_path)

      {:repl_reply, _from, _other} ->
        drain_late_replies(state, wal_path)
    after
      0 -> state
    end
  end

  # Wait until no follower has an outstanding push, or `deadline` passes.
  #
  # Only used BETWEEN catch-up rounds — see the call site in `ship/5` for why the quorum's early
  # return is the wrong stopping point there. `state.inflight` is already exactly "who we are
  # waiting on an answer from": `deliver/5` records an entry per push, `advance/2` removes it on an
  # ack, and `reconcile/2` removes it on any refusal. So "settled" is simply an empty
  # map, and a follower whose socket has dropped does not pin this — `Shipper.drop/2` fails its
  # waiter with `:disconnected` immediately, which is an answer.
  #
  # Same `receive`-inside-the-call shape as `await_seeds/3`, and for the same reason: nothing else
  # is reading this mailbox during a `handle_call`, so a reply left queued would be applied only
  # after the reply had already gone out.
  defp settle_inflight(state, wal_path, deadline) do
    if map_size(state.inflight) == 0 do
      state
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        state
      else
        refs = seed_refs(state)

        receive do
          {:repl_reply, from, {:ack, _shard, next}} ->
            state
            |> settle_late_ack(from, next)
            |> forget_inflight(from)
            |> settle_inflight(wal_path, deadline)

          {:repl_reply, from, {:reject, _shard, reason, follower_offset}} ->
            rejects = [{from, reason, follower_offset}]

            # No `forget_inflight/2` here: `reconcile/2` now clears the entry for every reject
            # reason, which is what makes this loop terminate on a follower that refused.
            state
            |> reconcile(rejects)
            |> start_seeds(wal_path, rejects)
            |> settle_inflight(wal_path, deadline)

          {:seeded, shipper, token, result} ->
            state
            |> apply_seed_result(shipper, token, result)
            |> settle_inflight(wal_path, deadline)

          {:DOWN, ref, :process, _pid, reason} when is_map_key(refs, ref) ->
            state
            |> apply_seed_down(ref, reason)
            |> settle_inflight(wal_path, deadline)
        after
          remaining -> state
        end
      end
    end
  end

  # An ACK that `settle_late_ack/3` declined to act on still ends the wait. It declines when the
  # acked position is not the one we recorded — believing it would advance the primary past bytes
  # the follower does not hold — but the follower HAS answered, so the expectation must go either
  # way. Refusals are handled by `reconcile/2`, which owns clearing for every reject reason.
  defp forget_inflight(state, from),
    do: %{state | inflight: Map.delete(state.inflight, from)}

  # Unlike the in-quorum path, nothing has vetted this ack yet: `collect/4` compares against what it
  # sent, and it is long gone. So the position is checked against the outstanding expectation here.
  # An ack for anywhere else means the two sides disagree, and believing it would advance the
  # primary past bytes the follower does not hold.
  defp settle_late_ack(state, shipper, next) do
    case Map.get(state.inflight, shipper) do
      %{offset: ^next} -> advance(state, shipper)
      _ -> state
    end
  end

  # Wait for the seeds THIS commit started, until `deadline`. Returns `{:ok, state}` once none are
  # outstanding, `{:timeout, state}` otherwise — in both cases with every result that did arrive
  # applied, so a partial wait still advances the followers it heard about.
  #
  # Consumed with `receive` rather than left to `handle_info/2` for the same reason
  # `drain_late_replies/2` does it: we are inside the `handle_call`, so nothing else is reading this
  # mailbox, and a `{:seeded, ...}` left sitting would be applied only after the reply had gone.
  #
  # `{:repl_reply, ...}` is drained alongside it. A straggler's answer to the push that just failed
  # arrives during exactly this window, and leaving it queued would let the retry's `collect/4`
  # mistake it for an answer to the retry.
  #
  # `prior` is the set of seed tokens already in flight when the call began (expert review
  # 2026-10-20 #6): those were started by an earlier call and are NOT waited on. A seed this call
  # started is skipped too when the Session's last observed seed rate says it cannot land inside
  # the time left — the wait would only burn the deadline. Either way the result still arrives in
  # `handle_info/2` and is applied there.
  defp await_seeds(state, deadline, wal_path, prior) do
    remaining = deadline - System.monotonic_time(:millisecond)

    case own_seeds(state, prior) do
      [] ->
        {:ok, state}

      _own when remaining <= 0 ->
        {:timeout, state}

      own ->
        if hopeless_seed?(state, own, remaining),
          do: {:timeout, state},
          else: await_seed_messages(state, deadline, wal_path, prior, remaining)
    end
  end

  defp seed_tokens(state), do: MapSet.new(state.seeding, fn {_shipper, e} -> e.token end)

  defp own_seeds(state, prior),
    do: for({_shipper, e} <- state.seeding, not MapSet.member?(prior, e.token), do: e)

  # Seeds to different followers stream concurrently, so the wait ends when the LAST one lands:
  # hopeless when any of them, at the last observed rate, finishes past the time left. No observed
  # rate yet (this Session's first seed) waits, as before. Underestimates the rate if anything (it
  # is measured start-to-result, queueing included), which errs toward not waiting.
  defp hopeless_seed?(%{seed_rate: rate}, own, remaining) when is_number(rate) and rate > 0 do
    now = System.monotonic_time(:millisecond)

    Enum.any?(own, fn
      %{bytes: b, started_at: t} when is_integer(b) -> t + b / rate - now > remaining
      _ -> false
    end)
  end

  defp hopeless_seed?(_state, _own, _remaining), do: false

  defp await_seed_messages(state, deadline, wal_path, prior, remaining) do
    refs = seed_refs(state)

    receive do
      {:seeded, shipper, token, result} ->
        state
        |> apply_seed_result(shipper, token, result)
        |> await_seeds(deadline, wal_path, prior)

      # A seed task that died WITHOUT answering (expert review 2026-10-13 #12). Only
      # `handle_info/2` used to consume its DOWN, which cannot run inside this call, so the
      # entry stayed in `seeding` and this loop ran to the commit deadline.
      {:DOWN, ref, :process, _pid, reason} when is_map_key(refs, ref) ->
        state
        |> apply_seed_down(ref, reason)
        |> await_seeds(deadline, wal_path, prior)

      {:repl_reply, from, {:ack, _shard, next}} ->
        state
        |> settle_late_ack(from, next)
        |> await_seeds(deadline, wal_path, prior)

      # A REJECT MUST BE RECONCILED, NOT DROPPED. The first draft of this fell through to the
      # catch-all and threw rejects away, and the rig showed exactly what this module's own
      # moduledoc predicts: the follower stayed pinned at the position we THOUGHT it had, the
      # retry planned a delta from there, it refused, and every subsequent commit failed
      # `:impossible` — having fixed the first commit and broken all the rest.
      #
      # These are answers to the push that just failed, so reconciling them before the retry is
      # the whole point: the retry plans from where each follower actually is.
      {:repl_reply, from, {:reject, _shard, reason, follower_offset}} ->
        rejects = [{from, reason, follower_offset}]

        state
        |> reconcile(rejects)
        |> start_seeds(wal_path, rejects)
        |> await_seeds(deadline, wal_path, prior)

      {:repl_reply, _from, _other} ->
        await_seeds(state, deadline, wal_path, prior)
    after
      remaining -> {:timeout, state}
    end
  end

  # Re-ship after a seed landed. ONE attempt, never a loop: a follower that rejects a delta built
  # from the position its own seed just reported is not going to accept a third, and retrying would
  # convert a bounded failure into a call that spins until the caller's timeout. `why` is the
  # ORIGINAL failure — reporting the retry's reason would blame the seed for a quorum that was
  # already short.
  defp retry_after_seed(state, wal_path, deadline, why, mode) do
    case ship(state, wal_path, state.epoch, deadline, mode) do
      {:ok, new_state, rejects} ->
        # Same ownership gate as the primary commit path (expert review 2026-09-05 #5): a post-seed
        # quorum is still only durable if this node still holds the lease.
        new_state
        |> start_seeds(wal_path, rejects)
        |> drain_late_replies(wal_path)
        |> ack_if_owned()

      # NOTHING TO SHIP IS NOT PROOF OF OWNERSHIP, here either (expert review 2026-09-29 #7; rig
      # 2026-10-14 B3). This is the branch a write acked through a re-seed takes (the seed already
      # carried its frames), so it must be fenced exactly like the primary path's `:nothing`.
      # NOTHING TO SHIP IS NOT PROOF OF OWNERSHIP, here either (expert review 2026-09-29 #7; rig
      # 2026-10-14 B3). This is the branch a write acked through a re-seed takes (the seed already
      # carried its frames), so it must be fenced exactly like the primary path's `:nothing`.
      {:nothing, state} ->
        ack_if_owned(state)

      {:error, {:no_quorum, _retry_why}, new_state, rejects} ->
        {:reply, {:error, {:no_quorum, why}}, start_seeds(new_state, wal_path, rejects)}

      {:error, reason, failed_state} ->
        {:reply, {:error, reason}, failed_state}
    end
  end

  # The bookkeeping `handle_info({:seeded, ...})` does, factored out so the in-call wait applies a
  # seed result identically. Divergence between the two would be invisible until a seed happened to
  # land on the path with the older logic.
  #
  # A result carries the TOKEN of the seed that produced it (expert review 2026-10-13 #35). One
  # already in the mailbox when `expire_seeds/1` killed its task and a replacement started would
  # otherwise be applied to the REPLACEMENT's entry (`forget_seed/2` is keyed by shipper only),
  # untracking a live seed and recording a position the replacement is about to overwrite. A result
  # whose token is not the current entry's is dropped.
  defp apply_seed_result(state, shipper, token, result) do
    case state.seeding do
      %{^shipper => %{token: ^token}} ->
        apply_current_seed_result(state, shipper, result)

      _ ->
        state
    end
  end

  defp apply_current_seed_result(state, shipper, result) do
    state = note_seed_rate(state, Map.get(state.seeding, shipper), result)
    state = %{state | seeding: forget_seed(state.seeding, shipper)}

    case result do
      {:ok, at} ->
        Logger.debug("replication seeded a follower for #{state.shard_id} at #{inspect(at)}")
        key = shipper

        %{
          state
          | followers: Map.put(state.followers, key, at),
            inflight: Map.delete(state.inflight, key)
        }

      {:error, r} ->
        Logger.warning("replication seed failed for #{state.shard_id}: #{inspect(r)}")
        state
    end
  end

  # The last completed seed's throughput, bytes per ms (expert review 2026-10-20 #6), for
  # `hopeless_seed?/3`. Wall time from start to result as this Session saw it.
  defp note_seed_rate(state, %{bytes: b, started_at: t}, {:ok, _}) when is_integer(b) and b > 0,
    do: %{state | seed_rate: b / max(System.monotonic_time(:millisecond) - t, 1)}

  defp note_seed_rate(state, _entry, _result), do: state

  defp start_seeds(state, wal_path, rejects) do
    # EXPIRE FIRST. A monitor covers a task that DIED; it does not cover one that is alive and
    # wedged — `do_seed/5` streams over a socket, and nothing below it is guaranteed to return.
    # Sweeping here rather than on a timer is deliberate: this is the one moment the stale entry
    # does damage (it refuses a seed the follower is asking for), so it is the one moment worth
    # checking. The expiry is well past `await_seed_reply`'s own 30 s wait, so a seed that
    # is merely slow is never cut off by it.
    state = expire_seeds(state)

    needs =
      for {shipper, :unknown_shard, _at} <- rejects,
          not Map.has_key?(state.seeding, shipper),
          do: shipper

    session = self()
    db_path = String.replace_suffix(wal_path, "-wal", "")

    # ONE pinned snapshot for the whole batch (perf review 2026-10-14 #12(a)): a reopened shard
    # re-seeds every follower at once, and each task used to open and pin its own identical
    # snapshot. See `SeedPin` for why sharing it keeps the pin-before-`Wal.read` ordering.
    pin =
      case needs do
        [] ->
          nil

        _ ->
          {:ok, pin} = SeedPin.start(session, db_path)
          pin
      end

    # What the batch is about to stream (one stat pair per batch, not per follower), recorded per
    # entry so `hopeless_seed?/3` can price the wait (expert review 2026-10-20 #6). A seed is rare.
    bytes = if needs == [], do: 0, else: seed_bytes(db_path, wal_path)

    seeded =
      Enum.reduce(needs, state, fn shipper, acc ->
        # `spawn_monitor`, not `Task.start`: monitoring has to be ATOMIC with the spawn, or a task
        # that dies in the gap leaves an entry nothing will ever remove — which is the bug being
        # fixed, reintroduced in a smaller window. Not linked: a raise inside `do_seed/5` must not
        # take down the Session, which holds the shard's serialization point. The `terminate/2`
        # sweep below is what covers the other half the finding asked for (a stopped Session must
        # not leave a task streaming a whole tenant database for a shard this node no longer owns).
        token = make_ref()

        {pid, ref} =
          spawn_monitor(fn ->
            send(
              session,
              {:seeded, shipper, token,
               gated_seed(
                 pin,
                 shipper,
                 acc.shard_id,
                 db_path,
                 wal_path,
                 acc.epoch,
                 acc.lineage,
                 {acc.wal_salt, acc.wal_ordinal}
               )}
            )
          end)

        watch_session(session, pid)

        entry = %{
          ref: ref,
          token: token,
          pid: pid,
          started_at: System.monotonic_time(:millisecond),
          bytes: bytes
        }

        %{acc | seeding: Map.put(acc.seeding, shipper, entry)}
      end)

    if pin do
      SeedPin.expect(pin, for(shipper <- needs, do: seeded.seeding[shipper].pid))
    end

    seeded
  end

  # A seed task must not outlive its Session (expert review 2026-10-12 #26). The `terminate/2`
  # sweep does not run when the Session is killed or shut down by its supervisor (it traps no
  # exits), which orphaned a task streaming a whole tenant database. The task is deliberately
  # unlinked, so this watcher monitors the Session and kills the task on its DOWN, whatever the
  # reason; it also ends when the task does. (The task cannot watch for itself: it spends its
  # life blocked inside `do_seed/5`.)
  defp watch_session(session, task) do
    spawn(fn ->
      session_ref = Process.monitor(session)
      task_ref = Process.monitor(task)

      receive do
        {:DOWN, ^session_ref, :process, _, _} -> Process.exit(task, :kill)
        {:DOWN, ^task_ref, :process, _, _} -> :ok
      end
    end)
  end

  # Monitor refs of the seeds in flight, as a map so a `receive` guard can test membership
  # (`is_map_key/2`): the in-call loops must consume ONLY a seed task's DOWN, never the
  # coordinator's, which `handle_info/2` needs in order to stop the Session.
  defp seed_refs(state), do: Map.new(state.seeding, fn {_shipper, e} -> {e.ref, true} end)

  # A seed task died without answering (expert review 2026-08-20 #26): forget its entry so the
  # follower can be seeded again. Shared by `handle_info/2` and the in-call receive loops.
  defp apply_seed_down(state, ref, reason) do
    case Enum.find(state.seeding, fn {_shipper, e} -> e.ref == ref end) do
      nil ->
        state

      {shipper, _entry} ->
        Logger.warning(
          "replication seed task for #{state.shard_id} died without answering " <>
            "(#{inspect(reason)}); the follower may ask again"
        )

        %{state | seeding: forget_seed(state.seeding, shipper)}
    end
  end

  # Drop one shipper's entry and stop watching its task. `demonitor(flush: true)` removes any DOWN
  # already sitting in the mailbox, so a completed seed never also arrives as a phantom failure.
  defp forget_seed(seeding, shipper) do
    case Map.pop(seeding, shipper) do
      {nil, rest} ->
        rest

      {%{ref: ref}, rest} ->
        Process.demonitor(ref, [:flush])
        rest
    end
  end

  defp seed_expiry_ms,
    do: Application.get_env(:fathom, :replication_seed_expiry_ms, @default_seed_expiry_ms)

  defp expire_seeds(%{seeding: seeding} = state) when map_size(seeding) == 0, do: state

  defp expire_seeds(state) do
    now = System.monotonic_time(:millisecond)

    Enum.reduce(state.seeding, state, fn {shipper, entry}, acc ->
      if now - entry.started_at >= seed_expiry_ms() do
        Logger.warning(
          "replication seed for #{acc.shard_id} to #{inspect(shipper)} expired after " <>
            "#{now - entry.started_at} ms; killing it and allowing a fresh one"
        )

        Process.exit(entry.pid, :kill)
        %{acc | seeding: forget_seed(acc.seeding, shipper)}
      else
        acc
      end
    end)
  end

  # A node-wide slot first (expert review 2026-10-01 #6): a failover burst starts one seed task per
  # shard x follower at once, and each holds its follower's single link for the whole transfer. The
  # wait is bounded below `seed_expiry_ms/0`, so a seed that never got a slot gives up on its own
  # rather than being killed as wedged. A killed task's slot is freed by SeedGate's monitor.
  defp gated_seed(pin, shipper, shard_id, db_path, wal_path, epoch, lineage, known) do
    case SeedGate.acquire(seed_gate_wait_ms()) do
      :ok ->
        try do
          timed_seed(pin, shipper, shard_id, db_path, wal_path, epoch, lineage, known)
        after
          SeedGate.release()
        end

      {:error, _} = error ->
        Logger.warning("replication seed for #{shard_id} found no free seed slot; will retry")
        error
    end
  end

  defp seed_gate_wait_ms,
    do: Application.get_env(:fathom, :replication_seed_gate_wait_ms, 60_000)

  # `do_seed/6` with its cost reported. A seed is the one replication operation whose cost scales
  # with DATABASE size rather than write volume, and every rule that would seed more often (expert
  # review 2026-09-29 #1: seed on ownership change) is priced by exactly two numbers — bytes and
  # wall time per seed. Neither was observable before: the only trace was a log line with the
  # resulting position. `bytes` is what was DECLARED (db + wal at seed_begin), measured before the
  # stream so a failed seed still reports what it was attempting.
  defp timed_seed(pin, shipper, shard_id, db_path, wal_path, epoch, lineage, known) do
    bytes = seed_bytes(db_path, wal_path)
    started = System.monotonic_time()

    result =
      shared_pin(pin, fn ->
        stream_seed(shipper, shard_id, db_path, wal_path, epoch, lineage, known)
      end)

    duration = System.monotonic_time() - started
    ms = System.convert_time_unit(duration, :native, :millisecond)

    :telemetry.execute(
      [:fathom, :replication, :seed],
      %{duration: duration, bytes: bytes},
      %{shard_id: shard_id, result: seed_outcome(result)}
    )

    Logger.debug(
      "replication seed #{seed_outcome(result)} for #{shard_id} to #{inspect(shipper)}: " <>
        "bytes=#{bytes} ms=#{ms}"
    )

    result
  end

  defp seed_outcome({:ok, _}), do: :ok
  defp seed_outcome({:error, _}), do: :error

  # db + the WAL's COMMIT EXTENT — what `stream_seed/7` actually sends (`wal_size_of/1`) — not the
  # `-wal` FILE size (expert review 2026-10-20 #8). SQLite restarts a WAL in place and keeps its
  # high-water mark, so on a hot shard the file sits near the 4000-frame threshold (~16.5 MB) while
  # the extent after a restart is a few hundred KB: the rig's "14 seeds of 16.5 MB" (2026-10-19,
  # pool-fleet reuse) were ~0.6 MB seeds reported at the file size, the follower's own
  # "seeded … 20480B db + 618032B wal" lines show the real sizes. Probe:
  # replication_seed_bytes_test.exs.
  @doc false
  @spec seed_bytes(Path.t(), Path.t()) :: non_neg_integer()
  def seed_bytes(db_path, wal_path) do
    db =
      case Fathom.FS.stat(db_path) do
        {:ok, %{size: size}} -> size
        _ -> 0
      end

    wal =
      case Wal.read(wal_path) do
        {:ok, header} -> wal_size_of(header)
        _ -> 0
      end

    db + wal
  end

  # Stream a CONSISTENT base copy.
  #
  # In WAL mode the `.db` file is only rewritten by a checkpoint, so reading it alongside the `-wal`
  # is safe as long as no checkpoint intervenes. The generation is read before AND after: if it
  # moved, a checkpoint rebuilt the `.db` under us and the two halves are from different points in
  # time — which would hand the follower a database and a WAL whose salts do not match. Retrying is
  # correct and cheap; shipping the inconsistent pair is neither.
  #
  # THE CHECK ABOVE DOES NOT SEE EVERY CHECKPOINT, SO A READ TRANSACTION PINS THE `.db` (expert
  # review 2026-10-08 #12, measured). Only a checkpoint that RESTARTS the log moves the generation. A
  # PASSIVE checkpoint — the commit hook's at 4000 pages, or a tenant's own `PRAGMA wal_checkpoint` —
  # backfills pages into the `.db` and leaves the WAL header byte-identical (same ckpt_seq, salt and
  # size: probed). Unpinned, the `.db` stream carried pages from frames committed AFTER `wal_size`
  # was read, and the follower got a base newer in places than the WAL prefix it was told to replay:
  # an inconsistent replica, promotable if the primary died after its first push.
  #
  # `with_pinned_snapshot/2` opens a read connection on the `.db` and takes a snapshot (BEGIN + one
  # read) BEFORE anything else is read, and holds it until the stream is over. SQLite never backfills
  # a frame past an active reader's mark (probed 2026-10-09: with a reader at mark 28 of a 58-frame
  # WAL, PASSIVE returned `checkpointed = 28`; with a reader at mark 0 — WAL fully backfilled when it
  # began — PASSIVE backfilled nothing and the `.db` bytes did not change). So every page that lands
  # in the `.db` while we stream comes from a frame at or below the reader's mark M.
  #
  # ORDERING IS THE WHOLE ARGUMENT. Snapshot first, THEN `Wal.read`: the extent E read afterwards
  # can only be >= M, because committed frames of a generation are only ever appended. So every page
  # in the shipped `.db` is either older than E or comes from a frame inside the shipped prefix 0..E,
  # which the follower replays over it with identical bytes. Read the header first and the snapshot
  # second and a commit landing between the two puts M past E — the original gap, narrowed, not
  # closed. The one case the reader does NOT block is a WAL restart while it holds mark 0 (restart
  # needs only marks 1..N free); that moves `ckpt_seq`/salt, so `stable?/2` still catches it — and a
  # restart before our `Wal.read` is harmless anyway, because mark 0 blocks every backfill and the
  # new generation's frames are exactly what the prefix ships.
  #
  # THE COST: while a seed streams, nothing past M reaches the `.db`, and a TRUNCATE checkpoint on
  # this shard cannot complete — the durability flush gets `busy` and falls through to its `VACUUM
  # INTO` snapshot path (`Fathom.Shard.checkpoint_and_verify/1`), and the WAL grows for the
  # duration of the seed. Bounded by the seed itself: the read is released on every exit, abort and
  # raise included (try/after), and a seed is gated by `SeedGate` and its own timeouts.
  #
  # Streaming widens that window (the copy now takes as long as the transfer rather than as long as
  # a `File.read`), which is exactly why the check moved to AFTER the last chunk and gained an
  # explicit `seed_abort`: the follower must be told to drop what it has, and the sender must not
  # then sit on its own timeout.
  #
  # Nothing here holds the database in memory — `:file.pread` walks it a chunk at a time. That is
  # the point of the change: the old path did `Fathom.FS.read(db_path)`, so a 2 GB tenant was 2 GB
  # resident on the primary and again on the follower.
  #
  # Public (`@doc false`) only so the 2026-10-08 #12 regression test can drive a seed against a fake
  # shipper that checkpoints mid-stream; production reaches it through `gated_seed/6`.
  @doc false
  @spec do_seed(GenServer.server(), String.t(), Path.t(), Path.t(), integer(), integer()) ::
          {:ok, map()} | {:error, term()}
  def do_seed(shipper, shard_id, db_path, wal_path, epoch, lineage) do
    with_pinned_snapshot(db_path, fn ->
      stream_seed(shipper, shard_id, db_path, wal_path, epoch, lineage, nil)
    end)
  end

  # The batch's shared pin (`SeedPin`) held for the duration of `fun`; attached AFTER the seed gate,
  # so a seed waiting for a slot never holds a snapshot.
  defp shared_pin(pin, fun) do
    case SeedPin.attach(pin) do
      :ok ->
        try do
          fun.()
        after
          # The pin process may already be gone (Session died); the claim is then moot.
          try do
            SeedPin.detach(pin)
          catch
            :exit, _ -> :ok
          end
        end

      {:error, _} = err ->
        err
    end
  end

  # Holds a read transaction on `db_path` for the duration of `fun` — see the note above
  # `do_seed/6`. `file_size/1` first because a `:ro` `Connection.open/2` materializes an ABSENT
  # file, and a seed of a shard with no `.db` must fail as it always did, not ship an empty database.
  # A trusted handle (no `tenant?`): no authorizer, no statement deadline.
  defp with_pinned_snapshot(db_path, fun) do
    with {:ok, _} <- file_size(db_path),
         {:ok, conn} <- Connection.open(db_path, scope: :ro) do
      try do
        case pin_snapshot(conn) do
          :ok -> fun.()
          {:error, reason} -> {:error, {:seed_snapshot_failed, reason}}
        end
      after
        # `conn` used here is also what keeps the handle reachable for the whole stream: a NIF
        # resource nothing references is finalized at the next GC, which releases the snapshot
        # mid-stream (probed: with these two lines dropped, the snapshot regression test in
        # seed_pinned_snapshot_test.exs failed in 1 run of 4).
        _ = Connection.rollback_if_open(conn)
        Connection.close(conn)
      end
    end
  end

  # BEGIN alone takes no snapshot (it is DEFERRED); the read is what acquires the WAL read mark.
  defp pin_snapshot(conn) do
    with :ok <- Connection.exec(conn, "BEGIN") do
      Connection.exec(conn, "SELECT 1 FROM sqlite_schema LIMIT 1")
    end
  end

  defp stream_seed(shipper, shard_id, db_path, wal_path, epoch, lineage, known) do
    # `index: true` (expert review 2026-10-20 #1): the extent comes from the wal-index `mxFrame`,
    # which SQLite publishes only after a commit's frames are written. The backward walk could
    # count a concurrent writer's commit HEADER sitting over a stale slot before its page lands,
    # and the seed would stream that torn frame. The index read is still AFTER the snapshot pin,
    # so the ordering argument above is unchanged (published extent at read time >= the mark).
    with {:ok, before} <- Wal.read(wal_path, index: true),
         {:ok, db_size} <- file_size(db_path),
         wal_size = wal_size_of(before),
         ordinal = seed_ordinal(before, known),
         :ok <- open_seed(shipper, shard_id, epoch, before, db_size, wal_size, lineage, ordinal),
         :ok <- stream_part(shipper, shard_id, :db, db_path, db_size),
         :ok <- stream_part(shipper, shard_id, :wal, wal_path, wal_size),
         {:ok, after_} <- Wal.read(wal_path, index: true),
         :ok <- stable?(before, after_) do
      Shipper.seed_end(shipper, shard_id)
      await_seed_reply(shipper, shard_id, before, wal_size)
    else
      {:error, :checkpoint_during_seed} = err ->
        # The halves stopped belonging together mid-stream. Tell the follower to discard, then
        # consume the reply that abort produces so it cannot be mistaken for an answer to a later
        # push on this shard.
        Shipper.seed_abort(shipper, shard_id)
        _ = await_seed_reply(shipper, shard_id, nil, 0)
        err

      other ->
        other
    end
  end

  defp open_seed(shipper, shard_id, epoch, before, db_size, wal_size, lineage, ordinal) do
    Shipper.seed_begin(shipper, %SeedBegin{
      shard_id: shard_id,
      epoch: epoch,
      wal_gen: gen_of(before),
      salt1: salt_of(before),
      wal_offset: wal_size,
      db_size: db_size,
      wal_size: wal_size,
      lineage: lineage,
      wal_ordinal: ordinal
    })
  end

  # THE ORDINAL OF THE WAL THIS SEED SHIPS (rig 2026-10-14 B1), or 0 ("not stated").
  #
  # Without it a seed left the replica at `wal_ordinal: 0`, which `Promote.fresher?/2` refuses to
  # rank — and that is not a corner case: a reopen under a new lineage re-seeds every follower from
  # inside the first write's commit, the seed's `-wal` already holds that write's frames, and the
  # post-seed retry then plans NOTHING for any follower, so no push states the ordinal either. The
  # write was acked on the quorum and every replica of it was unrankable; the survivor cold-opened
  # the older object and the write was gone (`./chaos.sh rpo`, 2026-10-14).
  #
  # `known` is the session's cached `{wal_salt, wal_ordinal}` — the coordinator's number for that
  # salt, the one every push from that WAL carries and the one the object is stamped with. It is
  # used ONLY when the salt the seed actually read matches; otherwise (a WAL recreate between the
  # last push and this read) the answer is 0, the pre-fix "unknown", never a number belonging to a
  # different WAL. Asking the coordinator from here instead is wrong in the other direction: its
  # counter bumps for any salt it is not standing on, so a stale read would MINT an ordinal.
  #
  # No per-commit cost: this runs once per seed, from data already in the session.
  defp seed_ordinal(%{salt1: salt}, {salt, n}) when is_integer(n) and n > 0, do: n
  defp seed_ordinal(_before, _known), do: 0

  # `before` is nil on the ABORT path, which only drains the abort's answer: an ack there is not a
  # completed seed, whatever produced it, and must not crash the task on `gen_of(nil)`.
  defp await_seed_reply(shipper, shard_id, before, wal_size) do
    receive do
      {:repl_reply, ^shipper, {:ack, ^shard_id, _}} when is_nil(before) ->
        {:error, :aborted}

      {:repl_reply, ^shipper, {:ack, ^shard_id, _}} ->
        {:ok, %{wal_gen: gen_of(before), salt1: salt_of(before), offset: wal_size}}

      {:repl_reply, ^shipper, {:reject, ^shard_id, r, _}} ->
        {:error, r}
    after
      30_000 -> {:error, :seed_timeout}
    end
  end

  # Walks the file with `pread` rather than reading it whole. A short read means the file shrank
  # under us (a checkpoint racing the copy); stop rather than send fewer bytes than declared, which
  # the follower would refuse anyway — better to fail here and let the `stable?` check name it.
  defp stream_part(_shipper, _shard_id, _part, _path, 0), do: :ok

  defp stream_part(shipper, shard_id, part, path, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          stream_chunks(shipper, shard_id, part, fd, 0, 0, size)
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stream_chunks(_shipper, _shard_id, _part, _fd, offset, _seq, size) when offset >= size,
    do: :ok

  defp stream_chunks(shipper, shard_id, part, fd, offset, seq, size) do
    len = min(chunk_bytes(), size - offset)

    case :file.pread(fd, offset, len) do
      {:ok, bin} when byte_size(bin) == len ->
        # Blocks until the chunk is on the wire, so a seed holds one chunk in memory, not the
        # whole database (expert review 2026-10-01 #6).
        with :ok <- Shipper.seed_chunk(shipper, shard_id, part, seq, bin) do
          stream_chunks(shipper, shard_id, part, fd, offset + len, seq + 1, size)
        end

      {:ok, _short} ->
        {:error, :short_read}

      :eof ->
        {:error, :short_read}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp file_size(path) do
    case Fathom.FS.stat(path) do
      {:ok, %{size: size}} -> {:ok, size}
      {:error, reason} -> {:error, reason}
    end
  end

  # The COMMITTED EXTENT, not the file size (expert review 2026-10-01 perf #1). A reused WAL keeps
  # its high-water mark, so the bytes past the extent are stale frames from an earlier generation.
  # Seeding them and recording the follower at the file size put it AHEAD of every later commit's
  # extent: each commit planned a reset, the follower (same salt, same generation) answered
  # `:offset_mismatch`, and once the extent finally passed the old mark the primary appended from
  # there, so the frames written in between never reached the replica. Committed frames of the
  # current generation are never rewritten before the next reset, so 0..extent is stable to stream.
  defp wal_size_of(:empty), do: 0
  defp wal_size_of(%{commit_extent: extent}), do: extent

  defp chunk_bytes,
    do: Application.get_env(:fathom, :replication_seed_chunk_bytes, 4 * 1024 * 1024)

  defp gen_of(:empty), do: 0
  defp gen_of(%{ckpt_seq: seq}), do: seq

  defp salt_of(:empty), do: 0
  defp salt_of(%{salt1: s}), do: s

  # `:empty` on both sides is stable (a shard with no WAL yet). Anything else must match on both
  # the generation and the salt, the same pair `Primary.plan/2` corroborates.
  # The reason is a parameter because both callers need the same COMPARISON and different
  # handling: a seed that loses its two halves mid-stream must tell the follower to discard
  # (`seed_abort`), while a push that loses its header just re-plans on the next round. Sharing
  # one reason name would make a push-path failure log as "during seed".
  defp stable?(before, after_, reason \\ :checkpoint_during_seed)
  defp stable?(:empty, :empty, _reason), do: :ok
  defp stable?(%{ckpt_seq: g, salt1: s}, %{ckpt_seq: g, salt1: s}, _reason), do: :ok
  defp stable?(_, _, reason), do: {:error, reason}

  defp shippers, do: Fathom.Shard.Replication.Fleet.shippers()
  defp quorum, do: Application.get_env(:fathom, :replication_quorum, 2)

  # THE MOST WAL ONE PUSH MAY CARRY. This is the bound that breaks the feedback loop; the
  # per-node byte budget in `Fathom.Shard.Replication.Budget` is the safety net underneath it.
  #
  # 1 MiB, chosen so it does not bind in health and does bind exactly where the runaway starts.
  # An ordinary Django transaction touches a few dozen pages — hundreds of KB at most — so the
  # common case still ships in one round and pays nothing for the loop above. The runaway's own
  # numbers sit at and above this line: the failing shipper's MEAN payload was 832 KB climbing to
  # 1,593 KB, with the largest at 4,775 KB.
  #
  # Set to 0 to disable, matching `:replication_max_queue`. That restores the unbounded delta and
  # with it the OOM, so it is a debugging lever, not a tuning one.
  defp max_push_bytes,
    do: Application.get_env(:fathom, :replication_max_push_bytes, 1024 * 1024)

  # Armed only when a follower was left behind, and only one timer at a time, so a shard whose
  # followers all ack never arms one and pays nothing.
  #
  # An already-armed timer is left alone. It used to REFRESH a `catchup_at` commit count, because
  # the fired timer skipped itself when a commit had landed since; a stale count once dropped a
  # timer that was still needed (armed at commit 2, fired at commit 6, skipped, shard went quiet,
  # laggard stranded: `commits=6 catchup_at=2 ref=nil`). The fired timer now decides from follower
  # positions (`catch_up_need/1`, expert review 2026-10-20-2027 #6), so there is no count to keep
  # fresh and no way to strand a laggard by skipping.
  #
  # `arm_catchup/2` is the LOUD arm — real lag: it marks `LagGauge` and emits the `:catchup` event.
  # An already-armed QUIET timer (`arm_quiet/1`) is escalated in place (expert review 2026-10-21 #25).
  defp arm_catchup(%{catchup_ref: ref} = state, _rejects) when ref != nil, do: escalate(state)

  defp arm_catchup(state, rejects) do
    case retry_delay_ms(state.catchup_fails, rejects) do
      0 ->
        state

      ms ->
        # Armed (not merely failed) catch-ups only; `fails` is the backoff depth. Admin P1.
        :telemetry.execute([:fathom, :replication, :catchup], %{fails: state.catchup_fails}, %{})

        LagGauge.mark(self(), true)

        %{state | catchup_ref: Process.send_after(self(), :catch_up, ms), catchup_lag: true}
    end
  end

  # The SAFETY-NET arm (expert review 2026-10-21 #25): followers whose only shortfall is a fresh
  # in-flight push. With N > Q every commit replies at the Q-th ack and leaves N-Q of those, so
  # this is the steady state of a healthy shard, not lag. The timer is still armed — a reply that
  # never comes must age into a retry, exactly as before — but nothing is marked or emitted unless
  # the fired timer (or a later arm) finds the shortfall has lasted a catch-up interval.
  defp arm_quiet(%{catchup_ref: ref} = state) when ref != nil, do: state

  defp arm_quiet(state) do
    case retry_delay_ms(state.catchup_fails, []) do
      0 -> state
      ms -> %{state | catchup_ref: Process.send_after(self(), :catch_up, ms)}
    end
  end

  # A quiet timer turned out to cover real lag: count it, once.
  defp escalate(%{catchup_lag: true} = state), do: state

  defp escalate(state) do
    :telemetry.execute([:fathom, :replication, :catchup], %{fails: state.catchup_fails}, %{})
    LagGauge.mark(self(), true)
    %{state | catchup_lag: true}
  end

  # Lag that can be judged AT ARM TIME: a follower that is not current and whose outstanding push
  # has gone unanswered for at least one catch-up interval (a healthy follower acks in milliseconds).
  # A follower that is merely short with nothing in flight is NOT judged here: on a busy N > Q shard
  # that is routine — our own shipper refused this round's push `:already_in_flight` behind the
  # previous one (measured: 95/100 healthy N=4/Q=2 commits marked when this counted it). It is
  # judged by the fired timer instead, which marks a follower still `:short` an interval later.
  # Nothing read yet: lag, the conservative answer.
  defp lagging?(%{seen: [{gen, salt, extent} | _]} = state) do
    now = now_ms()
    stale_after = catchup_ms()

    Enum.any?(shippers(), fn s ->
      not Map.has_key?(state.seeding, s) and not holds?(state, s, gen, salt, extent) and
        Map.has_key?(state.inflight, s) and
        now - Map.get(state.inflight_at, s, now) >= stale_after
    end)
  end

  defp lagging?(_state), do: true

  # A fired timer whose only short followers have a push in flight: wait one more interval for
  # their replies (`handle_info(:catch_up, ...)`). Not a new arm — no `:catchup` event, the gauge
  # flag is already set — and not a failure, so the backoff depth is untouched.
  defp rearm_catchup(state) do
    case retry_delay_ms(state.catchup_fails, []) do
      0 -> clear_catchup(state)
      ms -> %{state | catchup_ref: Process.send_after(self(), :catch_up, ms)}
    end
  end

  # `:none | :pending | :short` — see `handle_info(:catch_up, ...)`. Judged against the newest
  # extent this Session has read, with the same per-follower test as `arm_if_short/1`. Nothing read
  # yet (`seen == []`): `:short`, so the cycle reads, as it always did.
  @doc false
  @spec catch_up_need(map()) :: :none | :pending | :short
  def catch_up_need(%{seen: [{gen, salt, extent} | _]} = state) do
    now = System.monotonic_time(:millisecond)

    Enum.reduce_while(shippers(), :none, fn s, acc ->
      cond do
        Map.has_key?(state.seeding, s) -> {:cont, acc}
        holds?(state, s, gen, salt, extent) -> {:cont, acc}
        in_flight?(state, s, now) -> {:cont, :pending}
        true -> {:halt, :short}
      end
    end)
  end

  def catch_up_need(_state), do: :short

  defp holds?(state, shipper, gen, salt, extent),
    do:
      match?(
        %{wal_gen: ^gen, salt1: ^salt, offset: o} when is_integer(o) and o >= extent,
        Map.get(state.followers, shipper)
      )

  # Every place that disarms the catch-up timer goes through here so the node-level gauge flag
  # (`LagGauge`, expert review 2026-10-17 #2) cannot drift from `catchup_ref`. `terminate/2` clears
  # it too; a Session killed without terminate is pruned by the gauge's liveness check.
  defp clear_catchup(state) do
    LagGauge.mark(self(), false)
    %{state | catchup_ref: nil, catchup_lag: false}
  end

  # A CLEAN commit is the only thing that clears the failure count. Not tidiness: without it a
  # shard that recovers stays at the backed-off delay forever, so the next genuine straggler waits
  # a whole flush interval to be made good instead of the 1 s the retry was tuned for.
  defp arm_if_rejected(state, []), do: %{state | catchup_fails: 0}

  # A round whose only "rejects" are followers skipped `:already_in_flight` is the N > Q steady state
  # on a busy shard (the previous commit's straggler has not acked yet), not lag: arm quietly unless
  # `lagging?/1` says that push has been out a whole interval (expert review 2026-10-21 #25;
  # measured 45/100 healthy N=4/Q=2 commits marked through this path once `arm_if_short/1` alone
  # was fixed). The backoff count moves as before. Any other reject is a real failure: loud.
  defp arm_if_rejected(state, rejects) do
    state = %{state | catchup_fails: state.catchup_fails + 1}

    if state.catchup_lag or lagging?(state) or
         not Enum.all?(rejects, &match?({_, :already_in_flight, _}, &1)),
       do: arm_catchup(state, rejects),
       else: arm_quiet(state)
  end

  # Arm the catch-up when a connected follower is behind the newest extent this Session has read
  # (expert review 2026-10-20 #3). The commit loop used to keep every follower current itself, at
  # the cost of every commit waiting on the slowest; it now stops at the quorum, so this is what
  # keeps the rest converging. A follower being seeded is left to its seed. Not a failure: the
  # backoff count is untouched, and an already-armed timer is only refreshed.
  #
  # LOUD ONLY FOR REAL LAG (expert review 2026-10-21 #25). This used to arm the loud catch-up for any
  # follower not yet acked, and with N > Q the commit replies at the Q-th ack, so N-Q pushes are
  # always still in flight here: every commit of every active shard marked `LagGauge` and emitted
  # `:catchup` (measured: 100/100 healthy loopback commits at N=4/Q=2), so the operator gauge read
  # "every active shard is lagging" and hid a real laggard. Now the same timer is armed QUIETLY
  # unless `lagging?/1` sees a push unanswered for a whole catch-up interval; the fired timer marks
  # what is still behind then. Once a Session is marked (`catchup_lag`) it re-arms loud until every
  # follower is current, so a real laggard does not flicker off between rounds. Durability
  # unchanged: the catch-up never gates an ack, and the timer is armed in exactly the cases it was
  # before (`catch_up_need/1` is `:none` exactly when no follower is short).
  defp arm_if_short(%{seen: [_ | _]} = state) do
    case catch_up_need(state) do
      :none ->
        state

      _pending_or_short ->
        if state.catchup_lag or lagging?(state),
          do: arm_catchup(state, []),
          else: arm_quiet(state)
    end
  end

  defp arm_if_short(state), do: state

  # BACKOFF, AND WHY THE FLAT 1 Hz RETRY WAS A CASCADING FAILURE (expert review 2026-08-20 #25).
  #
  # `arm_if_rejected/2` fires on ANY non-empty reject list, and `:disconnected` is in that list —
  # `Shipper` produces one for every push to a follower whose socket is down, and `drop/2` produces
  # one for every waiter when a link fails. There was no backoff, no attempt limit and no "this
  # follower is not coming back" condition, so with one peer down every Session on the node ran a
  # full `Wal.read` + `Primary.plan` + `Wal.read_delta` + budget-reserve + ship cycle once per
  # second, indefinitely, on shards with ZERO tenant traffic. At the ~200 shards/node the rig holds
  # at 1024 tenants that is ~200 pointless plan-and-read cycles per second, each of which also
  # blocks its session for up to `replication_timeout_ms` (5 s) inside `handle_info`, so real client
  # commits queue behind a retry for a peer that is not coming back.
  #
  # AGENTS.md measures the retry as free. That measurement was taken with all peers UP, which is
  # precisely the case where it never arms.
  #
  # Two rules, and the split matters:
  #
  #   * ALL rejects `:disconnected` ⇒ go straight to the cap. There is no socket, so no amount of
  #     retrying sooner can help. Read off the REASON rather than probing `Shipper.connected?/1`,
  #     which would put a `GenServer.call` on the commit path to learn something the reject
  #     already says.
  #   * otherwise exponential from `catchup_ms()`, capped. `:already_in_flight` and `:overloaded`
  #     are transient and local, and the first retry at 1 s is what makes a quiet shard's laggard
  #     good — so the fast first attempt is preserved and only a REPEATEDLY refused shard slows.
  #
  # The cap is `:shard_flush_interval_ms` because that is the window past which staleness starts to
  # cost something: the durability flush checkpoints the WAL, and a follower still behind at that
  # point forces `Primary.plan/3` to `{:reset, 0, size}` — the whole WAL rather than a delta, which
  # AGENTS.md's 2026-08-18 A/B measures as the difference between 4 errors and 22,599.
  @doc false
  @spec retry_delay_ms(non_neg_integer(), [{term(), atom(), non_neg_integer()}]) ::
          non_neg_integer()
  def retry_delay_ms(fails, rejects) do
    case catchup_ms() do
      0 ->
        0

      base ->
        cap = max(base, flush_interval_ms())

        if rejects != [] and
             Enum.all?(rejects, fn {_s, reason, _at} -> reason == :disconnected end) do
          cap
        else
          min(base * Integer.pow(2, min(max(fails - 1, 0), 16)), cap)
        end
    end
  end

  defp flush_interval_ms,
    do: Application.get_env(:fathom, :shard_flush_interval_ms, 5_000)

  # How long after a locally-refused push before the retry re-enters the commit path. 1 s: above any
  # round trip, so a straggler's real reply lands first and the retry then finds nothing to do; and
  # far below the durability flush interval, so a laggard is made good well inside the window its
  # staleness would cost anything. 0 disables, restoring the stale-replica window.
  defp catchup_ms, do: Application.get_env(:fathom, :replication_catchup_ms, 1_000)
end
