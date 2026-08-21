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

  alias Fathom.Shard.Replication
  alias Fathom.Shard.Replication.Primary
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Protocol.SeedBegin
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Replication.Wal

  @registry Fathom.Shard.Replication.SessionRegistry

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
  """
  @spec commit(String.t(), Path.t(), pid()) :: :ok | {:error, term()}
  def commit(shard_id, wal_path, coordinator) do
    with {:ok, pid} <- ensure_started(shard_id, coordinator) do
      GenServer.call(pid, {:commit, wal_path}, timeout())
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
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {@registry, shard_id}})
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

  defp ensure_started(shard_id, coordinator) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               Fathom.Shard.Replication.SessionSupervisor,
               {__MODULE__, shard_id: shard_id, coordinator: coordinator}
             ) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp timeout, do: Application.get_env(:fathom, :replication_timeout_ms, 5_000)

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
       # The WAL this shard commits through, remembered from the last commit. A follower's reply can
       # arrive long after the call that provoked it returned, and acting on `:unknown_shard` means
       # reading the shard's files — so the handler needs the path without a caller to supply it.
       # `nil` until the first commit, which is also the first moment a reply can exist.
       wal_path: nil,
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
       # Deferred-retry bookkeeping. `catchup_ref` is the armed timer (nil when none); `commits`
       # counts commit cycles and `catchup_at` records the count at arm time, so the retry can tell
       # a QUIET shard from a busy one. See `handle_info(:catch_up, ...)`.
       catchup_ref: nil,
       commits: 0,
       catchup_at: 0,
       # Consecutive catch-up rounds that were ALSO refused. Drives the backoff in
       # `retry_delay_ms/2`; reset by any commit that comes back with no rejects.
       catchup_fails: 0
     }}
  end

  # Deliberately `case`, not `with`/`else`. The first version used `with {:ok, state} <- ...` and
  # then referenced `state` in the else branch — where it is still the OUTER binding, so the epoch
  # `with_epoch/1` had just resolved was silently `nil` again. The seed task then crashed encoding
  # `nil::64` inside a `Task.start`, which swallows it, and the only symptom was followers that
  # were never seeded. Nesting keeps the epoch-bearing state in scope where it is used.
  @impl true
  def handle_call({:commit, wal_path}, _from, state) do
    # One deadline for the whole call, taken before any work: a seed wait that reset the clock
    # could outlive the caller's `GenServer.call` timeout, and a reply nobody is waiting for is
    # worse than a late failure — the caller has already given up and the tenant already has an
    # error, while this process is still holding the shard's serialization point.
    deadline = System.monotonic_time(:millisecond) + timeout()

    state = %{state | commits: state.commits + 1}

    case with_epoch(state) do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:ok, state} ->
        # Drain first: a straggler's reply that landed while the PREVIOUS call was still running has
        # not reached `handle_info/2` yet, and leaving it would let the next `collect/4` mistake it
        # for an answer to the push about to be sent.
        state = drain_late_replies(%{state | wal_path: wal_path}, wal_path)

        case ship(state, wal_path, state.epoch, deadline) do
          {:ok, new_state, rejects} ->
            new_state = start_seeds(new_state, wal_path, rejects)

            {:reply, :ok,
             new_state
             |> drain_late_replies(wal_path)
             # A commit can SUCCEED on the quorum while a follower was refused by OUR OWN shipper.
             # That follower is now behind with nothing scheduled to fix it, and the tenant saw
             # `:ok` — so nothing anywhere looks wrong. This is the arm that matters.
             |> arm_if_rejected(rejects)}

          :nothing ->
            {:reply, :ok, state}

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
          # Still bounded, and still fails honestly: `await_seeds/3` waits only for seeds THIS
          # commit started, only until the caller's own deadline, and a shard too large to seed
          # inside it fails exactly as it did before, having spent time it was going to spend
          # anyway. Seeding remains write-gated — nothing seeds a shard nobody writes to, which is
          # the storm the durability `dirty` flag exists to prevent.
          {:error, {:no_quorum, why}, new_state, rejects} ->
            seeded_state = start_seeds(new_state, wal_path, rejects)

            case await_seeds(seeded_state, deadline, wal_path) do
              {:ok, ready_state} ->
                retry_after_seed(ready_state, wal_path, deadline, why)

              {:timeout, waited_state} ->
                {:reply, {:error, {:no_quorum, why}}, waited_state}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, state}
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
  def handle_info({:seeded, shipper, result}, state) do
    {:noreply, apply_seed_result(state, shipper, result)}
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
  def handle_info(:catch_up, %{wal_path: nil} = state),
    do: {:noreply, %{state | catchup_ref: nil}}

  def handle_info(:catch_up, %{commits: n, catchup_at: n} = state) do
    {:reply, _discarded, state} =
      handle_call({:commit, state.wal_path}, {self(), make_ref()}, %{state | catchup_ref: nil})

    {:noreply, state}
  end

  def handle_info(:catch_up, state) do
    # A commit landed while this was pending, and a commit already re-plans for EVERY follower, so
    # the laggard has been re-shipped. Drop the timer rather than duplicate that work: if that commit
    # refused a follower too, its own reject arms a fresh one. This is what keeps the retry free on a
    # busy shard — the first attempt fired regardless, and that alone cost 15% throughput.
    {:noreply, %{state | catchup_ref: nil}}
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
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.seeding, fn {_shipper, e} -> e.ref == ref end) do
      nil ->
        {:noreply, state}

      {shipper, _entry} ->
        Logger.warning(
          "replication seed task for #{state.shard_id} died without answering " <>
            "(#{inspect(reason)}); the follower may ask again"
        )

        {:noreply, %{state | seeding: forget_seed(state.seeding, shipper)}}
    end
  end

  def handle_info(_, state), do: {:noreply, state}

  # A STOPPED SESSION MUST NOT LEAVE A SEED STREAMING (expert review 2026-08-20 #26).
  #
  # The Session stops when its coordinator goes DOWN — the shard moved or drained — and an
  # unsupervised task started before that would go on streaming an entire tenant database to a
  # follower for a shard this node no longer owns, stamped with an epoch it no longer holds.
  # `spawn_monitor` deliberately does not link (a raise inside `do_seed/5` must not take down the
  # shard's serialization point), so the cancellation is explicit here instead.
  @impl true
  def terminate(_reason, %{seeding: seeding}) do
    for {_shipper, %{pid: pid}} <- seeding, do: Process.exit(pid, :kill)
    :ok
  end

  def terminate(_reason, _state), do: :ok

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
      {:ok, epoch} -> {:ok, %{state | epoch: epoch}}
      {:error, reason} -> {:error, reason}
    end
  end

  # SHIP UNTIL CAUGHT UP, IN BOUNDED ROUNDS.
  #
  # `Primary.plan/3` caps how much WAL one push may carry, which is what breaks the feedback loop
  # behind the 1024-tenant OOM (`docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`). The cost of
  # that cap is that one round no longer necessarily hands a follower everything, so a commit that
  # returned after one round could report success while the quorum was still short of the bytes it
  # just committed — the exact lie `Session`'s moduledoc refuses to tell.
  #
  # So the round repeats until every follower is current, or the caller's own deadline passes. This
  # is the SAME trade the seeding path made deliberately in `handle_call/3` below (see the long
  # comment there): a bounded wait beats a guaranteed error, and the failure at the end of it is
  # exactly as honest as the one round produced before.
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
  defp ship(state, wal_path, epoch, deadline), do: ship(state, wal_path, epoch, deadline, %{})

  defp ship(state, wal_path, epoch, deadline, seen) do
    case Wal.read(wal_path) do
      {:ok, header} ->
        case ship_planned(state, wal_path, epoch, header) do
          # Nothing left to plan. On the first round that means the commit wrote no frames; on a
          # later one it means the catch-up converged.
          :nothing when map_size(seen) == 0 ->
            :nothing

          :nothing ->
            {:ok, state, Map.values(seen)}

          {:ok, new_state, rejects, complete?} ->
            seen = merge_rejects(seen, rejects)

            cond do
              complete? ->
                {:ok, new_state, Map.values(seen)}

              # Out of time with a follower still behind. Fail rather than ack: the quorum does not
              # hold this commit's bytes yet.
              past?(deadline) ->
                {:error, {:no_quorum, :catching_up}, new_state, Map.values(seen)}

              true ->
                # LET THE ROUND SETTLE BEFORE STARTING THE NEXT ONE. Two distinct failures, both
                # found the first time this loop ran against three real followers:
                #
                #   1. `ship_quorum/4` returns at the Q-th ack, so the straggler answers into this
                #      mailbox afterwards — and the next round's `collect/4` reads that answer as a
                #      reply to ITSELF. It carries the previous round's offset, so it is scored
                #      `:offset_mismatch`, `reconcile/2` rewinds the follower to a position it has
                #      already passed, and a catch-up that should converge in N rounds thrashes
                #      until the deadline instead. The whole test file went from 0.1 s to 5.0 s.
                #
                #   2. The straggler's push is still outstanding in its SHIPPER, which holds one
                #      waiter per shard. Shipping to it again is refused `:already_in_flight` — so
                #      across a multi-round catch-up that follower is refused EVERY round and never
                #      advances, while the quorum keeps succeeding without it. Silent
                #      under-replication, the same class as the late-reply bugs `handle_info/2`
                #      above exists to prevent.
                #
                # So the loop waits for the round's outstanding pushes, not just for a quorum's
                # worth. That deliberately gives up the quorum's straggler win — but only here, on
                # a shard that is ALREADY behind, and still bounded by the caller's deadline. In
                # steady state `complete?` is true after the first round and this never runs.
                new_state
                |> settle_inflight(wal_path, deadline)
                # Re-read the header rather than reusing it. Other streams commit to this shard
                # concurrently, so the WAL may have grown again; planning from a stale header would
                # ship a delta that is already short by the time it lands.
                |> ship(wal_path, epoch, deadline, seen)
            end

          {:error, {:no_quorum, why}, new_state, rejects} ->
            {:error, {:no_quorum, why}, new_state, Map.values(merge_rejects(seen, rejects))}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        read_failed(state, reason)
    end
  end

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
  defp ship_planned(state, wal_path, epoch, header) do
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
      case build_pushes(state, wal_path, epoch, header, plans) do
        {:ok, pushes} -> tag(deliver(state, header, plans, pushes, current), plans, header)
        {:error, reason} -> read_failed(state, reason)
      end
    end
  end

  # Did this round hand every follower the whole WAL, or did `Primary.plan/3`'s cap stop short?
  #
  # Computed from the plans rather than signalled out of `plan/3`, so the cap stays a property of
  # the pure planner and `deliver/5`'s return shape is left alone. `{:reset, 0, len}` compares the
  # same way `{:append, off, len}` does — a reset that reached `size` is complete.
  defp tag({:ok, state, rejects}, plans, header),
    do: {:ok, state, rejects, complete?(plans, header)}

  defp tag(other, _plans, _header), do: other

  defp complete?(plans, %{size: size}),
    do: Enum.all?(plans, fn {_shipper, {_kind, off, len}} -> off + len >= size end)

  defp build_pushes(state, wal_path, epoch, header, plans) do
    # One read per DISTINCT range, not per follower.
    ranges = plans |> Enum.map(fn {_s, {_k, off, len}} -> {off, len} end) |> Enum.uniq()

    payloads =
      Enum.reduce_while(ranges, {:ok, %{}}, fn {off, len}, {:ok, acc} ->
        case Wal.read_delta(wal_path, off, len) do
          {:ok, bin} -> {:cont, {:ok, Map.put(acc, {off, len}, bin)}}
          {:error, r} -> {:halt, {:error, r}}
        end
      end)

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
    with {:ok, by_range} <- payloads,
         {:ok, after_} <- Wal.read(wal_path),
         :ok <- stable?(header, after_, :checkpoint_during_push) do
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
            payload: Map.fetch!(by_range, {off, len})
          }}
       end}
    end
  end

  defp deliver(state, header, plans, pushes, current) do
    # Recorded BEFORE anything is sent, because a reply can be observed by a later commit's drain
    # with no plan in scope. See the `inflight` note in `init/1`.
    state = %{state | inflight: Map.merge(state.inflight, expectations(plans, header))}

    case max(quorum() - current, 0) do
      # Followers that are already current carry the quorum on their own. The laggards still get
      # their bytes — skipping them is how a replica falls behind forever — but the tenant does not
      # wait for an ack it does not need.
      0 ->
        Replication.ship_async(pushes)
        {:ok, state, []}

      needed ->
        collect(state, pushes, needed)
    end
  end

  defp collect(state, pushes, needed) do
    case Replication.ship_quorum(pushes, needed, timeout()) do
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
    {:error, reason}
  end

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

          {:seeded, shipper, result} ->
            state
            |> apply_seed_result(shipper, result)
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
  defp await_seeds(%{seeding: seeding} = state, deadline, wal_path) do
    if map_size(seeding) == 0 do
      {:ok, state}
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        {:timeout, state}
      else
        receive do
          {:seeded, shipper, result} ->
            state
            |> apply_seed_result(shipper, result)
            |> await_seeds(deadline, wal_path)

          {:repl_reply, from, {:ack, _shard, next}} ->
            state
            |> settle_late_ack(from, next)
            |> await_seeds(deadline, wal_path)

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
            |> await_seeds(deadline, wal_path)

          {:repl_reply, _from, _other} ->
            await_seeds(state, deadline, wal_path)
        after
          remaining -> {:timeout, state}
        end
      end
    end
  end

  # Re-ship after a seed landed. ONE attempt, never a loop: a follower that rejects a delta built
  # from the position its own seed just reported is not going to accept a third, and retrying would
  # convert a bounded failure into a call that spins until the caller's timeout. `why` is the
  # ORIGINAL failure — reporting the retry's reason would blame the seed for a quorum that was
  # already short.
  defp retry_after_seed(state, wal_path, deadline, why) do
    case ship(state, wal_path, state.epoch, deadline) do
      {:ok, new_state, rejects} ->
        {:reply, :ok, drain_late_replies(start_seeds(new_state, wal_path, rejects), wal_path)}

      :nothing ->
        {:reply, :ok, state}

      {:error, {:no_quorum, _retry_why}, new_state, rejects} ->
        {:reply, {:error, {:no_quorum, why}}, start_seeds(new_state, wal_path, rejects)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # The bookkeeping `handle_info({:seeded, ...})` does, factored out so the in-call wait applies a
  # seed result identically. Divergence between the two would be invisible until a seed happened to
  # land on the path with the older logic.
  defp apply_seed_result(state, shipper, result) do
    state = %{state | seeding: forget_seed(state.seeding, shipper)}

    case result do
      {:ok, at} ->
        Logger.info("replication seeded a follower for #{state.shard_id} at #{inspect(at)}")
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

    Enum.reduce(needs, state, fn shipper, acc ->
      session = self()
      db_path = String.replace_suffix(wal_path, "-wal", "")

      # `spawn_monitor`, not `Task.start`: monitoring has to be ATOMIC with the spawn, or a task
      # that dies in the gap leaves an entry nothing will ever remove — which is the bug being
      # fixed, reintroduced in a smaller window. Not linked: a raise inside `do_seed/5` must not
      # take down the Session, which holds the shard's serialization point. The `terminate/2`
      # sweep below is what covers the other half the finding asked for (a stopped Session must
      # not leave a task streaming a whole tenant database for a shard this node no longer owns).
      {pid, ref} =
        spawn_monitor(fn ->
          send(
            session,
            {:seeded, shipper, do_seed(shipper, acc.shard_id, db_path, wal_path, acc.epoch)}
          )
        end)

      entry = %{ref: ref, pid: pid, started_at: System.monotonic_time(:millisecond)}
      %{acc | seeding: Map.put(acc.seeding, shipper, entry)}
    end)
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

  # Stream a CONSISTENT base copy.
  #
  # In WAL mode the `.db` file is only rewritten by a checkpoint, so reading it alongside the `-wal`
  # is safe as long as no checkpoint intervenes. The generation is read before AND after: if it
  # moved, a checkpoint rebuilt the `.db` under us and the two halves are from different points in
  # time — which would hand the follower a database and a WAL whose salts do not match. Retrying is
  # correct and cheap; shipping the inconsistent pair is neither.
  #
  # Streaming widens that window (the copy now takes as long as the transfer rather than as long as
  # a `File.read`), which is exactly why the check moved to AFTER the last chunk and gained an
  # explicit `seed_abort`: the follower must be told to drop what it has, and the sender must not
  # then sit on its own timeout.
  #
  # Nothing here holds the database in memory — `:file.pread` walks it a chunk at a time. That is
  # the point of the change: the old path did `File.read(db_path)`, so a 2 GB tenant was 2 GB
  # resident on the primary and again on the follower.
  defp do_seed(shipper, shard_id, db_path, wal_path, epoch) do
    with {:ok, before} <- Wal.read(wal_path),
         {:ok, db_size} <- file_size(db_path),
         wal_size = wal_size_of(before),
         :ok <- open_seed(shipper, shard_id, epoch, before, db_size, wal_size),
         :ok <- stream_part(shipper, shard_id, :db, db_path, db_size),
         :ok <- stream_part(shipper, shard_id, :wal, wal_path, wal_size),
         {:ok, after_} <- Wal.read(wal_path),
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

  defp open_seed(shipper, shard_id, epoch, before, db_size, wal_size) do
    Shipper.seed_begin(shipper, %SeedBegin{
      shard_id: shard_id,
      epoch: epoch,
      wal_gen: gen_of(before),
      salt1: salt_of(before),
      wal_offset: wal_size,
      db_size: db_size,
      wal_size: wal_size
    })
  end

  defp await_seed_reply(shipper, shard_id, before, wal_size) do
    receive do
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
        Shipper.seed_chunk(shipper, shard_id, part, seq, bin)
        stream_chunks(shipper, shard_id, part, fd, offset + len, seq + 1, size)

      {:ok, _short} ->
        {:error, :short_read}

      :eof ->
        {:error, :short_read}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> {:ok, size}
      {:error, reason} -> {:error, reason}
    end
  end

  defp wal_size_of(:empty), do: 0
  defp wal_size_of(%{size: size}), do: size

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

  # Armed only when a reject was actually seen, and only one timer at a time, so a shard whose
  # followers all ack never arms one and pays nothing.
  #
  # A reject arriving while a timer is ALREADY armed refreshes `catchup_at` rather than being
  # ignored, and that is load-bearing rather than tidy. The timer means "retry shortly after the
  # MOST RECENT reject"; `catchup_at` is the commit count as of that reject. Leaving it stale was a
  # real bug: armed at commit 2, four more commits each refused the laggard and each was swallowed
  # by this guard, so when the timer finally fired it compared 6 against 2, took the "a commit
  # landed, skip" branch, and DROPPED itself. The shard then went quiet with nothing armed and the
  # laggard stayed behind — the exact strand this retry exists to prevent, reintroduced by the
  # guard meant to keep it cheap. Caught by instrumenting the failure (`commits=6 catchup_at=2
  # ref=nil`) rather than by reading.
  defp arm_catchup(%{catchup_ref: ref} = state, _rejects) when ref != nil,
    do: %{state | catchup_at: state.commits}

  defp arm_catchup(state, rejects) do
    case retry_delay_ms(state.catchup_fails, rejects) do
      0 ->
        state

      ms ->
        %{
          state
          | catchup_ref: Process.send_after(self(), :catch_up, ms),
            catchup_at: state.commits
        }
    end
  end

  # A CLEAN commit is the only thing that clears the failure count. Not tidiness: without it a
  # shard that recovers stays at the backed-off delay forever, so the next genuine straggler waits
  # a whole flush interval to be made good instead of the 1 s the retry was tuned for.
  defp arm_if_rejected(state, []), do: %{state | catchup_fails: 0}

  defp arm_if_rejected(state, rejects),
    do: arm_catchup(%{state | catchup_fails: state.catchup_fails + 1}, rejects)

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
