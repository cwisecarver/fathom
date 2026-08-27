defmodule Fathom.Shard.Heartbeat do
  @moduledoc """
  The node's single liveness heartbeat — the F1 fix that replaces per-shard lease
  renewal (the ~100k PUT/s/node storm at a million shards) with **one** PUT per node.

  A node is "alive" iff its `heartbeat/<owner>` object is fresh (see
  `Fathom.Shard.Storage`). This GenServer renews that one object every `ttl/3` for
  the life of the node; `Fathom.Shard` coordinators no longer renew their shards.

  ## Fence for flushes

  A coordinator may only write a shard's data back to storage while it still owns
  the shard. A steal requires the owner's heartbeat to be *stale*, so a node whose
  heartbeat is continuously valid is never superseded. The coordinator records
  `generation/0` when it acquires a shard and calls `valid_for_write?/1` before each
  flush:

    * `:ok` — heartbeat valid with margin AND no lapse since acquire ⇒ we still own
      it ⇒ the write is safe to START, with no per-shard I/O.
    * `:revalidate` — valid, but the heartbeat lapsed since acquire ⇒ a steal *may*
      have happened during the gap ⇒ re-check the shard lock (`Storage.check_lease/2`)
      before writing, and self-fence if it was stolen.
    * `:not_valid` — heartbeat not comfortably valid ⇒ do NOT write (retry later);
      writing without confirmed liveness could race a steal.

  ## What the margin does and does not promise (expert review 2026-08-01 #28)

  This documentation used to say `:ok` meant the lease "won't expire mid-write". **It cannot
  promise that**, and the wording mattered because it is what stopped anyone asking how long a
  write takes. The margin is `max(div(ttl, 3), 1)` — 10 s at the default 30 s TTL — and is not
  derived from the write it is supposed to cover. That write is a full `quick_check` page scan
  plus `VACUUM INTO` plus a whole-object PUT. Measured against `Storage.Local`, no network:

      1.4 MB   8.9 ms        71 MB    265 ms
      13.8 MB  55.4 ms      284 MB  1,070 ms

  Dead linear at ~3.8 ms/MB, so the LOCAL half alone reaches the margin around 2.6 GB — under the
  4 GiB per-shard cap `Fathom.Shard.Connection` enforces, and before anything goes over the wire.

  What `:ok` does mean: the lease was valid with margin at the instant the write STARTED. The
  fence that actually prevents a clobber is the conditional PUT's etag (`If-Match`), not this —
  so an over-long write is not a correctness hole, it is a LOSS bias: shards whose flush outruns
  the margin take the `:superseded` self-fence path and quarantine an interval of acked writes,
  and small shards never do. That bias correlates with load, which is when failovers happen.

  `Fathom.Shard.snapshot_and_upload/1` re-checks this immediately before the PUT so the margin
  only has to cover the upload rather than scan + VACUUM + upload. It is cheap to do (this
  function is a lock-free ETS read in heartbeat mode) and it removes the purely-local part of the
  exposure, but it does not make the original promise true at every shard size. Sizing the TTL
  against the real flush distribution is the open question; `flush_p50_us` in the bench gate is
  the measurement that makes it answerable.

  A "lapse" is detected when a renewal tick finds the heartbeat already expired (a
  GC pause / partition let it lapse, during which another node could have stolen our
  shards). It bumps `generation` once per episode and broadcasts so coordinators can
  revalidate proactively instead of waiting for their next flush.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Storage

  @default_ttl_ms 30_000

  # How many renewal attempts one cycle affords, and the pause between them (expert review
  # 2026-08-26 #14). Three, because each attempt is budgeted at `renew_ms / 3`; the pause exists
  # so a fast failure (connection refused, which returns in microseconds) cannot spin the
  # remaining budget.
  @renew_attempts 3
  @renew_retry_pause_ms 50
  @topic "fathom:heartbeat"

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  This node's heartbeat owner string (matches the shard lease `owner`):
  `node()#<boot nonce>` (expert review #6). The incarnation nonce makes lease
  ownership boot-scoped — a lock held by a PREVIOUS incarnation of this node name
  is a foreign owner, so it goes through the liveness check and the epoch-bumping
  steal path instead of the silent same-owner/same-epoch reclaim, and a lingering
  same-name zombie (a replaced pod still S3-reachable) renews its OWN per-
  incarnation heartbeat object rather than sharing ours.
  """
  def owner, do: "#{node()}##{incarnation()}"

  @doc false
  # The boot nonce, set by Fathom.Application.start before the tree (race-free).
  # Lazy fallback for standalone use (iex without the app); the put is last-wins,
  # acceptable outside the supervised boot path.
  def incarnation do
    case :persistent_term.get({Fathom, :incarnation}, nil) do
      nil ->
        nonce = Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
        :persistent_term.put({Fathom, :incarnation}, nonce)
        nonce

      nonce ->
        nonce
    end
  end

  @doc "PubSub topic broadcast on a heartbeat lapse (`{:heartbeat_lapsed, generation}`)."
  def topic, do: @topic

  @doc "Whether the heartbeat process is running (coordinators fall back to the legacy fence if not)."
  def running?, do: Process.whereis(__MODULE__) != nil

  # The status ETS row: {:status, generation, mono_deadline_ms, margin_ms}, written
  # by the heartbeat process on every renew/lapse (round-2 #26). Reading it is
  # lock-free from any process — no GenServer call, so a lapse-broadcast fan-out of
  # fence checks can neither flood the heartbeat's mailbox nor time out into the
  # per-shard renew_lease PUT storm (the F1 regression) while the process is merely
  # busy. The named table dies with the process, so "table absent" still means
  # "heartbeat down" and the callers' legacy fallback semantics are unchanged.
  @status_table __MODULE__.Status

  @doc "Current lapse generation; a coordinator records this when it acquires a shard."
  @spec generation() :: non_neg_integer()
  def generation do
    case status() do
      {gen, _deadline, _margin} -> gen
      # Down (or booting): the call preserves the old exit-when-down semantics.
      :down -> GenServer.call(__MODULE__, :generation)
    end
  end

  @doc "Fence check for a flush given the generation recorded at acquire. See the moduledoc."
  @spec valid_for_write?(non_neg_integer()) :: :ok | :revalidate | :not_valid
  def valid_for_write?(acquire_gen) do
    case status() do
      {gen, deadline, margin} ->
        now = System.monotonic_time(:millisecond)

        cond do
          # Not comfortably valid (no confirmed renewal yet, expired, or about
          # to) — never write without confirmed liveness. Monotonic, so a
          # wall-clock step can't inflate perceived validity (expert review #21).
          is_nil(deadline) or now + margin >= deadline -> :not_valid
          gen != acquire_gen -> :revalidate
          true -> :ok
        end

      :down ->
        GenServer.call(__MODULE__, {:valid_for_write, acquire_gen})
    end
  end

  @doc """
  The safety margin `valid_for_write?/1` applies, in ms.

  This is how early a renewal deadline is treated as already unsafe, so it is also how long
  BEFORE a shard becomes stealable that a coordinator first sees `:not_valid`. The write
  circuit-breaker needs that offset to arm at the right moment rather than a whole TTL late
  (expert review 2026-08-01 #13). Falls back to the configured value when the heartbeat is
  down, which is the same `max(div(ttl, 3), 1)` the process itself computes.
  """
  @spec margin_ms() :: pos_integer()
  def margin_ms do
    case status() do
      {_gen, _deadline, margin} ->
        margin

      :down ->
        max(div(Application.get_env(:fathom, :shard_lease_ttl_ms, @default_ttl_ms), 3), 1)
    end
  end

  defp status do
    case :ets.lookup(@status_table, :status) do
      [{:status, gen, deadline, margin}] -> {gen, deadline, margin}
      [] -> :down
    end
  rescue
    ArgumentError -> :down
  end

  # Publish the fence-relevant slice of `state` to the status table. Public (and
  # the table is public) so tests that drive lapse/expiry via :sys.replace_state
  # can keep the published view in sync with the state they forged.
  @doc false
  def publish_status(state) do
    :ets.insert(
      @status_table,
      {:status, state.generation, state.mono_deadline_ms, state.margin_ms}
    )

    state
  rescue
    ArgumentError -> state
  end

  @impl true
  def init(opts) do
    # Trap exits so a supervisor :shutdown runs terminate/2 (which clears the
    # heartbeat for a fast planned failover).
    Process.flag(:trap_exit, true)

    ttl =
      Keyword.get(
        opts,
        :ttl_ms,
        Application.get_env(:fathom, :shard_lease_ttl_ms, @default_ttl_ms)
      )

    owner = Keyword.get(opts, :owner, owner())

    # Same-machine restart fast path (expert review #6): our previous incarnation's
    # heartbeat object would otherwise stay fresh for up to a TTL, holding this
    # node's own old locks {:held} against us. But sharing the nonce file is NOT
    # proof the predecessor is dead (expert review round-2 #16: a persisted /
    # remounted :shard_data_dir is shared with a live node), so a stale-by-margin
    # heartbeat is cleared now, while a still-fresh one is only RE-CHECKED after one
    # renew interval (see {:clear_previous_incarnation, ...} below): frozen expiry ⇒
    # dead predecessor ⇒ clear; advancing expiry ⇒ live renewer ⇒ refuse.
    clear_action =
      if owner == owner(), do: clear_previous_incarnation(), else: :ok

    # The lapse generation must survive this process: it is the coordinators' proof
    # that "no lapse happened since acquire ⇒ no steal was possible", and a restart
    # gap is itself a window in which the heartbeat may have lapsed and a steal
    # occurred. Resetting to 0 on restart forged that proof for every coordinator
    # acquired at generation 0 of the previous incarnation. Persist it per owner and
    # treat every re-boot of an already-seen owner as a lapse episode — all
    # pre-restart acquire_gens then mismatch and force one cheap revalidation.
    generation = :persistent_term.get(gen_key(owner), -1) + 1
    :persistent_term.put(gen_key(owner), generation)

    if generation > 0 do
      Logger.warning(
        "heartbeat restarted for #{owner} (generation #{generation}); coordinators will revalidate"
      )

      # A restart IS a lapse episode (the bump above says so), but only mark_lapse
      # broadcast — so the proactive revalidation coordinators subscribe to (expert
      # review #34) never fired for the restart-lapse case, and they sat on stale
      # acquire_gens until their next flush (unboundedly in idle-only mode) while the
      # restart gap is a real steal window. Broadcast so they revalidate NOW (expert
      # review round-2 #15).
      broadcast_lapse(generation)
    end

    state = %{
      owner: owner,
      ttl_ms: ttl,
      # Renew every third of the TTL (a couple of misses don't lapse the lease).
      renew_ms: max(div(ttl, 3), 1),
      # A flush is only safe if the heartbeat won't expire during it: require at
      # least this much validity remaining. In steady state the heartbeat always has
      # > 2*ttl/3 left, so flushes never block; the margin only trips when a renewal
      # has been missed.
      margin_ms: max(div(ttl, 3), 1),
      # Wall clock — what peers read in the stored heartbeat object.
      expires_at_ms: 0,
      # Monotonic — drives the LOCAL validity/lapse decisions (expert review #21):
      # "am I still valid?" and "did I lapse?" are pure elapsed-time questions that
      # need no clock agreement, and a backward wall-clock step (NTP correction, VM
      # live-migration) could otherwise inflate perceived validity past a real lapse —
      # never edge-detecting it, never bumping the generation, and letting flushes
      # skip revalidation against a peer that legitimately stole our shards.
      mono_deadline_ms: nil,
      generation: generation,
      lapsed: false,
      timer: nil
    }

    # The lock-free fence-status view (round-2 #26); published on every renew/lapse.
    :ets.new(@status_table, [:set, :public, :named_table, read_concurrency: true])
    publish_status(state)

    case clear_action do
      {:recheck, prev_owner, exp} ->
        # One full renew interval (+ slack) so a live predecessor renews at least
        # once before the second read.
        Process.send_after(
          self(),
          {:clear_previous_incarnation, prev_owner, exp},
          state.renew_ms + 1_000
        )

      :ok ->
        :ok
    end

    {:ok, state, {:continue, :renew}}
  end

  @impl true
  def handle_continue(:renew, state), do: {:noreply, do_renew(state)}

  @impl true
  def handle_info(:renew, state), do: {:noreply, do_renew(state)}

  # Second read of a previous incarnation's still-fresh heartbeat (expert review #16;
  # scheduled from init). A dead predecessor's heartbeat is frozen at the expiry we
  # captured, so an unchanged value means it is safe to clear (the #6 fast restart, one
  # renew interval late). ANY advance means a live renewer shares this data dir —
  # clearing its heartbeat would declare its every long-held shard stealable while it
  # keeps serving (split-brain), so refuse loudly and leave it.
  @impl true
  def handle_info({:clear_previous_incarnation, prev_owner, prev_exp}, state) do
    case Storage.read_heartbeat(prev_owner) do
      {:ok, %{expires_at_ms: ^prev_exp}} ->
        _ = Storage.clear_heartbeat(prev_owner)
        # Frozen expiry = no renewer = proven dead; see judge_previous (round-2 #34).
        Storage.mark_incarnation_dead(prev_owner)

      {:ok, _advanced} ->
        Logger.error(
          "refusing to clear previous incarnation #{prev_owner}: its heartbeat is being " <>
            "RENEWED, so a live node shares this data dir (persisted/remounted volume?). " <>
            "Clearing it would open a split-brain steal window (expert review #16)."
        )

      # Gone (cleanly shut down meanwhile) or unreadable — nothing to do / fail closed.
      _ ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_call(:generation, _from, state), do: {:reply, state.generation, state}

  def handle_call({:valid_for_write, acquire_gen}, _from, state) do
    now = System.monotonic_time(:millisecond)

    reply =
      cond do
        # Not comfortably valid (no confirmed renewal yet, expired, or about to) —
        # never write without confirmed liveness. Monotonic, so a wall-clock step
        # can't inflate perceived validity (expert review #21).
        is_nil(state.mono_deadline_ms) or now + state.margin_ms >= state.mono_deadline_ms ->
          :not_valid

        # A lapse happened since this shard was acquired — re-check ownership first.
        state.generation != acquire_gen ->
          :revalidate

        true ->
          :ok
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(reason, state) when reason in [:normal, :shutdown] do
    # Clean shutdown: drop our heartbeat so this node's shards are immediately
    # stealable (fast planned-failover) rather than waiting out the TTL.
    Storage.clear_heartbeat(state.owner)
    :ok
  end

  def terminate({:shutdown, _}, state) do
    Storage.clear_heartbeat(state.owner)
    :ok
  end

  # A crash the supervisor is about to reverse must NOT delete the liveness object:
  # with per-shard locks never renewed in heartbeat mode, an absent heartbeat makes
  # every long-lived shard on this (perfectly healthy, still-serving) node look dead
  # to owner_live?'s :not_found fallback and instantly stealable during the restart
  # gap. Leave the object in place — the restarted process re-renews it within
  # renew_ms, and until then the stale-but-present object still fences stealers.
  def terminate(_reason, _state), do: :ok

  defp do_renew(state) do
    # Captured BEFORE the storage PUT: the local deadline must not outlive the stored
    # object's expiry, and the PUT takes time — measuring after it would skew the
    # deadline later (the unsafe direction).
    now = System.monotonic_time(:millisecond)

    # Edge-detect a lapse: we had a valid heartbeat and it expired before we renewed.
    # Monotonic elapsed time, immune to wall-clock steps (expert review #21).
    state =
      if not state.lapsed and not is_nil(state.mono_deadline_ms) and
           now > state.mono_deadline_ms,
         do: mark_lapse(state),
         else: state

    state =
      case renew_within_cycle(state, now) do
        {:ok, %{expires_at_ms: exp}} ->
          :telemetry.execute(
            [:fathom, :shard, :heartbeat, :renewed],
            %{count: 1},
            %{owner: state.owner}
          )

          # Recovered — fresh expiry, lapse episode over.
          %{state | expires_at_ms: exp, mono_deadline_ms: now + state.ttl_ms, lapsed: false}

        {:error, reason} ->
          Logger.warning("heartbeat renew failed for #{state.owner}: #{inspect(reason)}")
          state
      end

    publish_status(state)
    schedule_renew(state, now)
  end

  # RETRY INSIDE THE CYCLE, not across cycles (expert review 2026-08-26 #14).
  #
  # `Storage.renew_heartbeat/2` is a PUT, and Req's default `retry: :safe_transient` covers only
  # GET/HEAD — so a transient 5xx or socket error used to lose a whole renewal cycle with no retry
  # at all. Two lost cycles put `valid_for_write?/1` at `:not_valid`, which stops every durability
  # flush on the node, starts the write-fence clock, and at `margin + steal_margin` 503s this
  # node's writes fleet-wide; past `ttl + steal_margin` peers may steal its whole keyspace slice
  # while it is healthy and serving. The trigger for all of that is ordinary pool contention.
  #
  # Each attempt is budgeted at `renew_ms / 3` (see the `:budget_ms` option), so the cycle affords
  # about three, and the WHOLE loop is bounded by `renew_ms` — where a single unbudgeted attempt
  # could previously run ~23 s past it.
  #
  # Blocking this GenServer for up to a cycle is deliberate and is not a stall: every reader
  # (`valid_for_write?/1`, `generation/0`, `margin_ms/0`) goes through the lock-free `status`
  # ETS table, never through this process.
  defp renew_within_cycle(state, started) do
    budget = max(div(state.renew_ms, 3), 1)
    attempt_renew(state, started + state.renew_ms, budget, @renew_attempts)
  end

  defp attempt_renew(state, _deadline, budget, 1) do
    Storage.renew_heartbeat(state.owner, state.ttl_ms, budget_ms: budget)
  end

  defp attempt_renew(state, deadline, budget, attempts_left) do
    case Storage.renew_heartbeat(state.owner, state.ttl_ms, budget_ms: budget) do
      {:ok, _} = ok ->
        ok

      {:error, _} = error ->
        # A connection-refused fails in microseconds, so without this the loop would spin the
        # remaining budget. Only retry if a whole further attempt fits before the cycle ends.
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining >= budget + @renew_retry_pause_ms do
          Process.sleep(@renew_retry_pause_ms)
          attempt_renew(state, deadline, budget, attempts_left - 1)
        else
          error
        end
    end
  end

  # The persisted-generation key for `owner` (see init/1 — restart continuity).
  defp gen_key(owner), do: {__MODULE__, :generation, owner}

  # See init/1. The nonce file lives next to the shard files (node-local disk).
  # Returns :ok, or {:recheck, prev_owner, expires_at_ms} when the previous
  # incarnation's heartbeat is still fresh and needs the second read (#16).
  defp clear_previous_incarnation do
    path = incarnation_file()
    current = incarnation()

    action =
      case File.read(path) do
        {:ok, prev} when prev != "" and prev != current ->
          judge_previous("#{node()}##{prev}")

        _ ->
          :ok
      end

    File.mkdir_p!(Path.dirname(path))
    File.write(path, current)
    action
  rescue
    e ->
      Logger.warning("previous-incarnation heartbeat clear failed: #{Exception.message(e)}")
      :ok
  end

  # Expert review round-2 #16: "shares the .incarnation file" is NOT proof the
  # predecessor is dead. A persisted :shard_data_dir (hostPath / PV / NFS — which the
  # warm-restart design actively encourages) is shared with a predecessor still in
  # its termination grace, or with a live node on another machine after a PV remount.
  # Unconditionally deleting a LIVE node's heartbeat declares its every long-held
  # shard instantly stealable (locks are never renewed in heartbeat mode, so their
  # frozen TTLs are long expired) while the victim keeps serving — fleet-wide
  # split-brain. So: stale-by-margin ⇒ dead for sure, clear now (the #6 fast path);
  # still fresh ⇒ can't tell "crashed seconds ago" from "alive", re-check after one
  # renew interval (a live renewer will have advanced the expiry; a dead one is
  # frozen); unreadable ⇒ leave it (fail closed — a TTL wait is the recoverable
  # direction, a spurious steal is not).
  defp judge_previous(prev_owner) do
    now = System.system_time(:millisecond)
    margin = Storage.steal_margin_ms()

    case Storage.read_heartbeat(prev_owner) do
      :not_found ->
        :ok

      {:ok, %{expires_at_ms: exp}} when now > exp + margin ->
        _ = Storage.clear_heartbeat(prev_owner)
        # Proven dead (round-2 #16) ⇒ its recently-renewed LOCKS are also dead —
        # skip the lock-TTL fallback that would otherwise block this node ~TTL+margin
        # per recently-held shard (round-2 #34).
        Storage.mark_incarnation_dead(prev_owner)
        :ok

      {:ok, %{expires_at_ms: exp}} ->
        {:recheck, prev_owner, exp}

      {:error, reason} ->
        Logger.warning(
          "could not verify previous incarnation #{prev_owner} before clearing " <>
            "(#{inspect(reason)}); leaving its heartbeat (fail closed)"
        )

        :ok
    end
  end

  defp incarnation_file do
    Path.join(Path.dirname(Fathom.Shard.db_path("x")), ".incarnation")
  end

  defp mark_lapse(state) do
    gen = state.generation + 1
    :persistent_term.put(gen_key(state.owner), gen)

    Logger.warning(
      "heartbeat lapsed for #{state.owner} (generation #{gen}); coordinators will revalidate"
    )

    :telemetry.execute([:fathom, :shard, :heartbeat, :lapsed], %{generation: gen}, %{
      owner: state.owner
    })

    broadcast_lapse(gen)
    publish_status(%{state | generation: gen, lapsed: true})
  end

  # Best-effort: the lapse broadcast is observability/proactive-revalidation only, so
  # a missing PubSub (e.g. the scale/bench harness runs without it) must not crash
  # the heartbeat.
  defp broadcast_lapse(gen) do
    Phoenix.PubSub.broadcast(Fathom.PubSub, @topic, {:heartbeat_lapsed, gen})
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  # FIXED RATE, measured from the start of the cycle (expert review 2026-08-26 #14). This used to
  # arm the next tick `renew_ms` after the previous attempt RETURNED, so the renewal cadence
  # degraded in proportion to store latency — precisely backwards for a liveness signal, which
  # needs to try HARDER as the store gets slower, not less often. A cycle that consumed its whole
  # budget now re-fires immediately rather than adding another full interval on top.
  defp schedule_renew(state, started) do
    if state.timer, do: Process.cancel_timer(state.timer)
    elapsed = System.monotonic_time(:millisecond) - started
    %{state | timer: Process.send_after(self(), :renew, renew_delay_ms(state.renew_ms, elapsed))}
  end

  # PURE so the cadence rule can be pinned without a timing test (AGENTS.md: a wall-clock
  # assertion here is the flake shape, and `@doc false` public is how `Shards.report_fork/2` and
  # `Shard.lapse_spread_ms/1` are tested for the same reason).
  # `round/1` is not decoration: the result flows straight into `Process.send_after/3`, which
  # requires a non-negative INTEGER, and `renew_ms` reaches this from an untyped state map — so
  # dialyzer can only see `number()` here and correctly refused the `non_neg_integer()` spec
  # without it. Normalizing at the boundary is the honest fix; widening the spec to admit a float
  # would document a value `send_after` would then reject at runtime.
  @doc false
  @spec renew_delay_ms(pos_integer(), integer()) :: non_neg_integer()
  def renew_delay_ms(renew_ms, elapsed_ms), do: max(round(renew_ms - elapsed_ms), 0)
end
