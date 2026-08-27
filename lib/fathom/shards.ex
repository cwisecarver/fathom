defmodule Fathom.Shards do
  @moduledoc """
  Find-or-start router for shard coordinators. Resolves a `shard_id` to its
  `Fathom.Shard` process — starting it on demand under the shard
  `DynamicSupervisor` — and hands back the path to the shard's database file.
  Callers open their own `Fathom.Shard.Connection` against that path (one per
  Hrana stream), which is what keeps per-stream transactions isolated.
  """
  require Logger

  @registry Fathom.ShardRegistry
  @supervisor Fathom.ShardSupervisor

  @default_drain_ms 5_000

  # drain/2's safety-net bound: how long a caller waits for the coordinator to EXIT after
  # the drain window before giving up (it normally replies via DOWN/:drain_aborted well
  # before this). The eviction path uses a much shorter budget instead — see evict/2.
  @default_exit_grace_ms 30_000

  # Total time the at-capacity admission path will spend WAITING for eviction to free a
  # slot before refusing (expert review 2026-07-14 #7). Bounds a new-tenant checkout's
  # coupling to OTHER tenants' flush-to-S3 latency: an evicted coordinator still runs its
  # full checkpoint + flush + drop + release in terminate (durability/lease unchanged) —
  # we just stop BLOCKING the checkout (Filo stream) process on it, so under slow/hung S3
  # admission 503s fast and the LB retries (the eviction completes in the background).
  @default_evict_budget_ms 2_000

  # Expert review #20: on a `{:shard_held}` at a pinned handoff target, how long a checkout will
  # HOLD + retry the acquire (backoff, capped) before falling back to the error — so the first
  # post-flip requests queue for the source's drain window instead of erroring. 0 disables the
  # hold (immediate error, the pre-#20 behavior). Only ever reached when the shard is pinned to
  # this node (a rebalancer handoff), so it's inert unless rebalancing is armed.
  @default_held_budget_ms 10_000
  @initial_held_backoff_ms 50
  @max_held_backoff_ms 1_000

  # Spread added to a sleep aimed at a known steal instant (expert review 2026-08-26 #23). Small
  # against a multi-second TTL wait, large enough to de-synchronize a whole keyspace slice whose
  # holders crashed together.
  @steal_wait_jitter_ms 100

  # Node-level graceful drain (expert review #28). Defaults sized for the common ~60s SIGTERM grace:
  # budget under 60s, a per-shard flush grace so a busy shard finishes its final flush before the
  # deadline, and bounded fan-out concurrency so the drain doesn't fire every coordinator's flush at
  # once (the #17 storm). All operator-tunable via config.
  @default_drain_all_budget_ms 55_000
  @default_drain_all_concurrency 16
  @default_drain_all_flush_grace_ms 5_000

  # Floor on a per-wave drain slice (expert review 2026-08-26 #5). With a large open-shard count
  # the computed slice can round toward zero, and a slice under the flush grace means
  # `drain_pid/2` asks the coordinator to drain in <= 0 ms — i.e. it never really tries. Better to
  # overshoot the budget slightly on a huge fleet than to convert every shard into a no-op drain.
  @min_drain_slice_ms 1_000

  # Expert review #21: how long a checkout HOLDS + retries a `{:shard_held}` when the holder is a
  # CRASHED node — heartbeat frozen and aging out within this window, so the steal is imminent. Turns
  # the TAIL of the hard-crash TTL window (RTO floor ≈ :shard_lease_ttl_ms + steal_margin) into
  # latency instead of client 503s. 0 disables. Separate from the handoff budget above — a crash and
  # a voluntary handoff have different natures and an operator may want to tune them apart.
  @default_crash_hold_ms 5_000

  # Mirrors `Fathom.Shard`'s own `@default_shutdown_ms` and reads the SAME config key, so
  # `stop_and_await/1` waits exactly as long as the supervisor's child `shutdown` budget would
  # have (expert review 2026-08-26 #9). Kept in sync by `shards_stop_test.exs`, which asserts the
  # two agree rather than trusting this comment.
  @default_shard_shutdown_ms 60_000

  @typedoc """
  A held checkout: the coordinator, the ref that identifies THIS checkout, and the local file path.

  The `ref` is not decoration — `Fathom.Shard.checkin/2` takes it, and the coordinator monitors the
  checking-out process against it, so a stream that dies without checking in is still released.
  """
  @type checkout :: {:ok, pid(), reference(), Path.t()}

  @doc """
  Ensures `shard_id`'s coordinator is running and checks out the shard for the
  caller, returning `{:ok, coordinator_pid, ref, path}` (or `{:error, reason}` for
  an invalid id / start failure). The caller opens its own connection at `path`
  and passes `ref` back via `Fathom.Shard.checkin/2` when it closes.
  """
  # `term()` in, not `ShardId.t()`: this is a TRUST BOUNDARY. It is called with request-derived
  # values and its first act is `ShardId.cast/1`, so promising a validated id here would describe
  # the opposite of what it does.
  @spec checkout(term()) :: checkout() | {:error, term()}
  def checkout(shard_id) do
    # Normalize (validate + downcase) at this trust boundary so the whole checkout — telemetry
    # tag, ensure, record_use, ShardLoad — and every downstream key uses the one canonical id
    # (finding #19). Direct callers (migration jobs, admin, tests) are covered here;
    # request-path callers are covered again in Fathom.ShardExecutor.open/1 (cast is idempotent).
    case Fathom.ShardId.cast(shard_id) do
      {:ok, id} ->
        # A `:telemetry.span` so each checkout is an OpenTelemetry trace span (cold-open cost shows
        # up here on a cold checkout) and a duration metric, tagged by outcome. See Fathom.Telemetry.
        :telemetry.span([:fathom, :shards, :checkout], %{shard_id: id}, fn ->
          result = do_checkout(id, 3)
          {result, %{shard_id: id, outcome: checkout_outcome(result)}}
        end)

      :error ->
        {:error, :invalid_shard_id}
    end
  end

  defp do_checkout(shard_id, attempts, held \\ nil) do
    with :ok <- maybe_lazy_migrate(shard_id),
         {:ok, pid} <- ensure(shard_id),
         {:ok, ref, path} <- Fathom.Shard.checkout(pid) do
      record_use(shard_id)
      # Per-shard load: the checkout (traffic) signal for the rebalancer. Lock-free
      # ETS bump, gated + off by default (see Fathom.ShardLoad).
      Fathom.ShardLoad.record_checkout(shard_id)
      # Recency for idle-eviction has ONE writer — the coordinator's release/1 stamps it
      # when the shard goes back to idle (review 2026-07-23 #17; a caller-side touch here
      # double-stamped every cycle from a second process). While a stream is held the
      # shard is busy-filtered from eviction anyway, so the release stamp is the one that
      # defines its LRU order.
      {:ok, pid, ref, path}
    else
      # Expected in-flight handoff (expert review #20): the LB flips to the target BEFORE the
      # source drains, so this node's acquire is `{:shard_held, source}` for the drain window.
      # `retry_checkout?` deliberately excludes `:held` (a genuinely-held foreign lease must not
      # spin), and the tenant's driver doesn't retry a mid-request 503 — so a handoff turns into a
      # burst of client errors on the HOTTEST shard. If this node is the pinned handoff target,
      # hold and retry the acquire with backoff up to a bounded budget (~the drain window) so the
      # first post-flip requests QUEUE instead of erroring. Anything else (foreign lease, no pin,
      # budget exhausted) falls back to the current error.
      {:error, {:shard_held, _}} = err ->
        held_retry(shard_id, attempts, held, err)

      # Race: `ensure` resolved a coordinator that lost a race with its own lifecycle, so a
      # 1 ms re-resolve to a fresh coordinator fixes it (see retry_checkout?/1). Bounded so a
      # genuinely unavailable shard still surfaces the error rather than spinning. Only the
      # (rare) race path sleeps; the happy path is untouched.
      {:error, reason} when attempts > 1 ->
        if retry_checkout?(reason) do
          Process.sleep(1)
          do_checkout(shard_id, attempts - 1, held)
        else
          {:error, reason}
        end

      other ->
        other
    end
  end

  # `held` is nil until the first `{:shard_held}` error, then
  # `{deadline_ms, next_backoff_ms, stealable_at_ms | nil}`.
  # On the FIRST held error we decide once whether this is our in-flight handoff (one Overrides
  # read); if so we open a time-bounded retry window and carry the deadline so we don't re-query
  # Postgres per retry. A reverted handoff just exhausts the (bounded) budget and then errors.
  defp held_retry(shard_id, attempts, nil, {:error, {:shard_held, owner}} = err) do
    cond do
      # #20 — an in-flight handoff to THIS node: hold for the source's drain window. No steal
      # instant to aim at — the source releases when its drain finishes, which is not a clock
      # value anyone can read — so this one polls.
      handoff_held_budget_ms() > 0 and handoff_pin_here?(shard_id) ->
        start_held_retry(shard_id, attempts, handoff_held_budget_ms(), :handoff_wait, err, nil)

      # #21 — a hard-crash failover: the holder's heartbeat is frozen (dead) and ages out within the
      # budget, so the steal is imminent. Hold the TAIL of the TTL window as latency instead of a
      # 503. A LIVE holder keeps renewing (its expiry stays ~ttl ahead > budget), so this never
      # holds for one.
      crash_failover_hold_ms() > 0 ->
        case holder_stealable_at(shard_id, owner, crash_failover_hold_ms()) do
          {:ok, at} ->
            start_held_retry(shard_id, attempts, crash_failover_hold_ms(), :crash_wait, err, at)

          :no ->
            err
        end

      true ->
        err
    end
  end

  defp held_retry(shard_id, attempts, {deadline, backoff, stealable_at}, err),
    do: backoff_held(shard_id, attempts, deadline, backoff, stealable_at, err)

  defp start_held_retry(shard_id, attempts, budget, event, err, stealable_at) do
    :telemetry.execute([:fathom, :shards, event], %{count: 1}, %{shard_id: shard_id})
    deadline = System.monotonic_time(:millisecond) + budget
    backoff_held(shard_id, attempts, deadline, @initial_held_backoff_ms, stealable_at, err)
  end

  # Whether the foreign holder's lease will become stealable within `budget`. A crashed holder's
  # signals are frozen at crash+ttl, so as they age this crosses into the budget (the crash-window
  # TAIL); a LIVE holder keeps renewing, so its stealable instant stays ~ttl ahead of now and this
  # is false — we never hold for a live owner.
  #
  # ASKS THE BACKEND rather than re-deriving the rule. This used to read the heartbeat itself and
  # compare `hb_exp + steal_margin` against the budget. Then #12 changed the rule under it: an
  # owner is dead only when BOTH its heartbeat and its lock TTL have lapsed. The predictor and the
  # performer disagreed from that point on, so a checkout whose holder had a lapsing heartbeat but
  # a still-fresh lock would hold and retry for the WHOLE budget waiting for a steal that could
  # not happen yet, then return the same error it would have returned immediately — the tenant
  # pays `crash_failover_hold_ms` of latency for nothing. Bounded, and never wrong in the unsafe
  # direction, but pure waste on an already-failing request.
  #
  # `lease_stealable_at/1` and `owner_live?/3` are derived from ONE function inside each backend,
  # so this class of drift cannot recur: whatever teaches the steal a new condition teaches the
  # prediction the same one.
  #
  # A different owner than the one our `acquire_lease` raced against ⇒ don't hold: the lock moved
  # while we were asking, so the error we already have is the fresher answer. `:free` likewise —
  # it is stealable NOW, so retrying immediately (the normal retry path) is right, not a hold.
  #
  # RETURNS THE INSTANT, it does not merely answer yes/no (expert review 2026-08-26 #23). The
  # backend has just computed the exact system-clock millisecond at which the hold becomes
  # stealable; the old shape reduced that to a boolean and then had `backoff_held/6` rediscover it
  # by polling — 50, 100, 200, 400, 800, 1 000, 1 000, 1 000 ms, roughly eight retries inside the
  # 5 s budget, each of which re-enters `do_checkout/3` and re-pays a create_lock PUT plus a
  # get_lock GET. Sleeping TO the known instant makes it one or two.
  defp holder_stealable_at(shard_id, owner, budget) do
    case Fathom.Shard.Storage.lease_stealable_at(shard_id) do
      {:held, ^owner, at} ->
        if at - System.system_time(:millisecond) <= budget, do: {:ok, at}, else: :no

      _ ->
        :no
    end
  rescue
    _ -> :no
  catch
    :exit, _ -> :no
  end

  defp crash_failover_hold_ms,
    do: Application.get_env(:fathom, :crash_failover_hold_ms, @default_crash_hold_ms)

  # Sleeps to the steal instant when one is known, otherwise backs off (expert review #23).
  #
  # `stealable_at` is used AT MOST ONCE and then cleared. Two reasons, and the second is the one
  # that bites: the instant is now in the past, so recomputing the wait would be <= 0 and the loop
  # would busy-spin through the rest of the budget; and the steal may legitimately not have
  # happened on the first attempt (a peer won it, a clock differs), for which polling is the right
  # shape. So it is one aimed sleep followed by today's backoff.
  #
  # `remaining` is a monotonic-clock duration and `stealable_at` a system-clock instant. Only the
  # DURATION derived from each is compared, never the instants themselves — a system-clock step
  # can make the aimed wait wrong, and the `min(_, remaining)` clamp plus the budget deadline is
  # what bounds the damage to today's behaviour.
  defp backoff_held(shard_id, attempts, deadline, backoff, stealable_at, err) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining > 0 do
      wait = min(held_wait_ms(backoff, stealable_at), remaining)

      # Emitted so the RETRY SHAPE is observable, not just the outcome. Both shapes end in a served
      # request, so nothing in the result distinguishes "slept once to the known instant" from
      # "polled eight times to rediscover it" — the same reason #3's flush route and #5's drain
      # window are emitted rather than inferred.
      :telemetry.execute(
        [:fathom, :shards, :held_retry],
        %{wait_ms: wait},
        %{shard_id: shard_id, aimed: stealable_at != nil}
      )

      Process.sleep(wait)
      next_backoff = min(backoff * 2, @max_held_backoff_ms)
      do_checkout(shard_id, attempts, {deadline, next_backoff, nil})
    else
      err
    end
  end

  # Jittered so a takeover of many shards — whose holders all crashed at ~the same moment, and
  # whose steal instants are therefore ~the same instant — does not wake every waiter together and
  # hand the survivor a synchronized burst of lock PUTs.
  defp held_wait_ms(backoff, nil), do: backoff

  defp held_wait_ms(backoff, stealable_at) do
    case stealable_at - System.system_time(:millisecond) do
      ahead when ahead > 0 -> ahead + :rand.uniform(@steal_wait_jitter_ms)
      _ -> backoff
    end
  end

  # Whether this node is the pinned target of an active (not reverted) handoff for this shard —
  # i.e. a `{:shard_held}` here is the expected post-flip / pre-drain window, not a foreign lease.
  # Fails CLOSED (false ⇒ surface the error) on any Postgres doubt, so a directory blip never
  # converts a real held-error into a multi-second stall.
  defp handoff_pin_here?(shard_id) do
    case Fathom.Rebalancer.Overrides.for_shard(shard_id) do
      %{pinned_node: node, failed_at: nil} -> node == Fathom.Rebalancer.node_key()
      _ -> false
    end
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp handoff_held_budget_ms,
    do: Application.get_env(:fathom, :handoff_held_retry_budget_ms, @default_held_budget_ms)

  # Two checkout errors are transient lifecycle races, not real failures — both clear on a
  # re-resolve to a fresh coordinator:
  #   * `:unavailable` (`:noproc`) — the coordinator had already stopped and its Registry
  #     entry lingered in the window before the Registry handled the `:DOWN`.
  #   * `:normal` — the checkout call was queued behind an `:idle_timeout`/drain stop, so the
  #     coordinator processed the stop first and the pending `GenServer.call` exited `:normal`.
  #     Idle stops are routine at scale, so without this a steady trickle of checkouts a 1 ms
  #     retry would have fixed surfaced as spurious `{:error, :normal}` to the client.
  @doc false
  @spec retry_checkout?(term()) :: boolean()
  def retry_checkout?(reason), do: reason in [:unavailable, :normal]

  defp checkout_outcome({:ok, _, _, _}), do: :ok
  defp checkout_outcome({:error, {:shard_held, _}}), do: :held
  defp checkout_outcome({:error, :unavailable}), do: :unavailable
  defp checkout_outcome({:error, :node_at_capacity}), do: :at_capacity
  defp checkout_outcome({:error, :novel_shard_rate_limited}), do: :novel_rate_limited
  defp checkout_outcome({:error, _}), do: :error

  # Migrate-on-touch: once a fleet version is released, a shard behind HEAD can serve the new
  # app version's traffic only after it migrates. Three modes (expert review #40), the whole
  # feature off by default — the deliberate hot-path/Postgres exception (a directory-cache would
  # remove the per-checkout reads):
  #
  #   :off (default) — nothing on checkout; the hourly ReconcileJob converges the cold tail (a
  #     stale-schema window up to the cron interval; expand-contract makes serving vN-1 correct).
  #   :async — spot the laggard and ENQUEUE its ShardMigrationJob (deduped per shard), then serve
  #     vN-1 THIS request. Converges the touched tenant within a job cycle with no inline blocking —
  #     the middle ground.
  #   :inline — block the checkout on the full blue/green migration (drain + copy + replay + S3
  #     round-trips). Zero stale window, but multi-second first-request latency at real S3 latency.
  #
  # `:migrate_on_touch` selects the mode; the legacy boolean `:lazy_migrate, true` maps to :inline.
  defp maybe_lazy_migrate(shard_id) do
    case migrate_on_touch_mode() do
      :off -> :ok
      :async -> enqueue_migrate_on_touch(shard_id)
      :inline -> lazy_migrate(shard_id)
    end
  end

  @doc """
  The resolved migrate-on-touch mode: `:off` | `:async` | `:inline`.

  Public because `Fathom.Migrator.HeadCache` has to gate its background poll on the SAME answer.
  It used to read `:lazy_migrate` directly, so turning on the current `:migrate_on_touch` knob
  enabled the cache's only consumer without enabling the poll that fills it — the cache stayed at
  its initial 0, `head > 0` was never true, and migrate-on-touch silently did nothing in either
  mode. One predicate, one place, so the two cannot drift again.
  """
  @spec migrate_on_touch_mode() :: :off | :async | :inline
  def migrate_on_touch_mode do
    case Application.get_env(:fathom, :migrate_on_touch) do
      mode when mode in [:off, :async, :inline] ->
        mode

      _ ->
        # Backward compatibility: the pre-#40 `:lazy_migrate` boolean is the inline mode.
        if Application.get_env(:fathom, :lazy_migrate, false), do: :inline, else: :off
    end
  end

  # Enqueue-on-touch: hand the laggard to the async rollout and serve vN-1 now. Best-effort — a
  # control-plane blip must never fail the checkout (the whole point of not blocking inline). The
  # job's `unique: [keys: [:shard_id]]` dedups, so repeated touches of the same laggard don't pile
  # up jobs.
  defp enqueue_migrate_on_touch(shard_id) do
    head = Fathom.Migrator.HeadCache.get()

    if head > 0 and behind?(shard_id, head) do
      %{shard_id: shard_id, target: head}
      |> Fathom.Migrator.ShardMigrationJob.new()
      |> Oban.insert()
    end

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp lazy_migrate(shard_id) do
    # HEAD from the TTL cache (persistent_term), not a per-checkout Postgres
    # `max(version)` — see Fathom.Migrator.HeadCache.
    head = Fathom.Migrator.HeadCache.get()

    if head > 0 and behind?(shard_id, head) do
      case Fathom.Migrator.ShardMigration.run(shard_id, head) do
        :ok ->
          :ok

        {:ok, _} ->
          :ok

        # Nothing stored to migrate yet: a brand-new shard was born empty (or, with
        # :fork_from_template enabled, already at HEAD via maybe_fork_novel/1) and
        # hasn't flushed — there is nothing behind HEAD to copy.
        {:error, :no_live_object} ->
          :ok

        # HEAD dropped (a yank) between this node's cache refresh and the run
        # (round-2 #23): the target no longer exists, and serving the shard at its
        # OLD version is exactly correct — never fail the client checkout for a
        # version the fleet just reverted away from.
        {:error, {:unknown_version, _}} ->
          :ok

        # Another worker holds it / it's busy — the caller retries.
        {:retry, reason} ->
          {:error, {:shard_migrating, reason}}

        # A migration that FAILED (bad captured SQL, a shard whose schema was advanced out of band,
        # anything deterministic) used to fail the CHECKOUT, which takes the tenant completely
        # offline — the client sees an opaque stream error on every request, forever, because the
        # next checkout retries the same doomed copy. That is the worst possible response: the shard
        # is intact at its current version, and serving vN-1 is safe by the same expand-contract
        # argument that makes `:off` and `:async` correct modes. So degrade to exactly what `:async`
        # does — hand the shard to the async rollout (which owns retry and quarantine) and serve it
        # now — instead of an outage plus a multi-second failed copy on every request.
        #
        # Found live 2026-07-31: a demo fleet whose tenants had been provisioned by a direct
        # per-shard `manage.py migrate` (so their schema was ahead of fathom's stamp) had every one
        # of its pages fail with STREAM_NOT_FOUND the moment a version was released, because the
        # replay hit "table already exists" on each page view.
        {:error, reason} ->
          Logger.error(
            "shard #{shard_id}: inline migrate-on-touch to v#{head} FAILED (#{inspect(reason)}); " <>
              "serving the shard at its current version and handing it to the async rollout. If " <>
              "this repeats, the shard's schema is likely ahead of its fathom version stamp — a " <>
              "direct `manage.py migrate` against a tenant does that (see :block_tenant_ddl)."
          )

          :telemetry.execute(
            [:fathom, :migrator, :inline_migrate_failed],
            %{count: 1},
            %{shard_id: shard_id, target: head}
          )

          enqueue_migrate_on_touch(shard_id)
      end
    else
      :ok
    end
  end

  defp behind?(shard_id, head) do
    # NEVER migrate-on-touch the reserved capture template. Django migrates it directly, so its
    # directory stamp never advances and it perpetually reads as a laggard — but replaying its OWN
    # captured DDL back onto itself is "already exists", and a drain racing an in-flight
    # `manage.py migrate` can drop the capture buffer and fork the fleet from the template.
    #
    # `Fathom.Directory.laggard_query/1` already excludes it for the reconcile/rollout sweep (expert
    # review 2026-07-14 #8). Migrate-on-touch (expert review #40) added a SECOND, independent
    # "is this shard behind HEAD?" path and did not inherit that exclusion, which reopened the same
    # bug through a different door: with `:inline`, every connection Django opens to migrate the
    # template first blocks on a doomed self-migration (capture then never sees the real migration —
    # the fleet silently stops receiving versions); with `:async` it enqueues a ShardMigrationJob per
    # touch that retries into `migration_failed` quarantine. Either way, turning on the documented
    # convergence knob broke the migration engine's own entry point.
    if capture_template?(shard_id) do
      false
    else
      case Fathom.Directory.get(shard_id) do
        {:ok, %{schema_version: v}} -> v < head
        # Not yet in the directory (brand-new): nothing to migrate — the shard is born
        # empty (or at HEAD via the gated fork-from-template, which registers its row).
        :error -> false
      end
    end
  end

  # Compare NORMALIZED ids (finding #19), so a mixed-case :template_shard_id still matches.
  defp capture_template?(shard_id) do
    case Fathom.ShardId.cast(Application.get_env(:fathom, :template_shard_id)) do
      {:ok, id} -> id == shard_id
      :error -> false
    end
  end

  # Record the shard's use in the Postgres directory (control plane). Off the hot
  # path: a lock-free ETS buffer write that `Fathom.Directory.Recorder` coalesces
  # and batch-flushes. Gated by config; the data path never blocks on or fails
  # because of the directory, so a Postgres outage just means a missed touch.
  defp record_use(shard_id) do
    if Application.get_env(:fathom, :directory_touch, true) do
      Fathom.Directory.Recorder.record(shard_id)
    end

    :ok
  end

  @doc """
  Stands a shard's coordinator down so the migrator can take over: refuse new
  checkouts, let in-flight connections drain (up to `drain_timeout` ms), flush the
  latest data to storage, drop the local copy, release the lease, and stop.

  Returns `:ok` once the coordinator has fully stopped (data is durable in storage
  and the lease is free), or `:ok` if it wasn't running (already cold). Returns
  `{:error, :busy}` if connections didn't drain in time — the coordinator keeps
  serving and the caller should retry later. Blocks until the coordinator exits.
  """
  # `ShardId.t()`, not `term()`: unlike `checkout/1` this is an INTERNAL caller's entry point
  # (eviction, the rebalancer handoff, tenant delete), and every one of them already holds a cast
  # id. It looks the shard up in the registry rather than casting, so an uncast id would silently
  # miss instead of being rejected.
  @spec drain(Fathom.ShardId.t(), non_neg_integer()) :: :ok | {:error, term()}
  def drain(shard_id, drain_timeout \\ @default_drain_ms) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        ref = Process.monitor(pid)
        Fathom.Shard.request_drain(pid, drain_timeout, self())

        # Preserve drain/2's public contract for its migration/admin callers: the
        # safety-net timeout still reads as :busy (the coordinator normally replies via
        # DOWN or :drain_aborted well before this). Only the eviction path (evict/2) uses
        # a shorter budget and keeps the :exit_timeout as "couldn't confirm this exited".
        case await_coordinator_exit(pid, ref, drain_timeout + @default_exit_grace_ms) do
          {:error, :exit_timeout} -> {:error, :busy}
          other -> other
        end
    end
  end

  @doc """
  Node-level graceful drain (expert review #28): the ordered voluntary alternative to funnelling
  every deploy / scale-down through the crash-adjacent supervisor-shutdown path. Marks this node
  draining (`Fathom.HealthPlug.begin_draining/0`, so the LB deregisters it first), optionally waits
  `:drain_lb_settle_ms` for the LB to notice, then fans out `request_drain` across every open
  coordinator — refuse new checkouts → let in-flight streams finish → flush + release the lease + stop
  — with bounded concurrency and a **bounded overall budget**. Returns `%{drained, busy, timed_out}`.

  Each coordinator gets a **slice** of the remaining budget — sized per wave of the bounded
  fan-out, so one busy shard cannot consume the whole window and strand the rest (expert review
  2026-08-26 #5; before it, this sentence described a property the code did not have). Wire it as a release **pre-stop** (`bin/fathom rpc
  "Fathom.Shards.drain_all()"`) ahead of SIGTERM — see `docs/runbooks/cluster.md`. It never
  `end_draining`s: the node is on its way down, and a cancelled drain resets the flag explicitly.
  """
  @spec drain_all(pos_integer()) :: %{
          drained: non_neg_integer(),
          busy: non_neg_integer(),
          timed_out: non_neg_integer()
        }
  def drain_all(budget_ms \\ drain_all_budget_ms()) do
    Fathom.HealthPlug.begin_draining()
    settle = drain_lb_settle_ms()
    if settle > 0, do: Process.sleep(settle)

    deadline = System.monotonic_time(:millisecond) + budget_ms
    pids = Registry.select(@registry, [{{:_, :"$1", :_}, [], [:"$1"]}])

    # A SLICE OF THE BUDGET, NOT ALL OF IT (expert review 2026-08-26 #5).
    #
    # This used to pass `deadline - now` to every coordinator, i.e. the WHOLE remaining budget,
    # while the docstring above claimed "each coordinator gets a slice". With the defaults
    # (55 s budget, concurrency 16, 5 s flush grace) the first 16 coordinators each got a 50 000 ms
    # drain window. A coordinator with a checked-out stream sets `draining: true` and arms a timer
    # for that whole window, exiting early only if its streams happen to check in — so a handful of
    # busy shards in the FIRST WAVE burned the entire budget, `deadline - now` went <= 0, and
    # `drain_pid/2`'s first clause returned `:timed_out` for every remaining shard WITHOUT
    # CONTACTING IT. Those shards then took the unbounded supervisor teardown, which is the exact
    # path `Fathom.Application.prep_stop/1` exists to avoid: "every open coordinator ran its
    # checkpoint/snapshot + full-object PUT at once… whatever had not finished when the shutdown
    # budget expired was killed mid-flush."
    #
    # So on a rolling deploy of a node serving long-lived django-libsql streams, the voluntary
    # drain drained close to nothing and reported `busy: 16, timed_out: N`.
    #
    # The slice is computed per WAVE (`ceil(pending / concurrency)`), not per shard, because the
    # waves run sequentially while the shards within a wave run in parallel. `pending` is recomputed
    # from a counter as the stream progresses, so shards that drain quickly hand their unused time
    # to the ones behind them rather than the slice being fixed up front.
    waves = max(1, ceil(length(pids) / drain_all_concurrency()))
    remaining_count = :counters.new(1, [:atomics])
    :counters.put(remaining_count, 1, length(pids))

    pids
    |> Task.async_stream(
      fn pid ->
        left = max(0, deadline - System.monotonic_time(:millisecond))
        pending = max(1, :counters.get(remaining_count, 1))
        waves_left = max(1, min(waves, ceil(pending / drain_all_concurrency())))
        slice = max(@min_drain_slice_ms, div(left, waves_left))
        :counters.sub(remaining_count, 1, 1)
        window = min(slice, left)

        # The window each coordinator was actually given. Emitted because it is the DECISION this
        # fix makes, and it is otherwise unobservable: a shard handed a zero window aborts `:busy`
        # immediately, which is indistinguishable from a shard that waited its full slice and then
        # aborted. (The audit predicted the leftovers would show up as `:timed_out`; traced, they
        # do not — a zero-window shard returns fast rather than exhausting the budget, so the
        # outcome tally looks identical either way.)
        :telemetry.execute(
          [:fathom, :shards, :drain_all, :slice],
          %{window_ms: window},
          %{}
        )

        drain_pid(pid, window)
      end,
      max_concurrency: drain_all_concurrency(),
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce(%{drained: 0, busy: 0, timed_out: 0}, fn {:ok, outcome}, acc ->
      Map.update!(acc, outcome, &(&1 + 1))
    end)
  end

  # Drain one coordinator within `remaining` ms of budget. The coordinator waits for its streams up
  # to `remaining - flush_grace`, then flushes + releases + stops within the grace, so it exits
  # before the budget deadline and we observe `:drained`; a still-busy shard aborts (`:busy`) and one
  # that neither finishes nor aborts in the window is `:timed_out` (left to the supervisor shutdown).
  defp drain_pid(_pid, remaining) when remaining <= 0, do: :timed_out

  defp drain_pid(pid, remaining) do
    ref = Process.monitor(pid)
    Fathom.Shard.request_drain(pid, max(0, remaining - drain_all_flush_grace_ms()), self())

    case await_coordinator_exit(pid, ref, remaining) do
      :ok -> :drained
      {:error, :busy} -> :busy
      _ -> :timed_out
    end
  end

  defp drain_all_budget_ms,
    do: Application.get_env(:fathom, :drain_all_budget_ms, @default_drain_all_budget_ms)

  defp drain_all_concurrency,
    do: Application.get_env(:fathom, :drain_all_concurrency, @default_drain_all_concurrency)

  defp drain_all_flush_grace_ms,
    do: Application.get_env(:fathom, :drain_all_flush_grace_ms, @default_drain_all_flush_grace_ms)

  defp drain_lb_settle_ms, do: Application.get_env(:fathom, :drain_lb_settle_ms, 0)

  @doc """
  Force-stops `shard_id`'s coordinator for tenant deletion (#15). Unlike `drain/2`, it does
  NOT wait for in-flight connections — a delete deliberately kicks them — and it terminates
  the coordinator via the supervisor **while its lease is still valid**, so the shutdown
  flushes/releases cleanly (or is brutal-killed) and NEVER takes the self-fence path that
  would quarantine the tenant's data to a `.fenced.<ts>` file the caller would then have to
  hunt down. Caller purges storage AFTER this returns. Returns `:ok` (stopped or already cold).
  """
  @spec stop(Fathom.ShardId.t()) :: :ok
  def stop(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        stop_and_await(pid)
    end
  end

  # The stop AND the wait both happen in the CALLING process (expert review 2026-08-26 #9).
  #
  # This was `DynamicSupervisor.terminate_child/2`, which is a `handle_call` served INSIDE the
  # supervisor process: it monitors the child, sends the shutdown exit, and blocks in a `receive`
  # until the `:DOWN` arrives or the child's `shutdown` budget elapses. `Fathom.ShardSupervisor`
  # is the single process every cold open goes through (`start/1` → `DynamicSupervisor.start_child`),
  # so for the entire duration of ONE coordinator's `terminate/2` — settle + checkpoint +
  # `VACUUM INTO` + full-object PUT + lease release, seconds at real S3 latency and budgeted up to
  # `:shard_shutdown_ms` (60 s default) — no other shard on the node could be started. Filo streams
  # sit in `Shard.checkout/1`'s 75 s call rather than erroring, so it surfaced as a latency cliff,
  # not an error rate. Measured with a standalone probe: a child whose `terminate/2` slept 3 s
  # blocked an unrelated `start_child` for 2801 ms of the 3002 ms terminate.
  #
  # The ordering guarantee `Tenants.purge/1` depends on — "the coordinator has fully stopped before
  # we delete the objects" — is UNCHANGED, because this still blocks until the `:DOWN`. What
  # changes is *who* blocks: this caller, instead of the supervisor every other tenant needs.
  #
  # Safe because the coordinator is `restart: :temporary` (verified at runtime:
  # `Fathom.Shard.child_spec/1` returns `%{restart: :temporary, shutdown: 60_000}`), so one that
  # exits on its own is reaped by the supervisor's async `handle_info` and never restarted. On a
  # `:permanent` or `:transient` child this change would be wrong.
  #
  # Do NOT swap `GenServer.stop` for `Process.exit(pid, :shutdown)`: the coordinator traps exits and
  # its `handle_info({:EXIT, _, _}, state)` SWALLOWS a non-parent exit signal, so that would
  # silently do nothing at all. `GenServer.stop` goes through `:proc_lib.stop`, which a trapping
  # process handles as a system message and runs `terminate/2` for.
  defp stop_and_await(pid) do
    ref = Process.monitor(pid)
    budget = Application.get_env(:fathom, :shard_shutdown_ms, @default_shard_shutdown_ms)

    try do
      GenServer.stop(pid, :shutdown, budget)
    catch
      # It exited concurrently — the old `{:error, :not_found}` branch, also done.
      :exit, :noproc ->
        :ok

      # `terminate/2` outran its own shutdown budget. Brutal-kill it at exactly the deadline the
      # supervisor would have, so the behaviour a caller sees is unchanged.
      :exit, _ ->
        Process.exit(pid, :kill)
    end

    # `GenServer.stop/3` only returns once the process is down, and the kill above is
    # unconditional, so a `:DOWN` is already in flight on every branch. The `after` is a backstop
    # against a wedged VM, not an expected path — `stop/1` must never block a caller forever.
    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      budget ->
        Process.demonitor(ref, [:flush])
        :ok
    end
  end

  @doc """
  Force-flushes `shard_id`'s live coordinator so its current on-disk state is durable in storage,
  WITHOUT dropping/stopping it (it keeps serving) — the flush-before-fork primitive (#10). Returns
  `:ok` once the shard is durably clean, `:ok` if no coordinator is running on THIS node (nothing is
  writing here, so the stored object is already current), or `{:error, reason}` on a flush error.

  **Local only.** It flushes the coordinator on this node. A shard whose coordinator lives on its LB
  home elsewhere must be flushed on that node — fan the call out across the fleet, or route it via the
  command channel (the production generalization, the same cross-node-coordinator follow-up as
  `Fathom.Tenants.purge`'s drain). The single-home / single-node case needs neither.
  """
  @spec flush(Fathom.ShardId.t()) :: :ok | {:error, term()}
  def flush(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        try do
          Fathom.Shard.flush_now(pid)
        catch
          # A hung/slow store (or a live-writer livelock) past @flush_now_timeout: the shard is
          # NOT durably clean, so the flush-before-fork primitive (Tenants.fork flush_source:, the
          # tenant API flush endpoint) must NOT report success — a keystone-fork would clone stale
          # bytes on a swallowed timeout (expert review #14). Surface it as an error.
          :exit, {:timeout, _} ->
            {:error, :flush_timeout}

          # The coordinator legitimately went away mid-call (idle-drop flushes on its way out; a
          # steal quarantines; a normal/shutdown stop) — its stored state stands, so best-effort
          # :ok. A force-stop with a pending waiter returns via an explicit reply, not this catch
          # (review 2026-07-18 #4 settles flush_waiters in every terminate clause).
          :exit, {reason, _} when reason in [:noproc, :normal, :shutdown] ->
            :ok

          :exit, {{:shutdown, _}, _} ->
            :ok

          # Any other exit (a genuine coordinator crash reason) left durability unknown — surface
          # it rather than swallow it as a false success.
          :exit, {reason, _} ->
            {:error, {:flush_exited, reason}}

          :exit, reason ->
            {:error, {:flush_exited, reason}}
        end
    end
  end

  # Wait up to `exit_wait_ms` for the coordinator to exit after a drain request, cleaning
  # the monitor and mailbox the SAME way on every branch (expert review #41): a bounded
  # wait must never leak the monitor or a stale {:drain_aborted, pid} — a later drain/2 or
  # evict/2 pins a DIFFERENT coordinator pid, so an unmatched abort would sit in a
  # long-lived caller's mailbox forever. Returns the drain outcome; the timeout is
  # {:error, :exit_timeout} so callers can distinguish it from a busy abort (drain/2 maps
  # it to :busy; evict/2 keeps it — the coordinator is stopping in the background).
  defp await_coordinator_exit(pid, ref, exit_wait_ms) do
    receive do
      {:DOWN, ^ref, :process, ^pid, reason} ->
        flush_drain_aborted(pid)
        drain_down_result(reason)

      {:drain_aborted, ^pid} ->
        Process.demonitor(ref, [:flush])
        {:error, :busy}
    after
      exit_wait_ms ->
        Process.demonitor(ref, [:flush])
        flush_drain_aborted(pid)
        {:error, :exit_timeout}
    end
  end

  # Consume a counterpart {:drain_aborted, pid} the coordinator may have sent before
  # (or instead of) the branch we resolved on (expert review #41): a later drain/2
  # call pins a DIFFERENT coordinator pid, so a stale message left behind is never
  # matched again — an unbounded mailbox leak in long-lived callers (migration job
  # runners, admin shells) and a hazard for any future bare receive. The [:flush]
  # demonitor already covers the DOWN side symmetrically.
  defp flush_drain_aborted(pid) do
    receive do
      {:drain_aborted, ^pid} -> :ok
    after
      0 -> :ok
    end
  end

  # The drain goal — coordinator stopped, no longer holding the file/lease — is met on a
  # clean `:normal` stop and equally on `:noproc`: the coordinator had already died in the
  # window between the Registry lookup and our monitor, so it is already cold. Both are `:ok`;
  # only a genuinely abnormal exit is a drain failure.
  @doc false
  @spec drain_down_result(term()) :: :ok | {:error, {:drain_failed, term()}}
  def drain_down_result(reason) when reason in [:normal, :noproc], do: :ok
  def drain_down_result(reason), do: {:error, {:drain_failed, reason}}

  @doc "Returns `{:ok, pid}` for `shard_id`, starting the coordinator if needed."
  @spec ensure(Fathom.ShardId.t()) :: {:ok, pid()} | {:error, term()}
  def ensure(shard_id) when is_binary(shard_id) do
    cond do
      not Fathom.ShardId.valid?(shard_id) ->
        {:error, :invalid_shard_id}

      # Lifecycle denies, checked on EVERY checkout (not just a new open) so a deleted or
      # suspended tenant is refused even when a coordinator is still running — closing the
      # window between a delete/suspend and the coordinator's stop. Both are O(1) ETS lookups
      # off the Postgres hot path (`Fathom.Tenants.Tombstones` / `.Suspensions`).
      Fathom.Tenants.tombstoned?(shard_id) ->
        {:error, :shard_tombstoned}

      Fathom.Tenants.suspended?(shard_id) ->
        {:error, :shard_suspended}

      true ->
        case Registry.lookup(@registry, shard_id) do
          [{pid, _}] -> {:ok, pid}
          [] -> start_if_capacity(shard_id)
        end
    end
  end

  # Per-node admission control: only NEW opens are gated — an already-running shard
  # (the branch above) is always checkoutable. At the cap we refuse cleanly with
  # `{:error, :node_at_capacity}` so the LB/client backs off, rather than letting
  # DynamicSupervisor spawn past the fd cliff (emfile) and degrade the whole node.
  # Off by default (`:max_open_shards` == :infinity); the operator sets it from the
  # measured fd/RSS density budget (`mix fathom.scale --ramp`).
  defp start_if_capacity(shard_id) do
    # The deleted/suspended lifecycle denies are enforced in ensure/1 (above), before we ever
    # reach here — so start_if_capacity only weighs capacity + novel-rate.
    #
    # Novelty ("nothing knows the shard") is consulted by BOTH the rate limiter and
    # fork-from-template, so it's computed AT MOST ONCE per open (review 2026-07-23 #28 —
    # with both gates on it ran twice: two File.exists? stats + two synchronous directory
    # reads, doubling novel-open control-plane latency under exactly the signup/spray
    # traffic these gates target). Neither gate on ⇒ no stat and no Postgres read at all.
    cond do
      # At the cap, first try to make room by evicting the least-recently-used IDLE
      # shard (soft cap). Only if nothing idle can be evicted do we refuse — a node
      # saturated with *active* connections genuinely has no room, and 503 tells the
      # LB/client to back off rather than letting DynamicSupervisor spawn past the fd
      # cliff (emfile) and degrade the whole node.
      at_capacity?() and not evicted_for_room?() ->
        :telemetry.execute([:fathom, :shards, :at_capacity], %{count: 1}, %{shard_id: shard_id})
        {:error, :node_at_capacity}

      true ->
        # `NovelLimiter.enabled?/0`, NOT `!= nil` (expert review 2026-08-24 #20). The gate must be
        # ON only for a POSITIVE rate: `NOVEL_SHARD_RATE=0` is the natural operator spelling of
        # "disabled" — the docs say "unset = off" — and `0` is not nil, so it used to reach
        # `allow/2`, crash the limiter on a `CaseClauseError`, and (via the plane supervisor's
        # `max_restarts: 30` in `max_seconds: 10`) take the data plane down after 31 novel-shard
        # requests in ten seconds. See that function for the whole sequence.
        rate_gated? = Fathom.Shards.NovelLimiter.enabled?()
        fork_gated? = Application.get_env(:fathom, :fork_from_template, false)
        novel? = (rate_gated? or fork_gated?) and novel_shard?(shard_id)

        # The churn half of finding #14: the cap above bounds how many shards this node
        # holds open; this bounds how FAST unseen ids can mint new ones (coordinator +
        # fds + file + S3 lock PUT + Postgres row per novel id). Refused before any of
        # that work runs.
        if rate_gated? and novel? and limiter_refused?(shard_id) do
          {:error, :novel_shard_rate_limited}
        else
          if fork_gated? and novel?, do: fork_novel(shard_id)
          start(shard_id)
        end
    end
  end

  # Fork-from-template (finding #10), gated OFF by default (`:fork_from_template`): when
  # enabled and this id is genuinely NOVEL, birth it AT HEAD by copying the retained
  # `template@HEAD` snapshot into its stored object (Fathom.Migrator.fork_from_template/1)
  # BEFORE the coordinator starts — the coordinator's normal cold-open pull then picks the
  # forked object up, so the lease/pull path is untouched. Best-effort in every direction:
  # any fork outcome other than success (no release yet, no snapshot retained, storage or
  # Postgres trouble, a concurrent forker holding the lease) falls back to today's
  # born-empty behavior — a checkout is NEVER failed for the fork. Flag off ⇒ one get_env
  # on the (already cold) novel-open path, nothing on the hot path.
  #
  # The fallback is LOUD (2026-07-31). With the flag on, the fork IS the birth path: a tenant
  # that falls back is born with no schema, so its first ORM query fails, and the rollout
  # cannot rescue it either — `django_migrations` is created by Django's recorder in
  # autocommit BEFORE any migration, so it belongs to no captured version and replaying v1
  # onto an empty file dies on `no such table: django_migrations` (pinned as characterization
  # in test/fathom/migrator/django_replay_test.exs). Silently swallowing the outcome made that
  # tenant indistinguishable from a healthy one. It still never fails the checkout — a fleet
  # whose object store blips must not stop minting tenants — but it is now alertable.
  defp fork_novel(shard_id) do
    # The rescue wraps ONLY the fork, so the reporter below can never re-enter it. A raise
    # inside the rescue clause is no longer caught, and this runs on the checkout path.
    outcome =
      try do
        Fathom.Migrator.fork_from_template(shard_id)
      rescue
        e -> {:error, {:exception, e}}
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end

    report_fork(outcome, shard_id)
  end

  # Public (`@doc false`) so the CLASSIFICATION can be tested directly. Which outcomes are benign
  # and which page is the whole content of this function — expert review 2026-08-20 #35 was a
  # misclassification, not a logic error — and reaching every branch through a real fork race would
  # need a concurrent lease holder per case.
  @doc false
  # Born at HEAD — the intended path, no noise.
  def report_fork({:ok, _}, _shard_id), do: :ok

  # The reserved capture template opening itself. Django migrates it directly and it is
  # excluded from every rollout sweep; refusing to fork it onto itself is correct, not a
  # fallback worth alarming on.
  def report_fork({:error, :template_shard}, _shard_id), do: :ok

  # A stored object already exists for this id (a flushed-then-forgotten shard whose
  # directory row is gone). The fork correctly refuses to clobber it, and the shard is NOT
  # born empty — it cold-opens onto its own data. Not a fallback.
  def report_fork({:error, :dst_exists}, _shard_id), do: :ok

  # A CONCURRENT FORKER WON, WHICH MEANS THE FORK SUCCEEDED (expert review 2026-08-20 #35).
  #
  # `ensure/1` is a find-or-start with a genuine race: two callers can both see
  # `Registry.lookup == []` and both enter `start_if_capacity/1`, and with `:fork_from_template`
  # on both call `fork_novel/1`. The loser gets `{:retry, reason}` — a DECLARED return of
  # `Migrator.fork_from_template/1`, and the comment above `fork_novel/1` already names "a
  # concurrent forker holding the lease" as expected.
  #
  # It fell through to the generic clause, which logs at ERROR that "the tenant is born EMPTY and
  # serving with NO schema … Delete the tenant and re-mint it", and pages via
  # `[:fathom, :migrator, :fork_fallback]` on ANY occurrence. All of that is FALSE here: the other
  # caller's fork succeeded and the tenant has its schema. An ordinary signup race produced a
  # page-worthy alarm instructing an operator to delete a healthy tenant.
  #
  # Still reported, on a DISTINCT event, because it is not nothing: it means two opens raced, and
  # the loser has already spent a `NovelLimiter` token, so one novel id can consume two. Keeping it
  # on `fork_fallback` would defeat the point — that alert pages on any occurrence precisely
  # because a born-empty tenant is a silent hard outage.
  def report_fork({:retry, reason}, shard_id) do
    Logger.info(
      "shard #{shard_id}: fork-from-template deferred to a concurrent forker " <>
        "(#{inspect(reason)}); the winner births the tenant at HEAD. Not a fallback."
    )

    :telemetry.execute(
      [:fathom, :migrator, :fork_retry],
      %{count: 1},
      %{shard_id: shard_id}
    )

    :ok
  end

  def report_fork(outcome, shard_id) do
    reason = fork_failure_reason(outcome)

    Logger.error(
      "shard #{shard_id}: :fork_from_template is ON but the fork FAILED " <>
        "(#{inspect(outcome)}); the tenant is born EMPTY and serving with NO schema. Its " <>
        "first ORM query will fail, and the rollout cannot heal it (replay onto an empty " <>
        "file dies on `no such table: django_migrations`). If this reason is " <>
        ":no_template_snapshot, the prerequisite never ran — release a version and then " <>
        "`mix fathom.snapshot template-head`. Delete the tenant and re-mint it once the " <>
        "fork works; do not leave it serving."
    )

    :telemetry.execute(
      [:fathom, :migrator, :fork_fallback],
      %{count: 1},
      %{shard_id: shard_id, reason: reason}
    )

    :ok
  end

  # Metadata rides into Telemetry.Metrics tags, so the reason must be a BOUNDED atom set —
  # a `{:retry, term}` carrying an arbitrary storage error would be unbounded cardinality.
  # The full outcome stays in the log line above.
  defp fork_failure_reason({:error, reason}) when is_atom(reason), do: reason
  defp fork_failure_reason({:error, {:exception, _}}), do: :exception
  defp fork_failure_reason({:error, {:exit, _}}), do: :exit
  defp fork_failure_reason({:error, _}), do: :error
  # `{:retry, _}` no longer reaches here — it is handled as the benign concurrent-forker case
  # above (expert review 2026-08-20 #35) — so there is deliberately no clause for it. The
  # catch-all below still covers any shape a future `fork_from_template/1` return adds.
  defp fork_failure_reason(_), do: :unknown

  # Novel = nothing knows the shard: no local file (a present file is an authoritative
  # un-flushed copy) and no directory row — the same definition novel_refused?/1 uses.
  # The directory read fails OPEN (treated as known ⇒ no fork), so a Postgres outage
  # births empty rather than blocking the open; the fork itself additionally refuses a
  # dst that already has a stored object (`:dst_exists`), so a flushed-then-forgotten
  # shard is never overwritten.
  defp novel_shard?(shard_id) do
    not File.exists?(Fathom.Shard.db_path(shard_id)) and not known_to_directory?(shard_id)
  end

  # How many least-recently-used shards to probe for idleness before giving up and
  # refusing. If the LRU-coldest handful are all still actively serving connections,
  # the node is genuinely hot and refusing is correct — bounding the probe keeps the
  # at-capacity path from doing O(open-shards) drain calls on a saturated node.
  @max_evict_probes 16

  # Evict the least-recently-used *idle* shard to free a slot, returning whether one was
  # freed. Off when `:evict_idle_at_capacity` is false (hard cap). `evict/2` is the atomic
  # evict-if-idle primitive: it stops the coordinator (flush + drop + release lease) iff it
  # has zero checked-out connections, else returns `{:error, :busy}` untouched. Walks
  # LRU-first, so the first probe — the coldest shard — is almost always idle and evicts in
  # one call; a dirty idle shard pays one flush, a clean one skips the upload.
  #
  # Time-bounded by `:evict_budget_ms` (expert review 2026-07-14 #7): probing stops once
  # the total wait would exceed the budget, and each probe's wait for the coordinator to
  # exit is capped at the REMAINING budget — so a slow/hung S3 flush in an evicted
  # coordinator's terminate can't block the checkout stream for tens of seconds (up to
  # 16 × 30 s across probes). Over budget we refuse fast; the LB retries.
  defp evicted_for_room? do
    if Fathom.Shards.Lru.enabled?() do
      deadline = System.monotonic_time(:millisecond) + evict_budget_ms()

      Fathom.Shards.Lru.lru_order(@max_evict_probes)
      |> Enum.filter(&shard_open?/1)
      |> Enum.reduce_while(false, fn shard_id, _ ->
        remaining = deadline - System.monotonic_time(:millisecond)

        cond do
          # Budget spent: stop probing and refuse fast (503) rather than block the checkout
          # stream on another tenant's slow flush. Any coordinator we already told to drain
          # frees its slot in the background; the LB's retry finds the room.
          remaining <= 0 ->
            {:halt, false}

          true ->
            case evict(shard_id, remaining) do
              :ok ->
                :telemetry.execute([:fathom, :shards, :evicted], %{count: 1}, %{
                  shard_id: shard_id
                })

                {:halt, true}

              # Accepted the drain but its terminate (S3 flush) didn't finish within the
              # remaining budget: it's stopping in the background, so stop probing and 503
              # fast — the freed slot shows up on the next admission attempt.
              {:error, :exit_timeout} ->
                {:halt, false}

              # Busy (connections checked out) or a drain failure — leave it, try the next.
              _ ->
                {:cont, false}
            end
        end
      end)
    else
      false
    end
  end

  # Bounded-wait eviction primitive, used ONLY by the at-capacity admission path. Like
  # `drain(id, 0)` — the atomic evict-if-idle — but it waits at most `budget_ms` (the
  # remaining admission budget) for the coordinator to exit rather than drain/2's 30 s
  # safety net (expert review 2026-07-14 #7). On :exit_timeout the coordinator has already
  # accepted the drain and is stopping in the background — its terminate still runs the
  # full checkpoint + flush + drop + release (durability/lease untouched); admission just
  # stops waiting. Monitor/mailbox cleanup is shared with drain/2 via await_coordinator_exit
  # (expert review #41), so a bounded wait never leaks a monitor or a stale abort.
  # Slack over the probe's own budget so the ordinary path is resolved by the probe's timeout
  # rather than by the caller shutting it down.
  @evict_probe_grace_ms 250

  defp evict(shard_id, budget_ms) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        # Probe from a SHORT-LIVED process, not the caller (expert review 2026-08-01 #34).
        #
        # `await_coordinator_exit/3`'s timeout branch peeks for a counterpart
        # `{:drain_aborted, pid}` with `after 0`, which can only consume one that has ALREADY
        # arrived. On the `:exit_timeout` branch the coordinator is by definition still alive
        # and has not yet acted on the drain cast, so the abort lands *after* the peek and is
        # never matched again — a later probe pins a different pid.
        #
        # That matters because of WHO calls this: `evicted_for_room?/0` runs eviction probing
        # inline in the admitting Filo stream process, with a 2s budget over up to 16
        # candidates, and a django-libsql WebSocket stream lives for hours. A coordinator
        # blocked in a legacy-mode renew PUT, a cold pull, or a settling flush will not dequeue
        # in 2s — so at-capacity nodes, where probing is the designed steady state, slowly
        # accumulated unmatched messages in exactly their longest-lived processes, invisible to
        # every metric.
        #
        # Running the monitor + wait in a task means the mailbox that can leak dies with the
        # probe. `Task.yield` + `shutdown` keeps the caller's own wait bounded, and the task is
        # unlinked so a probe crash cannot take the stream down with it.
        task =
          Task.Supervisor.async_nolink(Fathom.TaskSupervisor, fn ->
            ref = Process.monitor(pid)
            Fathom.Shard.request_drain(pid, 0, self())
            await_coordinator_exit(pid, ref, budget_ms)
          end)

        case Task.yield(task, budget_ms + @evict_probe_grace_ms) ||
               Task.shutdown(task, :brutal_kill) do
          {:ok, outcome} -> outcome
          _ -> {:error, :exit_timeout}
        end
    end
  end

  defp evict_budget_ms,
    do: Application.get_env(:fathom, :evict_budget_ms, @default_evict_budget_ms)

  # Is this shard currently open on this node? Filters stale Lru rows (a coordinator that
  # already stopped without its `forget` having landed) so we don't count a no-op
  # `drain/2` on an already-gone shard as having freed a slot.
  defp shard_open?(shard_id), do: Registry.lookup(@registry, shard_id) != []

  # The limiter call needs the same exit protection the directory read below has
  # (expert review #28): the limiter's own backpressure model is mailbox saturation,
  # which is exactly when GenServer.call starts exiting :timeout — and a limiter
  # crash/restart window exits :noproc. Un-caught, those exits crashed the whole open
  # path (Hrana stream 500s) under precisely the novel-shard spray the limiter exists
  # to absorb, with each saturated caller pinning its connection 5 s. Fail CLOSED
  # (refused) on an exit: under a spray, refusing is the limiter doing its job; a
  # crashed limiter recovering for a few ms refusing a genuinely novel mint is the
  # cheap direction (existing shards never reach this call).
  defp limiter_refused?(shard_id) do
    match?({:error, _}, Fathom.Shards.NovelLimiter.allow(shard_id))
  catch
    :exit, _ -> true
  end

  # The directory read fails OPEN (treat as known): the data path never blocks on or fails
  # because of Postgres (the Recorder principle) — an outage disables the limiter, it never
  # refuses real traffic. An existing-but-never-recorded shard costs one token; the bucket
  # absorbs it.
  defp known_to_directory?(shard_id) do
    match?({:ok, _}, Fathom.Directory.get(shard_id))
  rescue
    _ -> true
  catch
    :exit, _ -> true
  end

  # Soft cap: `Registry.count` is O(1) and a couple of concurrent opens may overshoot
  # by the concurrency — fine, since the cap sits below the hard fd limit with headroom.
  defp at_capacity? do
    case max_open_shards() do
      :infinity -> false
      cap when is_integer(cap) -> Registry.count(@registry) >= cap
    end
  end

  defp max_open_shards, do: Application.get_env(:fathom, :max_open_shards, :infinity)

  defp start(shard_id) do
    case DynamicSupervisor.start_child(@supervisor, {Fathom.Shard, shard_id}) do
      {:ok, pid} -> {:ok, pid}
      # The Registry `:via` name wins the race when two callers start at once.
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, _} = error -> error
    end
  end
end
