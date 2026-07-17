defmodule Fathom.Shards do
  @moduledoc """
  Find-or-start router for shard coordinators. Resolves a `shard_id` to its
  `Fathom.Shard` process — starting it on demand under the shard
  `DynamicSupervisor` — and hands back the path to the shard's database file.
  Callers open their own `Fathom.Shard.Connection` against that path (one per
  Hrana stream), which is what keeps per-stream transactions isolated.
  """
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

  @doc """
  Ensures `shard_id`'s coordinator is running and checks out the shard for the
  caller, returning `{:ok, coordinator_pid, ref, path}` (or `{:error, reason}` for
  an invalid id / start failure). The caller opens its own connection at `path`
  and passes `ref` back via `Fathom.Shard.checkin/2` when it closes.
  """
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

  defp do_checkout(shard_id, attempts) do
    with :ok <- maybe_lazy_migrate(shard_id),
         {:ok, pid} <- ensure(shard_id),
         {:ok, ref, path} <- Fathom.Shard.checkout(pid) do
      record_use(shard_id)
      # Per-shard load: the checkout (traffic) signal for the rebalancer. Lock-free
      # ETS bump, gated + off by default (see Fathom.ShardLoad).
      Fathom.ShardLoad.record_checkout(shard_id)
      # Node-local recency for idle-eviction at capacity. Lock-free ETS insert, and a
      # no-op unless a finite cap + :evict_idle_at_capacity make eviction reachable.
      Fathom.Shards.Lru.touch(shard_id)
      {:ok, pid, ref, path}
    else
      # Race: `ensure` resolved a coordinator that lost a race with its own lifecycle, so a
      # 1 ms re-resolve to a fresh coordinator fixes it (see retry_checkout?/1). Bounded so a
      # genuinely unavailable shard still surfaces the error rather than spinning. Only the
      # (rare) race path sleeps; the happy path is untouched.
      {:error, reason} when attempts > 1 ->
        if retry_checkout?(reason) do
          Process.sleep(1)
          do_checkout(shard_id, attempts - 1)
        else
          {:error, reason}
        end

      other ->
        other
    end
  end

  # Two checkout errors are transient lifecycle races, not real failures — both clear on a
  # re-resolve to a fresh coordinator:
  #   * `:unavailable` (`:noproc`) — the coordinator had already stopped and its Registry
  #     entry lingered in the window before the Registry handled the `:DOWN`.
  #   * `:normal` — the checkout call was queued behind an `:idle_timeout`/drain stop, so the
  #     coordinator processed the stop first and the pending `GenServer.call` exited `:normal`.
  #     Idle stops are routine at scale, so without this a steady trickle of checkouts a 1 ms
  #     retry would have fixed surfaced as spurious `{:error, :normal}` to the client.
  @doc false
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

  defp migrate_on_touch_mode do
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
        :ok -> :ok
        {:ok, _} -> :ok
        # Nothing stored to migrate yet: a brand-new shard was born empty (or, with
        # :fork_from_template enabled, already at HEAD via maybe_fork_novel/1) and
        # hasn't flushed — there is nothing behind HEAD to copy.
        {:error, :no_live_object} -> :ok
        # HEAD dropped (a yank) between this node's cache refresh and the run
        # (round-2 #23): the target no longer exists, and serving the shard at its
        # OLD version is exactly correct — never fail the client checkout for a
        # version the fleet just reverted away from.
        {:error, {:unknown_version, _}} -> :ok
        # Another worker holds it / it's busy — the caller retries.
        {:retry, reason} -> {:error, {:shard_migrating, reason}}
        {:error, _} = error -> error
      end
    else
      :ok
    end
  end

  defp behind?(shard_id, head) do
    case Fathom.Directory.get(shard_id) do
      {:ok, %{schema_version: v}} -> v < head
      # Not yet in the directory (brand-new): nothing to migrate — the shard is born
      # empty (or at HEAD via the gated fork-from-template, which registers its row).
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
  Force-stops `shard_id`'s coordinator for tenant deletion (#15). Unlike `drain/2`, it does
  NOT wait for in-flight connections — a delete deliberately kicks them — and it terminates
  the coordinator via the supervisor **while its lease is still valid**, so the shutdown
  flushes/releases cleanly (or is brutal-killed) and NEVER takes the self-fence path that
  would quarantine the tenant's data to a `.fenced.<ts>` file the caller would then have to
  hunt down. Caller purges storage AFTER this returns. Returns `:ok` (stopped or already cold).
  """
  @spec stop(String.t()) :: :ok
  def stop(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        # terminate_child blocks until the child is down (clean terminate, then brutal-kill
        # at the child shutdown timeout). :not_found = it exited concurrently — also done.
        case DynamicSupervisor.terminate_child(@supervisor, pid) do
          :ok -> :ok
          {:error, :not_found} -> :ok
        end
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
  @spec flush(String.t()) :: :ok | {:error, term()}
  def flush(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        try do
          Fathom.Shard.flush_now(pid)
        catch
          # The coordinator stopped mid-call (idle-drop flushes on its way out; a steal quarantines) —
          # its stored state stands, so best-effort :ok. The common quiesced case never hits this.
          :exit, _ -> :ok
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
  def drain_down_result(reason) when reason in [:normal, :noproc], do: :ok
  def drain_down_result(reason), do: {:error, {:drain_failed, reason}}

  @doc "Returns `{:ok, pid}` for `shard_id`, starting the coordinator if needed."
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
    cond do
      # At the cap, first try to make room by evicting the least-recently-used IDLE
      # shard (soft cap). Only if nothing idle can be evicted do we refuse — a node
      # saturated with *active* connections genuinely has no room, and 503 tells the
      # LB/client to back off rather than letting DynamicSupervisor spawn past the fd
      # cliff (emfile) and degrade the whole node.
      at_capacity?() and not evicted_for_room?() ->
        :telemetry.execute([:fathom, :shards, :at_capacity], %{count: 1}, %{shard_id: shard_id})
        {:error, :node_at_capacity}

      # The churn half of finding #14: the cap above bounds how many shards this node holds
      # open; this bounds how FAST unseen ids can mint new ones (coordinator + fds + file +
      # S3 lock PUT + Postgres row per novel id). Refused before any of that work runs.
      novel_refused?(shard_id) ->
        {:error, :novel_shard_rate_limited}

      true ->
        maybe_fork_novel(shard_id)
        start(shard_id)
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
  defp maybe_fork_novel(shard_id) do
    if Application.get_env(:fathom, :fork_from_template, false) and novel_shard?(shard_id) do
      _ = Fathom.Migrator.fork_from_template(shard_id)
    end

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

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
  defp evict(shard_id, budget_ms) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        ref = Process.monitor(pid)
        Fathom.Shard.request_drain(pid, 0, self())
        await_coordinator_exit(pid, ref, budget_ms)
    end
  end

  defp evict_budget_ms,
    do: Application.get_env(:fathom, :evict_budget_ms, @default_evict_budget_ms)

  # Is this shard currently open on this node? Filters stale Lru rows (a coordinator that
  # already stopped without its `forget` having landed) so we don't count a no-op
  # `drain/2` on an already-gone shard as having freed a slot.
  defp shard_open?(shard_id), do: Registry.lookup(@registry, shard_id) != []

  # Should this open be refused as an over-rate NOVEL creation? Only consulted on the
  # registry-miss path, and only does work when `:novel_shard_rate` is configured (nil =
  # off, the default — the cold path pays one get_env). "Novel" = nothing knows the shard:
  # no local file (a present file is an authoritative un-flushed copy) and no directory row.
  defp novel_refused?(shard_id) do
    case Application.get_env(:fathom, :novel_shard_rate) do
      nil ->
        false

      _rate ->
        not File.exists?(Fathom.Shard.db_path(shard_id)) and
          not known_to_directory?(shard_id) and
          limiter_refused?(shard_id)
    end
  end

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
