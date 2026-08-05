defmodule Fathom.Shard.WarmFollower do
  @moduledoc """
  Keeps the fleet's recently-active shards **warm** on this node so a failover skips
  the cold-open-from-S3 (the Phase-2 availability win — see `docs/phase2-scoping.md`).

  On node death the LB reroutes the dead node's subdomains to survivors, and each
  survivor `Storage.pull`s the shard from S3 (~150–250 ms cross-region) before it can
  serve. This process pre-pulls the hot set into a **cache directory** ahead of time,
  so when a rerouted checkout lands the file is already local and the open is *warm*
  (~ms) instead of cold.

  It holds no lease and never serves — it's a pure read cache in `warm_cache_dir`,
  distinct from the live data dir so it can't be confused with a *warm restart* (this
  node's own un-flushed writes, which stay authoritative).

  Because the cache may lag the owner's latest flush, a cached copy is **never served
  as-is**: each pull goes through `Storage.pull_if_changed/3` and records the object's
  etag in a `<shard>.db.etag` sidecar, and at failover the coordinator revalidates that
  etag against the store before promoting the cache (`Fathom.Shard.start_pull` — H2).

  ## Revalidation rides the directory's flush signal, not a GET-per-shard poll

  Keeping etags current used to mean one conditional GET per cached shard per poll —
  request-bound at exactly the scale warm capacity is supposed to be disk-bound (10k
  cached ⇒ 1,000 GET/s/node of steady-state 304s; review 2026-07-23 #15). The directory
  already persists **`last_flushed_at`** per shard (review #28), and `active_recent/1`
  returns it in the same read the follower already does each poll — the exact "did the
  owner flush since my pull" signal. So a cached shard whose `last_flushed_at` hasn't
  advanced past the value recorded at its last validation is **skipped**: the GET count
  tracks *flushes since the last poll*, not cache size. Two belts keep it honest: a
  shard with no flush signal (NULL `last_flushed_at` — never flushed, or the signal was
  dropped in a Postgres outage window) revalidates every poll exactly as before, and a
  rolling 1-in-10 slice of the cached set is force-revalidated each cycle regardless, so
  a lost signal bounds staleness at ~10 polls instead of forever. The failover
  promotion's own freshness check is unchanged — this loop is a cache-freshness
  optimization, never the serving authority.

  Gated by `:warm_follower` (off by default; a node opts in to the standby role) and
  bounded by `:warm_cache_max`. Refreshed every `:warm_poll_ms` from
  `Fathom.Directory.active_recent/1`, minus the shards this node owns **or recently
  owned**.

  ## Why "recently owned", not just "owned"

  A survivor only needs a shard warm if a *failover* will route it here — i.e. this
  node is NOT the shard's LB home. The home node has zero failover use for its own
  shards (they only move if the home itself dies, taking its cache with it), so
  warming them is pure waste that competes for the `:warm_cache_max` budget with the
  real failover set. The live exclusion catches a shard while a coordinator holds it,
  but on idle the coordinator flushes, drops the local copy, and **releases the lease**
  — so nothing on-disk or in S3 still marks this node as the home, and the next poll
  would re-warm the shard this node just dropped. So the follower remembers shards it
  owned for `:warm_home_retention_ms` past the last time it saw them owned, and excludes
  those too. A routine idle→reopen re-stamps the shard before the window lapses, so it
  never re-warms its own home set; a genuine LB remap (the home actually changed) lets
  the window lapse and the shard becomes a warmable failover target again.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Storage

  @default_poll_ms 10_000
  @default_cache_max 500
  # Per-shard warm-pull budget; a pull past this is killed and the shard skipped (finding #23).
  @default_pull_timeout 60_000
  # How long after this node last owned a shard it still counts as "home" (so we don't
  # re-warm what we just idle-dropped). Outlasts a routine idle→reopen gap; a real LB
  # remap lapses it so the shard becomes a warmable failover target again.
  @default_home_retention_ms 60_000
  # The rolling force-revalidation belt (see the moduledoc): each cycle force-checks the
  # 1/@sweep_cycles slice of ids whose hash matches the cycle, so a shard whose flush
  # signal was lost is at most ~@sweep_cycles polls stale.
  @sweep_cycles 10
  # Per-shard body re-pull cooldown, as a multiple of the poll interval (expert review
  # 2026-07-24 #26). See `min_repull_ms/1`.
  @default_repull_multiple 10

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether `shard_id` has a warm cached copy on this node."
  @spec cached?(String.t()) :: boolean()
  def cached?(shard_id), do: File.exists?(cache_path(shard_id))

  @doc """
  All shard ids currently warm-cached on this node, via a single directory read — for a
  caller that must test warm-membership across many shards without N `cached?/1` stat calls
  (finding #12). Skips the `-wal`/`-shm`/`.etag` sidecars; order is unspecified; a missing
  cache dir yields `[]`.
  """
  @spec cached_shard_ids() :: [String.t()]
  def cached_shard_ids do
    case File.ls(cache_dir()) do
      {:ok, entries} ->
        for name <- entries,
            String.ends_with?(name, ".db"),
            do: String.replace_suffix(name, ".db", "")

      {:error, _} ->
        []
    end
  end

  @doc "Local path of `shard_id`'s warm cache copy (may not exist)."
  @spec cache_path(String.t()) :: Path.t()
  def cache_path(shard_id) do
    # Path-traversal / isolation gate (review 2026-07-09 #6): shard_id becomes a file name
    # here, so a `..`/`/` id would escape cache_dir. Enforce ShardId's one rule. The
    # command-reachable entry (warm_now/1) validates first and skips gracefully, so this raise
    # is a fail-closed assertion that should never fire on a validated id.
    Fathom.ShardId.valid?(shard_id) ||
      raise(ArgumentError, "invalid shard id: #{inspect(shard_id)}")

    Path.join(cache_dir(), "#{shard_id}.db")
  end

  @doc """
  Warm-pulls one specific shard into this node's warm cache **now**, bypassing the poll
  cycle's owned/recently-owned exclusion — the targeted primitive the rebalance handoff
  uses to pre-warm a shard on its new node before the LB flip. Conditional and idempotent
  (an unchanged object 304s with no body transfer). Works whether or not the follower
  GenServer is running (pure over `:warm_cache_dir` + storage). Returns `:ok` on a
  warm/valid cache, `{:error, :not_warmable}` when the shard has no stored object yet
  (never flushed) or the pull failed — safe either way, since the target's cold-open
  revalidates or pulls fresh regardless.
  """
  @spec warm_now(String.t()) :: :ok | {:error, :not_warmable}
  def warm_now(shard_id) do
    # Validate at the command-reachable entry (#6): a poller runs this straight from a
    # command's shard_id, so an invalid id is skipped gracefully (best-effort warm) rather than
    # tripping cache_path/1's assertion and wedging the command.
    if Fathom.ShardId.valid?(shard_id) do
      # `pull_one/1` also reports the bytes transferred (0 on a 304) for the refresh
      # budget; a direct warm command ignores it — it is a one-shot, not a steady-state cost.
      case pull_one(shard_id) do
        {:ok, ^shard_id, _bytes} -> :ok
        :skip -> {:error, :not_warmable}
      end
    else
      {:error, :not_warmable}
    end
  end

  @doc """
  Drops `shard_id`'s warm copy from this node's cache **now** — the db file plus its
  `-wal`/`-shm` siblings and the `.etag` sidecar. The targeted eviction a tenant delete
  broadcasts fleet-wide (#15) so an erased shard's lease-less cached copy doesn't linger
  until the poll loop evicts it for leaving `active_recent`. Idempotent (a shard that
  isn't cached is a no-op) and pure over `:warm_cache_dir`, so it works whether or not the
  follower GenServer is running. An invalid id is ignored (best-effort, like `warm_now/1`).
  """
  @spec purge_now(String.t()) :: :ok
  def purge_now(shard_id) do
    if Fathom.ShardId.valid?(shard_id), do: evict(shard_id)
    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @doc """
  The stored etag of `shard_id`'s warm copy, or `nil` when it isn't cached (the db
  file is absent) or no etag was recorded. This is the freshness token the coordinator
  presents to `Storage.pull_if_changed/3` before promoting the cache — a `nil` here
  means "no validatable warm copy," so the coordinator cold-pulls instead. Requiring
  the db file to exist keeps a stale sidecar from ever passing off a missing file as
  cached.
  """
  @spec cached_etag(String.t()) :: String.t() | nil
  def cached_etag(shard_id) do
    with true <- File.exists?(cache_path(shard_id)),
         {:ok, body} <- File.read(etag_path(shard_id)),
         etag when etag != "" <- String.trim(body) do
      etag
    else
      _ -> nil
    end
  end

  @impl true
  def init(opts) do
    poll_ms =
      Keyword.get(opts, :poll_ms, Application.get_env(:fathom, :warm_poll_ms, @default_poll_ms))

    cache_max =
      Keyword.get(
        opts,
        :cache_max,
        Application.get_env(:fathom, :warm_cache_max, @default_cache_max)
      )

    home_retention_ms =
      Keyword.get(
        opts,
        :home_retention_ms,
        Application.get_env(:fathom, :warm_home_retention_ms, @default_home_retention_ms)
      )

    File.mkdir_p!(cache_dir())

    {:ok,
     %{
       poll_ms: poll_ms,
       cache_max: cache_max,
       home_retention_ms: home_retention_ms,
       cached: MapSet.new(),
       # shard_id => monotonic-ms of the last refresh that saw this node own it
       recent_owned: %{},
       # shard_id => the directory last_flushed_at observed when this shard was last
       # revalidated against storage — the skip signal (review 2026-07-23 #15).
       validated_flush: %{},
       # shard_id => monotonic-ms of the last refresh that actually transferred a BODY for
       # this shard. Drives the per-shard re-pull cooldown (review 2026-07-24 #26).
       last_body: %{},
       # shard_id => monotonic-ms of the last time this shard was checked at all (304 or
       # 200). Orders the byte budget lag-first so the cache converges instead of thrashing.
       last_checked: %{},
       # Token bucket for :warm_refresh_bytes_per_s. nil budget ⇒ unused (fast path).
       tokens: 0,
       # Running mean body size, the cost estimate for a candidate with no local file yet.
       mean_body_bytes: 0,
       # Monotonic refresh counter driving the rolling force-revalidation slice.
       cycle: 0,
       timer: nil
     }, {:continue, :refresh}}
  end

  @impl true
  def handle_continue(:refresh, state), do: {:noreply, refresh(state)}

  @impl true
  def handle_info(:refresh, state), do: {:noreply, refresh(state)}

  # Synchronous refresh for tests (drive one cycle deterministically, no sleep).
  @impl true
  def handle_call(:refresh, _from, state) do
    state = refresh(state)
    {:reply, MapSet.to_list(state.cached), state}
  end

  defp refresh(state) do
    # Reap orphaned temps from killed pulls (expert review round-2 #27): the
    # `on_timeout: :kill_task` below strands a uniquely-named `.dl.*` temp every
    # time a pull outlives its timeout — and failover congestion makes timeouts
    # CLUSTER, with a retry every poll, so the disk-bound density budget fills with
    # garbage nothing deletes. Age-gated past the pull timeout so live pulls'
    # fresh temps are never touched; the cache dir is bounded by :warm_cache_max,
    # so the glob is cheap.
    reaped = Storage.reap_stale_temps(Path.join(cache_dir(), "*"), 2 * pull_timeout())
    if reaped > 0, do: Logger.info("warm-follower: reaped #{reaped} orphaned temp(s)")

    # Stamp shards this node owns right now, decay ones last owned past the retention
    # window, and treat the survivors as "home" — excluded from the warm target so we
    # never re-warm what this node just idle-dropped.
    recent_owned = refresh_home(state.recent_owned, state.home_retention_ms)
    home = MapSet.new(Map.keys(recent_owned))

    # Disk back-pressure (expert review #36). `:warm_cache_max` bounds the cache in shard COUNT,
    # which says nothing about bytes — and this cache shares a filesystem with the live shard data,
    # so filling it fails every cold-open `pull` AND every dirty shard's `VACUUM INTO`: writes keep
    # being acked and can never be made durable.
    #
    # Under pressure, narrow the target to what is ALREADY cached rather than emptying it. Warming
    # is the thing to stop; evicting would throw away failover readiness we have already paid for,
    # and would not free the space that actually matters (the live data dir) any faster than the
    # ordinary count-based eviction does. Held shards therefore stay held and stay revalidated —
    # they just stop being joined by new ones.
    targets = target_rows(state.cache_max, home)

    targets =
      if disk_headroom?() do
        targets
      else
        kept = Enum.filter(targets, fn {id, _lfa} -> MapSet.member?(state.cached, id) end)

        :telemetry.execute(
          [:fathom, :warm_follower, :disk_pressure],
          %{held: length(kept), declined: length(targets) - length(kept)},
          %{cache_dir: cache_dir()}
        )

        kept
      end

    target = MapSet.new(targets, fn {id, _lfa} -> id end)
    to_evict = MapSet.difference(state.cached, target)

    Enum.each(to_evict, &evict/1)

    # Revalidate what the directory says COULD have moved (see the moduledoc): an
    # already-cached shard whose last_flushed_at hasn't advanced past its last validation
    # is skipped outright — no conditional GET — unless it lands in this cycle's rolling
    # force-check slice. Everything else (new, flush-advanced, or signal-less shards)
    # presents its stored etag, so a `pull_if_changed` is a cheap 304 (no body) when
    # unchanged, or a re-pull when the owner flushed. Keeping caches current is what makes
    # a failover promotion hit the 304 fast path instead of a full cold re-pull.
    cycle = state.cycle + 1
    now = System.monotonic_time(:millisecond)

    {skipped, checkable} =
      Enum.split_with(targets, fn {id, lfa} -> skip_revalidation?(id, lfa, state, cycle, now) end)

    {to_check, deferred, tokens} = admit(checkable, state)

    {checked, checked_stamps, bodies} = pull_all(to_check)

    # A budget-deferred shard is still cached if we already held it — we just didn't spend
    # this cycle's bytes revalidating it. One we don't hold yet stays uncached (correctly:
    # there is no file), and its lag grows, so the lag-first order admits it next cycle.
    held =
      MapSet.union(
        MapSet.new(skipped, fn {id, _} -> id end),
        MapSet.intersection(state.cached, MapSet.new(deferred, fn {id, _} -> id end))
      )

    cached = MapSet.union(held, checked)
    ids = MapSet.to_list(cached)
    body_bytes = bodies |> Map.values() |> Enum.sum()

    :telemetry.execute(
      [:fathom, :shard, :warm, :refresh],
      %{
        cached: MapSet.size(cached),
        checked: MapSet.size(checked),
        skipped: length(skipped),
        deferred: length(deferred),
        bodies: map_size(bodies),
        body_bytes: body_bytes
      },
      %{}
    )

    schedule(%{
      state
      | cached: cached,
        recent_owned: recent_owned,
        validated_flush: state.validated_flush |> Map.take(ids) |> Map.merge(checked_stamps),
        last_body:
          state.last_body
          |> Map.take(ids)
          |> Map.merge(Map.new(bodies, fn {id, _} -> {id, now} end)),
        last_checked:
          state.last_checked |> Map.take(ids) |> Map.merge(Map.new(checked, &{&1, now})),
        # Estimates gate admission; the bucket is charged the ACTUAL bytes, so a bad
        # estimate self-corrects into the next cycle instead of compounding.
        tokens: tokens - body_bytes,
        mean_body_bytes: update_mean(state.mean_body_bytes, bodies),
        cycle: cycle
    })
  end

  # Byte budget (`:warm_refresh_bytes_per_s`, review 2026-07-24 #26 (a)). Unset ⇒ admit
  # everything and skip the ordering and the `File.stat` per candidate entirely, so the
  # default path costs nothing.
  #
  # Admission is **lag-first** — the shard checked longest ago goes first — so a budget too
  # small for the whole set converges the cache round-robin instead of spending every cycle
  # re-pulling the same few hot shards and starving the rest.
  #
  # The bucket refills one poll interval's worth per cycle and is capped there: an idle
  # follower cannot bank a burst it would spend all at once on wake.
  defp admit(candidates, state) do
    case refresh_bytes_per_s() do
      nil ->
        {candidates, [], 0}

      bps ->
        per_cycle = div(bps * state.poll_ms, 1000)
        tokens = min(state.tokens + per_cycle, per_cycle)

        {to_check, deferred, _left} =
          candidates
          |> Enum.sort_by(fn {id, _} -> Map.get(state.last_checked, id, 0) end)
          |> Enum.reduce({[], [], tokens}, fn {id, _} = c, {take, defer, left} ->
            if left > 0 do
              {[c | take], defer, left - est_bytes(id, state.mean_body_bytes)}
            else
              {take, [c | defer], left}
            end
          end)

        {to_check, deferred, tokens}
    end
  end

  # What a candidate is expected to cost: the copy we already hold is the best estimate;
  # a shard we don't hold yet gets the running mean (0 on a cold follower, which admits the
  # first fill — that fill is one-time and already bounded by :warm_cache_max).
  defp est_bytes(shard_id, mean) do
    case File.stat(cache_path(shard_id)) do
      {:ok, %{size: size}} when size > 0 -> size
      _ -> mean
    end
  end

  defp update_mean(mean, bodies) when map_size(bodies) == 0, do: mean

  defp update_mean(mean, bodies) do
    observed = div(bodies |> Map.values() |> Enum.sum(), map_size(bodies))
    if mean == 0, do: observed, else: div(mean * 3 + observed, 4)
  end

  # Skip this cycle's conditional GET. Two independent reasons, and the belts around them
  # matter more than either: only an already-held copy can skip (never the cache fill), and
  # the rolling sweep slice force-checks regardless, so a lost flush signal still bounds
  # staleness at ~@sweep_cycles polls.
  #
  #   1. The directory shows the owner hasn't flushed since our last validation
  #      (review 2026-07-23 #15) — a nil signal never skips, so it degrades to the old
  #      revalidate-every-poll behaviour rather than to silence.
  #   2. The owner DID flush, but we transferred this shard's body too recently
  #      (review 2026-07-24 #26). This is the case #15 left unbounded: a continuously-
  #      written tenant flushes faster than the poll, so its signal advances every cycle and
  #      every GET is a 200 with a full body plus an fsync, forever. Skipping here costs only
  #      failover RTO on that shard — the coordinator's promotion revalidates before serving,
  #      so a staler cache is still correct, never wrong.
  defp skip_revalidation?(shard_id, last_flushed_at, state, cycle, now) do
    cond do
      not held?(shard_id, state) -> false
      forced?(shard_id, cycle) -> false
      unflushed_since_validation?(shard_id, last_flushed_at, state) -> true
      in_repull_cooldown?(shard_id, state, now) -> true
      true -> false
    end
  end

  defp held?(shard_id, state) do
    MapSet.member?(state.cached, shard_id) and File.exists?(cache_path(shard_id))
  end

  defp forced?(shard_id, cycle) do
    :erlang.phash2(shard_id, @sweep_cycles) == rem(cycle, @sweep_cycles)
  end

  defp unflushed_since_validation?(_shard_id, nil, _state), do: false

  defp unflushed_since_validation?(shard_id, last_flushed_at, state) do
    Map.get(state.validated_flush, shard_id) == last_flushed_at
  end

  defp in_repull_cooldown?(shard_id, state, now) do
    case Map.get(state.last_body, shard_id) do
      nil -> false
      at -> now - at < min_repull_ms(state.poll_ms)
    end
  end

  # Floor on how often one shard's BODY may be re-transferred, defaulting to
  # @default_repull_multiple × the poll. This is what bounds steady-state cost: worst case
  # ingress is Σ(cached sizes) / this, instead of Σ(write-active sizes) / :warm_poll_ms.
  defp min_repull_ms(poll_ms) do
    Application.get_env(:fathom, :warm_min_repull_ms, poll_ms * @default_repull_multiple)
  end

  defp refresh_bytes_per_s, do: Application.get_env(:fathom, :warm_refresh_bytes_per_s)

  # Re-stamp currently-owned shards to now, then drop entries last owned longer ago
  # than the retention window. The result's keys are the shards this node is (still
  # plausibly) the home for.
  defp refresh_home(recent_owned, retention_ms) do
    now = System.monotonic_time(:millisecond)

    recent_owned
    |> Map.merge(Map.new(owned_shards(), &{&1, now}))
    |> Map.reject(fn {_id, ts} -> now - ts > retention_ms end)
  end

  # The fleet hot set (most-recently-active) minus the shards this node owns or
  # recently owned (its home set — no failover use in warming what routes back to us)
  # — capped at the cache budget. Carries each shard's `last_flushed_at` (already in the
  # same directory read) as the revalidation-skip signal.
  defp target_rows(cache_max, home) do
    Fathom.Directory.active_recent(cache_max)
    |> Enum.reject(&MapSet.member?(home, &1.shard_id))
    |> Enum.take(cache_max)
    |> Enum.map(&{&1.shard_id, &1.last_flushed_at})
  rescue
    # The directory is best-effort here: a Postgres blip just skips a refresh.
    e ->
      Logger.warning("warm-follower: directory read failed (#{inspect(e)})")
      []
  end

  defp owned_shards do
    Registry.select(Fathom.ShardRegistry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  # Takes `{shard_id, last_flushed_at}` pairs; returns `{cached_set, stamps}` where
  # `stamps` records the flush signal each successful validation was performed under —
  # the next cycle's skip baseline.
  defp pull_all(pairs) do
    pairs
    |> Task.async_stream(fn {shard_id, lfa} -> {pull_one(shard_id), lfa} end,
      max_concurrency: System.schedulers_online() * 4,
      timeout: pull_timeout(),
      # A single slow pull (a >timeout S3 fetch — exactly the failover-congestion moment)
      # must not take the follower down: kill just that task and skip the shard (yielding
      # {:exit, :timeout}, caught by the `_other` clause). The default :exit would crash the
      # whole cycle. This is a warm cache, so a dropped pull just re-tries next poll.
      on_timeout: :kill_task,
      ordered: false
    )
    |> Enum.reduce({MapSet.new(), %{}, %{}}, fn
      {:ok, {{:ok, shard_id, bytes}, lfa}}, {set, stamps, bodies} ->
        bodies = if bytes > 0, do: Map.put(bodies, shard_id, bytes), else: bodies
        {MapSet.put(set, shard_id), Map.put(stamps, shard_id, lfa), bodies}

      _other, acc ->
        acc
    end)
  end

  defp pull_timeout,
    do: Application.get_env(:fathom, :warm_pull_timeout_ms, @default_pull_timeout)

  defp pull_one(shard_id) do
    path = cache_path(shard_id)
    # Only present an etag when the cached file actually exists, so a stale sidecar can
    # never make a 304 report a missing file as still cached.
    etag = if File.exists?(path), do: cached_etag(shard_id), else: nil

    case Storage.pull_if_changed(shard_id, path, etag) do
      # Object unchanged since our last pull — cache stays warm, no body transferred.
      # 0 bytes: a 304 costs no ingress and no fsync, so it never charges the budget and
      # never starts the re-pull cooldown.
      {:ok, :unchanged} ->
        {:ok, shard_id, 0}

      # Fresh bytes: record the new etag so the next cycle (and a failover promotion)
      # can validate against it.
      {:ok, {:written, new_etag}} ->
        write_etag(shard_id, new_etag)

        bytes =
          case File.stat(path) do
            {:ok, %{size: size}} -> size
            _ -> 0
          end

        :telemetry.execute([:fathom, :shard, :warm, :pulled], %{count: 1, bytes: bytes}, %{
          shard_id: shard_id
        })

        {:ok, shard_id, bytes}

      # No object yet (never flushed) or it vanished — make sure we aren't caching it.
      {:ok, :absent} ->
        drop(path)
        :skip

      {:error, _} ->
        :skip
    end
  rescue
    _ -> :skip
  catch
    :exit, _ -> :skip
  end

  defp evict(shard_id) do
    drop(cache_path(shard_id))
    :telemetry.execute([:fathom, :shard, :warm, :evicted], %{count: 1}, %{shard_id: shard_id})
  end

  # No etag from the store (can't conditionally validate) — drop any stale sidecar so
  # the next cycle re-pulls unconditionally rather than validating against a wrong value.
  defp write_etag(shard_id, nil), do: File.rm(etag_path(shard_id))
  defp write_etag(shard_id, etag), do: File.write(etag_path(shard_id), etag)

  defp etag_path(shard_id), do: cache_path(shard_id) <> ".etag"

  defp drop(path), do: Enum.each(["", "-wal", "-shm", ".etag"], &File.rm(path <> &1))

  defp schedule(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    %{state | timer: Process.send_after(self(), :refresh, state.poll_ms)}
  end

  @doc """
  Where this node's warm cache lives. Public (`@doc false`-ish in spirit) so the disk gauge can
  measure the volume it sits on without duplicating the config/default pair — the same reason
  `Fathom.Shard.data_dir/0` is public. Duplicating it is how the two drift apart and the gauge ends
  up measuring a directory nothing writes to.
  """
  @spec cache_dir() :: String.t()
  def cache_dir do
    Application.get_env(:fathom, :warm_cache_dir) ||
      Path.join(System.tmp_dir!(), "fathom_warm_cache")
  end

  @doc "Whether this node opted into the standby role (`:warm_follower`)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :warm_follower, false)

  @doc """
  Whether the warm cache may take on another shard, given local disk (expert review #36).

  `:warm_cache_max` bounds the cache in shard **COUNT**, which is the wrong unit for a disk-bound
  component: 500 shards is 8 MB or 2 TB depending on tenant size, reconciled against nothing. Two
  byte-aware brakes sit alongside it:

    * `:warm_cache_max_bytes` — a cap on what the cache itself may occupy. `nil` (default) leaves
      the count as the only bound, i.e. exactly today's behaviour, so enabling this is opt-in.
    * `:warm_disk_free_floor_bytes` — stop warming when the VOLUME drops below this much free,
      whatever the cache's own size. This is the one that matters, because the warm cache shares a
      filesystem with the live shard data: filling it fails every cold-open `pull` AND every dirty
      shard's `VACUUM INTO`, so writes keep being acked and can never be made durable. Default
      1 GiB.

  Fails **open** when disk cannot be read (`:disksup` absent, path unreadable): warming is an
  optimisation, and refusing to warm on an unreadable stat would silently disable standby on any
  node where os_mon is trimmed out of the release. The floor exists to stop a *known* shortage, not
  to guess at one.
  """
  @spec disk_headroom?() :: boolean()
  def disk_headroom? do
    headroom?(
      Fathom.Admin.Measurements.disk_info(cache_dir()),
      Application.get_env(:fathom, :warm_disk_free_floor_bytes, 1024 * 1024 * 1024),
      Application.get_env(:fathom, :warm_cache_max_bytes),
      &cache_bytes/0
    )
  end

  @doc """
  The back-pressure decision, as a pure function of its inputs.

  Split out from `disk_headroom?/0` so the `:error` branch is testable at all: after
  `Measurements.disk_info/1` learned to resolve a not-yet-created directory to its nearest existing
  ancestor, essentially every path on a healthy node reads successfully — which is correct
  behaviour and makes the fail-open case unreachable from the outside without removing `os_mon`
  from the release. An untestable safety branch is one that quietly rots.

  `cache_size` is a thunk so the (directory-walking) byte count is only paid when a byte budget is
  actually configured.
  """
  @spec headroom?(
          {:ok, %{free_bytes: integer()}} | :error,
          non_neg_integer(),
          non_neg_integer() | nil,
          (-> non_neg_integer())
        ) :: boolean()
  # Fails OPEN: warming is an optimisation, and refusing it on an unreadable stat would silently
  # disable standby on any node whose release trimmed os_mon. The floor exists to stop a KNOWN
  # shortage, not to guess at one.
  def headroom?(:error, _floor, _max_bytes, _cache_size), do: true

  def headroom?({:ok, %{free_bytes: free}}, floor, max_bytes, cache_size) do
    free >= floor and within_byte_budget?(max_bytes, cache_size)
  end

  defp within_byte_budget?(nil, _cache_size), do: true

  defp within_byte_budget?(max_bytes, cache_size) when is_integer(max_bytes),
    do: cache_size.() < max_bytes

  defp within_byte_budget?(_, _cache_size), do: true

  @doc "Bytes currently occupied by the warm cache directory (0 when it does not exist yet)."
  @spec cache_bytes() :: non_neg_integer()
  def cache_bytes do
    cache_dir()
    |> Path.join("*")
    |> Path.wildcard()
    |> Enum.reduce(0, fn f, acc ->
      case File.stat(f) do
        {:ok, %{size: size}} -> acc + size
        _ -> acc
      end
    end)
  end
end
