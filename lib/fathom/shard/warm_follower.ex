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
      case pull_one(shard_id) do
        {:ok, ^shard_id} -> :ok
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

    targets = target_rows(state.cache_max, home)
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

    {skipped, to_check} =
      Enum.split_with(targets, fn {id, lfa} -> skip_revalidation?(id, lfa, state, cycle) end)

    {checked, checked_stamps} = pull_all(to_check)
    cached = MapSet.union(MapSet.new(skipped, fn {id, _} -> id end), checked)

    validated_flush =
      state.validated_flush
      |> Map.take(MapSet.to_list(cached))
      |> Map.merge(checked_stamps)

    schedule(%{
      state
      | cached: cached,
        recent_owned: recent_owned,
        validated_flush: validated_flush,
        cycle: cycle
    })
  end

  # Skip the conditional GET for a shard the directory shows unflushed since our last
  # validation. Every condition is a belt: a nil flush signal never skips (revalidate as
  # before), only an already-cached-and-still-on-disk copy can skip, the recorded stamp
  # must equal the directory's current one, and the shard's rolling sweep slice
  # force-checks it every @sweep_cycles cycles regardless.
  defp skip_revalidation?(shard_id, last_flushed_at, state, cycle) do
    not is_nil(last_flushed_at) and
      MapSet.member?(state.cached, shard_id) and
      Map.get(state.validated_flush, shard_id) == last_flushed_at and
      :erlang.phash2(shard_id, @sweep_cycles) != rem(cycle, @sweep_cycles) and
      File.exists?(cache_path(shard_id))
  end

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
    |> Enum.reduce({MapSet.new(), %{}}, fn
      {:ok, {{:ok, shard_id}, lfa}}, {set, stamps} ->
        {MapSet.put(set, shard_id), Map.put(stamps, shard_id, lfa)}

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
      {:ok, :unchanged} ->
        {:ok, shard_id}

      # Fresh bytes: record the new etag so the next cycle (and a failover promotion)
      # can validate against it.
      {:ok, {:written, new_etag}} ->
        write_etag(shard_id, new_etag)
        :telemetry.execute([:fathom, :shard, :warm, :pulled], %{count: 1}, %{shard_id: shard_id})
        {:ok, shard_id}

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

  defp cache_dir do
    Application.get_env(:fathom, :warm_cache_dir) ||
      Path.join(System.tmp_dir!(), "fathom_warm_cache")
  end
end
