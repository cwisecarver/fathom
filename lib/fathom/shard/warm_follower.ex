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
  etag against the store before promoting the cache (`Fathom.Shard.start_pull` — H2). To
  keep the etag current, every poll **revalidates the whole cached set** (a conditional
  GET per cached shard: a cheap 304 when unchanged, a re-pull when the owner has
  flushed), so a failover promotion lands on the 304 fast path instead of a full cold
  re-pull. That cost is O(cached) per poll, not O(fleet).

  Gated by `:warm_follower` (off by default; a node opts in to the standby role) and
  bounded by `:warm_cache_max`. Refreshed every `:warm_poll_ms` from
  `Fathom.Directory.active_recent/1`, minus the shards this node already owns.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Storage

  @default_poll_ms 10_000
  @default_cache_max 500
  # Per-shard warm-pull budget; a pull past this is killed and the shard skipped (finding #23).
  @default_pull_timeout 60_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether `shard_id` has a warm cached copy on this node."
  @spec cached?(String.t()) :: boolean()
  def cached?(shard_id), do: File.exists?(cache_path(shard_id))

  @doc "Local path of `shard_id`'s warm cache copy (may not exist)."
  @spec cache_path(String.t()) :: Path.t()
  def cache_path(shard_id), do: Path.join(cache_dir(), "#{shard_id}.db")

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

    File.mkdir_p!(cache_dir())

    {:ok, %{poll_ms: poll_ms, cache_max: cache_max, cached: MapSet.new(), timer: nil},
     {:continue, :refresh}}
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

    target = target_set(state.cache_max)
    to_evict = MapSet.difference(state.cached, target)

    Enum.each(to_evict, &evict/1)

    # Revalidate the WHOLE target set each cycle, not just the newly-added shards:
    # already-cached shards present their stored etag, so a `pull_if_changed` is a cheap
    # 304 (no body) when the owner hasn't flushed since, or a re-pull when it has.
    # Keeping caches current is what makes a failover promotion hit the 304 fast path
    # instead of a full cold re-pull — without it the cache goes stale between the one
    # initial pull and failover, and the warm-standby win never materializes. Cost is
    # bounded to O(cached) conditional GETs per poll, not O(fleet).
    cached = pull_all(target)

    schedule(%{state | cached: cached})
  end

  # The fleet hot set (most-recently-active) minus the shards this node already
  # owns (no point warming what we serve) — capped at the cache budget.
  defp target_set(cache_max) do
    owned = owned_shards()

    Fathom.Directory.active_recent(cache_max)
    |> Enum.map(& &1.shard_id)
    |> Enum.reject(&MapSet.member?(owned, &1))
    |> Enum.take(cache_max)
    |> MapSet.new()
  rescue
    # The directory is best-effort here: a Postgres blip just skips a refresh.
    e ->
      Logger.warning("warm-follower: directory read failed (#{inspect(e)})")
      MapSet.new()
  end

  defp owned_shards do
    Registry.select(Fathom.ShardRegistry, [{{:"$1", :_, :_}, [], [:"$1"]}])
    |> MapSet.new()
  end

  defp pull_all(shard_ids) do
    shard_ids
    |> Task.async_stream(&pull_one/1,
      max_concurrency: System.schedulers_online() * 4,
      timeout: pull_timeout(),
      # A single slow pull (a >timeout S3 fetch — exactly the failover-congestion moment)
      # must not take the follower down: kill just that task and skip the shard (yielding
      # {:exit, :timeout}, caught by the `_other` clause). The default :exit would crash the
      # whole cycle. This is a warm cache, so a dropped pull just re-tries next poll.
      on_timeout: :kill_task,
      ordered: false
    )
    |> Enum.reduce(MapSet.new(), fn
      {:ok, {:ok, shard_id}}, acc -> MapSet.put(acc, shard_id)
      _other, acc -> acc
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
