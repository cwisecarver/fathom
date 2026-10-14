defmodule Fathom.Tenants.Tombstones do
  @moduledoc """
  A node-local set of tombstoned (deleted) shard ids — the admission re-mint guard
  for full tenant erasure (expert review 2026-07-14 #15).

  A deleted tenant's directory row is flipped to `deleted`, but that alone doesn't
  stop a stray request for the subdomain from re-minting an *empty* shard: novel-shard
  admission mints on first sight, and `known_to_directory?/1` treats ANY row as "known"
  (and fails open on a Postgres blip). Reading Postgres on the open path to check for a
  tombstone would couple the (near-hot) admission path to the control plane and regress
  cold-open — the exact cost `Fathom.Directory.Recorder` / `HranaAuth.Revocations` exist
  to avoid.

  So tombstones live in a public ETS set, checked O(1) with no Postgres round-trip:

    * **loaded at boot** from `Fathom.Directory.deleted_shard_ids/0`;
    * **pushed fleet-wide** on delete over Oban's LISTEN/NOTIFY (`:fathom_tenant_deleted`),
      so every node refuses the id immediately AND erases its own A2 replica of it
      (`forget_replica/1`, expert review 2026-09-29 #5);
    * **refreshed periodically** (`:tenant_tombstone_refresh_ms`, default 5 min) so a node
      that booted during a Postgres outage, or missed a fire-and-forget notification,
      still converges. The set is append-only in memory — a tombstone is permanent, so a
      refresh never removes an id (an operator who wants an id reusable hard-deletes the
      directory row, deletes the storage tombstone, and restarts);
    * **unioned at boot from durable storage** (`Fathom.Shard.Storage.tombstoned_ids/0`, #6):
      a Postgres point-in-time restore rolls the directory back and can un-tombstone a deleted
      tenant, but storage is *not* rolled back, so a `tombstones/<id>` marker written on delete
      keeps the guard complete across a directory restore. Boot-only (the set is append-only, so
      the directory-only periodic refresh never drops it) — no recurring storage LIST.

  Fathom has no BEAM cluster, so the one shared push channel is Postgres, and Oban's
  notifier is already running on it — best-effort, with the periodic refresh as the
  convergence backstop.
  """
  use GenServer

  require Logger

  alias Fathom.Directory
  alias Fathom.Tenants.DenyList

  @table __MODULE__
  @channel :fathom_tenant_deleted
  @default_refresh_ms 300_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "The Oban LISTEN/NOTIFY channel a delete broadcasts on."
  @spec channel() :: atom()
  def channel, do: @channel

  @doc """
  True if `shard_id` is tombstoned (deleted) — checked O(1) against the ETS set on the
  admission path. Returns `false` if the table isn't up yet (pre-boot), which is the safe
  default: nothing is tombstoned until a delete has run.
  """
  @spec tombstoned?(String.t()) :: boolean()
  def tombstoned?(shard_id) do
    :ets.member(@table, shard_id)
  rescue
    ArgumentError -> false
  end

  @doc "Records `shard_id` as tombstoned locally (called on the deleting node for immediacy)."
  @spec put(String.t()) :: :ok
  def put(shard_id) do
    insert(@table, shard_id)
    :ok
  end

  @impl true
  def init(opts) do
    # `:table` / `:name` / `:loader` are seams for an isolated test instance; the app singleton
    # uses the module defaults. `:retry_ms` overrides the first fast-retry delay per-instance.
    table = Keyword.get(opts, :table, @table)
    loader = Keyword.get(opts, :loader, &Directory.deleted_shard_ids/0)
    since_loader = Keyword.get(opts, :since_loader, &Directory.deleted_shard_ids_since/1)
    storage_loader = Keyword.get(opts, :storage_loader, &Fathom.Shard.Storage.tombstoned_ids/0)
    on_new = Keyword.get(opts, :on_new, &forget_replica/1)
    retry_ms = Keyword.get(opts, :retry_ms, DenyList.initial_retry_ms())

    # public read_concurrency: admission reads directly from the caller process.
    :ets.new(table, [:set, :public, :named_table, read_concurrency: true])

    listen()

    # expert review 2026-10-13 #20: both loads run in `handle_continue`, not here. Before, the
    # storage LIST ran synchronously in init, so a slow or down S3 blocked the supervisor's start of
    # this process (and every process after it in the tree), and a storage failure was logged once
    # and never retried, leaving the DR backstop missing until the next restart. `attempt_load/1`
    # retries both loads with the DenyList backoff until each succeeds.
    state = %{
      table: table,
      on_new: on_new,
      loader: loader,
      since_loader: since_loader,
      storage_loader: storage_loader,
      clock: Keyword.get(opts, :clock, &cursor_now/0),
      retry_ms: retry_ms,
      tick: 0,
      since: nil,
      loaded: false,
      storage_loaded: false,
      degraded: false
    }

    {:ok, state, {:continue, :load}}
  end

  @impl true
  def handle_continue(:load, state), do: {:noreply, attempt_load(state)}

  # One attempt at every load that has not yet succeeded. Each load is independent: the storage union
  # (#6) and the directory full load both run, and a success is never repeated. The directory's
  # success seeds the incremental cursor and starts the periodic refresh, exactly once.
  #
  # Expert review #33: a FAILED load must not wait the full refresh interval — that leaves the
  # re-mint guard empty (410 contract broken) for up to 5 min during a Postgres wobble coincident
  # with this restart. Fast-retry with backoff, and signal degraded so the window is alertable.
  defp attempt_load(state) do
    storage = if state.storage_loaded, do: :ok, else: load_from_storage(state)
    directory = if state.loaded, do: :ok, else: load_from_directory(state)

    state = %{state | storage_loaded: state.storage_loaded or storage == :ok}

    state =
      if not state.loaded and directory == :ok do
        schedule_refresh()
        # Seed the incremental cursor from the successful FULL load, so the very first periodic
        # refresh is already incremental (review 2026-07-24 #30).
        %{state | loaded: true, since: now_minus_overlap(state.clock)}
      else
        state
      end

    case {storage, directory} do
      {:ok, :ok} ->
        if state.degraded, do: DenyList.recovered(:tombstones)
        %{state | degraded: false}

      _ ->
        DenyList.degraded(:tombstones, failure_reason(storage, directory))
        schedule_retry(state.retry_ms)
        %{state | degraded: true, retry_ms: DenyList.next_retry_ms(state.retry_ms)}
    end
  end

  defp failure_reason({:error, reason}, _directory), do: {:storage, reason}
  defp failure_reason(_storage, {:error, reason}), do: {:directory, reason}

  @impl true
  def handle_info({:notification, @channel, %{"shard_id" => shard_id}}, state)
      when is_binary(shard_id) do
    # Block re-mint of the erased shard (ETS), and erase this node's A2 replica of it (see
    # `forget_replica/1`) — every node receives this, which is what reaches the FOLLOWERS.
    insert(state.table, shard_id, state.on_new)
    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    # Steady-state refresh: the set is already loaded and append-only, so a failure here just
    # logs — the last-known set stands. Only the BOOT-unloaded path (below) fast-retries.
    #
    # INCREMENTAL by default (expert review 2026-07-24 #30): the old shape re-read every id ever
    # deleted, on every node, every 5 minutes, so both the Postgres read and this process's heap
    # scaled with cumulative LIFETIME deletions rather than with anything current.
    state = refresh_from_directory(state)
    schedule_refresh()
    {:noreply, state}
  end

  def handle_info(:retry_load, state), do: {:noreply, attempt_load(state)}

  def handle_info(_msg, state), do: {:noreply, state}

  # `on_new` runs only for an id this node had NOT already tombstoned, so a full reload (which
  # re-reads every id ever deleted) does not re-run it for the whole history.
  defp insert(table, shard_id, on_new \\ &forget_replica/1) do
    if :ets.insert_new(table, {shard_id}), do: on_new.(shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # ERASE THIS NODE'S A2 REPLICA OF A DELETED SHARD (expert review 2026-09-29 #5).
  #
  # `Tenants.purge/1` removes the stored objects and the deleting node's own data dir, and nothing
  # else. With replication on (the prod default) every follower in the quorum also holds a complete
  # `.db` + `-wal` of the tenant, and `Follower.forget/2` was called only from promotions, so a
  # deleted tenant's full database stayed on Q other nodes indefinitely — against the "erase
  # everything" contract in docs/tenant-lifecycle.md. (The notification used to drop the
  # WarmFollower's copy; that step went with the WarmFollower on 2026-09-14 and was never ported.)
  #
  # This is the one place every node learns of a delete — the NOTIFY, the periodic refresh, and the
  # deleting node's own `put/1` all funnel through `insert/3` — so a node that missed the NOTIFY
  # still converges at the next refresh. Replicas that exist on disk before this node's Follower
  # has booted are handled by `Follower`'s recovery, which skips and deletes tombstoned shards;
  # `Follower.begin_seed/3` refuses to re-seed one.
  defp forget_replica(shard_id) do
    follower = Fathom.Shard.Replication.Follower

    if Process.whereis(follower) && follower.state_of(follower, shard_id) do
      follower.forget(follower, shard_id)
      Logger.info("tenant #{shard_id} is tombstoned — erased this node's A2 replica of it")
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  # Additive load — never clears the set, so a transiently-empty query result (or a
  # narrower refresh) can't un-tombstone an id this node already knows. Returns `:ok` on a
  # successful directory read, `{:error, reason}` on a failure so init/retry can fast-retry
  # (expert review #33) instead of silently waiting the full refresh interval.
  # A full reload every @full_reload_every refreshes bounds any drift the incremental key could
  # theoretically accumulate; otherwise load only rows changed since the last refresh, minus an
  # overlap so a transaction that began earlier but committed later cannot slip below the
  # high-water mark. Re-reading a row is free — the set is append-only and inserts are idempotent.
  #
  # Daily, not hourly (expert review 2026-10-12 #31): the full reload grows with LIFETIME deletions,
  # and with the cursor now on the Postgres clock (see `cursor_now/0`) the incremental key has no
  # skew hole left to cover; NOTIFY and the storage union remain the other backstops.
  @full_reload_every 288
  @overlap_ms 60_000

  defp refresh_from_directory(state) do
    tick = Map.get(state, :tick, 0) + 1
    since = Map.get(state, :since)

    result =
      cond do
        is_nil(since) or rem(tick, @full_reload_every) == 0 -> load_from_directory(state)
        true -> load_since(state, since)
      end

    case result do
      :ok -> state |> Map.put(:tick, tick) |> Map.put(:since, now_minus_overlap(state.clock))
      _ -> Map.put(state, :tick, tick)
    end
  end

  defp now_minus_overlap(clock), do: DateTime.add(clock.(), -@overlap_ms, :millisecond)

  # The incremental cursor is compared against `shards.updated_at`, which `Directory.tombstone/1`
  # stamps from the Postgres clock, so `since` must come from the same clock (expert review
  # 2026-10-12 #31; the a16ccf8 pattern). A database that can't answer falls back to the node clock:
  # the overlap still covers small skew, and the next successful refresh re-seeds the cursor.
  defp cursor_now do
    Directory.db_now()
  rescue
    _ -> DateTime.utc_now()
  catch
    :exit, _ -> DateTime.utc_now()
  end

  defp load_since(%{table: table, since_loader: loader, on_new: on_new}, since) do
    for id <- loader.(since), do: insert(table, id, on_new)
    :ok
  rescue
    e ->
      Logger.warning("tombstone incremental refresh failed: #{Exception.message(e)}")
      {:error, e}
  catch
    :exit, reason ->
      Logger.warning("tombstone incremental refresh failed: #{inspect(reason)}")
      {:error, reason}
  end

  defp load_from_directory(%{table: table, loader: loader, on_new: on_new}) do
    for id <- loader.(), do: insert(table, id, on_new)
    :ok
  rescue
    e ->
      Logger.warning("tombstone load failed: #{Exception.message(e)}")
      {:error, e}
  catch
    :exit, reason ->
      Logger.warning("tombstone load failed: #{inspect(reason)}")
      {:error, reason}
  end

  # Additive union from durable storage (#6) — the DR backstop that survives a directory restore.
  # Best-effort: a storage blip at boot just means the directory-derived set stands until the storage
  # is reachable (a restore is a rare, operator-driven event).
  # Returns `{:error, reason}` on failure so `attempt_load/1` retries it (expert review 2026-10-13 #20).
  # Before, a failure here was logged and reported as success, so it was never retried.
  defp load_from_storage(%{table: table, on_new: on_new, storage_loader: loader}) do
    case loader.() do
      {:ok, ids} ->
        for id <- ids, do: insert(table, id, on_new)
        :ok

      {:error, reason} ->
        Logger.warning("tombstone storage load failed: #{inspect(reason)}")
        {:error, reason}
    end
  rescue
    e ->
      Logger.warning("tombstone storage load failed: #{Exception.message(e)}")
      {:error, e}
  catch
    :exit, reason ->
      Logger.warning("tombstone storage load failed: #{inspect(reason)}")
      {:error, reason}
  end

  defp listen do
    :ok = Oban.Notifier.listen(Oban, [@channel])
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, refresh_ms())
  end

  defp schedule_retry(ms) do
    Process.send_after(self(), :retry_load, ms)
  end

  defp refresh_ms,
    do: Application.get_env(:fathom, :tenant_tombstone_refresh_ms, @default_refresh_ms)
end
