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
      so every node refuses the id and purges its warm-follower copy immediately;
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
    retry_ms = Keyword.get(opts, :retry_ms, DenyList.initial_retry_ms())

    # public read_concurrency: admission reads directly from the caller process.
    :ets.new(table, [:set, :public, :named_table, read_concurrency: true])

    listen()

    # Union in the durable storage tombstones at boot (#6): a Postgres point-in-time restore rolls the
    # directory back and can un-tombstone a deleted tenant, but storage is not rolled back. This scan
    # keeps the re-mint guard complete despite a directory restore. Boot-only — the in-memory set is
    # append-only, so the periodic (directory-only) refresh never drops these; no recurring LIST.
    # Storage is S3-derived, independent of the Postgres directory, so it also loads (and contributes
    # deny coverage) during a Postgres outage that fails the directory load below.
    load_from_storage(table)

    state = %{table: table, loader: loader, retry_ms: retry_ms}

    case load_from_directory(state) do
      :ok ->
        schedule_refresh()
        {:ok, Map.put(state, :loaded, true)}

      # Expert review #33: a FAILED boot load must not wait the full refresh interval — that
      # leaves the re-mint guard empty (410 contract broken) for up to 5 min during a Postgres
      # wobble coincident with this restart. Fast-retry with backoff, and signal degraded so the
      # window is alertable.
      {:error, reason} ->
        DenyList.degraded(:tombstones, reason)
        schedule_retry(retry_ms)
        {:ok, Map.put(state, :loaded, false)}
    end
  end

  @impl true
  def handle_info({:notification, @channel, %{"shard_id" => shard_id}}, state)
      when is_binary(shard_id) do
    # One event does both jobs of a delete broadcast: block re-mint (ETS) and drop this
    # node's lease-less warm copy of the erased shard (GDPR timeliness — otherwise the
    # copy lingers until the follower's next refresh evicts it for leaving active_recent).
    insert(state.table, shard_id)
    purge_warm(shard_id)
    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    # Steady-state refresh: the set is already loaded and append-only, so a failure here just
    # logs — the last-known set stands. Only the BOOT-unloaded path (below) fast-retries.
    load_from_directory(state)
    schedule_refresh()
    {:noreply, state}
  end

  def handle_info(:retry_load, %{loaded: false} = state) do
    case load_from_directory(state) do
      :ok ->
        # Recovered — re-attempt the storage backstop (it may have failed at boot too), signal
        # recovery, and fall back to the normal refresh cadence.
        load_from_storage(state.table)
        DenyList.recovered(:tombstones)
        schedule_refresh()
        {:noreply, %{state | loaded: true}}

      {:error, reason} ->
        DenyList.degraded(:tombstones, reason)
        next = DenyList.next_retry_ms(state.retry_ms)
        schedule_retry(next)
        {:noreply, %{state | retry_ms: next}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp insert(table, shard_id) do
    :ets.insert(table, {shard_id})
  rescue
    ArgumentError -> :ok
  end

  defp purge_warm(shard_id) do
    Fathom.Shard.WarmFollower.purge_now(shard_id)
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  # Additive load — never clears the set, so a transiently-empty query result (or a
  # narrower refresh) can't un-tombstone an id this node already knows. Returns `:ok` on a
  # successful directory read, `{:error, reason}` on a failure so init/retry can fast-retry
  # (expert review #33) instead of silently waiting the full refresh interval.
  defp load_from_directory(%{table: table, loader: loader}) do
    for id <- loader.(), do: insert(table, id)
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
  defp load_from_storage(table) do
    case Fathom.Shard.Storage.tombstoned_ids() do
      {:ok, ids} -> for id <- ids, do: insert(table, id)
      {:error, reason} -> Logger.warning("tombstone storage load failed: #{inspect(reason)}")
    end

    :ok
  rescue
    e ->
      Logger.warning("tombstone storage load failed: #{Exception.message(e)}")
      :ok
  catch
    :exit, reason ->
      Logger.warning("tombstone storage load failed: #{inspect(reason)}")
      :ok
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
