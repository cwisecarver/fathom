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
      directory row and restarts, the same escape hatch as any tombstone).

  Fathom has no BEAM cluster, so the one shared push channel is Postgres, and Oban's
  notifier is already running on it — best-effort, with the periodic refresh as the
  convergence backstop.
  """
  use GenServer

  require Logger

  alias Fathom.Directory

  @table __MODULE__
  @channel :fathom_tenant_deleted
  @default_refresh_ms 300_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

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
    insert(shard_id)
    :ok
  end

  @impl true
  def init(_opts) do
    # public read_concurrency: admission reads directly from the caller process.
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    listen()
    load_from_directory()
    schedule_refresh()

    {:ok, %{}}
  end

  @impl true
  def handle_info({:notification, @channel, %{"shard_id" => shard_id}}, state)
      when is_binary(shard_id) do
    # One event does both jobs of a delete broadcast: block re-mint (ETS) and drop this
    # node's lease-less warm copy of the erased shard (GDPR timeliness — otherwise the
    # copy lingers until the follower's next refresh evicts it for leaving active_recent).
    insert(shard_id)
    purge_warm(shard_id)
    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    load_from_directory()
    schedule_refresh()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp insert(shard_id) do
    :ets.insert(@table, {shard_id})
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
  # narrower refresh) can't un-tombstone an id this node already knows.
  defp load_from_directory do
    for id <- Directory.deleted_shard_ids(), do: insert(id)
    :ok
  rescue
    e ->
      Logger.warning("tombstone load failed: #{Exception.message(e)}")
      :ok
  catch
    :exit, reason ->
      Logger.warning("tombstone load failed: #{inspect(reason)}")
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

  defp refresh_ms,
    do: Application.get_env(:fathom, :tenant_tombstone_refresh_ms, @default_refresh_ms)
end
