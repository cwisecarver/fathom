defmodule Fathom.Shards.MigrationGate do
  @moduledoc """
  A short-lived, node-local refusal to RE-OPEN a shard this node just drained for another node's
  migration (expert review 2026-10-12 #23).

  ## Why

  A migration whose shard is held by a coordinator on another node asks that node to
  `drain_for_migration` over the command channel, waits for the command to complete, then acquires
  the lease itself (`Fathom.Migrator.ShardMigration`). The load balancer keeps routing the tenant to
  the holder throughout, so on a hot tenant the very next request after the drain re-opened the
  shard — re-acquiring the lease — before the migrator's acquire landed (it polls the command every
  200 ms). The migrator then read `held`, snoozed, and drained again next time with the same
  outcome: the hot tenants, the ones a rollout or an emergency revert most needs, never migrated.

  ## What

  The command poller closes the gate for the shard just before the drain and re-opens it if the
  drain fails. While closed (at most `:migration_drain_gate_ms`, default 3 s), a checkout that would
  START a coordinator is refused with `{:shard_migrating, :drained_for_migration}` — the existing
  retryable 503 `FILO_SHARD_BUSY`. Only the cold-open branch of `Fathom.Shards.ensure/1` consults it,
  so a running shard's checkout never pays even the ETS lookup.

  The gate only has to cover the gap between the drain and the migrator's acquire: once the migrator
  holds the lease, a re-open is refused `{:shard_held, …}` anyway. So it is a TTL, not a handshake —
  no second command to lift it, and nothing to leak if the migrator gives up (the gate simply
  expires). Its cost is that a migration finishing faster than the TTL leaves the tenant refused for
  the remainder; the default is sized to the migrator's poll + acquire, not to the migration.
  """
  use GenServer

  @table __MODULE__
  @default_ttl_ms 3_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Refuses re-opens of `shard_id` on this node for `ttl_ms` (default `:migration_drain_gate_ms`).
  Best-effort: `:ok` even when the table is not up.
  """
  @spec close(Fathom.ShardId.t(), non_neg_integer()) :: :ok
  def close(shard_id, ttl_ms \\ ttl_ms()) do
    now = now_ms()

    # Closes are rare (one per remote migration drain), so sweeping expired entries here bounds the
    # table without a timer.
    :ets.select_delete(@table, [{{:_, :"$1"}, [{:"=<", :"$1", now}], [true]}])
    :ets.insert(@table, {shard_id, now + ttl_ms})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Lifts the gate for `shard_id` (the drain failed, so the shard keeps serving here)."
  @spec open(Fathom.ShardId.t()) :: :ok
  def open(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Whether a re-open of `shard_id` is refused right now. `false` when the table is not up."
  @spec closed?(Fathom.ShardId.t()) :: boolean()
  def closed?(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [] -> false
      [{_, until}] -> now_ms() < until
    end
  rescue
    ArgumentError -> false
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp ttl_ms, do: Application.get_env(:fathom, :migration_drain_gate_ms, @default_ttl_ms)

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, nil}
  end
end
