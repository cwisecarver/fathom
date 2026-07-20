defmodule Fathom.Shard.WriteFence do
  @moduledoc """
  The write circuit-breaker's lock-free per-shard flag (expert review 2026-07-19 #3).

  A node reachable by its clients but cut from object storage keeps its coordinators alive: the
  heartbeat renewals fail, the periodic durability flush skips (`:fence_skip`) — but nothing on the
  checkout/execute path consults that, so the coordinator keeps **accepting and ACKing writes**. After
  `ttl + steal_margin` a peer legitimately steals the shard; when the partition heals the cut node
  self-fences and quarantines **every write it ACKed since its last flush** — a loss window equal to
  the *partition duration*, not the flush interval the RPO contract advertises.

  This is the missing consult. `Fathom.Shard` publishes a shard id here once its heartbeat has been
  not-valid for longer than `ttl + steal_margin` ("provably stealable"), and `Fathom.ShardExecutor`
  reads it O(1) before each **write** — refusing with 503 `FILO_STALE_LEASE` while **reads keep
  serving** from the local copy. The window collapses to ~`ttl + steal_margin`.

  A public, `read_concurrency` ETS set (the `Fathom.Tenants.Tombstones` shape): reads are lock-free
  from any process — no coordinator hop on the data path — and a missing table (owner not started, or
  gone during shutdown) rescues to "not fenced", so the executor fails toward *serving* if this table
  is ever absent (the coordinator, which owns the loss risk, is the one that sets the flag). The
  coordinator only ever sets the flag when `:fence_writes_when_stealable` is on (default: prod), so an
  empty table — and thus no refusals — is the whole behavior when the gate is off.
  """
  use GenServer

  @table __MODULE__

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether writes to `shard_id` are fenced (the node is provably stealable). Lock-free ETS read."
  @spec fenced?(String.t()) :: boolean()
  def fenced?(shard_id) do
    :ets.member(@table, shard_id)
  rescue
    ArgumentError -> false
  end

  @doc "Fence writes to `shard_id` (idempotent)."
  @spec fence(String.t()) :: :ok
  def fence(shard_id) do
    :ets.insert(@table, {shard_id})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Lift the write fence on `shard_id` (idempotent)."
  @spec unfence(String.t()) :: :ok
  def unfence(shard_id) do
    :ets.delete(@table, shard_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Drop the shard's row when its coordinator stops (alias of unfence)."
  @spec forget(String.t()) :: :ok
  def forget(shard_id), do: unfence(shard_id)

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, %{}}
  end
end
