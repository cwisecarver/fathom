defmodule Fathom.Shard.Storage.HeartbeatCache do
  @moduledoc """
  A tiny per-owner memo of heartbeat reads for the S3 steal path (review 2026-07-23 #13).

  On a node death the survivor steals MANY shards from the SAME dead owner, and each
  steal's `owner_live?` re-read the identical `heartbeat/<owner>` object — a 1,000-shard
  failover issued ~1,000 redundant GETs of one object, serialized per shard on the
  takeover critical path and contending the Finch pool with the pulls. This caches the
  raw heartbeat read per owner for a sub-second TTL, collapsing that to ~one GET per
  owner per second.

  Safety: the verdict a stale read can flip is bounded by the cache TTL, which must stay
  well inside `Storage.steal_margin_ms/0` (default 5000 ms) — the margin exists to absorb
  exactly this class of skew. Errors are never cached (the fail-closed no-steal-on-blip
  behavior stays per-call). The table is a public ETS set owned here; readers fall back
  to a plain miss when the table isn't up (boot/tests), so the S3 backend never depends
  on this process.
  """
  use GenServer

  @table __MODULE__

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Cached value for `owner` if fetched within `ttl_ms`, else `:miss`."
  @spec get(String.t(), pos_integer()) :: {:hit, term()} | :miss
  def get(owner, ttl_ms) do
    case :ets.lookup(@table, owner) do
      [{^owner, value, at}] ->
        if System.monotonic_time(:millisecond) - at <= ttl_ms, do: {:hit, value}, else: :miss

      _ ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc "Store `value` for `owner`, stamped now. Best-effort (no-op when the table isn't up)."
  @spec put(String.t(), term()) :: :ok
  def put(owner, value) do
    :ets.insert(@table, {owner, value, System.monotonic_time(:millisecond)})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:set, :public, :named_table, write_concurrency: true])
    {:ok, %{}}
  end
end
