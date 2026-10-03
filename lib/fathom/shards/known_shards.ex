defmodule Fathom.Shards.KnownShards do
  @moduledoc """
  A node-local set of shard ids this node has seen exist — consulted ONLY by the novel-shard RATE
  gate (expert review 2026-10-01 #18).

  ## Why

  With `NOVEL_SHARD_RATE` set (the docs recommend it for prod), every open decides "is this id
  novel?" as `not File.exists?(local db) and not in the directory`. An idle drop deletes the local
  file, so for a shard waking from idle the file check never short-circuits and every cold reopen
  does a synchronous `Directory.get/1` on the checkout path — thousands per second through the
  shared Repo pool during a failover burst, for shards that were obviously known.

  ## Only the rate gate, never fork-from-template

  `novel?` also decides fork-from-template, and there a stale "known" is wrong in a way that
  matters: a tenant deleted and re-created would skip its template fork and be born empty. For the
  rate gate a stale "known" costs one spray id let through, which is the gate's normal tolerance.
  So entries are never invalidated, and the fork decision never reads them.

  ## Bounded

  At `@max_entries` the table is cleared rather than evicted entry by entry: the cost is one
  directory read per shard as the set refills, the cost every open pays today.
  """
  use GenServer

  @table __MODULE__
  @max_entries 200_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether `shard_id` is known to exist. `false` when the table is not up."
  @spec known?(String.t()) :: boolean()
  def known?(shard_id) do
    :ets.member(@table, shard_id)
  rescue
    ArgumentError -> false
  end

  @doc "Record that `shard_id` exists (it opened, or the directory has it)."
  @spec put(String.t()) :: :ok
  def put(shard_id) do
    if :ets.info(@table, :size) >= @max_entries, do: :ets.delete_all_objects(@table)
    :ets.insert(@table, {shard_id})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  def max_entries, do: @max_entries

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, nil}
  end
end
