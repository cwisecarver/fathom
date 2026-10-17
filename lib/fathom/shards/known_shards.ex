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

  ## Bounded, with two generations

  The set is two generations: the current one and the previous one. A lookup checks both. When the
  current generation reaches `@max_entries`, the previous one is dropped and the current one
  becomes the previous, so an id survives one rotation and is forgotten after two. Size stays
  within about twice `@max_entries`.

  The old design cleared the whole set at the cap. That made every known shard unknown at once,
  so a fleet at the cap paid one directory read per shard in a burst (expert review 2026-10-17 #13).
  Two generations keep the recently-used ids answering across a rotation.

  Rotation is a compare-and-swap on a generation counter, so only one caller rotates. Losing
  callers re-read the counter and write into the new current generation. A write that races a
  rotation may land in the previous generation. That costs at most one extra directory read, and
  never a wrong answer for the fork decision.
  """
  use GenServer

  @table __MODULE__
  @prev_table __MODULE__.Prev
  @gen_key {__MODULE__, :generation}
  @max_entries 200_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Whether `shard_id` is known to exist. `false` when the table is not up."
  @spec known?(String.t()) :: boolean()
  def known?(shard_id) do
    :ets.member(@table, shard_id) or :ets.member(@prev_table, shard_id)
  rescue
    ArgumentError -> false
  end

  @doc "Record that `shard_id` exists (it opened, or the directory has it)."
  @spec put(String.t()) :: :ok
  def put(shard_id) do
    gen = current_generation()

    if :ets.info(table_for(gen), :size) >= @max_entries, do: rotate(gen)

    :ets.insert(table_for(current_generation()), {shard_id})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  def max_entries, do: @max_entries

  # Drops the previous generation and promotes the current one. Only the caller whose CAS from
  # `gen` succeeds clears: the table it clears is the old previous generation, which becomes the
  # new current. The CAS runs first, so a loser never clears a table that is already current.
  defp rotate(gen) do
    ref = :persistent_term.get(@gen_key)

    if :atomics.compare_exchange(ref, 1, gen, gen + 1) == :ok do
      :ets.delete_all_objects(table_for(gen + 1))
    end

    :ok
  end

  defp current_generation do
    :atomics.get(:persistent_term.get(@gen_key), 1)
  end

  # Generation N lives in @table when N is even and in @prev_table when N is odd, so the two tables
  # swap roles on each rotation without being renamed.
  defp table_for(gen) when rem(gen, 2) == 0, do: @table
  defp table_for(_gen), do: @prev_table

  @impl true
  def init(_opts) do
    for table <- [@table, @prev_table] do
      :ets.new(table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    :persistent_term.put(@gen_key, :atomics.new(1, signed: false))

    {:ok, nil}
  end
end
