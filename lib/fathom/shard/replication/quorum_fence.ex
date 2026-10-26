defmodule Fathom.Shard.Replication.QuorumFence do
  @moduledoc """
  Shards on this node holding a commit that failed its follower quorum (Jepsen finding 7,
  2026-10-25, `docs/reviews/jepsen-2026-10-24.md`).

  A write that misses its quorum gets a 503, but SQLite has already committed it locally and there
  is no un-commit. Without this fence later statements on the node could read it, and if ownership
  then moved to a node that never received the frames, the value a client had seen vanished: a
  history no serializable database allows. The Jepsen run saw exactly that, 7.5 s apart.

  So `Fathom.ShardExecutor` marks the shard here when `Session.commit/4` fails, and before any later
  statement on a marked shard it runs one more quorum round. If the round succeeds the frames are on
  a quorum and the mark is cleared; if it fails the statement gets the same 503 as the write did. A
  shard whose quorum is down therefore serves nothing on this node, which is no worse than before:
  its writes were already refused.

  ## What it does not cover

  A read that runs AFTER the failing write's local commit but BEFORE that write's quorum round has
  returned can still see it. That window is at most one replication timeout, and closing it would
  need reads pinned to the quorum-acked WAL extent, which SQLite does not offer through this driver.

  ## Marks are generations

  `mark/1` bumps a counter and `clear/2` deletes the row only if the counter is still the one read
  before the confirming round. Otherwise a round that started before a NEW failure could clear that
  failure's mark: stream A's write fails and marks while stream B's confirm, which read the WAL
  before A's frames, is finishing.

  The table is public and owned by this process (the `Fathom.Shard.WriteFence` shape); a missing
  table rescues to "nothing marked". Rows are not tied to a coordinator: a mark left by a
  coordinator that stopped is cleared by the first statement after the shard reopens, because its
  round then has nothing it cannot ship.
  """
  use GenServer

  @table __MODULE__

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "The shard's mark generation, or nil when it holds no unconfirmed commit. Lock-free."
  @spec marked(String.t()) :: pos_integer() | nil
  def marked(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{_, gen}] -> gen
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "Record that a commit on `shard_id` failed its quorum."
  @spec mark(String.t()) :: :ok
  def mark(shard_id) do
    _ = :ets.update_counter(@table, shard_id, 1, {shard_id, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Clear the mark, but only if no failure has bumped it past `gen` since it was read."
  @spec clear(String.t(), pos_integer()) :: :ok
  def clear(shard_id, gen) do
    _ = :ets.select_delete(@table, [{{shard_id, gen}, [], [true]}])
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "How many shards are marked right now."
  @spec count() :: non_neg_integer()
  def count do
    case :ets.info(@table, :size) do
      n when is_integer(n) -> n
      :undefined -> 0
    end
  end

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
