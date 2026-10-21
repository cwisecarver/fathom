defmodule Fathom.Shard.Replication.LagGauge do
  @moduledoc """
  The `catching_up_shards` gauge for the 10 s telemetry poller (admin observability 2026-10-15 P2).

  It used to call every Session (`Fleet.lag_report/1`, one `GenServer.call` each) every 10 s, which
  defeated Session hibernation and queued calls behind commits (expert review 2026-10-17 #2). Now a
  Session records itself in a small ETS set, owned by `Fleet`, when its catch-up retry arms and
  deletes the row when it clears or terminates, and the gauge reads the row count: ZERO calls to any
  Session. `Fleet.lag_report/1` stays for the on-demand admin page.

  Rows are keyed by Session pid. A Session that dies without running `terminate/2` (killed) leaves
  its row behind, so `count/0` drops rows whose pid is no longer alive while it counts; a leaked
  row can therefore overstate the gauge for at most one poll.
  """
  alias Fathom.Shard.Replication.Fleet

  @table __MODULE__

  @doc false
  @spec init_table() :: :ok
  def init_table do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    end

    :ok
  end

  @doc """
  Record that Session `pid` has (`true`) or no longer has (`false`) a catch-up retry armed. A no-op
  when the table does not exist (replication not running).
  """
  @spec mark(pid(), boolean()) :: :ok
  def mark(pid, true) do
    :ets.insert(@table, {pid})
    :ok
  rescue
    ArgumentError -> :ok
  end

  def mark(pid, false) do
    :ets.delete(@table, pid)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Sessions with a catch-up armed right now, pruning rows of Sessions that have died."
  @spec count() :: non_neg_integer()
  def count do
    @table
    |> :ets.tab2list()
    |> Enum.count(fn {pid} ->
      alive? = Process.alive?(pid)
      if not alive?, do: :ets.delete(@table, pid)
      alive?
    end)
  rescue
    ArgumentError -> 0
  end

  @doc """
  The flagged Session pids that are still alive — the set `Fleet.lag_report/1` asks (expert review
  2026-10-21 #24). Dead rows are left for `count/0` to prune.
  """
  @spec flagged() :: [pid()]
  def flagged do
    @table
    |> :ets.tab2list()
    |> Enum.flat_map(fn {pid} -> if Process.alive?(pid), do: [pid], else: [] end)
  rescue
    ArgumentError -> []
  end

  @doc "Poller entry point: emits `[:fathom, :replication, :lag]` with `catching_up_shards`."
  @spec measure() :: :ok
  def measure do
    if Fleet.replicating?() do
      :telemetry.execute([:fathom, :replication, :lag], %{catching_up_shards: count()}, %{})
    end

    :ok
  end
end
