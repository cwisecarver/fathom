defmodule Fathom.Rebalancer.DecisionLog do
  @moduledoc """
  A bounded, in-memory record of what the rebalancer decided and what the LB did with it (expert
  review 2026-10-15 #13). The policy's `reason` and every declined candidate used to be built and
  dropped; this keeps the last 50 interesting ticks so the `/admin/rebalancer` page can answer
  "why did / didn't it move that shard".

  **In-memory and per node, by decision (no new Postgres tables).** `RebalanceJob` runs on whichever
  node holds Oban leadership, so the tick ring is populated on THAT node only; a viewer on another
  node sees an empty ring (the page says so). The LB-apply status is written by whichever node ran
  `LbApply.apply!/0` last, likewise local.

  One named ETS table owned by this GenServer (so it survives the job process). The tick ring is a
  fixed set of slots written round-robin (`seq rem @max_ticks`), so it is bounded by construction —
  no trimming pass. Every write is wrapped: recording is diagnostics and must never fail a
  rebalance tick or an LB apply, so a missing table (the owner restarting, a test that did not
  start it) is a silent no-op.

  A tick is stored only if it moved or declined something; a quiet tick just refreshes
  `last_tick`, so an hour of idle ticks can't push the one interesting decision out of the ring.
  """
  use GenServer

  @table __MODULE__
  @max_ticks 50
  # A tick with a mass of declined candidates (a fleet-wide surge) keeps only its hottest few.
  @max_declined 20
  @seq_key :seq
  @last_tick_key :last_tick
  @lb_apply_key :lb_apply

  @type tick :: %{
          seq: integer(),
          at: DateTime.t(),
          threshold: float() | nil,
          moves: [Fathom.Rebalancer.Policy.move()],
          declined: [Fathom.Rebalancer.Policy.decline()],
          declined_total: non_neg_integer()
        }

  @type lb_apply :: %{
          outcome: atom(),
          at: DateTime.t(),
          error: String.t() | nil,
          hash: String.t() | nil,
          entries: non_neg_integer() | nil
        }

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, nil}
  end

  @doc """
  Record one rebalance tick: the moves the policy proposed and the hot candidates it declined
  (hottest first, capped). Always refreshes `last_tick/0`; stores a ring entry only when there is
  something to show.
  """
  @spec record_tick([Fathom.Rebalancer.Policy.move()], [Fathom.Rebalancer.Policy.decline()]) ::
          :ok
  def record_tick(moves, declined) do
    now = DateTime.utc_now()
    :ets.insert(@table, {@last_tick_key, now})

    if moves != [] or declined != [] do
      seq = :ets.update_counter(@table, @seq_key, 1, {@seq_key, 0})

      entry = %{
        seq: seq,
        at: now,
        threshold: moves |> List.first(%{}) |> Map.get(:threshold),
        moves: moves,
        declined:
          declined |> Enum.sort_by(&{-&1.q_per_s, &1.shard_id}) |> Enum.take(@max_declined),
        declined_total: length(declined)
      }

      :ets.insert(@table, {{:tick, rem(seq, @max_ticks)}, entry})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "The recorded ticks, newest first."
  @spec recent() :: [tick()]
  def recent do
    @table
    |> :ets.match({{:tick, :_}, :"$1"})
    |> Enum.map(&hd/1)
    |> Enum.sort_by(& &1.seq, :desc)
  rescue
    ArgumentError -> []
  end

  @doc "When the leader last ran a tick on THIS node (even a quiet one), or `nil`."
  @spec last_tick() :: DateTime.t() | nil
  def last_tick, do: lookup(@last_tick_key)

  @doc "Record the outcome of an `LbApply.apply!/0` (time stamped here)."
  @spec record_lb_apply(atom(), keyword()) :: :ok
  def record_lb_apply(outcome, opts \\ []) do
    :ets.insert(
      @table,
      {@lb_apply_key,
       %{
         outcome: outcome,
         at: DateTime.utc_now(),
         error: Keyword.get(opts, :error),
         hash: Keyword.get(opts, :hash),
         entries: Keyword.get(opts, :entries)
       }}
    )

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "The last LB-apply on this node, or `nil` if none has run."
  @spec lb_apply_status() :: lb_apply() | nil
  def lb_apply_status, do: lookup(@lb_apply_key)

  defp lookup(key) do
    case :ets.lookup(@table, key) do
      [{^key, v}] -> v
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end
end
