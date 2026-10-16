defmodule Fathom.Admin.LeaseEvents do
  @moduledoc """
  A bounded, node-local ring of the last 200 lease / fence events (expert review 2026-10-15 #21),
  fed by a telemetry handler, so the admin Cluster page can say WHICH shard was superseded or
  quarantined rather than only how many.

  In-memory by decision (no Postgres, no persistence): an ETS `:ordered_set` keyed by a monotonic
  sequence from a lock-free `:atomics` counter. The key `seq - capacity` is deleted on every insert,
  so the table is bounded without a trim pass, a lock, or a GenServer hop on the emitting process.
  The GenServer only owns the table and the handler attachment.

  Recorded: `:acquired`, `:held`, `:superseded` (the shard lease), `:renew_failed`, `:lapsed` and
  `:skew_lapse` (the node heartbeat), and `:fenced_quarantine`. A SUCCESSFUL `:renewed` is
  deliberately not recorded - it is the per-node lease RPS and would evict everything of interest.
  Detail strings are short and bounded; shard ids ride the ring (never a metric label).
  """
  use GenServer

  @capacity 200
  @table __MODULE__
  @counter_key {__MODULE__, :counter}
  @handler_id "fathom-admin-lease-events"

  @events [
    {[:fathom, :shard, :lease, :acquired], :acquired},
    {[:fathom, :shard, :lease, :held], :held},
    {[:fathom, :shard, :lease, :superseded], :superseded},
    {[:fathom, :shard, :fenced_quarantine], :fenced_quarantine},
    {[:fathom, :shard, :heartbeat, :renew_failed], :renew_failed},
    {[:fathom, :shard, :heartbeat, :lapsed], :lapsed},
    {[:fathom, :shard, :heartbeat, :skew_lapse], :skew_lapse}
  ]

  @type event :: %{
          seq: pos_integer(),
          at_ms: integer(),
          kind: atom(),
          shard_id: String.t() | nil,
          detail: String.t()
        }

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Ring capacity (events kept)."
  @spec capacity() :: pos_integer()
  def capacity, do: @capacity

  @doc "Record one event. Lock-free; a no-op before the owner is up."
  @spec record(atom(), String.t() | nil, String.t()) :: :ok
  def record(kind, shard_id, detail \\ "") do
    case :persistent_term.get(@counter_key, nil) do
      nil ->
        :ok

      ref ->
        seq = :atomics.add_get(ref, 1, 1)

        :ets.insert(
          @table,
          {seq, System.system_time(:millisecond), kind, shard_id, detail}
        )

        :ets.delete(@table, seq - @capacity)
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  @doc "The most recent events, newest first (at most `limit`, default the whole ring)."
  @spec recent(pos_integer()) :: [event()]
  def recent(limit \\ @capacity) do
    @table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0), :desc)
    |> Enum.take(limit)
    |> Enum.map(fn {seq, at, kind, shard_id, detail} ->
      %{seq: seq, at_ms: at, kind: kind, shard_id: shard_id, detail: detail}
    end)
  rescue
    ArgumentError -> []
  end

  @doc "Empty the ring (tests)."
  @spec clear() :: :ok
  def clear do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  def handle_event(_event, measurements, meta, kind) do
    record(kind, Map.get(meta, :shard_id), detail(kind, measurements, meta))
  end

  defp detail(:held, _m, meta), do: "holder " <> clip(Map.get(meta, :holder))
  defp detail(:acquired, _m, meta), do: "epoch " <> clip(Map.get(meta, :epoch))

  defp detail(:fenced_quarantine, m, _meta),
    do: "#{Map.get(m, :unflushed, 0)} un-flushed write(s)"

  defp detail(:renew_failed, _m, meta), do: clip(Map.get(meta, :reason))
  defp detail(:lapsed, m, _meta), do: "generation #{Map.get(m, :generation)}"
  defp detail(:skew_lapse, m, _meta), do: "owner clock lags store by #{Map.get(m, :skew_ms)} ms"
  defp detail(_kind, _m, _meta), do: ""

  defp clip(nil), do: ""
  defp clip(v) when is_binary(v), do: String.slice(v, 0, 80)
  defp clip(v), do: v |> inspect(limit: 5, printable_limit: 80) |> String.slice(0, 80)

  @impl true
  def init(_opts) do
    :ets.new(@table, [:ordered_set, :public, :named_table, write_concurrency: true])
    :persistent_term.put(@counter_key, :atomics.new(1, signed: false))

    for {name, kind} <- @events do
      :telemetry.attach({@handler_id, kind}, name, &__MODULE__.handle_event/4, kind)
    end

    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    for {_name, kind} <- @events, do: :telemetry.detach({@handler_id, kind})
    :ok
  end
end
