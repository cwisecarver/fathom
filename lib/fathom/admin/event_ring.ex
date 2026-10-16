defmodule Fathom.Admin.EventRing do
  @moduledoc """
  A bounded, node-local, in-memory ring of recent events per `kind` (expert review 2026-10-15 #10).

  For "what were the last N refusals?" panels: the Prometheus counters say HOW MANY, this keeps
  WHICH. Lives in ETS only — no Postgres, no persistence; a node restart empties it by design.

    * `push(kind, map)` — `:ets.update_counter` for the slot + one `:ets.insert`: lock-free, O(1),
      memory bounded at `capacity/0` entries per kind regardless of event rate.
    * `recent(kind, n)` — the newest `n` entries, newest first.

  **Write it from failure branches only** (a refusal, an eviction), never the success path: this
  is a diagnostic aid, and the hot path pays nothing. Both calls are no-ops/empty when the owner
  process is not running (admin off, tests), so an emitter never has to check.

  Reusable by any panel: pick an atom `kind` (a small closed set; each kind owns `capacity/0`
  slots) and push plain maps. The `Fathom.Admin.EventRing` process only owns the table.
  """
  use GenServer

  @table __MODULE__
  @capacity 64

  @doc false
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Slots retained per kind."
  @spec capacity() :: pos_integer()
  def capacity, do: @capacity

  @doc """
  Records `event` (a map) under `kind`, stamping it with `:t` (system ms) unless it carries one.
  Returns `:ok`; a no-op when the ring is not running.
  """
  @spec push(atom(), map()) :: :ok
  def push(kind, event) when is_atom(kind) and is_map(event) do
    seq = :ets.update_counter(@table, {kind, :head}, 1, {{kind, :head}, -1})
    event = Map.put_new(event, :t, System.system_time(:millisecond))
    :ets.insert(@table, {{kind, rem(seq, @capacity)}, seq, event})
    :ok
  rescue
    # Table absent: the owner is not running (admin disabled / test). Never fail an emitter.
    ArgumentError -> :ok
  end

  @doc "The newest `n` events of `kind`, newest first (at most `capacity/0`)."
  @spec recent(atom(), non_neg_integer()) :: [map()]
  def recent(kind, n) when is_atom(kind) and is_integer(n) and n >= 0 do
    case :ets.lookup(@table, {kind, :head}) do
      [{_, head}] ->
        count = min(n, @capacity)

        for seq <- head..(head - count + 1)//-1,
            seq >= 0,
            [{_, ^seq, event}] <- [:ets.lookup(@table, {kind, rem(seq, @capacity)})],
            do: event

      [] ->
        []
    end
  rescue
    ArgumentError -> []
  end

  @doc "Total events ever pushed under `kind` since the ring started (the head counter)."
  @spec total(atom()) :: non_neg_integer()
  def total(kind) when is_atom(kind) do
    case :ets.lookup(@table, {kind, :head}) do
      [{_, head}] when is_integer(head) -> head + 1
      _ -> 0
    end
  rescue
    ArgumentError -> 0
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:set, :public, :named_table, write_concurrency: true])
    {:ok, %{}}
  end
end
