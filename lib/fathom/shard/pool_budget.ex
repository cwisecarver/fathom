defmodule Fathom.Shard.PoolBudget do
  @moduledoc """
  A node-wide bound on IDLE pooled SQLite handles (perf review 2026-10-14 #16).

  Each coordinator's `Fathom.Shard.HandlePool` caps idle handles per shard (`max_per_scope` per
  scope), but nothing bounded them per NODE. With the pool drain grace (#4) an idle shard keeps its
  last stream's handle for `drain_grace_ms` after the stream closes, so residency grows with the
  rate of distinct shards served: about (shards touched in the last grace window, plus busy shards)
  x up to 2 handles x ~150-170 KB each. This counts them and caps them.

  ## What happens at the cap

  A checkin that would take the node over the budget is NOT pooled: the coordinator closes that
  handle (through its off-coordinator close chain, exactly like a checkin while draining), and the
  shard's next stream opens fresh. A checkout is never refused and nothing already pooled is closed.
  The budget is soft (check, then add; two coordinators racing at the edge can each add one).

  Closing the node's OLDEST idle handle instead would need a node-wide ordered index updated on every
  pooled checkin and take (the request path) plus a cross-coordinator eviction message, because each
  handle belongs to the coordinator that owns its shard's fence. Not built: with the default 250 ms
  grace an idle shard's handle is gone 250 ms after its last stream, so the cap is only reached at
  thousands of distinct shards per second, and the cheap refusal bounds memory the same way.

  ## Accounting

  A `:counters` total (read per checkin) plus one ETS row per coordinator `{pid, count, watched}`.
  The coordinator adjusts both whenever its pool's size changes. The row exists so a coordinator
  that dies WITHOUT running `terminate/2` (a brutal kill) cannot leak its share forever and slowly
  disable pooling node-wide: this process monitors each coordinator on its first adjustment and
  subtracts the row when it goes down. Steady state is two lock-free counter updates per pool change;
  the monitor costs one cast per coordinator lifetime.

  Every function degrades to "no budget" (always admit, no accounting) when this process is not
  running, so a coordinator never fails because of it.
  """
  use GenServer

  @table __MODULE__
  @total {__MODULE__, :total}
  @default_budget 8_192

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The node-wide idle pooled-handle budget: `:connection_pool_idle_budget` (default
  #{@default_budget}). `0` pools nothing.
  """
  @spec budget() :: non_neg_integer()
  def budget do
    case Application.get_env(:fathom, :connection_pool_idle_budget, @default_budget) do
      n when is_integer(n) and n >= 0 -> n
      _ -> @default_budget
    end
  end

  @doc "True while the node holds fewer idle pooled handles than `budget/0`."
  @spec admit?() :: boolean()
  def admit? do
    case :persistent_term.get(@total, nil) do
      nil -> true
      ref -> :counters.get(ref, 1) < budget()
    end
  end

  @doc "Idle pooled handles held node-wide (0 when the budget is not running)."
  @spec total() :: integer()
  def total do
    case :persistent_term.get(@total, nil) do
      nil -> 0
      ref -> :counters.get(ref, 1)
    end
  end

  @doc """
  Record that `owner`'s pool grew (`delta > 0`) or shrank (`delta < 0`). Called by the coordinator
  that owns the pool, on its own process, so its row has a single writer.
  """
  @spec adjust(pid(), integer()) :: :ok
  def adjust(_owner, 0), do: :ok

  def adjust(owner, delta) when is_integer(delta) do
    case :persistent_term.get(@total, nil) do
      nil ->
        :ok

      ref ->
        :counters.add(ref, 1, delta)

        [_count, watched] =
          :ets.update_counter(@table, owner, [{2, delta}, {3, 0}], {owner, 0, 0})

        if watched == 0 do
          :ets.update_element(@table, owner, {3, 1})
          GenServer.cast(__MODULE__, {:watch, owner})
        end

        :ok
    end
  rescue
    # The table went away under us (this process restarting): skip the row, never fail a checkin.
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    :persistent_term.put(@total, :counters.new(1, [:write_concurrency]))
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:watch, owner}, state) do
    Process.monitor(owner)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, owner, _reason}, state) do
    case :ets.take(@table, owner) do
      [{^owner, count, _watched}] -> :counters.sub(:persistent_term.get(@total), 1, count)
      [] -> :ok
    end

    {:noreply, state}
  end
end
