defmodule Fathom.Shard.ReopenGap do
  @moduledoc """
  How long a shard stays closed after an idle stop before something opens it again — the
  measurement the idle-timeout default has to be set from (expert review 2026-10-01 #10).

  ## Why it exists

  Every idle drop costs a reopen later: lock DELETE + PUT, a full GET + fsync, the open HEADs and,
  with replication on, a full seed to every follower. An open idle coordinator costs ~4–6 KiB. So
  whether `:shard_idle_ms` (60 s) is too short depends on one number nobody had: how soon a dropped
  shard comes back. A shard that is reopened 70 s after an idle drop paid the whole churn cycle to
  save a minute of a few KiB; one reopened a day later did not. This records that gap.

  ## Shape

  A public ETS row `{shard_id, dropped_at_ms}` written when a coordinator stops for idleness, taken
  (read + deleted) on the next successful cold open, which emits
  `[:fathom, :shard, :reopen]` with `%{gap_ms: …}`. Only IDLE stops are recorded: a drain, a
  shutdown or an eviction is not the policy being measured.

  ## Bounded

  A shard dropped and never reopened would leave its row forever, and a node sees far more distinct
  shards than it holds open. Rows older than `@horizon_ms` are swept every `@sweep_ms`: a gap that
  long is already past every bucket the decision cares about, so losing it costs nothing. The sweep
  is one `select_delete` — a full table pass, but on a table that holds at most an hour of drops.

  Every call rescues a missing table to a no-op: this is observability, and the cold-open path must
  never fail because of it.
  """
  use GenServer

  @table __MODULE__
  @horizon_ms 60 * 60 * 1000
  @sweep_ms 60 * 1000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Record that `shard_id` was just dropped for idleness."
  @spec dropped(String.t()) :: :ok
  def dropped(shard_id) do
    :ets.insert(@table, {shard_id, now_ms()})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  `shard_id` just finished a cold open. If it was idle-dropped within the horizon, emit the gap.
  """
  @spec reopened(String.t()) :: :ok
  def reopened(shard_id) do
    case :ets.take(@table, shard_id) do
      [{_, dropped_at}] ->
        :telemetry.execute(
          [:fathom, :shard, :reopen],
          %{gap_ms: now_ms() - dropped_at},
          %{shard_id: shard_id}
        )

      [] ->
        :ok
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  # Delete every row dropped before `cutoff_ms`. Public for the test; the timer calls it.
  @spec sweep(integer()) :: non_neg_integer()
  def sweep(cutoff_ms) do
    :ets.select_delete(@table, [{{:_, :"$1"}, [{:<, :"$1", cutoff_ms}], [true]}])
  end

  @doc false
  def now_ms, do: System.monotonic_time(:millisecond)

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    schedule_sweep()
    {:ok, nil}
  end

  @impl true
  def handle_info(:sweep, state) do
    sweep(now_ms() - @horizon_ms)
    schedule_sweep()
    {:noreply, state}
  end

  defp schedule_sweep, do: Process.send_after(self(), :sweep, @sweep_ms)
end
