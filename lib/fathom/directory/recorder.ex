defmodule Fathom.Directory.Recorder do
  @moduledoc """
  Keeps the per-checkout directory write off the shard data path.

  `Fathom.Shards.checkout/1` used to call `Fathom.Directory.resolve/1` inline — a
  synchronous Postgres upsert on **every** checkout, blocking the caller on the
  control plane. That is exactly the wrong cost for a system whose thesis is
  millions of small shards opened constantly.

  This GenServer replaces that with a coalescing, batched, fire-and-forget buffer:

    * `record/1` is a single lock-free `:ets.insert` (microseconds, no Postgres,
      no GenServer mailbox). A shard hit 1000×/s collapses to one buffered row.
    * a periodic flush (default 1s) drains the buffer and batch-upserts every
      touched shard in one chunked `Repo.insert_all` (see
      `Fathom.Directory.record_batch/1`).

  Flushing is best-effort: a Postgres outage drops a flush and logs, it never
  breaks shard serving — the checkout path no longer depends on Postgres at all.
  """
  use GenServer

  require Logger

  alias Fathom.Directory

  @table __MODULE__
  @flush_table Module.concat(__MODULE__, Flushes)
  @default_flush_ms 1_000

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Buffers a shard access for the next flush. Lock-free ETS write; returns `:ok`
  without ever touching Postgres. Safe to call before the recorder is up (a
  missed touch never breaks serving) — the access is simply dropped.
  """
  @spec record(String.t()) :: :ok
  def record(shard_id) do
    :ets.insert(@table, {shard_id, DateTime.utc_now()})
    :ok
  rescue
    # Table not up yet (boot/teardown). Best-effort: a dropped touch is harmless,
    # the next checkout re-records.
    ArgumentError -> :ok
  end

  @doc """
  Buffers a shard's durable-flush time for the next batch (expert review #28) — the flush
  counterpart of `record/1`, keeping the coordinator's flush hot path off Postgres. Lock-free ETS;
  best-effort (a missed flush record just reads as a slightly-larger loss window later — the safe
  direction). Called by `Fathom.Shard` after a successful upload.
  """
  @spec record_flush(String.t()) :: :ok
  def record_flush(shard_id) do
    :ets.insert(@flush_table, {shard_id, DateTime.utc_now()})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Flushes the buffer to Postgres now and returns the number of rows written.
  Synchronous — used by tests (deterministic, no sleep) and the graceful-stop
  path. The periodic timer does the same work in the background.
  """
  @spec flush() :: non_neg_integer()
  def flush, do: GenServer.call(__MODULE__, :flush)

  @impl true
  def init(opts) do
    # Trap exits so a supervisor :shutdown runs terminate/2's final flush (expert
    # review #30): without this a GenServer is killed outright by the shutdown
    # signal and terminate never runs — the documented "don't lose the last window
    # of touches on a graceful stop" was dead code on every deploy, and those
    # touches feed last_active_at, the revert write-age guard's input.
    Process.flag(:trap_exit, true)

    # public + write_concurrency: many checkout processes insert concurrently;
    # the recorder is the only reader (during flush).
    :ets.new(@table, [:set, :public, :named_table, write_concurrency: true])
    # Parallel buffer for durable-flush times (#28), same coalesce/batch shape.
    :ets.new(@flush_table, [:set, :public, :named_table, write_concurrency: true])

    flush_ms =
      Keyword.get(
        opts,
        :flush_ms,
        Application.get_env(:fathom, :directory_flush_ms, @default_flush_ms)
      )

    schedule(flush_ms)
    {:ok, %{flush_ms: flush_ms}}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    {:reply, do_flush(), state}
  end

  @impl true
  def handle_info(:flush, state) do
    do_flush()
    schedule(state.flush_ms)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    # Don't lose the last window of touches on a graceful stop (e.g. a deploy).
    do_flush()
    :ok
  end

  defp schedule(ms), do: Process.send_after(self(), :flush, ms)

  defp do_flush do
    flush_table(@table, &Directory.record_batch/1, [:fathom, :directory, :flush]) +
      flush_table(
        @flush_table,
        &Directory.record_flush_batch/1,
        [:fathom, :directory, :flush_recorded]
      )
  end

  defp flush_table(table, upsert, event) do
    case drain(table) do
      [] ->
        0

      rows ->
        try do
          n = upsert.(rows)
          if n > 0, do: :telemetry.execute(event, %{count: n}, %{})
          n
        rescue
          e ->
            Logger.warning("Directory.Recorder flush failed: #{inspect(e)}")
            restore(table, rows)
            0
        catch
          :exit, reason ->
            Logger.warning("Directory.Recorder flush exited: #{inspect(reason)}")
            restore(table, rows)
            0
        end
    end
  end

  # A failed batch must not lose the drained touches (expert review #11): these feed
  # `last_active_at`, the sole input to the revert write-age force-guard — dropping
  # them during a Postgres outage is exactly when operators are reverting things.
  # Re-buffer them for the next cycle. `insert_new` (not `insert`) so a fresher touch
  # recorded DURING the failed flush wins — anything re-recorded post-drain is by
  # construction newer than the drained value.
  defp restore(table, rows) do
    Enum.each(rows, &:ets.insert_new(table, &1))
    :telemetry.execute([:fathom, :directory, :flush_retry], %{count: length(rows)}, %{})
  rescue
    # Table gone (teardown) — nothing to restore into.
    ArgumentError -> :ok
  end

  # Atomically take each buffered key (`:ets.take` reads + deletes in one op), so a
  # touch arriving mid-flush is either captured with its freshest value or left in
  # the table for the next cycle — never silently lost.
  defp drain(table) do
    table
    |> :ets.tab2list()
    |> Enum.flat_map(fn {shard_id, _ts} -> :ets.take(table, shard_id) end)
  end
end
