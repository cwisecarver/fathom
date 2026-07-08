defmodule Fathom.Rebalancer.Reporter do
  @moduledoc """
  Publishes this node's hot shards to Postgres so the control plane can read a merged,
  fleet-wide view — the first half of Phase-2 B1 (dynamic rebalancing).

  There is no BEAM cluster (the LB-partition model coordinates only through S3 for the
  data path and Postgres for orchestration), so a control plane can't reach into every
  node's `Fathom.ShardLoad` ETS table. Instead each node **reports**: every
  `:load_report_interval_ms` it reads `Fathom.ShardLoad.snapshot/0`, diffs against the
  previous snapshot into per-shard **rates** (the churn-safe method the `--hotspots`
  harness validated), and writes the top-N hottest shards to `shard_load_samples`,
  tagged with this node's `Fathom.Rebalancer.node_key/0` (its stable LB backend key,
  which is also the shard's current serving node).

  Off the hot path and resilient like `Fathom.Directory.Recorder`: a Postgres outage
  drops a window, never crashes the node. Gated by `:load_reporter` (default off);
  needs `:shard_load` on for the counters to be non-empty.

  Config: `:load_report_interval_ms` (10_000), `:load_report_top_n` (50 hottest shards
  published per window), `:load_sample_retention_ms` (600_000 — older rows pruned).
  """
  use GenServer

  require Logger

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{LoadSample, Nodes, Stats}
  alias Fathom.Repo
  alias Fathom.ShardLoad

  import Ecto.Query, only: [from: 2]

  @default_interval_ms 10_000
  @default_top_n 50
  @default_retention_ms 600_000

  @doc "Whether per-node load reporting is enabled (`:load_reporter`, default off)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :load_reporter, false) == true

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Publishes one window synchronously (tests)."
  @spec report_now() :: :ok
  def report_now, do: GenServer.call(__MODULE__, :report_now)

  @impl true
  def init(_opts) do
    # Take the baseline snapshot. NB (#18): the ControlPlane (this reporter) starts before
    # the DataPlane's ShardLoad table (see application.ex), so at boot this snapshot is
    # usually empty (`snapshot/0` rescues the missing table to []). The first published
    # window then reads each shard's cumulative counter as a rate — but on a fresh boot the
    # counters are ~0 (the shard just opened ~one interval ago), so the over-report is
    # negligible, and `confirm_windows` masks a single inflated window regardless.
    state = %{prev: snapshot_map(), prev_mono: mono_ms()}
    schedule(interval_ms())
    {:ok, state}
  end

  @impl true
  def handle_info(:report, state), do: {:noreply, do_report(state) |> tap_schedule()}

  @impl true
  def handle_call(:report_now, _from, state) do
    {:reply, :ok, do_report(state)}
  end

  defp tap_schedule(state) do
    schedule(interval_ms())
    state
  end

  # Diff current vs previous snapshot into rates, publish the top-N by query rate, prune
  # old rows. Returns the new state (current becomes previous). Best-effort like
  # `Directory.Recorder`: a Postgres blip drops this window without crashing the node —
  # both a `rescue` (DBConnection.ConnectionError / ownership raises) AND a `catch :exit`
  # (pool / :noproc / shutdown surface as exits that `rescue` misses — finding #13).
  #
  # The snapshot + clock are read ONCE outside the try and the baseline always advances to
  # them, so a dropped window doesn't re-scan ShardLoad / re-read the clock (a later baseline
  # would shorten the next window — #18 minor), and the failed window's deltas roll cleanly
  # into the next.
  defp do_report(%{prev: prev, prev_mono: prev_mono} = state) do
    now_mono = mono_ms()
    curr = snapshot_map()
    window_s = max((now_mono - prev_mono) / 1000, 0.001)

    try do
      # The FULL positive-rate distribution this window (all active shards, before top-N
      # truncation). Its p99 is the fleet hot bar (#2); the top-N of it is what's published.
      rows = positive_rate_rows(prev, curr, window_s)
      p99 = Stats.percentile(Enum.map(rows, & &1.q_per_s), 99)

      # Liveness beat for the dead-node reconciler (#1b) — rides this tick so a serving node
      # is a beating node — now carrying this node's full-distribution p99 + sample count for
      # the fleet-relative hot bar (#2).
      Nodes.beat(Rebalancer.node_key(), q_p99: p99, sample_count: length(rows))
      publish(Enum.take(rows, top_n()))
      prune()
    rescue
      e -> Logger.warning("load reporter window dropped: #{Exception.message(e)}")
    catch
      :exit, reason -> Logger.warning("load reporter window dropped (exit): #{inspect(reason)}")
    end

    %{state | prev: curr, prev_mono: now_mono}
  end

  # Per-shard rates from two cumulative snapshots — every active shard (q_per_s > 0), sorted
  # hottest first, as insert maps. NOT truncated: the caller takes the top-N to publish but
  # computes the fleet p99 over the whole set (#2).
  defp positive_rate_rows(prev, curr, window_s) do
    node_key = Rebalancer.node_key()
    sampled_at = DateTime.utc_now()

    curr
    |> Enum.map(fn {id, c} ->
      p = Map.get(prev, id, %{queries: 0, rows_read: 0, checkouts: 0})

      %{
        node_key: node_key,
        shard_id: id,
        q_per_s: rate(c.queries, p.queries, window_s),
        rows_read_per_s: rate(c.rows_read, p.rows_read, window_s),
        checkouts_per_s: rate(c.checkouts, p.checkouts, window_s),
        window_s: window_s,
        sampled_at: sampled_at,
        inserted_at: sampled_at
      }
    end)
    |> Enum.reject(&(&1.q_per_s <= 0.0))
    |> Enum.sort_by(& &1.q_per_s, :desc)
  end

  # A shard's ShardLoad counters are cumulative + monotonic, so curr < prev happens ONLY
  # when the coordinator stopped + reopened between snapshots (the row was dropped and
  # recreated at 0). This system LRU-evicts idle shards and cold-re-opens them, so that
  # churn is routine — clamping the reset delta to max(curr-prev,0)=0 dropped a moderately
  # hot churny shard from the window (and reset its confirm streak). On a reset the correct
  # window rate is `curr` over the window, not 0 (finding #6).
  defp rate(curr, prev, window_s) when curr < prev, do: curr / window_s
  defp rate(curr, prev, window_s), do: (curr - prev) / window_s

  defp publish([]), do: :ok
  defp publish(rows), do: Repo.insert_all(LoadSample, rows)

  defp prune do
    cutoff = DateTime.add(DateTime.utc_now(), -retention_ms(), :millisecond)
    Repo.delete_all(from s in LoadSample, where: s.sampled_at < ^cutoff)
    :ok
  end

  defp snapshot_map do
    ShardLoad.snapshot() |> Map.new(&{&1.shard_id, &1})
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)
  defp schedule(ms), do: Process.send_after(self(), :report, ms)

  defp interval_ms,
    do: Application.get_env(:fathom, :load_report_interval_ms, @default_interval_ms)

  defp top_n, do: Application.get_env(:fathom, :load_report_top_n, @default_top_n)

  defp retention_ms,
    do: Application.get_env(:fathom, :load_sample_retention_ms, @default_retention_ms)
end
