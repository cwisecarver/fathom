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
  alias Fathom.Rebalancer.{LoadSample, LoadSamples, Nodes, Stats, WarmLocations}
  alias Fathom.Repo
  alias Fathom.Shard.WarmFollower
  alias Fathom.ShardLoad

  import Ecto.Query, only: [from: 2]

  @default_interval_ms 10_000
  @default_top_n 50
  @default_retention_ms 600_000
  # Shards hot within this window are handoff candidates; advertise warmth for them (#C).
  @warm_hot_window_ms 120_000

  @doc "Whether per-node load reporting is enabled (`:load_reporter`, default off)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :load_reporter, false) == true

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # report_now runs the full window (beat + publish + prune + warm-location publish) inline;
  # a large recent-hot set plus several Postgres round-trips can exceed the default 5s call
  # timeout and crash the caller (finding #12). It's test-only, so a generous explicit timeout
  # is enough (the timer path just blocks its own process).
  @report_now_timeout_ms 60_000

  @doc "Publishes one window synchronously (tests)."
  @spec report_now() :: :ok
  def report_now, do: GenServer.call(__MODULE__, :report_now, @report_now_timeout_ms)

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
      # truncation). The top-N of it is what's published; the whole distribution feeds the
      # fleet hot bar (#2/#4).
      rows = positive_rate_rows(prev, curr, window_s)
      qps = Enum.map(rows, & &1.q_per_s)

      # Liveness beat for the dead-node reconciler (#1b) — rides this tick so a serving node is
      # a beating node — carrying this node's full-distribution p99 + sample count (observability)
      # and a q/s histogram the orchestrator merges into the TRUE pooled fleet p99 (#4).
      # `replication_address` rides the same tick (A2 membership, 2026-08-10): this is already the
      # once-per-node fleet write, and a second writer racing the same row would only invent ways
      # for the two to disagree. nil when this node does not listen — see `advertised_address/0`.
      Nodes.beat(Rebalancer.node_key(),
        q_p99: Stats.percentile(qps, 99),
        sample_count: length(rows),
        q_hist: Stats.histogram(qps),
        replication_address: Fathom.Shard.Replication.Fleet.advertised_address()
      )

      publish(Enum.take(rows, top_n()))
      prune()
      # Advertise which fleet-hot shards THIS node has warm-cached (affinity-aware target, #C):
      # the intersection of the recent fleet-hot set and this node's warm cache. Bounded to hot
      # shards; a node not running the follower simply advertises none (cached? is false).
      publish_warm_locations()
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

    # Single reduce building only the positive-rate rows (review 2026-07-23 #29): the old
    # map-then-reject built an insert map per resident shard — including the idle majority
    # about to be rejected — per window. The hottest-first sort stays (publish/1 takes the
    # top-N of it, and it costs far less than the per-shard map churn did).
    curr
    |> Enum.reduce([], fn {id, {co, q, rr}}, acc ->
      {p_co, p_q, p_rr} = Map.get(prev, id, {0, 0, 0})
      q_per_s = rate(q, p_q, window_s)

      if q_per_s <= 0.0 do
        acc
      else
        [
          %{
            node_key: node_key,
            shard_id: id,
            q_per_s: q_per_s,
            rows_read_per_s: rate(rr, p_rr, window_s),
            checkouts_per_s: rate(co, p_co, window_s),
            window_s: window_s,
            sampled_at: sampled_at,
            inserted_at: sampled_at
          }
          | acc
        ]
      end
    end)
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

  # Prune THIS node's own rows every window, and sweep the fleet only occasionally (expert review
  # 2026-07-24 #27). The old shape deleted the whole expired set from every node every window, so a
  # 20-node fleet issued 20 deletes of the same rows per window: one useful, nineteen acquiring row
  # locks, blocking, re-checking and deleting zero — after each had done the full index range scan.
  # The waste grew linearly with node count, which is the opposite of the horizontal additivity the
  # density work established.
  #
  # Node-scoped deletes are disjoint per node, so there is no cross-node lock contention at all.
  # The rare fleet-wide sweep still reclaims a DEPARTED node's rows — scoping alone would strand
  # those forever — and uses a much older cutoff so it can never race a live node's fresh rows.
  #
  # Safe by nature of the data: `shard_load_samples` is observational (a rolling window the
  # rebalancer reads with its own anti-flap and confirm windows), so which node deletes an expired
  # row is unobservable. No lifecycle state, tombstone, suspension, or last_flushed_at record is
  # involved.
  @fleet_sweep_odds 30
  @fleet_sweep_age_multiplier 3

  defp prune do
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, -retention_ms(), :millisecond)
    key = Rebalancer.node_key()

    Repo.delete_all(from s in LoadSample, where: s.node_key == ^key and s.sampled_at < ^cutoff)

    if sweep_now?() do
      stale = DateTime.add(now, -(retention_ms() * @fleet_sweep_age_multiplier), :millisecond)
      Repo.delete_all(from s in LoadSample, where: s.sampled_at < ^stale)
    end

    :ok
  end

  # See WarmLocations.prune/1 — same seam, so both sweeps are forceable in a test.
  defp sweep_now? do
    case Application.get_env(:fathom, :rebalancer_fleet_sweep_odds, @fleet_sweep_odds) do
      1 -> true
      n when is_integer(n) and n > 1 -> :rand.uniform(n) == 1
      _ -> false
    end
  end

  # Advertise the fleet-hot shards this node has warm-cached (affinity-aware target, #C). The
  # fleet-hot set is the recent samples' distinct shards; intersect with this node's warm cache
  # (`WarmFollower.cached?/1` — a node not running the follower matches none). Prune dead-node
  # leftovers past the same window the reader trusts.
  defp publish_warm_locations do
    # One directory read for this node's whole warm set, then in-memory membership — not one
    # File.exists? per recent-hot shard inline in the GenServer (finding #12). A node not
    # running the follower has an empty set and advertises none.
    warm_set = MapSet.new(WarmFollower.cached_shard_ids())

    warm_hot =
      @warm_hot_window_ms
      |> LoadSamples.recent_shard_ids()
      |> Enum.filter(&MapSet.member?(warm_set, &1))

    WarmLocations.publish(Rebalancer.node_key(), warm_hot)
    WarmLocations.prune(retention_ms())
    :ok
  end

  # Raw-tuple snapshot: `%{id => {checkouts, queries, rows_read}}` — the retained `prev`
  # and each window's `curr` used to be maps of 5-key atom maps built from an intermediate
  # list of maps, i.e. two full materializations per window plus a comparable retained
  # copy (tens of MB transient at 100k resident shards; review 2026-07-23 #29).
  # rows_written isn't reported, so it's dropped at ingestion.
  defp snapshot_map do
    ShardLoad.snapshot_tuples()
    |> Map.new(fn {id, checkouts, queries, rows_read, _rows_written} ->
      {id, {checkouts, queries, rows_read}}
    end)
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)
  defp schedule(ms), do: Process.send_after(self(), :report, ms)

  defp interval_ms,
    do: Application.get_env(:fathom, :load_report_interval_ms, @default_interval_ms)

  defp top_n, do: Application.get_env(:fathom, :load_report_top_n, @default_top_n)

  defp retention_ms,
    do: Application.get_env(:fathom, :load_sample_retention_ms, @default_retention_ms)
end
