defmodule Fathom.Scale do
  @moduledoc """
  Scale test: provision N shards of a realistic size (~S MB each) and measure the
  two numbers fathom's "millions of small shards" thesis lives or dies on:

    * **Cold-open latency** at real size — `Fathom.Shards.checkout/1` + open + first
      query on a freshly-pulled S-MB shard (vs the empty-shard bench, which only
      measures coordinator startup).
    * **Fan-out node density** — open all N shards holding one live
      `Fathom.Shard.Connection` each (real page-cache + coordinator cost), and
      report BEAM and **RSS** memory per held shard, plus open throughput. The
      "how many S-MB shards can one node hold open at once" answer.

  Driven by `mix fathom.scale`. Shards are provisioned with incompressible
  `randomblob` data so the files are genuinely S MB on disk, and live in a
  dedicated scratch dir (`fathom_scale/`) so they never touch real data or the
  bench scratch. The harness starts the minimal subsystem itself (ShardRegistry +
  ShardSupervisor; no Repo/Oban/endpoint) and disables the durability flush /
  directory touch / lazy migrate on the hot path.

  Holding N live connections costs ~3 file descriptors each (db + `-wal` + `-shm`),
  so a 1000-shard run needs ~3000 fds — raise `ulimit -n` before running. The
  fan-out stops gracefully at the fd ceiling and reports how many it reached.
  """
  require Logger

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Storage
  alias Fathom.Shard.WarmFollower
  alias Fathom.ShardExecutor
  alias Fathom.Shards
  alias Filo.Stmt

  @blob_bytes 65_536

  @doc """
  Runs the sweep and returns a result map. Opts: `:shards` (1000),
  `:shard_size_mb` (4), `:cold_open_samples` (100).
  """
  @spec run(keyword()) :: map()
  def run(opts \\ []) do
    n = Keyword.get(opts, :shards, 1000)
    size_mb = Keyword.get(opts, :shard_size_mb, 4)
    samples = min(Keyword.get(opts, :cold_open_samples, 100), n)
    setup()

    Logger.warning("provisioning #{n} shards @ ~#{size_mb} MB ...")
    {provision_us, sizes} = :timer.tc(fn -> provision(n, size_mb) end)
    actual_mb = Enum.sum(sizes) / max(length(sizes), 1) / 1_048_576

    Logger.warning("cold-open sweep over #{samples} shards ...")
    cold = cold_open_sweep(samples)

    Logger.warning("fan-out: opening #{n} shards, holding a connection each ...")
    fan = fanout(n)

    %{
      shards: n,
      shard_size_mb_target: size_mb,
      shard_size_mb_actual: round1(actual_mb),
      disk_mb: round1(Enum.sum(sizes) / 1_048_576),
      provision_s: round1(provision_us / 1_000_000),
      cold_open_p50_us: cold.p50,
      cold_open_p99_us: cold.p99,
      fanout_opened: fan.opened,
      fanout_beam_kb_per_shard: fan.beam_per,
      fanout_rss_kb_per_shard: fan.rss_per,
      fanout_total_rss_mb: fan.total_rss_mb,
      fanout_open_per_s: fan.open_per_s
    }
  end

  # --- provisioning --------------------------------------------------------

  # Seed each shard's storage file to ~size_mb of incompressible blob data, so the
  # on-disk file is genuinely that big (randomblob defeats any compression/sparse).
  defp provision(n, size_mb) do
    rows = max(div(size_mb * 1_048_576, @blob_bytes), 1)

    Enum.map(1..n, fn i ->
      if rem(i, 200) == 0, do: Logger.warning("  provisioned #{i}/#{n}")
      path = storage_path("scale_#{i}")
      drop_db(path)
      {:ok, conn} = Connection.open(path)
      Connection.exec(conn, "CREATE TABLE blob (id INTEGER PRIMARY KEY, data BLOB)")
      Connection.exec(conn, "BEGIN")

      Connection.exec(conn, """
      WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < #{rows})
      INSERT INTO blob (data) SELECT randomblob(#{@blob_bytes}) FROM c
      """)

      Connection.exec(conn, "COMMIT")
      Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(conn)
      File.stat!(path).size
    end)
  end

  # --- cold open at real size ---------------------------------------------

  defp cold_open_sweep(k) do
    # Distinct provisioned shards, each pulled cold once (no reuse → no drain
    # needed). Leaves the coordinator up + the file local; fan-out reuses them.
    us =
      Enum.map(1..k, fn i ->
        id = "scale_#{i}"

        {t, {pid, ref, conn}} =
          :timer.tc(fn ->
            {:ok, pid, ref, path} = Shards.checkout(id)
            {:ok, conn} = Connection.open(path)
            {:ok, _} = Connection.query(conn, "SELECT count(*) FROM blob", [])
            {pid, ref, conn}
          end)

        # Release the timed connection; the coordinator stays warm for fan-out.
        Connection.close(conn)
        Fathom.Shard.checkin(pid, ref)
        t
      end)

    %{p50: percentile(us, 50), p99: percentile(us, 99)}
  end

  # --- fan-out node density ------------------------------------------------

  defp fanout(n) do
    :erlang.garbage_collect()
    beam_before = :erlang.memory(:total)
    rss_before = rss_kb()

    {open_us, handles} =
      :timer.tc(fn ->
        Enum.reduce_while(1..n, [], fn i, acc ->
          case open_held("scale_#{i}") do
            {:ok, h} ->
              {:cont, [h | acc]}

            {:error, reason} ->
              Logger.warning("fan-out stopped at shard #{i}: #{inspect(reason)} (fd ceiling?)")
              {:halt, acc}
          end
        end)
      end)

    :erlang.garbage_collect()
    beam_after = :erlang.memory(:total)
    rss_after = rss_kb()
    opened = length(handles)

    # Release connections (free fds); coordinators die with the beam at task exit.
    Enum.each(handles, fn {pid, ref, conn} ->
      Connection.close(conn)
      Fathom.Shard.checkin(pid, ref)
    end)

    %{
      opened: opened,
      beam_per: safe_div(beam_after - beam_before, opened) |> kb(),
      rss_per: safe_div(rss_after - rss_before, opened) |> round(),
      total_rss_mb: round(rss_after / 1024),
      open_per_s: round(opened / max(open_us / 1_000_000, 0.001))
    }
  end

  defp open_held(id) do
    with {:ok, pid, ref, path} <- Shards.checkout(id),
         {:ok, conn} <- Connection.open(path),
         {:ok, _} <- Connection.query(conn, "SELECT count(*) FROM blob", []) do
      {:ok, {pid, ref, conn}}
    end
  rescue
    e -> {:error, e}
  end

  # --- lease-renewal RPS (the S3 PUT storm) --------------------------------

  @doc """
  Measures that the **F1 lease-renewal storm is gone** — the node-heartbeat fix.

  The old model renewed *every shard's* lease every `ttl/3`, an `active_shards /
  (ttl/3)` PUT storm per node (millions of shards ⇒ ~100k PUT/s/node). The fix
  (`Fathom.Shard.Heartbeat`) makes liveness one per-node object, so a coordinator
  renews *nothing* per shard.

  This starts the node heartbeat (so coordinators open in heartbeat mode), then N
  coordinators **cheaply** — checkout + immediate checkin, no held connection/fd;
  `:shard_idle_ms` is raised so they persist — and over a steady-state window counts
  both `[:fathom, :shard, :lease, :renewed]` (the per-shard renewals — now ~0) and
  `[:fathom, :shard, :heartbeat, :renewed]` (the one node heartbeat — constant
  regardless of N). It reports both, plus the contrast: what the old per-shard model
  *would* cost at this N and at a million shards vs the flat heartbeat rate (~0.1
  PUT/s/node at the prod 10s cadence).

  Opts: `:shards` (5000), `:lease_ttl_ms` (900 — renew every 300ms so the window
  is short), `:window_ms` (3000).
  """
  @spec lease_rps(keyword()) :: map()
  def lease_rps(opts \\ []) do
    n = Keyword.get(opts, :shards, 5000)
    ttl_ms = Keyword.get(opts, :lease_ttl_ms, 900)
    window_ms = Keyword.get(opts, :window_ms, 3000)
    renew_ms = max(div(ttl_ms, 3), 1)

    setup()
    # The scale task loads config without starting apps; the telemetry the
    # coordinators + heartbeat emit (and the handlers below) needs :telemetry running.
    {:ok, _} = Application.ensure_all_started(:telemetry)
    Application.put_env(:fathom, :shard_lease_ttl_ms, ttl_ms)
    # Keep coordinators alive through the run without holding a connection each, so N
    # isn't fd-bound — this measures the renewal rate, not fan-out density.
    Application.put_env(:fathom, :shard_idle_ms, max(window_ms * 20, 600_000))

    # Start the node heartbeat so coordinators open in HEARTBEAT mode — the whole
    # point of the F1 fix: per-shard renewal (the storm) is replaced by one heartbeat.
    {:ok, hb} = start_heartbeat(ttl_ms)

    per_shard = :counters.new(1, [:write_concurrency])
    node_hb = :counters.new(1, [:write_concurrency])
    ps_handler = {__MODULE__, :lease_rps_per_shard}
    hb_handler = {__MODULE__, :lease_rps_heartbeat}

    :telemetry.attach(
      ps_handler,
      [:fathom, :shard, :lease, :renewed],
      fn _e, _m, _meta, _cfg -> :counters.add(per_shard, 1, 1) end,
      nil
    )

    :telemetry.attach(
      hb_handler,
      [:fathom, :shard, :heartbeat, :renewed],
      fn _e, _m, _meta, _cfg -> :counters.add(node_hb, 1, 1) end,
      nil
    )

    Logger.warning(
      "lease-rps (heartbeat mode): starting #{n} coordinators (ttl #{ttl_ms}ms, renew #{renew_ms}ms) ..."
    )

    {start_us, started} = :timer.tc(fn -> start_coordinators(n) end)

    # Let the cadence reach steady state, then count over a fixed wall-clock window.
    # Process.sleep is the measurement window, not test synchronization.
    Process.sleep(renew_ms * 2)
    ps_before = :counters.get(per_shard, 1)
    hb_before = :counters.get(node_hb, 1)
    Process.sleep(window_ms)
    ps_observed = :counters.get(per_shard, 1) - ps_before
    hb_observed = :counters.get(node_hb, 1) - hb_before

    :telemetry.detach(ps_handler)
    :telemetry.detach(hb_handler)
    # Stop coordinators + heartbeat before the caller's cleanup rm_rf's the scratch
    # dir, so nothing races the delete.
    stop_all_coordinators()
    stop_heartbeat(hb)

    per_shard_rps = ps_observed / (window_ms / 1000)
    node_hb_rps = hb_observed / (window_ms / 1000)
    # Prod default lease TTL is 30s, so the cadence is every 10s (ttl/3).
    prod_renew_s = 10

    %{
      mode: "lease_rps",
      shards_requested: n,
      coordinators_started: started,
      start_rate_per_s: round(safe_div(started, max(start_us / 1_000_000, 0.001))),
      lease_ttl_ms: ttl_ms,
      renew_interval_ms: renew_ms,
      window_ms: window_ms,
      # The fix, measured: per-shard renewals are gone; one node heartbeat covers all
      # N shards (so its rate is independent of N).
      per_shard_renewals_observed: ps_observed,
      per_shard_renew_rps: round1(per_shard_rps),
      node_heartbeat_renewals_observed: hb_observed,
      node_heartbeat_rps: round1(node_hb_rps),
      prod_renew_interval_s: prod_renew_s,
      # The old per-shard model cost N/(ttl/3) PUT/s/node; the heartbeat is 1/(ttl/3),
      # independent of N — so at a million shards, ~100k PUT/s/node collapses to ~0.1.
      legacy_projected_rps_per_node: round1(safe_div(started, prod_renew_s)),
      legacy_projected_rps_per_node_at_1m: round(1_000_000 / prod_renew_s),
      heartbeat_rps_per_node_prod: round2(1 / prod_renew_s)
    }
  end

  defp start_heartbeat(ttl_ms) do
    case Fathom.Shard.Heartbeat.start_link(ttl_ms: ttl_ms) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  defp stop_heartbeat(_pid) do
    if Process.whereis(Fathom.Shard.Heartbeat),
      do: GenServer.stop(Fathom.Shard.Heartbeat, :normal, 5_000)

    :ok
  end

  # Start N coordinators without holding a connection: checkout starts the
  # coordinator + acquires the lease + arms the renew timer, checkin releases the
  # connection so no fd is held. Coordinators persist (idle raised) and renew.
  # Concurrent so the per-coordinator open I/O parallelizes (sequential is far too
  # slow at the N this measurement wants); returns how many started.
  defp start_coordinators(n) do
    1..n
    |> Task.async_stream(
      fn i ->
        case Shards.checkout("lease_#{i}") do
          {:ok, pid, ref, _path} ->
            Fathom.Shard.checkin(pid, ref)
            :ok

          {:error, _reason} ->
            :error
        end
      end,
      max_concurrency: System.schedulers_online() * 4,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.count(&match?({:ok, :ok}, &1))
  end

  # Terminate every coordinator the run started. Concurrent so it doesn't itself
  # become the bottleneck at high N.
  defp stop_all_coordinators do
    Fathom.ShardSupervisor
    |> DynamicSupervisor.which_children()
    |> Task.async_stream(
      fn
        {_, pid, _, _} when is_pid(pid) ->
          DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, pid)

        _ ->
          :ok
      end,
      max_concurrency: System.schedulers_online() * 4,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  # --- warm-standby density ------------------------------------------------

  @doc """
  Warm-standby density: how cheaply a standby holds shards **warm** (pre-pulled into
  the follower cache) vs **open** (a live coordinator — `fanout/1`, ~196 KiB BEAM +
  ~3 fds each).

  A warm-cached shard is just its file on disk plus one id in the follower's set — no
  coordinator, no `Fathom.Shard.Connection`, no file descriptors. So a standby's warm
  capacity is **disk-bound** (`disk_budget / shard_size`), far above its open-shard
  ceiling: the follower's per-shard *process* overhead is ~0.

  Provisions N `:shard_size_mb` shards to storage, pre-pulls them ALL into the warm
  cache (the `Storage.pull_if_changed/3` + etag-sidecar path the real follower runs),
  and reports per-cached-shard disk, the follower's per-shard BEAM bookkeeping (the
  cached-id set — the only thing it retains in memory), warming throughput, and the
  open-vs-warm contrast.

  Opts: `:shards` (1000), `:shard_size_mb` (1).
  """
  @spec warm_density(keyword()) :: map()
  def warm_density(opts \\ []) do
    n = Keyword.get(opts, :shards, 1000)
    size_mb = Keyword.get(opts, :shard_size_mb, 1)
    setup()

    cache = Path.join(scratch(), "warm_cache")
    File.rm_rf!(cache)
    File.mkdir_p!(cache)
    Application.put_env(:fathom, :warm_cache_dir, cache)

    Logger.warning("provisioning #{n} shards @ ~#{size_mb} MB to storage ...")
    {_us, sizes} = :timer.tc(fn -> provision(n, size_mb) end)
    ids = Enum.map(1..n, &"scale_#{&1}")

    :erlang.garbage_collect()
    beam_before = :erlang.memory(:total)
    rss_before = rss_kb()

    Logger.warning("warming #{n} shards into the follower cache ...")
    {warm_us, cached} = :timer.tc(fn -> warm_pull_all(ids) end)

    # Retain the follower's per-shard bookkeeping (its cached-id set) so the BEAM
    # delta reflects what the WarmFollower actually holds in memory per cached shard —
    # the cache bytes themselves live on disk, not the heap.
    cached_set = MapSet.new(cached)
    :erlang.garbage_collect()
    beam_after = :erlang.memory(:total)
    rss_after = rss_kb()
    held = max(MapSet.size(cached_set), 1)

    cache_disk_kb = dir_size_kb(cache)

    %{
      mode: "warm_density",
      shards_requested: n,
      cached: MapSet.size(cached_set),
      shard_size_mb_actual: round1(Enum.sum(sizes) / max(length(sizes), 1) / 1_048_576),
      warm_cache_disk_mb: round1(cache_disk_kb / 1024),
      warm_disk_kb_per_shard: round(safe_div(cache_disk_kb, held)),
      # The follower's per-shard heap cost (its cached-id set) — the point is it's ~0
      # next to an open coordinator's ~196 KiB.
      warm_beam_kb_per_shard: kb(safe_div(beam_after - beam_before, held)),
      # RSS delta includes transient page cache from writing the files, so it's noisy —
      # reported for context, not as the density limit (disk is).
      warm_rss_kb_per_shard: round(safe_div(rss_after - rss_before, held)),
      warm_pull_per_s: round(safe_div(held, max(warm_us / 1_000_000, 0.001))),
      # An OPEN shard (live coordinator + held connection) costs ~196 KiB BEAM + ~3
      # fds; a WARM shard costs ~0 process/BEAM/fd — only disk. So a standby holds warm
      # shards until it runs out of DISK, orders of magnitude past its open ceiling.
      open_shard_beam_kb_ref: 196,
      open_shard_fds_ref: 3
    }
  end

  # Pre-pull each shard's current object into the warm cache + record its etag, exactly
  # as Fathom.Shard.WarmFollower does. Concurrent so warming parallelizes.
  defp warm_pull_all(ids) do
    ids
    |> Task.async_stream(&warm_pull_one/1,
      max_concurrency: System.schedulers_online() * 4,
      timeout: 120_000,
      ordered: false
    )
    |> Enum.reduce([], fn
      {:ok, {:ok, id}}, acc -> [id | acc]
      _other, acc -> acc
    end)
  end

  defp warm_pull_one(id) do
    path = WarmFollower.cache_path(id)

    case Storage.pull_if_changed(id, path, nil) do
      {:ok, {:written, etag}} ->
        File.write(path <> ".etag", etag)
        {:ok, id}

      _ ->
        :skip
    end
  rescue
    _ -> :skip
  catch
    :exit, _ -> :skip
  end

  defp dir_size_kb(dir) do
    case :os.cmd(String.to_charlist("du -sk #{dir}")) |> List.to_string() |> String.split() do
      [kb | _] ->
        case Integer.parse(kb) do
          {n, _} -> n
          :error -> 0
        end

      _ ->
        0
    end
  end

  # --- hot-spot evidence (Phase-2 §B rebalancing prerequisite) -------------

  @doc """
  Hot-spot evidence for Phase-2 dynamic rebalancing (`docs/phase2-scoping.md` §B).

  §B gates the rebalancer on **real** hot-spot data: turn on `:shard_load` and read
  `Fathom.ShardLoad.top/2`. Nothing reads those counters yet — this is the first
  reader. It drives a **Zipf-skewed** load across N shards **through the real recording
  path** (`Fathom.Shards.checkout` → `Fathom.ShardExecutor.execute`, so
  `record_checkout`/`record_query` fire exactly as a Hrana stream would), then reads
  `Fathom.ShardLoad` the way a rebalancer would: **diff two snapshots over a window**
  into per-shard *rates* (churn-safe — a shard that stopped between snapshots just
  drops out).

  **Persistent-stream model (`:stream_len`).** Each unit of work is a Hrana *stream*:
  checkout the target shard + open a connection **once** at stream start, run a burst of
  `:stream_len` queries on the held connection (`record_query` per query), then close +
  checkin at stream end — the real model (AGENTS.md: one connection per Hrana stream,
  opened at stream start, closed at stream end). This amortizes the coordinator
  checkout/checkin over the whole burst, so throughput is not capped by the per-query
  coordinator round-trip. Streams draw their target shard from the Zipf distribution, so
  hot shards get more concurrent streams *and* more queries — a hot shard held open by
  many streams at once is exactly how it looks in production.

  The default `:stream_len` is **1** — every query is its own one-shot stream. That is the
  per-query lower bound, and it maximizes **detection sampling** (the unit of sampling is
  the stream, so 1-query streams give the finest per-shard rate resolution for the
  threshold/anti-flap finding). Raise `:stream_len` to measure **realistic throughput**
  (persistent streams lift q/s several-fold), but then scale `:queries` so
  `queries / stream_len` (the stream count) stays well above the shard count — otherwise
  the tail is under-sampled and the top-set rates get noisy.

  It answers the two questions B needs before anyone builds the LB override table:

    * **Is the hot set detectable?** The query-rate distribution across shards
      (p50/p90/p99/max), the skew ratio (max/median), and whether the shipped read
      API `ShardLoad.top(20)` recovers the Zipf head (recall).
    * **What threshold, and does it flap?** A `> K x median` rule swept at K = 5/10/20
      (how many shards each flags, and does it catch the true hot set), plus **two
      windows** to measure how stable the flagged set is (Jaccard) — the raw input for
      the anti-flap policy.

  The threshold value and anti-flap policy are an operator call; this produces the
  numbers to make it. The staging real-traffic run (non-synthetic skew) is the
  follow-on this unblocks.

  Opts: `:shards` (1000), `:queries` (per window, 50_000), `:zipf` (exponent s, 1.1),
  `:workers` (schedulers * 4, the concurrent-stream count), `:stream_len` (1 — queries per
  stream before it rotates to a new Zipf-drawn shard; raise it to measure persistent-stream
  throughput). At most `:workers` connections are open at once, so `ulimit -n` bounds
  `:workers`, not the shard count.
  """
  @spec hotspots(keyword()) :: map()
  def hotspots(opts \\ []) do
    n = Keyword.get(opts, :shards, 1000)
    per_window = Keyword.get(opts, :queries, 50_000)
    s = Keyword.get(opts, :zipf, 1.1) / 1.0
    workers = Keyword.get(opts, :workers, System.schedulers_online() * 4)
    stream_len = max(Keyword.get(opts, :stream_len, 1), 1)

    setup()
    # Turn the counters on (record_* no-op otherwise) and keep coordinators from
    # evicting/idle-stopping mid-drive, so the rate signal reflects load, not churn.
    ensure_started(Fathom.ShardLoad, [])
    Application.put_env(:fathom, :shard_load, true)
    Application.put_env(:fathom, :max_open_shards, :infinity)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Fathom.ShardLoad.reset()

    Logger.warning("provisioning #{n} tiny shards ...")
    provision_tiny(n)

    cdf = zipf_cdf(n, s)

    Logger.warning(
      "window A: driving ~#{per_window} Zipf(s=#{s}) queries over #{n} shards " <>
        "(#{workers} streams x #{stream_len} queries) ..."
    )

    {rates_a, wa, exec_a} = drive_window(per_window, cdf, workers, stream_len)
    Logger.warning("window B: repeating the drive for anti-flap stability ...")
    {rates_b, wb, _exec_b} = drive_window(per_window, cdf, workers, stream_len)

    result = report(n, s, stream_len, exec_a, rates_a, wa, rates_b, wb)
    # Stop the coordinators (idle was raised so they persist through the run) before the
    # caller's cleanup rm_rf's the store, so a teardown flush never races the delete or
    # self-fences on a vanished lease object.
    stop_all_coordinators()
    result
  end

  # Seed N tiny shards (a 10-row table) so `SELECT id FROM t LIMIT 8` reads a handful
  # of rows — enough to populate the rows_read cost dimension without shard size
  # mattering (hot-spot detection is about rate, not bytes).
  defp provision_tiny(n) do
    Enum.each(1..n, fn i ->
      if rem(i, 500) == 0, do: Logger.warning("  provisioned #{i}/#{n}")
      path = storage_path("hot_#{i}")
      drop_db(path)
      {:ok, conn} = Connection.open(path)
      Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      Connection.exec(conn, "INSERT INTO t (id) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
      Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(conn)
    end)
  end

  # One measurement window: snapshot, drive ~`q` Zipf queries concurrently, snapshot,
  # diff into per-shard query/read rates over the wall-clock window. This is exactly how
  # a rebalancer reads ShardLoad — cumulative counters differenced over a window. Returns
  # the rates, the window seconds, and the actual query count executed (streams round `q`
  # down to a whole number of `stream_len` bursts).
  defp drive_window(q, cdf, workers, stream_len) do
    before = index_by_id(Fathom.ShardLoad.snapshot())
    {us, executed} = :timer.tc(fn -> drive(q, cdf, workers, stream_len) end)
    secs = max(us / 1_000_000, 0.001)
    now = index_by_id(Fathom.ShardLoad.snapshot())

    rates =
      now
      |> Enum.map(fn {id, a} ->
        b = Map.get(before, id, %{queries: 0, rows_read: 0})

        %{
          shard_id: id,
          q_per_s: (a.queries - b.queries) / secs,
          rows_read_per_s: (a.rows_read - b.rows_read) / secs
        }
      end)
      |> Enum.reject(&(&1.q_per_s <= 0))

    {rates, secs, executed}
  end

  defp index_by_id(snapshot), do: Map.new(snapshot, &{&1.shard_id, &1})

  # Drive ~`q` queries as `q / stream_len` Hrana streams, `workers` running concurrently.
  # Each stream bursts `stream_len` queries on one held connection, so the coordinator
  # checkout/checkin is paid once per burst, not per query — the persistent-stream model.
  defp drive(q, cdf, workers, stream_len) do
    stmt = %Stmt{sql: "SELECT id FROM t LIMIT 8", args: []}
    streams = max(div(q, stream_len), 1)

    1..streams
    |> Task.async_stream(fn _ -> stream_burst("hot_#{zipf_sample(cdf)}", stmt, stream_len) end,
      max_concurrency: workers,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()

    streams * stream_len
  end

  # One Hrana stream, the real lifecycle: checkout (records the checkout signal,
  # starts/reuses the coordinator) + open a connection **once** at stream start, run
  # `len` queries on the held connection (each `execute` records the query-cost signal),
  # then close + checkin at stream end. A failed checkout/open just drops that stream —
  # the rate signal survives.
  defp stream_burst(id, stmt, len) do
    with {:ok, pid, ref, path} <- Shards.checkout(id),
         {:ok, conn} <- Connection.open(path) do
      # Full-access handle (the benchmark isn't auth-gated); the 5th element is the token scope
      # (#24), the 6th the per-stream opts resolved at open (template capture / DDL guard are
      # both off for the harness's synthetic shards).
      # nil token version: the scale harness has no auth context, so no revocation re-check.
      handle = {pid, ref, conn, id, :rw, nil, %{template?: false, block_ddl?: false}}
      Enum.each(1..len, fn _ -> ShardExecutor.execute(handle, stmt) end)
      Connection.close(conn)
      Fathom.Shard.checkin(pid, ref)
    end

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  # Zipf(s) over ranks 1..n: P(k) ∝ 1/k^s. Precompute the ascending cumulative
  # distribution once (a tuple) so each draw is an O(log n) binary search.
  defp zipf_cdf(n, s) do
    weights = Enum.map(1..n, fn k -> 1.0 / :math.pow(k, s) end)
    total = Enum.sum(weights)

    weights
    |> Enum.scan(0.0, fn w, acc -> acc + w / total end)
    |> List.to_tuple()
  end

  # Draw a rank 1..n from the cumulative tuple. Rank 1 is the hottest shard, so the
  # true hot order is hot_1, hot_2, ... — `zipf_recall/1` scores ShardLoad's top-N
  # against it.
  defp zipf_sample(cdf) do
    zipf_bsearch(cdf, :rand.uniform(), 0, tuple_size(cdf) - 1) + 1
  end

  defp zipf_bsearch(cdf, u, lo, hi) when lo < hi do
    mid = div(lo + hi, 2)

    if elem(cdf, mid) >= u,
      do: zipf_bsearch(cdf, u, lo, mid),
      else: zipf_bsearch(cdf, u, mid + 1, hi)
  end

  defp zipf_bsearch(_cdf, _u, lo, _hi), do: lo

  # --- hot-spot reporting --------------------------------------------------

  defp report(n, s, stream_len, executed, rates_a, wa, rates_b, wb) do
    qs_a = Enum.map(rates_a, & &1.q_per_s)
    median = percentile(qs_a, 50)
    p99 = percentile(qs_a, 99)
    max_q = Enum.max([0.0 | qs_a])
    # max/median is inflated when a long cold tail drags the median toward 0; max/p99 is
    # the tail-robust separation a fleet-scale threshold should key on.
    sep_p99 = safe_div(max_q, p99)

    # Three threshold families: >Kx median (over-flags on a Zipf tail — see
    # median_collapsed), >Kx p99 (tail-robust), and an absolute q/s floor set to isolate
    # the top-N (the portable production knob — a fixed rate, not a distribution ratio).
    thresholds = ratio_sweep(rates_a, [5, 10, 20], median)
    thresholds_p99 = ratio_sweep(rates_a, [5, 10, 20], p99)
    thresholds_absolute = absolute_sweep(rates_a, [5, 10, 20, 50])

    # Median-relative is the wrong threshold SHAPE when >10x-median flags many more shards
    # than the tail-robust >10x-p99 rule — i.e. a K-multiple of the median still sits in the
    # warm/cold bulk (a long Zipf tail or a near-idle fleet), not on the hot head. Compares
    # the two sweeps (ground-truth-free), so it holds at any N or absolute throughput — an
    # absolute `median < 1 q/s` test would flip just because a faster drive lifts all rates.
    median_10x = flagged_at(thresholds, 10)
    p99_10x = flagged_at(thresholds_p99, 10)
    median_collapsed = median_10x > max(5, 3 * p99_10x)

    # Anti-flap on a scale-robust, policy-neutral set: are the top-20 hottest shards the
    # same across two windows? (A threshold-anchored flap set is empty at small N and
    # huge at large N — top-K is meaningful at both.)
    flap = jaccard(MapSet.new(top_ids(rates_a, 20)), MapSet.new(top_ids(rates_b, 20)))

    # Exercise the shipped read API a rebalancer would call, not just my diff.
    shardload_top = Fathom.ShardLoad.top(20, :queries) |> Enum.map(& &1.shard_id)
    top20_recall = zipf_recall(top_ids(rates_a, 20))

    %{
      mode: "hotspots",
      shards: n,
      zipf_s: round2(s),
      stream_len: stream_len,
      queries_per_window: executed,
      window_a_s: round1(wa),
      window_b_s: round1(wb),
      queries_per_s: round(safe_div(executed, wa)),
      active_shards: length(rates_a),
      rate_p50_qps: round1(median),
      rate_p90_qps: round1(percentile(qs_a, 90)),
      rate_p99_qps: round1(p99),
      rate_max_qps: round1(max_q),
      # Separation, two ways: max/median (inflated by a near-0 median) vs max/p99 (robust).
      skew_ratio: round1(safe_div(max_q, median)),
      separation_over_p99: round1(sep_p99),
      median_collapsed: median_collapsed,
      top10: top10_detail(rates_a),
      # >Kx-median (deprecated at scale), >Kx-p99, and absolute top-N-floor sweeps.
      thresholds: thresholds,
      thresholds_p99: thresholds_p99,
      thresholds_absolute: thresholds_absolute,
      # Anti-flap: top-20-by-rate set overlap across the two windows.
      flap_top20_jaccard: round2(flap),
      # Does the diff-based top and the cumulative ShardLoad.top both recover the head?
      top20_zipf_recall: round2(top20_recall),
      shardload_top20_zipf_recall: round2(zipf_recall(shardload_top)),
      verdict: verdict(top20_recall, flap, median_collapsed)
    }
  end

  defp flagged_at(sweep, k) do
    Enum.find_value(sweep, 0, fn t -> if t.k == k, do: t.flagged end)
  end

  # >Kx-anchor sweep: for each k, how many shards exceed k*anchor and how well that set
  # matches the true Zipf head (recall).
  defp ratio_sweep(rates, ks, anchor) do
    Enum.map(ks, fn k ->
      cutoff = k * anchor
      flagged = for r <- rates, r.q_per_s > cutoff, do: r.shard_id
      %{k: k, flagged: length(flagged), zipf_recall: round2(zipf_recall(flagged))}
    end)
  end

  # Absolute q/s floor set to the top-N-th highest rate: "flag shards at/above F q/s to
  # catch the top N." The portable production knob (a fixed rate, not a distribution
  # ratio), and the one that stays tight when the median collapses.
  defp absolute_sweep(rates, top_ns) do
    desc = rates |> Enum.map(& &1.q_per_s) |> Enum.sort(:desc)

    Enum.map(top_ns, fn topn ->
      floor = Enum.at(desc, topn - 1) || 0.0
      flagged = for r <- rates, r.q_per_s >= floor, do: r.shard_id

      %{
        top_n: topn,
        floor_qps: round1(floor),
        flagged: length(flagged),
        zipf_recall: round2(zipf_recall(flagged))
      }
    end)
  end

  # Precision of the flagged set vs the true top-|flagged| by Zipf weight (the true
  # hot shards are hot_1..hot_m). 1.0 = the flag caught exactly the right shards.
  defp zipf_recall([]), do: 1.0

  defp zipf_recall(ids) do
    m = length(ids)
    true_top = MapSet.new(1..m, &"hot_#{&1}")
    Enum.count(ids, &MapSet.member?(true_top, &1)) / m
  end

  defp jaccard(a, b) do
    union = MapSet.size(MapSet.union(a, b))
    if union == 0, do: 1.0, else: MapSet.size(MapSet.intersection(a, b)) / union
  end

  defp top_ids(rates, k) do
    rates |> Enum.sort_by(& &1.q_per_s, :desc) |> Enum.take(k) |> Enum.map(& &1.shard_id)
  end

  defp top10_detail(rates) do
    rates
    |> Enum.sort_by(& &1.q_per_s, :desc)
    |> Enum.take(10)
    |> Enum.map(fn r ->
      %{
        shard_id: r.shard_id,
        q_per_s: round1(r.q_per_s),
        rows_read_per_s: round1(r.rows_read_per_s)
      }
    end)
  end

  # A one-line human read of whether B is justified: the hot set must be recoverable by
  # the shipped read API and stable across windows; the policy note names the threshold
  # SHAPE the distribution supports (median-relative breaks once the tail pulls the
  # median to ~0 — the fleet-scale finding).
  defp verdict(top_recall, flap, median_collapsed) do
    detectable = top_recall >= 0.8
    stable = flap >= 0.8

    core =
      cond do
        detectable and stable ->
          "hot set detectable (ShardLoad.top recall #{round2(top_recall)}) and stable across " <>
            "windows (top-20 Jaccard #{round2(flap)})"

        detectable ->
          "hot set detectable (recall #{round2(top_recall)}) but the top set drifts across " <>
            "windows (Jaccard #{round2(flap)}) — widen the confirm window / add hysteresis"

        true ->
          "hot set not cleanly recovered (recall #{round2(top_recall)}) — raise --zipf/--queries, " <>
            "or the load is too flat to rebalance on"
      end

    policy =
      if median_collapsed do
        " — >Kx-median over-flags (a long Zipf tail / near-idle fleet): set the threshold on p99 (>Kx p99) or an absolute q/s floor, NOT >Kx median"
      else
        " — a p99-anchored or absolute-qps floor with a 2-window confirm is a viable anti-flap gate"
      end

    core <> policy
  end

  # --- ramp: find the node-density ceiling ---------------------------------

  @doc """
  Ramps open shards until the first `Fathom.Shard.Connection` open fails (the
  per-process fd ceiling — `kern.maxfilesperproc` / ~3 fds per WAL connection) or
  `:max` is reached. Uses **empty** shards (no provisioning) so it isolates the
  *count* ceiling cheaply; the per-shard memory here is the floor (no page cache).
  Returns the max held, the limiting factor, and RSS at the ceiling.
  """
  @spec ramp(keyword()) :: map()
  def ramp(opts \\ []) do
    max = Keyword.get(opts, :max, 200_000)
    step = Keyword.get(opts, :checkpoint, 10_000)
    setup()
    :erlang.garbage_collect()
    base_rss = rss_kb()
    base_beam = :erlang.memory(:total)

    {opened, reason, handles} = open_until_fail(max, step)

    :erlang.garbage_collect()
    rss = rss_kb()
    beam = :erlang.memory(:total)

    Enum.each(handles, fn {pid, ref, conn} ->
      Connection.close(conn)
      Fathom.Shard.checkin(pid, ref)
    end)

    mfp = maxfilesperproc()

    %{
      ceiling_shards: opened,
      limit: limit_label(reason, opened, max),
      rss_at_ceiling_mb: round(rss / 1024),
      rss_per_shard_kb: round(safe_div(rss - base_rss, opened)),
      beam_per_shard_kb: kb(safe_div(beam - base_beam, opened)),
      maxfilesperproc: mfp,
      fd_predicted_ceiling: if(mfp, do: div(mfp, 3), else: nil),
      reason_detail: inspect(reason)
    }
  end

  defp open_until_fail(max, step) do
    {acc, reason} =
      Enum.reduce_while(1..max, {[], nil}, fn i, {acc, _} ->
        if rem(i, step) == 0,
          do: Logger.warning("  held #{i} shards, RSS #{round(rss_kb() / 1024)} MB")

        case open_empty("ramp_#{i}") do
          {:ok, h} ->
            {:cont, {[h | acc], nil}}

          {:error, reason} ->
            Logger.warning("  open failed at shard #{i}: #{inspect(reason)}")
            {:halt, {acc, reason}}
        end
      end)

    {length(acc), reason, acc}
  end

  # Empty shard: checkout creates a fresh coordinator (no remote file → no pull),
  # Connection.open creates the empty DB + WAL (3 fds), SELECT 1 needs no table.
  defp open_empty(id) do
    with {:ok, pid, ref, path} <- Shards.checkout(id),
         {:ok, conn} <- Connection.open(path),
         {:ok, _} <- Connection.query(conn, "SELECT 1", []) do
      {:ok, {pid, ref, conn}}
    end
  rescue
    e -> {:error, e}
  catch
    :exit, r -> {:error, {:exit, r}}
  end

  defp limit_label(nil, opened, max) when opened >= max, do: "reached --max (no ceiling hit)"
  defp limit_label(_reason, _opened, _max), do: "file descriptors (open failed — maxfilesperproc)"

  defp maxfilesperproc do
    case :os.cmd(~c"sysctl -n kern.maxfilesperproc")
         |> List.to_string()
         |> String.trim()
         |> Integer.parse() do
      {n, _} -> n
      :error -> nil
    end
  end

  # --- setup ---------------------------------------------------------------

  defp setup do
    data = Path.join(scratch(), "live")
    store = Path.join(scratch(), "remote")
    File.rm_rf!(data)
    File.rm_rf!(store)
    File.mkdir_p!(data)
    File.mkdir_p!(store)

    Application.put_env(:fathom, :shard_data_dir, data)
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: store)
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :directory_touch, false)
    Application.put_env(:fathom, :lazy_migrate, false)

    ensure_started(Registry, keys: :unique, name: Fathom.ShardRegistry)
    ensure_started(DynamicSupervisor, name: Fathom.ShardSupervisor, strategy: :one_for_one)
    :ok
  end

  defp ensure_started(mod, opts) do
    case mod.start_link(opts) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  @doc "Removes the scale scratch dir (called by the task after the run)."
  def cleanup, do: File.rm_rf!(scratch())

  # --- helpers -------------------------------------------------------------

  defp rss_kb do
    cmd = "ps -o rss= -p #{List.to_string(:os.getpid())}"

    case :os.cmd(String.to_charlist(cmd))
         |> List.to_string()
         |> String.trim()
         |> Integer.parse() do
      {kb, _} -> kb
      :error -> 0
    end
  end

  defp percentile([], _), do: 0.0

  defp percentile(values, pct) do
    sorted = Enum.sort(values)
    idx = round(pct / 100 * (length(sorted) - 1))
    (Enum.at(sorted, idx) || 0) / 1.0
  end

  defp safe_div(_num, 0), do: 0
  defp safe_div(num, den), do: num / den

  defp kb(n), do: round(n / 1024)
  defp round1(n), do: Float.round(n / 1, 1)
  defp round2(n), do: Float.round(n / 1, 2)
  defp drop_db(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))

  defp storage_path(id) do
    dir = Application.get_env(:fathom, Fathom.Shard.Storage.Local, [])[:dir]
    Path.join(dir, "#{id}.db")
  end

  defp scratch, do: Path.join(System.tmp_dir!(), "fathom_scale")
end
