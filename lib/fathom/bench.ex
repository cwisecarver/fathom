defmodule Fathom.Bench do
  @moduledoc """
  Hot-path benchmark primitives for the bench-then-commit gate.

  Fathom's scaling story is *millions of small shards*, so cost is dominated by
  per-shard open and fan-out, not single-query throughput. No single number
  captures that, so this module measures the four live hot paths independently
  (see `docs/benchmark-plan.md`):

    * `cold_open/1`     — `cold_open_p50_us`     — `Fathom.Shards.checkout/1` + first query (warm/local pull)
    * `cold_open_s3/1`  — `cold_open_s3_p50_us`  — same, pulling from S3 (opt-in: `FATHOM_S3_TEST_*` env)
    * `dir_resolve/1`   — `dir_resolve_p50_us`   — `Fathom.Directory.resolve/1` (needs Postgres)
    * `copy_throughput/1` — `copy_keystone_rows_per_s` — `Fathom.Migrator.Copy.migrate/4`
    * `fanout/1`        — `fanout_kb_per_shard`  — BEAM memory per concurrently-open shard
    * `hrana_rt/1`      — `hrana_rt_us`          — one `SELECT 1` round trip over real Hrana
    * `wire_rows/1`     — `wire_rows_per_s`      — result-set encode throughput over real Hrana

  ## Why there are two wire metrics

  Until 2026-07-31 `hrana_rt_us` was a recorded `null` placeholder and **nothing in the gate
  executed a line of `Filo`**. That is how a 200x regression sat in row encoding unnoticed:
  `Filo.Value.encode_json/1` raised and rescued an exception per BLOB cell (32.84 µs/value
  against 0.16 µs for text), and no gated metric could see it — `cold_open` and
  `copy_keystone_rows_per_s` are SQLite and storage, `dir_resolve` is Postgres, and
  `fanout_kb_per_shard` is BEAM memory.

  `hrana_rt_us` alone would **not** have caught it: a `SELECT 1` returns one integer cell, and
  the bug was per-cell on blobs. So the wire is gated by two metrics with different jobs:

    * `hrana_rt_us` — per-request overhead. Framing, routing, stream lookup, `Request.handle`.
      One tiny result, so it is dominated by fixed cost.
    * `wire_rows_per_s` — per-CELL cost at volume, over a `Fathom.Keystone` table, so every
      storage class including BLOB crosses the encoder. This is the one that catches an
      encoder regression.

  Both are **loopback software cost**, not a network RTT: a µs link, no bandwidth-delay, no
  TLS, no LB hop. The chaos rig measures real network cost (`docs/deploy-cluster.md`). They
  run over HTTP (`Filo.Client`, on the transitive `:mint`) rather than WebSocket, because
  `mint_web_socket` is a dev/test-only dependency and the gate compiles `MIX_ENV=prod`. Row
  encoding is shared by both JSON transports, so the HTTP path covers it.

  Each returns a number (or `nil` when a bench can't run, e.g. no Postgres for the
  directory bench). `all/1` runs the requested set and returns the perf-history
  metric map. `mix fathom.bench` and the `@tag :bench` test guards both call these,
  so "how we measure" lives in exactly one place.

  This is measurement-only code: it starts a **minimal** subset of the tree itself
  (`Fathom.ShardRegistry` + `Fathom.ShardSupervisor`; `Fathom.Repo` only for the
  directory bench), never the Hrana listener, Oban, or the web endpoint. It sets
  its own runtime knobs (clean Local storage dir, durability flush off,
  `:directory_touch`/`:lazy_migrate` off) so a bench can't touch Postgres on the
  hot path or fight the periodic flush.
  """
  require Logger

  alias Fathom.Migrator.Copy
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Storage
  alias Fathom.Shard.WarmFollower
  alias Fathom.Shards

  @default_trials 5
  @cold_open_samples 50
  @cold_open_s3_samples 20
  @resolve_samples 200
  @copy_rows 100_000
  @fanout_n 200
  # Small on purpose: each held connection is ~3 fds (db + -wal + -shm), so the served bench is
  # fd-bound long before it is interesting to push. It measures the per-shard SLOPE, not a ceiling.
  @served_n 64
  @served_rows 200
  @concurrent_shards 64
  @concurrent_ms 1_000
  @recorder_rows 2_000
  @warm_shards 200
  @warm_size_kb 256
  @hrana_rt_samples 200
  @wire_rows 1_000

  @all_metrics [
    :cold_open,
    :cold_open_s3,
    :warm_s3,
    :failover_rto,
    :dir_resolve,
    :dir_recorder,
    :copy,
    :fanout,
    :served,
    :concurrent,
    :hrana_rt,
    :hrana_open_rt,
    :wire_rows,
    :wire_encode,
    :flush
  ]

  @doc """
  Runs the requested benches and returns the perf-history metric map. Opts:

    * `:only`    — list of `#{inspect(@all_metrics)}` (default all)
    * `:trials`  — median trials for throughput/memory benches (default #{@default_trials})
    * `:cold_open_samples` / `:resolve_samples` / `:copy_rows` / `:fanout_n`
  """
  @spec all(keyword()) :: map()
  def all(opts \\ []) do
    only = Keyword.get(opts, :only, @all_metrics)

    # The failover-RTO bench measures cold vs warm together (same seeded shard), so
    # run it once and split into the two perf-history metrics. Opt-in: nil without S3.
    rto = run_if(only, :failover_rto, fn -> failover_rto(opts) end)

    # Same one-run-two-metrics split as `rto` above: p50 and p99 must describe the SAME sample
    # set, or the gate compares two independent runs of a noisy bench and calls the difference a
    # regression (expert review 2026-08-01 #41.5).
    served = run_if(only, :served, fn -> served(opts) end)
    conc = run_if(only, :concurrent, fn -> concurrent_checkout(opts) end)
    cold = run_if(only, :cold_open, fn -> cold_open_stats(opts) end)
    rt = run_if(only, :hrana_rt, fn -> hrana_rt_stats(opts) end)

    %{
      cold_open_p50_us: cold && cold.p50_us,
      # The TAIL. Every other latency metric is a p50, so a change that leaves p50 flat and
      # doubles p99 — a blocking call back into the coordinator mailbox, twice now — scored ~0%.
      cold_open_p99_us: cold && cold.p99_us,
      # The cold-S3 path — nil unless an S3 endpoint is configured (opt-in).
      cold_open_s3_p50_us: run_if(only, :cold_open_s3, fn -> cold_open_s3(opts) end),
      # Aggregate warming rate (shards/s) pulling many shards from S3 at once — opt-in.
      warm_s3_shards_per_s: run_if(only, :warm_s3, fn -> warm_s3_throughput(opts) end),
      # Failover RTO at real size: cold pull vs warm 304-promote (opt-in, S3).
      failover_cold_s3_p50_us: rto && rto.cold_us,
      failover_warm_s3_p50_us: rto && rto.warm_us,
      dir_resolve_p50_us: run_if(only, :dir_resolve, fn -> dir_resolve(opts) end),
      # The path per-checkout directory work ACTUALLY takes since the Recorder landed (#41.6);
      # dir_resolve above is control-plane only and was standing in for a per-request cost it no
      # longer represents.
      dir_recorder_flush_rows_per_s:
        run_if(only, :dir_recorder, fn -> dir_recorder_flush(opts) end),
      copy_keystone_rows_per_s: run_if(only, :copy, fn -> copy_throughput(opts) end),
      fanout_kb_per_shard: run_if(only, :fanout, fn -> fanout(opts) end),
      # The SERVED regime (#41.2). fanout above holds NO connections, so it only ever measured
      # the ~16 KiB idle cost; this is the ~220 KiB one that actually binds a deployment.
      # `:binary` is split out because the statement cache's sub-binary pin lands there and not
      # in the process heap — one combined number would hide that whole class of regression.
      served_kb_per_shard: served && served.total_kb,
      served_binary_kb_per_shard: served && served.binary_kb,
      # CONTENTION (#41.4). Every other metric here is serial, so a change that serialises the
      # ETS counters, the LRU, or the coordinator GenServer.call left all of them flat.
      concurrent_checkout_per_s: conc && conc.spread_per_s,
      same_shard_checkout_per_s: conc && conc.same_shard_per_s,
      # No longer a placeholder (2026-07-31). Loopback wire software cost, not a network RTT.
      hrana_rt_us: rt && rt.p50_us,
      hrana_rt_p99_us: rt && rt.p99_us,
      # Per-STREAM open, which for HTTP SDKs is the per-REQUEST path (expert review
      # 2026-08-01 #41.1). hrana_rt_us above reuses one client, so every one of its samples is
      # a baton-resumed stream and the open is explicitly excluded from the timed window.
      hrana_open_rt_us: run_if(only, :hrana_open_rt, fn -> hrana_open_rt(opts) end),
      wire_rows_per_s: run_if(only, :wire_rows, fn -> wire_rows(opts) end),
      # The OTHER encoder (#41.7). wire_rows_per_s drives encode_json/1; this drives encode/1,
      # the tagged-map builder Cursor and Protobuf reach, whose own moduledoc says the tagged-map
      # layer "roughly doubled" per-cell cost.
      wire_encode_rows_per_s: run_if(only, :wire_encode, fn -> wire_encode_rows(opts) end),
      # The write path. Before this, NOTHING in the harness executed a line of the durability
      # flush — setup/1 sets :shard_flush_interval_ms to 0 (#41.3).
      flush_p50_us: run_if(only, :flush, fn -> flush_p50(opts) end)
    }
  end

  defp run_if(only, metric, fun), do: if(metric in only, do: fun.(), else: nil)

  # --- cold open -----------------------------------------------------------

  @doc """
  Median latency (µs) to cold-open a shard: `Fathom.Shards.checkout/1`, open the
  connection at the returned path, and run the first query. The shard is drained
  (flush + drop local + release lease) between samples so every open is genuinely
  cold (pulled from storage), matching production's pull-on-wake.
  """
  @spec cold_open(keyword()) :: float()
  def cold_open(opts \\ []), do: cold_open_stats(opts).p50_us

  @doc """
  `cold_open/1`'s samples reduced to BOTH p50 and p99 (expert review 2026-08-01 #41.5).

  The gate was a pure ratio of p50s against the parent, which AGENTS.md forbids in as many
  words: "Assert an absolute floor, not only a ratio… The ratio holds while throughput
  collapses." A change that leaves p50 flat and doubles p99 — reintroducing a blocking Storage
  call into the coordinator mailbox, which has happened twice — produces a ~0% delta and sails
  through. `docs/benchmark-plan.md` records that moving the flush off-process was done for
  "recurring **p99** checkout spikes", so the tail is the thing the harness most needed to see
  and was the one thing it never reduced for.

  One run, two reducers: no extra work, and the two numbers are guaranteed to describe the same
  sample set rather than two separate runs of a noisy bench.
  """
  @spec cold_open_stats(keyword()) :: %{p50_us: float(), p99_us: float()}
  def cold_open_stats(opts \\ []) do
    samples = cold_open_samples(opts)
    %{p50_us: p50(samples), p99_us: percentile(samples, 99)}
  end

  defp cold_open_samples(opts) do
    setup(opts)
    samples = Keyword.get(opts, :cold_open_samples, @cold_open_samples)

    # Warm code paths once (first run pays one-time module/NIF load).
    warm = "bench_coldopen_warm"
    seed_storage_shard(warm)
    teardown_open(warm, open_and_query(warm))

    # A fresh id per sample: after drain the coordinator is dead, but the Registry
    # clears its entry asynchronously — reusing the id could hand the next checkout
    # the dying pid and call a dead process. Unique ids sidestep the race and keep
    # every open a genuine cold pull from storage.
    samples
    |> times(fn i ->
      id = "bench_coldopen_#{i}"
      seed_storage_shard(id)
      {us, handle} = :timer.tc(fn -> open_and_query(id) end)
      teardown_open(id, handle)
      us
    end)
  end

  @doc """
  Median latency (µs) to cold-open a shard **pulled from S3** — the production cold
  path, vs `cold_open/1` which pulls from local NVMe. Returns `nil` unless an
  S3-compatible endpoint is configured (env `FATHOM_S3_TEST_ENDPOINT`, as
  `scripts/minio_test.sh` sets), so the default gate stays S3-free and fast.

  Realism: against MinIO-on-localhost this measures the S3 *protocol* + loopback
  (~ms), NOT real S3 latency. Point `FATHOM_S3_TEST_ENDPOINT` at S3/R2 in-region for
  the production number (TTFB/RTT-bound, ~tens of ms). Each sample seeds a shard to
  S3, drops local, then times the checkout (pull from S3) + open + first query.
  """
  @spec cold_open_s3(keyword()) :: float() | nil
  def cold_open_s3(opts \\ []) do
    if s3_opt_in?() do
      setup_s3(opts)
      samples = Keyword.get(opts, :cold_open_s3_samples, @cold_open_s3_samples)

      try do
        # Warm-up doubles as a reachability probe: a flush/pull against a down or
        # misconfigured store raises here and we degrade to nil.
        warm = uniq("bench_s3_warm")
        seed_s3_shard(warm)
        teardown_open(warm, open_and_query(warm))

        samples
        |> times(fn _ ->
          id = uniq("bench_s3_cold")
          seed_s3_shard(id)
          {us, handle} = :timer.tc(fn -> open_and_query(id) end)
          teardown_open(id, handle)
          us
        end)
        |> p50()
      rescue
        e ->
          Logger.warning("cold_open_s3 skipped: S3 unreachable/misconfigured (#{inspect(e)})")
          nil
      catch
        :exit, reason ->
          Logger.warning("cold_open_s3 skipped: #{inspect(reason)}")
          nil
      end
    else
      nil
    end
  end

  # Opt-in only when an endpoint is explicitly set — keeps `mix fathom.bench` and
  # the commit gate S3-free by default.
  defp s3_opt_in?, do: System.get_env("FATHOM_S3_TEST_ENDPOINT") != nil

  # Seed a shard's object into S3 (create local, flush = PUT, drop local) so the
  # next checkout is a genuine cold pull from S3. Fresh ids ⇒ no stale lease.
  defp seed_s3_shard(id) do
    data = Application.get_env(:fathom, :shard_data_dir)
    tmp = Path.join(data, "#{id}.seed")
    drop_db(tmp)
    {:ok, conn} = Connection.open(tmp)
    Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    Connection.exec(conn, "INSERT INTO t DEFAULT VALUES")
    Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(id, tmp)
    drop_db(tmp)
  end

  defp uniq(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  # --- warming throughput (S3) ---------------------------------------------

  @doc """
  Aggregate warming throughput (shards/s): provision N `:warm_size_kb` shards in S3,
  drop local, then warm them ALL concurrently (`Fathom.Shards.checkout/1` each, which
  pulls from S3) and report `N ÷ wall`. The node-startup / failover number. Returns
  `nil` unless an S3 endpoint is configured (opt-in). Concurrency is bounded by the
  Req/Finch connection pool to the S3 host, so this exposes the pool ceiling.
  """
  @spec warm_s3_throughput(keyword()) :: float() | nil
  def warm_s3_throughput(opts \\ []) do
    if s3_opt_in?() do
      setup_s3(opts)
      n = Keyword.get(opts, :warm_shards, @warm_shards)
      size_kb = Keyword.get(opts, :warm_size_kb, @warm_size_kb)

      try do
        ids = for _ <- 1..n, do: uniq("bench_s3_warm")
        Enum.each(ids, &seed_s3_sized(&1, size_kb))

        {us, _} = :timer.tc(fn -> warm_all(ids) end)

        Enum.each(ids, &Shards.drain/1)
        n / (us / 1_000_000)
      rescue
        e ->
          Logger.warning("warm_s3 skipped: S3 unreachable/misconfigured (#{inspect(e)})")
          nil
      catch
        :exit, reason ->
          Logger.warning("warm_s3 skipped: #{inspect(reason)}")
          nil
      end
    else
      nil
    end
  end

  # Fire every checkout concurrently; the S3 pulls bottleneck on the Finch pool.
  defp warm_all(ids) do
    ids
    |> Task.async_stream(&Shards.checkout/1, max_concurrency: length(ids), timeout: 120_000)
    |> Stream.run()
  end

  # Seed a ~size_kb shard object into S3 (incompressible randomblob), drop local.
  defp seed_s3_sized(id, size_kb) do
    data = Application.get_env(:fathom, :shard_data_dir)
    tmp = Path.join(data, "#{id}.seed")
    rows = max(div(size_kb * 1024, 4096), 1)
    drop_db(tmp)
    {:ok, conn} = Connection.open(tmp)
    Connection.exec(conn, "CREATE TABLE blob (id INTEGER PRIMARY KEY, data BLOB)")
    Connection.exec(conn, "BEGIN")

    Connection.exec(conn, """
    WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < #{rows})
    INSERT INTO blob (data) SELECT randomblob(4096) FROM c
    """)

    Connection.exec(conn, "COMMIT")
    Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(id, tmp)
    drop_db(tmp)
  end

  # --- failover RTO: warm-standby vs cold (S3) -----------------------------

  @doc """
  Failover RTO: median latency (µs) to open a shard on a survivor **cold** (pull the
  whole object from S3, today's failover cost) vs **warm** (the shard is already in
  the warm-follower cache). Both take the real crash-failover path — a lease STEAL that
  touches (etag-rotates) the object (#15) — so the warm sample exercises the steal+touch
  takeover the warm follower must survive, not a no-steal open. Both at `:warm_size_kb`.
  Returns `%{cold_us, warm_us}`, or `nil` without an S3 endpoint (opt-in, like `cold_open_s3/1`).

  The honest delta: the warm path is **not** purely local. H2 must confirm the cache
  hasn't gone stale (a warm copy can lag the owner's latest flush), so it still pays
  ONE S3 round-trip — but a `304` with no body. The win over cold is the object
  **body transfer** avoided, so it grows with shard size and bandwidth-delay and is
  marginal for a tiny shard on a fast link. Against localhost MinIO this measures
  protocol + loopback; inject latency/bandwidth (`scripts/benchmark_s3_latency.sh`) or
  point `FATHOM_S3_TEST_ENDPOINT` at in-region S3 for the production gap.
  """
  @spec failover_rto(keyword()) :: %{cold_us: float(), warm_us: float()} | nil
  def failover_rto(opts \\ []) do
    if s3_opt_in?() do
      setup_s3(opts)
      samples = Keyword.get(opts, :failover_samples, @cold_open_s3_samples)
      size_kb = Keyword.get(opts, :warm_size_kb, @warm_size_kb)

      cache = Path.join(tmp_dir(), "bench_warm_cache")
      File.rm_rf!(cache)
      File.mkdir_p!(cache)
      Application.put_env(:fathom, :warm_cache_dir, cache)

      try do
        # Warm-up doubles as a reachability probe (raises → degrade to nil).
        warm = uniq("bench_rto_probe")
        seed_s3_sized(warm, size_kb)
        teardown_open(warm, open_and_query(warm))

        # Cold: every sample pulls the whole object from S3 (no warm cache). A real
        # failover STEALS the dead owner's lease, which touches (etag-rotates) the
        # object — seed the dead-owner lock so the measurement takes that path (#15).
        cold =
          times(samples, fn _ ->
            id = uniq("bench_rto_cold")
            seed_s3_sized(id, size_kb)
            seed_dead_lock(id)
            {us, handle} = :timer.tc(fn -> open_and_query(id) end)
            teardown_open(id, handle)
            us
          end)
          |> p50()

        # Warm: the shard is pre-cached with its etag. Expert review #15: on a real
        # crash failover the steal touches (etag-rotates) the object, so the pre-cached
        # etag no longer matches — seed the dead-owner lock so the "warm" sample
        # measures the steal+touch takeover the warm follower must survive, not the
        # no-steal open it took before (which never rotated the etag and so overstated
        # the win). With the #15 fix the takeover adopts the touched etag from the copy
        # it already holds instead of re-pulling the whole object.
        warm_us =
          times(samples, fn _ ->
            id = uniq("bench_rto_warm")
            seed_s3_sized(id, size_kb)
            populate_warm_cache(id)
            seed_dead_lock(id)
            {us, handle} = :timer.tc(fn -> open_and_query(id) end)
            teardown_open(id, handle)
            us
          end)
          |> p50()

        %{cold_us: cold, warm_us: warm_us}
      rescue
        e ->
          Logger.warning("failover_rto skipped: S3 unreachable/misconfigured (#{inspect(e)})")
          nil
      catch
        :exit, reason ->
          Logger.warning("failover_rto skipped: #{inspect(reason)}")
          nil
      end
    else
      nil
    end
  end

  # Pre-pull the shard's current S3 object into the warm cache and record its etag,
  # exactly as `Fathom.Shard.WarmFollower` does — so the next open takes the H2
  # freshness-validated promotion path (a 304), not a cold pull.
  defp populate_warm_cache(id) do
    path = WarmFollower.cache_path(id)
    File.mkdir_p!(Path.dirname(path))
    {:ok, {:written, etag}} = Storage.pull_if_changed(id, path, nil)
    File.write!(path <> ".etag", etag)
  end

  # Seed a dead prior owner's lock so the next open STEALS it and touches (etag-rotates)
  # the object — the crash-failover path (#15). A negative TTL lands `expires_at_ms` past
  # the steal margin, and no heartbeat exists for this synthetic owner, so `owner_live?`
  # resolves it dead. No prod seam needed: a plain `acquire_lease` writes the lock object.
  defp seed_dead_lock(id) do
    ttl = -(Storage.steal_margin_ms() + 60_000)

    {:ok, _lease} =
      Storage.acquire_lease(id, "dead@bench##{System.unique_integer([:positive])}", ttl)

    :ok
  end

  defp open_and_query(id) do
    {:ok, pid, ref, path} = Shards.checkout(id)
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "SELECT 1", [])
    {pid, ref, conn}
  end

  defp teardown_open(id, {pid, ref, conn}) do
    Connection.close(conn)
    Fathom.Shard.checkin(pid, ref)
    Shards.drain(id)
    :ok
  end

  # --- directory resolve ---------------------------------------------------

  @doc """
  Median latency (µs) of `Fathom.Directory.resolve/1` in warm steady state (the
  on-conflict update path that runs on every request). Returns `nil` if Postgres
  / the `shards` table isn't reachable, so the harness still runs without a DB.
  """
  @spec dir_resolve(keyword()) :: float() | nil
  def dir_resolve(opts \\ []) do
    setup(opts)
    samples = Keyword.get(opts, :resolve_samples, @resolve_samples)

    with :ok <- ensure_repo(),
         true <- directory_ready?() do
      id = "bench_resolve_#{System.unique_integer([:positive])}"
      # Seed the row, then measure the steady-state update path.
      {:ok, _} = Fathom.Directory.resolve(id)

      result =
        samples
        |> times(fn _ ->
          {us, {:ok, _}} = :timer.tc(fn -> Fathom.Directory.resolve(id) end)
          us
        end)
        |> p50()

      # Don't leave bench rows behind — this may run against a real DB (fathom_dev
      # on a bare `mix fathom.bench`), not just the throwaway fathom_bench.
      cleanup_resolve_row(id)
      result
    else
      _ ->
        Logger.warning("dir_resolve bench skipped: Postgres/shards table unavailable")
        nil
    end
  end

  @doc """
  `dir_recorder_flush_rows_per_s` — rows/sec through `Fathom.Directory.Recorder.flush/0`
  (expert review 2026-08-01 #41.6).

  **`dir_resolve_p50_us` is not the request path and has not been since the Recorder landed.**
  `Directory.resolve/1`'s live callers are `Fathom.Tenants` (provision, fork) and
  `Migrator.ShardMigration` — provisioning and migration, all control-plane. It stays gated
  because it still guards that reader, but it was standing in for a per-request cost it no
  longer represents, and `docs/benchmark-plan.md` said so in as many words until this commit.

  What per-checkout directory work actually costs now is THIS: touches coalesce into ETS and are
  batch-flushed, so the live cost is an `:ets.select` walk plus one `:ets.take` per touched
  shard, then `insert_all` in 1,000-row chunks — and it scales with DENSITY. At 30k active
  shards that is ~30k ETS takes and ~30 multi-row upserts every second against a `pool_size` of
  25, which is the shape that falls over quietly.
  """
  @spec dir_recorder_flush(keyword()) :: float() | nil
  def dir_recorder_flush(opts \\ []) do
    setup(opts)
    n = Keyword.get(opts, :recorder_rows, @recorder_rows)
    trials = Keyword.get(opts, :trials, @default_trials)

    with :ok <- ensure_repo(),
         true <- directory_ready?(),
         :ok <- ensure_started(Fathom.Directory.Recorder, []) do
      # Drain anything buffered before timing, so the first trial is not credited with a
      # neighbour's rows.
      _ = Fathom.Directory.Recorder.flush()

      ids =
        Enum.map(1..n, fn i -> "bench_rec_#{System.unique_integer([:positive])}_#{i}" end)

      result =
        trials
        |> times(fn _ ->
          Enum.each(ids, &Fathom.Directory.Recorder.record/1)
          {us, flushed} = :timer.tc(fn -> Fathom.Directory.Recorder.flush() end)

          # The whole point is the batch upsert; a flush that drained nothing would report an
          # enormous rows/sec for work that never happened.
          if flushed < n do
            raise "recorder flush drained #{flushed}/#{n} rows — measuring nothing"
          end

          flushed / (us / 1_000_000)
        end)
        |> median()

      Enum.each(ids, &cleanup_resolve_row/1)
      result
    else
      _ ->
        Logger.warning("dir_recorder_flush bench skipped: Postgres/shards table unavailable")
        nil
    end
  end

  # --- migration copy throughput ------------------------------------------

  @doc """
  Median rows/sec for `Fathom.Migrator.Copy.migrate/4`: copy a `Fathom.Keystone` seeded with
  `:copy_rows` rows, replay one representative `ALTER` plus an index build, stamp + checkpoint.
  Fsync-light by construction — the real `migrate/4` issues a single end-of-copy checkpoint, not
  a per-row fsync, so the number isn't APFS-dominated.

  **The source is the keystone, not a toy.** This measured a three-column
  `(INTEGER, TEXT, INTEGER)` table until 2026-07-31, which made "migration copy throughput"
  mostly a measurement of `File.cp` over unusually narrow rows: no BLOBs, no NULLs, no wide
  rows, nothing that reaches SQLite's overflow pages. Real tenant rows do. The keystone carries
  every storage class and affinity, so bytes-per-row is realistic and so is the number.

  Reported as **`copy_keystone_rows_per_s`**, deliberately a different metric name from the old
  `copy_rows_per_s`. Rows got much wider, so rows/second dropped by construction; comparing
  across that switch would read a fixture change as a code regression. The rename starts a new
  series (see `Fathom.Bench.Gate`).

  Not comparable to the TPC-B / TPC-C numbers either, and not meant to be: those are
  externally-specified schemas whose value is comparability with `sqld` and published results
  (`docs/tpc-benchmark-plan.md`). This one is fathom-internal.
  """
  @spec copy_throughput(keyword()) :: float()
  def copy_throughput(opts \\ []) do
    setup(opts)
    rows = Keyword.get(opts, :copy_rows, @copy_rows)
    trials = Keyword.get(opts, :trials, @default_trials)
    src = Path.join(tmp_dir(), "bench_copy_src.db")
    drop_db(src)

    # A fixed seed: the fixture must not move between runs, or the benchmark measures the
    # fixture instead of the code.
    {:ok, _} = Fathom.Keystone.build!(src, rows: rows)

    # `{sql, args}` pairs — the shape `Copy.replay_each/2` binds. Bare strings crash it with a
    # FunctionClauseError. This caller went stale when statement args landed (`beff929`), which
    # updated copy_test.exs but not the bench: `mix fathom.bench` is not part of `mix test`, so a
    # green suite hid a broken gate for three commits. Any change to Copy's statement shape must
    # grep its callers here too.
    statements = [
      {"ALTER TABLE ks_scalars ADD COLUMN added_col TEXT", []},
      # CREATE INDEX scans every row, so the replay does work proportional to row
      # count — representative of a real Django migration's per-row cost and far
      # more sensitive to a replay regression than an O(1) ADD COLUMN, which would
      # leave the metric measuring little more than a page-cache-warm File.cp.
      {"CREATE INDEX ks_added_idx ON ks_scalars (added_col)", []}
    ]

    trials
    |> times(fn i ->
      dest = Path.join(tmp_dir(), "bench_copy_dest_#{i}.db")
      drop_db(dest)
      {us, :ok} = :timer.tc(fn -> Copy.migrate(src, dest, 1, statements) end)
      drop_db(dest)
      rows / (us / 1_000_000)
    end)
    |> median()
  end

  # --- the wire (real Hrana, loopback) --------------------------------------

  @doc """
  `hrana_rt_us` — median µs of a warm-stream `SELECT 1` round trip through a real Hrana
  listener: `Filo.Client` → HTTP → `Filo.Plug` → `Fathom.ShardExecutor` → the shard → back.

  A read that issues no fsync and returns one cell, so it isolates **per-request** wire
  overhead (framing, stream lookup, routing, `Request.handle`) from storage noise. Per-cell
  encoding cost is `wire_rows/1`'s job, not this one.
  """
  @spec hrana_rt(keyword()) :: float() | nil
  def hrana_rt(opts \\ []) do
    case hrana_rt_stats(opts) do
      nil -> nil
      stats -> stats.p50_us
    end
  end

  @doc """
  `hrana_rt/1`'s samples reduced to BOTH p50 and p99 — see `cold_open_stats/1` for why the tail
  is gated at all (expert review 2026-08-01 #41.5). This is the wire's tail: a request-path
  change that adds an occasional stall shows up here and nowhere else in the gate.
  """
  @spec hrana_rt_stats(keyword()) :: %{p50_us: float(), p99_us: float()} | nil
  def hrana_rt_stats(opts \\ []) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    with_wire(opts, "benchrt", fn client ->
      # Warm: opens the shard and primes the stream. Never timed — a cold open here would
      # measure storage, which cold_open/1 already owns.
      {:ok, _, client} = Filo.Client.execute(client, "SELECT 1")

      {_client, us} =
        Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
          {t, result} = :timer.tc(fn -> Filo.Client.execute(c, "SELECT 1") end)
          {:ok, _, c} = result
          {c, [t | acc]}
        end)

      %{p50_us: p50(us), p99_us: percentile(us, 99)}
    end)
  end

  @doc """
  `hrana_open_rt_us` — median round trip for a request that must OPEN its stream.

  `hrana_rt_us` reuses one client, so every sample rides a baton-resumed stream and its
  warm-up — which is the only sample that pays the open — is explicitly discarded. But
  `Filo.Plug` opens a fresh stream for every Hrana v1 request and every v2/v3 request arriving
  with no baton, so for HTTP SDKs the open IS the per-request path: `ShardId.cast`, a telemetry
  span, the lifecycle ETS lookups, a `Registry.lookup`, a `GenServer.call` into the coordinator,
  and `Connection.open/1`'s `File.mkdir_p!` + `Sqlite3.open` + six PRAGMA dirty-IO NIF
  dispatches.

  AGENTS.md's hotspots A/B calls this the unexplained L=1 ceiling and says "measure-first
  pending `hrana_rt_us`" — that metric shipped and does not answer it, because of the baton
  reuse. This one does: the baton is cleared before each sample, so `Executor.open/2` and
  `Connection.open/1` are inside the timed window (expert review 2026-08-01 #41.1).
  """
  @spec hrana_open_rt_us(keyword()) :: float() | nil
  def hrana_open_rt_us(opts \\ []), do: hrana_open_rt(opts)

  defp hrana_open_rt(opts) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    with_wire(opts, "benchopen", fn client ->
      # Warm the SHARD (cold-open cost is cold_open/1's metric, not this one) — then every
      # timed sample starts from a fresh stream against an already-open shard.
      {:ok, _, client} = Filo.Client.execute(client, "SELECT 1")

      {_client, us} =
        Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
          # No baton ⇒ Filo.Plug opens a new stream for this request.
          c = %{c | baton: nil}
          {t, result} = :timer.tc(fn -> Filo.Client.execute(c, "SELECT 1") end)
          {:ok, _, c} = result
          {c, [t | acc]}
        end)

      p50(us)
    end)
  end

  @doc """
  `flush_p50_us` — median wall time of one real durability flush.

  The whole write path was ungated: `setup/1` sets `:shard_flush_interval_ms` to `0`, so no
  gated metric executed a single line of `snapshot/2`, `snapshot_and_upload/1`, or
  `handle_info(:durability_flush, …)` — roughly 2× the shard size in local I/O plus the shard
  size on the wire, per shard, per interval. Both recent flush regressions (the wasted fsyncs,
  the inline autocheckpoint) were caught by human review, not by the harness
  (expert review 2026-08-01 #41.3).

  Runs against `Storage.Local` over a keystone-sized shard, so it is S3-free and stays in the
  default gate. Drives `flush_now/1`, which is the same fence → `VACUUM INTO` → conditional
  PUT path the periodic timer takes.
  """
  @spec flush_p50(keyword()) :: float() | nil
  def flush_p50(opts \\ []) do
    trials = Keyword.get(opts, :trials, @default_trials)
    id = "benchflush"

    setup(opts)
    setup_write_path()

    try do
      {:ok, pid, ref, path} = Shards.checkout(id)
      {:ok, _} = Fathom.Keystone.build!(path, rows: Keyword.get(opts, :flush_rows, 2_000))
      :ok = Fathom.Shard.stamp_local_provenance(id)

      us =
        for _ <- 1..trials do
          # Dirty the shard so the flush has real work, then time the whole flush.
          Fathom.Shard.WriteCounter.bump(id)

          # A flush that did not happen is the failure mode this metric is most likely to
          # regress into — `flush_now/1` returns immediately on a clean shard, which reads as
          # a spectacular ~2 µs "result". Assert the shard was actually dirty first.
          unless Fathom.Shard.dirty?(pid) do
            raise "flush bench is measuring nothing: the shard is not dirty before flush_now/1"
          end

          {t, _} = :timer.tc(fn -> Fathom.Shard.flush_now(pid) end)
          t
        end

      Fathom.Shard.checkin(pid, ref)
      Shards.drain(id, 5_000)
      p50(us)
    after
      Shards.drain(id, 5_000)
      drop_db(Fathom.Shard.db_path(id))
    end
  end

  @doc """
  `wire_rows_per_s` — rows/sec the server can read, encode and ship for a whole result set,
  over a real Hrana listener.

  The source is a `Fathom.Keystone` (`:wire_rows` rows, default #{@wire_rows}), so every SQLite
  storage class — **including BLOB** — crosses `Filo.Value`'s encoder on every run. That is
  deliberate and is the whole point of the metric: the encoder bug this metric exists to catch
  cost 32.84 µs per blob cell and was invisible to every other gated number, because every other
  workload fathom benchmarks (TPC-B, TPC-C, `hrana_rt`) is INTEGER, REAL and TEXT only.

  Reported as rows/sec rather than cells/sec because the keystone's column count is fixed; a
  schema change there moves this metric, which is correct — it is a different measurement.
  """
  @spec wire_rows(keyword()) :: float() | nil
  def wire_rows(opts \\ []) do
    rows = Keyword.get(opts, :wire_rows, @wire_rows)
    trials = Keyword.get(opts, :trials, @default_trials)

    with_wire(opts, "benchwire", fn client ->
      # Seed through the shard's own file, then let the coordinator serve it: the client is
      # measuring the READ path, so the write must not be inside the timed window.
      path = Fathom.Shard.db_path("benchwire")
      drop_db(path)
      {:ok, _} = Fathom.Keystone.build!(path, rows: rows)
      # A file built straight into the live dir has no provenance, and cold-open now refuses
      # to serve an unprovenanced copy (expert review 2026-08-01 #2). Declare it.
      :ok = Fathom.Shard.stamp_local_provenance("benchwire")

      {:ok, _, client} = Filo.Client.execute(client, "SELECT count(*) FROM ks_scalars")

      {_client, samples} =
        Enum.reduce(1..trials, {client, []}, fn _, {c, acc} ->
          {us, result} = :timer.tc(fn -> Filo.Client.execute(c, "SELECT * FROM ks_scalars") end)
          {:ok, _, c} = result
          {c, [rows / (us / 1_000_000) | acc]}
        end)

      median(samples)
    end)
  end

  @doc """
  `wire_encode_rows_per_s` — rows/sec through `Filo.Value.encode/1`, the OTHER encoder
  (expert review 2026-08-01 #41.7).

  `Filo.Value` has two encoding PATHS and the gate only ever ran one. `wire_rows_per_s` drives
  `encode_json/1`, which emits JSON directly. The other path builds tagged maps with `encode/1`
  and then serialises them, and `Filo.Value`'s own moduledoc records that the tagged-map layer
  "roughly doubled" per-cell cost. `Cursor.entries/2` takes it for `rows: :maps`, and
  `Protobuf.encode_value/1` consumes the same maps — i.e. exactly when result sets are large.

  **This times `encode/1` FOLLOWED BY the JSON serialisation, not `encode/1` alone**, and the
  first draft got that wrong. Measured per cell on keystone rows:

      encode/1 alone          0.053 µs   <- FASTER than encode_json/1; times half the work
      encode_json/1           0.111 µs
      encode/1 + Jason        0.269 µs   <- 2.42x, the doubling the moduledoc describes

  Timing `encode/1` by itself made the "slower encoder" look 0.41x the cost of the fast one,
  because building a map is cheap and the expense is serialising it afterwards. A metric that
  reads *better* than its comparison is the tell that it is measuring the wrong span.

  **NOT driven through the cursor HTTP transport**, which is what the review proposed.
  `Filo.Client` states outright that cursors are outside its scope, so that shape would mean new
  HTTP plumbing to cover a path the finding itself calls "no live bug today — coverage only".
  This is the per-cell cost, which is the whole of the risk; cursor/protobuf FRAMING stays
  ungated, and that is a deliberate, stated limit rather than an oversight.

  Same keystone source as `wire_rows_per_s`, so every SQLite storage class — including the BLOB
  that carried the 200x regression — crosses this path too.
  """
  @spec wire_encode_rows(keyword()) :: float() | nil
  def wire_encode_rows(opts \\ []) do
    setup(opts)
    rows = Keyword.get(opts, :wire_rows, @wire_rows)
    trials = Keyword.get(opts, :trials, @default_trials)

    path = Fathom.Shard.db_path("benchencode")
    drop_db(path)
    {:ok, _} = Fathom.Keystone.build!(path, rows: rows)
    {:ok, conn} = Connection.open(path)

    try do
      {:ok, %{rows: fetched}} = Connection.query(conn, "SELECT * FROM ks_scalars", [])

      # Assert the fixture before timing it: encoding zero rows would report a spectacular
      # rows/sec for work that never happened (the `flush_p50_us` 2 µs lesson).
      if fetched == [], do: raise("wire_encode bench read 0 rows — measuring nothing")

      cells = length(fetched) * length(hd(fetched))
      if cells == 0, do: raise("wire_encode bench found 0 cells — measuring nothing")

      trials
      |> times(fn _ ->
        {us, _} =
          :timer.tc(fn ->
            Enum.each(fetched, fn row ->
              row |> Enum.map(&Filo.Value.encode/1) |> Jason.encode!()
            end)
          end)

        length(fetched) / (us / 1_000_000)
      end)
      |> median()
    after
      Connection.close(conn)
      drop_db(path)
    end
  end

  # Starts a real Filo listener on a loopback port, opens one client stream routed to `shard`
  # by Host subdomain, runs `fun`, and tears the whole thing down. Returns nil (metric skipped,
  # never faked) if the listener or the client cannot start.
  defp with_wire(opts, shard, fun) do
    setup(opts)
    # Route by Host subdomain exactly as the LB does. Pinned rather than inherited so the
    # metric does not depend on whatever SHARD_BASE_DOMAIN happens to be set in the shell.
    Application.put_env(:fathom, :shard_base_domain, nil)
    Application.put_env(:fathom, :default_shard, nil)

    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Application.ensure_all_started(:mint)

    case start_listener() do
      {:ok, sup, port} ->
        try do
          case Filo.Client.connect("http://127.0.0.1:#{port}", authority: "#{shard}.local") do
            {:ok, client} ->
              try do
                fun.(client)
              after
                Filo.Client.close(client)
              end

            {:error, reason} ->
              Logger.warning("wire bench: client connect failed (#{inspect(reason)}); skipping")
              nil
          end
        after
          if Process.alive?(sup), do: Supervisor.stop(sup)
          Fathom.Shards.drain(shard, 5_000)
        end

      {:error, reason} ->
        Logger.warning("wire bench: listener failed to start (#{inspect(reason)}); skipping")
        nil
    end
  end

  defp start_listener do
    port = free_port()

    plug_opts = [
      executor: Fathom.ShardExecutor,
      streams: Fathom.Bench.WireStreams,
      key: Filo.Baton.new_key(),
      open_arg: &Fathom.ShardExecutor.shard_from_conn/1
      # No :authorize — the bench runs with Hrana auth disabled.
    ]

    children = [
      {Filo.Streams, name: Fathom.Bench.WireStreams},
      {Bandit, plug: {Filo.Plug, plug_opts}, scheme: :http, ip: {127, 0, 0, 1}, port: port}
    ]

    case Supervisor.start_link(children, strategy: :one_for_one) do
      {:ok, sup} -> {:ok, sup, port}
      {:error, reason} -> {:error, reason}
    end
  end

  # Ask the OS for a free port and immediately release it. Racy in principle; in a serialized
  # bench run (the host-wide lock in mix fathom.bench) nothing else is claiming ports.
  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  # --- concurrent fan-out --------------------------------------------------

  @doc """
  Median BEAM memory (KiB) per concurrently-open shard: open `:fanout_n` shard
  coordinators and divide the `:erlang.memory(:total)` delta by N. The node-density
  number — how many shards a node can hold open at once.

  Measures the **coordinator** (`Fathom.Shard` GenServer + registry entry + lease),
  which is what an open-but-idle shard actually costs: the coordinator holds no
  SQLite connection (connections are per-stream and transient). Deliberately does
  NOT hold a connection per shard — that would burn ~3 file descriptors each
  (db + `-wal` + `-shm`) and exhaust the OS fd limit well before N is interesting.
  BEAM-side cost only; SQLite's off-heap page cache (held by active-stream
  connections, not the coordinator) is not counted (see `docs/benchmark-plan.md`).
  """
  @spec fanout(keyword()) :: float()
  def fanout(opts \\ []) do
    setup(opts)
    n = Keyword.get(opts, :fanout_n, @fanout_n)
    trials = Keyword.get(opts, :trials, @default_trials)

    # Ids carry the trial number too: a later trial reusing an id a drained-but-
    # not-yet-deregistered coordinator still holds would call a dead process.
    trials
    |> times(fn t ->
      :erlang.garbage_collect()
      before = :erlang.memory(:total)
      handles = Enum.map(1..n, fn i -> open_fanout_shard(t, i) end)
      :erlang.garbage_collect()
      used = :erlang.memory(:total) - before
      Enum.each(handles, &close_fanout_shard/1)
      used / n / 1024
    end)
    |> median()
  end

  defp open_fanout_shard(t, i) do
    id = "bench_fanout_#{t}_#{i}"
    {:ok, pid, ref, _path} = Shards.checkout(id)
    {id, pid, ref}
  end

  # --- the SERVED regime ----------------------------------------------------

  @doc """
  `served_kb_per_shard` — BEAM memory (KiB) per shard held under a LIVE connection that has run a
  query, reported as `{total, binary}` per shard (expert review 2026-08-01 #41.2).

  `fanout/1` deliberately holds no connections, so it measures only the ~16 KiB **idle** regime.
  The regime that actually binds a deployment is the ~220 KiB **served** one, and every
  per-connection resource decision was invisible to the gate:

    * `:shard_cache_size_kb` (`connection.ex`, default 2000 ⇒ up to 2 MiB of SQLite page cache
      per held stream), and
    * the statement cache's sub-binary pin, whose own fix comment says outright: "**It never
      showed up in the density numbers because it lands in `:erlang.memory(:binary)`, not the
      process heap.**"

  Which is why `:binary` is reported separately rather than folded into the total — a regression
  that pins sub-binaries moves `:binary` and leaves `:total` almost flat, so one number would
  have hidden exactly the class of bug this metric exists for.

  N is small (default #{@served_n}) on purpose: each held connection costs ~3 file descriptors
  (db + `-wal` + `-shm`), so this is fd-bound long before it is interesting to push. It measures
  the per-shard SLOPE, not a density ceiling — `mix fathom.scale` owns the ceiling.
  """
  @spec served(keyword()) :: %{total_kb: float(), binary_kb: float()}
  def served(opts \\ []) do
    setup(opts)
    n = Keyword.get(opts, :served_n, @served_n)
    trials = Keyword.get(opts, :trials, @default_trials)
    rows = Keyword.get(opts, :served_rows, @served_rows)

    results =
      times(trials, fn t ->
        collect_all()
        before_total = :erlang.memory(:total)
        before_bin = :erlang.memory(:binary)

        handles = Enum.map(1..n, fn i -> open_served_shard(t, i, rows) end)

        # Assert the precondition rather than trust it (the `flush_p50_us` 2 µs lesson: a metric
        # whose work silently did not happen reports a spectacular number and the gate says OK).
        # A held connection that never ran a query would measure the idle regime under a new name.
        unless length(handles) == n do
          raise "served bench opened #{length(handles)}/#{n} shards — measuring nothing"
        end

        collect_all()
        total = :erlang.memory(:total) - before_total
        bin = :erlang.memory(:binary) - before_bin

        Enum.each(handles, &close_served_shard/1)
        {total / n / 1024, bin / n / 1024}
      end)

    %{
      total_kb: results |> Enum.map(&elem(&1, 0)) |> median(),
      binary_kb: results |> Enum.map(&elem(&1, 1)) |> median()
    }
  end

  defp open_served_shard(t, i, rows) do
    id = "bench_served_#{t}_#{i}"
    {:ok, pid, ref, path} = Shards.checkout(id)
    {:ok, _} = Fathom.Keystone.build!(path, rows: rows)
    {:ok, conn} = Connection.open(path)

    # One real query per shard: primes the statement cache and the page cache, which is the whole
    # difference between "a connection exists" and "a connection is serving".
    {:ok, %{rows: got}} = Connection.query(conn, "SELECT * FROM ks_scalars LIMIT 50", [])

    # The query has to actually RETURN rows, or this measures a connection that was opened and
    # never used — the idle regime under a new name. (`SELECT * FROM keystone` silently failed
    # this way in the first draft: wrong table, and the match on `{:ok, _}` would have accepted
    # an empty result had the table merely been empty.)
    if got == [], do: raise("served bench read 0 rows — the page/statement caches are cold")

    {id, pid, ref, conn}
  end

  # --- contention -----------------------------------------------------------

  @doc """
  `concurrent_checkout_per_s` — checkout+checkin throughput with
  `System.schedulers_online() * 4` processes hammering `#{@concurrent_shards}` warm shards for
  `:concurrent_ms` (default #{@concurrent_ms}) (expert review 2026-08-01 #41.4).

  **Nothing in the gate could see contention.** `cold_open`, `copy`, `fanout`, `hrana_rt` and
  `wire_rows` are all serial; the only concurrent bench was `warm_all/1`, inside the opt-in S3
  path. Yet the things most likely to regress under load are all concurrency-tuned:
  `write_concurrency` + `decentralized_counters` on `ShardLoad`/`ShardLatency`/`WriteCounter`, the
  `Lru` CA-tree coarsening, `+SDio 10`, and the per-shard coordinator `GenServer.call`. A change
  that serialises any of them leaves every serial p50 flat.

  `spread` is the fleet shape — many shards, so the ETS counters and the LRU are the contended
  resources. `same_shard` points every worker at ONE coordinator, which bounds its head-of-line
  throughput: that `GenServer.call` is the single lock every stream on a hot shard queues behind,
  and it is invisible to any spread measurement.
  """
  @spec concurrent_checkout(keyword()) :: %{spread_per_s: float(), same_shard_per_s: float()}
  def concurrent_checkout(opts \\ []) do
    setup(opts)
    ms = Keyword.get(opts, :concurrent_ms, @concurrent_ms)
    n = Keyword.get(opts, :concurrent_shards, @concurrent_shards)
    workers = Keyword.get(opts, :concurrent_workers, System.schedulers_online() * 4)
    trials = Keyword.get(opts, :trials, @default_trials)

    spread_ids = Enum.map(1..n, fn i -> "bench_conc_#{i}" end)
    all_ids = ["bench_conc_hot" | spread_ids]
    Enum.each(all_ids, &warm_checkout_shard/1)

    try do
      %{
        spread_per_s: median(times(trials, fn _ -> churn(spread_ids, workers, ms) end)),
        same_shard_per_s:
          median(times(trials, fn _ -> churn(["bench_conc_hot"], workers, ms) end))
      }
    after
      # DRAIN, or this bench changes what every LATER bench measures. `all/1` runs them in one
      # BEAM, and leaving 65 coordinators resident made `fanout_kb_per_shard` read 4.09 against a
      # 3.77–4.09 band while the clean-tree baseline read 2.69 — the gate blocked at +52%. That is
      # precisely the harness-topology hazard AGENTS.md records from the last time
      # (`WriteCounter`/`FlushWatermark` in the shared `setup/1`, blocked at +46.5%): a new metric
      # must not silently redefine an existing series. Scope the residue to the metric that made it.
      Enum.each(all_ids, &Shards.drain(&1, 5_000))
    end
  end

  # Open once so the timed window measures CHECKOUT contention, not cold-open storage cost — the
  # thing cold_open/1 already owns and which would otherwise dominate and mask it.
  defp warm_checkout_shard(id) do
    {:ok, pid, ref, _path} = Shards.checkout(id)
    Fathom.Shard.checkin(pid, ref)
    id
  end

  defp churn(ids, workers, ms) do
    deadline = System.monotonic_time(:millisecond) + ms
    parent = self()

    pids =
      Enum.map(1..workers, fn w ->
        spawn_link(fn -> send(parent, {:done, self(), churn_loop(ids, w, deadline, 0)}) end)
      end)

    total =
      Enum.reduce(pids, 0, fn pid, acc ->
        receive do
          {:done, ^pid, count} -> acc + count
        after
          # Generous: the loop is deadline-bounded, so overshooting this means a worker WEDGED —
          # which is itself the regression this metric is for, and must not read as a fast zero.
          ms * 10 + 5_000 -> raise "concurrent checkout worker did not finish — wedged?"
        end
      end)

    if total == 0, do: raise("concurrent checkout completed 0 operations — measuring nothing")

    total * 1000 / ms
  end

  defp churn_loop(ids, w, deadline, count) do
    if System.monotonic_time(:millisecond) >= deadline do
      count
    else
      id = Enum.at(ids, rem(count + w, length(ids)))
      {:ok, pid, ref, _path} = Shards.checkout(id)
      Fathom.Shard.checkin(pid, ref)
      churn_loop(ids, w, deadline, count + 1)
    end
  end

  # GC EVERY process, not just this one. `:erlang.garbage_collect/0` collects the caller, which is
  # enough for a `:total` delta dominated by the caller's own heap — but refcounted binaries are
  # released only when the process HOLDING them collects, and here that is 64 coordinators and 64
  # connections. Measured self-GC-only, `:erlang.memory(:binary)` came back at **-2.6 KiB/shard**:
  # not a small number, a meaningless one, and a metric that can go negative makes the gate's
  # ratio nonsense (a move from -2.6 to -1 is a 62% "regression"). A metric this cheap to get
  # wrong is exactly the kind AGENTS.md says to distrust until it is proven to measure something.
  defp collect_all do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
  end

  defp close_served_shard({id, pid, ref, conn}) do
    Connection.close(conn)
    Fathom.Shard.checkin(pid, ref)
    Shards.drain(id, 5_000)
  end

  defp close_fanout_shard({id, pid, ref}) do
    Fathom.Shard.checkin(pid, ref)
    Shards.drain(id)
  end

  # --- setup ---------------------------------------------------------------

  defp setup(opts) do
    data = Keyword.get(opts, :data_dir, Path.join(tmp_dir(), "live"))
    store = Keyword.get(opts, :storage_dir, Path.join(tmp_dir(), "remote"))
    # Clean state per run: each metric self-seeds, so the harness owns its own
    # scratch and benchmark.sh never needs to know the tmp path. Only ever the
    # bench dirs — setup overrides :shard_data_dir, so real data is never touched.
    File.rm_rf!(data)
    File.rm_rf!(store)
    File.mkdir_p!(data)
    File.mkdir_p!(store)

    Application.put_env(:fathom, :shard_data_dir, data)
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: store)
    # No durability flush, no Postgres touch, no lazy migrate on the hot path.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :directory_touch, false)
    Application.put_env(:fathom, :lazy_migrate, false)

    ensure_started(Registry, keys: :unique, name: Fathom.ShardRegistry)
    ensure_started(DynamicSupervisor, name: Fathom.ShardSupervisor, strategy: :one_for_one)
    :ok
  end

  # The write path needs the dirty-flag tables; the READ metrics deliberately do not start them.
  #
  # Starting them in setup/1 changed what `fanout_kb_per_shard` measures — it counts
  # `:erlang.memory` per open shard, and each open shard gains a WriteCounter row and a
  # FlushWatermark row — which showed up as a +46% "regression" against a series measured
  # without them. That is a HARNESS topology change, and AGENTS.md is explicit that it makes
  # earlier entries incomparable. Scoping it to the flush metric keeps the fanout series intact.
  defp setup_write_path do
    ensure_started(Fathom.Shard.WriteCounter, [])
    ensure_started(Fathom.Admin.FlushWatermark, [])
    :ok
  end

  # Like setup/1 but points the storage backend at S3 (config from FATHOM_S3_TEST_*),
  # for the cold-open-from-S3 metric. Same hot-path knobs off.
  defp setup_s3(opts) do
    data = Keyword.get(opts, :data_dir, Path.join(tmp_dir(), "s3live"))
    File.rm_rf!(data)
    File.mkdir_p!(data)

    {:ok, _} = Application.ensure_all_started(:req)
    Application.put_env(:fathom, :shard_data_dir, data)
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.S3)
    Application.put_env(:fathom, Fathom.Shard.Storage.S3, s3_config_from_env())
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :directory_touch, false)
    Application.put_env(:fathom, :lazy_migrate, false)

    # The warm_s3 metric exists to exercise the dedicated S3 Finch pool's
    # ceiling, but `mix fathom.bench` doesn't boot the app supervision tree, so
    # the bench has to start the pool itself (sized from the S3 config above —
    # FATHOM_S3_TEST_POOL_SIZE lets the sweep vary it).
    {Finch, finch_opts} = Fathom.Shard.Storage.S3.finch_child_spec()
    ensure_started(Finch, finch_opts)

    ensure_started(Registry, keys: :unique, name: Fathom.ShardRegistry)
    ensure_started(DynamicSupervisor, name: Fathom.ShardSupervisor, strategy: :one_for_one)
    :ok
  end

  # The write path needs the dirty-flag tables; the READ metrics deliberately do not start them.
  #
  # Starting them in setup/1 changed what `fanout_kb_per_shard` measures — it counts
  # `:erlang.memory` per open shard, and each open shard gains a WriteCounter row and a
  # FlushWatermark row — which showed up as a +46% "regression" against a series measured
  # without them. That is a HARNESS topology change, and AGENTS.md is explicit that it makes
  # earlier entries incomparable. Scoping it to the flush metric keeps the fanout series intact.

  defp s3_config_from_env do
    base = [
      bucket: System.get_env("FATHOM_S3_TEST_BUCKET", "fathom-shards-test"),
      region: System.get_env("FATHOM_S3_TEST_REGION", "us-east-1"),
      endpoint: System.get_env("FATHOM_S3_TEST_ENDPOINT", "http://localhost:9100"),
      path_style: true,
      prefix: "bench/",
      access_key_id: System.get_env("FATHOM_S3_TEST_ACCESS_KEY", "fathomtest"),
      secret_access_key: System.get_env("FATHOM_S3_TEST_SECRET_KEY", "fathomtest123")
    ]

    # Unset → the storage module's defaults; set → sweep override. pool_size is
    # connections per pool, pool_count is the number of pools.
    base
    |> put_env_int(:pool_size, "FATHOM_S3_TEST_POOL_SIZE")
    |> put_env_int(:pool_count, "FATHOM_S3_TEST_POOL_COUNT")
  end

  defp put_env_int(config, key, env) do
    case System.get_env(env) do
      nil -> config
      val -> Keyword.put(config, key, String.to_integer(val))
    end
  end

  defp ensure_started(mod, opts) do
    case mod.start_link(opts) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp ensure_repo do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)

    case Fathom.Repo.start_link() do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, _reason} -> :error
    end
  rescue
    _ -> :error
  end

  defp directory_ready? do
    case Fathom.Repo.query("SELECT 1 FROM shards LIMIT 1") do
      {:ok, _} -> true
      _ -> false
    end
  rescue
    _ -> false
  end

  defp cleanup_resolve_row(id) do
    Fathom.Repo.query("DELETE FROM shards WHERE shard_id = $1", [id])
    :ok
  rescue
    _ -> :ok
  end

  # --- seeding -------------------------------------------------------------

  # Place a real shard DB in storage so the cold-open path actually pulls a file.
  defp seed_storage_shard(id) do
    path = Path.join(storage_dir(), "#{id}.db")
    drop_db(path)
    {:ok, conn} = Connection.open(path)
    Connection.exec(conn, "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)")
    Connection.exec(conn, "INSERT INTO t (v) VALUES ('seed')")
    Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
  end

  # --- helpers -------------------------------------------------------------

  defp times(n, fun), do: Enum.map(1..n, fun)

  defp p50(samples), do: percentile(samples, 50)

  defp percentile([], _), do: 0.0

  defp percentile(samples, pct) do
    sorted = Enum.sort(samples)
    idx = round(pct / 100 * (length(sorted) - 1))
    Enum.at(sorted, idx) / 1.0
  end

  defp median([]), do: 0.0

  defp median(values) do
    sorted = Enum.sort(values)
    n = length(sorted)
    mid = div(n, 2)

    if rem(n, 2) == 1 do
      Enum.at(sorted, mid) / 1.0
    else
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2.0
    end
  end

  defp drop_db(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "fathom_bench")
    File.mkdir_p!(dir)
    dir
  end

  defp storage_dir do
    Application.get_env(:fathom, Fathom.Shard.Storage.Local, [])[:dir] ||
      Path.join(tmp_dir(), "remote")
  end
end
