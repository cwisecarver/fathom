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
    :copy,
    :fanout,
    :hrana_rt,
    :wire_rows
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

    %{
      cold_open_p50_us: run_if(only, :cold_open, fn -> cold_open(opts) end),
      # The cold-S3 path — nil unless an S3 endpoint is configured (opt-in).
      cold_open_s3_p50_us: run_if(only, :cold_open_s3, fn -> cold_open_s3(opts) end),
      # Aggregate warming rate (shards/s) pulling many shards from S3 at once — opt-in.
      warm_s3_shards_per_s: run_if(only, :warm_s3, fn -> warm_s3_throughput(opts) end),
      # Failover RTO at real size: cold pull vs warm 304-promote (opt-in, S3).
      failover_cold_s3_p50_us: rto && rto.cold_us,
      failover_warm_s3_p50_us: rto && rto.warm_us,
      dir_resolve_p50_us: run_if(only, :dir_resolve, fn -> dir_resolve(opts) end),
      copy_keystone_rows_per_s: run_if(only, :copy, fn -> copy_throughput(opts) end),
      fanout_kb_per_shard: run_if(only, :fanout, fn -> fanout(opts) end),
      # No longer a placeholder (2026-07-31). Loopback wire software cost, not a network RTT.
      hrana_rt_us: run_if(only, :hrana_rt, fn -> hrana_rt(opts) end),
      wire_rows_per_s: run_if(only, :wire_rows, fn -> wire_rows(opts) end)
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
  def cold_open(opts \\ []) do
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
    |> p50()
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

      p50(us)
    end)
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
