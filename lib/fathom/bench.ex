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
  run over HTTP (`Filo.Client`, on the transitive `:mint`); row encoding is shared by both JSON
  transports, so the HTTP path covers it. The WebSocket path (django-libsql's) has its own
  watch-only metric, `hrana_ws_rt_us/1`, since `mint_web_socket` became a prod-compiled dep
  (expert review 2026-10-19 #12).

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
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Shard.Storage
  alias Fathom.ShardExecutor
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
  # Distinct wide keystone statements primed per served shard (expert review 2026-10-20-2027 #7a).
  # `served/1` and `served_repl/1` keep priming ONE statement (@served_stmts), so the gated
  # `served_kb_per_shard` series is unbroken. The statement cache (count-capped at 64, sized in
  # bytes by nothing) was barely populated by that and the gate was blind to its footprint, so the
  # NEW watch-only series `served_mix_kb_per_shard` primes @served_mix_stmts: 48 sits inside the
  # 64 cap on purpose - a realistic warm working set, not eviction churn. NEW SERIES 2026-10-20.
  @served_stmts 1
  @served_mix_stmts 48
  @concurrent_shards 64
  @concurrent_ms 1_000
  @recorder_rows 2_000
  @warm_size_kb 256
  # A2 promote-on-open failover arm: the shard holds @promote_rows, the stored object is stamped
  # holding only the first @promote_flush_after (so a survivor that cold-opened would be short),
  # and a local replica carries the rest. The gap is what promote-on-open recovers, and the row
  # count is the in-harness guard that the promotion actually fired.
  @promote_rows 20
  @promote_flush_after 10
  # How many shards `warm_s3_throughput/1` pulls concurrently. The name is legacy (the metric was
  # `warm_s3` when it shared harness bits with the retired WarmFollower); the measurement is and
  # always was concurrent S3-pull throughput, kept under its original series name for continuity.
  @warm_shards 200
  @hrana_rt_samples 200
  # hrana_ws_sparse_rt_us: few samples, each after an idle gap (expert review 2026-10-20 #25).
  @hrana_sparse_samples 20
  @hrana_sparse_gap_ms 200
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
    :fanout_gc,
    :served,
    :served_mix,
    :concurrent,
    :hrana_rt,
    :hrana_open_rt,
    :hrana_oneshot,
    :hrana_pooled,
    :hrana_pooled_pipeline,
    :wire_rows,
    :wire_server,
    :wire_encode,
    :flush,
    :served_repl,
    :hrana_write,
    :hrana_open_auth,
    :hrana_observed,
    :observability_tick,
    :flush_codec,
    :pull_decode,
    :s3_loopback,
    :hrana_ws,
    :hrana_ws_sparse,
    :same_shard_stmt,
    :serial_checkout,
    :hrana_wide,
    :cold_open_stub,
    :concurrent_cold_cycle,
    :hrana_sdk_write,
    :hrana_sdk_read,
    :served_django,
    :flush_vacuum,
    :drop_flush,
    :flush_stub,
    :hrana_django,
    :served_pooled,
    :idle,
    :repl_reopen,
    :flush_vacuum_32,
    :hrana_ws_connect,
    :retained_reopen
  ]

  @doc false
  # Public only so `mix fathom.bench` can be pinned against it — its `--only` allowlist is a
  # separate literal that silently drifted out of sync (see that module's @all_metrics).
  def known_metrics, do: @all_metrics

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
    served_mix = run_if(only, :served_mix, fn -> served_mix(opts) end)
    served_django = run_if(only, :served_django, fn -> served_django(opts) end)
    conc = run_if(only, :concurrent, fn -> concurrent_checkout(opts) end)
    cold = run_if(only, :cold_open, fn -> cold_open_stats(opts) end)
    rt = run_if(only, :hrana_rt, fn -> hrana_rt_stats(opts) end)

    # Replication-ON density (perf review 2026-10-14 #17). Runs in its OWN scratch topology (a peer
    # VM holding the followers, a Fleet started and stopped here), so it cannot change what any other
    # metric measures. See `served_repl/1`.
    repl = run_if(only, :served_repl, fn -> served_repl(opts) end)

    idle = run_if(only, :idle, fn -> idle_stats(opts) end)
    reopen = run_if(only, :repl_reopen, fn -> repl_first_commit_stats(opts) end)

    retained =
      run_if(only, :retained_reopen, fn -> Fathom.Bench.StorageIO.retained_reopen(opts) end)

    # One run, two metrics (HEAD and conditional PUT through the real S3 Finch pool, loopback stub).
    s3io = run_if(only, :s3_loopback, fn -> Fathom.Bench.StorageIO.s3_loopback(opts) end)

    %{
      cold_open_p50_us: cold && cold.p50_us,
      # The TAIL. Every other latency metric is a p50, so a change that leaves p50 flat and
      # doubles p99 — a blocking call back into the coordinator mailbox, twice now — scored ~0%.
      cold_open_p99_us: cold && cold.p99_us,
      # The cold-S3 path — nil unless an S3 endpoint is configured (opt-in).
      cold_open_s3_p50_us: run_if(only, :cold_open_s3, fn -> cold_open_s3(opts) end),
      # Concurrent S3-pull throughput (N shards pulled at once ÷ wall) — the node-startup / mass
      # failover number. Named `warm_s3` for series continuity; it never depended on WarmFollower.
      warm_s3_shards_per_s: run_if(only, :warm_s3, fn -> warm_s3_throughput(opts) end),
      # Failover RTO at real size, both opt-in (S3): the cold S3 body-pull baseline, and the A2
      # promote-on-open arm that serves a pre-positioned local replica instead. The promote arm
      # replaces the warm 304-promote arm removed 2026-09-14 with the WarmFollower retirement.
      failover_cold_s3_p50_us: rto && rto.cold_us,
      failover_promote_p50_us: rto && rto.promote_us,
      dir_resolve_p50_us: run_if(only, :dir_resolve, fn -> dir_resolve(opts) end),
      # The path per-checkout directory work ACTUALLY takes since the Recorder landed (#41.6);
      # dir_resolve above is control-plane only and was standing in for a per-request cost it no
      # longer represents.
      dir_recorder_flush_rows_per_s:
        run_if(only, :dir_recorder, fn -> dir_recorder_flush(opts) end),
      copy_keystone_rows_per_s: run_if(only, :copy, fn -> copy_throughput(opts) end),
      fanout_kb_per_shard: run_if(only, :fanout, fn -> fanout(opts) end),
      # The same sample with the coordinators collected. Paired with the line above it says WHICH
      # kind of change moved fanout: churn/size-class tip (this stays flat) vs a real retained-heap
      # regression (this moves too). Added 2026-08-10 after fanout false-blocked for two sessions.
      fanout_gc_kb_per_shard: run_if(only, :fanout_gc, fn -> fanout_gc(opts) end),
      # The SERVED regime (#41.2). fanout above holds NO connections, so it only ever measured
      # the ~16 KiB idle cost; this is the ~220 KiB one that actually binds a deployment.
      # `:binary` is split out because the statement cache's sub-binary pin lands there and not
      # in the process heap — one combined number would hide that whole class of regression.
      served_kb_per_shard: served && served.total_kb,
      served_binary_kb_per_shard: served && served.binary_kb,
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-22 r8 #1): live-BLOCK bytes (allocator-mode
      # independent), their SQLite (`driver_alloc`) slice, and the per-process sum. Read THESE, not
      # the carrier-granular total above, when judging a density change. See `memory_snapshot/0`.
      served_blocks_kb_per_shard: served && served.blocks_kb,
      served_sqlite_kb_per_shard: served && served.sqlite_kb,
      served_proc_kb_per_shard: served && served.proc_kb,
      # NEW SERIES (expert review 2026-10-20-2027 #7a): the same regime with 48 distinct wide
      # statements primed per shard, so the statement cache is populated. Watch-only.
      served_mix_kb_per_shard: served_mix && served_mix.total_kb,
      served_mix_binary_kb_per_shard: served_mix && served_mix.binary_kb,
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-21 #18a): the served regime on a Django-sized
      # 45-table / ~100-index schema; the keystone series above is unchanged.
      served_django_kb_per_shard: served_django && served_django.total_kb,
      served_django_binary_kb_per_shard: served_django && served_django.binary_kb,
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-21 r7 #1): the served regime with a pooled
      # idle handle that has been checked in and reused per shard (the checkin probe, the carried
      # statement cache), which no raw-connection density arm reaches.
      served_pooled_kb_per_shard: run_if(only, :served_pooled, fn -> served_pooled(opts) end),
      # WATCH-ONLY (perf review 2026-10-14 #17): the SERVED regime with A2 replication ON, which is
      # what prod runs. Every density number above was measured replication OFF. Primary side
      # (coordinator + connection + Session + shippers) and, separately, the FOLLOWER side per
      # replica, measured in a peer VM. See `served_repl/1`.
      served_repl_kb_per_shard: repl && repl.total_kb,
      follower_kb_per_shard: repl && repl.follower_kb,
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-21 #29): the replication Session's own memory
      # per shard (Process.info), the number `served_repl_kb_per_shard` cannot isolate.
      repl_session_kb_per_shard: repl && repl.session_kb,
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-22 r8 #1): the allocator-mode-independent
      # readings of the replicating served regime.
      served_repl_blocks_kb_per_shard: repl && repl.blocks_kb,
      served_repl_sqlite_kb_per_shard: repl && repl.sqlite_kb,
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
      # The same request as a real HTTP SDK sends it: open, execute AND close, so every sample
      # runs the executor's close (checkin, pool reset or close) that hrana_open_rt_us never
      # reaches (expert review 2026-10-01 perf #19). Watch-only until its variance is measured.
      hrana_oneshot_rt_us: run_if(only, :hrana_oneshot, fn -> hrana_oneshot_rt(opts) end),
      # The same one-shot request while ANOTHER stream holds the shard open, so each sample's open
      # is a pool HIT — the reuse path (`reset_for_reuse`, the temp-schema probe) no other metric
      # reaches (expert review 2026-10-01 perf #19/#24). Watch-only until its variance is measured.
      hrana_pooled_open_rt_us: run_if(only, :hrana_pooled, fn -> hrana_pooled_open_rt(opts) end),
      # A SERIAL five-statement one-shot on a pooled handle (perf review 2026-10-14 #4/#10): pool
      # hit through the drain grace AND statement-cache hits, both asserted. Watch-only.
      hrana_pooled_pipeline_rt_us:
        run_if(only, :hrana_pooled_pipeline, fn -> hrana_pooled_pipeline_rt_us(opts) end),
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-21 r7 #1): Django's request shape (per-
      # connection PRAGMA foreign_keys = ON, then a 29-column SELECT and a 3-way join) on a pooled
      # reused handle; the PRAGMA expires every carried cached statement.
      hrana_django_request_rt_us:
        run_if(only, :hrana_django, fn -> hrana_django_request_rt(opts) end),
      wire_rows_per_s: run_if(only, :wire_rows, fn -> wire_rows(opts) end),
      # The SERVER half of wire_rows_per_s (expert review 2026-10-20-2027 #4): no client, no decode.
      wire_server_rows_per_s: run_if(only, :wire_server, fn -> wire_server_rows(opts) end),
      # The OTHER encoder (#41.7). wire_rows_per_s drives encode_json/1; this drives encode/1,
      # the tagged-map builder Cursor and Protobuf reach, whose own moduledoc says the tagged-map
      # layer "roughly doubled" per-cell cost.
      wire_encode_rows_per_s: run_if(only, :wire_encode, fn -> wire_encode_rows(opts) end),
      # The write path. Before this, NOTHING in the harness executed a line of the durability
      # flush — setup/1 sets :shard_flush_interval_ms to 0 (#41.3).
      flush_p50_us: run_if(only, :flush, fn -> flush_p50(opts) end),
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-21 r7 #2): the other flush routes prod runs.
      # `flush_p50_us` above is the pinned-live route only. Each asserts its own route.
      flush_vacuum_p50_us: run_if(only, :flush_vacuum, fn -> flush_vacuum_p50(opts) end),
      flush_vacuum_32mb_p50_us:
        run_if(only, :flush_vacuum_32, fn -> flush_vacuum_32_p50(opts) end),
      drop_flush_p50_us: run_if(only, :drop_flush, fn -> drop_flush_p50(opts) end),
      flush_stub_p50_us:
        run_if(only, :flush_stub, fn -> Fathom.Bench.StorageIO.flush_stub(opts) end),
      # Placed AFTER every density metric on purpose: these two start helpers in their own scoped
      # setup (the dirty-flag tables, a Revocations server) that would change what a later
      # fanout/served reading measures. Keep them last, like flush_p50_us.
      #
      # NEW SERIES, GATED (expert review 2026-10-17 #8). The wire write with A2 replication ON, and
      # the open-per-request round trip with Hrana auth REQUIRED; before these the gate saw
      # neither a replicated write nor `HranaAuth.authorize/2`.
      hrana_write_rt_us: run_if(only, :hrana_write, fn -> hrana_write_rt(opts) end),
      hrana_open_auth_rt_us: run_if(only, :hrana_open_auth, fn -> hrana_open_auth_rt(opts) end),
      # WATCH-ONLY (expert review 2026-10-19 #26/#25), both with their OWN scoped setup and placed
      # after every density metric: the first starts the Prometheus reporter children (telemetry
      # handlers on the request path), the second seeds ~20k rows into the dirty-flag/load tables.
      hrana_rt_observed_us: run_if(only, :hrana_observed, fn -> hrana_rt_observed_us(opts) end),
      observability_tick_us:
        run_if(only, :observability_tick, fn -> observability_tick_us(opts) end),
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-22 r8 #2): the IDLE checked-in shard with prod's
      # per-shard ETS state and heartbeat mode on. Own scoped setup; see `idle_stats/1`. Placed here,
      # after every density metric, because it starts Lru/WriteCounter/FlushWatermark itself.
      idle_kb_per_shard: idle && idle.kb,
      idle_ets_b_per_shard: idle && idle.ets_b,
      idle_hibernated_frac: idle && idle.hibernated,
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-22 r8 #5): the first commit after a coordinator
      # reopen with A2 replication on, and the reset rounds it paid.
      repl_first_commit_after_reopen_us: reopen && reopen.p50_us,
      repl_first_commit_reset_rounds: reopen && reopen.resets,
      # NEW SERIES, WATCH-ONLY (r8 Q4=B, the baseline for #26): reopen of a retained local copy
      # (`SHARD_RETAIN_LOCAL_BYTES > 0`) against the real S3 client + loopback stub, and the S3
      # requests that reopen issued.
      retained_reopen_stub_p50_us: retained && retained.p50_us,
      retained_reopen_requests: retained && retained.requests,
      # WATCH-ONLY storage-IO metrics (expert review 2026-10-19 #7), each with its own scoped
      # setup in `Fathom.Bench.StorageIO`: a ~20 MB realistic snapshot through the flush codec.
      flush_codec_ms_per_mb:
        run_if(only, :flush_codec, fn -> Fathom.Bench.StorageIO.flush_codec_ms_per_mb(opts) end),
      # S3 download decode + verify through a loopback stub and the real Finch pool.
      pull_decode_ms_per_mb:
        run_if(only, :pull_decode, fn -> Fathom.Bench.StorageIO.pull_decode_ms_per_mb(opts) end),
      # The S3 client itself (expert review 2026-10-19 #3): no other gated metric goes through the
      # Req/Finch adapter, so the per-request Req deprecation IO.warn was invisible.
      s3_loopback_op_us: s3io && s3io.op_us,
      s3_loopback_put_us: s3io && s3io.put_us,
      # WATCH-ONLY (expert review 2026-10-19 #12): the WebSocket round trip, the path django-libsql
      # uses. Every other wire metric is HTTP.
      hrana_ws_rt_us: run_if(only, :hrana_ws, fn -> hrana_ws_rt_us(opts) end),
      # WATCH-ONLY (expert review 2026-10-22 r8 #4, Q7=A): a FRESH WebSocket per request, which is
      # what django-libsql does at Django's default CONN_MAX_AGE=0.
      hrana_ws_connect_rt_us:
        run_if(only, :hrana_ws_connect, fn -> hrana_ws_connect_rt_us(opts) end),
      # HOT-SHARD CONTENTION, watch-only (expert review 2026-10-19 #5). Last for the same reason as
      # the two above: each starts the ShardLoad/ShardLatency/Lru tables in its own scoped setup.
      # Many streams on ONE shard through the executor (the per-statement counter keys), and one
      # worker per shard so every checkin is a 1 -> 0 Lru edge.
      same_shard_stmt_per_s: run_if(only, :same_shard_stmt, fn -> same_shard_stmt(opts) end),
      serial_checkout_per_s: run_if(only, :serial_checkout, fn -> serial_checkout(opts) end),
      # WATCH-ONLY (expert review 2026-10-19 #10): an 8-column point read, the per-COLUMN encode
      # cost every other wire metric (one-cell SELECT 1) hides.
      hrana_wide_rt_us: run_if(only, :hrana_wide, fn -> hrana_wide_rt_us(opts) end),
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-21 #2), user-approved. The cold open and the
      # concurrent cold cycle against the REAL Storage.S3 client + Finch pool + coordinator over a
      # loopback S3Stub (with DELETE), instead of the Storage.Local double. Each owns its scoped
      # setup in `Fathom.Bench.StorageIO`, so the shared `setup/1` is unchanged.
      cold_open_stub_p50_us:
        run_if(only, :cold_open_stub, fn -> Fathom.Bench.StorageIO.cold_open_stub(opts) end),
      concurrent_cold_cycle_per_s:
        run_if(only, :concurrent_cold_cycle, fn ->
          Fathom.Bench.StorageIO.concurrent_cold_cycle(opts)
        end),
      # NEW SERIES, WATCH-ONLY (expert review 2026-10-21 #3): the libSQL Rust SDK's request shape
      # (describe before every statement, batch + get_autocommit + close, reads via /v3/cursor).
      hrana_sdk_write_rt_us: run_if(only, :hrana_sdk_write, fn -> hrana_sdk_rt(opts, :write) end),
      hrana_sdk_read_rt_us: run_if(only, :hrana_sdk_read, fn -> hrana_sdk_rt(opts, :read) end)
    }
    |> Map.merge(sparse_ws_metrics(only, opts))
  end

  # WATCH-ONLY (expert review 2026-10-20 #25): the same WS round trip but SPARSE (an idle gap
  # before every request), auth on, prod listener settings. Every other hrana_* metric is a hot
  # loop; sparse traffic measured ~10x the hot cost. One run yields three keys (expert review
  # 2026-10-20-2027 #25): the sparse p50, a raw-TCP host calibration after the SAME idle gap, and
  # their ratio - most of the sparse-vs-hot 6x is the host waking from idle, which the ratio
  # factors out. Merged last, as the entry used to be positioned.
  defp sparse_ws_metrics(only, opts) do
    s = run_if(only, :hrana_ws_sparse, fn -> hrana_ws_sparse_stats(opts) end)

    %{
      hrana_ws_sparse_rt_us: s && s.p50_us,
      hrana_ws_sparse_calibration_us: s && s.calib_us,
      hrana_ws_sparse_ratio: s && s.ratio
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

  @doc """
  Concurrent S3-pull throughput: pull `@warm_shards` shard objects from S3 at once and report
  `N ÷ wall` (shards/s). The node-startup / mass-failover number — `cold_open_s3` covers the
  single-pull LATENCY, this covers the aggregate THROUGHPUT of many pulls contending on the
  dedicated S3 Finch pool. `nil` without an S3 endpoint (opt-in, like `cold_open_s3/1`).

  The `warm_s3` name is legacy: it shared harness setup with the retired WarmFollower and was
  labeled "warm", but the measurement never touched that cache — it is plain concurrent
  `Shards.checkout/1` (each a cold pull). Kept under the original series name so the historical
  `warm_s3_shards_per_s` entries in perf_history.jsonl stay comparable.
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

  # Concurrently check out (cold-pull) every id. Finite timeout, not :infinity — one wedged pull
  # must not hang the whole sweep.
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

  # --- failover RTO: A2 promote-on-open vs cold (S3) -----------------------

  @doc """
  Failover RTO: median latency (µs) to open a shard on a survivor **cold** (pull the whole object
  from S3, today's baseline failover cost) vs **promote** (a local A2 replica is already on disk,
  so `Fathom.Shard.Replication.PromoteOnOpen` serves it instead of a body GET). Both at
  `:warm_size_kb`. Returns `%{cold_us, promote_us}` — `promote_us` is `nil` if the promote arm
  could not be exercised — or the whole thing is `nil` without an S3 endpoint (opt-in, like
  `cold_open_s3/1`).

  The honest delta: promote-on-open is **not** purely local — it still takes a pre-promotion
  snapshot, reads the object head, and does a fenced PUT of the promoted bytes. The win over cold
  is the object **body transfer** (the S3 GET) avoided, so it grows with shard size and
  bandwidth-delay and is marginal for a tiny shard on a fast link. Against localhost MinIO this
  measures protocol + loopback; inject latency/bandwidth (`scripts/benchmark_s3_latency.sh`) or
  point `FATHOM_S3_TEST_ENDPOINT` at in-region S3 for the production gap.

  Replaces the removed warm-follower arm (`failover_warm_s3_p50_us`, retired 2026-09-14): A2
  promote-on-open is the mechanism that now shortens failover by pre-positioning bytes. The promote
  sample asserts the promotion **actually fired** (the served row count must include the rows only
  the replica held); a silent cold-open fallback raises rather than being timed and mislabeled.

  Scope caveat: like `PromoteOnOpenTest`'s fixtures, this seeds the replica strictly ahead of the
  object's own stamp, so it is a "promotion-path latency" number — it does not itself re-prove the
  lineage-vs-lock-epoch wire ranking. That ranking IS proven (the review-#12 gap is closed): see
  `ownership_cycle_position_test.exs` "a real seeded replica ranks on the same counter as the
  object's stamp", which drives a real lineage across the wire.
  """
  @spec failover_rto(keyword()) :: %{cold_us: float(), promote_us: float() | nil} | nil
  def failover_rto(opts \\ []) do
    if s3_opt_in?() do
      setup_s3(opts)
      samples = Keyword.get(opts, :failover_samples, @cold_open_s3_samples)
      size_kb = Keyword.get(opts, :warm_size_kb, @warm_size_kb)

      # The clock-free steal watch (Jepsen finding 3, 2026-10-24) is a fixed ttl + margin wait from
      # the first look, not work; this arm times the takeover's S3 work, so it runs with the watch off.
      prev_watch = Application.get_env(:fathom, :steal_watch_ms)
      Application.put_env(:fathom, :steal_watch_ms, 0)

      try do
        # Warm-up doubles as a reachability probe (raises → degrade to nil).
        probe = uniq("bench_rto_probe")
        seed_s3_sized(probe, size_kb)
        teardown_open(probe, open_and_query(probe))

        # Cold: every sample pulls the whole object from S3. A real failover STEALS the dead
        # owner's lease, which touches (etag-rotates) the object — seed the dead-owner lock so the
        # measurement takes that path (#15).
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

        %{cold_us: cold, promote_us: promote_rto(samples, size_kb)}
      rescue
        e ->
          Logger.warning("failover_rto skipped: S3 unreachable/misconfigured (#{inspect(e)})")
          nil
      catch
        :exit, reason ->
          Logger.warning("failover_rto skipped: #{inspect(reason)}")
          nil
      after
        restore_env(:steal_watch_ms, prev_watch)
      end
    else
      nil
    end
  end

  # The A2 promote-on-open arm. Degrades to nil (never a fake number) if the promote path cannot be
  # exercised — the row-count guard inside promote_open_sample/3 turns a silent cold-open fallback
  # into a raise, which is caught here and reported as "not measured". The bench_test.exs @tag :bench
  # test is what asserts the mechanism actually fires on every run (it runs on Local storage, so it
  # works without an S3 endpoint); this arm carries the real-size S3 number when a rig provides one.
  defp promote_rto(samples, size_kb) do
    replica_dir = Path.join(tmp_dir(), "bench_replica_#{System.unique_integer([:positive])}")
    File.mkdir_p!(replica_dir)
    ensure_started(Follower, name: Follower, port: 0, dir: replica_dir)
    setup_write_path()

    prev_gate = Application.get_env(:fathom, :replication_promote_on_open)
    prev_recover = Application.get_env(:fathom, :replication_recover_from_peers)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    # Force the local promote path (no peer sockets); there is no fleet here to ask.
    Application.put_env(:fathom, :replication_recover_from_peers, false)
    blob_bytes = div(size_kb * 1024, @promote_rows)

    try do
      times(samples, fn _ ->
        promote_open_sample(@promote_rows, @promote_flush_after, blob_bytes)
      end)
      |> p50()
    rescue
      e ->
        Logger.warning("failover_rto promote arm skipped: #{inspect(e)}")
        nil
    catch
      :exit, reason ->
        Logger.warning("failover_rto promote arm skipped: #{inspect(reason)}")
        nil
    after
      restore_env(:replication_promote_on_open, prev_gate)
      restore_env(:replication_recover_from_peers, prev_recover)
    end
  end

  @doc false
  # One promote-on-open failover sample: a shard whose stored object is stamped holding only
  # `flush_after` rows while a local A2 replica carries all `total`. Kill the primary, time the
  # survivor open (which runs `maybe_promote_replica/7`), and RAISE unless the promotion fired —
  # the served row count must be the full `total`, or a silent cold open was measured. Public so the
  # @tag :bench verification test drives the exact same code the S3 arm does. Requires a
  # default-named `Follower` already running and `:replication_promote_on_open` true.
  @spec promote_open_sample(pos_integer(), pos_integer(), non_neg_integer()) :: non_neg_integer()
  def promote_open_sample(total, flush_after, blob_bytes) do
    id = uniq("bench_promote")
    {handle, coordinator} = build_stamped_shard(id, total, flush_after, blob_bytes)
    {:ok, stamp} = Storage.object_position(id)

    install_bench_replica(id, %{
      epoch: stamp.epoch,
      wal_gen: stamp.wal_gen,
      wal_ordinal: Map.get(stamp, :wal_ordinal, 0),
      offset: stamp.offset + 1
    })

    tear_down_bench_primary(id, handle, coordinator)

    {us, served} = :timer.tc(fn -> open_count(id) end)
    Shards.drain(id)

    unless served == total do
      raise "promote bench measured a cold open: served #{served}/#{total} rows, promotion did " <>
              "not fire (unstamped object, or replica not ranked ahead)"
    end

    us
  end

  # Build a shard through the coordinator write path, flushing at `flush_after` so the STORED OBJECT
  # is stamped holding only those rows, then write the rest so the live shard (and the replica taken
  # from it) is ahead. `assign_ordinal!/2` is the step a replicating Session normally does — without
  # it the object stamp carries no WAL ordinal and `Promote.fresher?/2` refuses to rank any replica.
  defp build_stamped_shard(id, total, flush_after, blob_bytes) do
    {:ok, coordinator} = Shards.ensure(id)
    {:ok, handle} = ShardExecutor.open(id)

    {:ok, _} =
      ShardExecutor.execute(handle, %Filo.Stmt{
        sql: "CREATE TABLE t (a INTEGER PRIMARY KEY, data BLOB)",
        args: []
      })

    for i <- 1..total do
      {:ok, _} =
        ShardExecutor.execute(handle, %Filo.Stmt{
          sql: "INSERT INTO t VALUES (?1, randomblob(?2))",
          args: [i, blob_bytes]
        })

      if i == flush_after do
        _ = assign_ordinal!(id, coordinator)
        flush_settle(coordinator)
      end
    end

    {handle, coordinator}
  end

  # Read the current WAL salt and have the coordinator assign the ordinal for it — the stamp the
  # flush will carry. Called immediately before the flush: a checkpoint restarts the log with fresh
  # salts, so a salt read any earlier names a WAL the flush will not see.
  defp assign_ordinal!(id, coordinator) do
    wal = Fathom.Shard.db_path(id) <> "-wal"
    {:ok, %{salt1: salt}} = Wal.read(wal)
    n = Fathom.Shard.wal_ordinal(coordinator, salt)

    if n <= 0 do
      raise "bench: the coordinator assigned no WAL ordinal; the object stamp cannot be ranked"
    end

    n
  end

  defp flush_settle(coordinator) do
    send(coordinator, :durability_flush)
    settle_flush(coordinator, 400)
  end

  defp settle_flush(_c, 0), do: raise("bench: durability flush task never settled")

  defp settle_flush(c, tries) do
    if :sys.get_state(c).flush_task == nil do
      :ok
    else
      Process.sleep(10)
      settle_flush(c, tries - 1)
    end
  end

  # Copy the live shard's own bytes into the follower dir and seed its position — a real replica,
  # not a fabrication (the WAL offsets refer to the primary's WAL, as shipped frames would leave
  # them). Lock epoch pinned at 1; the ranking field is the lineage (position.epoch) + ordinal.
  defp install_bench_replica(id, position) do
    path = Fathom.Shard.db_path(id)
    File.cp!(path, Follower.db_path(Follower, id))

    case File.stat(path <> "-wal") do
      {:ok, %{size: size}} when size > 0 ->
        File.cp!(path <> "-wal", Follower.wal_path(Follower, id))

      _ ->
        File.write!(Follower.wal_path(Follower, id), "")
    end

    Follower.seed(Follower, id, 1, position.wal_gen, 0, position.offset, position.epoch)
    :ok = Follower.note_ordinal(Follower, id, Map.get(position, :wal_ordinal, 0))
  end

  # Node loss, not a shutdown: kill the coordinator (a graceful stop would drop-flush every row to
  # the object and make the promote guard vacuous) and remove the dead node's local files, leaving
  # only the object and the replica.
  defp tear_down_bench_primary(id, handle, coordinator) do
    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)

    receive do
      {:DOWN, ^ref, :process, ^coordinator, _} -> :ok
    after
      2_000 -> raise "bench: the coordinator did not die"
    end

    _ =
      try do
        ShardExecutor.close(handle)
      catch
        :exit, _ -> :ok
      end

    for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    :ok
  end

  # The survivor open — runs the coordinator open path where promote-on-open fires — returning the
  # served row count so the caller can prove the promotion happened.
  defp open_count(id) do
    {:ok, handle} = ShardExecutor.open(id)

    {:ok, %{rows: [[n]]}} =
      ShardExecutor.execute(handle, %Filo.Stmt{sql: "SELECT count(*) FROM t", args: []})

    :ok = ShardExecutor.close(handle)
    n
  end

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, val), do: Application.put_env(:fathom, key, val)

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

  # `dir_recorder_flush_rows_per_s` — rows/sec through `Fathom.Directory.Recorder.flush/0`
  # (expert review 2026-08-01 #41.6).
  #
  # **`dir_resolve_p50_us` is not the request path and has not been since the Recorder landed.**
  # `Directory.resolve/1`'s live callers are `Fathom.Tenants` (provision, fork) and
  # `Migrator.ShardMigration` — provisioning and migration, all control-plane. It stays gated
  # because it still guards that reader, but it was standing in for a per-request cost it no
  # longer represents, and `docs/benchmark-plan.md` said so in as many words until this commit.
  #
  # What per-checkout directory work actually costs now is THIS: touches coalesce into ETS and are
  # batch-flushed, so the live cost is an `:ets.select` walk plus one `:ets.take` per touched
  # shard, then `insert_all` in 1,000-row chunks — and it scales with DENSITY. At 30k active
  # shards that is ~30k ETS takes and ~30 multi-row upserts every second against a `pool_size` of
  # 25, which is the shape that falls over quietly.
  @spec dir_recorder_flush(keyword()) :: float() | nil
  defp dir_recorder_flush(opts) do
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
          buffered = :ets.info(Fathom.Directory.Recorder, :size)
          {us, _written} = :timer.tc(fn -> Fathom.Directory.Recorder.flush() end)
          left = :ets.info(Fathom.Directory.Recorder, :size)

          # The whole point is the batch upsert; a flush that drained nothing would report an
          # enormous rows/sec for work that never happened. Checked on the BUFFER, not on
          # flush/0's rows-written: since the #22 write guard (perf review 2026-10-01), trials
          # after the first re-touch rows inside the granularity window, which Postgres skips —
          # the steady-state path at density, and the one worth timing — so they write ~0 rows.
          if buffered < n or left != 0 do
            raise "recorder flush drained #{buffered - left}/#{n} touches — measuring nothing"
          end

          n / (us / 1_000_000)
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

  # `hrana_rt/1`'s samples reduced to BOTH p50 and p99 — see `cold_open_stats/1` for why the tail
  # is gated at all (expert review 2026-08-01 #41.5). This is the wire's tail: a request-path
  # change that adds an occasional stall shows up here and nowhere else in the gate.
  @spec hrana_rt_stats(keyword()) :: %{p50_us: float(), p99_us: float()} | nil
  defp hrana_rt_stats(opts) do
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

      assert_load_recorded!("benchrt", "hrana_rt_us")
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

  ## Pooling the SQLite handle was measured and REJECTED (expert review 2026-08-26 #11)

  That review claimed "opening a fresh SQLite handle per stream is ~98% of a fresh-stream
  request's cost", from a probe reading 459 µs for open→query→close against 9 µs on a reused
  connection, and predicted that pooling would pull this metric "toward ~130–150 µs".

  Measured with a throwaway per-`{shard_id, scope}` handle cache in `ShardExecutor.do_open/3`
  (every reset guard skipped, so an upper bound on what a real pool could reach):

      pooled     hrana_open_rt_us  351 / 314 / 295      hrana_rt_us  83 / 90 / 90
      unpooled   hrana_open_rt_us  373 / 340 / 334      hrana_rt_us  139 / 126 / 130

  Nowhere near 130–150. And `hrana_rt_us` moved in the SAME direction by a similar proportion,
  which by AGENTS.md's own rule ("when two unrelated metrics move together in one run, it is the
  machine") means most of even that gap is contention, not the pool.

  What the original probe actually measured was `Connection.open/2` in isolation, and part of it
  was the `File.mkdir_p!` review #10 has since removed — this metric fell 433 → 339 µs on that
  one-line change.

  The residual, though, is NOT the Filo stream open — an earlier draft of this note said it was
  (review dsv41f.perf 2026-09-16 corrected it). It is dominated by SQLite's per-open **WAL-index
  (`-shm`) setup**: SQLite builds and maps the `-shm` on the FIRST statement of a connection and
  unlinks it on the LAST close (`Fathom.Shard`'s snapshot path documents the unlink side). A
  one-query Hrana stream is the shard's ONLY connection, so the previous close unlinked `-shm` and
  the next open recreates it. Re-probed 2026-09-16 (raw `Exqlite.Sqlite3`, WAL, MIN of 400 samples —
  the min filters the load-10-12 noise and held at 230/224/222 µs across three interleaved runs, so
  it is trustworthy despite the loaded box): a bare open + first statement + close costs ~230 µs
  with the shard's only connection vs ~54 µs when a second handle keeps `-shm` mapped — **~76% of
  the bare open is the WAL-index creation** (~176 µs, roughly half of `hrana_open_rt_us`). It is
  also the flat `L=1` per-stream-open ceiling in `docs/benchmark-plan.md`. (The review's ~50–58%
  figure is the same cost as a fraction of the fuller `Connection.open/2`, which also pays the
  extension load and the pragmas.)

  This DOES bear on pooling, against what the paragraph above concludes. A real pool that keeps the
  handle (and therefore `-shm`) alive removes that ~176 µs — so the 2026-08-26 #11 A/B (pooled ≈
  unpooled) most likely never hit its cache: a process-local cache in a per-request stream process
  misses across the HTTP request→stream hop (see the side-channel note in `Fathom.ShardExecutor`).
  **Still do not build the pool on this note alone.** A persistent handle carries the per-connection
  page cache (the ~2 MiB tail review 2026-07-24 #29 bounded), trading the `-shm` setup for the
  density ceiling `served_kb_per_shard` exists to protect — the win is real and past the 20% gate,
  but it has to be measured against that density cost, with the four reset guards (autocommit check,
  statement purge, dropping the per-connection process-dictionary keys, re-applying
  `@tenant_pragma_allow` pragmas, plus separate `:ro`/`:rw` pools) as the floor of the work, not the
  whole of it.
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
  `hrana_oneshot_rt_us` — median round trip of a ONE-SHOT stream: `[execute, close]` in one
  pipeline with no baton, which is exactly what an HTTP SDK (and django-libsql under Django's
  default `CONN_MAX_AGE=0`) sends per request.

  Why it exists next to `hrana_open_rt_us` (expert review 2026-10-01 perf #19): that metric sends
  `execute` only and drops the baton, so its streams are never closed. A run accumulates ~200
  live streams (and SQLite handles) on one shard until Filo's idle timeout reaps them, which
  means it (a) never runs `ShardExecutor.close/1` — no checkin, no pool reset, no
  `release_owner_state` — and (b) always measures the regime where another handle keeps `-shm`
  mapped. Prod pays both. This metric asserts its own precondition: if streams accumulate, it is
  measuring the wrong thing and raises.
  """
  @spec hrana_oneshot_rt_us(keyword()) :: float() | nil
  def hrana_oneshot_rt_us(opts \\ []), do: hrana_oneshot_rt(opts)

  defp hrana_oneshot_rt(opts) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    requests = [
      %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1", "args" => []}},
      %{"type" => "close"}
    ]

    with_wire(opts, "benchoneshot", fn client ->
      # Warm the SHARD, then close that stream too so the timed loop starts from zero streams.
      {:ok, _, client} = Filo.Client.pipeline(client, requests)

      {_client, us} =
        Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
          c = %{c | baton: nil}
          {t, result} = :timer.tc(fn -> Filo.Client.pipeline(c, requests) end)
          {:ok, [%{"type" => "ok"}, %{"type" => "ok"}], c} = result
          {c, [t | acc]}
        end)

      live = Registry.count(Module.concat(Fathom.Bench.WireStreams, Registry))

      unless live <= 1,
        do: raise("hrana_oneshot_rt_us left #{live} live streams; it is not closing them")

      p50(us)
    end)
  end

  @doc """
  `hrana_sdk_write_rt_us` / `hrana_sdk_read_rt_us` - median round trip of a statement sent EXACTLY as
  the libSQL Rust SDK over HTTP (`libsql-experimental` 0.0.55, libsql crate 0.9.10) sends it, replayed
  from a capture against `Filo.Plug` (expert review 2026-10-21 #3; fixtures: panel 2 `p2/probe_out3.log`).
  No other metric sends this shape, and it is unlike `hrana_oneshot_rt_us`: the SDK describes the
  statement first, and the describe pipeline carries no `close`, so the inline-oneshot fast path never
  applies.

    * WRITE (`hrana_sdk_write_rt_us`): two HTTP round trips on a fresh stream -
      `POST /v3/pipeline [describe]` (no baton, opens a stream), then
      `POST /v3/pipeline [batch(1 step, condition null), get_autocommit, close]` on the returned baton.
      The describe text lacks the trailing `;` that the executed text carries, as the SDK sends it.
    * READ (`hrana_sdk_read_rt_us`): a HELD stream; per statement `[describe]` then `POST /v3/cursor`
      (a chunked NDJSON body) with a one-step batch.

  Real Bandit + `Filo.Plug` listener with the connection pool forced on (as prod) and the
  `Filo.Client` Mint transport; the cursor request is sent through the client's own transport because
  `Filo.Client` has no cursor sugar. WATCH-ONLY, new series. Compare `hrana_sdk_write_rt_us` with
  `hrana_oneshot_rt_us`: if it is ~2x and a later describe-cache change moves it by less than the
  noise band, the SDK path is RTT-bound and that change is hygiene.

  Preconditions (raises otherwise): the describe was served (`describe` response with the right
  parameter count), the batch step had no condition and no step error, the write affected exactly one
  row / the read returned its row in a `row` cursor entry, `get_autocommit` and `close` returned ok,
  the pool served at least half the write streams from a pooled handle, and no stream is left live.
  """
  @spec hrana_sdk_write_rt_us(keyword()) :: float() | nil
  def hrana_sdk_write_rt_us(opts \\ []), do: hrana_sdk_rt(opts, :write)

  @doc "The READ half of `hrana_sdk_write_rt_us/1`: describe + `/v3/cursor` on a held stream."
  @spec hrana_sdk_read_rt_us(keyword()) :: float() | nil
  def hrana_sdk_read_rt_us(opts \\ []), do: hrana_sdk_rt(opts, :read)

  @sdk_insert "INSERT INTO sdk_kv VALUES (?, ?)"
  @sdk_select "SELECT v FROM sdk_kv WHERE k = ?"

  defp hrana_sdk_rt(opts, mode) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)
    label = "hrana_sdk_#{mode}_rt_us"
    prev_pool = Application.get_env(:fathom, :connection_pool)
    Application.put_env(:fathom, :connection_pool, true)
    # This arm writes THROUGH THE WIRE, and the prod-compiled bench has replication configured on
    # but none of its processes started, so every write raised `unknown registry: ...SessionRegistry`
    # and failed the whole run (first prod-compiled run of this arm, 2026-10-22). The arm measures
    # the SDK's request shape, not the replication wait (hrana_write_rt_us owns that), so it runs
    # with replication off for its duration.
    prev_repl = Application.get_env(:fathom, :replication_enabled)
    Application.put_env(:fathom, :replication_enabled, false)
    hits = :counters.new(2, [])
    handler = "bench-sdk-#{mode}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :pool_take],
      fn _event, _m, %{result: result}, _ ->
        :counters.add(hits, if(result == :hit, do: 1, else: 2), 1)
      end,
      nil
    )

    try do
      with_wire(
        opts,
        "benchsdk#{mode}",
        fn client ->
          {:ok, _, client} =
            Filo.Client.execute(
              client,
              "CREATE TABLE IF NOT EXISTS sdk_kv (k INTEGER PRIMARY KEY, v TEXT)"
            )

          {:ok, _, client} =
            Filo.Client.execute(client, "INSERT OR REPLACE INTO sdk_kv VALUES (1, 'one')")

          {:ok, _, client} = Filo.Client.pipeline(client, [%{"type" => "close"}])
          client = %{client | baton: nil}

          p50(sdk_samples(mode, client, samples, label, hits))
        end,
        [],
        version: "v3"
      )
    after
      :telemetry.detach(handler)
      restore_env(:replication_enabled, prev_repl)

      if is_nil(prev_pool),
        do: Application.delete_env(:fathom, :connection_pool),
        else: Application.put_env(:fathom, :connection_pool, prev_pool)
    end
  end

  defp sdk_samples(:write, client, samples, label, hits) do
    # Warm: opens the shard and puts a handle in the pool. Never timed.
    {_, parts, client} = sdk_write(client, 2_000_000)
    sdk_check_write!(parts, label)
    :counters.put(hits, 1, 0)
    :counters.put(hits, 2, 0)

    {_client, us} =
      Enum.reduce(1..samples, {client, []}, fn i, {c, acc} ->
        {t, parts, c} = sdk_write(c, 2_000_000 + i)
        sdk_check_write!(parts, label)
        {c, [t | acc]}
      end)

    hit = :counters.get(hits, 1)

    unless hit * 2 >= samples,
      do:
        raise(
          "#{label} got #{hit} pool hits in #{samples} samples; the SDK stream is not " <>
            "reusing a pooled handle, so this is not the prod regime"
        )

    sdk_assert_no_live_streams!(label)
    us
  end

  defp sdk_samples(:read, client, samples, label, _hits) do
    # The SDK holds one stream open for its reads: a describe opens it, the cursor keeps it.
    {_, parts, client} = sdk_read(client)
    sdk_check_read!(parts, label)

    {client, us} =
      Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
        {t, parts, c} = sdk_read(c)
        sdk_check_read!(parts, label)
        {c, [t | acc]}
      end)

    {:ok, [%{"type" => "ok"}], _} = Filo.Client.pipeline(client, [%{"type" => "close"}])
    sdk_assert_no_live_streams!(label)
    us
  end

  # One SDK write: describe on a fresh stream, then batch + get_autocommit + close on its baton.
  # Returns the timed span (both round trips) and the raw replies for the precondition checks,
  # which run OUTSIDE the timed window.
  defp sdk_write(c, key) do
    c = %{c | baton: nil}

    {t, {d, b}} =
      :timer.tc(fn ->
        {:ok, d, c1} = Filo.Client.pipeline(c, [sdk_describe(@sdk_insert)])

        {:ok, b, _} =
          Filo.Client.pipeline(c1, [
            sdk_batch(@sdk_insert <> ";", [key, "x"]),
            %{"type" => "get_autocommit"},
            %{"type" => "close"}
          ])

        {d, b}
      end)

    {t, {d, b}, c}
  end

  # One SDK read on the held stream: describe, then /v3/cursor.
  defp sdk_read(c) do
    {t, {d, raw, c2}} =
      :timer.tc(fn ->
        {:ok, d, c1} = Filo.Client.pipeline(c, [sdk_describe(@sdk_select)])
        {raw, c2} = sdk_cursor(c1, sdk_batch(@sdk_select <> ";", [1], false))
        {d, raw, c2}
      end)

    {t, {d, raw}, c2}
  end

  defp sdk_describe(sql),
    do: %{"type" => "describe", "sql" => sql, "sql_id" => nil, "replication_index" => nil}

  # The exact step the SDK sends: `condition: null`, `want_rows: true`, args typed. With
  # `wrap? = true` it is a pipeline `batch` request, else the bare batch the cursor endpoint takes.
  defp sdk_batch(sql, args, wrap? \\ true) do
    batch = %{
      "steps" => [
        %{
          "condition" => nil,
          "stmt" => %{
            "sql" => sql,
            "sql_id" => nil,
            "args" => Enum.map(args, &Filo.Value.encode/1),
            "named_args" => [],
            "want_rows" => true,
            "replication_index" => nil
          }
        }
      ],
      "replication_index" => nil
    }

    if wrap?, do: %{"type" => "batch", "batch" => batch}, else: batch
  end

  # `Filo.Client` has no cursor sugar, so the request goes through its own transport. The response
  # is NDJSON: line 1 `{"baton": ..., "base_url": null}`, then step_begin / row / step_end entries.
  defp sdk_cursor(c, batch) do
    body = Jason.encode_to_iodata!(%{"baton" => c.baton, "batch" => batch})
    headers = [{"host", c.authority} | c.headers]

    {:ok, %{status: 200, body: raw}, state} =
      c.transport.request(c.transport_state, "POST", "/v3/cursor", headers, body)

    [first | _] = String.split(raw, "\n", parts: 2)
    {raw, %{c | transport_state: state, baton: Jason.decode!(first)["baton"]}}
  end

  defp sdk_check_write!({d, b}, label) do
    with [%{"type" => "ok", "response" => %{"type" => "describe", "result" => dr}}] <- d,
         2 <- length(dr["params"] || []),
         [
           %{"type" => "ok", "response" => %{"type" => "batch", "result" => br}},
           %{
             "type" => "ok",
             "response" => %{"type" => "get_autocommit", "is_autocommit" => true}
           },
           %{"type" => "ok", "response" => %{"type" => "close"}}
         ] <- b,
         [nil] <- br["step_errors"],
         [%{"affected_row_count" => 1}] <- br["step_results"] do
      :ok
    else
      other ->
        raise "#{label}: the SDK write shape was not served as expected (#{inspect({d, b})}; #{inspect(other)})"
    end
  end

  defp sdk_check_read!({d, raw}, label) do
    served? =
      match?([%{"type" => "ok", "response" => %{"type" => "describe"}}], d) and
        String.contains?(raw, ~s("type":"row")) and String.contains?(raw, "step_end")

    unless served?,
      do: raise("#{label}: describe or the /v3/cursor row was not served (#{inspect({d, raw})})")

    :ok
  end

  defp sdk_assert_no_live_streams!(label) do
    live = Registry.count(Module.concat(Fathom.Bench.WireStreams, Registry))
    unless live <= 1, do: raise("#{label} left #{live} live streams; it is not closing them")
    :ok
  end

  @doc """
  `hrana_ws_rt_us` — median µs of a warm-stream `SELECT 1` round trip over the Hrana WEBSOCKET
  path (expert review 2026-10-19 #12): `Fathom.Bench.HranaClient` (Mint.WebSocket) → `Filo.Socket`
  → `ShardExecutor` → the shard → back. This is the path django-libsql, the primary prod client,
  uses; every other wire metric here is HTTP, so WS framing, `Filo.Socket` routing and the
  `validate_text_frames` check were gated by nothing. Watch-only.

  Precondition: every reply must be the single row `[[1]]`, so a client that silently stopped
  executing (and just echoed) cannot produce a number.
  """
  @spec hrana_ws_rt_us(keyword()) :: float() | nil
  def hrana_ws_rt_us(opts \\ []) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)
    shard = "benchwsrt"

    setup(opts)
    Application.put_env(:fathom, :shard_base_domain, nil)
    Application.put_env(:fathom, :default_shard, nil)
    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Application.ensure_all_started(:mint)

    # Production's listener settings (expert review 2026-10-20 #25). SERIES BREAK: until 2026-10-20 this
    # ran Bandit's defaults (validate_text_frames: true, no hibernate_after), which prod does not;
    # earlier hrana_ws_rt_us entries are not comparable.
    {:ok, sup, port} = Fathom.Bench.HranaClient.start_listener(prod: true)

    try do
      {:ok, client} = Fathom.Bench.HranaClient.connect(port, shard)
      # Warm the shard and the stream; not timed.
      {:ok, client, _} = Fathom.Bench.HranaClient.execute(client, "SELECT 1")

      {client, us} =
        Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
          {t, {:ok, c, %{rows: [[1]]}}} =
            :timer.tc(fn -> Fathom.Bench.HranaClient.execute(c, "SELECT 1") end)

          {c, [t | acc]}
        end)

      Fathom.Bench.HranaClient.close(client)
      p50(us)
    after
      Fathom.Bench.HranaClient.stop_listener(sup)
      Fathom.Shards.drain(shard, 5_000)
    end
  end

  @hrana_ws_connect_samples 100

  @doc """
  `hrana_ws_connect_rt_us` - median µs of one request that opens a FRESH WebSocket: connect,
  upgrade, `hello` (which runs the auth callback), `open_stream`, one `SELECT 1`, close (expert
  review 2026-10-22 r8 #4, user decision Q7=A). django-libsql speaks WebSocket only, and Django's
  default `CONN_MAX_AGE=0` opens a connection per request, so this is the primary client's default
  request shape. `hrana_django_request_rt_us` is the HTTP-SDK one-shot over keep-alive and costs
  ~6x less (probe `ws_connect2.exs`, loopback: persistent WS execute 69 us, fresh WS per request
  617 us, HTTP one-shot 105 us), so ~510 us/request of protocol cost was unmeasured. In prod the
  LB adds an upstream connect per WebSocket on top.

  Prod listener settings, Hrana auth REQUIRED, connection pool on (as prod). Loopback: the
  software cost, not a network RTT.

  Preconditions (raises otherwise): every reply is `[[1]]`, the auth callback ran exactly once per
  connection (warm-up plus every sample), a missing token is refused, and the pool served at least
  half the stream opens as a hit (REPORTED to stderr). WATCH-ONLY (new series).
  """
  @spec hrana_ws_connect_rt_us(keyword()) :: float()
  def hrana_ws_connect_rt_us(opts \\ []) do
    alias Fathom.Bench.HranaClient
    alias Fathom.HranaAuth
    alias Fathom.HranaAuth.Revocations

    samples = Keyword.get(opts, :hrana_ws_connect_samples, @hrana_ws_connect_samples)
    shard = "benchwsconnect"
    keys = [:hrana_auth, :hrana_token_max_age, :hrana_token_secret, :connection_pool]
    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})

    setup(opts)
    Application.put_env(:fathom, :shard_base_domain, nil)
    Application.put_env(:fathom, :default_shard, nil)
    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Application.ensure_all_started(:mint)
    {:ok, _} = Application.ensure_all_started(:telemetry)

    Application.put_env(:fathom, :hrana_auth, :required)
    Application.put_env(:fathom, :hrana_token_max_age, 3_600)
    Application.put_env(:fathom, :connection_pool, true)

    if Application.get_env(:fathom, :hrana_token_secret) in [nil, ""],
      do: Application.put_env(:fathom, :hrana_token_secret, String.duplicate("a", 64))

    revocations =
      case Revocations.start_link([]) do
        {:ok, pid} -> pid
        {:error, {:already_started, _}} -> nil
      end

    calls = :counters.new(1, [])
    hits = :counters.new(2, [])
    handler = "bench-ws-connect-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :pool_take],
      fn _event, _m, %{result: result}, _ ->
        :counters.add(hits, if(result == :hit, do: 1, else: 2), 1)
      end,
      nil
    )

    authorize = fn arg, tok ->
      :counters.add(calls, 1, 1)
      HranaAuth.authorize(arg, tok)
    end

    {:ok, sup, port} = HranaClient.start_listener(prod: true, authorize: authorize)

    try do
      token =
        Phoenix.Token.sign(
          Application.fetch_env!(:fathom, :hrana_token_secret),
          "fathom hrana shard",
          %{"s" => shard, "v" => 1, "e" => System.system_time(:second) + 3_600}
        )

      Revocations.put(shard, 1, nil)

      unless match?({:error, %Filo.Error{status: 401}}, HranaAuth.authorize(shard, nil)) and
               match?({:ok, {:rw, _}}, HranaAuth.authorize(shard, token)),
             do:
               raise(
                 "hrana_ws_connect_rt_us: :required auth is not enforcing - measuring nothing"
               )

      one = fn ->
        {:ok, c} = HranaClient.connect(port, shard, auth_token: token)
        {:ok, c, %{rows: [[1]]}} = HranaClient.execute(c, "SELECT 1")
        HranaClient.close(c)
      end

      # Untimed warm-up: loads the shard and the pool.
      one.()
      :counters.put(calls, 1, 0)
      :counters.put(hits, 1, 0)
      :counters.put(hits, 2, 0)

      us = for _ <- 1..samples, do: elem(:timer.tc(one), 0)

      authorized = :counters.get(calls, 1)

      unless authorized == samples,
        do:
          raise(
            "hrana_ws_connect_rt_us: auth ran #{authorized}x for #{samples} fresh connections - " <>
              "it is not measuring a connect per request"
          )

      hit = :counters.get(hits, 1)
      Logger.warning("bench info: hrana_ws_connect_rt_us pool hits #{hit}/#{samples}")

      unless hit * 2 >= samples,
        do:
          raise(
            "hrana_ws_connect_rt_us: only #{hit}/#{samples} pool hits - the pool is not reused"
          )

      p50(us)
    after
      :telemetry.detach(handler)
      HranaClient.stop_listener(sup)
      Fathom.Shards.drain(shard, 5_000)
      if revocations, do: GenServer.stop(revocations)
      Enum.each(keys, &restore_env(&1, Map.fetch!(prev, &1)))
    end
  end

  @doc """
  `hrana_ws_sparse_rt_us` - median µs of a `SELECT 1` over the Hrana WEBSOCKET path when each request
  follows an IDLE GAP (default 200 ms; `:hrana_sparse_gap_ms`), with Hrana auth REQUIRED and
  production's listener settings (expert review 2026-10-20 #25). Watch-only.

  Every other `hrana_*` metric is a hot loop: the previous request's work keeps the scheduler awake,
  caches warm and the connection process un-hibernated. Real django-libsql traffic is sparse, and
  sparse requests measured ~10x the hot cost (in-process 8 µs at a 0 ms gap vs 103-110 µs at
  200 ms-1 s; WS 230-370 µs after 1.5 s idle vs ~55 µs), so wake-dependent regressions (a per-request
  timer or watchdog, hibernation, scheduler wake) passed the gate unseen. The auth-on per-statement
  path (`HranaAuth.version_current?` -> `Revocations.cached_floor/1`) is also exercised here and
  nowhere else.

  Kept small (default 20 samples x gap) so it adds ~4 s, not minutes. The default gap is below the 5 s
  hibernate delay, so it measures the idle wake, not the hibernate wake; a `:hrana_sparse_gap_ms`
  above `:hrana_ws_hibernate_ms` measures that.

  Asserts its own preconditions: the gap really elapsed before every timed request, every request ran
  on the SAME socket, every reply is `[[1]]`, auth was enforced (a missing token is refused and the
  authorize callback ran at `hello`), and the revocation floor stayed a cache hit.
  """
  @spec hrana_ws_sparse_rt_us(keyword()) :: float()
  def hrana_ws_sparse_rt_us(opts \\ []), do: hrana_ws_sparse_stats(opts).p50_us

  @doc """
  `hrana_ws_sparse_rt_us` plus its host calibration, from ONE run: `%{p50_us, calib_us, ratio}`.
  `calib_us` is a raw loopback TCP echo after the same idle gap (`calibration_idle_rt_us/2`) and
  `ratio` is `p50_us / calib_us` (expert review 2026-10-20-2027 #25).
  """
  @spec hrana_ws_sparse_stats(keyword()) :: %{p50_us: float(), calib_us: float(), ratio: float()}
  def hrana_ws_sparse_stats(opts \\ []) do
    alias Fathom.Bench.HranaClient
    alias Fathom.HranaAuth
    alias Fathom.HranaAuth.Revocations

    samples = Keyword.get(opts, :hrana_sparse_samples, @hrana_sparse_samples)
    gap_ms = Keyword.get(opts, :hrana_sparse_gap_ms, @hrana_sparse_gap_ms)
    shard = "benchwssparse"
    keys = [:hrana_auth, :hrana_token_max_age, :hrana_token_secret]
    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})

    setup(opts)
    Application.put_env(:fathom, :shard_base_domain, nil)
    Application.put_env(:fathom, :default_shard, nil)
    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Application.ensure_all_started(:mint)

    Application.put_env(:fathom, :hrana_auth, :required)
    Application.put_env(:fathom, :hrana_token_max_age, 3_600)

    if Application.get_env(:fathom, :hrana_token_secret) in [nil, ""],
      do: Application.put_env(:fathom, :hrana_token_secret, String.duplicate("a", 64))

    revocations =
      case Revocations.start_link([]) do
        {:ok, pid} -> pid
        {:error, {:already_started, _}} -> nil
      end

    # Host calibration, same samples x gap, run BEFORE the listener starts (expert review
    # 2026-10-20-2027 #25). Most of the sparse-vs-hot 6x is the HOST waking from an idle gap (CPU
    # power state, scheduler wake), which no fathom change can move; the ratio is what a fathom
    # regression moves. Placed here and not after the loop because dialyzer's opaque
    # mint_web_socket cascade marks everything after the first HranaClient call unreachable, which
    # made it report the calibration helpers as never called.
    calib_us = calibration_idle_rt_us(samples, gap_ms)

    calls = :counters.new(1, [])

    authorize = fn arg, tok ->
      :counters.add(calls, 1, 1)
      HranaAuth.authorize(arg, tok)
    end

    {:ok, sup, port} = HranaClient.start_listener(prod: true, authorize: authorize)

    try do
      # Signed directly (see hrana_open_auth_rt): the public mint writes a Postgres ledger.
      token =
        Phoenix.Token.sign(
          Application.fetch_env!(:fathom, :hrana_token_secret),
          "fathom hrana shard",
          %{"s" => shard, "v" => 1, "e" => System.system_time(:second) + 3_600}
        )

      Revocations.put(shard, 1, nil)

      unless match?({:error, %Filo.Error{status: 401}}, HranaAuth.authorize(shard, nil)) and
               match?({:ok, {:rw, _}}, HranaAuth.authorize(shard, token)),
             do:
               raise("hrana_ws_sparse_rt_us: :required auth is not enforcing - measuring nothing")

      {:ok, client} = HranaClient.connect(port, shard, auth_token: token)

      unless :counters.get(calls, 1) >= 1,
        do: raise("hrana_ws_sparse_rt_us: the authorize callback never ran at hello")

      # Warm the shard and the stream; not timed.
      {:ok, client, _} = HranaClient.execute(client, "SELECT 1")
      socket = client.conn.socket

      {client, us, _done} =
        Enum.reduce(1..samples, {client, [], System.monotonic_time(:millisecond)}, fn _,
                                                                                      {c, acc,
                                                                                       done} ->
          Process.sleep(gap_ms)
          started = System.monotonic_time(:millisecond)

          unless started - done >= gap_ms,
            do: raise("hrana_ws_sparse_rt_us: idle gap #{started - done} ms < #{gap_ms} ms")

          {t, {:ok, c, %{rows: [[1]]}}} = :timer.tc(fn -> HranaClient.execute(c, "SELECT 1") end)

          unless c.conn.socket == socket,
            do: raise("hrana_ws_sparse_rt_us: the socket changed mid-run (reconnected)")

          {c, [t | acc], System.monotonic_time(:millisecond)}
        end)

      now = System.monotonic_time(:millisecond)

      unless match?([{_, 1, nil, exp}] when exp > now, :ets.lookup(Revocations, shard)),
        do: raise("hrana_ws_sparse_rt_us: the revocation floor lapsed mid-run")

      HranaClient.close(client)

      p50_us = p50(us)
      %{p50_us: p50_us, calib_us: calib_us, ratio: p50_us / max(calib_us, 1.0)}
    after
      HranaClient.stop_listener(sup)
      Fathom.Shards.drain(shard, 5_000)
      if revocations, do: GenServer.stop(revocations)
      Enum.each(keys, &restore_env(&1, Map.fetch!(prev, &1)))
    end
  end

  # `hrana_ws_sparse_calibration_us`: median µs of a 1-byte echo over a raw loopback `:gen_tcp`
  # pair (no Mint, no WebSocket, no auth, no fathom code) after the same idle gap per sample as
  # `hrana_ws_sparse_rt_us`. It pays the host's idle-wake cost (CPU power state, scheduler wake,
  # loopback syscalls) and nothing else, so `hrana_ws_sparse_rt_us / this` separates a host that
  # is slow to wake from fathom work added to the sparse request path. Asserts the gap elapsed
  # and every echo returned the byte sent.
  defp calibration_idle_rt_us(samples, gap_ms) do
    {:ok, lsock} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}, nodelay: true])
    {:ok, port} = :inet.port(lsock)

    echo =
      spawn_link(fn ->
        {:ok, s} = :gen_tcp.accept(lsock)
        echo_loop(s)
      end)

    {:ok, c} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, nodelay: true])

    try do
      :ok = :gen_tcp.send(c, "x")
      {:ok, "x"} = :gen_tcp.recv(c, 1, 5_000)

      {_done, us} =
        Enum.reduce(1..samples, {System.monotonic_time(:millisecond), []}, fn _, {done, acc} ->
          Process.sleep(gap_ms)
          started = System.monotonic_time(:millisecond)

          unless started - done >= gap_ms,
            do: raise("calibration_idle_rt_us: idle gap #{started - done} ms < #{gap_ms} ms")

          {t, {:ok, "x"}} =
            :timer.tc(fn ->
              :ok = :gen_tcp.send(c, "x")
              :gen_tcp.recv(c, 1, 5_000)
            end)

          {System.monotonic_time(:millisecond), [t | acc]}
        end)

      p50(us)
    after
      :gen_tcp.close(c)
      :gen_tcp.close(lsock)
      Process.unlink(echo)
      Process.exit(echo, :kill)
    end
  end

  defp echo_loop(s) do
    case :gen_tcp.recv(s, 1) do
      {:ok, data} ->
        :ok = :gen_tcp.send(s, data)
        echo_loop(s)

      {:error, _} ->
        :ok
    end
  end

  @doc """
  `hrana_pooled_open_rt_us` — median round trip of a one-shot `[execute, close]` stream that
  REUSES a pooled handle.

  `hrana_oneshot_rt_us` closes its streams but never reuses one: a serial client takes the shard's
  open-connection count to zero on every close, which drains the pool (2026-09-29 #25), so every
  sample opens fresh. Prod reuses whenever streams overlap. Here one background stream stays open
  for the whole run, so each timed stream's open is a pool hit and its close a pool checkin —
  `reset_for_reuse/2` and the temp-schema probe, the path expert review 2026-10-01 perf #24 is
  about. The pool is forced on for this metric only (it is off outside prod config), and the
  metric counts `[:fathom, :shard, :pool_take]` hits and raises if they are not the norm.
  """
  @spec hrana_pooled_open_rt_us(keyword()) :: float() | nil
  def hrana_pooled_open_rt_us(opts \\ []), do: hrana_pooled_open_rt(opts)

  defp hrana_pooled_open_rt(opts) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    requests = [
      %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1", "args" => []}},
      %{"type" => "close"}
    ]

    prev_pool = Application.get_env(:fathom, :connection_pool)
    Application.put_env(:fathom, :connection_pool, true)
    hits = :counters.new(2, [])
    handler = "bench-pooled-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :pool_take],
      fn _event, _m, %{result: result}, _ ->
        :counters.add(hits, if(result == :hit, do: 1, else: 2), 1)
      end,
      nil
    )

    try do
      with_wire(opts, "benchpooled", fn client ->
        # The background stream: execute, keep its baton, never close it until the end. While it
        # is open the shard's connection count never reaches zero, so the pool is never drained.
        {:ok, _, held} = Filo.Client.execute(%{client | baton: nil}, "SELECT 1")
        held_baton = held.baton

        # Warm: one one-shot puts a handle in the pool for the first timed sample to take.
        {:ok, _, client} = Filo.Client.pipeline(%{held | baton: nil}, requests)
        :counters.put(hits, 1, 0)
        :counters.put(hits, 2, 0)

        {client, us} =
          Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
            c = %{c | baton: nil}
            {t, result} = :timer.tc(fn -> Filo.Client.pipeline(c, requests) end)
            {:ok, [%{"type" => "ok"}, %{"type" => "ok"}], c} = result
            {c, [t | acc]}
          end)

        {:ok, _, _} = Filo.Client.pipeline(%{client | baton: held_baton}, [%{"type" => "close"}])

        hit = :counters.get(hits, 1)

        unless hit * 2 >= samples,
          do:
            raise(
              "hrana_pooled_open_rt_us got #{hit} pool hits in #{samples} samples " <>
                "(#{:counters.get(hits, 2)} misses); it is not measuring the reuse path"
            )

        p50(us)
      end)
    after
      :telemetry.detach(handler)

      if is_nil(prev_pool),
        do: Application.delete_env(:fathom, :connection_pool),
        else: Application.put_env(:fathom, :connection_pool, prev_pool)
    end
  end

  @doc """
  `hrana_pooled_pipeline_rt_us` — median round trip of a SERIAL one-shot stream running a
  five-statement pipeline (`[execute x5, close]`), the shape of one ORM request on a pooled handle.

  Perf review 2026-10-14 #4 and #10: with the pool drain grace a serial client's next stream is a
  pool HIT, and with the statement-cache carry it adopts the previous stream's prepared statements,
  so a steady-state sample prepares NOTHING. The metric asserts both preconditions: pool hits are the
  norm (`[:fathom, :shard, :pool_take]`), and `Exqlite.Sqlite3.prepare/2` ran less than once per
  sample during the timed loop (a global `:call_count` trace pattern, which needs no process trace
  flags). Either failing raises: the metric would be measuring the fresh-open or re-prepare path.
  Unlike `hrana_pooled_open_rt_us` there is NO background stream, so the hit depends on the grace.
  The pool is forced on for this metric only. Watch-only.
  """
  @spec hrana_pooled_pipeline_rt_us(keyword()) :: float() | nil
  def hrana_pooled_pipeline_rt_us(opts \\ []) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    stmts = [
      "SELECT v FROM kv WHERE k = 1",
      "SELECT count(*) FROM kv",
      # READS ONLY, for the reason the seed below gives.
      "SELECT k FROM kv WHERE v = 'b'",
      "SELECT k, v FROM kv ORDER BY k LIMIT 5",
      "SELECT max(k) FROM kv"
    ]

    requests =
      Enum.map(stmts, &%{"type" => "execute", "stmt" => %{"sql" => &1, "args" => []}}) ++
        [%{"type" => "close"}]

    n_req = length(requests)
    prev_pool = Application.get_env(:fathom, :connection_pool)
    Application.put_env(:fathom, :connection_pool, true)
    hits = :counters.new(2, [])
    handler = "bench-pooled-pipeline-#{System.unique_integer([:positive])}"
    prepare = {Exqlite.Sqlite3, :prepare, 2}

    :telemetry.attach(
      handler,
      [:fathom, :shard, :pool_take],
      fn _event, _m, %{result: result}, _ ->
        :counters.add(hits, if(result == :hit, do: 1, else: 2), 1)
      end,
      nil
    )

    try do
      with_wire(opts, "benchpipeline", fn client ->
        # Seed through the shard's own file, as wire_rows does: no write crosses the wire (the
        # prod-compiled bench has replication configured on but none of its processes started,
        # so a wire write raises — first run, 2026-10-14).
        path = Fathom.Shard.db_path("benchpipeline")
        drop_db(path)
        {:ok, db} = Exqlite.Sqlite3.open(path)

        :ok =
          Exqlite.Sqlite3.execute(
            db,
            "CREATE TABLE kv (k INTEGER, v TEXT); INSERT INTO kv VALUES (1, 'a'), (2, 'b')"
          )

        :ok = Exqlite.Sqlite3.close(db)
        :ok = Fathom.Shard.stamp_local_provenance("benchpipeline")

        # Warm: one pipeline prepares the five statements and leaves the handle pooled with them.
        {:ok, warm, client} = Filo.Client.pipeline(%{client | baton: nil}, requests)
        true = pipeline_ok?(warm, n_req)
        :counters.put(hits, 1, 0)
        :counters.put(hits, 2, 0)
        :erlang.trace_pattern(prepare, true, [:call_count])

        {_client, us} =
          Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
            c = %{c | baton: nil}
            {t, result} = :timer.tc(fn -> Filo.Client.pipeline(c, requests) end)
            {:ok, results, c} = result
            true = pipeline_ok?(results, n_req)
            {c, [t | acc]}
          end)

        prepares =
          case :erlang.trace_info(prepare, :call_count) do
            {:call_count, n} when is_integer(n) -> n
            _ -> 0
          end

        hit = :counters.get(hits, 1)

        unless hit * 2 >= samples,
          do:
            raise(
              "hrana_pooled_pipeline_rt_us got #{hit} pool hits in #{samples} samples " <>
                "(#{:counters.get(hits, 2)} misses); the drain grace is not keeping the pool"
            )

        unless prepares < samples,
          do:
            raise(
              "hrana_pooled_pipeline_rt_us prepared #{prepares} statements in #{samples} " <>
                "samples; the statement cache is not travelling with the pooled handle"
            )

        p50(us)
      end)
    after
      :erlang.trace_pattern(prepare, false, [:call_count])
      :telemetry.detach(handler)

      if is_nil(prev_pool),
        do: Application.delete_env(:fathom, :connection_pool),
        else: Application.put_env(:fathom, :connection_pool, prev_pool)
    end
  end

  defp pipeline_ok?(results, n),
    do: length(results) == n and Enum.all?(results, &match?(%{"type" => "ok"}, &1))

  @doc """
  `flush_p50_us` — median wall time of one real durability flush.

  The whole write path was ungated: `setup/1` sets `:shard_flush_interval_ms` to `0`, so no
  gated metric executed a single line of `snapshot/2`, `snapshot_and_upload/1`, or
  `handle_info(:durability_flush, …)` — roughly 2× the shard size in local I/O plus the shard
  size on the wire, per shard, per interval. Both recent flush regressions (the wasted fsyncs,
  the inline autocheckpoint) were caught by human review, not by the harness
  (expert review 2026-08-01 #41.3).

  Runs against `Storage.Local` over a keystone-sized shard, so it is S3-free and stays in the
  default gate. Drives `flush_now/1`, the same entry the periodic timer takes.

  **Which route it measures (r7 #2).** Since 04c387c8 the shard has no WAL between trials
  (Keystone `build!` ends with TRUNCATE + close and the trials only bump the write counter), so
  every trial takes the PINNED-LIVE route (`quick_check` + a live-file upload under a read pin),
  NOT `VACUUM INTO`. The trials assert that via `[:fathom, :shard, :flush, :route]`. The VACUUM
  route is `flush_vacuum_p50_us`, the drop flush `drop_flush_p50_us`, and the codec route
  `flush_stub_p50_us`. Series name kept for continuity.

  **About 60% of this number is the `Storage.Local` test double, not production code** (expert
  review 2026-10-22 r8 #3): SHA-256 of the remote object, MD5 of the source, `File.cp` + fsync +
  rename, a `.pos` atomic write + fsync and a SHA-256 of the destination, none of which `Storage.S3`
  runs. Its 40% same-host spread (7.5-10.4 ms over 12 entries) false-blocks unrelated commits and a
  20% regression in the prod codec passes it. The prod-path flush costs are `flush_codec_ms_per_mb`
  (gated since r8) and `flush_vacuum_p50_us` / `flush_stub_p50_us` (watch-only until ~5 steady
  entries, see `Fathom.Bench.Gate`).
  """
  # No `| nil`, unlike the wire metrics: those return nil when the listener or client cannot start
  # (the metric is skipped, never faked), but this one is S3-free and local — there is nothing for
  # it to skip, and it ends in `p50(us)` unconditionally.
  @spec flush_p50(keyword()) :: float()
  def flush_p50(opts \\ []) do
    trials = Keyword.get(opts, :trials, @default_trials)
    id = "benchflush"

    setup(opts)
    setup_write_path()
    routes = Fathom.Bench.StorageIO.attach_routes()

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

      # The route this series is documented to measure (r7 #2): a regression that moved these
      # trials onto another route would otherwise change the series silently.
      taken = Fathom.Bench.StorageIO.take_routes(routes)

      unless taken == List.duplicate({:flush, :pinned_live}, trials),
        do:
          raise(
            "flush_p50_us expected #{trials} x {:flush, :pinned_live} routes, got #{inspect(taken)}"
          )

      Fathom.Shard.checkin(pid, ref)
      Shards.drain(id, 5_000)
      p50(us)
    after
      Fathom.Bench.StorageIO.detach_routes(routes)
      Shards.drain(id, 5_000)
      drop_db(Fathom.Shard.db_path(id))
    end
  end

  @doc """
  `flush_vacuum_p50_us` - median wall time of one durability flush on the `VACUUM INTO` route
  (`quick_check` + `VACUUM INTO` a temp + upload), `Storage.Local` (expert review 2026-10-21 r7 #2).
  `flush_p50_us` runs the pinned-live route; this is the route a write-active replicated shard
  takes in prod. A reader transaction is held for the run so the WAL is never fully backfilled, and
  one small write is committed before every trial. WATCH-ONLY (new series).

  Preconditions (raises otherwise): the shard is dirty before every flush and every flush reports
  the `:vacuum` route.
  """
  @spec flush_vacuum_p50(keyword()) :: float()
  def flush_vacuum_p50(opts \\ []) do
    trials = Keyword.get(opts, :trials, @default_trials)
    id = "benchflushvac"

    setup(opts)
    setup_write_path()

    try do
      {pid, ref, path} =
        Fathom.Bench.StorageIO.open_flush_shard(id, Keyword.get(opts, :flush_rows, 2_000))

      # The 32 MB variant (r8 #3) must really be that size, or it re-measures the keystone series.
      min_bytes = Keyword.get(opts, :min_db_bytes, 0)
      size = Fathom.Bench.StorageIO.db_bytes(path)

      if size < min_bytes,
        do:
          raise(
            "flush_vacuum: shard is #{size} B, expected >= #{min_bytes} - measuring the wrong size"
          )

      us =
        Fathom.Bench.StorageIO.vacuum_route_flush_us(
          id,
          pid,
          path,
          trials,
          Keyword.get(opts, :metric_name, "flush_vacuum_p50_us")
        )

      Fathom.Shard.checkin(pid, ref)
      Shards.drain(id, 5_000)
      p50(us)
    after
      Shards.drain(id, 5_000)
      drop_db(Fathom.Shard.db_path(id))
    end
  end

  @flush_32_rows 43_000
  @flush_32_min_bytes 28_000_000

  @doc """
  `flush_vacuum_32mb_p50_us` - `flush_vacuum_p50/1` on a ~32 MB shard (expert review 2026-10-22 r8
  #3). The keystone series (1.5 MB) hides how the VACUUM route scales with shard size, which is
  what r8 #9's `sqlite3_serialize` switch changes (probe: 50 ms VACUUM INTO vs 4.6 ms serialize at
  32 MB). Fewer trials (default 3) because each flush is tens of ms. Precondition: the db file is
  at least #{@flush_32_min_bytes} bytes. WATCH-ONLY (new series).
  """
  @spec flush_vacuum_32_p50(keyword()) :: float()
  def flush_vacuum_32_p50(opts \\ []) do
    opts
    |> Keyword.put(:flush_rows, Keyword.get(opts, :flush_32_rows, @flush_32_rows))
    |> Keyword.put(:min_db_bytes, @flush_32_min_bytes)
    |> Keyword.put(:metric_name, "flush_vacuum_32mb_p50_us")
    |> Keyword.update(:trials, 3, &min(&1, 3))
    |> flush_vacuum_p50()
  end

  @doc """
  `drop_flush_p50_us` - median wall time of `Shards.drain/2` on a dirty, fully checked-in shard:
  the idle-drop durability flush (`checkpoint_and_verify` TRUNCATE + `quick_check` + live-file
  upload), prod's main durability producer because `SHARD_IDLE_MS` is shorter than the flush
  interval (expert review 2026-10-21 r7 #2). Includes the coordinator stop and file drop, which is
  what an idle drop costs. Each trial uses a fresh shard (a drop consumes it); the Keystone build
  is untimed. WATCH-ONLY (new series).

  Preconditions (raises otherwise): the shard is dirty before the drain and each drain reports the
  `:checkpoint` drop route.
  """
  @spec drop_flush_p50(keyword()) :: float()
  def drop_flush_p50(opts \\ []) do
    trials = Keyword.get(opts, :trials, @default_trials)
    rows = Keyword.get(opts, :flush_rows, 2_000)

    setup(opts)
    setup_write_path()
    routes = Fathom.Bench.StorageIO.attach_routes()
    ids = for i <- 1..trials, do: "benchdropflush#{i}"

    try do
      us =
        for id <- ids do
          {pid, ref, _path} = Fathom.Bench.StorageIO.open_flush_shard(id, rows)
          Fathom.Shard.WriteCounter.bump(id)

          unless Fathom.Shard.dirty?(pid),
            do: raise("drop_flush_p50_us is measuring nothing: the shard is not dirty")

          Fathom.Shard.checkin(pid, ref)
          _ = :sys.get_state(pid)
          {t, res} = :timer.tc(fn -> Shards.drain(id, 10_000) end)
          if res != :ok, do: raise("drop_flush_p50_us: drain returned #{inspect(res)}")
          t
        end

      taken = Fathom.Bench.StorageIO.take_routes(routes)

      unless Enum.count(taken, &(&1 == {:drop_flush, :checkpoint})) == trials,
        do:
          raise(
            "drop_flush_p50_us expected #{trials} x {:drop_flush, :checkpoint}, got #{inspect(taken)}"
          )

      p50(us)
    after
      Fathom.Bench.StorageIO.detach_routes(routes)

      for id <- ids do
        Shards.drain(id, 5_000)
        drop_db(Fathom.Shard.db_path(id))
      end
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
  `wire_server_rows_per_s` - rows/sec the SERVER side alone sustains for the `wire_rows_per_s`
  workload: an in-process `Fathom.ShardExecutor.execute/2` of `SELECT * FROM ks_scalars` on a
  keystone, then `Filo.StmtResult.encode(rows: :json)` and the final JSON serialisation (what
  `Filo.Plug` does with the reply). No listener, no `Filo.Client`, so no client-side decode.

  Why it exists (expert review 2026-10-20-2027 #4): `wire_rows_per_s` times the whole round
  trip including the bench client's own decode, which a profile put at ~65% of the sample, so a
  server-side win (or a filo change on the encode path) is diluted ~3x in that metric. This one
  is the number to judge encode/execute changes by; the old metric is kept unchanged as the
  end-to-end figure. New series, watch-only until a band is measured.

  PRECONDITION asserted: the execute returned the full keystone and the encoded body is non-empty.
  """
  @spec wire_server_rows(keyword()) :: float()
  def wire_server_rows(opts \\ []) do
    setup(opts)
    rows = Keyword.get(opts, :wire_rows, @wire_rows)
    trials = Keyword.get(opts, :trials, @default_trials)
    shard = "benchwiresrv"
    path = Fathom.Shard.db_path(shard)
    drop_db(path)
    {:ok, _} = Fathom.Keystone.build!(path, rows: rows)
    :ok = Fathom.Shard.stamp_local_provenance(shard)

    {:ok, handle} = Fathom.ShardExecutor.open(shard, :trusted)

    try do
      stmt = %Filo.Stmt{sql: "SELECT * FROM ks_scalars"}

      run = fn ->
        {:ok, result} = Fathom.ShardExecutor.execute(handle, stmt)
        body = result |> Filo.StmtResult.encode(rows: :json) |> Jason.encode_to_iodata!()
        {length(result.rows), IO.iodata_length(body)}
      end

      {got, bytes} = run.()

      if got != rows or bytes == 0,
        do: raise("wire_server_rows: expected #{rows} rows, got #{got} - measuring nothing")

      trials
      |> times(fn _ ->
        {us, _} = :timer.tc(run)
        rows / (us / 1_000_000)
      end)
      |> median()
    after
      Fathom.ShardExecutor.close(handle)
      Fathom.Shards.drain(shard, 5_000)
      drop_db(path)
    end
  end

  # `wire_encode_rows_per_s` — rows/sec through `Filo.Value.encode/1`, the OTHER encoder
  # (expert review 2026-08-01 #41.7).
  #
  # `Filo.Value` has two encoding PATHS and the gate only ever ran one. `wire_rows_per_s` drives
  # `encode_json/1`, which emits JSON directly. The other path builds tagged maps with `encode/1`
  # and then serialises them, and `Filo.Value`'s own moduledoc records that the tagged-map layer
  # "roughly doubled" per-cell cost. `Cursor.entries/2` takes it for `rows: :maps`, and
  # `Protobuf.encode_value/1` consumes the same maps — i.e. exactly when result sets are large.
  #
  # **This times `encode/1` FOLLOWED BY the JSON serialisation, not `encode/1` alone**, and the
  # first draft got that wrong. Measured per cell on keystone rows:
  #
  #     encode/1 alone          0.053 µs   <- FASTER than encode_json/1; times half the work
  #     encode_json/1           0.111 µs
  #     encode/1 + Jason        0.269 µs   <- 2.42x, the doubling the moduledoc describes
  #
  # Timing `encode/1` by itself made the "slower encoder" look 0.41x the cost of the fast one,
  # because building a map is cheap and the expense is serialising it afterwards. A metric that
  # reads *better* than its comparison is the tell that it is measuring the wrong span.
  #
  # **NOT driven through the cursor HTTP transport**, which is what the review proposed.
  # `Filo.Client` states outright that cursors are outside its scope, so that shape would mean new
  # HTTP plumbing to cover a path the finding itself calls "no live bug today — coverage only".
  # This is the per-cell cost, which is the whole of the risk; cursor/protobuf FRAMING stays
  # ungated, and that is a deliberate, stated limit rather than an oversight.
  #
  # Same keystone source as `wire_rows_per_s`, so every SQLite storage class — including the BLOB
  # that carried the 200x regression — crosses this path too.
  @spec wire_encode_rows(keyword()) :: float() | nil
  defp wire_encode_rows(opts) do
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
  #
  # The spec is polymorphic because that is what the helper honestly promises: it returns whatever
  # `fun` returned, or nil.
  #
  # It does NOT silence the four dialyzer findings on the metrics that call this, and that is worth
  # knowing before someone tries it again. Four callbacks return different shapes — a
  # `%{p50_us, p99_us}` map from `hrana_rt_stats/1`, a bare float from the other three — so
  # dialyzer computes ONE success typing for this helper that unions all of them and hands that
  # union to every caller, which is why `hrana_rt_stats/1` reads as "missing float()". A type
  # variable does not change that: dialyzer uses SUCCESS TYPINGS, not contracts, when analyzing
  # callers, so a spec here constrains nothing downstream. (Measured twice on 2026-08-14 — the same
  # attempt on `HranaClient.await_upgrade/2` also changed nothing.) Those four are recorded in
  # `.dialyzer_ignore.exs` as the dialyzer limitation they are.
  @spec with_wire(keyword(), String.t(), (term() -> result)) :: result | nil when result: var
  defp with_wire(opts, shard, fun), do: with_wire(opts, shard, fun, [], [])

  # `plug_extra` is merged into the Filo.Plug options (the auth metric adds `:authorize`);
  # `connect_extra` into `Filo.Client.connect/2` (its `:auth_token`). Both empty for every
  # pre-existing metric, so their listener and client are exactly what they were.
  defp with_wire(opts, shard, fun, plug_extra, connect_extra) do
    setup(opts)
    # Route by Host subdomain exactly as the LB does. Pinned rather than inherited so the
    # metric does not depend on whatever SHARD_BASE_DOMAIN happens to be set in the shell.
    Application.put_env(:fathom, :shard_base_domain, nil)
    Application.put_env(:fathom, :default_shard, nil)

    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Application.ensure_all_started(:mint)

    case start_listener(plug_extra) do
      {:ok, sup, port} ->
        try do
          case Filo.Client.connect(
                 "http://127.0.0.1:#{port}",
                 [authority: "#{shard}.local"] ++ connect_extra
               ) do
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

  defp start_listener(plug_extra) do
    port = free_port()

    plug_opts =
      [
        executor: Fathom.ShardExecutor,
        streams: Fathom.Bench.WireStreams,
        key: Filo.Baton.new_key(),
        open_arg: &Fathom.ShardExecutor.shard_from_conn/1
        # No :authorize unless `plug_extra` adds one - every metric but `hrana_open_auth_rt_us`
        # runs with Hrana auth disabled.
      ] ++ plug_extra

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

  ## This deliberately does NOT collect the coordinators — read `fanout_gc/1` before "fixing" it

  Only the CALLING process is collected, so the N coordinator heaps are sampled holding whatever
  the open path just allocated. That is intentional (expert review 2026-07-24 #9): coordinators run
  `fullsweep_after: 0`, and this is the metric that measures whether that policy actually reclaims
  retained heap — the cost that dominates at 30k shards/node. Forcing a sweep here would measure
  the same figure with or without the policy and silently blind the gate to it, which is exactly
  what `bench_test.exs`'s coordinator-GC-policy test says in its comment.

  The price is precision. Retained heap includes open-path churn, BEAM heaps grow in geometric size
  classes, and N heaps tip a class together — so this number moves ~30% in EITHER direction on
  changes that cannot affect steady-state footprint, and it has a 12× lifetime spread. **It is a
  poor threshold to gate on and a good signal to watch.** When it blocks a commit, read
  `fanout_gc_kb_per_shard` (the same sample with the coordinators collected) and
  `served_kb_per_shard` before believing it — see `fanout_gc/1` for the 2026-08-10 measurements
  that established this.
  """
  @spec fanout(keyword()) :: float()
  def fanout(opts \\ []) do
    fanout_sample(opts, false)
  end

  @doc """
  `fanout_gc_kb_per_shard` — `fanout/1`'s sample with the N coordinators force-collected first.

  The steady-state floor: what an open-but-idle shard costs once its heap has been swept, with the
  open path's transient garbage removed. `fanout/1` minus this is churn.

  **Why it exists.** `fanout_kb_per_shard` blocked `promote-on-open` at +38% for two sessions.
  Splitting `:erlang.memory/1` by bucket on both branches showed the entire delta was `:processes`
  — `:code`, `:ets`, `:binary` and `:system` identical, so never fixed bytecode wrongly divided by
  N. Then force-collecting the coordinators **inverted** the verdict: parent 4.412, change 4.112
  KiB/shard. The phantom also reproduced on demand — the PARENT's own reading moved 3.893 → 4.958
  (+27%) between two probe builds differing only in code that ran strictly AFTER the sample point.
  The buckets do not even reconcile with `:total` to better than ~1 KiB/shard, which is the size of
  the whole disputed 1.4 KiB/shard effect: at N=200 the precision WAS the effect.
  `served_kb_per_shard` agreed throughout (+0.06% against its own ±0.6 KiB band over 61 samples).

  How noisy `fanout/1` is at small N is worth stating outright: writing the `fanout_gc <= fanout`
  guard for `bench_test.exs` produced a run where `fanout/1` read **-5.75 KiB/shard** — a sweep
  between its own two samples freed more than the coordinators allocated. It is an unsigned
  quantity that can come back negative. Gate on it accordingly.

  So this is not a replacement for `fanout/1` — it is the second reading that tells you which kind
  of change you are looking at:

    * `fanout` up, `fanout_gc` flat ⇒ open-path churn or a heap size-class tip. Not a footprint
      regression.
    * both up ⇒ retained heap really grew. Take it seriously; that is #9's signal firing.
  """
  @spec fanout_gc(keyword()) :: float()
  def fanout_gc(opts \\ []) do
    fanout_sample(opts, true)
  end

  defp fanout_sample(opts, gc_coordinators?) do
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

      if gc_coordinators? do
        Enum.each(handles, fn {_id, pid, _ref} -> :erlang.garbage_collect(pid) end)
      end

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
  @spec served(keyword()) :: %{
          total_kb: float(),
          binary_kb: float(),
          blocks_kb: float(),
          sqlite_kb: float(),
          proc_kb: float()
        }
  def served(opts \\ []) do
    setup(opts)
    n = Keyword.get(opts, :served_n, @served_n)
    trials = Keyword.get(opts, :trials, @default_trials)
    rows = Keyword.get(opts, :served_rows, @served_rows)
    stmts = Keyword.get(opts, :served_stmts, @served_stmts)
    schema = Keyword.get(opts, :served_schema, :keystone)

    results =
      times(trials, fn t ->
        collect_all()
        before_mem = memory_snapshot()
        before_total = :erlang.memory(:total)
        before_bin = :erlang.memory(:binary)

        handles = Enum.map(1..n, fn i -> open_served_shard(t, i, rows, stmts, schema) end)

        # Assert the precondition rather than trust it (the `flush_p50_us` 2 µs lesson: a metric
        # whose work silently did not happen reports a spectacular number and the gate says OK).
        # A held connection that never ran a query would measure the idle regime under a new name.
        unless length(handles) == n do
          raise "served bench opened #{length(handles)}/#{n} shards — measuring nothing"
        end

        collect_all()
        total = :erlang.memory(:total) - before_total
        bin = :erlang.memory(:binary) - before_bin
        det = memory_delta(before_mem, memory_snapshot(), Enum.map(handles, &elem(&1, 1)))
        log_memory_delta("served", t, n, det)

        Enum.each(handles, &close_served_shard/1)

        {total / n / 1024, bin / n / 1024, det.blocks / n / 1024, det.sqlite / n / 1024,
         det.proc / n / 1024}
      end)

    %{
      total_kb: results |> Enum.map(&elem(&1, 0)) |> median(),
      binary_kb: results |> Enum.map(&elem(&1, 1)) |> median(),
      blocks_kb: results |> Enum.map(&elem(&1, 2)) |> median(),
      sqlite_kb: results |> Enum.map(&elem(&1, 3)) |> median(),
      proc_kb: results |> Enum.map(&elem(&1, 4)) |> median()
    }
  end

  # --- mode-independent memory reading (expert review 2026-10-22 r8 #1) -------------------------
  #
  # `:erlang.memory(:total)` is CARRIER-granular: it moves with how the allocators happened to
  # carve carriers (mmap'd single-block vs multiblock) after whatever arm ran earlier in the VM.
  # perf_history shows two stable modes for served / served_repl (A: 193-205 / 203-219, B:
  # 178-190 / 233-251) and the r7 "-7.3% win" / "+13.5% regression" pair was ONE mode flip, not code.
  # So alongside the (unchanged, still gated) total this reads LIVE BLOCK bytes, which count what was
  # allocated and not how carriers were laid out:
  #   * blocks - sum of live block bytes over every alloc_util allocator (heaps, ETS, binaries, NIF)
  #   * sqlite - the `driver_alloc` slice of that: exqlite routes ALL SQLite allocations through
  #     `enif_alloc` (driver_alloc), so this is sqlite3_memory_used's moral equivalent without a NIF
  #   * proc   - Process.info(:memory) + refc binaries over the coordinators, plus this (caller)
  #     process's delta, which owns every connection's statement cache in its dictionary
  # `:erlang.memory/0` buckets are logged per trial so a future mode flip is attributable.
  defp memory_snapshot do
    # This process owns every connection's statement cache; collect it so its heap is comparable
    # between the two snapshots (the delta went negative in a test process full of earlier garbage).
    :erlang.garbage_collect()
    {blocks, sqlite} = alloc_block_bytes()
    {:memory, self_mem} = Process.info(self(), :memory)
    %{blocks: blocks, sqlite: sqlite, self: self_mem, vm: Map.new(:erlang.memory())}
  end

  defp memory_delta(before, after_, pids) do
    proc =
      Enum.reduce(pids, 0, fn pid, acc ->
        with {:memory, mem} <- Process.info(pid, :memory),
             {:binary, bins} <- Process.info(pid, :binary) do
          acc + mem + (bins |> Enum.map(&elem(&1, 1)) |> Enum.sum())
        else
          _ -> raise "memory reading: coordinator #{inspect(pid)} is gone - measuring nothing"
        end
      end)

    d = %{
      blocks: after_.blocks - before.blocks,
      sqlite: after_.sqlite - before.sqlite,
      proc: proc + (after_.self - before.self),
      vm: Map.new(after_.vm, fn {k, v} -> {k, v - Map.get(before.vm, k, 0)} end)
    }

    # A reading of zero live bytes means the allocator statistics are off or nothing was opened.
    # `blocks` is a net figure over EVERY allocator, so unrelated frees elsewhere in the VM (a process
    # that exited mid-trial) can push it down; `sqlite` is the slice only this arm allocates.
    if d.sqlite <= 0,
      do: raise("memory reading: sqlite delta <= 0 (#{inspect(Map.delete(d, :vm))})")

    d
  end

  defp alloc_block_bytes do
    {sum_alloc_blocks(:erlang.system_info(:alloc_util_allocators)),
     sum_alloc_blocks([:driver_alloc])}
  end

  # Live block bytes summed over the given alloc_util allocators (0 for a disabled one).
  defp sum_alloc_blocks(allocators) do
    Enum.reduce(allocators, 0, fn alloc, acc ->
      case :erlang.system_info({:allocator_sizes, alloc}) do
        instances when is_list(instances) ->
          {_carriers, blocks} = Fathom.Admin.Measurements.sum_allocator_sizes(instances)
          acc + blocks

        _disabled ->
          acc
      end
    end)
  end

  defp log_memory_delta(label, t, n, det) do
    kb = fn bytes -> Float.round(bytes / n / 1024, 1) end

    vm =
      Map.new(Map.take(det.vm, [:total, :processes, :binary, :ets, :system]), fn {k, v} ->
        {k, kb.(v)}
      end)

    # `warning`, not `info`: `mix fathom.bench` raises the Logger level to :warning to keep the run quiet.
    Logger.warning(
      "bench info: #{label} trial #{t} KiB/shard: blocks=#{kb.(det.blocks)} sqlite=#{kb.(det.sqlite)} " <>
        "proc=#{kb.(det.proc)} erlang.memory=#{inspect(vm)}"
    )
  end

  @doc """
  `served_mix_kb_per_shard` / `served_mix_binary_kb_per_shard` - `served/1` with #{@served_mix_stmts}
  distinct wide keystone statements primed per shard (expert review 2026-10-20-2027 #7a), so the
  statement cache is populated like a real ORM-driven connection. Measured on the dev build at
  8 shards: ~160 KiB/shard with one statement vs ~865 KiB/shard with the mix, i.e. the cache is the
  dominant per-connection cost the single-statement series could not see. Watch-only new series.
  """
  @spec served_mix(keyword()) :: %{total_kb: float(), binary_kb: float()}
  def served_mix(opts \\ []), do: served(Keyword.put_new(opts, :served_stmts, @served_mix_stmts))

  @doc """
  `served_django_kb_per_shard` / `served_django_binary_kb_per_shard` - `served/1` with a
  Django-sized WIDE schema (`Fathom.Bench.DjangoSchema`: 45 tables / ~100 indexes, the 12 contrib
  tables plus generated app models) instead of the 6-table keystone (expert review 2026-10-21
  #18a). Each SQLite connection parses and holds its own copy of the schema, so per-connection
  memory grows with the tenant's schema, and every other density number (and `PoolBudget`'s sizing)
  was measured on the keystone. Watch-only new series, beside the gated keystone one.

  Preconditions: the fixture really has >= 40 tables and >= 80 indexes, and the held connection's
  first query returned rows.
  """
  @spec served_django(keyword()) :: %{total_kb: float(), binary_kb: float()}
  def served_django(opts \\ []), do: served(Keyword.put(opts, :served_schema, :django))

  @doc """
  `served_pooled_kb_per_shard` - BEAM memory (KiB) per shard when each shard holds one live stream
  AND one pooled IDLE handle that has been checked in and reused (expert review 2026-10-21 r7 #1,
  panel 2 #6). `served_kb_per_shard` holds raw connections that never go through the pool, so the
  checkin probe (r7 #14: it opens every connection's TEMP database), the carried statement cache and
  the pool's own bookkeeping are invisible to every density arm.

  Per shard: a keeper stream runs the `served` query (so the pool is not drained when the others
  close); a second stream runs it and closes (a pool MISS, the handle is pooled), a third runs it and
  closes (a pool HIT: reuse, `reset_for_reuse`, adopted statements; pooled again). Memory is read
  with the keeper still open, so the delta is coordinator + keeper handle + one pooled idle handle.
  Compare with `served_kb_per_shard` (coordinator + one held handle); the difference is roughly the
  pooled handle. The pool's idle-shrink (`shrink_memory`) has not run yet when this reads.

  Preconditions (raises otherwise): every shard's third stream was a pool HIT, every query returned
  rows, and the pool really is on. WATCH-ONLY (new series).
  """
  @spec served_pooled(keyword()) :: float()
  def served_pooled(opts \\ []) do
    setup(opts)
    n = Keyword.get(opts, :served_n, @served_n)
    trials = Keyword.get(opts, :trials, @default_trials)
    rows = Keyword.get(opts, :served_rows, @served_rows)
    prev_pool = Application.get_env(:fathom, :connection_pool)
    Application.put_env(:fathom, :connection_pool, true)
    hits = :counters.new(2, [])
    handler = "bench-served-pooled-#{System.unique_integer([:positive])}"
    {:ok, _} = Application.ensure_all_started(:telemetry)

    :telemetry.attach(
      handler,
      [:fathom, :shard, :pool_take],
      fn _event, _m, %{result: result}, _ ->
        :counters.add(hits, if(result == :hit, do: 1, else: 2), 1)
      end,
      nil
    )

    try do
      times(trials, fn t ->
        ids = for i <- 1..n, do: "bench_servedpool_#{t}_#{i}"

        for id <- ids do
          path = Fathom.Shard.db_path(id)
          drop_db(path)
          {:ok, _} = Fathom.Keystone.build!(path, rows: rows)
          :ok = Fathom.Shard.stamp_local_provenance(id)
        end

        collect_all()
        :counters.put(hits, 1, 0)
        :counters.put(hits, 2, 0)
        before_total = :erlang.memory(:total)

        keepers =
          for id <- ids do
            {:ok, keeper} = ShardExecutor.open(id)
            pooled_served_query!(keeper)

            for _ <- 1..2 do
              {:ok, h} = ShardExecutor.open(id)
              pooled_served_query!(h)
              :ok = ShardExecutor.close(h)
            end

            keeper
          end

        # Per shard the keeper and the first cycled stream MISS (the pool is empty), the second
        # cycled stream HITS the handle the first one pooled: n hits, 2n misses.
        hit = :counters.get(hits, 1)

        unless hit >= n,
          do:
            raise(
              "served_pooled_kb_per_shard got #{hit} pool hits for #{n} shards " <>
                "(#{:counters.get(hits, 2)} misses); it is not measuring reused pooled handles"
            )

        collect_all()
        used = :erlang.memory(:total) - before_total
        Enum.each(keepers, &ShardExecutor.close/1)

        for id <- ids do
          Shards.drain(id, 5_000)
          drop_db(Fathom.Shard.db_path(id))
        end

        used / n / 1024
      end)
      |> median()
    after
      :telemetry.detach(handler)
      restore_env(:connection_pool, prev_pool)
    end
  end

  @doc """
  `hrana_django_request_rt_us` - median round trip of one Django-shaped request on a POOLED reused
  handle (expert review 2026-10-21 r7 #1, panel 2 #6): a one-shot stream `[execute PRAGMA
  foreign_keys = ON, execute <29-column point SELECT>, execute <36-column 3-way join>, close]`.
  Default Django (`CONN_MAX_AGE=0` over django-libsql) sends that per-connection pragma first, and
  SQLite expires EVERY prepared statement on the connection for any flag-pragma assignment
  (`OP_Expire`, even when the value does not change), so each carried cached statement is
  re-prepared inside its first step on the dirty-IO thread; no dispatch counter can see it
  (`fathom_stmt_reprepares` can). `hrana_pooled_open_rt_us` runs `SELECT 1` and never reaches it.
  The fixture is `Fathom.Bench.DjangoSchema` with its `wide: true` 29-column table.

  A background stream stays open so every timed stream's open is a pool hit. Preconditions (raises
  otherwise): pool hits are at least half the samples, and every reply is `ok` with the wide
  SELECT returning its row with 29 columns and the join returning 36. WATCH-ONLY (new series).
  The `INSERT ... RETURNING` into an FK table from the panel is NOT in this arm: a wire write
  raises in the prod-compiled bench (replication configured on, no Session registry), and
  `hrana_write_rt_us` / `hrana_sdk_write_rt_us` own the write shape.
  """
  @spec hrana_django_request_rt_us(keyword()) :: float() | nil
  def hrana_django_request_rt_us(opts \\ []), do: hrana_django_request_rt(opts)

  defp hrana_django_request_rt(opts) do
    alias Fathom.Bench.DjangoSchema
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)
    id_arg = [%{"type" => "integer", "value" => "1"}]

    requests = [
      %{"type" => "execute", "stmt" => %{"sql" => "PRAGMA foreign_keys = ON", "args" => []}},
      %{
        "type" => "execute",
        "stmt" => %{"sql" => DjangoSchema.wide_select_sql(), "args" => id_arg}
      },
      %{"type" => "execute", "stmt" => %{"sql" => DjangoSchema.join_sql(), "args" => id_arg}},
      %{"type" => "close"}
    ]

    prev_pool = Application.get_env(:fathom, :connection_pool)
    Application.put_env(:fathom, :connection_pool, true)
    hits = :counters.new(2, [])
    handler = "bench-django-#{System.unique_integer([:positive])}"
    {:ok, _} = Application.ensure_all_started(:telemetry)
    setup_write_path()

    :telemetry.attach(
      handler,
      [:fathom, :shard, :pool_take],
      fn _event, _m, %{result: result}, _ ->
        :counters.add(hits, if(result == :hit, do: 1, else: 2), 1)
      end,
      nil
    )

    try do
      with_wire(opts, "benchdjango", fn client ->
        path = Fathom.Shard.db_path("benchdjango")
        drop_db(path)
        _ = DjangoSchema.build!(path, wide: true, rows: 50)
        :ok = Fathom.Shard.stamp_local_provenance("benchdjango")

        {:ok, _, held} = Filo.Client.execute(%{client | baton: nil}, "SELECT 1")
        held_baton = held.baton

        # Warm: one request puts a handle in the pool (and the statements in its cache).
        {:ok, warm, client} = Filo.Client.pipeline(%{held | baton: nil}, requests)
        django_replies!(warm)
        :counters.put(hits, 1, 0)
        :counters.put(hits, 2, 0)

        {client, us} =
          Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
            c = %{c | baton: nil}
            {t, result} = :timer.tc(fn -> Filo.Client.pipeline(c, requests) end)
            {:ok, replies, c} = result
            django_replies!(replies)
            {c, [t | acc]}
          end)

        {:ok, _, _} = Filo.Client.pipeline(%{client | baton: held_baton}, [%{"type" => "close"}])

        hit = :counters.get(hits, 1)

        unless hit * 2 >= samples,
          do:
            raise(
              "hrana_django_request_rt_us got #{hit} pool hits in #{samples} samples " <>
                "(#{:counters.get(hits, 2)} misses); it is not measuring the pooled-reuse path"
            )

        p50(us)
      end)
    after
      :telemetry.detach(handler)
      restore_env(:connection_pool, prev_pool)
    end
  end

  defp django_replies!([
         %{"type" => "ok"},
         %{"type" => "ok", "response" => %{"result" => %{"cols" => wide, "rows" => [_]}}},
         %{"type" => "ok", "response" => %{"result" => %{"cols" => join, "rows" => [_ | _]}}},
         %{"type" => "ok"}
       ])
       when length(wide) == 29 and length(join) == 36,
       do: :ok

  defp django_replies!(other),
    do:
      raise(
        "hrana_django_request_rt_us: unexpected replies (pragma served, 29-col row, 36-col " <>
          "join row expected): #{inspect(other, limit: 8, printable_limit: 120)}"
      )

  defp pooled_served_query!(handle) do
    {:ok, %Filo.StmtResult{rows: rows}} =
      ShardExecutor.execute(handle, %Filo.Stmt{sql: "SELECT * FROM ks_scalars LIMIT 50", args: []})

    if rows == [], do: raise("served_pooled read 0 rows - the page/statement caches are cold")
    :ok
  end

  defp open_served_shard(t, i, rows, stmts \\ @served_stmts, schema \\ :keystone)

  defp open_served_shard(t, i, _rows, _stmts, :django) do
    id = "bench_served_django_#{t}_#{i}"
    {:ok, pid, ref, path} = Shards.checkout(id)
    %{tables: tables, indexes: indexes} = Fathom.Bench.DjangoSchema.build!(path)

    if tables < 40 or indexes < 80,
      do:
        raise(
          "served_django: fixture has #{tables} tables / #{indexes} indexes - not Django-sized"
        )

    {:ok, conn} = Connection.open(path)

    {:ok, %{rows: got}} =
      Connection.query(
        conn,
        "SELECT * FROM #{Fathom.Bench.DjangoSchema.probe_table()} LIMIT 50",
        []
      )

    if got == [], do: raise("served_django read 0 rows - the page/statement caches are cold")
    {id, pid, ref, conn}
  end

  defp open_served_shard(t, i, rows, stmts, :keystone) do
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

    prime_wide_statements(conn, stmts)

    {id, pid, ref, conn}
  end

  # Primes `n` DISTINCT wide statements (all 29 `ks_scalars` columns, rotated per statement, over
  # three predicate shapes ORMs emit: point, range + ORDER BY, filtered range), the way a Django
  # model's many queries populate a connection's statement cache. The cache is keyed by SQL text,
  # so each statement is textually distinct; each is parameterised, as real traffic is. Asserts
  # every statement returned rows and that the cache actually holds them afterwards: a bench whose
  # cache did not fill would report the old single-statement regime under a new name.
  defp prime_wide_statements(_conn, n) when n <= 1, do: :ok

  defp prime_wide_statements(conn, n) do
    {:ok, %{rows: info}} = Connection.query(conn, "PRAGMA table_info(ks_scalars)", [])
    cols = Enum.map(info, fn [_cid, name | _] -> name end)
    width = length(cols)

    shapes = [
      {"WHERE id = ?", [1]},
      {"WHERE id > ? ORDER BY id LIMIT 10", [1]},
      {"WHERE c_integer IS NOT NULL AND id >= ? ORDER BY id DESC LIMIT 5", [1]}
    ]

    for k <- 0..(n - 1) do
      {tail, args} = Enum.at(shapes, rem(k, length(shapes)))
      {a, b} = Enum.split(cols, rem(k, width))
      sql = "SELECT " <> Enum.join(b ++ a, ", ") <> " FROM ks_scalars " <> tail
      {:ok, %{rows: got}} = Connection.query(conn, sql, args)
      if got == [], do: raise("served bench: primed statement #{k} returned no rows")
    end

    # Connection keeps its statement cache in the calling process's dictionary, keyed by conn.
    {_seq, cache} = Process.get({Connection, :stmt_cache, conn}, {0, %{}})

    # Full means every primed statement is held, OR the byte budget (expert review 2026-10-20-2027
    # #7b) is what stopped it: an evicting cache sits within one wide entry (~19 KiB) of the budget.
    # Either way the series measures a cache at its steady-state cap.
    held = cache |> Map.values() |> Enum.map(&elem(&1, 3)) |> Enum.sum()
    budget = Application.get_env(:fathom, :stmt_cache_bytes, 256 * 1024)
    at_budget? = is_integer(budget) and budget > 0 and held >= budget - 32 * 1024

    if map_size(cache) < n and not at_budget?,
      do:
        raise(
          "served bench: statement cache holds #{map_size(cache)}/#{n} (#{held} B) - measuring nothing"
        )

    :ok
  end

  # --- the SERVED regime with replication ON ---------------------------------

  @served_repl_n 32
  @repl_followers 2

  @doc """
  `served_repl_kb_per_shard` and `follower_kb_per_shard` — the `served/1` regime with A2 replication
  ON (perf review 2026-10-14 #17). WATCH-ONLY.

  Every capacity number in the docs was measured with replication OFF, and prod runs it ON, so the
  density that actually binds a replicating fleet was unmeasured. This holds N shards each under a
  live connection that has run a query AND has been replicated to a quorum, and reports per shard:

    * `total_kb` — BEAM memory delta of THIS VM (coordinator + connection + the per-shard `Session`
      + the shippers' bookkeeping). **Do NOT read `total_kb - served_kb_per_shard` as the cost of
      replication** (that sentence used to be here; expert review 2026-10-21 #29): the arm also
      runs its own CREATE TABLE + INSERT, which costs ~+41-45 KiB of SQLite write-path memory, and
      the first commit's seed checkpoint frees ~25-39 KiB again, so the delta is mostly write noise
      and a 2x Session growth would vanish in it. `total_kb` is a "served + replicating + written"
      density figure and nothing more.
    * `session_kb` (`repl_session_kb_per_shard`) — the replication `Session` process alone
      (`Process.info` memory plus its refc binaries) per shard. Deterministic; this is the number
      that attributes replication memory (~5 KiB per shard measured by `repl_mem.exs`).
    * `follower_kb` — BEAM memory delta of the FOLLOWER VM, per shard per replica. The followers
      live in a separate `:peer` VM so their memory is not folded into the primary's number
      (in-process loopback followers would be counted in `total_kb` and could not be split out).

  `follower_kb` is NEAR THE ALLOCATOR'S NOISE FLOOR: a follower keeps its replica on disk, so its BEAM
  footprint per shard is ~1-2 KiB in a dev build and reads ~-1 KiB (GC'd garbage from the previous
  trial outweighing the new state) in the prod-compiled bench. Treat it as "about zero"; the number
  to watch is `total_kb`. Trial 0 is a warm-up outlier (prod: 235/30 KiB vs 172/-1), which the
  median discards.

  The peer VM is a bare node, so `start_peer_followers/3` copies the primary's `replication_*` wire
  env (and signing secret) into it. Without that a PROD-compiled primary (frame signing and the
  connection nonce default ON in prod) and a default-env peer cannot complete the handshake and
  the shippers time out: that is what got this metric reverted in 378b028b.

  It is scoped to its own setup — followers, shippers and a `Fleet` started here and torn down in
  `after` — and does not touch the shared `setup/1` topology (AGENTS.md: changing it changes every
  metric). Disk is not counted; this is BEAM memory only, like its siblings.

  PRECONDITIONS are asserted, because a replication arm that silently did not replicate would
  report the OFF number under a new name: replication is on, every shipper is connected, one
  `Session` exists per shard, and every shard's replica is present on every follower.
  """
  @spec served_repl(keyword()) :: %{
          total_kb: float(),
          follower_kb: float(),
          session_kb: float(),
          blocks_kb: float(),
          sqlite_kb: float()
        }
  def served_repl(opts \\ []) do
    n = Keyword.get(opts, :served_repl_n, @served_repl_n)
    trials = Keyword.get(opts, :trials, @default_trials)
    rows = Keyword.get(opts, :served_rows, @served_rows)

    with_replication(opts, fn peer, names ->
      results = times(trials, fn t -> repl_trial(t, n, rows, peer, names) end)

      %{
        total_kb: results |> Enum.map(&elem(&1, 0)) |> median(),
        follower_kb: results |> Enum.map(&elem(&1, 1)) |> median(),
        session_kb: results |> Enum.map(&elem(&1, 2)) |> median(),
        blocks_kb: results |> Enum.map(&elem(&1, 3)) |> median(),
        sqlite_kb: results |> Enum.map(&elem(&1, 4)) |> median()
      }
    end)
  end

  # The replication-ON scratch topology shared by `served_repl/1` and `hrana_write_rt_us`: a `:peer`
  # VM holding the followers, a `Fleet` started here, all torn down in `after`. Calls
  # `fun.(peer, follower_names)` once every shipper is connected. Extracted unchanged from
  # `served_repl/1` (expert review 2026-10-17 #8) so the write metric cannot drift from it.
  defp with_replication(opts, fun) do
    setup(opts)

    root = Path.join(tmp_dir(), "served_repl_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    {:ok, peer, _node} =
      :peer.start(%{
        connection: :standard_io,
        wait_boot: 30_000,
        args: Enum.flat_map(:code.get_path(), fn dir -> [~c"-pa", dir] end)
      })

    names = for i <- 1..@repl_followers, do: :"bench_rf#{i}_#{System.unique_integer([:positive])}"

    # The peer VM is a bare node with NO app env, so it runs the replication wire at its code
    # defaults (no frame signing, no connection nonce) while a PROD-compiled primary has them ON
    # (config.exs) — the handshake then times out and the shippers never connect (the failure that
    # got this metric reverted in 378b028b). Hand the peer the primary's wire-affecting env so both
    # ends speak the same protocol, and make sure a signing secret exists on both when signing is on
    # (prod boot refuses to start without one; a bench has none configured).
    secret_keys = [:replication_hmac_secret, :hrana_token_secret]

    prev_secret =
      Map.new(secret_keys, &{&1, Application.get_env(:fathom, &1)})

    if Enum.all?(secret_keys, &(Application.get_env(:fathom, &1) in [nil, ""])),
      do: Application.put_env(:fathom, :replication_hmac_secret, String.duplicate("b", 32))

    wire_env =
      for {k, v} <- Application.get_all_env(:fathom),
          String.starts_with?(Atom.to_string(k), "replication_") or k in secret_keys,
          do: {k, v}

    ports = peer_call(peer, __MODULE__, :start_peer_followers, [names, root, wire_env])

    # `:replication_recovery_symmetric`: the bench's followers ARE every other node, and under
    # MIX_ENV=prod the recovery boot guard (`Fleet.recovery_peers_problem/0`) would otherwise refuse
    # this `Fleet.start_link/1` and crash every replication-ON metric.
    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
      :replication_recovery_symmetric
    ]

    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_recovery_symmetric, true)
    Application.put_env(:fathom, :replication_quorum, 1)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({port, i} <- Enum.with_index(ports, 1), do: {"bench-f#{i}", ~c"127.0.0.1", port})
    )

    {:ok, fleet} = Fathom.Shard.Replication.Fleet.start_link([])

    try do
      await_shippers!()
      fun.(peer, names)
    after
      Supervisor.stop(fleet)
      :peer.stop(peer)
      Enum.each(keys, &restore_env(&1, Map.fetch!(prev, &1)))
      Enum.each(secret_keys, &restore_env(&1, Map.fetch!(prev_secret, &1)))
      Fathom.Shard.Replication.FrameAuth.forget_key()
      File.rm_rf(root)
    end
  end

  @doc false
  # Runs INSIDE the peer VM: adopts the primary's replication wire env, starts the named followers
  # (unlinked from the short-lived call process) and returns their ports.
  def start_peer_followers(names, root, wire_env) do
    for {k, v} <- wire_env, do: Application.put_env(:fathom, k, v)

    Enum.map(names, fn name ->
      {:ok, pid} = Follower.start_link(name: name, port: 0, dir: Path.join(root, to_string(name)))
      Process.unlink(pid)
      {:ok, port} = Follower.port(name)
      port
    end)
  end

  @doc false
  # Peer-side GC + memory read, so the follower delta is taken the same way `collect_all/0` takes
  # the primary's.
  def peer_memory_total do
    collect_all()
    :erlang.memory(:total)
  end

  defp peer_call(peer, m, f, a), do: :peer.call(peer, m, f, a, 60_000)

  defp await_shippers! do
    alias Fathom.Shard.Replication.{Fleet, Shipper}
    deadline = System.monotonic_time(:millisecond) + 10_000

    ready? = fn ->
      shippers = Fleet.shippers()
      length(shippers) == @repl_followers and Enum.all?(shippers, &Shipper.connected?/1)
    end

    Stream.repeatedly(fn ->
      cond do
        ready?.() ->
          :up

        System.monotonic_time(:millisecond) > deadline ->
          :timeout

        true ->
          Process.sleep(20)
          :wait
      end
    end)
    |> Enum.find(&(&1 != :wait))
    |> case do
      :up ->
        :ok

      :timeout ->
        raise "served_repl: shippers never connected — this would measure replication OFF"
    end
  end

  defp repl_trial(t, n, rows, peer, names) do
    alias Fathom.Shard.Replication.{Fleet, Session}

    unless Fleet.replicating?(),
      do: raise("served_repl: replication is not on — measuring nothing")

    collect_all()
    before_mem = memory_snapshot()
    before_local = :erlang.memory(:total)
    before_remote = peer_call(peer, __MODULE__, :peer_memory_total, [])

    handles = Enum.map(1..n, fn i -> open_repl_shard(t, i, rows) end)
    ids = Enum.map(handles, &elem(&1, 0))

    # Preconditions, asserted rather than trusted (the `flush_p50_us` 2 µs lesson).
    sessions = Registry.count(Fathom.Shard.Replication.SessionRegistry)

    if sessions < n,
      do: raise("served_repl: #{sessions}/#{n} replication Sessions — measuring nothing")

    for name <- names do
      held = peer_call(peer, Follower, :replica_shard_ids, [name])

      if ids -- held != [],
        do:
          raise("served_repl: follower #{name} holds no replica of #{length(ids -- held)} shards")
    end

    collect_all()
    local = :erlang.memory(:total) - before_local
    det = memory_delta(before_mem, memory_snapshot(), Enum.map(handles, &elem(&1, 1)))
    log_memory_delta("served_repl", t, n, det)
    remote = peer_call(peer, __MODULE__, :peer_memory_total, []) - before_remote
    session_kb = session_bytes!(ids) / n / 1024

    Enum.each(handles, fn {id, _pid, _ref, _conn} = h ->
      Session.stop(id)
      close_served_shard(h)
      for name <- names, do: peer_call(peer, Follower, :forget, [name, id])
    end)

    {local / n / 1024, remote / n / length(names) / 1024, session_kb, det.blocks / n / 1024,
     det.sqlite / n / 1024}
  end

  # Σ over the shards' replication Sessions of `Process.info(:memory)` (heap, stack, mailbox) plus
  # the refc binaries each holds (`Process.info(:binary)` sizes). Deterministic and needs no
  # allocator delta, unlike `served_repl`'s `total_kb`, which also contains the arm's own
  # CREATE TABLE + INSERT (expert review 2026-10-21 #29: Session ~5 KiB of a ~246 KiB total, so a
  # 2x Session growth was invisible in it). Raises on a missing Session or a zero size.
  defp session_bytes!(ids) do
    bytes =
      Enum.map(ids, fn id ->
        case Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id) do
          [{pid, _}] ->
            {:memory, mem} = Process.info(pid, :memory)
            {:binary, bins} = Process.info(pid, :binary)
            mem + (bins |> Enum.map(&elem(&1, 1)) |> Enum.sum())

          other ->
            raise "served_repl: no replication Session for #{id} (#{inspect(other)}) - " <>
                    "repl_session_kb_per_shard would measure nothing"
        end
      end)

    if Enum.any?(bytes, &(&1 <= 0)),
      do: raise("served_repl: a replication Session reported 0 bytes - measuring nothing")

    Enum.sum(bytes)
  end

  defp open_repl_shard(t, i, rows) do
    alias Fathom.Shard.Replication.Session

    {id, pid, ref, conn} = open_served_shard(t, i, rows)
    path = Fathom.Shard.db_path(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE IF NOT EXISTS repl_probe (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO repl_probe VALUES (1)", [])

    commit_until_quorum!(id, path, pid, 10)

    {id, pid, ref, conn}
  end

  # The first commit finds the follower unseeded and answers with the seed; keep committing until
  # a quorum acks (bounded - a stuck seed must fail loudly, not hang the bench).
  defp commit_until_quorum!(id, path, pid, pause_ms) do
    alias Fathom.Shard.Replication.Session
    deadline = System.monotonic_time(:millisecond) + 10_000

    Stream.repeatedly(fn ->
      case Session.commit(id, path <> "-wal", pid) do
        :ok ->
          :ok

        _ ->
          if System.monotonic_time(:millisecond) > deadline,
            do: raise("served_repl: shard #{id} never reached a replication quorum"),
            else: Process.sleep(pause_ms)
      end
    end)
    |> Enum.find(&(&1 == :ok))
  end

  @repl_reopen_samples 8

  @doc """
  `repl_first_commit_after_reopen_us` / `repl_first_commit_reset_rounds` - the FIRST commit after a
  coordinator reopen with A2 replication ON (expert review 2026-10-22 r8 #5): the most expensive
  replication path per tenant session. Each sample opens a shard, replicates it to quorum, drains it
  (Session stopped, coordinator and local copy gone), reopens it and times a `Session.commit` of one
  fresh INSERT until a quorum acks. The new Session starts with no follower state, so that commit
  pays the lineage-bump reset round, the seed and the retry (r8 #16 removes the doomed reset round)
  plus the Session warm-up (r8 #17's per-start `:memory:` registry open). `served_repl` only ever
  measured a coordinator's first life.

    * `p50_us` - median of #{@repl_reopen_samples} timed first commits (one untimed warm-up shard).
    * `resets` - median count of `[:fathom, :replication, :commit_stage]` `:collect` rounds with
      `reset: true` in that window (counted by telemetry because `quorum IMPOSSIBLE` is rate-limited
      to 5 lines/s per node and cannot be counted from the log). Expect 1+ today; fewer after #16.

  Preconditions (raises otherwise): replication is on, the reopened coordinator is a NEW pid, the
  reopened file was pulled back with the shard's rows, and the commit reached quorum.
  Own scoped topology (`with_replication/2`). WATCH-ONLY (new series).
  """
  @spec repl_first_commit_stats(keyword()) :: %{p50_us: float(), resets: float()}
  def repl_first_commit_stats(opts \\ []) do
    alias Fathom.Shard.Replication.{Fleet, Session}
    samples = Keyword.get(opts, :repl_reopen_samples, @repl_reopen_samples)
    rows = Keyword.get(opts, :served_rows, @served_rows)
    setup_write_path()

    with_replication(opts, fn peer, names ->
      unless Fleet.replicating?(),
        do: raise("repl_first_commit_after_reopen_us: replication is not on - measuring nothing")

      {:ok, _} = Application.ensure_all_started(:telemetry)
      resets = :counters.new(1, [])
      handler = "bench-reopen-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :replication, :commit_stage],
        fn _e, _m, meta, _ ->
          if meta.stage == :collect and meta.reset == true, do: :counters.add(resets, 1, 1)
        end,
        nil
      )

      try do
        results =
          for i <- 0..samples do
            {id, pid, ref, conn} = open_repl_shard(1000, i, rows)

            {:ok, %{rows: [[expected]]}} =
              Connection.query(conn, "SELECT count(*) FROM ks_scalars", [])

            :ok = Connection.close(conn)
            Session.stop(id)

            # The bench writes through a raw connection, which does not mark the shard dirty; without
            # this the drain would drop the file unflushed and the reopen would find an empty shard.
            Fathom.Shard.WriteCounter.bump(id)
            Fathom.Shard.checkin(pid, ref)
            :ok = Shards.drain(id, 5_000)

            {:ok, pid2, ref2, path} = Shards.checkout(id)

            if pid2 == pid,
              do: raise("repl_first_commit_after_reopen_us: the coordinator was not reopened")

            {:ok, conn2} = Connection.open(path)

            {:ok, %{rows: [[count]]}} =
              Connection.query(conn2, "SELECT count(*) FROM ks_scalars", [])

            if expected == 0 or count != expected,
              do:
                raise(
                  "repl_first_commit_after_reopen_us: reopened file has #{count}/#{expected} rows"
                )

            {:ok, _} = Connection.query(conn2, "INSERT INTO repl_probe VALUES (2)", [])
            :counters.put(resets, 1, 0)
            {us, :ok} = :timer.tc(fn -> commit_until_quorum!(id, path, pid2, 1) end)
            r = :counters.get(resets, 1)

            Connection.close(conn2)
            Session.stop(id)
            Fathom.Shard.checkin(pid2, ref2)
            Shards.drain(id, 5_000)
            for name <- names, do: peer_call(peer, Follower, :forget, [name, id])
            {us, r}
          end

        timed = tl(results)

        %{
          p50_us: p50(Enum.map(timed, &elem(&1, 0))),
          resets: median(Enum.map(timed, &elem(&1, 1)))
        }
      after
        :telemetry.detach(handler)
      end
    end)
  end

  # --- write + auth wire metrics (expert review 2026-10-17 #8) ---------------

  @doc """
  `hrana_write_rt_us` - median round trip of an autocommit `INSERT ... RETURNING` over real Hrana
  (a warm stream, like `hrana_rt_us`) with A2 replication ON and a quorum of loopback followers.

  Why it exists (expert review 2026-10-17 #8): `hrana_rt_us`/`hrana_open_rt_us` are `SELECT 1`
  with replication off, so nothing in the gate executed `maybe_replicate`, the Session commit, the
  shipper push or the quorum wait - how a ~5% per-write cost survived. Own scoped setup: the
  followers/Fleet from `with_replication/2` (shared with `served_repl/1`) plus the dirty-flag
  tables; the shared `setup/1` is untouched.

  PRECONDITIONS are asserted, because a write metric that did not replicate would report the
  replication-OFF number under a new name: replication is on, and the timed loop produced at least
  one quorum ship (`[:fathom, :replication, :commit_stage]`, stage `:collect`, fired only when a
  push round actually ran) for 90% of the samples.
  """
  @spec hrana_write_rt_us(keyword()) :: float() | nil
  def hrana_write_rt_us(opts \\ []), do: hrana_write_rt(opts)

  defp hrana_write_rt(opts) do
    alias Fathom.Shard.Replication.{Fleet, Session}
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)
    shard = "benchwrite"
    setup_write_path()

    with_replication(opts, fn _peer, _names ->
      unless Fleet.replicating?() and Session.enabled?() == true,
        do: raise("hrana_write_rt_us: replication is not on - it would measure the OFF path")

      ships = :counters.new(1, [])
      handler = "bench-write-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:fathom, :replication, :commit_stage],
        fn _event, _m, %{stage: stage}, _ ->
          if stage == :collect, do: :counters.add(ships, 1, 1)
        end,
        nil
      )

      try do
        result =
          with_wire(opts, shard, fn client ->
            insert = "INSERT INTO bench_w (v) VALUES (1) RETURNING id"

            # The first commits find the follower unseeded and answer 503 until the seed lands
            # (same as `open_repl_shard/3`); retry, bounded, so a stuck seed fails loudly.
            {:ok, _, client} =
              write_with_retry(
                client,
                "CREATE TABLE IF NOT EXISTS bench_w (id INTEGER PRIMARY KEY, v INTEGER)"
              )

            {:ok, _, client} = write_with_retry(client, insert)
            :counters.put(ships, 1, 0)

            {_client, us} =
              Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
                {t, result} = :timer.tc(fn -> Filo.Client.execute(c, insert) end)
                {:ok, _, c} = result
                {c, [t | acc]}
              end)

            shipped = :counters.get(ships, 1)

            unless shipped * 10 >= samples * 9,
              do:
                raise(
                  "hrana_write_rt_us: #{shipped} quorum ships in #{samples} writes - " <>
                    "it is not measuring the replicated write path"
                )

            p50(us)
          end)

        Session.stop(shard)
        result
      after
        :telemetry.detach(handler)
      end
    end)
  end

  # --- wide-row wire metric (expert review 2026-10-19 #10) -------------------

  @doc """
  `hrana_wide_rt_us` - median round trip of a warm-stream 8-column point `SELECT` over real
  Hrana HTTP (`rows: :json`), the shape a Django model fetch has. `hrana_rt_us` returns one
  unnamed cell, so a per-column cost (Filo rebuilding a map per column per execution, expert
  review 2026-10-19 #10) is invisible to it. The one row is seeded straight into the shard file;
  the shared `setup/1` is untouched.

  PRECONDITION asserted on every sample: the reply carries all 8 columns and the seeded row, so
  a broken query cannot report a fast empty result.
  """
  @spec hrana_wide_rt_us(keyword()) :: float() | nil
  def hrana_wide_rt_us(opts \\ []) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    with_wire(opts, "benchwide", fn client ->
      # Seed through the shard's own file, as hrana_pooled_pipeline does: the prod-compiled bench
      # has replication configured on but none of its processes started, so a wire write raises
      # (`unknown registry: ...SessionRegistry`, first prod run of this metric, 2026-10-19).
      path = Fathom.Shard.db_path("benchwide")
      drop_db(path)
      {:ok, db} = Exqlite.Sqlite3.open(path)

      :ok =
        Exqlite.Sqlite3.execute(
          db,
          "CREATE TABLE bench_wide (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, " <>
            "price REAL, note TEXT, flags INTEGER, payload BLOB, created_at TEXT); " <>
            "INSERT INTO bench_wide VALUES (1, 'widget', 3, 9.5, 'n', 0, x'00ff', " <>
            "'2026-10-19T00:00:00Z')"
        )

      :ok = Exqlite.Sqlite3.close(db)
      :ok = Fathom.Shard.stamp_local_provenance("benchwide")

      select =
        "SELECT id, name, qty, price, note, flags, payload, created_at FROM bench_wide WHERE id = 1"

      {:ok, _, client} = Filo.Client.execute(client, select)

      {_client, us} =
        Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
          {t, result} = :timer.tc(fn -> Filo.Client.execute(c, select) end)
          {:ok, %Filo.StmtResult{cols: cols, rows: rows}, c} = result

          unless length(cols) == 8 and match?([[1 | _]], rows),
            do:
              raise(
                "hrana_wide_rt_us: expected 8 columns and row 1, got #{inspect({cols, rows})}"
              )

          {c, [t | acc]}
        end)

      assert_load_recorded!("benchwide", "hrana_wide_rt_us")
      p50(us)
    end)
  end

  # --- observed wire + poller tick (expert review 2026-10-19 #26, #25) --------

  @doc """
  `hrana_rt_observed_us` - `hrana_rt_us` with the Prometheus reporter children running, the shape
  prod has and every other wire metric lacks (expert review 2026-10-19 #26): `Fathom.Telemetry`'s
  `reporter_children/0` attach the `[:fathom, :shard, :query]` and checkout handlers
  (`FastHistogram`, `FastCounter`, Core), so each statement pays the telemetry dispatch. The
  ShardLoad/ShardLatency tables are present via `setup/1` (#4).

  PRECONDITION asserted: after the timed loop the scrape reports a query-duration count of at least
  the number of samples, otherwise the reporters were not attached and this would silently be
  `hrana_rt_us` under another name. The reporters are stopped (handlers detached) afterwards.
  Watch-only; the difference against `hrana_rt_us` is the telemetry cost.
  """
  @spec hrana_rt_observed_us(keyword()) :: float() | nil
  def hrana_rt_observed_us(opts \\ []) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)
    # `mix fathom.bench` only runs app.config, and the reporters register :telemetry handlers.
    {:ok, _} = Application.ensure_all_started(:telemetry)

    {:ok, sup} =
      Supervisor.start_link(Fathom.Telemetry.reporter_children(), strategy: :one_for_one)

    try do
      with_wire(opts, "benchobs", fn client ->
        {:ok, _, client} = Filo.Client.execute(client, "SELECT 1")

        {_client, us} =
          Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
            {t, result} = :timer.tc(fn -> Filo.Client.execute(c, "SELECT 1") end)
            {:ok, _, c} = result
            {c, [t | acc]}
          end)

        observed = query_duration_count(Fathom.Telemetry.scrape())

        unless observed >= samples,
          do:
            raise(
              "hrana_rt_observed_us: scrape counted #{observed} query durations for #{samples} " <>
                "samples - the reporters are not attached, this would measure the unobserved path"
            )

        assert_load_recorded!("benchobs", "hrana_rt_observed_us")
        p50(us)
      end)
    after
      Supervisor.stop(sup)
    end
  end

  # The `_count` sample of the per-query duration histogram in a Prometheus exposition (0 if absent).
  defp query_duration_count(scrape) do
    case Regex.run(~r/^fathom_shard_query_duration\w*_count(?:\{[^}]*\})? (\d+)/m, scrape) do
      [_, n] -> String.to_integer(n)
      nil -> 0
    end
  end

  @tick_rows 20_000

  @doc """
  `observability_tick_us` - one pass of the 10 s observability tick at a realistic row count
  (expert review 2026-10-19 #25): every `Fathom.Telemetry` poller measurement
  (`Fathom.Admin.Measurements.*`, `LeaseHealth`, `LagGauge`, active shards) plus one
  `MetricsCollector.hot_shards/2` pass, with #{@tick_rows} rows seeded into ShardLoad, WriteCounter
  and FlushWatermark. Round 1 (#1, #14-#16) and round 2 (#27) removed several O(shards) walks and
  none of those fixes was gated.

  Like the real poller, a measurement that raises is skipped rather than fatal (some need the
  supervision tree the bench does not boot); PRECONDITIONS asserted: `durability/0` ran and its
  event saw every seeded watermark row (its walk is the O(rows) one), and the `hot_shards/2` pass
  baselined every seeded ShardLoad row. Median of trials, own scoped setup, tables emptied after.
  Watch-only.
  """
  @spec observability_tick_us(keyword()) :: float()
  def observability_tick_us(opts \\ []) do
    rows = Keyword.get(opts, :tick_rows, @tick_rows)
    trials = Keyword.get(opts, :trials, @default_trials)
    setup(opts)
    setup_write_path()
    # `disk/0` reads :disksup, which `mix fathom.bench` (app.config only) does not start.
    {:ok, _} = Application.ensure_all_started(:os_mon)
    # Prod shape: the collector/watermarks are on (off in the test env) and so is :shard_load.
    restore_load = put_env_scoped(:shard_load, true)
    restore_collector = put_env_scoped(:metrics_collector, true)
    gen = Fathom.Shard.WriteCounter.generation()

    for i <- 1..rows do
      id = "benchtick_#{i}"
      Fathom.ShardLoad.record_query(id, 1, 1)
      Fathom.Shard.WriteCounter.bump(id)
      Fathom.Admin.FlushWatermark.record(id, 0, gen)
    end

    # The measurement runs in this process, so the handler can just message it.
    parent = self()
    handler = "bench-tick-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :durability, :rpo],
      fn _e, m, _meta, _ -> send(parent, {:bench_rpo, m}) end,
      nil
    )

    try do
      measurements = [
        {Fathom.Telemetry, :measure_active_shards, []},
        {Fathom.Admin.Measurements, :connections, []},
        {Fathom.Admin.Measurements, :vm_limits, []},
        {Fathom.Admin.Measurements, :durability, []},
        {Fathom.Admin.Measurements, :disk, []},
        {Fathom.Admin.Measurements, :replication, []},
        {Fathom.Admin.Measurements, :flush_gate, []},
        {Fathom.Admin.Measurements, :schedulers, []},
        {Fathom.Admin.Measurements, :memory_breakdown, []},
        {Fathom.Admin.Measurements, :recorder, []},
        {Fathom.Admin.LeaseHealth, :measure, []},
        {Fathom.Shard.Replication.LagGauge, :measure, []}
      ]

      tick = fn ->
        Enum.each(measurements, fn {m, f, a} ->
          try do
            apply(m, f, a)
          catch
            _, _ -> :skipped
          end
        end)

        {hot, next} = Fathom.Admin.MetricsCollector.hot_shards(%{}, 10)
        {length(hot), map_size(next)}
      end

      tick.()

      m =
        receive do
          {:bench_rpo, m} -> m
        after
          0 -> nil
        end

      unless is_map(m) and m.watermark_rows == rows and m.dirty_shards == rows,
        do:
          raise(
            "observability_tick_us: durability saw #{inspect(m)}, expected #{rows} dirty " <>
              "watermark rows - the tick is not walking the seeded tables"
          )

      samples =
        times(trials, fn _ ->
          {us, {_hot, baselined}} = :timer.tc(tick)

          unless baselined == rows,
            do: raise("observability_tick_us: hot_shards baselined #{baselined} of #{rows} rows")

          us
        end)

      median(samples)
    after
      :telemetry.detach(handler)
      Fathom.ShardLoad.reset()
      Fathom.Shard.WriteCounter.reset()
      Fathom.Admin.FlushWatermark.reset()

      restore_load.()
      restore_collector.()
    end
  end

  @idle_n 200
  # Prod's flush interval and idle stop (see `idle_stats/1`). With them `hibernate_after` is
  # 30 s (r8 #12: min(376 s floor, idle / 2)); the wait covers that plus the open loop and must stay
  # under the 60 s idle stop, or coordinators would drop before they are counted.
  @idle_flush_interval_ms 300_000
  @idle_stop_ms 60_000
  @idle_wait_ms 45_000

  @doc false
  @spec idle_wait_ms() :: pos_integer()
  def idle_wait_ms, do: @idle_wait_ms

  @doc """
  `idle_kb_per_shard` / `idle_ets_b_per_shard` / `idle_hibernated_frac` - the IDLE, checked-in shard
  with prod's per-shard state switched on (expert review 2026-10-22 r8 #2). Every other density arm
  holds a live checkout under a setup with `:shard_load` and the FlushWatermark off; prod has both on
  and most open shards are idle, so ~1.2 KB/shard of ETS/counter rows plus the coordinator's own
  heap were measured by nothing.

  Own scoped setup (the shared `setup/1` is untouched): `:shard_load` and `:metrics_collector` on,
  ShardLoad/ShardLatency/Lru/WriteCounter/FlushWatermark running, heartbeat mode (a `Heartbeat` is
  started and every coordinator's `acquire_gen` is asserted non-nil), prod-shaped timers (flush
  interval 300 s, idle stop 60 s - prod's, so `hibernate_after` is whatever `Fathom.Shard` derives
  for prod: 30 s with r8 #12). N shards are written through `ShardExecutor` and checked in, then the arm waits
  up to #{@idle_wait_ms} ms for the coordinators to hibernate.

    * `idle_kb_per_shard` - `Process.info(:memory)` + refc binaries over the coordinators, per shard.
    * `idle_ets_b_per_shard` - BYTES per shard in the per-shard ETS tables (rows + indexes) plus the
      NIF counter arrays those rows point at (`std_alloc` live-block delta around the opens, so a
      `:write_concurrency` counter regression shows up here).
    * `idle_hibernated_frac` - share of coordinators whose `current_function` is
      `{:gen_server, :loop_hibernate, 4}`. ~0.9 with r8 #12 (hibernate after 30 s; measured dev
      0.88-0.92 vs 0.00 with #12 backed out, idle_kb 4.2 vs 9.0). Not 1.0, and correctly so: r8
      #11 draws each coordinator's FIRST flush tick uniform over (0, 300 s], so ~10% of these
      dirty shards take that tick inside the wait and restart their 30 s hibernate countdown -
      prod's behaviour too. 0.0 without #12 (`hibernate_after` 376 s, past the idle stop). REPORTED, not asserted: a regression shows
      as a drop to 0.0 here and a ~2.6 KB rise in `idle_kb_per_shard`. In prod an idle coordinator
      lives 60 s and is hibernated for its second 30 s, so this is the hibernated half's footprint.

  Preconditions (raises otherwise): every coordinator is in heartbeat mode, and the ShardLoad,
  ShardLatency, WriteCounter and FlushWatermark tables each hold at least N rows. WATCH-ONLY.
  """
  @spec idle_stats(keyword()) :: %{
          kb: float(),
          ets_b: float(),
          hibernated: float(),
          hibernate_after_ms: pos_integer()
        }
  def idle_stats(opts \\ []) do
    n = Keyword.get(opts, :idle_n, @idle_n)
    wait_ms = Keyword.get(opts, :idle_wait_ms, @idle_wait_ms)

    setup(opts)

    restores = [
      put_env_scoped(:shard_load, true),
      put_env_scoped(:metrics_collector, true),
      # PROD'S TIMERS, BOTH OF THEM: config/runtime.exs sets a 300 s flush interval in prod and
      # leaves `:shard_idle_ms` at `Fathom.Shard`'s 60 s default (no deploy sets SHARD_IDLE_MS;
      # only the rig's 20 s and sweep-only 900 s overrides exist). This arm used to pin a 600 s
      # idle stop, which is not prod's shape AND is the one regime where r8 #12 does not apply
      # (#12 caps `hibernate_after` only when interval > idle), so it reported 0.0 hibernated on
      # code where every prod idle coordinator hibernates at 30 s. 60 s also outlasts the wait
      # below, so no coordinator idle-stops mid-measurement.
      put_env_scoped(:shard_flush_interval_ms, @idle_flush_interval_ms),
      put_env_scoped(:shard_idle_ms, @idle_stop_ms),
      # Prod config turns replication on, but the bench starts no replication supervisor, so a
      # coordinator write raises "unknown registry: ...SessionRegistry" (the round-7 hrana_sdk_rt
      # crash, 1d7b73a4). This arm measures the idle coordinator, not A2.
      put_env_scoped(:replication_enabled, false)
    ]

    # Coordinators in heartbeat mode subscribe to the lapse topic; without the PubSub they log a
    # warning per open and skip the subscription, which is not prod's per-coordinator state.
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)

    pubsub =
      case Phoenix.PubSub.Supervisor.start_link(name: Fathom.PubSub) do
        {:ok, pid} ->
          Process.unlink(pid)
          pid

        {:error, {:already_started, _}} ->
          nil
      end

    owned =
      for mod <- [
            Fathom.Shard.WriteCounter,
            Fathom.Admin.FlushWatermark,
            Fathom.Shards.Lru,
            Fathom.Shards.KnownShards,
            Fathom.Shard.Heartbeat
          ],
          pid = start_owned(mod),
          do: pid

    ids = for i <- 1..n, do: "bench_idle_#{i}"

    try do
      unless Fathom.ShardLoad.enabled?(),
        do: raise("idle_kb_per_shard: :shard_load is off - measuring the wrong topology")

      unless Fathom.Shard.Heartbeat.running?(),
        do: raise("idle_kb_per_shard: no Heartbeat - coordinators would open in legacy mode")

      :erlang.garbage_collect()
      counters_before = sum_alloc_blocks([:std_alloc])

      pids =
        for id <- ids do
          {:ok, conn} = ShardExecutor.open(id)

          {:ok, _} =
            ShardExecutor.execute(conn, %Filo.Stmt{sql: "CREATE TABLE t (v INTEGER)", args: []})

          {:ok, _} =
            ShardExecutor.execute(conn, %Filo.Stmt{sql: "INSERT INTO t VALUES (1)", args: []})

          :ok = ShardExecutor.close(conn)
          [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)

          # Asked HERE, before the wait: a call to a hibernated process wakes it.
          unless is_integer(Fathom.Shard.acquire_gen(pid)),
            do: raise("idle_kb_per_shard: #{id} opened in legacy mode (acquire_gen nil)")

          pid
        end

      for t <- [
            Fathom.ShardLoad,
            Fathom.ShardLatency,
            Fathom.Shard.WriteCounter,
            Fathom.Admin.FlushWatermark
          ] do
        size = :ets.info(t, :size)

        unless is_integer(size) and size >= n,
          do:
            raise("idle_kb_per_shard: #{inspect(t)} has #{inspect(size)} rows, expected >= #{n}")
      end

      hibernated = await_hibernated(pids, System.monotonic_time(:millisecond) + wait_ms)
      :erlang.garbage_collect()

      counter_delta = sum_alloc_blocks([:std_alloc]) - counters_before

      coord_bytes =
        Enum.reduce(pids, 0, fn pid, acc ->
          {:memory, mem} = Process.info(pid, :memory)
          {:binary, bins} = Process.info(pid, :binary)
          acc + mem + (bins |> Enum.map(&elem(&1, 1)) |> Enum.sum())
        end)

      owners =
        Enum.flat_map(
          owned ++ [Process.whereis(Fathom.ShardLoad), Process.whereis(Fathom.ShardLatency)],
          &List.wrap/1
        )

      ets_bytes =
        for t <- :ets.all(), :ets.info(t, :owner) in owners, reduce: 0 do
          acc -> acc + :ets.info(t, :memory) * :erlang.system_info(:wordsize)
        end

      if coord_bytes <= 0 or ets_bytes <= 0,
        do: raise("idle_kb_per_shard: zero coordinator/ETS bytes - measuring nothing")

      %{
        kb: coord_bytes / n / 1024,
        ets_b: (ets_bytes + max(counter_delta, 0)) / n,
        hibernated: hibernated / n,
        # The `hibernate_after` these coordinators were started with under the arm's timers - the
        # arm's own shape check: 30 s under prod's timers with r8 #12, 376 s under the old 600 s
        # idle stop that hid #12 (test/fathom/bench_test.exs pins it).
        hibernate_after_ms:
          Fathom.Shard.hibernate_after_ms(
            Application.get_env(:fathom, :shard_flush_interval_ms),
            Application.get_env(:fathom, :shard_flush_jitter_ratio, 0.25),
            Application.get_env(:fathom, :shard_idle_ms)
          )
      }
    after
      Enum.each(ids, &Shards.drain(&1, 5_000))
      Enum.each(owned, &GenServer.stop/1)
      if pubsub, do: Supervisor.stop(pubsub)
      Enum.each(restores, & &1.())
    end
  end

  # Polls until every pid is hibernated or the deadline passes; returns how many are.
  defp await_hibernated(pids, deadline) do
    count = Enum.count(pids, &hibernated?/1)

    cond do
      count == length(pids) ->
        count

      System.monotonic_time(:millisecond) >= deadline ->
        count

      true ->
        Process.sleep(500)
        await_hibernated(pids, deadline)
    end
  end

  defp hibernated?(pid),
    do:
      Process.info(pid, :current_function) ==
        {:current_function, {:gen_server, :loop_hibernate, 4}}

  # Sets a :fathom env key and returns a function that puts the previous state back.
  defp put_env_scoped(key, value) do
    prev = Application.fetch_env(:fathom, key)
    Application.put_env(:fathom, key, value)

    fn ->
      case prev do
        {:ok, v} -> Application.put_env(:fathom, key, v)
        :error -> Application.delete_env(:fathom, key)
      end
    end
  end

  defp write_with_retry(client, sql, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 10_000

    case Filo.Client.execute(client, sql) do
      {:ok, _, _} = ok ->
        ok

      {:error, _reason, client} ->
        if System.monotonic_time(:millisecond) > deadline,
          do: raise("hrana_write_rt_us: the warm-up write never reached a replication quorum")

        Process.sleep(10)
        write_with_retry(client, sql, deadline)
    end
  end

  @doc """
  `hrana_open_auth_rt_us` - median round trip for a request that must OPEN its stream with Hrana
  auth REQUIRED (the `hrana_open_rt_us` shape plus `HranaAuth.authorize/2`, which also verifies a
  `Phoenix.Token`, re-checks the shard grant and reads the revocation floor).

  Why it exists (expert review 2026-10-17 #8/#17): `with_wire` ran with no `:authorize`, so the
  auth cost paid by every open-per-request client was outside the gate. Own scoped setup: the
  config, a `Revocations` server with a pre-seeded floor (so no sample reads Postgres), and an
  `:authorize` wrapper that counts calls, all restored in `after`; the shared `setup/1` and the
  other wire metrics' listener are untouched.

  PRECONDITIONS asserted: `:required` mode refuses a missing token and a foreign shard's token, the
  floor stayed a cache hit for the whole loop, and `authorize/2` ran once per timed request.
  """
  @spec hrana_open_auth_rt_us(keyword()) :: float() | nil
  def hrana_open_auth_rt_us(opts \\ []), do: hrana_open_auth_rt(opts)

  defp hrana_open_auth_rt(opts) do
    alias Fathom.HranaAuth
    alias Fathom.HranaAuth.Revocations

    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)
    shard = "benchauth"
    keys = [:hrana_auth, :hrana_token_max_age, :hrana_token_secret]
    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})

    Application.put_env(:fathom, :hrana_auth, :required)
    # Prod REFUSES to boot :required without a finite max_age, so measure the finite-expiry path.
    Application.put_env(:fathom, :hrana_token_max_age, 3_600)

    if Application.get_env(:fathom, :hrana_token_secret) in [nil, ""],
      do: Application.put_env(:fathom, :hrana_token_secret, String.duplicate("a", 64))

    revocations =
      case Revocations.start_link([]) do
        {:ok, pid} -> pid
        {:error, {:already_started, _}} -> nil
      end

    try do
      # Signed directly (same salt/claims as `HranaAuth.token_for/2`) rather than through it: the
      # public mint also writes the issuance ledger to Postgres, which a bench must not touch.
      token =
        Phoenix.Token.sign(
          Application.fetch_env!(:fathom, :hrana_token_secret),
          "fathom hrana shard",
          %{"s" => shard, "v" => 1, "e" => System.system_time(:second) + 3_600}
        )

      Revocations.put(shard, 1, nil)

      unless match?({:error, %Filo.Error{status: 401}}, HranaAuth.authorize(shard, nil)) and
               match?({:error, %Filo.Error{status: 401}}, HranaAuth.authorize("other", token)) and
               match?({:ok, {:rw, _}}, HranaAuth.authorize(shard, token)),
             do:
               raise("hrana_open_auth_rt_us: :required auth is not enforcing - measuring nothing")

      calls = :counters.new(1, [])

      authorize = fn arg, tok ->
        :counters.add(calls, 1, 1)
        HranaAuth.authorize(arg, tok)
      end

      with_wire(
        opts,
        shard,
        fn client ->
          {:ok, _, client} = Filo.Client.execute(client, "SELECT 1")
          :counters.put(calls, 1, 0)

          {_client, us} =
            Enum.reduce(1..samples, {client, []}, fn _, {c, acc} ->
              # No baton => Filo.Plug authorizes and opens a new stream for this request.
              c = %{c | baton: nil}
              {t, result} = :timer.tc(fn -> Filo.Client.execute(c, "SELECT 1") end)
              {:ok, _, c} = result
              {c, [t | acc]}
            end)

          ran = :counters.get(calls, 1)

          unless ran >= samples,
            do: raise("hrana_open_auth_rt_us: authorize ran #{ran}/#{samples} times")

          # The floor must have stayed a cache hit (its TTL outlives the loop), or the samples
          # include Postgres read-throughs and measure the wrong thing.
          now = System.monotonic_time(:millisecond)

          unless match?([{_, 1, nil, exp}] when exp > now, :ets.lookup(Revocations, shard)),
            do: raise("hrana_open_auth_rt_us: the revocation floor lapsed mid-run")

          p50(us)
        end,
        [authorize: authorize],
        auth_token: token
      )
    after
      if revocations, do: GenServer.stop(revocations)
      Enum.each(keys, &restore_env(&1, Map.fetch!(prev, &1)))
    end
  end

  # --- contention -----------------------------------------------------------

  @doc """
  `concurrent_checkout_per_s` — checkout+checkin throughput with
  `System.schedulers_online() * 4` processes hammering `#{@concurrent_shards}` warm shards for
  `:concurrent_ms` (default #{@concurrent_ms}) (expert review 2026-08-01 #41.4).

  **Nothing in the gate could see contention.** `cold_open`, `copy`, `fanout`, `hrana_rt` and
  `wire_rows` are all serial; the only concurrent bench was `warm_all/1`, inside the opt-in S3
  path. Yet the things most likely to regress under load are all concurrency-tuned:
  `write_concurrency` on `ShardLoad`/`ShardLatency`/`WriteCounter`, the
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

  @doc """
  `same_shard_stmt_per_s` — point-SELECT throughput through `ShardExecutor.execute/2` with
  `System.schedulers_online()` workers, each on its OWN tenant handle, all on ONE shard, with
  `:shard_load` ON and its tables (and the Lru) started, as in prod (expert review 2026-10-19 #5).

  Every other statement metric is serial, so a per-statement write that serialises one hot shard's
  streams on a single ETS key (the load counters, the latency histogram) left all of them flat.
  This is the one tenant with a connection pool: many streams, one shard.

  Own scoped setup: it starts `Fathom.ShardLoad`, `Fathom.ShardLatency` and `Fathom.Shards.Lru`
  and flips `:shard_load` only for its own window (restored after), so it changes no other series.
  Watch-only until its band is measured.
  """
  @spec same_shard_stmt(keyword()) :: float()
  def same_shard_stmt(opts \\ []) do
    setup(opts)
    owned = setup_load_tables()
    ms = Keyword.get(opts, :concurrent_ms, @concurrent_ms)
    workers = Keyword.get(opts, :stmt_workers, System.schedulers_online())
    trials = Keyword.get(opts, :trials, @default_trials)
    # `shard_load: false` is the A/B lever that proves the metric discriminates (and isolates the
    # counters' share when it moves); the recorded series always runs it ON, as prod does.
    load? = Keyword.get(opts, :shard_load, true)
    prev = Application.get_env(:fathom, :shard_load)
    Application.put_env(:fathom, :shard_load, load?)
    id = uniq("bench_stmt_hot")

    try do
      # Seed through the shard's own file, as hrana_pooled_pipeline does: the prod-compiled bench
      # has replication configured on but none of its processes started, so a write through the
      # executor raises ("unknown registry: ...SessionRegistry", first full run 2026-10-19).
      path = Fathom.Shard.db_path(id)
      drop_db(path)
      {:ok, db} = Exqlite.Sqlite3.open(path)

      :ok =
        Exqlite.Sqlite3.execute(
          db,
          "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t (id, v) VALUES (1, 'x')"
        )

      :ok = Exqlite.Sqlite3.close(db)
      :ok = Fathom.Shard.stamp_local_provenance(id)

      pids = Enum.map(1..workers, fn _ -> spawn_stmt_worker(id) end)

      try do
        before = Fathom.ShardLoad.get(id)[:queries] || 0
        result = median(times(trials, fn _ -> stmt_trial(pids, ms) end))

        # The precondition this metric exists for: the load counters were actually being bumped.
        # Without it a table that failed to start (the rescued-ArgumentError path) reads as fast.
        if load? and (Fathom.ShardLoad.get(id)[:queries] || 0) <= before,
          do: raise("same_shard_stmt: ShardLoad recorded nothing — measuring the wrong path")

        result
      after
        Enum.each(pids, &stop_stmt_worker/1)
      end
    after
      restore_env(:shard_load, prev)
      Shards.drain(id, 5_000)
      teardown_load_tables(owned)
    end
  end

  # One long-lived worker per stream: it OWNS its handle (a Connection's statement cache and flags
  # live in the owning process, as with a real Filo stream), and runs one timed window per `:go`.
  defp spawn_stmt_worker(id) do
    parent = self()

    pid =
      spawn_link(fn ->
        {:ok, handle} = ShardExecutor.open(id, :trusted)
        send(parent, {:ready, self()})
        stmt_worker_loop(handle)
      end)

    receive do
      {:ready, ^pid} -> pid
    after
      10_000 -> raise "same_shard_stmt: worker never opened its handle"
    end
  end

  defp stmt_worker_loop(handle) do
    receive do
      {:go, from, deadline} ->
        send(from, {:done, self(), stmt_loop(handle, deadline, 0)})
        stmt_worker_loop(handle)

      {:stop, from} ->
        :ok = ShardExecutor.close(handle)
        send(from, {:stopped, self()})
    end
  end

  @point_select %Filo.Stmt{sql: "SELECT v FROM t WHERE id = ?", args: [1]}

  defp stmt_loop(handle, deadline, count) do
    if System.monotonic_time(:millisecond) >= deadline do
      count
    else
      {:ok, _} = ShardExecutor.execute(handle, @point_select)
      stmt_loop(handle, deadline, count + 1)
    end
  end

  defp stmt_trial(pids, ms) do
    deadline = System.monotonic_time(:millisecond) + ms
    Enum.each(pids, &send(&1, {:go, self(), deadline}))

    total =
      Enum.reduce(pids, 0, fn pid, acc ->
        receive do
          {:done, ^pid, n} -> acc + n
        after
          ms * 10 + 5_000 -> raise "same_shard_stmt worker did not finish — wedged?"
        end
      end)

    if total == 0, do: raise("same_shard_stmt completed 0 statements — measuring nothing")
    total * 1000 / ms
  end

  defp stop_stmt_worker(pid) do
    send(pid, {:stop, self()})

    receive do
      {:stopped, ^pid} -> :ok
    after
      5_000 -> :ok
    end
  end

  @doc """
  `serial_checkout_per_s` — checkout+checkin throughput with `System.schedulers_online()` workers,
  each PINNED to its own warm shard, so every checkin is a 1 -> 0 edge and every grant a 0 -> 1
  edge (expert review 2026-10-19 #5). Those edges are where the coordinator writes the Lru busy
  hint and re-keys the recency `:ordered_set` — always its right-most slot, from every coordinator
  at once. `concurrent_checkout_per_s` cannot see that: with 4x schedulers workers over 64 shards
  most checkins are not edges.

  Own scoped setup (`:shard_load` ON, its tables and the Lru started); watch-only.
  """
  @spec serial_checkout(keyword()) :: float()
  def serial_checkout(opts \\ []) do
    setup(opts)
    owned = setup_load_tables()
    ms = Keyword.get(opts, :concurrent_ms, @concurrent_ms)
    workers = Keyword.get(opts, :serial_workers, System.schedulers_online())
    trials = Keyword.get(opts, :trials, @default_trials)
    # A/B levers, as in `same_shard_stmt/1`: `shard_load: false` and `lru: false` switch off the
    # load counters / the Lru edge writes for this window. The recorded series runs both ON.
    load? = Keyword.get(opts, :shard_load, true)
    lru? = Keyword.get(opts, :lru, true)
    prev = Application.get_env(:fathom, :shard_load)
    prev_evict = Application.get_env(:fathom, :evict_idle_at_capacity)
    Application.put_env(:fathom, :shard_load, load?)
    Application.put_env(:fathom, :evict_idle_at_capacity, lru?)

    if lru? and not Fathom.Shards.Lru.enabled?(),
      do: raise("serial_checkout: Lru is disabled — the edge writes this measures never run")

    ids = Enum.map(1..workers, fn i -> "bench_serial_#{i}" end)
    Enum.each(ids, &warm_checkout_shard/1)

    try do
      median(times(trials, fn _ -> pinned_churn(ids, ms) end))
    after
      restore_env(:shard_load, prev)
      restore_env(:evict_idle_at_capacity, prev_evict)
      Enum.each(ids, &Shards.drain(&1, 5_000))
      teardown_load_tables(owned)
    end
  end

  defp pinned_churn(ids, ms) do
    deadline = System.monotonic_time(:millisecond) + ms
    parent = self()

    pids =
      Enum.map(ids, fn id ->
        spawn_link(fn -> send(parent, {:done, self(), churn_loop([id], 0, deadline, 0)}) end)
      end)

    total =
      Enum.reduce(pids, 0, fn pid, acc ->
        receive do
          {:done, ^pid, count} -> acc + count
        after
          ms * 10 + 5_000 -> raise "serial checkout worker did not finish — wedged?"
        end
      end)

    if total == 0, do: raise("serial checkout completed 0 operations — measuring nothing")
    total * 1000 / ms
  end

  # The prod-shaped per-shard tables, started ONLY by the metrics that need them (not setup/1).
  # Returns the owners THIS call started so `teardown_load_tables/1` stops exactly those: a table
  # left up is residue that changes what a later metric in the same BEAM measures (with the Lru up,
  # `concurrent_checkout_per_s` spread reads ~127k/s instead of ~1M/s on the dev host).
  defp setup_load_tables do
    for mod <- [Fathom.ShardLoad, Fathom.ShardLatency, Fathom.Shards.Lru],
        pid = start_owned(mod),
        do: pid
  end

  defp start_owned(mod) do
    case mod.start_link([]) do
      {:ok, pid} ->
        Process.unlink(pid)
        pid

      {:error, {:already_started, _pid}} ->
        nil
    end
  end

  defp teardown_load_tables(pids), do: Enum.each(pids, &GenServer.stop/1)

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

    # The per-shard load/latency tables prod always has. `mix fathom.bench` runs MIX_ENV=prod, where
    # runtime.exs turns `:shard_load` ON, but the bench does not boot the supervision tree - so
    # before this every query paid two raised-and-rescued ArgumentErrors (ShardLoad.bump/2,
    # ShardLatency.record/2) that prod never pays: ~+17 us on a ~40 us execute (expert review
    # 2026-10-19 #4). SERIES BREAK (user-approved 2026-10-19): hrana_* and the open metrics read
    # ~15-20% faster from this commit on and are not comparable with earlier perf_history entries.
    ensure_started(Fathom.ShardLoad, [])
    ensure_started(Fathom.ShardLatency, [])
    assert_load_tables!()
    :ok
  end

  # Raises unless both tables exist: a bench that silently falls back to the rescue path measures a
  # topology prod does not run (expert review 2026-10-19 #4).
  defp assert_load_tables! do
    for t <- [Fathom.ShardLoad, Fathom.ShardLatency], :ets.whereis(t) == :undefined do
      raise "bench setup: #{inspect(t)} table is missing - the metrics would measure the rescue path"
    end

    :ok
  end

  # When `:shard_load` is on (prod), the timed loop must have actually fed the tables: an empty
  # ShardLoad/ShardLatency row means the executor never recorded and the bench is again off prod's
  # topology (expert review 2026-10-19 #4). A no-op when the gate is off (dev/test).
  defp assert_load_recorded!(shard, label) do
    if Fathom.ShardLoad.enabled?() do
      queries = (Fathom.ShardLoad.get(shard) || %{queries: 0}).queries
      latencies = shard |> Fathom.ShardLatency.histogram() |> Enum.sum()

      if queries == 0 or latencies == 0,
        do:
          raise(
            "#{label}: shard_load is on but ShardLoad/ShardLatency recorded nothing for " <>
              "#{shard} (queries=#{queries}, latency samples=#{latencies})"
          )
    end

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

    # The S3 metrics (cold_open_s3, failover_rto) exercise the dedicated S3 Finch pool, but
    # `mix fathom.bench` doesn't boot the app supervision tree, so the bench has to start the pool
    # itself (sized from the S3 config above — FATHOM_S3_TEST_POOL_SIZE lets the sweep vary it).
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
