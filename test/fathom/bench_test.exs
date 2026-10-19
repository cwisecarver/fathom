defmodule Fathom.BenchTest do
  @moduledoc """
  Hot-path floor/ceiling guards (AGENTS.md §Benchmarking). These assert
  order-of-magnitude bounds, NOT exact latencies — a dev build is slower than
  `MIX_ENV=prod`, so the bounds are deliberately generous. Their job is to fail
  loudly if a hot path regresses by an order of magnitude even when nobody runs
  the full `mix fathom.bench` gate.

  Tagged `:bench`, excluded from the default suite (test_helper.exs). Run with:

      mix test --include bench test/fathom/bench_test.exs

  The directory-resolve bench is exercised by the real harness against a bench
  Postgres DB, not here, to keep these guards free of the Ecto sandbox. They cover
  the three no-Postgres metrics.
  """
  use ExUnit.Case, async: false

  @moduletag :bench

  test "cold-open p50 is within the order-of-magnitude ceiling" do
    us = Fathom.Bench.cold_open(cold_open_samples: 5)
    assert is_float(us)
    assert us < 50_000, "cold-open p50 #{us}µs exceeded the 50ms ceiling"
  end

  # ABSOLUTE ceilings, which the gate structurally cannot provide (expert review 2026-08-01
  # #41.5). `Gate.delta/4` is a ratio against the parent on an uncontrolled host, so a slow drift
  # that moves every commit a few percent never trips it, and a uniform collapse that moves parent
  # and child together is invisible by construction — AGENTS.md: "The ratio holds while throughput
  # collapses." These are the floor under the ratio. Deliberately generous (a dev build is slower
  # than MIX_ENV=prod): they exist to catch an order-of-magnitude break, not to police noise.
  test "the wire metrics clear their absolute bounds" do
    rt = Fathom.Bench.hrana_rt(hrana_rt_samples: 30)
    assert is_float(rt), "the loopback listener did not come up — this measured nothing"
    assert rt < 20_000, "hrana round trip #{rt}µs exceeded the 20ms ceiling"

    rows_per_s = Fathom.Bench.wire_rows(wire_rows: 200)
    assert is_float(rows_per_s)

    assert rows_per_s > 1_000,
           "wire encode #{rows_per_s} rows/s fell under the 1k floor — the 200x row-encoding " <>
             "regression this metric exists for was exactly this shape"
  end

  # The other half of the same gap (expert review 2026-08-26 #39). The comment above explains WHY
  # an absolute floor is needed; six metrics had one and six did not, and the three guarded here
  # are the three that own a hot path.
  #
  # `hrana_open_rt_us` is the sharpest omission. It is the dominant cost of an HTTP-SDK request
  # (and of django-libsql under Django's default CONN_MAX_AGE=0, where every request is a fresh
  # stream), and it was gated by a parent-ratio alone. The proof that a ratio is not enough is
  # #10 from the same review: `Connection.open/2` called `File.mkdir_p!` on a directory that
  # already existed, on EVERY stream open, for the entire life of the project — measured at ~24%
  # of this metric — and no gate, no bench and no test ever saw it, because it was in the parent
  # too. That is exactly the uniform-drift blind spot the ratio cannot cover.
  # Expert review 2026-10-01 perf #19: the first metric that CLOSES its streams, so the first that
  # runs the executor's close, the pool checkin and the reset. Measured ~620 us on a dev build
  # against ~347 us for hrana_open_rt_us, whose streams are never closed; the difference is the
  # close path plus `-shm` re-creation when the shard's last connection goes. 20 ms is ~30x headroom:
  # an order-of-magnitude guard, and proof the metric ran (it raises if streams accumulate).
  test "the one-shot stream metric clears its absolute bound" do
    oneshot = Fathom.Bench.hrana_oneshot_rt_us(hrana_rt_samples: 30)
    assert is_float(oneshot), "the loopback listener did not come up — this measured nothing"

    assert oneshot < 20_000,
           "one-shot [execute, close] round trip #{oneshot}us exceeded the 20ms ceiling"
  end

  test "the per-stream open and flush metrics clear their absolute bounds" do
    open_rt = Fathom.Bench.hrana_open_rt_us(hrana_rt_samples: 30)
    assert is_float(open_rt), "the loopback listener did not come up — this measured nothing"

    # Composition being bounded: Filo stream open + shard-id cast + auth + the lifecycle ETS
    # lookups + Registry.lookup + the coordinator GenServer.call + Connection.open/2
    # (mkdir/sqlite3_open/pragmas/extension load) + one trivial round trip. Measured ~430µs on a
    # dev build; 20ms is ~45x headroom, so this catches an order-of-magnitude break only.
    assert open_rt < 20_000,
           "hrana OPEN round trip #{open_rt}µs exceeded the 20ms ceiling — this is stream open " <>
             "(no baton) plus one query, i.e. what an HTTP-SDK request costs"

    # A fresh-stream request must stay in the same order of magnitude as a baton-reusing one.
    # Not a tight ratio (open really is ~4x a warm round trip); this catches open-path work
    # that scales with something it should not.
    rt = Fathom.Bench.hrana_rt(hrana_rt_samples: 30)
    assert is_float(rt)

    assert open_rt < rt * 50,
           "stream open #{open_rt}µs is more than 50x a warm round trip #{rt}µs — something on " <>
             "the open path is scaling with a population it should not"

    flush = Fathom.Bench.flush_p50(trials: 2, flush_rows: 500)
    assert is_float(flush)

    # flush_p50 raises if the shard is not dirty, so a "measuring nothing" run cannot reach here
    # (bench.ex:flush_p50/1). The floor guards the OTHER direction the AGENTS.md rule names: a
    # suspiciously GOOD number. A full VACUUM INTO + local PUT cannot be microseconds; the
    # 2µs reading that motivated that rule would fail this.
    assert flush > 100,
           "flush p50 #{flush}µs is implausibly fast for VACUUM INTO + a full-object write — " <>
             "suspect the flush did no work rather than banking the number"

    assert flush < 5_000_000, "flush p50 #{flush}µs exceeded the 5s ceiling"
  end

  # `same_shard_checkout_per_s` is the head-of-line throughput of one coordinator's GenServer.call
  # — the single lock every stream on a hot shard queues behind, and the metric #9 (Shards.stop/1
  # blocking the shard supervisor) lands nearest. Ratio-gated only, until now.
  test "same-shard checkout throughput clears its absolute floor" do
    stats = Fathom.Bench.concurrent_checkout(concurrent_ms: 200, concurrent_shards: 8, trials: 1)

    assert is_float(stats.same_shard_per_s)
    assert is_float(stats.spread_per_s)

    # Floors sized from measurement, not from caution. Three back-to-back samples on this machine
    # read same_shard 465_200 / 468_600 / 466_300 per second and spread 1_030_430 / 1_053_495 /
    # 1_047_375 — under 1% and ~2% spread respectively, which is tight enough to set a real bound.
    # A floor of 100/s (the first draft here) would have passed a 4,600x collapse; that is not a
    # guard, it is decoration. These sit ~9x and ~10x under the observed values: generous enough
    # for a contended machine and a dev build, tight enough that an order-of-magnitude break fails.
    assert stats.same_shard_per_s > 50_000,
           "same-shard checkout #{stats.same_shard_per_s}/s fell under the 50k/s floor — one " <>
             "coordinator's GenServer.call serializes every stream on a hot shard, so this is " <>
             "the hot-shard ceiling and ~465k/s is what it measured when the floor was set"

    assert stats.spread_per_s > 100_000,
           "spread checkout #{stats.spread_per_s}/s fell under the 100k/s floor — this is the " <>
             "ETS-counter and LRU path, measured at ~1.04M/s when the floor was set"
  end

  # Expert review 2026-10-19 #5: the hot-shard contention metrics. Floors ~10x under what the dev
  # host measured BEFORE the fix (same_shard_stmt ~190-220k stmt/s, serial_checkout ~80-150k ops/s
  # at 18 workers), so they catch an order-of-magnitude collapse and prove the metric ran at all.
  test "the hot-shard contention metrics clear their absolute floors" do
    lru_before = Process.whereis(Fathom.Shards.Lru)
    stmt = Fathom.Bench.same_shard_stmt(concurrent_ms: 200, trials: 1)
    assert is_float(stmt)
    assert stmt > 10_000, "same-shard statements #{stmt}/s fell under the 10k/s floor"

    serial = Fathom.Bench.serial_checkout(concurrent_ms: 200, trials: 1)
    assert is_float(serial)
    assert serial > 10_000, "pinned serial checkout #{serial}/s fell under the 10k/s floor"

    # Scoped setup: a table owner they started is gone again (and one already running is left
    # alone), so no later metric inherits them.
    assert Process.whereis(Fathom.Shards.Lru) == lru_before
  end

  test "the p99 metrics describe the same run as their p50, and bound it" do
    # One run, two reducers. If these ever came from separate runs the gate would compare two
    # samples of a noisy bench and call the difference a regression.
    stats = Fathom.Bench.cold_open_stats(cold_open_samples: 5)

    assert stats.p99_us >= stats.p50_us,
           "p99 #{stats.p99_us}µs below p50 #{stats.p50_us}µs — these are not the same samples"

    assert stats.p99_us < 100_000, "cold-open p99 #{stats.p99_us}µs exceeded the 100ms ceiling"
  end

  test "migration copy clears the throughput floor" do
    rows_per_s = Fathom.Bench.copy_throughput(copy_rows: 2_000, trials: 2)
    assert is_float(rows_per_s)
    assert rows_per_s > 1_000, "copy #{rows_per_s} rows/s fell under the 1k floor"
  end

  test "per-shard fan-out memory is within the ceiling" do
    kb = Fathom.Bench.fanout(fanout_n: 20, trials: 2)
    assert is_float(kb)
    assert kb < 5_000, "fan-out #{kb} KiB/shard exceeded the 5 MiB ceiling"
  end

  # Ceiling guard for the GC'd companion to fanout, same shape as its neighbour above.
  #
  # It deliberately does NOT assert `fanout_gc <= fanout`, which is the invariant you would
  # reach for first. That assertion was written, and it FAILED here — not because collecting
  # added heap, but because at these guard-sized parameters `fanout/1` read **-5.75 KiB/shard**:
  # a sweep between its two samples freed more than 20 coordinators allocated. A metric that
  # returns negative numbers has no usable ordering, and any slack term wide enough to absorb
  # that is wide enough to assert nothing. (That it can go negative at all is recorded in
  # `fanout_gc/1`'s @doc as evidence about the metric, which is where it earns its keep.)
  #
  # Nor does this prove the collection happened: if `fanout_gc/1` silently stopped collecting it
  # would equal `fanout/1` and still pass. Proving the sweep needs coordinators holding known
  # garbage on demand and there is no seam for that. The evidence the sweep works is the
  # measurement in `fanout_gc/1`'s @doc. This is a smoke + order-of-magnitude guard, nothing more.
  test "the GC'd fan-out floor is within the ceiling" do
    kb = Fathom.Bench.fanout_gc(fanout_n: 20, trials: 2)
    assert is_float(kb)
    assert kb < 5_000, "GC'd fan-out #{kb} KiB/shard exceeded the 5 MiB ceiling"
  end

  # Expert review 2026-07-24 #9. Pins the coordinator's memory POLICY, not a post-GC byte count:
  # a test that forces `:erlang.garbage_collect/1` measures the same figure with or without the
  # policy (verified on this change — 2 KiB vs 3 KiB), because forcing a sweep is precisely what
  # the policy automates. The retained-heap win is measured by `fanout_kb_per_shard` in the real
  # bench gate and by `chaos.sh density`; what belongs in a unit guard is that the flags are set.
  #
  # The `max_heap_size` half is the safety-critical one: a coordinator killed at a heap limit
  # mid-`terminate/2` skips the final fence + checkpoint + PUT + lease release — a durability
  # regression that can also strand a lease. It must stay unset.
  test "a coordinator is spawned with the reclaiming GC policy and no heap-kill limit" do
    shard = "benchmem_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Fathom.Shards.drain(shard, 2_000)

      for p <- Path.wildcard(Path.join(Fathom.Shard.data_dir(), "#{shard}*")),
          do: File.rm(p)
    end)

    {:ok, pid} = Fathom.Shards.ensure(shard)
    {:garbage_collection, gc} = Process.info(pid, :garbage_collection)

    assert Keyword.fetch!(gc, :fullsweep_after) == 0,
           "coordinators must full-sweep on every GC — it is the only thing that reclaims the old " <>
             "heap and returns pages, and retained heap dominates per-shard cost at 30k/node"

    # ERTS reports the limit as 0 (or a map with size 0) when unset.
    max_heap = Keyword.get(gc, :max_heap_size)
    size = if is_map(max_heap), do: Map.get(max_heap, :size, 0), else: max_heap || 0

    assert size == 0,
           "max_heap_size must stay UNSET on coordinators: a heap-limit kill mid-terminate/2 " <>
             "skips the final fence + checkpoint + PUT + lease release"
  end

  test "cold_open_s3 is opt-in: nil without an S3 endpoint" do
    # The cold-S3 path needs an S3 endpoint; without one it must degrade to nil so
    # the default gate stays S3-free. Skip the assertion if an endpoint IS set
    # (e.g. running under the MinIO harness), where it returns a real number.
    unless System.get_env("FATHOM_S3_TEST_ENDPOINT") do
      assert Fathom.Bench.cold_open_s3() == nil
    end
  end

  test "warm_s3 is opt-in: nil without an S3 endpoint" do
    unless System.get_env("FATHOM_S3_TEST_ENDPOINT") do
      assert Fathom.Bench.warm_s3_throughput() == nil
    end
  end

  test "failover_rto is opt-in: nil without an S3 endpoint" do
    # The cold-vs-promote failover RTO delta is only meaningful against a network store
    # (the promote win is the S3 body transfer avoided), so it's S3-only like cold_open_s3.
    unless System.get_env("FATHOM_S3_TEST_ENDPOINT") do
      assert Fathom.Bench.failover_rto() == nil
    end
  end

  # The A2 promote-on-open failover arm's MECHANISM, on Local storage so it runs on every machine
  # without an S3 endpoint (the S3 `failover_rto` arm above is nil-skipped here). This is the guard
  # AGENTS.md demands for a new bench metric: `promote_open_sample/3` RAISES unless the promotion
  # actually fired — the survivor open must serve all `total` rows, not just the `flush_after` the
  # stored object held — so a silent cold-open fallback (unstamped object, replica not ranked ahead)
  # fails this test loudly instead of banking a mislabeled number. Without it the metric could ship
  # measuring a cold open and calling it a promote.
  test "the promote-on-open arm actually promotes, and clears its ceiling" do
    dir = Path.join(System.tmp_dir!(), "bench_promote_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    start_supervised!(
      {Fathom.Shard.Replication.Follower,
       name: Fathom.Shard.Replication.Follower, port: 0, dir: dir}
    )

    prev_gate = Application.get_env(:fathom, :replication_promote_on_open)
    prev_recover = Application.get_env(:fathom, :replication_recover_from_peers)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.put_env(:fathom, :replication_recover_from_peers, false)

    on_exit(fn ->
      if is_nil(prev_gate),
        do: Application.delete_env(:fathom, :replication_promote_on_open),
        else: Application.put_env(:fathom, :replication_promote_on_open, prev_gate)

      if is_nil(prev_recover),
        do: Application.delete_env(:fathom, :replication_recover_from_peers),
        else: Application.put_env(:fathom, :replication_recover_from_peers, prev_recover)

      File.rm_rf(dir)
    end)

    # 20 rows, object stamped at 10 — a cold open would serve 10, a promote serves 20. If the
    # promotion did not fire, promote_open_sample/3 raises and this test fails, which is the point.
    us = Fathom.Bench.promote_open_sample(20, 10, 4096)

    assert is_integer(us) and us > 0
    assert us < 5_000_000, "promote-on-open open #{us}µs exceeded the 5s ceiling"
  end

  # The guard BITES (AGENTS.md: a guard that cannot fail is decoration). With promote-on-open
  # disabled, the survivor open serves only the stored object's 10 rows, so promote_open_sample/3
  # must RAISE rather than time a cold open and call it a promote. This is the "run the new test
  # against the unfixed code" probe made permanent — it is what stands between the metric and a
  # silently-mislabeled number.
  test "the promote sample raises when the promotion does not fire" do
    dir = Path.join(System.tmp_dir!(), "bench_promote_off_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    start_supervised!(
      {Fathom.Shard.Replication.Follower,
       name: Fathom.Shard.Replication.Follower, port: 0, dir: dir}
    )

    prev_gate = Application.get_env(:fathom, :replication_promote_on_open)
    prev_recover = Application.get_env(:fathom, :replication_recover_from_peers)
    Application.put_env(:fathom, :replication_promote_on_open, false)
    Application.put_env(:fathom, :replication_recover_from_peers, false)

    on_exit(fn ->
      if is_nil(prev_gate),
        do: Application.delete_env(:fathom, :replication_promote_on_open),
        else: Application.put_env(:fathom, :replication_promote_on_open, prev_gate)

      if is_nil(prev_recover),
        do: Application.delete_env(:fathom, :replication_recover_from_peers),
        else: Application.put_env(:fathom, :replication_recover_from_peers, prev_recover)

      File.rm_rf(dir)
    end)

    assert_raise RuntimeError, ~r/measured a cold open/, fn ->
      Fathom.Bench.promote_open_sample(20, 10, 4096)
    end
  end

  # perf review 2026-10-14 #17. The replication-ON density arm asserts its own preconditions
  # (replication on, shippers connected, one Session per shard, replicas present on every follower)
  # and raises otherwise; this runs it small and bounds the answer to an order of magnitude. The
  # replicating shard must cost the primary MORE than the plain served regime's ~220 KiB-ish floor
  # is not asserted (watch-only metric, no band yet) — only that it is positive and finite.
  @tag timeout: 300_000
  test "served_repl measures a replicating fleet and reports both sides" do
    r = Fathom.Bench.served_repl(served_repl_n: 8, trials: 1)
    IO.puts("served_repl: #{inspect(r)}")
    assert r.total_kb > 10 and r.total_kb < 5_000
    assert r.follower_kb > 0 and r.follower_kb < 5_000
    refute Fathom.Shard.Replication.Fleet.replicating?(), "the arm left replication on"
  end

  # expert review 2026-10-17 #8. Both new wire metrics assert their own preconditions and raise
  # otherwise (a write metric that did not replicate; an auth metric that was not enforcing); this
  # runs each small and bounds the answer to an order of magnitude, and checks the scoped setup was
  # torn down (replication off, auth config restored).
  @tag timeout: 300_000
  test "hrana_write_rt_us measures a replicated write and restores replication off" do
    us = Fathom.Bench.hrana_write_rt_us(hrana_rt_samples: 50)
    IO.puts("hrana_write_rt_us: #{inspect(us)}")
    assert is_float(us) and us > 20 and us < 1_000_000
    refute Fathom.Shard.Replication.Fleet.replicating?(), "the arm left replication on"
  end

  test "hrana_open_auth_rt_us measures an authenticated open and restores the auth config" do
    prev = Application.get_env(:fathom, :hrana_auth)
    us = Fathom.Bench.hrana_open_auth_rt_us(hrana_rt_samples: 50)
    IO.puts("hrana_open_auth_rt_us: #{inspect(us)}")
    assert is_float(us) and us > 20 and us < 1_000_000
    assert Application.get_env(:fathom, :hrana_auth) == prev, "the arm left hrana_auth changed"
  end

  # expert review 2026-10-19 #4: the bench topology must have the ShardLoad/ShardLatency tables
  # prod has (MIX_ENV=prod turns :shard_load on, and without the tables every query raises and
  # rescues two ArgumentErrors). With the gate forced on, a wire run must leave rows in both.
  @tag timeout: 120_000
  test "setup starts the load tables and a wire run feeds them when :shard_load is on" do
    prev = Application.get_env(:fathom, :shard_load)
    Application.put_env(:fathom, :shard_load, true)

    on_exit(fn ->
      if prev == nil,
        do: Application.delete_env(:fathom, :shard_load),
        else: Application.put_env(:fathom, :shard_load, prev)
    end)

    # hrana_rt_stats/1 raises if the loop recorded nothing (assert_load_recorded!/2).
    assert is_float(Fathom.Bench.hrana_rt(hrana_rt_samples: 20))
    assert :ets.whereis(Fathom.ShardLoad) != :undefined
    assert :ets.whereis(Fathom.ShardLatency) != :undefined
  end

  # expert review 2026-10-19 #26: reporters attached, scrape count asserted inside the metric (it
  # raises if the query-duration count is below the sample count), handlers detached afterwards.
  @tag timeout: 120_000
  test "hrana_rt_observed_us runs with reporters attached and detaches them" do
    before = length(:telemetry.list_handlers([]))
    us = Fathom.Bench.hrana_rt_observed_us(hrana_rt_samples: 50)
    assert is_float(us) and us > 5 and us < 1_000_000
    assert length(:telemetry.list_handlers([])) == before, "the arm leaked telemetry handlers"
  end

  # expert review 2026-10-19 #25: preconditions (durability saw every seeded row, hot_shards
  # baselined every row) are asserted inside the metric; the tables are emptied afterwards.
  @tag timeout: 120_000
  test "observability_tick_us walks the seeded tables and empties them afterwards" do
    us = Fathom.Bench.observability_tick_us(tick_rows: 2_000, trials: 2)
    assert is_float(us) and us > 0
    assert Fathom.ShardLoad.snapshot_tuples() == []
    assert Fathom.Admin.FlushWatermark.size() == 0
  end

  # expert review 2026-10-19 #7. The flush codec on a ~20 MB realistic snapshot; the metric raises
  # itself on a non-compressible or undersized fixture. Baseline ~6-7 ms/MB prod; the dev-build
  # ceiling is an order of magnitude loose, and the floor catches a measurement that ran no codec.
  @tag timeout: 300_000
  test "flush_codec_ms_per_mb measures the zstd flush codec to an order of magnitude" do
    ms = Fathom.Bench.StorageIO.flush_codec_ms_per_mb(codec_trials: 1)
    IO.puts("flush_codec_ms_per_mb: #{inspect(ms)}")
    assert is_float(ms)
    assert ms > 0.5, "#{ms} ms/MB is too fast to have run compress + 2 MD5s + decode"
    assert ms < 100, "#{ms} ms/MB: the flush codec regressed by an order of magnitude"
  end

  # expert review 2026-10-19 #7. A whole S3 pull of the ~20 MB fixture through the loopback stub and
  # the real Finch pool; the metric itself asserts the Finch path was used and the pulled file is
  # byte-identical. Prod ~2 ms/MB.
  @tag timeout: 300_000
  test "pull_decode_ms_per_mb measures an S3 pull to an order of magnitude" do
    ms = Fathom.Bench.StorageIO.pull_decode_ms_per_mb(pull_trials: 1)
    IO.puts("pull_decode_ms_per_mb: #{inspect(ms)}")
    assert is_float(ms)
    assert ms > 0.2, "#{ms} ms/MB is too fast to have run GET + decode + MD5"
    assert ms < 100, "#{ms} ms/MB: the S3 pull regressed by an order of magnitude"
  end

  # expert review 2026-10-19 #3. HEAD and a conditional PUT through the real S3 Finch pool against a
  # loopback stub. The metric raises if the requests did not cross the pool. Prod ~220 us / ~500 us
  # (of which ~118 us each is the Req `finch:` deprecation warn until it is fixed).
  test "s3_loopback measures the S3 client through the Finch pool to an order of magnitude" do
    %{op_us: op, put_us: put} =
      Fathom.Bench.StorageIO.s3_loopback(s3_head_samples: 50, s3_put_samples: 20)

    IO.puts("s3_loopback: op_us=#{op} put_us=#{put}")
    assert op > 20 and op < 20_000, "object_head #{op}us outside the order-of-magnitude band"
    assert put > 20 and put < 50_000, "conditional PUT #{put}us outside the band"
  end

  # expert review 2026-10-19 #12. A warm-stream SELECT 1 over the WebSocket path; the metric
  # asserts each reply is [[1]]. Prod ~100-200 us.
  test "hrana_ws_rt_us measures the WebSocket round trip to an order of magnitude" do
    us = Fathom.Bench.hrana_ws_rt_us(hrana_rt_samples: 30)
    IO.puts("hrana_ws_rt_us: #{inspect(us)}")
    assert is_float(us), "the WS listener/client did not come up - this measured nothing"
    assert us > 10 and us < 20_000, "WS round trip #{us}us outside the order-of-magnitude band"
  end
end
