defmodule Fathom.Admin.MetricsTestUsageStub do
  @moduledoc false
  # A `stored_usage/0` that blocks until the test releases it, so the collector's storage-usage
  # poll is observably "in flight" and the overlap guard is testable (expert review 2026-07-14 #22).
  # Signals the gate pid (the test) when the poll starts, then waits for `:release`.
  def stored_usage do
    pid = Application.get_env(:fathom, :usage_poll_gate)
    send(pid, {:usage_poll_started, self()})

    receive do
      :release -> {5, 500}
    after
      5_000 -> {0, 0}
    end
  end
end

defmodule Fathom.Admin.MetricsTest do
  @moduledoc """
  The metrics-layer instrumentation: the per-shard flush watermark + RPO derivation
  (`Fathom.Admin.FlushWatermark` / `Fathom.Admin.Measurements.durability/0`), the node-wide
  query-latency telemetry event, the S3-op meter, and the storage-usage backend callback.

  Not async — these touch shared named ETS tables + global `:telemetry` handlers + Application env.
  """
  use ExUnit.Case, async: false

  alias Fathom.Admin.{FlushWatermark, Measurements}
  alias Fathom.Shard.{Storage, WriteCounter}
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    prev = Application.get_env(:fathom, :metrics_collector)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    # The dashboard/metrics layer is off in test by default; turn it on so watermark writes land.
    Application.put_env(:fathom, :metrics_collector, true)
    FlushWatermark.reset()

    on_exit(fn ->
      FlushWatermark.reset()
      restore(:metrics_collector, prev)
      restore(:shard_idle_ms, prev_idle)
    end)

    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp uniq(p), do: "#{p}_#{System.unique_integer([:positive])}"
  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp clean_shard(id) do
    for base <- [Path.join(@local_dir, "#{id}.db"), Path.join(@remote_dir, "#{id}.db")],
        suffix <- ["", "-wal", "-shm"],
        do: File.rm(base <> suffix)

    File.rm(Path.join(@remote_dir, "#{id}.lock"))
  end

  # Named-function telemetry handler (no anonymous-fun perf warning) forwarding to the test pid.
  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  defp attach(event) do
    name = "test-#{System.unique_integer([:positive])}"
    :telemetry.attach(name, event, &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(name) end)
    :ok
  end

  # ── FlushWatermark: the per-shard watermark table ──

  test "record publishes a row; forget drops it; snapshot lists them" do
    gen = WriteCounter.generation()
    FlushWatermark.record("wm_a", 7, gen)
    FlushWatermark.record("wm_b", 0, gen)

    rows = FlushWatermark.snapshot()
    assert {"wm_a", 7, ^gen, _at} = List.keyfind(rows, "wm_a", 0)
    assert {"wm_b", 0, ^gen, _at} = List.keyfind(rows, "wm_b", 0)

    FlushWatermark.forget("wm_a")
    refute List.keyfind(FlushWatermark.snapshot(), "wm_a", 0)
  end

  test "record is a no-op when the metrics layer is disabled" do
    Application.put_env(:fathom, :metrics_collector, false)
    FlushWatermark.record("off", 1, WriteCounter.generation())
    assert FlushWatermark.snapshot() == []
  end

  # ── Durability / RPO derivation (the core 1b invariant): matches Shard.unflushed?/1 ──

  test "durability counts dirty shards by watermark comparison and generation mismatch" do
    attach([:fathom, :durability, :rpo])
    gen = WriteCounter.generation()

    clean = uniq("clean")
    dirty = uniq("dirty")
    stale = uniq("stale")
    on_exit(fn -> for id <- [clean, dirty, stale], do: WriteCounter.forget(id) end)

    # clean: flushed watermark == current write count, same generation ⇒ not dirty.
    WriteCounter.bump(clean)
    FlushWatermark.record(clean, WriteCounter.count(clean), gen)

    # dirty: a write landed past the flushed watermark (the mid-flush-write case) ⇒ dirty.
    WriteCounter.bump(dirty)
    WriteCounter.bump(dirty)
    FlushWatermark.record(dirty, 1, gen)
    assert WriteCounter.count(dirty) > 1

    # stale: the watermark was captured under an older WriteCounter generation ⇒ unknown ⇒ dirty,
    # regardless of the count (the table-reset case Shard.unflushed?/1 guards).
    WriteCounter.bump(stale)
    FlushWatermark.record(stale, WriteCounter.count(stale), gen - 1)

    Measurements.durability()

    assert_receive {:telemetry, [:fathom, :durability, :rpo],
                    %{dirty_shards: 2, oldest_age_ms: age}, _}

    assert is_integer(age) and age >= 0
  end

  # ── Query-latency telemetry event through the real executor hot path (1a) ──

  test "a successful query emits [:fathom,:shard,:query] with a positive duration" do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    attach([:fathom, :shard, :query])
    id = uniq("q")
    on_exit(fn -> clean_shard(id) end)

    {:ok, handle} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(handle, stmt("CREATE TABLE kv (v TEXT)"))

    assert_receive {:telemetry, [:fathom, :shard, :query], %{duration: d}, %{}}
    assert d > 0

    :ok = ShardExecutor.close(handle)
    Shards.drain(id)
  end

  test "an open shard publishes a watermark; draining forgets it" do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    id = uniq("wmshard")
    on_exit(fn -> clean_shard(id) end)

    {:ok, handle} = ShardExecutor.open(id)
    assert List.keyfind(FlushWatermark.snapshot(), id, 0), "open publishes a watermark row"

    :ok = ShardExecutor.close(handle)
    Shards.drain(id)
    refute List.keyfind(FlushWatermark.snapshot(), id, 0), "terminate forgets the row"
  end

  # ── S3-op meter (1c): the Req response step emits [:fathom,:s3,:op] tagged by method ──

  test "meter/1 emits an S3-op event with method tag and byte count" do
    attach([:fathom, :s3, :op])

    # A PUT: the request carries the body size; S3's response is content-length 0. The upload
    # bytes must come from the request body, not the response's 0 (regression: op_bytes preferred
    # the response and 0 is truthy in Elixir, so PUT volume read 0).
    put_req = %Req.Request{method: :put, headers: %{"content-length" => ["2048"]}}
    put_resp = %Req.Response{status: 200, headers: %{"content-length" => ["0"]}}
    assert {^put_req, ^put_resp} = Fathom.Shard.Storage.S3.meter({put_req, put_resp})
    assert_receive {:telemetry, [:fathom, :s3, :op], %{count: 1, bytes: 2048}, %{op: :put}}

    get_req = %Req.Request{method: :get, headers: %{}}
    get_resp = %Req.Response{status: 200, headers: %{"content-length" => ["4096"]}}
    assert {^get_req, ^get_resp} = Fathom.Shard.Storage.S3.meter({get_req, get_resp})
    assert_receive {:telemetry, [:fathom, :s3, :op], %{count: 1, bytes: 4096}, %{op: :get}}
  end

  # ── stored_usage (1d): Local counts live .db objects, excluding versions + locks ──

  test "Storage.stored_usage counts live .db objects and sums bytes" do
    dir = Path.join(System.tmp_dir!(), "fathom_usage_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    prev = Application.get_env(:fathom, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: dir)

    on_exit(fn ->
      File.rm_rf(dir)
      restore(Fathom.Shard.Storage.Local, prev)
    end)

    File.write!(Path.join(dir, "a.db"), String.duplicate("x", 100))
    File.write!(Path.join(dir, "b.db"), String.duplicate("y", 200))
    # A retained version copy and a lock must NOT be counted as live objects.
    File.write!(Path.join(dir, "a@3.db"), String.duplicate("z", 999))
    File.write!(Path.join(dir, "a.lock"), "lease")

    assert {2, 300} = Storage.stored_usage()
  end

  # ── MetricsCollector (Phase 2): broadcasts a well-formed metrics map each tick ──

  test "the collector broadcasts a metrics map with direct + aggregate fields" do
    prev_load = Application.get_env(:fathom, :shard_load)
    prev_tick = Application.get_env(:fathom, :admin_tick_ms)
    Application.put_env(:fathom, :shard_load, true)
    Application.put_env(:fathom, :admin_tick_ms, 80)

    on_exit(fn ->
      restore(:shard_load, prev_load)
      restore(:admin_tick_ms, prev_tick)
    end)

    Phoenix.PubSub.subscribe(Fathom.PubSub, Fathom.Admin.MetricsCollector.topic())
    start_supervised!(Fathom.Admin.MetricsCollector)

    assert_receive {:metrics, m}, 1_000

    # Direct (node-local) fields are always populated, even with the Prometheus reporter off in test.
    assert is_integer(m.open_shards) and m.open_shards >= 0
    assert is_integer(m.memory_bytes) and m.memory_bytes > 0
    assert is_number(m.node_qps)
    assert is_list(m.hot_shards)
    assert is_integer(m.dirty_shards)
    # Aggregate (scrape-derived) fields degrade to zeros/maps when the reporter isn't running.
    assert is_number(m.query_p50_ms)
    assert is_map(m.s3_ops_per_s)
    assert is_map(m.checkout_per_s)

    # snapshot/0 serves the last value + a history ring for a freshly-connected LiveView.
    snap = Fathom.Admin.MetricsCollector.snapshot()
    assert is_map(snap.current)
    assert is_list(snap.history)
  end

  # ── #11: a blank/failed scrape must not reset the rate baselines (no false-recovery spike) ──

  test "a blank scrape holds the diff baselines so recovery doesn't spike (expert review 2026-07-14 #11)" do
    alias Fathom.Admin.{MetricsCollector, PrometheusScrape}

    real = fn n -> PrometheusScrape.parse("fathom_s3_op_count{op=\"get\"} #{n}\n") end

    # Tick 1: first sample establishes the baseline (rate 0 — no prior).
    s1 = MetricsCollector.scrape_step(real.(10), [], %{}, 1.0)
    assert s1.counter_rates.s3_ops["get"] == 0.0

    # Tick 2: a genuine +5 over one window ⇒ 5 q/s.
    s2 = MetricsCollector.scrape_step(real.(15), s1.query_buckets, s1.counters, 1.0)
    assert_in_delta s2.counter_rates.s3_ops["get"], 5.0, 0.001

    # Tick 3: the reporter errored/timed out ⇒ blank scrape. It emits zeros AND HOLDS the last-good
    # baseline instead of storing the empty scrape (the bug: an all-zero baseline).
    s3 = MetricsCollector.scrape_step([], s2.query_buckets, s2.counters, 1.0)
    assert s3.counter_rates == %{s3_ops: %{}, s3_bytes: %{}, checkout: %{}}
    assert s3.counters == s2.counters, "the last-good baseline is held, not reset to zeros"
    assert s3.query_buckets == s2.query_buckets

    # Tick 4: recovery. Because tick 3 held the baseline (15), the +5 → 20 reads as 5 q/s — NOT the
    # whole cumulative 20 a zeroed baseline would have produced (the false-recovery spike).
    s4 = MetricsCollector.scrape_step(real.(20), s3.query_buckets, s3.counters, 1.0)
    assert_in_delta s4.counter_rates.s3_ops["get"], 5.0, 0.001
    refute s4.counter_rates.s3_ops["get"] > 10.0
  end

  # ── #12: per-shard latency percentiles are windowed, not lifetime-cumulative ──

  test "window_latency reports the recent window, not the lifetime max (expert review 2026-07-14 #12)" do
    prev_load = Application.get_env(:fathom, :shard_load)
    Application.put_env(:fathom, :shard_load, true)
    Fathom.ShardLatency.reset()

    on_exit(fn ->
      Fathom.ShardLatency.reset()
      restore(:shard_load, prev_load)
    end)

    id = uniq("w12")
    native = fn us -> System.convert_time_unit(us, :microsecond, :native) end

    # Tick 1: the shard is SLOW (50 ms). First appearance ⇒ cumulative fallback ⇒ p99 catches it.
    for _ <- 1..50, do: Fathom.ShardLatency.record(id, native.(50_000))
    {[h1], prev1} = Fathom.Admin.MetricsCollector.window_latency([%{shard_id: id}], %{})
    assert h1.p99_ms >= 10.0, "tick1 p99 catches the 50ms samples (got #{h1.p99_ms})"

    # Between ticks, only FAST (0.5 ms) traffic arrives; the histogram is cumulative (slow + fast).
    for _ <- 1..50, do: Fathom.ShardLatency.record(id, native.(500))

    # Tick 2: windowed against tick 1 ⇒ the diff is the fast samples only, so p99 is FAST — NOT the
    # 50 ms lifetime max a straight cumulative read would still report.
    {[h2], _prev2} = Fathom.Admin.MetricsCollector.window_latency([%{shard_id: id}], prev1)

    assert h2.p99_ms < 5.0,
           "tick2 p99 reflects the recent fast window, not the 50ms lifetime max (got #{h2.p99_ms})"
  end

  # ── #22: the storage-usage poll is supervised, overlap-guarded, and caches on completion ──

  test "the storage-usage poll is supervised + overlap-guarded (expert review 2026-07-14 #22)" do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_tick = Application.get_env(:fathom, :admin_tick_ms)
    prev_usage = Application.get_env(:fathom, :admin_usage_ms)
    prev_gate = Application.get_env(:fathom, :usage_poll_gate)

    # Slow the automatic tick + poll cadence right down (we drive :usage_poll manually) and point
    # storage at the blocking stub so a poll stays observably in flight.
    Application.put_env(:fathom, :shard_storage, Fathom.Admin.MetricsTestUsageStub)
    Application.put_env(:fathom, :admin_tick_ms, 60_000)
    Application.put_env(:fathom, :admin_usage_ms, 60_000)
    Application.put_env(:fathom, :usage_poll_gate, self())

    on_exit(fn ->
      restore(:shard_storage, prev_storage)
      restore(:admin_tick_ms, prev_tick)
      restore(:admin_usage_ms, prev_usage)
      restore(:usage_poll_gate, prev_gate)
    end)

    attach([:fathom, :storage, :usage])
    pid = start_supervised!(Fathom.Admin.MetricsCollector)

    # init fires one :usage_poll ⇒ exactly one SUPERVISED task starts and blocks in the stub.
    assert_receive {:usage_poll_started, task_pid}, 1_000
    assert length(Task.Supervisor.children(Fathom.Admin.TaskSupervisor)) == 1

    # A second :usage_poll while the first is in flight must NOT spawn a second task (overlap guard).
    send(pid, :usage_poll)
    refute_receive {:usage_poll_started, _}, 200
    assert length(Task.Supervisor.children(Fathom.Admin.TaskSupervisor)) == 1

    # Release the in-flight poll: it completes, caches usage (republished as the gauge), frees the slot.
    send(task_pid, :release)
    assert_receive {:telemetry, [:fathom, :storage, :usage], %{objects: 5, bytes: 500}, _}, 1_000

    # The cache is updated and readable via snapshot/0 after a tick would read it — assert the state.
    assert %{usage: {5, 500}, usage_task: nil} = :sys.get_state(pid)

    # Slot freed ⇒ a subsequent poll runs (proves completion cleared the guard); release it to clean up.
    send(pid, :usage_poll)
    assert_receive {:usage_poll_started, task_pid2}, 1_000
    send(task_pid2, :release)
  end
end
