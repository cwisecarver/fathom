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

    put_req = %Req.Request{method: :put, headers: %{"content-length" => ["2048"]}}
    put_resp = %Req.Response{status: 200, headers: %{}}
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
end
