defmodule Fathom.ShardLatencyTest do
  @moduledoc """
  Per-shard query-latency histograms — the "which shard is slow" companion to ShardLoad's
  "which shard is hot". A per-shard ETS histogram (bounded by resident shards, NOT a Prometheus
  tag), read as p50/p95/p99. Not async — the histogram table is a shared named ETS table.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, ShardLatency, Shards}
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    prev = Application.get_env(:fathom, :shard_load)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_load, true)
    ShardLatency.reset()

    on_exit(fn ->
      ShardLatency.reset()
      restore(:shard_load, prev)
      restore(:shard_idle_ms, prev_idle)
    end)

    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp uniq(p), do: "#{p}_#{System.unique_integer([:positive])}"
  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # A dt in the native time unit that record/2 expects, from a µs value.
  defp native_us(us), do: System.convert_time_unit(us, :microsecond, :native)

  defp clean_shard(id) do
    for base <- [Path.join(@local_dir, "#{id}.db"), Path.join(@remote_dir, "#{id}.db")],
        suffix <- ["", "-wal", "-shm"],
        do: File.rm(base <> suffix)

    File.rm(Path.join(@remote_dir, "#{id}.lock"))
  end

  # ── the histogram API ──

  test "record lands a sample in the µs log-bucket for its duration" do
    # 1500µs → the [1000, 2000)µs bucket (edges [0,100,500,1000,2000,…], index 3).
    ShardLatency.record("a", native_us(1500))

    hist = ShardLatency.histogram("a")
    assert length(hist) == length(ShardLatency.edges())
    assert Enum.sum(hist) == 1
    assert Enum.at(hist, 3) == 1, "1500µs belongs to the [1000,2000)µs bucket"
  end

  test "histogram is an all-zero vector for an unrecorded shard" do
    hist = ShardLatency.histogram("never")
    assert length(hist) == length(ShardLatency.edges())
    assert Enum.sum(hist) == 0
  end

  test "percentiles report p50/p95/p99 in ms from the histogram" do
    # A tight cluster at 1.5ms: every percentile falls in the [1000,2000)µs bucket.
    for _ <- 1..100, do: ShardLatency.record("tight", native_us(1500))

    p = ShardLatency.percentiles("tight")
    assert p.p50_ms >= 1.0 and p.p50_ms <= 2.0
    assert p.p99_ms >= 1.0 and p.p99_ms <= 2.0
    assert p.p99_ms >= p.p50_ms
  end

  test "percentiles separate the tail (p99 ≫ p50) — the whole point over a mean" do
    # 90 fast (500µs) + 10 slow (50ms): the mean hides the tail, the percentiles don't.
    for _ <- 1..90, do: ShardLatency.record("bimodal", native_us(500))
    for _ <- 1..10, do: ShardLatency.record("bimodal", native_us(50_000))

    p = ShardLatency.percentiles("bimodal")
    assert p.p50_ms < 5.0, "the median tracks the fast 90% (~0.5ms)"
    assert p.p99_ms >= 10.0, "the p99 catches the slow 10% (~50ms)"
  end

  test "percentiles are zero for a shard with no samples" do
    assert ShardLatency.percentiles("empty") == %{p50_ms: 0.0, p95_ms: 0.0, p99_ms: 0.0}
  end

  test "forget drops a shard's histogram" do
    ShardLatency.record("gone", native_us(1000))
    assert Enum.sum(ShardLatency.histogram("gone")) == 1

    ShardLatency.forget("gone")
    assert Enum.sum(ShardLatency.histogram("gone")) == 0
  end

  test "recording is a no-op when disabled" do
    Application.put_env(:fathom, :shard_load, false)
    ShardLatency.record("x", native_us(1500))

    assert Enum.sum(ShardLatency.histogram("x")) == 0
  end

  # ── through the hot path ──

  test "a query records latency, and a stop forgets it" do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    id = uniq("lat")
    on_exit(fn -> clean_shard(id) end)

    {:ok, handle} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(handle, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(handle, stmt("INSERT INTO kv VALUES ('a')"))
    {:ok, _} = ShardExecutor.execute(handle, stmt("SELECT v FROM kv"))

    assert Enum.sum(ShardLatency.histogram(id)) >= 3,
           "each executed statement records a latency sample"

    p = ShardLatency.percentiles(id)
    assert p.p50_ms >= 0.0 and p.p99_ms >= p.p50_ms

    :ok = ShardExecutor.close(handle)
    :ok = Shards.drain(id)
    assert Enum.sum(ShardLatency.histogram(id)) == 0, "a stopped shard's histogram is forgotten"
  end

  test "the hot path records nothing when disabled" do
    Application.put_env(:fathom, :shard_load, false)
    Application.put_env(:fathom, :shard_idle_ms, 50)
    id = uniq("latoff")
    on_exit(fn -> clean_shard(id) end)

    {:ok, handle} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(handle, stmt("CREATE TABLE kv (v TEXT)"))
    :ok = ShardExecutor.close(handle)

    assert Enum.sum(ShardLatency.histogram(id)) == 0
    Shards.drain(id)
  end

  # ── hot-path floor/ceiling guard (excluded from the default suite) ──

  @tag :bench
  test "record is a cheap lock-free ETS bump" do
    dt = native_us(1000)

    {us, :ok} =
      :timer.tc(fn -> Enum.each(1..100_000, fn _ -> ShardLatency.record("bench", dt) end) end)

    # 100k bumps on one key — an order-of-magnitude ceiling, not an exact latency.
    assert us < 2_000_000, "100k ShardLatency.record calls took #{us}µs (expected < 2s)"
  end
end
