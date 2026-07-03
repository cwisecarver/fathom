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
    # The warm-vs-cold failover RTO delta is only meaningful against a network store
    # (the warm win is the S3 body transfer avoided), so it's S3-only like cold_open_s3.
    unless System.get_env("FATHOM_S3_TEST_ENDPOINT") do
      assert Fathom.Bench.failover_rto() == nil
    end
  end
end
