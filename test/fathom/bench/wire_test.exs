defmodule Fathom.Bench.WireTest do
  @moduledoc "Smoke-checks the wire benches (Phase 1). Not async — starts a Filo listener."
  use ExUnit.Case, async: false

  alias Fathom.Bench.Wire

  test "hrana_rt returns a sane warm-stream round-trip p50 over the wire" do
    # A warm loopback WS SELECT-1 round-trip is sub-millisecond; a 50 ms ceiling is a generous
    # order-of-magnitude smoke bound (per AGENTS.md hot-path guidance), not an exact latency.
    us = Wire.hrana_rt(hrana_rt_samples: 25)
    assert is_float(us)
    assert us > 0.0
    assert us < 50_000.0, "hrana_rt p50 #{us}µs exceeded the 50ms smoke ceiling"
  end

  test "cold_open_wire returns a sane cold-first-query p50 over the wire" do
    # Each sample cold-opens a fresh shard (pull from storage + coordinator start) behind a
    # fresh WS connect. Local storage on loopback is a few ms/open; 100 ms is a generous
    # order-of-magnitude smoke ceiling.
    us = Wire.cold_open_wire(cold_open_wire_samples: 5)
    assert is_float(us)
    assert us > 0.0
    assert us < 100_000.0, "cold_open_wire p50 #{us}µs exceeded the 100ms smoke ceiling"
  end

  # :bench, unlike its neighbours, because this is the one assertion here that a margin cannot
  # rescue. The others bound a single measurement on one side and can be given generous headroom;
  # this asserts the SIGN OF A DIFFERENCE between two independently noisy timings. Under machine
  # load the raw leg can lose the race to the wire leg outright — observed 2026-07-26 at
  # -51,610µs, i.e. the delta inverted by 51ms — and no ceiling fixes a sign flip. Per AGENTS.md,
  # a comparative microbench belongs behind :bench, where the host is expected quiet.
  #
  # Its real gate is `mix fathom.wire_bench`; this was only ever a smoke check.
  @tag :bench
  test "tpcb_wire_overhead is a positive per-txn delta (wire costs more than raw exqlite)" do
    # The wire leg sends 7 statements as 7 round-trips vs raw local calls, so the delta is
    # positive and dominated by the round-trips. Small txn count for a smoke check.
    us = Wire.tpcb_wire_overhead(tpcb_txns: 20)
    assert is_float(us)
    assert us > 0.0, "wire TPC-B txn should cost more than raw exqlite (got #{us}µs)"
    assert us < 100_000.0, "tpcb_wire_overhead #{us}µs exceeded the 100ms/txn smoke ceiling"
  end

  test "tpcb_node_tps returns a sane aggregate write throughput over the wire" do
    # A few shards each bursting a handful of concurrent TPC-B write txns through the wire.
    # We assert only a sane positive rate here (throughput is host/fsync-dominated); the
    # loose 50% gate lives in mix fathom.wire_bench, and isolation is proven separately in
    # test/fathom/tpcb_isolation_test.exs.
    tps = Wire.tpcb_node_tps(tpcb_shards: 4, tpcb_node_txns: 3)
    assert is_float(tps)
    assert tps > 0.0, "aggregate node TPS should be positive (got #{tps})"
  end
end
