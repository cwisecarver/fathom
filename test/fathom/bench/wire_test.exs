defmodule Fathom.Bench.WireTest do
  @moduledoc """
  Smoke-checks the wire benches (Phase 1). Not async — starts a Filo listener.

  **:bench, i.e. excluded from the default suite.** Every assertion here bounds a real measured
  latency, so the whole module is a microbench and AGENTS.md puts those behind `:bench` — the
  default suite should fail on broken behaviour, not on a busy host. Two of them were observed
  failing purely from machine load on 2026-07-26: `tpcb_wire_overhead`'s delta inverted by 51ms
  (a sign flip no margin can rescue), and `cold_open_wire` exceeded its 100ms ceiling at load
  average 30-60.

  These still run — `mix test --include bench`, and `mix fathom.wire_bench` is the real gate.
  Run them on a quiet host, which is the only place a latency number means anything.
  """
  use ExUnit.Case, async: false

  @moduletag :bench

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

  # The most load-fragile assertion in the module: unlike its neighbours, which bound a single
  # measurement on one side, this asserts the SIGN OF A DIFFERENCE between two independently noisy
  # timings. Under load the raw leg can lose the race to the wire leg outright (observed at
  # -51,610µs), and no ceiling fixes a sign flip.
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
