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
end
