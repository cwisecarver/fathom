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
end
