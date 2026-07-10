defmodule Fathom.Bench.TpccSweepTest do
  @moduledoc """
  Smoke-checks the TPC-C driver + W-sweep (Phase 3, unit 3) end to end over the wire: a tiny
  sweep produces one JSON-ready row per warehouse count, each with `tpcc_tpmc` and the per-txn-
  type p50/p95/p99/max keys the `mix fathom.tpcc` task appends to `scripts/tpc_history.jsonl`.
  Tests the driver function directly, so it never touches the real history file.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.Tpcc

  @txn_names ~w(neworder payment order_status delivery stock_level)

  test "sweep yields one result row per W with tpmC and per-txn percentile keys" do
    rows = Tpcc.sweep(max_w: 2, threads: 2, txns: 40, scale: 0.001)

    assert length(rows) == 2
    assert Enum.map(rows, & &1["warehouses"]) == [1, 2]

    for row <- rows do
      assert row["threads"] == 2
      assert is_float(row["tpcc_tpmc"]) and row["tpcc_tpmc"] >= 0.0

      for name <- @txn_names, stat <- ~w(p50 p95 p99 max) do
        key = "tpcc_#{name}_#{stat}_us"
        assert Map.has_key?(row, key), "missing #{key}"
        # New-Order/Payment dominate the deck, so their percentiles are populated (not nil).
        if name in ["neworder", "payment"], do: assert(is_float(row[key]), "#{key} should be set")
      end
    end
  end
end
