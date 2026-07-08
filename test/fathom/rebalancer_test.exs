defmodule Fathom.RebalancerTest do
  @moduledoc "The rebalancer config hub — node identity, backend set, env parsing."
  use ExUnit.Case, async: true

  alias Fathom.Rebalancer

  describe "parse_hot_qps_floor!/1 (finding #16)" do
    test "accepts an integer or float string" do
      assert Rebalancer.parse_hot_qps_floor!("500") == 500.0
      assert Rebalancer.parse_hot_qps_floor!("500.0") == 500.0
      assert Rebalancer.parse_hot_qps_floor!(" 250.5 ") == 250.5
    end

    test "raises on an unusable value instead of silently disabling the rebalancer" do
      # Regression for #16: the old String.to_float/1 boot-crashed on the integer form, and
      # a 0/negative/non-numeric floor silently degraded to the (often inert) p99 path.
      assert_raise ArgumentError, ~r/positive number/, fn ->
        Rebalancer.parse_hot_qps_floor!("0")
      end

      assert_raise ArgumentError, fn -> Rebalancer.parse_hot_qps_floor!("-5") end
      assert_raise ArgumentError, fn -> Rebalancer.parse_hot_qps_floor!("abc") end
      assert_raise ArgumentError, fn -> Rebalancer.parse_hot_qps_floor!("500x") end
      assert_raise ArgumentError, fn -> Rebalancer.parse_hot_qps_floor!("") end
    end
  end
end
