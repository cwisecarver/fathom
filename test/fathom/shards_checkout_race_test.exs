defmodule Fathom.ShardsCheckoutRaceTest do
  # Finding #18: a checkout (or drain) that loses a race with the coordinator's lifecycle must
  # be recovered, not surfaced. Two transient reasons reach the router:
  #   * :unavailable — the coordinator stopped and its Registry entry lingered in the window
  #     before the Registry handled the :DOWN, so the checkout hit a dead pid (:noproc).
  #   * :normal — the :checkout was queued behind an :idle_timeout/drain stop, so the
  #     coordinator processed the stop first and the pending GenServer.call exited :normal.
  # Idle stops are routine at scale, so pre-fix (only :unavailable retried) :normal leaked to
  # clients as a spurious {:error, :normal} a 1 ms retry would have fixed. drain/2 had the
  # mirror bug: an already-dead coordinator (:noproc at monitor time) reported
  # {:error, {:drain_failed, :noproc}} instead of :ok. These are lifecycle races, so per the
  # testing guide we pin the classification directly rather than racing a real coordinator.
  use ExUnit.Case, async: true

  alias Fathom.Shards

  describe "retry_checkout?/1" do
    test "retries the transient lifecycle races" do
      assert Shards.retry_checkout?(:unavailable)
      assert Shards.retry_checkout?(:normal)
    end

    test "surfaces every real failure instead of spinning" do
      refute Shards.retry_checkout?(:node_at_capacity)
      refute Shards.retry_checkout?(:invalid_shard_id)
      refute Shards.retry_checkout?(:timeout)
      refute Shards.retry_checkout?({:shard_held, self()})
      refute Shards.retry_checkout?({:shard_migrating, :busy})
    end
  end

  describe "drain_down_result/1" do
    test "a clean stop or an already-gone coordinator is a successful drain" do
      assert Shards.drain_down_result(:normal) == :ok
      assert Shards.drain_down_result(:noproc) == :ok
    end

    test "an abnormal exit is a drain failure" do
      assert Shards.drain_down_result(:killed) == {:error, {:drain_failed, :killed}}

      assert Shards.drain_down_result({:shutdown, :boom}) ==
               {:error, {:drain_failed, {:shutdown, :boom}}}
    end
  end
end
