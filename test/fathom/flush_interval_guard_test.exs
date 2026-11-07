defmodule Fathom.FlushIntervalGuardTest do
  @moduledoc """
  Expert review 2026-11-07 #81 (1). Symptom: `SHARD_FLUSH_INTERVAL_MS=0` (or negative) was accepted
  in prod with no signal. A non-positive interval disables the periodic durability flush
  ("idle-only durability"), so a continuously active shard never flushes and its loss window is
  unbounded; `check_replication_flush_interval!/0` only looked at `0 < interval < 30 s`.
  Invariant: prod logs a warning for a non-positive interval, and only for that.

  Pure `flush_interval_problem/2` with explicit arguments, so no global env is touched.
  """
  use ExUnit.Case, async: true

  alias Fathom.Application, as: App

  test "prod warns for 0 and negative intervals, naming the variable and the consequence" do
    for interval <- [0, -1, -300_000] do
      message = App.flush_interval_problem(:prod, interval)
      assert message =~ "SHARD_FLUSH_INTERVAL_MS = #{interval}"
      assert message =~ "DISABLES the periodic durability flush"
    end
  end

  test "a positive interval, and non-prod envs, never warn" do
    for interval <- [1, 5_000, 30_000, 300_000] do
      assert App.flush_interval_problem(:prod, interval) == nil
    end

    for env <- [:dev, :test] do
      assert App.flush_interval_problem(env, 0) == nil
    end
  end

  test "a non-integer interval is not this guard's concern" do
    assert App.flush_interval_problem(:prod, nil) == nil
    assert App.flush_interval_problem(:prod, :infinity) == nil
  end
end
