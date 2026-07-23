defmodule Fathom.RateLimiterTest do
  @moduledoc """
  The ETS fixed-window counter behind the control-plane throttles (expert review #34). Windows are
  driven with an explicit `now` so rollover is deterministic (no sleeps). The table is node-global
  (started by the app), so each test resets it.
  """
  use ExUnit.Case, async: false

  alias Fathom.RateLimiter

  setup do
    RateLimiter.reset()
    on_exit(&RateLimiter.reset/0)
    :ok
  end

  test "count/bump track hits within a window and reset once it elapses" do
    b = :rl_win
    k = {127, 0, 0, 1}
    now = 1_000

    assert RateLimiter.count(b, k, 100, now) == 0
    assert RateLimiter.bump(b, k, 100, now) == 1
    assert RateLimiter.bump(b, k, 100, now + 50) == 2
    assert RateLimiter.count(b, k, 100, now + 99) == 2

    # At exactly start+window the window has elapsed: count resets to 0, and the next bump opens a
    # fresh window.
    assert RateLimiter.count(b, k, 100, now + 100) == 0
    assert RateLimiter.bump(b, k, 100, now + 100) == 1
  end

  test "check allows exactly `limit` hits per window, then refuses" do
    b = :rl_check
    k = :ip
    now = 0

    assert RateLimiter.check(b, k, 2, 100, now) == :ok
    assert RateLimiter.check(b, k, 2, 100, now) == :ok
    assert RateLimiter.check(b, k, 2, 100, now) == :limited

    # A new window admits again.
    assert RateLimiter.check(b, k, 2, 100, now + 100) == :ok
  end

  test "forget clears a key's counter (the admin success-resets path)" do
    b = :rl_forget
    k = :ip
    now = 0

    RateLimiter.bump(b, k, 100, now)
    RateLimiter.bump(b, k, 100, now)
    assert RateLimiter.count(b, k, 100, now) == 2

    RateLimiter.forget(b, k)
    assert RateLimiter.count(b, k, 100, now) == 0
  end

  test "buckets and keys are independent" do
    now = 0
    RateLimiter.bump(:bucket_a, :ip1, 100, now)

    assert RateLimiter.count(:bucket_a, :ip1, 100, now) == 1
    assert RateLimiter.count(:bucket_b, :ip1, 100, now) == 0, "a different bucket is separate"
    assert RateLimiter.count(:bucket_a, :ip2, 100, now) == 0, "a different key is separate"
  end
end
