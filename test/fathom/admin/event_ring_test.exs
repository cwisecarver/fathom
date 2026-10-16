defmodule Fathom.Admin.EventRingTest do
  use ExUnit.Case, async: false

  alias Fathom.Admin.EventRing

  test "push/recent are no-ops when the ring is not running" do
    refute Process.whereis(EventRing)
    assert :ok = EventRing.push(:k, %{a: 1})
    assert EventRing.recent(:k, 5) == []
    assert EventRing.total(:k) == 0
  end

  describe "running" do
    setup do
      start_supervised!(EventRing)
      :ok
    end

    test "newest first, per-kind isolation, t stamped" do
      for i <- 1..3, do: EventRing.push(:a, %{i: i})
      EventRing.push(:b, %{i: 99})

      assert [%{i: 3, t: t}, %{i: 2}, %{i: 1}] = EventRing.recent(:a, 10)
      assert is_integer(t)
      assert [%{i: 99}] = EventRing.recent(:b, 10)
      assert [%{i: 3}] = EventRing.recent(:a, 1)
      assert EventRing.recent(:a, 0) == []
      assert EventRing.recent(:never, 5) == []
    end

    # The memory bound: pushing far past capacity keeps exactly `capacity/0` entries per kind, the
    # NEWEST ones, and the table does not grow.
    test "is bounded at capacity per kind, keeping the newest" do
      cap = EventRing.capacity()
      total = cap * 3 + 7
      for i <- 1..total, do: EventRing.push(:a, %{i: i})

      recent = EventRing.recent(:a, 10_000)
      assert length(recent) == cap
      assert hd(recent).i == total
      assert List.last(recent).i == total - cap + 1
      assert EventRing.total(:a) == total

      # one head row + `cap` slots, regardless of how many were pushed
      assert :ets.info(EventRing, :size) == cap + 1
    end
  end
end
