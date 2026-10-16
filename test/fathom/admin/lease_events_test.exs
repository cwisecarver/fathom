defmodule Fathom.Admin.LeaseEventsTest do
  @moduledoc """
  The bounded lease-event ring (expert review 2026-10-15 #21), the lease-health snapshot (#9) and
  the heartbeat telemetry additions: `renew_ms` on `:renewed` and `:skew_lapse` once per episode.
  """
  use ExUnit.Case, async: false

  alias Fathom.Admin.{LeaseEvents, LeaseHealth}
  alias Fathom.Shard.{Heartbeat, WriteFence}

  setup do
    LeaseEvents.clear()
    :ok
  end

  describe "LeaseEvents ring" do
    test "starts empty" do
      assert LeaseEvents.recent() == []
    end

    test "records telemetry events newest-first with the shard id" do
      :telemetry.execute([:fathom, :shard, :lease, :superseded], %{count: 1}, %{
        shard_id: "ring_a"
      })

      :telemetry.execute([:fathom, :shard, :fenced_quarantine], %{count: 1, unflushed: 3}, %{
        shard_id: "ring_b"
      })

      assert [%{kind: :fenced_quarantine, shard_id: "ring_b", detail: d}, %{kind: :superseded}] =
               LeaseEvents.recent()

      assert d =~ "3 un-flushed"
    end

    test "a successful renewal is NOT recorded (it is the lease RPS and would evict everything)" do
      :telemetry.execute([:fathom, :shard, :lease, :renewed], %{count: 1}, %{shard_id: "ring_c"})
      assert LeaseEvents.recent() == []
    end

    test "is bounded at capacity, keeping the newest" do
      cap = LeaseEvents.capacity()
      for i <- 1..(cap + 25), do: LeaseEvents.record(:held, "s#{i}", "")

      events = LeaseEvents.recent(cap + 100)
      assert length(events) == cap
      assert hd(events).shard_id == "s#{cap + 25}"
      assert List.last(events).shard_id == "s26"
    end
  end

  describe "LeaseHealth.snapshot/0" do
    test "legacy mode (no heartbeat process) reports mode and nil lapsed" do
      refute Heartbeat.running?()
      s = LeaseHealth.snapshot()
      assert s.mode == :legacy
      assert s.lapsed == nil
      assert s.generation == nil
      assert s.write_fenced_shards >= 0
    end

    test "heartbeat mode reports generation, deadline and margin from the status row" do
      hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
      _ = :sys.get_state(hb)
      s = LeaseHealth.snapshot()
      assert s.mode == :heartbeat
      assert is_integer(s.generation)
      assert s.margin_ms == 10_000
      assert s.skew_ms >= 0
    end

    test "a past deadline at the same generation reads as lapsed" do
      hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
      state = :sys.get_state(hb)

      Heartbeat.publish_status(%{
        state
        | mono_deadline_ms: System.monotonic_time(:millisecond) - 1
      })

      s = LeaseHealth.snapshot()
      assert s.lapsed == true
      assert s.ms_until_deadline < 0
    end

    test "counts write-fenced shards" do
      before = WriteFence.count()
      WriteFence.fence("lh_fenced_#{System.unique_integer([:positive])}")
      assert LeaseHealth.snapshot().write_fenced_shards == before + 1
    end
  end

  describe "heartbeat telemetry" do
    test ":renewed carries renew_ms" do
      me = self()
      id = {__MODULE__, :renewed}

      :telemetry.attach(
        id,
        [:fathom, :shard, :heartbeat, :renewed],
        fn _e, m, _meta, _ ->
          send(me, {:renewed, m})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(id) end)
      start_supervised!({Heartbeat, ttl_ms: 30_000})
      assert_receive {:renewed, %{count: 1, renew_ms: ms}}, 2_000
      assert is_integer(ms) and ms >= 0
    end
  end
end
