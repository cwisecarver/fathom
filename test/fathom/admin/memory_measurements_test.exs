defmodule Fathom.Admin.MemoryMeasurementsTest do
  # Expert review 2026-10-17 #33: the poller called :erlang.memory twice per tick (node_memory/0 and
  # memory_breakdown/0). One call now feeds both events; the node-total gauge must still be emitted
  # and the poller must not list a second memory measurement.
  use ExUnit.Case, async: true

  test "memory_breakdown/0 emits both the breakdown and the node total from one call" do
    ref = make_ref()
    test = self()

    :telemetry.attach_many(
      {__MODULE__, ref},
      [[:fathom, :node, :memory], [:fathom, :node, :memory_breakdown]],
      fn event, m, _meta, _ -> send(test, {ref, event, m}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

    Fathom.Admin.Measurements.memory_breakdown()

    assert_receive {^ref, [:fathom, :node, :memory], %{total: total}} when total > 0
    assert_receive {^ref, [:fathom, :node, :memory_breakdown], %{processes: _, ets: _, system: _}}
  end
end
