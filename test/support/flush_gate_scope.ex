defmodule Fathom.Test.FlushGateScope do
  @moduledoc """
  `Fathom.Shard.FlushGate` slot counts with other tests' coordinators taken out.

  The gate's counters are node-global and bounded by default, so a coordinator lingering from an
  EARLIER test that flushes mid-scenario takes a slot and moves `FlushGate.in_flight/0` (CI OTP 28,
  2026-11-06: `shard_flush_deadline_test` saw a "leaked" slot that was a stranger's flush, fixed in
  15e4f6d8). The gate records every holder by pid (`{:holder, pid}`, `{:stamp_holder, pid}`), so a
  holder that is a coordinator registered to a shard other than `shard` (any coordinator when
  `shard` is nil) is subtracted. A test-process or spawned holder is the test's own and counts.
  """

  alias Fathom.Shard.FlushGate

  @doc "Main-lane slots in flight, minus those held by foreign coordinators."
  @spec in_flight(String.t() | nil) :: integer()
  def in_flight(shard \\ nil), do: FlushGate.in_flight() - foreign(:holder, shard)

  @doc "Stamp-lane slots in flight, minus those held by foreign coordinators."
  @spec stamp_in_flight(String.t() | nil) :: integer()
  def stamp_in_flight(shard \\ nil),
    do: FlushGate.stamp_in_flight() - foreign(:stamp_holder, shard)

  defp foreign(tag, shard) do
    FlushGate
    |> :ets.match({{tag, :"$1"}, :_})
    |> Enum.count(fn [pid] ->
      Enum.any?(Registry.keys(Fathom.ShardRegistry, pid), &(&1 != shard))
    end)
  end
end
