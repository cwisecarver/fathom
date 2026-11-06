defmodule Fathom.Test.TelemetryScope do
  @moduledoc """
  Scoping for `:telemetry` handlers in tests whose events carry no shard id.

  Telemetry is node-global: a handler attached by one test also sees the events a lingering
  coordinator, replication Session or follower from an EARLIER test emits (two CI flakes in
  2026-11 had this shape: `s3_op_count_test`, 9bdcb563, and a `capture_log` refute, 98163a74). A
  handler that only counts or refutes should call `foreign?/1` first and drop the event.
  """

  @registries [Fathom.ShardRegistry, Fathom.Shard.Replication.SessionRegistry]

  @doc """
  True when the calling process, or one it works for (`$callers`), is registered to a shard other
  than `shard` in the coordinator or replication-session registry. Call it INSIDE the handler (it
  runs in the emitter). Unattributed emitters (a test process, an unregistered task) are kept.
  """
  @spec foreign?(String.t()) :: boolean()
  def foreign?(shard) do
    keys =
      for pid <- [self() | Process.get(:"$callers", [])],
          registry <- @registries,
          # The replication registry only exists when replication is on (off in dev/test).
          Process.whereis(registry) != nil,
          key <- Registry.keys(registry, pid),
          do: key

    Enum.any?(keys, &(&1 != shard))
  end

  @doc """
  Drains every `{event, id}` message from the test mailbox (the shape the eviction handlers send)
  and returns the ids that are in `ids`. `refute_received` takes no runtime list in a guard, so a
  refute scoped to "this test's tenants" is `assert received_for(event, ids) == []`.
  """
  @spec received_for(term(), [String.t()]) :: [String.t()]
  def received_for(event, ids), do: drain(event, ids, [])

  defp drain(event, ids, acc) do
    receive do
      {^event, id} -> drain(event, ids, if(id in ids, do: [id | acc], else: acc))
    after
      0 -> Enum.reverse(acc)
    end
  end
end
