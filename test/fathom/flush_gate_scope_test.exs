defmodule Fathom.Test.FlushGateScopeTest do
  # Pins the test helper the FlushGate-counting tests rely on: a slot held by a coordinator of
  # ANOTHER shard (what a lingering coordinator from an earlier test is) must not count, while
  # the test's own holders and its own shard's coordinator must. CI OTP 28, 2026-11-06.
  use ExUnit.Case, async: false

  alias Fathom.Shard.FlushGate
  alias Fathom.Test.FlushGateScope

  setup do
    prev = Application.get_env(:fathom, :shard_flush_max_concurrency)
    Application.put_env(:fathom, :shard_flush_max_concurrency, 4)
    FlushGate.reset()

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :shard_flush_max_concurrency, prev),
        else: Application.delete_env(:fathom, :shard_flush_max_concurrency)

      FlushGate.reset()
    end)

    :ok
  end

  # A process registered as `shard`'s coordinator that takes a main-lane slot and holds it.
  defp holder_for(shard) do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, _} = Registry.register(Fathom.ShardRegistry, shard, nil)
        send(parent, {:held, self(), FlushGate.try_acquire()})
        receive do: (:release -> FlushGate.release())
      end)

    assert_receive {:held, ^pid, :ok}, 2_000
    on_exit(fn -> send(pid, :release) end)
    pid
  end

  test "another shard's coordinator holding a slot is not counted" do
    own = "fgscope-own-#{System.unique_integer([:positive])}"
    _stray = holder_for("fgscope-stray-#{System.unique_integer([:positive])}")
    :ok = FlushGate.try_acquire()

    assert FlushGate.in_flight() == 2, "precondition: the stray really holds a slot"
    assert FlushGateScope.in_flight() == 1
    assert FlushGateScope.in_flight(own) == 1

    FlushGate.release()
  end

  test "the test's own shard's coordinator is counted when the shard is named" do
    own = "fgscope-own-#{System.unique_integer([:positive])}"
    _mine = holder_for(own)

    assert FlushGateScope.in_flight(own) == 1
    assert FlushGateScope.in_flight() == 0, "unnamed, every coordinator is foreign"
  end
end
