defmodule Fathom.Test.ShardFlush do
  @moduledoc """
  Drive a shard coordinator's durability flush to completion in a test.

  The flush runs in a task the coordinator spawns (`state.flush_task`), so "the flush is done" is a
  state, not a message. Twenty-odd test files each polled `:sys.get_state(c).flush_task == nil`
  with their own try-counter loop (cleanliness audit 2026-11-13 #38); this is the one copy, on a
  monotonic deadline (`Fathom.Test.Await`) so a slow `:sys.get_state` cannot stretch the wait.
  """

  alias Fathom.Test.Await

  @doc """
  Wait until the coordinator has no flush task in flight. Does NOT start a flush: use it after a
  write/close that arms one, or after `flush!/2`.
  """
  @spec settle!(pid(), non_neg_integer()) :: :ok
  def settle!(coordinator, timeout_ms \\ 5_000) when is_pid(coordinator) do
    Await.until(
      fn -> :sys.get_state(coordinator).flush_task == nil end,
      "the flush task never settled",
      timeout_ms
    )
  end

  @doc """
  Ask the coordinator to flush now (`:durability_flush`), then wait for the task to finish. The
  `:sys.get_state` inside `settle!/2` is ordered after the message, so a task the message starts is
  visible to it.
  """
  @spec flush!(pid(), non_neg_integer()) :: :ok
  def flush!(coordinator, timeout_ms \\ 5_000) when is_pid(coordinator) do
    send(coordinator, :durability_flush)
    settle!(coordinator, timeout_ms)
  end
end
