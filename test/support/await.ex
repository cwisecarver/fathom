defmodule Fathom.Test.Await do
  @moduledoc """
  Polling helpers for tests that wait on a condition no process announces.

  Both poll every 10 ms against a monotonic deadline (not a try counter, so a slow `fun` cannot
  stretch the wait). Prefer a message or a monitor where one exists; use these when the thing
  being waited for exposes only state.
  """

  import ExUnit.Assertions, only: [flunk: 1]

  @step_ms 10

  @doc "Poll `fun` until it returns a truthy value; `flunk`s with `label` on timeout."
  @spec until((-> as_boolean(term())), String.t(), non_neg_integer()) :: :ok
  def until(fun, label, timeout_ms \\ 5_000) do
    if eventually?(fun, timeout_ms), do: :ok, else: flunk(label)
  end

  @doc "Poll `fun` until truthy; true on success, false when `timeout_ms` passes."
  @spec eventually?((-> as_boolean(term())), non_neg_integer()) :: boolean()
  def eventually?(fun, timeout_ms \\ 3_000) do
    poll(fun, System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp poll(fun, deadline) do
    cond do
      fun.() -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> Process.sleep(@step_ms) && poll(fun, deadline)
    end
  end
end
