defmodule Fathom.Test.RegistryKill do
  @moduledoc """
  Cleanup for tests that kill a `Fathom.ShardRegistry` partition on purpose.

  The Registry's supervisor is `:one_for_all` with the default budget of 3 restarts in 5 s. A killed
  partition restarts EVERY partition, which drops every registration, so two things leak into the
  next test unless the killing test cleans up:

    1. **The restart may still be in flight.** A later test that registers a coordinator before the
       one_for_all restart lands loses that registration, its coordinator stops as an orphan, and
       the test sees `{:error, :unavailable}` (CI 2026-10-13, OTP 28,
       `shard_coordinator_open_test` fork-evidence retry).
    2. **The restart counts against the budget.** Several partition-kill tests inside 5 s make the
       4th restart exceed it, the Registry supervisor gives up, and the next `:sys.get_state/1`
       exits `shutdown` (CI 2026-10-13, OTP 28, `shards_open_count_test`).

  `settle/0` waits for the restart (a supervisor handles the partition's EXIT and restarts its
  children before it answers a system message) and then drops the restart from the supervisor's
  history. The kill is a test fixture, not a fault the suite measures.
  """
  import ExUnit.Assertions

  @spec settle(atom()) :: :ok
  def settle(registry \\ Fathom.ShardRegistry) do
    case Process.whereis(registry) do
      sup when is_pid(sup) ->
        _ = :sys.get_state(sup)
        forget_restarts(sup)

      nil ->
        :ok
    end
  end

  # `restarts` follows `intensity` and `period` in OTP's supervisor state record (element 7 on OTP
  # 28 and 29). The shape is asserted so a future layout change fails loudly here instead of
  # corrupting the supervisor's state.
  defp forget_restarts(sup) do
    :sys.replace_state(sup, fn state ->
      assert :state = elem(state, 0)

      assert is_integer(elem(state, 5)) and is_integer(elem(state, 6)) and
               is_list(elem(state, 7)),
             "OTP's supervisor state layout changed; update Fathom.Test.RegistryKill"

      put_elem(state, 7, [])
    end)

    :ok
  end
end
