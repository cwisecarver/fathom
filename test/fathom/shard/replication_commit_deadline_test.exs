defmodule Fathom.Shard.Replication.CommitDeadlineTest do
  @moduledoc """
  Expert review 2026-08-31 #21: `Session.commit/3` calls `GenServer.call(pid, {:commit, _},
  timeout())` and the handler set its own deadline to `now + timeout()` — the SAME timeout. The
  caller starts its call timer BEFORE the message is delivered and the reply travels back after the
  handler finishes, so a commit that ran right to the deadline was abandoned by the caller
  (`{:session_down, :timeout}`) exactly when the handler would have replied cleanly. The handler's
  deadline budget is now shorter by a reply margin, floored at half the timeout.

  A pure relationship test — no cluster fixture needed.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Session

  test "the handler's commit deadline budget is strictly shorter than the caller's call timeout" do
    prev_t = Application.get_env(:fathom, :replication_timeout_ms)
    prev_m = Application.get_env(:fathom, :replication_reply_margin_ms)

    on_exit(fn ->
      restore(:replication_timeout_ms, prev_t)
      restore(:replication_reply_margin_ms, prev_m)
    end)

    # Default margin, across a large and a small timeout.
    Application.delete_env(:fathom, :replication_reply_margin_ms)

    for t <- [5_000, 500, 200] do
      Application.put_env(:fathom, :replication_timeout_ms, t)
      budget = Session.commit_deadline_budget()

      assert budget < Session.timeout(),
             "the handler deadline (#{budget}) must be shorter than the caller call timeout (#{t}) " <>
               "so the reply lands before the caller gives up"

      assert budget >= div(t, 2),
             "the reply margin must not gut a small timeout (t=#{t}, budget=#{budget})"
    end

    # A margin larger than half the timeout is floored, not allowed to gut the budget.
    Application.put_env(:fathom, :replication_timeout_ms, 1_000)
    Application.put_env(:fathom, :replication_reply_margin_ms, 900)
    assert Session.commit_deadline_budget() == 500
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)
end
