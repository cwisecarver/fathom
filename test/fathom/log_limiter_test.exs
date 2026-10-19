defmodule Fathom.LogLimiterTest do
  @moduledoc """
  Expert review 2026-10-19 #14: per-failed-write quorum warnings are throttled to `limit` per window
  with one summary of the suppressed count at the next window. Time is injected, so these pin the
  window boundaries deterministically.
  """
  use ExUnit.Case, async: false

  alias Fathom.LogLimiter

  setup do
    key = :"lim_#{System.unique_integer([:positive])}"
    on_exit(fn -> LogLimiter.reset(key) end)
    %{key: key}
  end

  test "admits the first N per window and suppresses the rest", %{key: k} do
    verdicts = for _ <- 1..8, do: LogLimiter.check(k, 3, 1_000, 10_000)
    assert verdicts == [:log, :log, :log, :suppress, :suppress, :suppress, :suppress, :suppress]
  end

  test "the next window reports the suppressed count once, then admits again", %{key: k} do
    for _ <- 1..8, do: LogLimiter.check(k, 3, 1_000, 10_000)

    # 10_999 is the last ms of the window; 11_000 is the first of the next.
    assert LogLimiter.check(k, 3, 1_000, 10_999) == :suppress
    assert LogLimiter.check(k, 3, 1_000, 11_000) == {:log, 6}
    assert LogLimiter.check(k, 3, 1_000, 11_001) == :log
    assert LogLimiter.check(k, 3, 1_000, 11_002) == :log
    assert LogLimiter.check(k, 3, 1_000, 11_003) == :suppress
  end

  test "a quiet window produces no summary", %{key: k} do
    assert LogLimiter.check(k, 3, 1_000, 5_000) == :log
    assert LogLimiter.check(k, 3, 1_000, 9_000) == :log
  end

  test "keys are independent", %{key: a} do
    b = :"#{a}_b"
    on_exit(fn -> LogLimiter.reset(b) end)
    assert LogLimiter.check(a, 1, 1_000, 1_000) == :log
    assert LogLimiter.check(a, 1, 1_000, 1_000) == :suppress
    assert LogLimiter.check(b, 1, 1_000, 1_000) == :log
  end

  test "no_quorum_error counts every failed write but logs only the first few" do
    LogLimiter.reset(:no_quorum)
    on_exit(fn -> LogLimiter.reset(:no_quorum) end)
    me = self()
    id = "lim-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      [:fathom, :replication, :no_quorum],
      fn _, m, md, _ -> send(me, {:nq, m, md}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        for _ <- 1..20 do
          assert %{code: "FILO_NO_QUORUM", status: 503} =
                   Fathom.ShardExecutor.no_quorum_error("s", :timeout)
        end
      end)

    for _ <- 1..20, do: assert_receive({:nq, _, %{reason: :timeout}})

    # 20 failed writes, all counted; at most 5 logged per window (a rollover mid-loop may add a
    # few lines plus one summary).
    assert length(String.split(log, "write not replicated")) - 1 <= 11
  end
end
