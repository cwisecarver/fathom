defmodule Fathom.LogLimiter do
  @moduledoc """
  A per-node, lock-free log rate limiter for lines that fire once per failed request.

  Expert review 2026-10-19 #14: a lost replication quorum logged twice per failed write (62 µs per
  call at 16 concurrent writers, `Logger.warning` + `inspect`), exactly when the node is degraded
  and every writer is failing. The signal survives in telemetry
  (`fathom.replication.no_quorum.count`), so the log only needs to name the incident, not each
  write.

  `check/4` admits the first `limit` calls in each `window_ms` window and suppresses the rest.
  The first call of the NEXT window reports how many were suppressed in the window before, so the
  caller can log one summary line. State is one `:atomics` array per key (window id, admitted
  count, suppressed count) in `:persistent_term`; the hot path is one `:atomics.get` plus one
  `add_get`. A window-rollover race can admit a few extra lines or drop one summary — it is a log
  throttle, not an accounting device. If traffic stops, the last window's suppressed count is not
  reported (the telemetry counter holds it).
  """

  @type key :: atom()
  @type verdict :: :log | {:log, suppressed :: pos_integer()} | :suppress

  @doc """
  Decides whether a line keyed `key` may be logged now. `now_ms` is injectable for tests.

  Returns `:log`, `{:log, n}` (log, and `n` lines were suppressed in the previous window), or
  `:suppress`.
  """
  @spec check(key(), pos_integer(), pos_integer(), integer()) :: verdict()
  def check(key, limit \\ 5, window_ms \\ 1_000, now_ms \\ System.monotonic_time(:millisecond)) do
    ref = ref(key)
    window = div(now_ms, window_ms) + 1
    current = :atomics.get(ref, 1)

    cond do
      current == window ->
        count(ref, limit)

      :atomics.compare_exchange(ref, 1, current, window) == :ok ->
        # Won the rollover: reset the counters, and surface the previous window's suppressed count.
        # (Window ids are offset by 1 so the array's initial 0 means "no window yet".)
        suppressed = :atomics.exchange(ref, 3, 0)
        :atomics.put(ref, 2, 1)
        if suppressed > 0, do: {:log, suppressed}, else: :log

      true ->
        count(ref, limit)
    end
  end

  defp count(ref, limit) do
    if :atomics.add_get(ref, 2, 1) <= limit do
      :log
    else
      :atomics.add(ref, 3, 1)
      :suppress
    end
  end

  defp ref(key) do
    pt = {__MODULE__, key}

    case :persistent_term.get(pt, nil) do
      nil ->
        # Two racers may each create one; the loser's array is simply garbage after the next get.
        :persistent_term.put(pt, :atomics.new(3, signed: true))
        :persistent_term.get(pt)

      ref ->
        ref
    end
  end

  @doc "Forgets a key's window state (tests)."
  @spec reset(key()) :: :ok
  def reset(key) do
    _ = :persistent_term.erase({__MODULE__, key})
    :ok
  end
end
