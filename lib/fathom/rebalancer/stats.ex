defmodule Fathom.Rebalancer.Stats do
  @moduledoc """
  The shared percentile helper — used by the reporter (which computes each node's
  full-distribution p99) and the policy (its legacy sample-based p99 fallback). Kept in one
  place so the percentile method can't drift between them (finding #2).
  """

  @doc """
  Linear-interpolating percentile of `values` (0..100). Unlike a nearest-rank method, this
  does NOT collapse to the maximum for small N (finding #2): `round(0.99·(N-1)) == N-1` for
  all N ≤ 51, so nearest-rank p99 returned the max and `mult × p99 ≥ max` flagged nothing.
  Interpolation returns a value strictly between the top two ranks, so a sharp head can clear
  `mult × p99`. Empty ⇒ 0.0.
  """
  @spec percentile([number()], number()) :: float()
  def percentile([], _pct), do: 0.0

  def percentile(values, pct) do
    sorted = Enum.sort(values)
    n = length(sorted)
    rank = pct / 100 * (n - 1)
    lo = trunc(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo

    Enum.at(sorted, lo) * (1 - frac) + Enum.at(sorted, hi) * frac
  end
end
