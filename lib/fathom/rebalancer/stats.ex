defmodule Fathom.Rebalancer.Stats do
  @moduledoc """
  The shared percentile helpers — used by the reporter (which computes each node's
  full-distribution p99) and the policy (its legacy sample-based p99 fallback). Kept in one
  place so the percentile method can't drift between them (finding #2).

  ## Fleet histogram (finding #4)

  A count-weighted *mean* of per-node p99s is not the pooled-distribution p99 (it drifts with
  fleet composition — a busy node can pull the bar the wrong way). To compute a faithful fleet
  p99 without shipping every raw per-shard rate, each reporter publishes a compact fixed-bucket
  `histogram/1` of its per-shard q/s; the orchestrator sums the buckets element-wise across live
  nodes (`Fathom.Rebalancer.Nodes.fleet_p99/2`) and reads the true pooled p99 with
  `percentile_from_histogram/2`. Fixed `bucket_edges/0` (shared, so a merge is a plain
  element-wise add) span the q/s range at 1-2-5 resolution.
  """

  # Lower edges of the q/s histogram buckets (1-2-5 decades). Bucket i is [edges[i], edges[i+1]);
  # the last is [50000, +inf). Values ≥ 0 (per-shard rates are positive), so nothing underflows.
  @hist_edges [
    0.0,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    200.0,
    500.0,
    1000.0,
    2000.0,
    5000.0,
    10000.0,
    20000.0,
    50000.0
  ]

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

  @doc "The shared histogram bucket lower-edges (fixed, so merges are element-wise adds)."
  @spec bucket_edges() :: [float()]
  def bucket_edges, do: @hist_edges

  @doc "How many histogram buckets `histogram/1` emits (for validating a merged/stored vector)."
  @spec bucket_count() :: pos_integer()
  def bucket_count, do: length(@hist_edges)

  @doc """
  Bucket `values` into fixed `bucket_edges/0` counts (finding #4) — a compact, mergeable
  summary of a per-shard q/s distribution. Returns a list of `bucket_count/0` integers.
  """
  @spec histogram([number()]) :: [non_neg_integer()]
  def histogram(values) do
    counts =
      Enum.reduce(values, %{}, fn v, acc ->
        Map.update(acc, bucket_index(v), 1, &(&1 + 1))
      end)

    for i <- 0..(bucket_count() - 1), do: Map.get(counts, i, 0)
  end

  @doc """
  The `pct` percentile (0..100) of a pooled histogram (element-wise sum of `histogram/1`
  vectors), linearly interpolated within the crossing bucket — the fleet hot bar (finding #4).
  An empty/all-zero histogram ⇒ 0.0; a value landing in the open-topped overflow bucket returns
  that bucket's lower edge.
  """
  @spec percentile_from_histogram([non_neg_integer()], number()) :: float()
  def percentile_from_histogram(counts, pct) do
    total = Enum.sum(counts)

    if total == 0 do
      0.0
    else
      target = pct / 100 * total
      quantile_walk(counts, target)
    end
  end

  # The largest bucket index whose lower edge is ≤ v (values ≥ last edge land in the overflow).
  defp bucket_index(v), do: max(Enum.count(@hist_edges, &(&1 <= v)) - 1, 0)

  defp quantile_walk(counts, target) do
    {value, _cum} =
      counts
      |> Enum.with_index()
      |> Enum.reduce_while({0.0, 0}, fn {c, i}, {_v, cum} ->
        new_cum = cum + c

        if c > 0 and new_cum >= target do
          {:halt, {bucket_value(i, (target - cum) / c), new_cum}}
        else
          {:cont, {0.0, new_cum}}
        end
      end)

    value
  end

  # Interpolate within bucket `i` at fraction `frac`; the open-topped last bucket has no upper
  # edge, so return its lower edge.
  defp bucket_value(i, frac) do
    lower = Enum.at(@hist_edges, i)

    case Enum.at(@hist_edges, i + 1) do
      nil -> lower
      upper -> lower + frac * (upper - lower)
    end
  end
end
