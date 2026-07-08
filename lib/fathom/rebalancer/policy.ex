defmodule Fathom.Rebalancer.Policy do
  @moduledoc """
  The rebalance decision — pure functions from load samples to proposed shard moves. No
  DB, no side effects: the orchestrator (`Fathom.Rebalancer.RebalanceJob`) fetches the
  inputs and executes the output.

  ## The hot rule (the `--hotspots` finding, encoded)

  A shard is a **hot candidate** when its current query rate clears the bar, where the bar
  is an **absolute q/s floor** if one is configured, else **`p99_multiple × fleet-p99`** —
  never `K × median` (at fleet scale the cold tail pulls the median to ~0, so a median
  multiple flags hundreds; p99-relative and absolute floors stay tight). The p99-relative
  bar self-scales: a uniform fleet has p99≈max so `mult × p99` flags nothing (no false
  hotspot); a sharp Zipf head has a low p99 so the head clears it.

  ## Anti-flap + safety

  - **Confirm windows:** a candidate must clear the bar in ≥ `confirm_windows` recent
    samples before it's moved — a one-window spike doesn't trigger a move.
  - **Cooldown:** a shard pinned within `cooldown_ms` is skipped, so a shard isn't
    ping-ponged.
  - **Improvement guard:** a move is proposed only if the target's *post-move* load stays
    below the source's *current* load — otherwise the move just relocates the hotspot.
    A uniformly-loaded fleet (nowhere better to put it) yields no moves.
  - **max_moves:** at most N moves per tick (default 1), hottest first, one shard per
    target per tick — moves are deliberate and re-evaluated against fresh samples next
    tick.

  Config (all overridable via `opts`): `:floor` (absolute q/s, default from
  `:rebalance_hot_qps_floor`, nil ⇒ p99-relative), `:p99_multiple` (20), `:confirm_windows`
  (2), `:cooldown_ms` (300_000), `:max_moves` (1).

  ## Trust assumption (expert review #17)

  The hot decision trusts `q_per_s`, a signal a tenant fully controls from the data path. So
  enabling the rebalancer (`REBALANCER_ENABLED`) presumes the Hrana trust boundary is
  enforced — the network boundary (LB-only reachability, the default posture) or
  `HRANA_AUTH=required`. Without it, an LB-reachable caller could drive a shard they don't
  own over the bar to induce a handoff (a brief drain blip on the victim). The anti-flap
  guards above bound the abuse, but the boundary is the defense, not the policy.
  """

  @type move :: %{
          shard_id: String.t(),
          from_node: String.t(),
          to_node: String.t(),
          q_per_s: float(),
          threshold: float(),
          reason: String.t()
        }

  @doc """
  Proposes moves from `samples` (a short recent history), the current `overrides` (for
  cooldown), and the `backends` set (`%{node_key => address}`, the candidate targets).
  Returns `[]` when nothing is safely worth moving.
  """
  @spec propose([map()], [map()], %{optional(String.t()) => String.t()}, keyword()) :: [move()]
  def propose(samples, overrides, backends, opts \\ []) do
    floor = Keyword.get(opts, :floor, Application.get_env(:fathom, :rebalance_hot_qps_floor))
    mult = Keyword.get(opts, :p99_multiple, cfg(:rebalance_p99_multiple, 20)) / 1.0
    confirm = Keyword.get(opts, :confirm_windows, cfg(:rebalance_confirm_windows, 2))
    cooldown_ms = Keyword.get(opts, :cooldown_ms, cfg(:rebalance_cooldown_ms, 300_000))
    max_moves = Keyword.get(opts, :max_moves, cfg(:rebalance_max_moves, 1))
    now = Keyword.get(opts, :now, DateTime.utc_now())

    latest = latest_per_shard(samples)
    rates = latest |> Map.values() |> Enum.map(& &1.q_per_s)
    threshold = hot_threshold(floor, mult, rates)
    node_load = node_load(latest)
    cooling = cooling_shards(overrides, now, cooldown_ms)
    backend_keys = Map.keys(backends)

    latest
    |> Map.values()
    |> Enum.filter(&(&1.q_per_s >= threshold and threshold > 0.0))
    |> Enum.filter(&(&1.node_key in backend_keys))
    |> Enum.reject(&(&1.shard_id in cooling))
    |> Enum.filter(&confirmed_hot?(&1, samples, threshold, confirm))
    # Hottest first, tie-broken by shard_id for a canonical (not iteration-order) choice (#18).
    |> Enum.sort_by(&{-&1.q_per_s, &1.shard_id})
    |> plan_moves(node_load, backend_keys, threshold, max_moves)
  end

  # max_moves ≤ 0 means "make no moves" — short-circuit before the reduce, which otherwise
  # appended one move and only then checked the bound (#18).
  defp plan_moves(_hot, _node_load, _backend_keys, _threshold, max_moves) when max_moves <= 0,
    do: []

  # Walk the hot shards (hottest first), assigning each to the least-loaded viable target,
  # updating a running projected node-load so we don't pile several onto one target in a
  # single tick. Stops at max_moves.
  defp plan_moves(hot, node_load, backend_keys, threshold, max_moves) do
    {moves, _load} =
      Enum.reduce_while(hot, {[], node_load}, fn s, {moves, load} ->
        case best_target(s, load, backend_keys) do
          nil ->
            {:cont, {moves, load}}

          target ->
            move = %{
              shard_id: s.shard_id,
              from_node: s.node_key,
              to_node: target,
              q_per_s: s.q_per_s,
              threshold: threshold,
              reason:
                "hot #{Float.round(s.q_per_s / 1, 1)} q/s ≥ #{Float.round(threshold / 1, 1)} q/s"
            }

            # Reflect the move in the running load so the next hot shard sees the target
            # heavier and the source lighter.
            load =
              load
              |> Map.update(target, s.q_per_s, &(&1 + s.q_per_s))
              |> Map.update(s.node_key, 0.0, &max(&1 - s.q_per_s, 0.0))

            new_moves = [move | moves]

            if length(new_moves) >= max_moves,
              do: {:halt, {new_moves, load}},
              else: {:cont, {new_moves, load}}
        end
      end)

    Enum.reverse(moves)
  end

  # The least-loaded backend other than the source whose POST-move load stays below the
  # source's current load (the improvement guard — else we'd just move the hotspot). nil
  # when no target improves balance (uniform fleet).
  defp best_target(sample, node_load, backend_keys) do
    from = sample.node_key
    from_load = Map.get(node_load, from, sample.q_per_s)

    backend_keys
    |> Enum.reject(&(&1 == from))
    |> Enum.map(&{&1, Map.get(node_load, &1, 0.0)})
    |> Enum.filter(fn {_t, t_load} -> t_load + sample.q_per_s < from_load end)
    # Least-loaded, tie-broken by node_key so equal-load targets resolve canonically (#18).
    |> Enum.min_by(fn {t, t_load} -> {t_load, t} end, fn -> nil end)
    |> case do
      nil -> nil
      {target, _load} -> target
    end
  end

  # The hot bar: an absolute floor if configured, else p99_multiple × fleet-p99.
  defp hot_threshold(floor, _mult, _rates) when is_number(floor) and floor > 0, do: floor / 1.0
  defp hot_threshold(_floor, mult, rates), do: mult * percentile(rates, 99)

  # Anti-flap (finding #9): a candidate must clear the bar in ≥ confirm DISTINCT windows on
  # its CURRENT serving node. Filtering to `candidate.node_key` stops an LB remap (where the
  # same shard_id is reported by both the old and new serving node) from double-counting one
  # hot window across nodes to reach `confirm`; de-duping by `sampled_at` counts windows, not
  # rows. (Recency is already bounded by the caller's read horizon; the latest-rate filter in
  # propose/4 ensures the shard is still hot right now.)
  defp confirmed_hot?(candidate, samples, threshold, confirm) do
    samples
    |> Enum.filter(fn s ->
      s.shard_id == candidate.shard_id and s.node_key == candidate.node_key and
        s.q_per_s >= threshold
    end)
    |> Enum.uniq_by(& &1.sampled_at)
    |> length()
    |> Kernel.>=(confirm)
  end

  # Newest sample per shard (current rate + serving node).
  defp latest_per_shard(samples) do
    samples
    |> Enum.sort_by(& &1.sampled_at, {:desc, DateTime})
    |> Enum.reduce(%{}, fn s, acc -> Map.put_new(acc, s.shard_id, s) end)
  end

  defp node_load(latest) do
    latest
    |> Map.values()
    |> Enum.reduce(%{}, fn s, acc -> Map.update(acc, s.node_key, s.q_per_s, &(&1 + s.q_per_s)) end)
  end

  defp cooling_shards(overrides, now, cooldown_ms) do
    for o <- overrides, cooling?(o, now, cooldown_ms), into: MapSet.new(), do: o.shard_id
  end

  # A nil/absent updated_at can't happen on a persisted row (timestamps always set it), but
  # guard it (#18): a nil would crash DateTime.diff and drop the whole tick. Treat it as
  # cooling (conservative — don't move a shard whose pin time is unknown).
  defp cooling?(o, now, cooldown_ms) do
    case Map.get(o, :updated_at) do
      nil -> true
      ts -> DateTime.diff(now, ts, :millisecond) < cooldown_ms
    end
  end

  defp percentile([], _), do: 0.0

  defp percentile(values, pct) do
    sorted = Enum.sort(values)
    idx = round(pct / 100 * (length(sorted) - 1))
    Enum.at(sorted, idx) || 0.0
  end

  defp cfg(key, default), do: Application.get_env(:fathom, key, default)
end
