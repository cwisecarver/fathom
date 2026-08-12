defmodule Fathom.Bench.Gate do
  @moduledoc """
  The multi-metric regression decision for the bench-then-commit gate.

  Fathom records several hot-path numbers per run (see `Fathom.Bench`), and a
  change can regress one while leaving the others flat — so the gate refuses a
  commit if **any** gated metric regresses by ≥ the block threshold, multi-metric
  rather than a single throughput scalar. The comparison is a pure function so it
  is unit-testable without running a bench.

  Direction is per metric: latency and memory are higher-is-worse; copy throughput
  is lower-is-worse. A metric that is `nil` in either run (e.g. the directory bench
  with no Postgres, or the always-`nil` `hrana_rt_us` placeholder) or whose parent
  value is zero is skipped, never silently treated as flat.
  """

  # Ordered so the report reads top-to-bottom in a stable order.
  @metrics [
    {:cold_open_p50_us, :higher_worse},
    {:cold_open_s3_p50_us, :higher_worse},
    {:warm_s3_shards_per_s, :lower_worse},
    # The failover RTO pair. Opt-in like the two S3 metrics above (nil ⇒ skipped), and ungated for
    # the same non-reason they were: nobody added them. Gating an opt-in metric costs nothing when
    # it is unset and compares it when it is not, which is exactly how `cold_open_s3_p50_us` is
    # already treated — there was no argument for splitting them.
    {:failover_cold_s3_p50_us, :higher_worse},
    {:failover_warm_s3_p50_us, :higher_worse},
    {:dir_resolve_p50_us, :higher_worse},
    # The LIVE directory path, gated from 2026-08-03 (#41.6). `dir_resolve_p50_us` above is
    # control-plane only (provision/fork/migrate) and stopped being the per-request path when the
    # Recorder landed — it was standing in for a cost it no longer represents. This is what
    # per-checkout directory work actually costs, and unlike resolve it scales with DENSITY.
    {:dir_recorder_flush_rows_per_s, :lower_worse},
    # Renamed from `copy_rows_per_s` on 2026-07-31, when the copy bench moved from a
    # three-column toy table to `Fathom.Keystone`. Rows are far wider now, so the two numbers
    # measure different work and the old series is not comparable. A NEW NAME is the honest way
    # to say that: entries before the switch keep `copy_rows_per_s` and are never compared
    # against entries after it, instead of one series with an unexplained cliff that reads like
    # a code regression forever. Any future harness change to this bench should rename again.
    {:copy_keystone_rows_per_s, :lower_worse},
    # Gated at 50%, not the default 20% — the same treatment `cold_open_p99_us` gets below, and
    # for the same reason: this is a fat-tailed number, not a precise one. It divides an
    # `:erlang.memory(:total)` delta across 200 idle coordinators WITHOUT collecting them first
    # (deliberately — see `Fathom.Bench.fanout/1`; it is the only thing that measures whether the
    # coordinators' `fullsweep_after: 0` actually reclaims retained heap). The cost is precision:
    # measured 2026-08-10, it moves ~30% in EITHER direction on changes that cannot affect
    # footprint, reproduced on demand by adding code that runs strictly after its own sample; it
    # has a 12× lifetime spread; and it can return NEGATIVE. At 20% it spent two sessions blocking
    # a change that was later proven to cost nothing, which is worse than not gating it at all.
    # 50% still catches a genuine density collapse. The tight per-shard bound is the pair below.
    {:fanout_kb_per_shard, :higher_worse, 50},
    # The steady-state floor — `fanout` with the coordinators collected, so open-path churn is out
    # of it. Gated at the default 20% because this is the reading meant to be believed: `fanout`
    # up with this flat is churn, both up is a real retained-heap regression. Early and on few
    # samples, but it read 3.64/3.65 across runs where `fanout` read 5.37 through 5.74. If that
    # stability does not hold, widen this — do not widen it pre-emptively, or the pair gates
    # nothing between them.
    {:fanout_gc_kb_per_shard, :higher_worse},
    # The SERVED regime, gated from 2026-08-03 (#41.2). `fanout_kb_per_shard` holds no
    # connections, so every per-connection resource decision — `:shard_cache_size_kb`'s up-to-2 MiB
    # page cache per held stream, the statement cache's sub-binary pin — was outside the gate.
    # `:binary` is a SEPARATE metric because the pin lands there and leaves the total nearly flat;
    # folding them would hide exactly the regression this is for.
    {:served_kb_per_shard, :higher_worse},
    {:served_binary_kb_per_shard, :higher_worse},
    # CONTENTION, gated from 2026-08-03 (#41.4). Every other gated metric is single-threaded, so
    # the concurrency-tuned machinery (write_concurrency + decentralized_counters on the ETS
    # counters, the Lru CA-tree, +SDio, the per-shard coordinator GenServer.call) had no gate at
    # all. `same_shard` is separate on purpose: it bounds ONE coordinator's head-of-line
    # throughput, which a spread measurement averages away.
    {:concurrent_checkout_per_s, :lower_worse},
    {:same_shard_checkout_per_s, :lower_worse},
    # The wire, gated from 2026-07-31. Before this the gate ran no Filo code at all, which is
    # how a 200x regression in row encoding stayed invisible: every other metric is SQLite,
    # storage, Postgres or BEAM memory. `hrana_rt_us` covers per-REQUEST overhead;
    # `wire_rows_per_s` covers per-CELL encoding over keystone rows (blobs included) and is
    # the one that would have caught it.
    {:hrana_rt_us, :higher_worse},
    {:wire_rows_per_s, :lower_worse},
    # The OTHER encoder, gated from 2026-08-03 (#41.7). `Filo.Value` has TWO encoders and the
    # gate only ran one: `wire_rows_per_s` drives `encode_json/1`, while `encode/1` — the
    # tagged-map builder whose own moduledoc says that layer "roughly doubled" per-cell cost — is
    # what `Cursor.entries/2` and `Protobuf.encode_value/1` reach, i.e. exactly when result sets
    # are large. Gated at the FUNCTION, not through the cursor HTTP transport: Filo.Client does
    # not do cursors, and `encode/1` is the code both transports share and the whole of the
    # per-cell risk. Transport framing stays ungated — a stated limit, not an oversight.
    {:wire_encode_rows_per_s, :lower_worse},
    # The write path and per-stream open, gated from 2026-08-02. Both were ADDED to the bench by
    # review #41.1/#41.3 and verified to discriminate — but were never added HERE, so for a day
    # they were measured, printed, written to perf_history.jsonl, and never compared. A metric the
    # gate does not read cannot block anything; #41 is about the gate's blind spots, and recording
    # a number is not closing one.
    #
    # Measured variance before gating, 9 same-host samples each (2026-08-02):
    #   hrana_open_rt_us  380–413, 8.7% spread — TIGHTER than hrana_rt_us (11.7%), already gated.
    #   flush_p50_us      8971–10456, 16.6% spread, but heavy-tailed rather than broadly noisy:
    #                     7 of 9 sit within 3.3% (8971–9263) and two excursions reach ~+16%.
    # Both therefore clear the project's 20% block threshold on observed range, flush_p50_us with
    # the least headroom of any gated metric. If it starts false-blocking, the fix is MORE TRIALS
    # to tighten the p50 (and a fresh series, per the same-topology rule) — not a looser threshold,
    # which would just re-blind the write path.
    {:hrana_open_rt_us, :higher_worse},
    {:flush_p50_us, :higher_worse},
    # THE TAIL, gated from 2026-08-03 (expert review #41.5). Every metric above is a p50, and
    # `delta/4` is a pure ratio — which AGENTS.md forbids in as many words ("Assert an absolute
    # floor, not only a ratio… The ratio holds while throughput collapses"). A change that leaves
    # p50 flat and doubles p99 — a blocking Storage call back in the coordinator mailbox, which
    # has happened twice — scored ~0% and sailed through. `docs/benchmark-plan.md` records that
    # moving the flush off-process was done for "recurring p99 checkout spikes", so the tail was
    # both the reason for the work and the one thing the gate never looked at.
    #
    # These reduce the SAME samples the p50s do (`cold_open_stats/1`, `hrana_rt_stats/1`), so they
    # cost nothing to collect and cannot disagree with their own p50 about which run they describe.
    #
    # BLOCK AT 50%, NOT THE GLOBAL 20% — and this corrects what this comment said when they were
    # added on 2026-08-03 ("if either starts false-blocking, raise the sample count, do not loosen
    # the threshold"). That was written before there was variance data. There now is, 13 same-host
    # runs each:
    #
    #   hrana_rt_p99_us   168–230   36.9% spread
    #   cold_open_p99_us  2233–3058 36.9% spread
    #
    # Both span well past 20%, and `hrana_rt_p99_us` duly blocked a commit at +22.73% with its own
    # p50 dead flat. A threshold below a metric's noise floor does not gate anything — it reports
    # noise as regression, and the predictable end of that is someone passing --skip habitually,
    # which is worse than not gating the tail at all.
    #
    # Raising the sample count was the other option and is the WRONG one here: p99 of 200 hrana
    # samples is the 2nd-worst, and p99 of 50 cold opens is essentially the max, so both would need
    # to grow a lot — and `@cold_open_samples` is shared with `cold_open_p50_us`, whose series is
    # 335 entries long. Changing it is a harness-topology change that invalidates that history, for
    # a metric that is not the problem. Per-metric thresholds keep the p50 series intact.
    #
    # 50% still catches what the tail is FOR: the failure this exists to see is p50 flat while p99
    # DOUBLES (+100%) — a blocking Storage call back in the coordinator mailbox, which has happened
    # twice. That clears 50% with room to spare.
    {:cold_open_p99_us, :higher_worse, 50},
    {:hrana_rt_p99_us, :higher_worse, 50}
  ]

  @doc "The gated metrics and their regression direction."
  def metrics, do: @metrics

  @doc """
  Compares `parent` and `new` metric maps (string- or atom-keyed, as decoded from
  `perf_history.jsonl`). `block`/`warn` are percent thresholds. Returns

      %{verdict: :ok | :warn | :block | :no_data, worst: float, deltas: [delta]}

  where each `delta` is `%{metric, parent, new, pct, skipped, reason}` and `pct` is
  the regression percent (positive = worse).
  """
  @spec compare(map(), map(), number(), number()) :: map()
  def compare(parent, new, block \\ 20, warn \\ 20) do
    deltas =
      Enum.map(@metrics, fn entry ->
        {metric, dir, metric_block} = normalize(entry, block)

        metric
        |> delta(dir, value(parent, metric), value(new, metric))
        |> Map.put(:block, metric_block)
      end)

    comparable = Enum.reject(deltas, & &1.skipped)
    worst = comparable |> Enum.map(& &1.pct) |> max_or(0.0)
    blocked = Enum.filter(comparable, &(&1.pct >= &1.block))

    verdict =
      cond do
        comparable == [] -> :no_data
        blocked != [] -> :block
        worst > warn -> :warn
        true -> :ok
      end

    %{verdict: verdict, worst: worst, deltas: deltas, blocked: Enum.map(blocked, & &1.metric)}
  end

  # A metric may carry its OWN block threshold. Not a loophole — a threshold has to sit above the
  # metric's measured noise floor or it reports noise as regression, and 20% was chosen for
  # single-threaded p50s. See the tail entries in @metrics for the measured basis.
  defp normalize({metric, dir}, default_block), do: {metric, dir, default_block}
  defp normalize({metric, dir, block}, _default_block), do: {metric, dir, block}

  @doc "A human-readable multi-line report of a `compare/3` result."
  @spec format(map(), String.t(), number(), number()) :: String.t()
  def format(result, parent_sha, block \\ 20, warn \\ 20) do
    header = "=== bench gate vs parent #{parent_sha} ==="

    rows =
      Enum.map(result.deltas, fn d ->
        name = String.pad_trailing(to_string(d.metric), 20)

        if d.skipped do
          "  #{name} skipped (#{d.reason})"
        else
          own = Map.get(d, :block, block)
          note = if own != block, do: " (block >=#{own}%)", else: ""

          "  #{name} #{fmt(d.parent)} -> #{fmt(d.new)}   #{signed(d.pct)}%   " <>
            "#{tag(d.pct, own, warn)}#{note}"
        end
      end)

    verdict =
      "  verdict: #{result.verdict |> to_string() |> String.upcase()} " <>
        "(worst #{signed(result.worst)}%, block >=#{block}%, warn >#{warn}%)"

    Enum.join([header | rows] ++ [verdict], "\n")
  end

  # --- internals -----------------------------------------------------------

  defp value(map, metric) do
    Map.get(map, metric) || Map.get(map, Atom.to_string(metric))
  end

  defp delta(metric, _dir, parent, new) when is_nil(parent) or is_nil(new) do
    %{metric: metric, parent: parent, new: new, pct: 0.0, skipped: true, reason: "nil"}
  end

  defp delta(metric, _dir, parent, new) when parent == 0 do
    %{metric: metric, parent: parent, new: new, pct: 0.0, skipped: true, reason: "parent zero"}
  end

  defp delta(metric, dir, parent, new) do
    pct =
      case dir do
        :higher_worse -> (new - parent) / parent * 100
        :lower_worse -> (parent - new) / parent * 100
      end

    %{metric: metric, parent: parent, new: new, pct: Float.round(pct / 1, 2), skipped: false}
  end

  defp max_or([], default), do: default
  defp max_or(list, _default), do: Enum.max(list)

  defp tag(pct, block, _warn) when pct >= block, do: "BLOCK"
  defp tag(pct, _block, warn) when pct > warn, do: "warn"
  defp tag(_pct, _block, _warn), do: "ok"

  defp signed(n) when n >= 0, do: "+#{:erlang.float_to_binary(n / 1, decimals: 2)}"
  defp signed(n), do: :erlang.float_to_binary(n / 1, decimals: 2)

  defp fmt(n) when is_float(n), do: :erlang.float_to_binary(n, decimals: 2)
  defp fmt(n), do: to_string(n)
end
