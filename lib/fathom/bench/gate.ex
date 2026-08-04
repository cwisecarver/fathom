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
    {:fanout_kb_per_shard, :higher_worse},
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
    # A tail is noisier than a median by construction — if either starts false-blocking, raise the
    # sample count to tighten it, do not loosen the threshold.
    {:cold_open_p99_us, :higher_worse},
    {:hrana_rt_p99_us, :higher_worse}
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
      Enum.map(@metrics, fn {metric, dir} ->
        delta(metric, dir, value(parent, metric), value(new, metric))
      end)

    comparable = Enum.reject(deltas, & &1.skipped)
    worst = comparable |> Enum.map(& &1.pct) |> max_or(0.0)

    verdict =
      cond do
        comparable == [] -> :no_data
        worst >= block -> :block
        worst > warn -> :warn
        true -> :ok
      end

    %{verdict: verdict, worst: worst, deltas: deltas}
  end

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
          "  #{name} #{fmt(d.parent)} -> #{fmt(d.new)}   #{signed(d.pct)}%   #{tag(d.pct, block, warn)}"
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
