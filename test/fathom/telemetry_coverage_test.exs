defmodule Fathom.TelemetryCoverageTest do
  @moduledoc """
  Every `:telemetry` event fathom emits is either exported by `Fathom.Telemetry.metrics/0` or
  listed below with a reason (2026-08-06).

  ## Why this test exists

  Emitting an event is the cheap half. Nothing fails when the other half — a `Telemetry.Metrics`
  definition, so the event reaches Prometheus at all — is missing, so the gap is invisible by
  construction: the code looks instrumented, the log line is there, and the metric simply does not
  exist. A sweep on 2026-08-06 found **13** events in that state, including:

    * `[:fathom, :migrator, :fork_fallback]` — a tenant serving with NO schema. Its checkout
      deliberately succeeds, so it moves no 5xx rate anywhere and nothing else would ever surface
      it. Emitted since 2026-07-31 specifically so it would be "alertable"; never was.
    * `[:fathom, :shard, :forked]` — acked writes quarantined off the lineage, recovery manual.
    * `[:fathom, :shard, :write_fenced]` — writes refused fleet-side while reads keep succeeding.
    * `[:fathom, :migrator, :template_drift]` — whose own docstring says it emits "so a post-revert
      wedge is alertable, not discovered at the next fleet-wide `makemigrations`".

  Three of those were written with an explicit intention to alert. Intention is not wiring, and
  there was no check standing between the two.

  ## What it is and is not

  A **backstop, not a proof.** It reads source text, so it only sees `:telemetry.execute` calls
  whose event name is a literal list. An event assembled from a variable or module attribute is
  invisible to it (`dynamic_event_sites/0` counts those so they cannot silently drop to zero, which
  would mean the regex stopped matching rather than the code stopped emitting).

  Its real job is to force a DECISION. Adding an event now either exports it or writes down why
  not — both fine, silence is not.
  """
  use ExUnit.Case, async: true

  # Events deliberately NOT exported, each with the reason. An entry here is a decision on the
  # record, not a suppression: it should say what an operator would do with the metric, and why
  # that is already covered or not worth a series.
  @not_exported %{}

  # `:telemetry.execute([...], ...)` where the event name is a literal list of atoms.
  @execute_re ~r/:telemetry\.execute\(\s*\[([^\]]+)\]/

  defp lib_sources do
    Path.wildcard("lib/**/*.ex")
  end

  # Every literal event name emitted anywhere in lib/, as a list of atoms.
  defp emitted_events do
    for path <- lib_sources(),
        [_, inner] <- Regex.scan(@execute_re, File.read!(path)),
        parts = inner |> String.split(",") |> Enum.map(&String.trim/1),
        Enum.all?(parts, &String.starts_with?(&1, ":")),
        into: MapSet.new() do
      Enum.map(parts, fn ":" <> atom -> String.to_atom(atom) end)
    end
  end

  # Call sites whose event name is NOT a literal (built from a variable). Invisible to this test by
  # construction — counted so the count itself is pinned.
  defp dynamic_event_sites do
    for path <- lib_sources(),
        [_, inner] <- Regex.scan(@execute_re, File.read!(path)),
        parts = inner |> String.split(",") |> Enum.map(&String.trim/1),
        not Enum.all?(parts, &String.starts_with?(&1, ":")) do
      {path, inner}
    end
  end

  test "every emitted telemetry event is exported by metrics/0 (or listed with a reason)" do
    exported =
      Fathom.Telemetry.metrics()
      |> Enum.map(& &1.event_name)
      |> MapSet.new()

    unexported =
      emitted_events()
      |> Enum.reject(&(&1 in exported or Map.has_key?(@not_exported, &1)))
      |> Enum.sort()

    assert unexported == [],
           """
           These events are emitted by lib/ but reach no reporter — no Prometheus series, so no
           alert rule can ever reference them:

           #{Enum.map_join(unexported, "\n", &"  #{inspect(&1)}")}

           Either add a metric in Fathom.Telemetry.metrics/0, or add the event to @not_exported in
           this file with the reason. Do not delete this assertion: emitting into the void is the
           exact failure it was written for (13 of them on 2026-08-06).

           Cardinality rule for the metric: tag by BOUNDED sets (outcome / kind / reason) and never
           by shard_id — at a million tenants that label is cardinality death, which is why the
           shard_id rides event metadata for the log line instead.
           """
  end

  # The sanity check on the check. If this drops to zero, the regex stopped matching rather than
  # the code stopping emitting — and then the test above passes while seeing nothing.
  test "the dynamic-event sites are still there (the regex still matches)" do
    sites = dynamic_event_sites()

    assert length(sites) >= 2,
           "expected the known variable-named execute sites (Shards.start_held_retry/5, " <>
             "HandoffJob's retry/stop pair); found #{inspect(sites)} — the regex probably " <>
             "stopped matching, which would make the coverage test above vacuous"
  end

  test "the scan actually reads a meaningful amount of source" do
    # Guards the same vacuity from the other side: a bad wildcard yielding [] would make every
    # assertion above trivially true.
    assert length(lib_sources()) > 50
    assert MapSet.size(emitted_events()) > 25
  end

  # The other end of the chain. A rule referencing a series nothing produces is not an error
  # anywhere — Prometheus evaluates it forever against no data and never fires, which looks
  # identical to "the condition never happened". `FathomTenantBornEmpty` was written the same day
  # as its metric; a typo in either would have shipped silently.
  test "every fathom_* series an alert rule references is producible by metrics/0" do
    rules = File.read!("deploy/observability/alert-rules.yml")

    # A Prometheus series name is the metric name joined by underscores. Distributions render as
    # <name>_bucket / _sum / _count, so accept those suffixes too.
    producible =
      for m <- Fathom.Telemetry.metrics(), base = Enum.join(m.name, "_"), reduce: MapSet.new() do
        acc ->
          acc
          |> MapSet.put(base)
          |> MapSet.union(MapSet.new(for s <- ~w(bucket sum count), do: "#{base}_#{s}"))
      end

    referenced =
      ~r/\bfathom_[a-z0-9_]+/
      |> Regex.scan(rules)
      |> List.flatten()
      |> MapSet.new()

    dangling = referenced |> MapSet.difference(producible) |> Enum.sort()

    assert dangling == [],
           """
           These series are referenced by deploy/observability/alert-rules.yml but no metric in
           Fathom.Telemetry.metrics/0 can produce them:

           #{Enum.map_join(dangling, "\n", &"  #{&1}")}

           A rule on a series that is never produced does not error — it evaluates against no data
           forever and never fires, which is indistinguishable from the condition never happening.
           """
  end

  test "no exported metric is tagged by shard_id" do
    offenders =
      Fathom.Telemetry.metrics()
      |> Enum.filter(&(:shard_id in &1.tags))
      |> Enum.map(& &1.name)

    assert offenders == [],
           "shard_id is unbounded as a Prometheus label (one series per tenant, at a million " <>
             "tenants) — the ShardLoad read API is how per-shard numbers are meant to be read. " <>
             "Offending metrics: #{inspect(offenders)}"
  end
end
