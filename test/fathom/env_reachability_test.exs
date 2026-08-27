defmodule Fathom.EnvReachabilityTest do
  @moduledoc """
  The mirror of `Fathom.ConfigurationDocTest`, and the half that was missing.

  That test asks "is every env var the code READS documented?" — it walks `config/runtime.exs` and
  requires a row in `docs/configuration.md`. This one asks the opposite and more damaging question:
  **is every env var we ADVERTISE actually read by anything?**

  ## Why this exists

  Four times in one session (2026-08-25/26) a feature shipped correct and unreachable, because the
  config key that gates it had no environment wiring — settable only from a config file, which a
  release cannot edit:

    * `:template_shard_id` — the migration engine's entry point. `mix fathom.snapshot` printed
      "set TEMPLATE_SHARD_ID", a variable nothing read, so capture could not be turned on at all.
    * `:replication_lineage_wire` — review #12's fix. Correct, tested, and undeployable; the
      documented second step of its own rollout had no mechanism.
    * `:restore_drill_sample` — the restore drill, and therefore the ONLY thing that populates
      `stamp_drift` in `Migrator.status/0`. `docs/configuration.md` referred to
      `RESTORE_DRILL_SAMPLE` by name inside another knob's row, as if it existed.
    * `:shard_max_bytes` / `:shard_max_page_count` — found BY this test on its first run. The cap
      that stops a shard acking writes past S3's 5 GiB single-PUT ceiling, where the consequence is
      permanent and has no operator remedy. `connection.ex` said "Set `SHARD_MAX_BYTES=0` … to opt
      out"; there was no such variable.

  Every one of those is silent. The feature looks configured, the code is right, and nothing fails.

  ## Why it is scoped to ADVERTISED names, not to every key

  The tempting rule — "every `Application.get_env(:fathom, …)` key must be env-wired" — was measured
  before being written: **180 keys are read in `lib/` and 85 are wired**, so it would demand ~98
  exemptions. A list that long is not maintained, and an unmaintained exemption list is how
  `env_nonneg_int` stayed invisible to the sibling test for its whole existence. Most of those 98
  are internal tuning constants with sane compiled defaults that no deployment should be setting.

  So the rule is narrower and has no exemption list at all: **a name we tell a human to set must be
  a name something reads.** That is mechanically exact, and it is the property that was actually
  violated all four times.

  ## What counts as "read"

  Anywhere in the repo's own code: `config/runtime.exs` (the usual home), any other `config/*.exs`
  (`WEB_INSECURE_LOCAL` is deliberately compile-time in `prod.exs`), or `lib/` directly
  (`FATHOM_BENCH_LOCK` is read by the Mix task that uses it). The claim is reachability, not a
  particular file.
  """
  use ExUnit.Case, async: true

  @doc_path "docs/configuration.md"

  # Names the repo actually READS, from anywhere it may legitimately read one.
  #
  # A SUBSTRING SEARCH OVER THE SOURCE IS NOT ENOUGH, and the first draft of this test was exactly
  # that — which made it vacuous, because the defect it was written for advertises the name in a
  # COMMENT. `connection.ex` says "Set `SHARD_MAX_BYTES=0` … to opt out", so the string is present
  # in `lib/` whether or not anything reads it, and the probe (delete the runtime.exs wiring, expect
  # a failure) passed. Caught only by running that probe; a guard that cannot fail is worse than no
  # guard, because it reads as coverage.
  #
  # So: match the READ FORMS. Line-based rather than expression-based, mirroring
  # `Fathom.ConfigurationDocTest`, so the piped `"X" |> System.get_env("")` shape is caught too.
  # Any new helper in `config/runtime.exs` must be added to this alternation — the sibling test
  # carries the same warning, and the same drift happened there twice.
  @read_forms ~r/(System\.(get_env|fetch_env!?)|env_int\.\(|env_nonneg_int\.\(|env_bool\.\()/

  defp read_names do
    (Path.wildcard("config/*.exs") ++ Path.wildcard("lib/**/*.ex"))
    |> Enum.flat_map(fn path ->
      path
      |> File.read!()
      |> String.split("\n")
      |> Enum.reject(&String.starts_with?(String.trim_leading(&1), "#"))
      |> Enum.filter(&(&1 =~ @read_forms))
      |> Enum.flat_map(&Regex.scan(~r/"([A-Z][A-Z0-9_]{2,})"/, &1, capture: :all_but_first))
    end)
    |> List.flatten()
    |> MapSet.new()
  end

  # The operator reference's table rows: `| `NAME` | default | … |`. Only the first column, so a
  # variable merely MENTIONED in prose or inside another row's description is not treated as a
  # promise — the promise is having a row of your own.
  defp advertised_in_doc do
    @doc_path
    |> File.read!()
    |> String.split("\n")
    |> Enum.flat_map(fn line ->
      case Regex.run(~r/^\|\s*`([A-Z][A-Z0-9_]{2,})`\s*\|/, line) do
        [_, name] -> [name]
        nil -> []
      end
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  # Names a message in `lib/` tells a human to set or export. This is the half that would have
  # caught TEMPLATE_SHARD_ID, whose only advertisement was a mix task's error message.
  defp advertised_in_code do
    "lib/**/*.ex"
    |> Path.wildcard()
    |> Enum.flat_map(fn path ->
      Regex.scan(~r/\b(?:set|Set|export)\s+`?([A-Z][A-Z0-9_]{3,})`?\b/, File.read!(path),
        capture: :all_but_first
      )
    end)
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.sort()
  end

  test "every env var documented as a knob is actually read by something" do
    advertised = advertised_in_doc()
    read = read_names()

    # Sanity: the scan found the table. A regex or format change that silently matched nothing
    # would make this test pass while checking zero things — the failure mode the sibling test
    # guards against the same way.
    assert length(advertised) > 80,
           "expected 80+ documented env vars, found #{length(advertised)} — the table format " <>
             "probably changed and this scan is now vacuous"

    assert "DATABASE_URL" in advertised
    assert "SHARD_STORAGE" in advertised

    unread = Enum.reject(advertised, &MapSet.member?(read, &1))

    assert unread == [],
           "these env vars have a row in #{@doc_path} but NOTHING in config/ or lib/ reads them, " <>
             "so an operator who sets them changes nothing and is never told: " <>
             "#{Enum.join(unread, ", ")}"
  end

  test "every env var a message tells a human to set is actually read by something" do
    advertised = advertised_in_code()
    read = read_names()

    # No floor assertion here on purpose: unlike the doc table, a repo legitimately might not tell
    # anyone to set anything from code, so "found none" is a valid state rather than a broken scan.
    unread = Enum.reject(advertised, &MapSet.member?(read, &1))

    assert unread == [],
           "a message in lib/ tells an operator to set these, but nothing reads them — the " <>
             "instruction cannot work: #{Enum.join(unread, ", ")}"
  end

  # The reverse direction, deliberately NARROW (expert review 2026-08-26 #15).
  #
  # That finding asked for "a test asserting that a config key something reads is also an
  # advertised env var" — the general form of this file's rule. The moduledoc above already
  # records why the general form was measured and REJECTED: 180 keys are read in `lib/` and 85 are
  # wired, so it would ship with ~98 exemptions, and an exemption list that long is not maintained.
  #
  # So this is the maintainable half: an explicit, short list of keys whose ABSENCE from the
  # environment is a safety problem rather than a tuning inconvenience. Each entry carries the
  # consequence of it being unreachable. The list is meant to stay small; a key belongs here only
  # if an operator being unable to set it on a running release is itself an incident.
  @must_be_env_reachable %{
    "query_timeout_ms" =>
      "the ONLY thing that makes a lock wait cancellable — unset, ten blocked statements park " <>
        "the whole dirty-IO pool node-wide (#15)",
    "query_max_rows" =>
      "the row cap; uncapped, one SELECT * on a large tenant is a node-level memory event (#15)",
    "max_checkouts_per_shard" =>
      "the per-tenant checkout/fd blast-radius cap that HRANA_STREAM_IDLE_MS's own docs cite (#15)",
    "shard_max_bytes" =>
      "stops a shard acking writes past S3's single-PUT ceiling, where the damage is permanent",
    "max_open_shards" => "per-node admission control; unset, a novel-shard burst has no ceiling"
  }

  test "every safety-critical config key is reachable from the environment" do
    wired = read_names()

    unreachable =
      Enum.reject(@must_be_env_reachable, fn {key, _why} ->
        MapSet.member?(wired, String.upcase(key))
      end)

    assert unreachable == [],
           """
           These config keys gate a SAFETY bound and have no environment wiring, so an operator
           running a release cannot turn them on without a code change:

           #{Enum.map_join(unreachable, "\n", fn {k, why} -> "  * :#{k} — #{why}" end)}

           Wire them in config/runtime.exs (and give them a row in #{@doc_path}, which the sibling
           test then enforces).

           This list is deliberately short. The GENERAL rule — every read key must be env-wired —
           was measured and rejected: 180 keys are read in lib/ and 85 are wired, so it would need
           ~98 exemptions. See this module's moduledoc.
           """
  end
end
