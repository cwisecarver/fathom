defmodule Fathom.ConfigurationDocTest do
  # Review #17: deploying fathom means setting ~30+ env knobs correctly, and every undocumented
  # knob is a chance to deploy something unsafe. docs/configuration.md is the reference (name,
  # default, safety consequence). This test pins that it stays COMPLETE: every env var read in
  # config/runtime.exs must have a row in the doc. Adding a new System.get_env knob without
  # documenting it fails here — the doc can't silently drift behind the code.
  use ExUnit.Case, async: true

  @runtime "config/runtime.exs"
  @doc_path "docs/configuration.md"

  # Env vars that live in the doc but are read outside runtime.exs (compile-time in prod.exs), so
  # they won't appear in the runtime.exs scan — documented, just not required-by-scan.
  # (No exclusions needed for the scan itself; commented lines are skipped below.)

  test "every env var read in runtime.exs is documented in docs/configuration.md" do
    runtime = File.read!(@runtime)
    doc = File.read!(@doc_path)

    env_vars =
      runtime
      |> String.split("\n")
      # Skip comment lines — the phx.gen scaffolding (SSL/Mailgun examples) is commented out and
      # is not an active fathom knob.
      |> Enum.reject(fn line -> String.trim_leading(line) |> String.starts_with?("#") end)
      # Only lines that actually read an env var. `env_int.("X")` is included deliberately: the
      # scan used to match `System.get_env` alone, so the typed-helper form introduced by expert
      # review 2026-07-24 #14 read three real knobs (S3_POOL_SIZE / S3_POOL_COUNT /
      # S3_CONN_MAX_IDLE_MS) that this guard could not see — undocumented AND invisible. Any future
      # helper of this shape must be added here too, or it reopens the same blind spot.
      |> Enum.filter(&(&1 =~ ~r/(System\.(get_env|fetch_env!)|env_int\.\()/))
      # Pull the UPPER_SNAKE literal(s) off each such line (the "" default / lowercase values
      # never match). Handles both System.get_env("X") and the "X" |> System.get_env("") form.
      |> Enum.flat_map(fn line ->
        Regex.scan(~r/"([A-Z][A-Z0-9_]+)"/, line, capture: :all_but_first)
      end)
      |> List.flatten()
      |> Enum.uniq()
      |> Enum.sort()

    # Sanity: the scan actually found the knobs (guards against a regex/format change silently
    # making this test vacuously pass).
    assert length(env_vars) > 40, "expected 40+ env vars, found #{length(env_vars)}: unexpected"
    assert "SHARD_BASE_DOMAIN" in env_vars, "the piped System.get_env form wasn't scanned"
    assert "DATABASE_URL" in env_vars

    missing = Enum.reject(env_vars, &String.contains?(doc, &1))

    assert missing == [],
           "these env vars are read in #{@runtime} but not documented in #{@doc_path}: " <>
             Enum.join(missing, ", ")
  end
end
