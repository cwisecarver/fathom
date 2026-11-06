defmodule Fathom.ConfigurationDocTest do
  # Review #17: deploying fathom means setting ~30+ env knobs correctly, and every undocumented
  # knob is a chance to deploy something unsafe. docs/configuration.md is the reference (name,
  # default, safety consequence). This test pins that it stays COMPLETE: every env var read in
  # config/runtime.exs must have a row in the doc. Adding a new System.get_env knob without
  # documenting it fails here — the doc can't silently drift behind the code.
  #
  # The reverse direction (expert review 2026-11-06 #2) pins that every knob ROW in the doc is
  # still read by runtime.exs, so a retired or never-wired knob can't stay documented as if it
  # worked. It also covers the typed-integer boot failure (#8).
  #
  # async: false — the integer test mutates the process environment.
  use ExUnit.Case, async: false

  @runtime "config/runtime.exs"
  @doc_path "docs/configuration.md"

  # Names that have a `| `NAME` |` row in the doc but are deliberately NOT read by runtime.exs.
  # Each must say why; an entry that stops being needed fails the reverse test.
  @doc_rows_not_in_runtime %{
    # Read at COMPILE time in config/prod.exs (a build-time env), so runtime.exs never sees it.
    "WEB_INSECURE_LOCAL" => "compile-time, config/prod.exs",
    # Read by `mix fathom.bench` (lib/mix/tasks/fathom.bench.ex), not by the release.
    "FATHOM_BENCH_LOCK" => "bench task, not a release knob"
    # The retired WARM_* env vars have no row (they appear only in prose under the A2 section),
    # so they need no entry; if one is ever given a row, list it here with its reason.
  }

  # Every `| `NAME` |` knob row in the doc (first cell is exactly one UPPER_SNAKE name).
  defp doc_knob_rows(doc) do
    ~r/^\| `([A-Z][A-Z0-9_]+)` \|/m
    |> Regex.scan(doc, capture: :all_but_first)
    |> List.flatten()
    |> Enum.uniq()
  end

  # runtime.exs without comment lines (the phx.gen scaffolding and our own prose mention names).
  defp runtime_code do
    @runtime
    |> File.read!()
    |> String.split("\n")
    |> Enum.reject(&(&1 |> String.trim_leading() |> String.starts_with?("#")))
    |> Enum.join("\n")
  end

  test "every env var read in runtime.exs is documented in docs/configuration.md" do
    doc = File.read!(@doc_path)

    env_vars =
      runtime_code()
      |> String.split("\n")
      # Only lines that actually read an env var. The typed-helper forms are included deliberately:
      # the scan used to match `System.get_env` alone, so the helper introduced by expert review
      # 2026-07-24 #14 read three real knobs (S3_POOL_SIZE / S3_POOL_COUNT / S3_CONN_MAX_IDLE_MS)
      # that this guard could not see — undocumented AND invisible.
      #
      # THE WARNING BELOW WAS ALREADY STALE WHEN IT WAS WRITTEN, which is the lesson: only
      # `env_int` was added, and `env_nonneg_int` — the replication byte caps — stayed invisible for
      # as long as it existed. `env_bool` (2026-08-25) is the third. Keep this alternation in sync
      # with every helper in config/runtime.exs that reads the environment BY NAME, or that knob
      # silently leaves the guard's sight. (`parse_int!` is not one: it takes the already-read
      # value, so its call sites are on the same line as the `System.get_env` that names the knob.)
      |> Enum.filter(
        &(&1 =~ ~r/(System\.(get_env|fetch_env!)|env_int\.\(|env_nonneg_int\.\(|env_bool\.\()/)
      )
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

    # A knob ROW, not any substring: a name that only appears inside another row's prose (the old
    # LAPSE_REVALIDATE_JITTER_MS mistake, expert review 2026-11-06 #2) is not documented.
    rows = doc_knob_rows(doc)
    missing = Enum.reject(env_vars, &(&1 in rows))

    assert missing == [],
           "these env vars are read in #{@runtime} but have no `| `NAME` |` row in #{@doc_path}: " <>
             Enum.join(missing, ", ")
  end

  test "every knob row in docs/configuration.md is read by runtime.exs or explicitly allowlisted" do
    code = runtime_code()
    rows = doc_knob_rows(File.read!(@doc_path))

    assert length(rows) > 100, "expected 100+ knob rows, found #{length(rows)}: regex broke?"

    read? = fn name -> String.contains?(code, ~s("#{name}")) end

    stale = Enum.reject(rows, &(Map.has_key?(@doc_rows_not_in_runtime, &1) or read?.(&1)))

    assert stale == [],
           "these knob rows in #{@doc_path} are not read by #{@runtime} (retired or never wired: " <>
             "fix the doc, or allowlist with a reason): " <> Enum.join(stale, ", ")

    unneeded =
      for {name, _why} <- @doc_rows_not_in_runtime, name not in rows or read?.(name), do: name

    assert unneeded == [], "stale allowlist entries: " <> Enum.join(unneeded, ", ")
  end

  test "LAPSE_REVALIDATE_JITTER_MS is documented as the app-env key, not an env var" do
    # Expert review 2026-11-06 #2: nothing reads this as an env var; the override is the app-env
    # key :lapse_revalidate_jitter_ms. The doc must not claim an env var override.
    doc = File.read!(@doc_path)
    refute doc =~ "LAPSE_REVALIDATE_JITTER_MS"
    assert doc =~ ":lapse_revalidate_jitter_ms"
  end

  test "a malformed integer env value fails the boot naming the variable" do
    # Expert review 2026-11-06 #8: `{n, _}` used to truncate "12abc" to 12 and a bare
    # String.to_integer raised an unnamed ArgumentError. Run runtime.exs as :prod with a minimal
    # valid env plus one bad value.
    base = %{
      "DATABASE_URL" => "ecto://u:p@localhost/db",
      "SECRET_KEY_BASE" => String.duplicate("a", 64),
      "HRANA_AUTH" => "disabled"
    }

    # One name per read form: raw site (idle, flush, port), env_int, env_nonneg_int.
    for name <- ~w(SHARD_IDLE_MS SHARD_FLUSH_INTERVAL_MS PORT HRANA_BACKLOG QUERY_MAX_ROWS),
        bad <- ["12abc", "abc", "1.5"] do
      assert {:error, msg} = read_prod_config(Map.put(base, name, bad))
      assert msg =~ "#{name} must be an integer, got: #{inspect(bad)}"
    end

    # A valid value passes through unchanged.
    assert {:ok, cfg} = read_prod_config(Map.put(base, "SHARD_IDLE_MS", "12345"))
    assert get_in(cfg, [:fathom, :shard_idle_ms]) == 12_345
  end

  # Reads config/runtime.exs as :prod with `env` added to the process environment (restored after).
  defp read_prod_config(env) do
    saved = Map.new(env, fn {k, _} -> {k, System.get_env(k)} end)
    Enum.each(env, fn {k, v} -> System.put_env(k, v) end)

    try do
      {:ok, Config.Reader.read!(@runtime, env: :prod)}
    rescue
      e in RuntimeError -> {:error, Exception.message(e)}
    after
      Enum.each(saved, fn
        {k, nil} -> System.delete_env(k)
        {k, v} -> System.put_env(k, v)
      end)
    end
  end
end
