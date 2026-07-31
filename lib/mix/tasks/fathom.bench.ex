defmodule Mix.Tasks.Fathom.Bench do
  @moduledoc """
  Run fathom's hot-path benchmarks and emit one perf-history JSON line.

      mix fathom.bench [options]

  Measures the hot paths (see `docs/benchmark-plan.md`) and prints a human table to
  stderr plus one complete JSON line to stdout. The JSON line is the unit the
  bench-then-commit gate (`scripts/commit_with_bench.sh`, B3) compares against the
  parent commit's entry in `scripts/perf_history.jsonl`.

  ## Options

    * `--only m1,m2`     — subset of `cold_open,cold_open_s3,dir_resolve,copy,fanout` (default all)
    * `--trials N`       — median trials for throughput/memory benches (default 5)
    * `--cold-open-s3-samples N` — samples for the S3 cold-open bench (opt-in; needs
      `FATHOM_S3_TEST_*` env, else skipped)
    * `--append PATH`    — append the JSON line to PATH (e.g. scripts/perf_history.jsonl)
    * `--commit` / `--commit-full` / `--branch` / `--dirty` / `--host` / `--log`
                         — git/run context (defaulted from `git` so it runs standalone)
    * `--cold-open-samples` / `--resolve-samples` / `--copy-rows` / `--fanout-n`

  Run prod-compiled against a clean data dir for a real number:

      MIX_ENV=prod mix fathom.bench --append scripts/perf_history.jsonl

  `--only cold_open,copy,fanout` skips the directory bench, so it runs with no
  Postgres.

  ## Benchmark lock

  Benchmarks are only meaningful in isolation — a second run (this repo or a sibling
  sharing the host) contends for CPU/disk and skews both. So the task takes a host-wide
  lock file for the duration of the run: it refuses to start if the lock already exists
  (another run is in progress, or a crashed run left it behind — `rm` it), and creates +
  removes it otherwise (removed even if the bench fails). The create is atomic (`O_EXCL`),
  so two simultaneous starts can't both win.

  The path defaults to `/tmp/fathom_bench.lock` and is overridable with the
  `FATHOM_BENCH_LOCK` environment variable. Point several projects at the *same* path to
  interlock their benchmarks across a shared host:

      FATHOM_BENCH_LOCK=/tmp/shared_bench.lock mix fathom.bench
  """
  @shortdoc "Run fathom hot-path benchmarks, emit a perf-history JSON line"

  use Mix.Task

  # Host-wide benchmark lock — one benchmark at a time across everything sharing this host,
  # so no run measures under another run's load. Env-overridable (rather than hardcoded to
  # one project's name) so co-tenant projects can agree on a shared path without either
  # repo carrying the other's name.
  @default_lock_file "/tmp/fathom_bench.lock"

  defp lock_file, do: System.get_env("FATHOM_BENCH_LOCK", @default_lock_file)

  # Load config + compile without starting the app — Fathom.Bench starts the
  # minimal subset it needs itself (no Oban, no Hrana port, no endpoint).
  @requirements ["app.config"]

  @all_metrics [:cold_open, :cold_open_s3, :warm_s3, :failover_rto, :dir_resolve, :copy, :fanout]

  @switches [
    only: :string,
    trials: :integer,
    append: :string,
    commit: :string,
    commit_full: :string,
    branch: :string,
    dirty: :boolean,
    host: :string,
    log: :string,
    cold_open_samples: :integer,
    cold_open_s3_samples: :integer,
    failover_samples: :integer,
    warm_shards: :integer,
    warm_size_kb: :integer,
    resolve_samples: :integer,
    copy_rows: :integer,
    fanout_n: :integer
  ]

  @impl true
  def run(argv) do
    # Keep the bench log readable: the directory bench alone issues hundreds of
    # Ecto queries, each logged at :debug. Warnings (e.g. a skipped bench) survive.
    Logger.configure(level: :warning)
    {opts, _, _} = OptionParser.parse(argv, strict: @switches)

    with_lock(lock_file(), fn -> do_run(opts) end)
  end

  # Runs `fun` while holding the host-wide benchmark lock at `path`, refusing (via `Mix.raise`)
  # if the lock already exists so no benchmark measures under another's load. Creates the lock
  # atomically (`O_EXCL`) and removes it when `fun` returns OR raises; a pre-existing lock (not
  # ours) is left in place. Returns `fun`'s result. Public only so the lock logic is testable.
  @doc false
  @spec with_lock(Path.t(), (-> result)) :: result when result: var
  def with_lock(path, fun) do
    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        _ = IO.write(io, "fathom.bench pid #{System.pid()} #{DateTime.utc_now()}\n")
        File.close(io)

        try do
          fun.()
        after
          File.rm(path)
        end

      {:error, :eexist} ->
        Mix.raise("""
        benchmark lock present: #{path}
        A benchmark is already running (or a previous run crashed and left the lock behind).
        Refusing to run — a concurrent benchmark would skew both. If nothing is running, remove it:
            rm #{path}
        """)

      {:error, reason} ->
        Mix.raise("could not create benchmark lock #{path}: #{:file.format_error(reason)}")
    end
  end

  defp do_run(opts) do
    bench_opts =
      []
      |> put_opt(opts, :only, &parse_only/1)
      |> put_opt(opts, :trials)
      |> put_opt(opts, :cold_open_samples)
      |> put_opt(opts, :cold_open_s3_samples)
      |> put_opt(opts, :failover_samples)
      |> put_opt(opts, :warm_shards)
      |> put_opt(opts, :warm_size_kb)
      |> put_opt(opts, :resolve_samples)
      |> put_opt(opts, :copy_rows)
      |> put_opt(opts, :fanout_n)

    metrics = Fathom.Bench.all(bench_opts)
    line = build_line(metrics, opts, bench_opts)

    print_table(metrics, line)
    json = Jason.encode!(line)
    # The JSON line is the task's stdout contract (the perf-history record);
    # the human table goes to stderr. Both routed through Mix.shell().
    Mix.shell().info(json)

    case Keyword.get(opts, :append) do
      nil -> :ok
      path -> File.write!(path, json <> "\n", [:append])
    end
  end

  defp build_line(metrics, opts, bench_opts) do
    %{
      ts: DateTime.utc_now() |> DateTime.to_iso8601(),
      commit: Keyword.get(opts, :commit) || git(["rev-parse", "--short", "HEAD"], "unknown"),
      commit_full: Keyword.get(opts, :commit_full) || git(["rev-parse", "HEAD"], "unknown"),
      branch: Keyword.get(opts, :branch) || git(["rev-parse", "--abbrev-ref", "HEAD"], "unknown"),
      dirty: Keyword.get(opts, :dirty, default_dirty()),
      host: Keyword.get(opts, :host) || default_host(),
      mix_env: to_string(Mix.env()),
      trials: Keyword.get(bench_opts, :trials, 5),
      cold_open_p50_us: round2(metrics.cold_open_p50_us),
      cold_open_s3_p50_us: round2(metrics.cold_open_s3_p50_us),
      warm_s3_shards_per_s: round2(metrics.warm_s3_shards_per_s),
      failover_cold_s3_p50_us: round2(metrics.failover_cold_s3_p50_us),
      failover_warm_s3_p50_us: round2(metrics.failover_warm_s3_p50_us),
      dir_resolve_p50_us: round2(metrics.dir_resolve_p50_us),
      copy_keystone_rows_per_s: round2(metrics.copy_keystone_rows_per_s),
      fanout_kb_per_shard: round2(metrics.fanout_kb_per_shard),
      hrana_rt_us: metrics.hrana_rt_us,
      log: Keyword.get(opts, :log)
    }
  end

  defp print_table(metrics, line) do
    rows = [
      {"cold_open_p50_us", metrics.cold_open_p50_us, "µs   (cold-open, warm/local pull)"},
      {"cold_open_s3_p50_us", metrics.cold_open_s3_p50_us,
       "µs   (cold-open, pull from S3; opt-in)"},
      {"warm_s3_shards_per_s", metrics.warm_s3_shards_per_s,
       "shards/s (warm many from S3; opt-in)"},
      {"failover_cold_s3_p50_us", metrics.failover_cold_s3_p50_us,
       "µs   (failover open, cold full pull; opt-in)"},
      {"failover_warm_s3_p50_us", metrics.failover_warm_s3_p50_us,
       "µs   (failover open, warm 304-promote; opt-in)#{rto_speedup(metrics)}"},
      {"dir_resolve_p50_us", metrics.dir_resolve_p50_us, "µs   (directory resolve, warm)"},
      {"copy_keystone_rows_per_s", metrics.copy_keystone_rows_per_s,
       "rows/s (migration copy throughput, keystone rows)"},
      {"fanout_kb_per_shard", metrics.fanout_kb_per_shard, "KiB/shard (node density)"}
    ]

    dirty = if line.dirty, do: "-dirty", else: ""

    Mix.shell().error(
      "\n=== fathom.bench (#{line.commit}#{dirty}, #{line.host}, #{line.mix_env}) ==="
    )

    Enum.each(rows, fn {name, value, unit} ->
      Mix.shell().error("  #{String.pad_trailing(name, 20)} #{format_value(value)}  #{unit}")
    end)

    Mix.shell().error("")
  end

  # The warm-vs-cold RTO speedup, appended to the warm row when both are measured.
  defp rto_speedup(%{failover_cold_s3_p50_us: cold, failover_warm_s3_p50_us: warm})
       when is_number(cold) and is_number(warm) and warm > 0 do
    " — #{Float.round(cold / warm, 2)}x vs cold"
  end

  defp rto_speedup(_), do: ""

  defp format_value(nil), do: String.pad_leading("skipped", 12)

  defp format_value(v),
    do: String.pad_leading(:erlang.float_to_binary(round2(v) * 1.0, decimals: 2), 12)

  # --- option plumbing -----------------------------------------------------

  defp put_opt(acc, opts, key, transform \\ & &1) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> Keyword.put(acc, key, transform.(value))
      :error -> acc
    end
  end

  defp parse_only(str) do
    str
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_existing_atom(String.trim(&1)))
    |> Enum.filter(&(&1 in @all_metrics))
  end

  defp round2(nil), do: nil
  defp round2(v) when is_number(v), do: Float.round(v * 1.0, 2)

  # --- git / host context --------------------------------------------------

  defp git(args, default) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> String.trim(out)
      _ -> default
    end
  rescue
    _ -> default
  end

  defp default_dirty do
    case System.cmd("git", ["status", "--porcelain"], stderr_to_stdout: true) do
      {"", 0} -> false
      {_out, 0} -> true
      _ -> false
    end
  rescue
    _ -> false
  end

  defp default_host, do: :os.type() |> elem(1) |> to_string()
end
